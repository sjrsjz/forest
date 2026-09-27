import asyncio
import base64
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.facet_preparation import document_facets
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.module_configuration import ModuleConfigurationRepository


@pytest.mark.postgres
async def test_embedding_budget_serializes_claims_and_keeps_unknown_spend(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "budget-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
    )
    namespace = package.descriptor.storage.spec.namespace

    async def invoke(function, operation, body):
        context = {
            "bot_id": store.bot_id,
            "operation_id": operation,
            "participant_input_digest": digest_json(body),
        }
        envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "memory.graph", "generation_id": "generation/1"},
            "input": body,
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace), sql.Identifier(function)
                    ),
                    (Jsonb(context), Jsonb(envelope)),
                )
            ).fetchone()
        return row["result"]

    bound = await invoke("memory_bind_domain", "domain/budget", {
        "protocol": "forest.memory-domain-bind/1",
        "space_id": "sha256:" + "a" * 64,
        "embedding_model_kind": "TEXT_IMAGE",
        "model_revision_digest": "sha256:" + "b" * 64,
        "dimensions": 4096,
        "normalization": "L2",
        "fusion_policy_revision": "independent-facets/1",
    })
    assert bound["ok"] is True

    def reserve_body(name, *, request_limit=1):
        return {
            "protocol": "forest.memory-embedding-budget-reserve/1",
            "reservation_id": f"budget/{name}",
            "batch_ref": f"batch/{name}",
            "policy_revision": "daily-cost-cap/1",
            "request_limit": request_limit,
            "token_limit": 1000,
            "reserved_tokens": 100,
        }

    first, second = await asyncio.gather(
        invoke("memory_reserve_embedding_budget", "reserve/one", reserve_body("one")),
        invoke("memory_reserve_embedding_budget", "reserve/two", reserve_body("two")),
    )
    assert sorted(item["ok"] for item in (first, second)) == [False, True]
    winner = first if first["ok"] else second
    loser = second if first["ok"] else first
    assert loser["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_EXHAUSTED"
    name = "one" if first["ok"] else "two"
    operation = f"reserve/{name}"
    assert await invoke(
        "memory_reserve_embedding_budget", operation, reserve_body(name)
    ) == winner
    same_batch = reserve_body("different-id")
    same_batch["batch_ref"] = f"batch/{name}"
    conflict = await invoke(
        "memory_reserve_embedding_budget", "reserve/different-id", same_batch
    )
    assert conflict["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_CONFLICT"

    unknown = {
        "protocol": "forest.memory-embedding-budget-settle/1",
        "reservation_id": f"budget/{name}",
        "state": "UNKNOWN", "charged_tokens": None,
        "error_code": "EMBEDDING_TRANSPORT_UNKNOWN",
    }
    settled = await invoke("memory_settle_embedding_budget", "settle/unknown", unknown)
    assert settled["value"]["state"] == "UNKNOWN"
    assert await invoke(
        "memory_settle_embedding_budget", "settle/unknown", unknown
    ) == settled
    conflicting = await invoke(
        "memory_settle_embedding_budget", "settle/different", unknown
    )
    assert conflicting["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_CONFLICT"
    exhausted = await invoke(
        "memory_reserve_embedding_budget", "reserve/after-unknown",
        reserve_body("after-unknown"),
    )
    assert exhausted["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_EXHAUSTED"

    charged = await invoke(
        "memory_reserve_embedding_budget", "reserve/charged",
        reserve_body("charged", request_limit=2),
    )
    assert charged["value"]["state"] == "RESERVED"
    result = await invoke("memory_settle_embedding_budget", "settle/charged", {
        "protocol": "forest.memory-embedding-budget-settle/1",
        "reservation_id": "budget/charged",
        "state": "CHARGED", "charged_tokens": 150, "error_code": None,
    })
    assert result["value"]["charged_tokens"] == 150
    released = await invoke(
        "memory_reserve_embedding_budget", "reserve/released",
        reserve_body("released", request_limit=3),
    )
    assert released["value"]["state"] == "RESERVED"
    for name in ("released", "after-release"):
        if name == "after-release":
            admitted = await invoke(
                "memory_reserve_embedding_budget", f"reserve/{name}",
                reserve_body(name, request_limit=3),
            )
            assert admitted["value"]["state"] == "RESERVED"
        settled_release = await invoke("memory_settle_embedding_budget", f"settle/{name}", {
            "protocol": "forest.memory-embedding-budget-settle/1",
            "reservation_id": f"budget/{name}",
            "state": "RELEASED", "charged_tokens": None, "error_code": None,
        })
        assert settled_release["value"]["state"] == "RELEASED"
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                sql.SQL("SELECT reserved_requests,reserved_tokens,settled_requests,"
                        "charged_tokens,unknown_requests,unknown_tokens "
                        "FROM {}.embedding_budget_days WHERE bot_id=%s").format(
                    sql.Identifier(namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
    assert tuple(row.values()) == (0, 0, 1, 150, 1, 100)


@pytest.mark.postgres
async def test_graph_snapshot_excludes_facts_committed_after_registration(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "snapshot-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    async with store.pool.connection() as writer, store.pool.connection() as reader:
        async with writer.transaction():
            await writer.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_contributions "
                    "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                    "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                    "weight_micros,admitted_deletion_epoch) VALUES "
                    "(%s,'late-contribution','late-operation',%s,'late-relation',"
                    "'ASSOCIATES_WITH','ENTITY','left','ENTITY','right','RULE',500000,0)"
                ).format(namespace),
                (store.bot_id, "sha256:" + "a" * 64),
            )
            boundary = await (
                await reader.execute(
                    "SELECT pg_current_snapshot()::text AS snapshot, clock_timestamp() AS observed_at"
                )
            ).fetchone()
        result = await (
            await reader.execute(
                sql.SQL(
                    "SELECT pg_visible_in_snapshot(created_xid,%s::pg_snapshot) AS old_visible,"
                    "pg_visible_in_snapshot(created_xid,pg_current_snapshot()) AS new_visible,"
                    "created_at <= %s AS timestamp_precedes_boundary "
                    "FROM {}.relation_contributions WHERE bot_id=%s AND contribution_id=%s"
                ).format(namespace),
                (boundary["snapshot"], boundary["observed_at"], store.bot_id, "late-contribution"),
            )
        ).fetchone()
        assert result == {
            "old_visible": False,
            "new_visible": True,
            "timestamp_precedes_boundary": True,
        }


def _configuration():
    return {
        "protocol": "forest.memory.graph/2",
        "embedding": {
            "model_kind": "TEXT_IMAGE",
            "deployment_id": "embedding.multimodal/qiyu-vl/default",
            "model_revision": "operator-declared/1",
            "model": "Qwen/Qwen3-VL-Embedding-8B",
            "dimensions": 4096,
            "normalization": "L2",
        },
        "fusion_policy_revision": "independent-facets/1",
        "retrieval": {
            "policy_revision": "qiyu-graph-region/1",
            "seed_limit": 4,
            "candidate_limit_per_facet": 64,
            "min_similarity": 0.35,
            "min_graph_relevance": 0.05,
            "max_graph_nodes": 32,
            "max_association_edges_per_node": 32,
            "max_retrieval_chars": 24000,
            "sequence_weight": 0.55,
            "association_weight": 0.9,
        },
        "embedding_budget": {
            "policy_revision": "daily-cost-cap/1",
            "daily_request_limit": 1000,
            "daily_token_limit": 10000000,
            "reservation_tokens": 32768,
        },
    }


def test_document_facets_preserve_text_spans_and_image_identity():
    text = "甲" * 4200
    text_ref = {
        "payload_id": "payload/text", "digest": "sha256:" + "1" * 64,
        "owner_component_id": "adapter.onebot", "owner_kind": "context.input",
        "owner_id": "source/text",
    }
    image_ref = {
        **text_ref, "payload_id": "payload/image", "digest": "sha256:" + "2" * 64,
        "owner_id": "source/image",
    }
    parts = [
        {"part_id": "part/text", "part_kind": "TEXT", "payload": text_ref,
         "byte_length": len(text.encode())},
        {"part_id": "part/image", "part_kind": "IMAGE", "payload": image_ref,
         "byte_length": 128},
    ]
    first = document_facets("source/1", 1, "sha256:" + "a" * 64, parts, {
        "part/text": text,
    })
    assert first == document_facets("source/1", 1, "sha256:" + "a" * 64, parts, {
        "part/text": text,
    })
    assert [(f["selector_start_char"], f["selector_end_char"])
            for f in first[:3]] == [(0, 2048), (1920, 3968), (3840, 4200)]
    assert first[3]["modality"] == "IMAGE"
    assert first[3]["facet_content_digest"] == image_ref["digest"]
    assert len({item["facet_id"] for item in first}) == 4


def _envelope(
    generation_id,
    selection,
    deletion_epoch,
    *,
    operation="agent/acquire-model-input/1",
):
    return {
        "protocol": "forest.uow-step/1",
        "caller": {
            "component_id": "agent.runtime",
            "generation_id": generation_id,
        },
        "input": {
            "selection_step_id": "conversation",
            "deletion_epoch_step_id": "payload_epoch",
        },
        "prior": {
            "conversation": selection,
            "payload_epoch": deletion_epoch,
        },
        "_operation": operation,
    }


@pytest.mark.postgres
async def test_memory_embedding_identity_is_immutable_across_configuration_plans(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    source = Path(__file__).parents[1] / "src/forest/builtin/memory_graph"
    catalog = PackageCatalog(tmp_path / "memory-packages")
    package = await bootstrap_builtin_package(
        source, catalog, store, installer, kernel=kernel,
        initial_configuration=_configuration(),
    )
    schema_digest = digest_json(package.snapshot.schema("schemas/config.json"))
    changed_model = _configuration()
    changed_model["embedding"]["model_revision"] = "different-model/1"
    with pytest.raises(ForestError) as caught:
        await package.configuration.plan(
            operation_id="change-memory-model",
            expected_desired_revision=1,
            value=changed_model,
            validation_digest=schema_digest,
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IMMUTABLE"

    # An older writer's outstanding plan cannot bypass the new package policy.
    old_writer = ModuleConfigurationRepository(
        store,
        component_id="memory.graph",
        namespace=package.configuration.namespace,
        payloads=kernel.payload,
    )
    old_plan = await old_writer.plan(
        operation_id="old-writer-model-plan",
        expected_desired_revision=1,
        value=changed_model,
        validation_digest=schema_digest,
    )
    with pytest.raises(ForestError) as caught:
        await package.configuration.apply(
            operation_id="apply-old-model-plan",
            plan_id=old_plan["plan_id"],
            expected_desired_revision=1,
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IMMUTABLE"

    changed_budget = _configuration()
    changed_budget["embedding_budget"]["daily_request_limit"] = 250
    plan = await package.configuration.plan(
        operation_id="change-memory-budget",
        expected_desired_revision=1,
        value=changed_budget,
        validation_digest=schema_digest,
    )
    await package.configuration.apply(
        operation_id="apply-memory-budget",
        plan_id=plan["plan_id"],
        expected_desired_revision=1,
    )
    with pytest.raises(ForestError) as caught:
        await bootstrap_builtin_package(
            source, catalog, store, installer, kernel=kernel,
            initial_configuration=changed_model,
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IMMUTABLE"


@pytest.mark.postgres
@pytest.mark.parametrize(
    "resolution", ["HANDOFF", "ABANDON", "RESPONDED", "NOT_SENT", "UNKNOWN"]
)
async def test_memory_package_qualifies_exact_conversation_batch(
    clean_authority, tmp_path, resolution
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration=_configuration(),
    )
    lease = await kernel.runtime.claim_host(
        "memory-package-host",
        "memory-package-incarnation",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(host, (*kernel.descriptors, package.descriptor))
    await registry.start()
    try:
        instance = registry.get("memory.graph")
        generation_id = instance.generation_identity.generation_id
        participant = instance.unit_of_work_participant()
        payload_participant = registry.get("kernel.payload").unit_of_work_participant()
        domain_input = {
            "protocol": "forest.memory-domain-bind/1",
            "space_id": "sha256:" + "a" * 64,
            "embedding_model_kind": "TEXT_IMAGE",
            "model_revision_digest": "sha256:" + "b" * 64,
            "dimensions": 4096,
            "normalization": "L2",
            "fusion_policy_revision": "independent-facets/1",
        }
        async with store.pool.connection() as conn, conn.transaction():
            bound = await participant.invoke_in(
                conn,
                "memory.bind-domain-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "memory.graph",
                        "generation_id": generation_id,
                    },
                    "input": domain_input,
                    "prior": {},
                },
                operation_id="memory/domain/test",
                causal_root_kind="memory.domain",
                causal_root_id="bot-bundle",
                parent_call_id="call/memory/domain",
                timeout_seconds=10,
            )
            assert bound["space_id"] == domain_input["space_id"]
        async with store.pool.connection() as conn, conn.transaction():
            with pytest.raises(ForestError) as conflict:
                await participant.invoke_in(
                    conn,
                    "memory.bind-domain-record",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "memory.graph",
                            "generation_id": generation_id,
                        },
                        "input": {**domain_input, "space_id": "sha256:" + "c" * 64},
                        "prior": {},
                    },
                    operation_id="memory/domain/conflict",
                    causal_root_kind="memory.domain",
                    causal_root_id="bot-bundle",
                    parent_call_id="call/memory/domain-conflict",
                    timeout_seconds=10,
                )
            assert conflict.value.code == "MEMORY_DOMAIN_IDENTITY_CONFLICT"
        text_payload = await kernel.payload.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="context.input",
            owner_id="unit/event/captured:text",
            media_type="text/plain; charset=utf-8",
            content="来自 933179609 群的原始消息".encode(),
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        image_payload = await kernel.payload.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="context.image",
            owner_id="unit/event/captured:image",
            media_type="image/png",
            content=base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lXcAAAAASUVORK5CYII="
            ),
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        author_payload = await kernel.payload.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="message.author",
            owner_id="event/captured",
            media_type="application/json",
            content=b'{"platform":"onebot-v11","user_id":"42"}',
            expected_deletion_epoch=await kernel.payload.epoch(),
        )

        pending_selection = {
            "status": "BATCH",
            "input_batch_id": "batch/memory/pending",
            "conversation_id": "onebot:group:933179609",
            "context_revision": 1,
            "event_ids": ["event/pending"],
        }
        async with store.pool.connection() as conn, conn.transaction():
            deletion_epoch = await payload_participant.invoke_in(
                conn,
                "payload.capture-deletion-epoch",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "agent.runtime",
                        "generation_id": generation_id,
                    },
                    "input": {"protocol": "forest.payload-epoch-capture/1"},
                    "prior": {},
                },
            )
            pending_envelope = _envelope(
                generation_id,
                pending_selection,
                deletion_epoch,
            )
            pending = await participant.invoke_in(
                conn,
                "memory.qualify-input-batch",
                {key: value for key, value in pending_envelope.items() if key != "_operation"},
                operation_id=pending_envelope["_operation"],
                causal_root_kind="agent.execute",
                causal_root_id="run/memory/package",
                parent_call_id="call/memory/pending",
                timeout_seconds=10,
            )
        assert pending == {
            "protocol": "forest.memory-input-qualified/1",
            "input_batch_ref": "batch/memory/pending",
            "qualified_count": 0,
            "pending_count": 1,
            "deletion_epoch": 0,
        }

        namespace = package.descriptor.storage.spec.namespace
        async with store.pool.connection() as conn, conn.transaction():
            archived_pending = await participant.invoke_in(
                conn,
                "memory.qualify-archive",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": generation_id,
                    },
                    "input": {},
                    "prior": {
                        "prepare": {
                            "status": "WRITE",
                            "commit": {
                                "conversation_id": "onebot:group:933179609",
                                "compaction_id": "compaction/memory/pending",
                                "archived_unit_count": 1,
                                "units": [{
                                    "event_id": "event/pending",
                                    "input_batch_id": "batch/memory/pending",
                                    "context_revision": 1,
                                }],
                            },
                        },
                        "commit": {
                            "status": "BATCH",
                            "operation_id": "conversation/silent-archive/pending",
                            "conversation_id": "onebot:group:933179609",
                            "classified_through_seq": 1,
                            "selected_input_end_seq": 1,
                            "mailbox_revision": 1,
                            "input_batch_id": "batch/memory/pending",
                            "boundary": "IDLE",
                            "kind": "SILENT_DRAIN",
                            "start_seq": 1,
                            "end_seq": 1,
                            "trigger_cut_seq": None,
                            "session_ref": None,
                            "run_ref": None,
                            "context_revision": 1,
                            "context_view_revision": 1,
                            "event_ids": ["event/pending"],
                            "trigger_ids": [],
                            "triggers": [],
                            "memory_archive": {
                                "compaction_ref": "compaction/memory/pending",
                                "event_refs": ["event/pending"],
                            },
                        },
                        "payload_epoch": deletion_epoch,
                    },
                },
                operation_id="memory/archive/pending",
                causal_root_kind="conversation.silent-drain",
                causal_root_id="onebot:group:933179609",
                parent_call_id="call/memory/archive/pending",
                timeout_seconds=10,
            )
            assert archived_pending["memory_archive"]["event_refs"] == ["event/pending"]
            await participant.invoke_in(
                conn,
                "memory.mark-source-unavailable-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "memory.graph",
                        "generation_id": generation_id,
                    },
                    "input": {
                        "conversation_ref": "onebot:group:933179609",
                        "event_ref": "event/pending",
                        "error_code": "SOURCE_NOT_AVAILABLE",
                    },
                    "prior": {},
                },
                operation_id="memory/unavailable/pending",
                causal_root_kind="memory.capture",
                causal_root_id="event/pending",
                parent_call_id="call/memory/unavailable/pending",
                timeout_seconds=10,
            )
        async with store.pool.connection() as conn:
            pending_states = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT i.state AS input_state,a.state AS archive_state "
                        "FROM {}.pending_input_events i JOIN {}.pending_archive_events a "
                        "USING (bot_id,conversation_ref,event_ref) "
                        "WHERE i.bot_id=%s AND i.event_ref=%s"
                    ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                    (store.bot_id, "event/pending"),
                )
            ).fetchone()
        assert pending_states == {
            "input_state": "UNAVAILABLE",
            "archive_state": "UNAVAILABLE",
        }

        async with store.pool.connection() as conn, conn.transaction():
            deletion_epoch = await payload_participant.invoke_in(
                conn,
                "payload.capture-deletion-epoch",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "adapter.onebot",
                        "generation_id": generation_id,
                    },
                    "input": {"protocol": "forest.payload-epoch-capture/1"},
                    "prior": {},
                },
            )
            captured_source = await participant.invoke_in(
                conn,
                "memory.capture-source-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "adapter.onebot",
                        "generation_id": generation_id,
                    },
                    "input": {
                        "capture": {
                            "identity": {
                                "source_id": "source/group/933179609",
                                "source_kind": "GROUP",
                                "identity_incarnation": 1,
                                "platform": "onebot-v11",
                                "realm_id": "qq",
                                "external_group_id": "933179609",
                                "trunk_id": "trunk/group/933179609",
                            },
                            "source_unit_id": "unit/event/captured",
                            "source_revision": 1,
                            "unit_kind": "MESSAGE",
                            "author_subject_ref": "qq:42",
                            "author": {
                                "payload_id": author_payload.payload_id,
                                "digest": author_payload.content_digest,
                                "owner_component_id": "adapter.onebot",
                                "owner_kind": "message.author",
                                "owner_id": "event/captured",
                            },
                            "parts": [
                                {
                                    "part_id": "part/event/captured/text",
                                    "part_index": 0,
                                    "part_kind": "TEXT",
                                    "media_type": text_payload.media_type,
                                    "payload": {
                                        "payload_id": text_payload.payload_id,
                                        "digest": text_payload.content_digest,
                                        "owner_component_id": "adapter.onebot",
                                        "owner_kind": "context.input",
                                        "owner_id": "unit/event/captured:text",
                                    },
                                    "width": None,
                                    "height": None,
                                    "byte_length": text_payload.byte_length,
                                },
                                {
                                    "part_id": "part/event/captured/image",
                                    "part_index": 1,
                                    "part_kind": "IMAGE",
                                    "media_type": image_payload.media_type,
                                    "payload": {
                                        "payload_id": image_payload.payload_id,
                                        "digest": image_payload.content_digest,
                                        "owner_component_id": "adapter.onebot",
                                        "owner_kind": "context.image",
                                        "owner_id": "unit/event/captured:image",
                                    },
                                    "width": 1,
                                    "height": 1,
                                    "byte_length": image_payload.byte_length,
                                },
                            ],
                            "occurrence": {
                                "occurrence_id": "occurrence/event/captured",
                                "binding_ref": "binding/main",
                                "conversation_ref": "onebot:group:933179609",
                                "event_ref": "event/captured",
                                "ingress_seq": None,
                                "observed_at": "2026-09-25T00:00:00+00:00",
                            },
                            "occurred_at": "2026-09-25T00:00:00+00:00",
                        },
                        "conversation_step_id": "conversation",
                        "deletion_epoch_step_id": "payload_epoch",
                    },
                    "prior": {
                        "conversation": {
                            "conversation_id": "onebot:group:933179609",
                            "event_id": "event/captured",
                            "ingress_seq": 1,
                        },
                        "payload_epoch": deletion_epoch,
                    },
                },
                operation_id="capture/event/captured",
                causal_root_kind="adapter.ingress",
                causal_root_id="event/captured",
                parent_call_id="call/memory/capture",
                timeout_seconds=10,
            )
        assert captured_source["eligibility_state"] == "REGISTERED"

        captured_selection = {
            "status": "BATCH",
            "input_batch_id": "batch/memory/captured",
            "conversation_id": "onebot:group:933179609",
            "context_revision": 2,
            "event_ids": ["event/captured"],
        }
        for replay in range(2):
            async with store.pool.connection() as conn, conn.transaction():
                deletion_epoch = await payload_participant.invoke_in(
                    conn,
                    "payload.capture-deletion-epoch",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "agent.runtime",
                            "generation_id": generation_id,
                        },
                        "input": {"protocol": "forest.payload-epoch-capture/1"},
                        "prior": {},
                    },
                )
                captured_envelope = _envelope(
                    generation_id,
                    captured_selection,
                    deletion_epoch,
                    operation="agent/acquire-model-input/2",
                )
                captured = await participant.invoke_in(
                    conn,
                    "memory.qualify-input-batch",
                    {
                        key: value
                        for key, value in captured_envelope.items()
                        if key != "_operation"
                    },
                    operation_id=captured_envelope["_operation"],
                    causal_root_kind="agent.execute",
                    causal_root_id="run/memory/package",
                    parent_call_id=f"call/memory/captured/{replay}",
                    timeout_seconds=10,
                )
            assert captured["qualified_count"] == 1
            assert captured["pending_count"] == 0

        async with store.pool.connection() as conn:
            active_unit = await (
                await conn.execute(
                    sql.SQL("SELECT eligibility_state FROM {}.source_units "
                            "WHERE bot_id=%s AND source_unit_id=%s").format(
                        sql.Identifier(namespace)
                    ),
                    (store.bot_id, "unit/event/captured"),
                )
            ).fetchone()
        assert active_unit["eligibility_state"] == "REGISTERED"
        async with store.pool.connection() as conn:
            pending_facets = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS n FROM {}.facet_preparation_work "
                            "WHERE bot_id=%s AND source_unit_id=%s").format(
                        sql.Identifier(namespace)
                    ),
                    (store.bot_id, "unit/event/captured"),
                )
            ).fetchone()
        assert pending_facets["n"] == 0

        archived_selection = {
            "status": "BATCH",
            "operation_id": "conversation/silent-archive/test",
            "conversation_id": "onebot:group:933179609",
            "classified_through_seq": 1,
            "selected_input_end_seq": 1,
            "mailbox_revision": 1,
            "input_batch_id": "batch/memory/captured",
            "boundary": "IDLE",
            "kind": "SILENT_DRAIN",
            "start_seq": 1,
            "end_seq": 1,
            "trigger_cut_seq": None,
            "session_ref": None,
            "run_ref": None,
            "context_revision": 2,
            "context_view_revision": 2,
            "event_ids": ["event/captured"],
            "trigger_ids": [],
            "triggers": [],
            "memory_archive": {
                "compaction_ref": "compaction/memory/test",
                "event_refs": ["event/captured"],
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            epoch = await payload_participant.invoke_in(
                conn,
                "payload.capture-deletion-epoch",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": generation_id,
                    },
                    "input": {"protocol": "forest.payload-epoch-capture/1"},
                    "prior": {},
                },
            )
            archive = await participant.invoke_in(
                conn,
                "memory.qualify-archive",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": generation_id,
                    },
                    "input": {},
                    "prior": {
                        "prepare": {
                            "status": "WRITE",
                            "commit": {
                                "conversation_id": "onebot:group:933179609",
                                "compaction_id": "compaction/memory/test",
                                "archived_unit_count": 1,
                                "units": [{
                                    "event_id": "event/captured",
                                    "input_batch_id": "batch/memory/captured",
                                    "context_revision": 2,
                                }],
                            },
                        },
                        "commit": archived_selection,
                        "payload_epoch": epoch,
                    },
                },
                operation_id="memory/archive/test",
                causal_root_kind="conversation.silent-drain",
                causal_root_id="onebot:group:933179609",
                parent_call_id="call/memory/archive",
                timeout_seconds=10,
            )
        assert archive == archived_selection

        async with asyncio.timeout(20):
            while True:
                async with store.pool.connection() as conn:
                    prepared = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,prepared_facet_count,last_error_code "
                                "FROM {}.facet_preparation_work WHERE bot_id=%s "
                                "AND source_unit_id=%s"
                            ).format(sql.Identifier(namespace)),
                            (store.bot_id, "unit/event/captured"),
                        )
                    ).fetchone()
                if prepared is not None and prepared["state"] == "READY":
                    break
                await asyncio.sleep(0.1)
        assert prepared["prepared_facet_count"] == 2
        async with store.pool.connection() as conn:
            saved_facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT modality,purpose,state,source_unit_id,space_id "
                        "FROM {}.embedding_facets WHERE bot_id=%s ORDER BY modality DESC"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id,),
                )
            ).fetchall()
        assert [(row["modality"], row["purpose"], row["state"])
                for row in saved_facets] == [
                    ("TEXT", "DOCUMENT", "PENDING"),
                    ("IMAGE", "DOCUMENT", "PENDING"),
                ]
        assert {row["space_id"] for row in saved_facets} == {domain_input["space_id"]}
        async with store.pool.connection() as conn:
            candidate_row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT {}.memory_read_embedding_candidates(%s::jsonb,%s::jsonb) "
                        "AS result"
                    ).format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({"limit": 1})),
                )
            ).fetchone()
        candidates = candidate_row["result"]
        assert candidates["ok"] is True
        assert candidates["value"]["space_id"] == domain_input["space_id"]
        assert candidates["value"]["has_more"] is True
        assert len(candidates["value"]["facets"]) == 1
        candidate = candidates["value"]["facets"][0]
        assert candidate["trunk_id"] == "trunk/group/933179609"
        assert candidate["conversation_ref"] == "onebot:group:933179609"
        assert candidate["author_ref"] == "qq:42"
        assert candidate["input_payload"]["digest"] in {
            text_payload.content_digest, image_payload.content_digest,
        }

        async def claim_batch(token, lease_seconds=3):
            async with store.pool.connection() as conn, conn.transaction():
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_claim_embedding_batch(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({"bot_id": store.bot_id}),
                            Jsonb({
                                "protocol": "forest.owned-work-claim-request/1",
                                "claim_token": token,
                                "lease_seconds": lease_seconds,
                            }),
                        ),
                    )
                ).fetchone()
            return row["result"]

        first_claim, second_claim = await asyncio.gather(
            claim_batch("embedding/claim/one"), claim_batch("embedding/claim/two")
        )
        assert first_claim["ok"] and second_claim["ok"]
        claimed = [
            claim["value"] for claim in (first_claim, second_claim)
            if claim["value"] is not None
        ]
        assert sum(len(claim["payload"]["facets"]) for claim in claimed) == 2
        assert len({
            facet["facet_id"] for claim in claimed for facet in claim["payload"]["facets"]
        }) == 2
        assert await claim_batch("embedding/claim/one") == first_claim
        assert (await claim_batch("embedding/claim/three"))["value"] is None
        assert all(
            claim["payload"]["space_id"] == domain_input["space_id"]
            and claim["payload"]["source_epoch_high_watermark"] >= 0
            for claim in claimed
        )
        # Keep background dispatch out of this manual lease-expiry arbitration.
        instance.freeze_owned_admission()
        await instance.drain_owned_work()
        await asyncio.sleep(3.1)
        reclaimed = await claim_batch("embedding/claim/after-expiry", lease_seconds=360)
        assert reclaimed["ok"] is True
        assert {
            facet["facet_id"] for facet in reclaimed["value"]["payload"]["facets"]
        } == {
            facet["facet_id"] for claim in claimed for facet in claim["payload"]["facets"]
        }
        claim_id = reclaimed["value"]["claim_token"]
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.embedding_budget_days("
                    "bot_id,budget_day,reserved_requests,reserved_tokens) "
                    "VALUES(%s,current_date,1,100)"
                ).format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.embedding_budget_reservations("
                    "bot_id,reservation_id,operation_id,input_digest,batch_ref,"
                    "policy_revision,budget_day,request_limit,token_limit,"
                    "reserved_tokens,deadline_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,current_date,1000,100000,100,"
                    "clock_timestamp()+interval '1 hour')"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "budget/embedding/claim", "reserve/embedding/claim",
                    "sha256:" + "b" * 64, claim_id, "daily-cost-cap/1",
                ),
            )
            prepared_attempt = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT {}.memory_prepare_embedding_attempt(%s::jsonb,%s::jsonb) "
                        "AS result"
                    ).format(sql.Identifier(namespace)),
                    (
                        Jsonb({"bot_id": store.bot_id}),
                        Jsonb({
                            "protocol": "forest.uow-step/1",
                            "caller": {
                                "component_id": "memory.graph",
                                "generation_id": generation_id,
                            },
                            "input": {
                                "protocol": "forest.memory-embedding-attempt-prepare/1",
                                "claim_id": claim_id,
                                "attempt_id": "attempt/embedding/claim",
                                "effect_operation_id": "effect/embedding/claim",
                                "reservation_id": "budget/embedding/claim",
                                "space_id": domain_input["space_id"],
                            },
                            "prior": {
                                "payload_epoch": {
                                    "protocol": "forest.payload-epoch/1",
                                    "deletion_epoch": 0,
                                },
                                "budget": {
                                    "protocol": "forest.memory-embedding-budget/1",
                                    "reservation_id": "budget/embedding/claim",
                                    "state": "RESERVED",
                                },
                                "authorization": {},
                                "effect": {
                                    "protocol": "forest.effect-command-permit/1",
                                    "authorization_ticket_id": "ticket/embedding/claim",
                                    "wire_payload": {
                                        "payload_id": "payload/effect-wire",
                                        "digest": "sha256:" + "d" * 64,
                                        "owner_component_id": "kernel.effect",
                                        "owner_kind": "effect.wire",
                                        "owner_id": "attempt/embedding/claim",
                                    },
                                    "permit": {
                                        "protocol": "forest.effect-permit/1",
                                        "effect_operation_id": "effect/embedding/claim",
                                        "attempt_id": "attempt/embedding/claim",
                                        "attempt_no": 1,
                                        "executor_component_id": "embedding.multimodal",
                                        "executor_generation_id": "generation/embedding",
                                        "fence_kind": "runtime.graph",
                                        "fence_id": "graph/1",
                                        "fence_epoch": 1,
                                        "wire_payload_id": "payload/effect-wire",
                                        "wire_payload_digest": "sha256:" + "d" * 64,
                                        "deadline_at": "2099-01-01T00:00:00+00:00",
                                    },
                                },
                            },
                        }),
                    ),
                )
            ).fetchone()
            attempt_members = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS n FROM {}.embedding_facets "
                        "WHERE bot_id=%s AND current_attempt_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "attempt/embedding/claim"),
                )
            ).fetchone()
        assert attempt_members["n"] == 2
        assert prepared_attempt["result"]["value"]["facet_count"] == 2

        if resolution == "ABANDON":
            budget_input = {
                "protocol": "forest.memory-embedding-budget-settle/1",
                "reservation_id": "budget/embedding/claim",
                "state": "RELEASED",
                "charged_tokens": None,
                "error_code": "MEMORY_EMBEDDING_HANDOFF_NOT_SENT",
            }
            abandon_input = {
                "protocol": "forest.memory-embedding-abandon/1",
                "claim_id": claim_id,
                "attempt_id": "attempt/embedding/claim",
                "effect_operation_id": "effect/embedding/claim",
                "abandonment_operation_id": "effect/abandon/embedding/claim",
                "reservation_id": "budget/embedding/claim",
                "reason_code": "MEMORY_EMBEDDING_HANDOFF_NOT_SENT",
            }
            async with store.pool.connection() as conn, conn.transaction():
                released = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_settle_embedding_budget(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "budget/release/embedding/claim",
                                "participant_input_digest": digest_json(budget_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": budget_input,
                                "prior": {},
                            }),
                        ),
                    )
                ).fetchone()
                assert released["result"]["ok"] is True
                abandoned = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_abandon_embedding_attempt(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "memory/abandon/embedding/claim",
                                "participant_input_digest": digest_json(abandon_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": abandon_input,
                                "prior": {
                                    "effect": {
                                        "attempt_id": "attempt/embedding/claim",
                                        "abandonment_operation_id": "effect/abandon/embedding/claim",
                                        "reason_code": "MEMORY_EMBEDDING_HANDOFF_NOT_SENT",
                                        "cancel_effect": False,
                                    },
                                    "budget": released["result"]["value"],
                                },
                            }),
                        ),
                    )
                ).fetchone()
                states = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT state,current_attempt_id FROM {}.embedding_facets "
                            "WHERE bot_id=%s AND facet_id IN "
                            "(SELECT facet_id FROM {}.embedding_claim_items "
                            "WHERE bot_id=%s AND claim_id=%s)"
                        ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                        (store.bot_id, store.bot_id, claim_id),
                    )
                ).fetchall()
            assert abandoned["result"]["value"]["state"] == "NOT_SENT"
            assert len(states) == 2
            assert all(row["state"] == "PENDING" and row["current_attempt_id"] is None
                       for row in states)
            retry_claim = await claim_batch("embedding/claim/after-abandon", lease_seconds=360)
            assert retry_claim["ok"] is True
            assert retry_claim["value"] is None
            async with store.pool.connection() as conn:
                cooldown = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT count(*) AS n FROM {}.embedding_facets "
                            "WHERE bot_id=%s AND state='PENDING' "
                            "AND next_attempt_at>clock_timestamp()"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id,),
                    )
                ).fetchone()
            assert cooldown["n"] == 2
            return

        async def handoff(epoch):
            async with store.pool.connection() as conn, conn.transaction():
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_mark_embedding_handoff(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({"bot_id": store.bot_id}),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": {
                                    "protocol": "forest.memory-embedding-handoff/1",
                                    "claim_id": claim_id,
                                    "attempt_id": "attempt/embedding/claim",
                                    "effect_operation_id": "effect/embedding/claim",
                                    "handoff_operation_id": "handoff/embedding/claim",
                                },
                                "prior": {
                                    "payload_epoch": {
                                        "protocol": "forest.payload-epoch/1",
                                        "deletion_epoch": epoch,
                                    },
                                    "effect": {
                                        "effect_operation_id": "effect/embedding/claim",
                                        "attempt_id": "attempt/embedding/claim",
                                        "handoff_operation_id": "handoff/embedding/claim",
                                        "state": "DISPATCHING",
                                        "wire_payload_id": "payload/effect-wire",
                                        "wire_payload_digest": "sha256:" + "d" * 64,
                                        "executor_component_id": "embedding.multimodal",
                                        "executor_generation_id": "generation/embedding",
                                        "fence_id": "graph/1",
                                        "fence_epoch": 1,
                                    },
                                },
                            }),
                        ),
                    )
                ).fetchone()
                return row["result"]

        stale_handoff = await handoff(1)
        assert stale_handoff["ok"] is False
        assert stale_handoff["error"]["code"] == "MEMORY_EMBEDDING_HANDOFF_STALE"
        dispatched_handoff = await handoff(0)
        assert dispatched_handoff["ok"] is True
        assert dispatched_handoff["value"]["facet_count"] == 2
        async with store.pool.connection() as conn:
            dispatched_facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS n FROM {}.embedding_facets "
                        "WHERE bot_id=%s AND current_attempt_id=%s AND state='DISPATCHED'"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "attempt/embedding/claim"),
                )
            ).fetchone()
        assert dispatched_facets["n"] == 2

        if resolution == "HANDOFF":
            failed_work = {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": claim_id,
                "claim_token": claim_id,
                "attempt": 1,
                "outcome": {
                    "status": "FAILED",
                    "error": {
                        "code": "WORKER_RESTARTED",
                        "status": 503,
                        "retryable": False,
                        "unknown": True,
                    },
                },
            }
            async with store.pool.connection() as conn, conn.transaction():
                settled = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_settle_embedding_batch("
                            "%s::jsonb,%s::jsonb) AS result"
                        ).format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id}), Jsonb(failed_work)),
                    )
                ).fetchone()
                persisted = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT c.state AS claim_state,a.state AS attempt_state "
                            "FROM {}.embedding_claims c JOIN {}.embedding_batch_attempts a "
                            "USING(bot_id,claim_id) WHERE c.bot_id=%s AND c.claim_id=%s"
                        ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                        (store.bot_id, claim_id),
                    )
                ).fetchone()
            assert settled["result"]["value"]["work_id"] == claim_id
            assert persisted == {"claim_state": "CLAIMED", "attempt_state": "DISPATCHED"}

        if resolution != "HANDOFF":
            observed_state = {
                "RESPONDED": "SUCCEEDED",
                "NOT_SENT": "NOT_APPLIED",
                "UNKNOWN": "UNKNOWN",
            }[resolution]
            budget_state = {
                "RESPONDED": "CHARGED",
                "NOT_SENT": "RELEASED",
                "UNKNOWN": "UNKNOWN",
            }[resolution]
            charged_tokens = 12 if resolution == "RESPONDED" else None
            result_ref = ({
                "payload_id": "payload/embedding-result",
                "digest": "sha256:" + "c" * 64,
                "owner_component_id": "embedding.multimodal",
                "owner_kind": "embedding.result",
                "owner_id": "attempt/embedding/claim",
            } if resolution == "RESPONDED" else None)
            vectors = [
                {
                    "facet_id": facet["facet_id"],
                    "index": index,
                    "vector_digest": "sha256:" + str(index + 1) * 64,
                }
                for index, facet in enumerate(reclaimed["value"]["payload"]["facets"])
            ] if result_ref is not None else []
            budget_input = {
                "protocol": "forest.memory-embedding-budget-settle/1",
                "reservation_id": "budget/embedding/claim",
                "state": budget_state,
                "charged_tokens": charged_tokens,
                "error_code": None if result_ref is not None else "EMBEDDING_RESULT_UNKNOWN",
            }
            memory_input = {
                "protocol": "forest.memory-embedding-observe/1",
                "claim_id": claim_id,
                "attempt_id": "attempt/embedding/claim",
                "effect_operation_id": "effect/embedding/claim",
                "observation_operation_id": "effect/observation/embedding/claim",
                "reservation_id": "budget/embedding/claim",
                "observed_state": observed_state,
                "evidence_digest": "sha256:" + "e" * 64,
                "result": result_ref,
                "vectors": vectors,
                "charged_tokens": charged_tokens,
                "error_code": budget_input["error_code"],
            }
            async with store.pool.connection() as conn, conn.transaction():
                released = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_settle_embedding_budget(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "budget/observe/embedding/claim",
                                "participant_input_digest": digest_json(budget_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": budget_input,
                                "prior": {},
                            }),
                        ),
                    )
                ).fetchone()
                assert released["result"]["ok"] is True
                observed = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_observe_embedding_attempt(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "memory/observe/embedding/claim",
                                "participant_input_digest": digest_json(memory_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": memory_input,
                                "prior": {
                                    "effect": {
                                        "protocol": "forest.effect-command-observation/1",
                                        "observation": {
                                            "attempt_id": "attempt/embedding/claim",
                                            "observation_operation_id":
                                                "effect/observation/embedding/claim",
                                            "observed_status": observed_state,
                                            "resulting_state": observed_state,
                                            "evidence_kind": "executor",
                                            "accepted": True,
                                        },
                                        "evidence_payload": {
                                            "owner_component_id": "kernel.effect",
                                            "owner_kind": "effect.evidence",
                                            "owner_id": "effect/observation/embedding/claim",
                                            "digest": "sha256:" + "e" * 64,
                                        },
                                        "evidence_parents": [result_ref] if result_ref else [],
                                    },
                                    "budget": released["result"]["value"],
                                },
                            }),
                        ),
                    )
                ).fetchone()
                facet_states = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT state,vector_digest,result_payload_id "
                            "FROM {}.embedding_facets WHERE bot_id=%s "
                            "AND current_attempt_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id, "attempt/embedding/claim"),
                    )
                ).fetchall()
            assert observed["result"]["ok"] is True
            assert observed["result"]["value"]["state"] == {
                "RESPONDED": "SUCCEEDED", "NOT_SENT": "NOT_SENT", "UNKNOWN": "UNKNOWN"
            }[resolution]
            if resolution == "NOT_SENT":
                retry_claim = await claim_batch(
                    "embedding/claim/after-not-sent", lease_seconds=360
                )
                assert retry_claim["ok"] is True
                assert retry_claim["value"] is None
                async with store.pool.connection() as conn:
                    cooldown = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT count(*) AS n FROM {}.embedding_facets "
                                "WHERE bot_id=%s AND state='PENDING' "
                                "AND next_attempt_at>clock_timestamp()"
                            ).format(sql.Identifier(namespace)),
                            (store.bot_id,),
                        )
                    ).fetchone()
                assert cooldown["n"] == 2
            else:
                assert len(facet_states) == 2
                assert all(row["state"] == (
                    "EMBEDDED" if result_ref else "UNKNOWN"
                ) for row in facet_states)
                assert all((row["result_payload_id"] == "payload/embedding-result")
                           == (result_ref is not None) for row in facet_states)
            return

        identity = instance.generation_identity
        endpoint = instance.runtime_service_handlers()[
            ("agent.tool.memory-exact-recall", 1)
        ]
        receipt = await endpoint.invoke_routed(
            FrozenServiceRoute(
                service_id="agent.tool.memory-exact-recall",
                version=1,
                caller_component_id="agent.runtime",
                provider_component_id="memory.graph",
                provider_generation_id=identity.generation_id,
                graph_id=identity.graph_id,
                graph_revision=identity.graph_revision,
                admission_lane="DATA",
                owned_work=False,
            ),
            RuntimeServiceInvocation(
                "memory-exact-recall/package-test",
                20,
                "agent.tool",
                "tool-call/memory/package-test",
            ),
            (),
            {
                "protocol": "forest.agent-tool-execute/1",
                "tool_call_id": "tool-call/memory/package-test",
                "effect_operation_id": "effect/memory/package-test",
                "effect_attempt_id": "attempt/memory/package-test/1",
                "component_id": "memory.graph",
                "generation_id": identity.generation_id,
                "service_id": "agent.tool.memory-exact-recall",
                "contract_digest": "sha256:" + "5" * 64,
                "binding_digest": "sha256:" + "6" * 64,
                "authority_trigger_ref": "event/captured",
                "authority_principal_ref": "qq:42",
                "arguments": {
                    "source_unit_ids": [
                        "unit/event/captured",
                        "unit/event/missing",
                    ]
                },
            },
        )
        assert receipt["status"] == "SUCCEEDED"
        assert receipt["effect_attempt_id"] == "attempt/memory/package-test/1"
        result = json.loads(
            (await kernel.payload.read_bytes(receipt["result"]["payload_id"])).decode()
        )
        assert result["missing_source_unit_ids"] == ["unit/event/missing"]
        assert result["evidence"] == [
            {
                "source_unit_id": "unit/event/captured",
                "source_revision": 1,
                "source": {
                    "source_id": "source/group/933179609",
                    "source_kind": "GROUP",
                    "platform": "onebot-v11",
                    "realm_id": "qq",
                    "group_id": "933179609",
                    "trunk_id": "trunk/group/933179609",
                },
                "author": {
                    "principal_ref": "qq:42",
                    "identity_origin": "memory_verified",
                },
                "occurred_at": result["evidence"][0]["occurred_at"],
                "parts": [
                    {
                        "part_id": "part/event/captured/text",
                        "type": "text",
                        "text": "来自 933179609 群的原始消息",
                    },
                    {
                        "part_id": "part/event/captured/image",
                        "type": "image",
                        "media_ref": {
                            "payload_id": image_payload.payload_id,
                            "digest": image_payload.content_digest,
                            "owner_component_id": "adapter.onebot",
                            "owner_kind": "context.image",
                            "owner_id": "unit/event/captured:image",
                        },
                        "media_type": "image/png",
                        "width": 1,
                        "height": 1,
                    },
                ],
            }
        ]

        queries = {
            declaration["name"]: declaration
            for declaration in instance.component["domain_queries"]
        }
        trunks = await instance.invoke_management_query(
            queries["memory.list-trunks"], {}, timeout_seconds=10
        )
        assert trunks["items"] == [
            {
                "trunk_id": "trunk/group/933179609",
                "source_id": "source/group/933179609",
                "platform": "onebot-v11",
                "realm_id": "qq",
                "external_group_id": "933179609",
                "registered_sources": 1,
                "eligible_sources": 1,
            }
        ]
        sources = await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"author_subject_ref": "qq:42", "trunk_id": "trunk/group/933179609"},
            timeout_seconds=10,
        )
        assert [item["source_unit_id"] for item in sources["items"]] == [
            "unit/event/captured"
        ]
        assert sources["items"][0]["external_group_id"] == "933179609"
        assert sources["items"][0]["author_subject_ref"] == "qq:42"
        assert sources["has_more"] is False
        cursor = {
            "snapshot_accepted_at": sources["snapshot_accepted_at"],
            "after_occurred_at": sources["items"][0]["occurred_at"],
            "after_source_unit_id": sources["items"][0]["source_unit_id"],
            "cursor_filter_digest": sources["filter_digest"],
        }
        assert (await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"author_subject_ref": "qq:42", "trunk_id": "trunk/group/933179609", **cursor},
            timeout_seconds=10,
        ))["items"] == []
        with pytest.raises(ForestError) as changed_filter:
            await instance.invoke_management_query(
                queries["memory.query-sources"],
                {"author_subject_ref": "qq:other", "trunk_id": "trunk/group/933179609", **cursor},
                timeout_seconds=10,
            )
        assert changed_filter.value.code == "MEMORY_QUERY_INVALID"
        assert (await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"author_subject_ref": "qq:other"}, timeout_seconds=10
        ))["items"] == []
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_contributions "
                    "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                    "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                    "weight_micros,admitted_deletion_epoch) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "contribution/test", "relation/test",
                    "sha256:" + "a" * 64, "relation/topic/test", "ASSOCIATES_WITH",
                    "SOURCE_UNIT", "unit/event/captured", "ENTITY", "topic/test",
                    "MANUAL", 900000, 0,
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_evidence "
                    "(bot_id,contribution_id,evidence_index,source_unit_id,"
                    "source_revision,part_id) VALUES (%s,%s,%s,%s,%s,%s)"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "contribution/test", 0, "unit/event/captured",
                    1, "part/event/captured/text",
                ),
            )
        relations = await instance.invoke_management_query(
            queries["memory.query-relations"],
            {"endpoint_kind": "SOURCE_UNIT", "endpoint_id": "unit/event/captured"},
            timeout_seconds=10,
        )
        assert len(relations["items"]) == 1
        assert relations["items"][0]["right_id"] == "topic/test"
        assert relations["items"][0]["evidence_count"] == 1
        assert relations["items"][0]["evidence"] == [{
            "source_unit_id": "unit/event/captured",
            "source_revision": 1,
            "part_id": "part/event/captured/text",
            "trunk_id": "trunk/group/933179609",
            "external_group_id": "933179609",
            "author_subject_ref": "qq:42",
        }]

        async with store.pool.connection() as conn:
            occurrence = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT eligibility_state,input_batch_ref,context_revision FROM {}."
                        "source_occurrences WHERE bot_id=%s AND occurrence_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "occurrence/event/captured"),
                )
            ).fetchone()
            operation_count = (
                await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT count(*) AS n FROM {}.eligibility_operations "
                            "WHERE bot_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id,),
                    )
                ).fetchone()
            )["n"]
        assert occurrence == {
            "eligibility_state": "ELIGIBLE",
            "input_batch_ref": "batch/memory/captured",
            "context_revision": 2,
        }
        assert operation_count == 1
    finally:
        assert await registry.close() == []
