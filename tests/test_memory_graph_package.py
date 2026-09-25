import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.module_configuration import ModuleConfigurationRepository


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
async def test_memory_package_qualifies_exact_conversation_batch(clean_authority, tmp_path):
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
                                }
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
                    }
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
