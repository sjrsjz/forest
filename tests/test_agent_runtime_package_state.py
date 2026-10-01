import asyncio
import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import RuntimeModuleRegistry


@dataclass(frozen=True)
class _PayloadRef:
    payload_id: str
    digest: str
    owner_component_id: str
    owner_kind: str
    owner_id: str

    def canonical(self):
        return {
            "payload_id": self.payload_id,
            "digest": self.digest,
            "owner_component_id": self.owner_component_id,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
        }


async def _payload(
    kernel,
    *,
    owner_kind: str,
    owner_id: str,
    value: bytes,
    owner_component_id: str = "agent.runtime",
) -> _PayloadRef:
    saved = await kernel.payload.put_bytes(
        owner_component_id=owner_component_id,
        owner_kind=owner_kind,
        owner_id=owner_id,
        media_type="application/json",
        content=value,
        expected_deletion_epoch=await kernel.payload.epoch(),
    )
    return _PayloadRef(
        saved.payload_id,
        saved.content_digest,
        owner_component_id,
        owner_kind,
        owner_id,
    )


async def _seed_final_ready(store, namespace, run_id):
    """Set only the precondition for the final-session UOW under the current schema guard."""
    async with store.pool.connection() as conn, conn.transaction():
        for expected, target in (
            ("PREPARING_MODEL", "MODEL_PENDING"),
            ("MODEL_PENDING", "PARSING"),
            ("PARSING", "SAFE_BOUNDARY"),
            ("SAFE_BOUNDARY", "FINAL_READY"),
        ):
            row = await (await conn.execute(
                sql.SQL(
                    "UPDATE {}.agent_runs SET state=%s WHERE bot_id=%s AND run_id=%s "
                    "AND state=%s RETURNING run_id"
                ).format(sql.Identifier(namespace)),
                (target, store.bot_id, run_id, expected),
            )).fetchone()
            assert row == {"run_id": run_id}


async def _lease_operation(store, participant, generation_id, name, input_body, operation_id):
    run_id = input_body.get("run_id") or input_body["owner"]["run_id"]
    async with store.pool.connection() as conn, conn.transaction():
        return await participant.invoke_in(
            conn,
            name,
            {
                "protocol": "forest.uow-step/1",
                "caller": {"component_id": "agent.runtime", "generation_id": generation_id},
                "input": input_body,
                "prior": {},
            },
            operation_id=operation_id,
            causal_root_kind="agent.run",
            causal_root_id=run_id,
            parent_call_id="call:" + operation_id,
            timeout_seconds=10,
        )


def _owner_ref(owner):
    return {key: owner[key] for key in ("run_id", "owner_id", "epoch")}


@pytest.mark.postgres
async def test_agent_package_owns_run_snapshot_and_epoch_fenced_lease(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    conversation_package = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/conversation_scheduler"))),
        PackageCatalog(tmp_path / "conversation-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={
            "protocol": "forest.conversation.scheduler/2",
            "context_view": {
                "high_watermark_units": 1024,
                "retain_units": 768,
                "policy_revision": "context-view-unit-window-v1",
            },
        },
        resolved_contracts={},
    )
    memory_package = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/memory_graph"))),
        PackageCatalog(tmp_path / "memory-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={
            "protocol": "forest.memory.graph/2",
            "embedding": {
                "model_kind": "TEXT_IMAGE",
                "deployment_id": "embedding.multimodal/test/default",
                "model_revision": "test/1",
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
        },
        resolved_contracts={},
    )
    preset = await _payload(
        kernel,
        owner_kind="preset.context",
        owner_id="preset/agent-package/1",
        value=b'{"system":["You are the package Agent."]}',
    )
    package = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/agent_runtime"))),
        PackageCatalog(tmp_path / "agent-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={
            "protocol": "forest.agent.runtime/1",
            "agent_spec_revision": "agent-spec/1",
            "preset_revision": "preset/1",
            "preset": {
                "payload_id": preset.payload_id,
                "digest": preset.digest,
                "owner_id": preset.owner_id,
            },
            "run_timeout_seconds": 300,
            "max_steps": 32,
            "output_tokens": 4096,
            "stream": True,
        },
        resolved_contracts={},
    )
    lease = await kernel.runtime.claim_host(
        "agent-package-host",
        "agent-package-incarnation",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    descriptors = (
        *kernel.descriptors,
        conversation_package.descriptor,
        memory_package.descriptor,
        package.descriptor,
    )
    # This fixture omits other official packages; retain only dependencies
    # whose Generations are actually assembled here.
    available = {descriptor.component_id for descriptor in descriptors}
    isolated_descriptors = tuple(
            replace(
                descriptor,
                retention_requires=tuple(
                    component_id for component_id in descriptor.retention_requires
                    if component_id in available
                ),
            ) for descriptor in descriptors
    )
    registry = RuntimeModuleRegistry(host, isolated_descriptors)
    await registry.start()
    try:
        instance = registry.get("agent.runtime")
        generation_id = instance.generation_identity.generation_id
        config = await _payload(
            kernel,
            owner_kind="module.configuration",
            owner_id="configuration/agent-package/1",
            value=b'{"protocol":"forest.agent.runtime/1"}',
        )
        participant = instance.unit_of_work_participant()
        caller = {"component_id": "agent.runtime", "generation_id": generation_id}
        start_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "conversation_ref": "conversation/onebot/group/933179609",
                "session_id": "session/agent-package/1",
                "run_id": "run/agent-package/1",
                "input_batch": {
                    "input_batch_ref": "input-batch/agent-package/1",
                    "input_batch_digest": digest_json({"batch": 1}),
                    "selected_input_end_seq": 1,
                    "context_revision": 1,
                    "context_view_revision": 0,
                    "source_manifest_digest": digest_json({"source": 1}),
                    "trigger_claim": {
                        "ref": "claim:" + "1" * 64, "count": 1,
                        "last_trigger_ref": "trigger/1", "last_trigger_ingress_seq": 1,
                        "digest": digest_json({"claim": 1}),
                    },
                },
                "agent_spec_revision": "agent-spec/1",
                "config": config.canonical(),
                "preset_revision": "preset/1",
                "preset": preset.canonical(),
                "absolute_deadline": (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat(),
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            started = await participant.invoke_in(
                conn,
                "agent.start-run-record",
                start_envelope,
                operation_id="agent-run-start/package/1",
                causal_root_kind="conversation.run",
                causal_root_id="run/agent-package/1",
                parent_call_id="call:agent-package/start",
                timeout_seconds=10,
            )
        assert started["state"] == "PREPARING_MODEL"

        class _RollbackVerification(Exception):
            pass

        async def verify_effect_requeue(conn, run_ref, session_ref, conversation_ref):
            namespace = package.descriptor.storage.spec.namespace
            proof = {
                "protocol": "forest.uow-step/1",
                "caller": {"component_id": "conversation.scheduler"},
                "input": {"protocol": "forest.agent-requeue-verification/1",
                          "prior_step_id": "conversation"},
                "prior": {"conversation": {
                    "protocol": "forest.conversation-run-requeued/1",
                    "conversation_id": conversation_ref,
                    "session_ref": session_ref,
                    "run_ref": run_ref,
                }},
            }
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.agent_verify_requeue(%s::jsonb,%s::jsonb) "
                            "AS result").format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(proof)),
                )
            ).fetchone()
            return row["result"]

        with pytest.raises(_RollbackVerification):
            async with store.pool.connection() as conn, conn.transaction():
                namespace = package.descriptor.storage.spec.namespace
                proof = {
                    "protocol": "forest.uow-step/1",
                    "caller": {"component_id": "conversation.scheduler"},
                    "input": {"protocol": "forest.agent-requeue-verification/1",
                              "prior_step_id": "conversation"},
                    "prior": {"conversation": {
                        "protocol": "forest.conversation-run-requeued/1",
                        "conversation_id": "conversation/onebot/group/933179609",
                        "session_ref": "session/agent-package/1",
                        "run_ref": "run/agent-package/1",
                        "claim_ref": "claim:" + "1" * 64,
                        "trigger_count": 1,
                    }},
                }

                async def verify():
                    row = await (
                        await conn.execute(
                            sql.SQL("SELECT {}.agent_verify_requeue(%s::jsonb,%s::jsonb) "
                                    "AS result").format(sql.Identifier(namespace)),
                            (Jsonb({"bot_id": store.bot_id}), Jsonb(proof)),
                        )
                    ).fetchone()
                    return row["result"]

                assert (await verify())["error"]["code"] == "AGENT_RUN_NOT_ABANDONED"
                await conn.execute(
                    sql.SQL("UPDATE {}.agent_runs SET state='FAILED',"
                            "error_code='TEST_ABANDONED',terminal_at=clock_timestamp() "
                            "WHERE bot_id=%s AND run_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "run/agent-package/1"),
                )
                assert (await verify())["error"]["code"] == "AGENT_RUN_NOT_ABANDONED"
                await conn.execute(
                    sql.SQL("UPDATE {}.agent_sessions SET state='FAILED',"
                            "active_run_id=NULL,terminal_reason_code='TEST_ABANDONED',"
                            "closed_at=clock_timestamp() "
                            "WHERE bot_id=%s AND session_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "session/agent-package/1"),
                )
                verified = await verify()
                assert verified["ok"] is True
                assert verified["value"] == proof["prior"]["conversation"]
                raise _RollbackVerification

        endpoint = instance.runtime_service_handlers()[("agent.run.query", 1)]
        route = FrozenServiceRoute(
            service_id="agent.run.query",
            version=1,
            caller_component_id="kernel.management",
            provider_component_id="agent.runtime",
            provider_generation_id=generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="CONTROL",
            owned_work=False,
        )
        invocation = RuntimeServiceInvocation(
            "agent-run-query/package/1",
            10,
            "test.agent-runtime-package",
            "run/agent-package/1",
        )
        snapshot = await endpoint.invoke_routed(
            route,
            invocation,
            ({"run_id": "run/agent-package/1"},),
            {},
        )
        assert snapshot["conversation_ref"] == "conversation/onebot/group/933179609"
        assert snapshot["profile"]["core_prompt_digest"].startswith("sha256:")
        assert snapshot["inputs"][0]["trigger_count"] == 1
        assert snapshot["inputs"][0]["last_trigger_ref"] == "trigger/1"
        assert snapshot["inputs"][0]["last_trigger_ingress_seq"] == 1
        assert "triggers" not in snapshot

        recovery_declaration = next(
            item
            for item in instance.component["domain_queries"]
            if item["name"] == "agent.run-recovery"
        )
        history_declaration = next(
            item
            for item in instance.component["domain_queries"]
            if item["name"] == "agent.execution-history"
        )
        recovery = await instance.invoke_management_query(
            recovery_declaration,
            {"run_id": "run/agent-package/1"},
            timeout_seconds=10,
        )
        assert recovery == {
            "run_id": "run/agent-package/1",
            "state": "PREPARING_MODEL",
            "next_step_seq": 1,
            "repair_count": 0,
            "cancel_request": None,
            "model_request": None,
            "model_attempt": None,
            "tool_call": None,
            "latest_turn": None,
            "protocol_failure": None,
        }

        model_start = {
            **start_envelope,
            "input": {
                **start_envelope["input"],
                "conversation_ref": "conversation/onebot/group/model-port",
                "session_id": "session/agent-package/model-port",
                "run_id": "run/agent-package/model-port",
                "input_batch": {
                    **start_envelope["input"]["input_batch"],
                    "input_batch_ref": "input-batch/agent-package/model-port",
                    "input_batch_digest": digest_json({"batch": "model-port"}),
                    "source_manifest_digest": digest_json({"source": "model-port"}),
                    "trigger_claim": {
                        "ref": "claim:" + "2" * 64, "count": 1,
                        "last_trigger_ref": "trigger/model-port",
                        "last_trigger_ingress_seq": 1,
                        "digest": digest_json({"claim": "model-port"}),
                    },
                },
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn,
                "agent.start-run-record",
                model_start,
                operation_id="agent-run-start/package/model-port",
                causal_root_kind="conversation.run",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/start",
                timeout_seconds=10,
            )
        model_acquire = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "run_id": "run/agent-package/model-port",
                "owner_id": "agent-worker/model-port",
                "lease_seconds": 120,
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            model_owner = await participant.invoke_in(
                conn,
                "agent.acquire-run-record",
                model_acquire,
                operation_id="agent-run-acquire/package/model-port",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/acquire",
                timeout_seconds=10,
            )
        projection = await _payload(
            kernel,
            owner_component_id="agent.runtime",
            owner_kind="model.projection",
            owner_id="model-request/package/1",
            value=b'{"messages":[{"role":"user","content":"A"}]}',
        )
        request_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": {
                    "run_id": model_owner["run_id"],
                    "owner_id": model_owner["owner_id"],
                    "epoch": model_owner["epoch"],
                },
                "request": {
                    "request_id": "model-request/package/1",
                    "request_kind": "PRIMARY",
                    "provider_component_id": "provider.chat",
                    "provider_generation_id": "generation/provider/package-test",
                    "provider_profile_ref": "profile/provider/package-test",
                    "model_ref": "model/package-test",
                    "instruction_channel": "SYSTEM",
                    "capability_digest": digest_json({"capability": "text"}),
                    "projection": projection.canonical(),
                    "projected_content_digest": digest_json({"content": "A"}),
                    "source_manifest_digest": digest_json({"source": "context"}),
                    "learning_candidates": [],
                    "output_token_budget": 256,
                },
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            prepared_request = await participant.invoke_in(
                conn,
                "agent.prepare-model-request-record",
                request_envelope,
                operation_id="agent-model-request/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/request",
                timeout_seconds=10,
            )
        assert prepared_request["state"] == "READY"
        wire = await _payload(
            kernel,
            owner_component_id="kernel.effect",
            owner_kind="effect.wire",
            owner_id="model-attempt/package/1",
            value=b'{"model":"model/package-test","messages":[]}',
        )
        effect_input = await _payload(
            kernel,
            owner_component_id="kernel.effect",
            owner_kind="effect.input",
            owner_id="model-effect/package/1",
            value=b'{"request_id":"model-request/package/1"}',
        )
        attempt_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request_id": "model-request/package/1",
                "attempt_id": "model-attempt/package/1",
                "effect_operation_ref": "model-effect/package/1",
                "provider_service_id": "provider.model.dispatch",
                "effect_step_id": "effect",
            },
            "prior": {
                "authorization": {
                    "protocol": "forest.authorization-issued/1",
                    "ticket": {},
                },
                "effect": {
                    "protocol": "forest.effect-command-permit/1",
                    "authorization_ticket_id": "ticket/model/package/1",
                    "input_payload": effect_input.canonical(),
                    "wire_payload": wire.canonical(),
                    "permit": {
                        "protocol": "forest.effect-permit/1",
                        "effect_operation_id": "model-effect/package/1",
                        "attempt_id": "model-attempt/package/1",
                        "attempt_no": 1,
                        "wire_payload_id": wire.payload_id,
                        "wire_payload_digest": wire.digest,
                        "executor_component_id": "provider.chat",
                        "executor_generation_id": "generation/provider/package-test",
                        "fence_kind": "runtime.graph",
                        "fence_id": "graph/package-test",
                        "fence_epoch": 1,
                        "deadline_at": "2030-01-01T00:00:00+00:00",
                    },
                },
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            prepared_attempt = await participant.invoke_in(
                conn,
                "agent.prepare-model-attempt-record",
                attempt_envelope,
                operation_id="agent-model-attempt/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/attempt",
                timeout_seconds=10,
            )
        assert prepared_attempt["state"] == "PREPARED"
        assert prepared_attempt["protocol"] == "forest.agent-model-effect-prepared/1"
        assert prepared_attempt["authorization_ticket_id"] == "ticket/model/package/1"
        assert prepared_attempt["input_payload"] == effect_input.canonical()
        assert prepared_attempt["wire_payload"] == wire.canonical()
        assert prepared_attempt["permit"]["attempt_id"] == "model-attempt/package/1"
        handoff_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request_id": "model-request/package/1",
                "attempt_id": "model-attempt/package/1",
            },
            "prior": {
                "effect": {
                    "effect_operation_id": "model-effect/package/1",
                    "attempt_id": "model-attempt/package/1",
                    "state": "DISPATCHING",
                    "wire_payload_id": wire.payload_id,
                    "wire_payload_digest": wire.digest,
                    "executor_component_id": "provider.chat",
                    "executor_generation_id": "generation/provider/package-test",
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            handed_off = await participant.invoke_in(
                conn,
                "agent.mark-model-handoff-record",
                handoff_envelope,
                operation_id="agent-model-handoff/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/handoff",
                timeout_seconds=10,
            )
        assert handed_off["state"] == "DISPATCHING"
        model_recovery = await instance.invoke_management_query(
            recovery_declaration,
            {"run_id": "run/agent-package/model-port"},
            timeout_seconds=10,
        )
        assert model_recovery["state"] == "MODEL_PENDING"
        assert model_recovery["model_request"]["request_id"] == "model-request/package/1"
        assert model_recovery["model_attempt"] == {
            "attempt_id": "model-attempt/package/1",
            "effect_operation_ref": "model-effect/package/1",
            "result_payload_id": None,
            "result_payload_digest": None,
            "state": "DISPATCHING",
        }
        with pytest.raises(_RollbackVerification):
            async with store.pool.connection() as conn, conn.transaction():
                namespace = package.descriptor.storage.spec.namespace
                await conn.execute(
                    sql.SQL("UPDATE {}.agent_runs SET state='FAILED',"
                            "error_code='TEST_ABANDONED',terminal_at=clock_timestamp() "
                            "WHERE bot_id=%s AND run_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "run/agent-package/model-port"),
                )
                await conn.execute(
                    sql.SQL("UPDATE {}.agent_sessions SET state='FAILED',"
                            "active_run_id=NULL,terminal_reason_code='TEST_ABANDONED',"
                            "closed_at=clock_timestamp() "
                            "WHERE bot_id=%s AND session_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "session/agent-package/model-port"),
                )
                rejected = await verify_effect_requeue(
                    conn, "run/agent-package/model-port",
                    "session/agent-package/model-port",
                    "conversation/onebot/group/model-port",
                )
                assert rejected["error"]["code"] == "AGENT_REQUEUE_EFFECT_UNSAFE"
                raise _RollbackVerification
        result_payload = await _payload(
            kernel,
            owner_component_id="provider.chat",
            owner_kind="model.result",
            owner_id="model-attempt/package/1",
            value=b'{"status":"RESPONDED","text":"<broken>"}',
        )
        receipt = {
            "status": "RESPONDED",
            "request_id": "model-request/package/1",
            "transport_attempt_id": "model-attempt/package/1",
            "payload_digest": digest_json({"wire": "package/1"}),
            "result": result_payload.canonical(),
            "usage": None,
            "error_code": None,
        }
        evidence = await _payload(
            kernel,
            owner_component_id="kernel.effect",
            owner_kind="effect.evidence",
            owner_id="model-observation/package/1",
            value=json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode(),
        )
        observation_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "observation_operation_id": "model-observation/package/1",
                "request_id": "model-request/package/1",
                "attempt_id": "model-attempt/package/1",
                "observed_state": "SUCCEEDED",
                "evidence_kind": "EXECUTOR",
                "effect_observation_ref": "model-effect-observation/package/1",
                "effect_step_id": "effect",
                "evidence_digest": evidence.digest,
                "payloads": [
                    {"role": "RESULT", "payload": result_payload.canonical()}
                ],
            },
            "prior": {
                "effect": {
                    "protocol": "forest.effect-command-observation/1",
                    "evidence_payload": evidence.canonical(),
                    "evidence_parents": [result_payload.canonical()],
                    "observation": {
                        "attempt_id": "model-attempt/package/1",
                        "observation_operation_id": "model-observation/package/1",
                        "observed_status": "SUCCEEDED",
                        "evidence_kind": "executor",
                        "accepted": True,
                        "resulting_state": "SUCCEEDED",
                    },
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            observation = await participant.invoke_in(
                conn,
                "agent.observe-model-result-record",
                observation_envelope,
                operation_id="agent-model-observation/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/observation",
                timeout_seconds=10,
            )
        assert observation == {
            "observation_operation_id": "model-observation/package/1",
            "attempt_id": "model-attempt/package/1",
            "evidence_kind": "EXECUTOR",
            "observed_state": "SUCCEEDED",
            "accepted": True,
            "previous_state": "DISPATCHING",
            "resulting_state": "SUCCEEDED",
            "conflict_code": None,
        }
        async with store.pool.connection() as conn, conn.transaction():
            replayed_request = await participant.invoke_in(
                conn,
                "agent.prepare-model-request-record",
                request_envelope,
                operation_id="agent-model-request/package/replay",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/request-replay",
                timeout_seconds=10,
            )
        assert replayed_request["state"] == "SUCCEEDED"
        observed_recovery = await instance.invoke_management_query(
            recovery_declaration,
            {"run_id": "run/agent-package/model-port"},
            timeout_seconds=10,
        )
        assert observed_recovery["state"] == "PARSING"
        assert observed_recovery["model_request"]["state"] == "SUCCEEDED"
        assert observed_recovery["model_attempt"]["state"] == "SUCCEEDED"
        feedback = await _payload(
            kernel,
            owner_kind="protocol.repair.feedback",
            owner_id="protocol-failure/package/1",
            value=b'{"repair_count":1,"error_code":"AGENT_PROTOCOL_ROOT"}',
        )
        failure_checkpoint = await _payload(
            kernel,
            owner_kind="checkpoint.state",
            owner_id="checkpoint/protocol-failure/package/1",
            value=b'{"phase":"REPAIR_READY"}',
        )
        candidate_digest = digest_json({"candidate": "<broken>"})
        failure_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "failure_id": "protocol-failure/package/1",
                "request_id": "model-request/package/1",
                "attempt_id": "model-attempt/package/1",
                "error_code": "AGENT_PROTOCOL_ROOT",
                "candidate_digest": candidate_digest,
                "feedback": feedback.canonical(),
                "checkpoint_id": "checkpoint/protocol-failure/package/1",
                "checkpoint_state": failure_checkpoint.canonical(),
                "allow_repair": True,
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            failure = await participant.invoke_in(
                conn,
                "agent.record-protocol-failure-record",
                failure_envelope,
                operation_id="agent-protocol-failure/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/protocol-failure",
                timeout_seconds=10,
            )
        assert failure == {
            "failure_id": "protocol-failure/package/1",
            "failed_request_id": "model-request/package/1",
            "error_code": "AGENT_PROTOCOL_ROOT",
            "candidate_digest": candidate_digest,
            "resulting_state": "REPAIR_READY",
            "repair_step_seq": 2,
        }
        failed_recovery = await instance.invoke_management_query(
            recovery_declaration,
            {"run_id": "run/agent-package/model-port"},
            timeout_seconds=10,
        )
        assert failed_recovery["state"] == "REPAIR_READY"
        assert failed_recovery["repair_count"] == 1
        assert failed_recovery["protocol_failure"]["failed_request_id"] == (
            "model-request/package/1"
        )
        repair_projection = await _payload(
            kernel,
            owner_component_id="agent.runtime",
            owner_kind="model.projection",
            owner_id="model-request/package/repair",
            value=b'{"messages":[{"role":"user","content":"repair"}]}',
        )
        repair_request_envelope = {
            **request_envelope,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request": {
                    **request_envelope["input"]["request"],
                    "request_id": "model-request/package/repair",
                    "request_kind": "REPAIR",
                    "projection": repair_projection.canonical(),
                    "projected_content_digest": digest_json({"content": "repair"}),
                },
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            repair_request = await participant.invoke_in(
                conn,
                "agent.prepare-model-request-record",
                repair_request_envelope,
                operation_id="agent-model-request/package/repair",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/repair-request",
                timeout_seconds=10,
            )
        assert repair_request["step_seq"] == 2
        repair_wire = await _payload(
            kernel,
            owner_component_id="kernel.effect",
            owner_kind="effect.wire",
            owner_id="model-attempt/package/repair",
            value=b'{"model":"model/package-test","repair":true}',
        )
        repair_effect_input = await _payload(
            kernel,
            owner_component_id="kernel.effect",
            owner_kind="effect.input",
            owner_id="model-effect/package/repair",
            value=b'{"request_id":"model-request/package/repair"}',
        )
        repair_attempt_envelope = {
            **attempt_envelope,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request_id": "model-request/package/repair",
                "attempt_id": "model-attempt/package/repair",
                "effect_operation_ref": "model-effect/package/repair",
                "provider_service_id": "provider.model.dispatch",
                "effect_step_id": "effect",
            },
            "prior": {
                "authorization": {
                    "protocol": "forest.authorization-issued/1",
                    "ticket": {},
                },
                "effect": {
                    "protocol": "forest.effect-command-permit/1",
                    "authorization_ticket_id": "ticket/model/package/repair",
                    "input_payload": repair_effect_input.canonical(),
                    "wire_payload": repair_wire.canonical(),
                    "permit": {
                        "protocol": "forest.effect-permit/1",
                        "effect_operation_id": "model-effect/package/repair",
                        "attempt_id": "model-attempt/package/repair",
                        "attempt_no": 1,
                        "wire_payload_id": repair_wire.payload_id,
                        "wire_payload_digest": repair_wire.digest,
                        "executor_component_id": "provider.chat",
                        "executor_generation_id": "generation/provider/package-test",
                        "fence_kind": "runtime.graph",
                        "fence_id": "graph/package-test",
                        "fence_epoch": 1,
                        "deadline_at": "2030-01-01T00:00:00+00:00",
                    },
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn,
                "agent.prepare-model-attempt-record",
                repair_attempt_envelope,
                operation_id="agent-model-attempt/package/repair",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/repair-attempt",
                timeout_seconds=10,
            )
        repair_handoff = {
            **handoff_envelope,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request_id": "model-request/package/repair",
                "attempt_id": "model-attempt/package/repair",
            },
            "prior": {
                "effect": {
                    "effect_operation_id": "model-effect/package/repair",
                    "attempt_id": "model-attempt/package/repair",
                    "state": "DISPATCHING",
                    "wire_payload_id": repair_wire.payload_id,
                    "wire_payload_digest": repair_wire.digest,
                    "executor_component_id": "provider.chat",
                    "executor_generation_id": "generation/provider/package-test",
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn,
                "agent.mark-model-handoff-record",
                repair_handoff,
                operation_id="agent-model-handoff/package/repair",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/repair-handoff",
                timeout_seconds=10,
            )
        repair_result = await _payload(
            kernel,
            owner_component_id="provider.chat",
            owner_kind="model.result",
            owner_id="model-attempt/package/repair",
            value=b'{"status":"RESPONDED","text":"valid"}',
        )
        repair_receipt = {
            "status": "RESPONDED",
            "request_id": "model-request/package/repair",
            "transport_attempt_id": "model-attempt/package/repair",
            "payload_digest": digest_json({"wire": "package/repair"}),
            "result": repair_result.canonical(),
            "usage": None,
            "error_code": None,
        }
        repair_evidence = await _payload(
            kernel,
            owner_component_id="kernel.effect",
            owner_kind="effect.evidence",
            owner_id="model-observation/package/repair",
            value=json.dumps(repair_receipt, sort_keys=True, separators=(",", ":")).encode(),
        )
        repair_observation = {
            **observation_envelope,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "observation_operation_id": "model-observation/package/repair",
                "request_id": "model-request/package/repair",
                "attempt_id": "model-attempt/package/repair",
                "observed_state": "SUCCEEDED",
                "evidence_kind": "EXECUTOR",
                "effect_observation_ref": "model-effect-observation/package/repair",
                "effect_step_id": "effect",
                "evidence_digest": repair_evidence.digest,
                "payloads": [{"role": "RESULT", "payload": repair_result.canonical()}],
            },
            "prior": {
                "effect": {
                    "protocol": "forest.effect-command-observation/1",
                    "evidence_payload": repair_evidence.canonical(),
                    "evidence_parents": [repair_result.canonical()],
                    "observation": {
                        "attempt_id": "model-attempt/package/repair",
                        "observation_operation_id": "model-observation/package/repair",
                        "observed_status": "SUCCEEDED",
                        "evidence_kind": "executor",
                        "accepted": True,
                        "resulting_state": "SUCCEEDED",
                    },
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn,
                "agent.observe-model-result-record",
                repair_observation,
                operation_id="agent-model-observation/package/repair",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/repair-observation",
                timeout_seconds=10,
            )
        turn_payload = await _payload(
            kernel,
            owner_kind="model.turn",
            owner_id="model-turn/package/repair",
            value=b'{"protocol_version":1,"blocks":[{"block_type":"FunctionCall","name":"lookup"}]}',
        )
        tool_arguments = await _payload(
            kernel,
            owner_kind="tool.arguments",
            owner_id="tool-call/package/repair",
            value=b'{"authority_trigger_id":"trigger/model-port","arguments":{"q":"A"}}',
        )
        function_block = {
            "block_type": "FunctionCall",
            "name": "lookup",
            "arguments": {
                "authority_trigger_id": "trigger/model-port",
                "arguments": {"q": "A"},
            },
        }
        turn_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "turn_id": "model-turn/package/repair",
                "request_id": "model-request/package/repair",
                "turn": {
                    "protocol_version": 1,
                    "blocks": [function_block],
                    "raw_candidate_digest": digest_json({"candidate": "valid"}),
                    "truncated_after_function_call": False,
                },
                "artifacts": [
                    {"role": "TURN", "payload": turn_payload.canonical()},
                    {"role": "TOOL_ARGUMENTS", "payload": tool_arguments.canonical()},
                ],
                "tool": {
                    "tool_call_id": "tool-call/package/repair",
                    "component_id": "tool.echo",
                    "generation_id": "generation/tool/package-test",
                    "service_id": "tool.echo.execute",
                    "tool_contract_digest": digest_json({"contract": "tool.echo/1"}),
                    "tool_binding_digest": digest_json({"binding": "tool.echo/1"}),
                    "approval_required": True,
                    "supports_query": True,
                    "authority_trigger_ref": "trigger/model-port",
                    "effect_operation_ref": "tool-effect/package/repair",
                },
                "repair_of_request_id": "model-request/package/1",
            },
            "prior": {
                "authority": {
                    "protocol": "forest.conversation-run-trigger-authority/1",
                    "run_ref": "run/agent-package/model-port",
                    "trigger_ref": "trigger/model-port",
                    "session_ref": "session/agent-package/model-port",
                    "conversation_ref": "conversation/onebot/group/model-port",
                    "principal_ref": "qq:43",
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            repaired_turn = await participant.invoke_in(
                conn,
                "agent.record-model-turn-record",
                turn_envelope,
                operation_id="agent-model-turn/package/repair",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/repair-turn",
                timeout_seconds=10,
            )
        assert repaired_turn == {
            "turn_id": "model-turn/package/repair",
            "request_id": "model-request/package/repair",
            "step_seq": 2,
            "terminal": False,
            "thinking_only": False,
            "truncated_after_function_call": False,
            "next_state": "WAIT_TOOL",
            "tool_call_id": "tool-call/package/repair",
        }
        repaired_recovery = await instance.invoke_management_query(
            recovery_declaration,
            {"run_id": "run/agent-package/model-port"},
            timeout_seconds=10,
        )
        assert repaired_recovery["state"] == "WAIT_TOOL"
        assert repaired_recovery["latest_turn"]["turn_id"] == "model-turn/package/repair"
        assert repaired_recovery["tool_call"]["tool_generation_id"] == (
            "generation/tool/package-test"
        )
        assert repaired_recovery["tool_call"]["arguments_payload_digest"] == (
            tool_arguments.digest
        )
        tool_command = {
            "protocol": "forest.agent-tool-execute/1",
            "tool_call_id": "tool-call/package/repair",
            "effect_operation_id": "tool-effect/package/repair",
            "effect_attempt_id": "tool-attempt/package/repair",
            "component_id": "tool.echo",
            "generation_id": "generation/tool/package-test",
            "service_id": "tool.echo.execute",
            "contract_digest": digest_json({"contract": "tool.echo/1"}),
            "binding_digest": digest_json({"binding": "tool.echo/1"}),
            "authority_trigger_ref": "trigger/model-port",
            "authority_principal_ref": "qq:43",
            "arguments": {"q": "A"},
        }
        tool_effect_input = await _payload(
            kernel, owner_component_id="kernel.effect", owner_kind="effect.input",
            owner_id="tool-effect/package/repair",
            value=json.dumps(tool_command, sort_keys=True, separators=(",", ":")).encode(),
        )
        tool_wire = await _payload(
            kernel, owner_component_id="kernel.effect", owner_kind="effect.wire",
            owner_id="tool-attempt/package/repair",
            value=json.dumps(tool_command, sort_keys=True, separators=(",", ":")).encode(),
        )
        tool_permit = {
            "protocol": "forest.effect-permit/1",
            "effect_operation_id": "tool-effect/package/repair",
            "attempt_id": "tool-attempt/package/repair",
            "attempt_no": 1,
            "wire_payload_id": tool_wire.payload_id,
            "wire_payload_digest": tool_wire.digest,
            "executor_component_id": "tool.echo",
            "executor_generation_id": "generation/tool/package-test",
            "fence_kind": "runtime.graph",
            "fence_id": "graph/package-test",
            "fence_epoch": 1,
            "deadline_at": "2030-01-01T00:00:00+00:00",
        }
        approval_state = {
            "protocol": "forest.approval-state/1",
            "approval_id": "approval/tool/package/repair",
            "request_kind": "agent.tool",
            "request_id": "tool-call/package/repair",
            "action": "tool.invoke",
            "input_digest": tool_wire.digest,
            "plan_digest": digest_json({"binding": "tool.echo/1"}),
            "state": "PENDING",
            "expires_at": "2030-01-01T00:00:00+00:00",
        }
        approval_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "tool_call_id": "tool-call/package/repair",
                "command_digest": tool_wire.digest,
                "observation_operation_id": "tool-approval-observation/package/request",
                "approval_step_id": "approval",
            },
            "prior": {"approval": approval_state},
        }
        async with store.pool.connection() as conn, conn.transaction():
            requested_approval = await participant.invoke_in(
                conn,
                "agent.record-tool-approval-request",
                approval_envelope,
                operation_id="agent-tool-approval/package/request",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/tool-approval-request",
                timeout_seconds=10,
            )
        assert requested_approval["resulting_state"] == "WAIT_APPROVAL"
        approval_envelope["prior"]["approval"] = {
            **approval_state,
            "state": "APPROVED",
        }
        approval_envelope["input"]["observation_operation_id"] = (
            "tool-approval-observation/package/inspect"
        )
        async with store.pool.connection() as conn, conn.transaction():
            approved_tool = await participant.invoke_in(
                conn,
                "agent.resolve-tool-approval",
                approval_envelope,
                operation_id="agent-tool-approval/package/inspect",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/tool-approval-inspect",
                timeout_seconds=10,
            )
        assert approved_tool["approval_state"] == "APPROVED"
        prepare_tool_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "tool_call_id": "tool-call/package/repair",
                "effect_operation_ref": "tool-effect/package/repair",
                "effect_attempt_id": "tool-attempt/package/repair",
                "command_digest": tool_wire.digest,
                "effect_step_id": "effect",
                "approval_step_id": "approval",
            },
            "prior": {
                "approval": {**approval_state, "state": "CONSUMED"},
                "effect": {
                    "protocol": "forest.effect-command-permit/1",
                    "authorization_ticket_id": "ticket/tool/package/repair",
                    "input_payload": tool_effect_input.canonical(),
                    "wire_payload": tool_wire.canonical(),
                    "permit": tool_permit,
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            prepared_tool = await participant.invoke_in(
                conn, "agent.prepare-tool-effect-record", prepare_tool_envelope,
                operation_id="agent-tool-prepare/package/repair",
                causal_root_kind="agent.execute", causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/tool-prepare", timeout_seconds=10,
            )
        assert prepared_tool["permit"] == tool_permit
        prepared_recovery = await instance.invoke_management_query(
            recovery_declaration,
            {"run_id": "run/agent-package/model-port"},
            timeout_seconds=10,
        )
        assert prepared_recovery["tool_call"]["effect_attempt_id"] == (
            "tool-attempt/package/repair"
        )
        handoff_tool_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "observation_operation_id": "tool-observation/package/dispatch",
                "tool_call_id": "tool-call/package/repair",
                "effect_attempt_id": "tool-attempt/package/repair",
                "effect_step_id": "effect",
            },
            "prior": {"effect": {**tool_permit, "state": "DISPATCHING"}},
        }
        async with store.pool.connection() as conn, conn.transaction():
            handed_off_tool = await participant.invoke_in(
                conn, "agent.mark-tool-handoff-record", handoff_tool_envelope,
                operation_id="agent-tool-handoff/package/dispatch",
                causal_root_kind="agent.execute", causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/tool-dispatch", timeout_seconds=10,
            )
        assert handed_off_tool["resulting_state"] == "DISPATCHING"
        tool_result = await _payload(
            kernel, owner_component_id="tool.echo", owner_kind="tool.result",
            owner_id="tool-call/package/repair", value=b'{"value":"A"}',
        )
        tool_receipt = {
            "protocol": "forest.agent-tool-receipt/1",
            "status": "SUCCEEDED",
            "tool_call_id": "tool-call/package/repair",
            "effect_operation_id": "tool-effect/package/repair",
            "effect_attempt_id": "tool-attempt/package/repair",
            "result": tool_result.canonical(),
            "error_code": None,
            "observation_ref": "tool-result/package/repair",
        }
        tool_evidence = await _payload(
            kernel, owner_component_id="kernel.effect", owner_kind="effect.evidence",
            owner_id="tool-observation/package/success",
            value=json.dumps(tool_receipt, sort_keys=True, separators=(",", ":")).encode(),
        )
        settle_tool_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "observation_operation_id": "tool-observation/package/success",
                "tool_call_id": "tool-call/package/repair",
                "effect_attempt_id": "tool-attempt/package/repair",
                "observed_state": "SUCCEEDED", "evidence_kind": "EXECUTOR",
                "effect_observation_ref": "tool-result/package/repair",
                "approval_ref": None,
                "error_code": None, "results": [tool_result.canonical()],
                "effect_step_id": "effect",
                "evidence_digest": tool_evidence.digest,
            },
            "prior": {
                "effect": {
                    "protocol": "forest.effect-command-observation/1",
                    "evidence_payload": tool_evidence.canonical(),
                    "evidence_parents": [tool_result.canonical()],
                    "observation": {
                        "attempt_id": "tool-attempt/package/repair",
                        "observation_operation_id": "tool-observation/package/success",
                        "observed_status": "SUCCEEDED",
                        "evidence_kind": "executor",
                        "accepted": True,
                        "resulting_state": "SUCCEEDED",
                    },
                }
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            settled_tool = await participant.invoke_in(
                conn, "agent.observe-tool-effect-record", settle_tool_envelope,
                operation_id="agent-tool-observation/package/success",
                causal_root_kind="agent.execute", causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/tool-success", timeout_seconds=10,
            )
        assert settled_tool["resulting_state"] == "SUCCEEDED"
        tool_recovery = await instance.invoke_management_query(
            recovery_declaration, {"run_id": "run/agent-package/model-port"}, timeout_seconds=10
        )
        assert tool_recovery["state"] == "PREPARING_MODEL"
        assert tool_recovery["tool_call"] is None
        with pytest.raises(_RollbackVerification):
            async with store.pool.connection() as conn, conn.transaction():
                namespace = package.descriptor.storage.spec.namespace
                await conn.execute(
                    sql.SQL("UPDATE {}.agent_runs SET state='FAILED',"
                            "error_code='TEST_ABANDONED',terminal_at=clock_timestamp() "
                            "WHERE bot_id=%s AND run_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "run/agent-package/model-port"),
                )
                await conn.execute(
                    sql.SQL("UPDATE {}.agent_sessions SET state='FAILED',"
                            "active_run_id=NULL,terminal_reason_code='TEST_ABANDONED',"
                            "closed_at=clock_timestamp() "
                            "WHERE bot_id=%s AND session_id=%s")
                    .format(sql.Identifier(namespace)),
                    (store.bot_id, "session/agent-package/model-port"),
                )
                rejected = await verify_effect_requeue(
                    conn, "run/agent-package/model-port",
                    "session/agent-package/model-port",
                    "conversation/onebot/group/model-port",
                )
                assert rejected["error"]["code"] == "AGENT_REQUEUE_EFFECT_UNSAFE"
                raise _RollbackVerification
        execution_history = await instance.invoke_management_query(
            history_declaration,
            {"run_id": "run/agent-package/model-port"},
            timeout_seconds=10,
        )
        assert execution_history["protocol"] == "forest.agent-execution-history/1"
        assert execution_history["run_id"] == "run/agent-package/model-port"
        assert len(execution_history["turns"]) == 1
        history_turn = execution_history["turns"][0]
        assert history_turn["turn_id"] == "model-turn/package/repair"
        assert history_turn["input_revision"] == 1
        assert history_turn["model_attempt_id"] == "model-attempt/package/repair"
        assert history_turn["model_result_payload_id"] == repair_result.payload_id
        assert history_turn["function_call_payload_id"] == tool_arguments.payload_id
        assert history_turn["tool_component_id"] == "tool.echo"
        assert history_turn["tool_state"] == "SUCCEEDED"
        assert history_turn["result_payload_id"] == tool_result.payload_id

        acquire_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "run_id": "run/agent-package/1",
                "owner_id": "agent-worker/1",
                "lease_seconds": 120,
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            owner = await participant.invoke_in(
                conn,
                "agent.acquire-run-record",
                acquire_envelope,
                operation_id="agent-run-acquire/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/1",
                parent_call_id="call:agent-package/acquire",
                timeout_seconds=10,
            )
        assert owner["epoch"] == 1

        async with store.pool.connection() as conn, conn.transaction():
            with pytest.raises(ForestError) as busy:
                await participant.invoke_in(
                    conn,
                    "agent.acquire-run-record",
                    {
                        **acquire_envelope,
                        "input": {**acquire_envelope["input"], "owner_id": "agent-worker/2"},
                    },
                    operation_id="agent-run-acquire/package/2",
                    causal_root_kind="agent.execute",
                    causal_root_id="run/agent-package/1",
                    parent_call_id="call:agent-package/busy",
                    timeout_seconds=10,
                )
        assert busy.value.code == "AGENT_RUN_BUSY"

        async with store.pool.connection() as conn, conn.transaction():
            renewed = await participant.invoke_in(
                conn,
                "agent.renew-run-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": caller,
                    "input": {
                        "owner": {
                            "run_id": owner["run_id"],
                            "owner_id": owner["owner_id"],
                            "epoch": owner["epoch"],
                        },
                        "lease_seconds": 180,
                    },
                    "prior": {},
                },
                operation_id="agent-run-renew/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/1",
                parent_call_id="call:agent-package/renew",
                timeout_seconds=10,
            )
        assert renewed["epoch"] == owner["epoch"]

        conversation_namespace = conversation_package.descriptor.storage.spec.namespace
        digest = "sha256:" + "7" * 64
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_lanes("
                    "bot_id,conversation_id,conversation_key_digest,source_component_id,"
                    "binding_ref,platform,channel_kind,channel_ref,next_ingress_seq,"
                    "classified_through_seq,context_applied_seq,context_revision,"
                    "context_view_revision,context_view_unit_count,actor_owner_id,actor_epoch,"
                    "actor_expires_at,active_session_ref,active_run_ref) VALUES ("
                    "%s,%s,%s,'adapter.onebot','binding/main','onebot-v11','GROUP',"
                    "'933179609',4,3,1,1,1,1,%s,1,clock_timestamp()+interval '5 minutes',%s,%s)"
                ).format(sql.Identifier(conversation_namespace)),
                (
                    store.bot_id,
                    "conversation/onebot/group/933179609",
                    digest_json({"conversation": "933179609"}),
                    "conversation-owner/package/1",
                    "session/agent-package/1",
                    "run/agent-package/1",
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_lanes("
                    "bot_id,conversation_id,conversation_key_digest,source_component_id,"
                    "binding_ref,platform,channel_kind,channel_ref,next_ingress_seq,"
                    "classified_through_seq,context_applied_seq,context_revision,"
                    "context_view_revision,context_view_unit_count,actor_owner_id,actor_epoch,"
                    "actor_expires_at) VALUES ("
                    "%s,%s,%s,'adapter.onebot','binding/main','onebot-v11','GROUP',"
                    "'start-atomic',2,1,0,0,0,0,%s,1,clock_timestamp()+interval '5 minutes')"
                ).format(sql.Identifier(conversation_namespace)),
                (
                    store.bot_id,
                    "conversation/onebot/group/start-atomic",
                    digest_json({"conversation": "start-atomic"}),
                    "conversation-owner/start-atomic",
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.inbox_messages("
                    "bot_id,conversation_id,event_id,ingress_seq,admission_operation_id,"
                    "input_digest,source_component_id,source_event_ref,event_kind,"
                    "platform_message_ref,author_subject_ref,content_payload_id,"
                    "content_payload_digest,content_owner_component_id,content_owner_kind,"
                    "content_owner_id,author_payload_id,author_payload_digest,"
                    "author_owner_component_id,author_owner_kind,author_owner_id,"
                    "context_disposition,classification_state,trigger_decision,"
                    "classification_operation_id,classification_input_digest,"
                    "classifier_component_id,classifier_generation_id,rule_revision,"
                    "reason_code,received_at,classification_deadline,classified_at) VALUES ("
                    "%s,%s,'event/start-atomic',1,'admit/start-atomic',%s,'adapter.onebot',"
                    "'source/start-atomic','message.group','message/start-atomic','qq:99',"
                    "'payload/content/start-atomic',%s,'adapter.onebot','onebot.normalized-event',"
                    "'event/start-atomic','payload/author/start-atomic',%s,'adapter.onebot',"
                    "'onebot.author-snapshot','event/start-atomic','MATERIALIZE','FINAL','TRIGGER',"
                    "'classify/start-atomic',%s,'conversation.trigger-policy',"
                    "'generation/trigger/test','rule/1','test',clock_timestamp(),"
                    "clock_timestamp()+interval '1 minute',clock_timestamp())"
                ).format(sql.Identifier(conversation_namespace)),
                (
                    store.bot_id,
                    "conversation/onebot/group/start-atomic",
                    digest,
                    digest,
                    digest,
                    digest,
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_triggers("
                    "bot_id,conversation_id,trigger_id,event_id,ingress_seq,principal_ref,"
                    "input_digest) VALUES (%s,%s,'trigger/start-atomic',"
                    "'event/start-atomic',1,'qq:99',%s)"
                ).format(sql.Identifier(conversation_namespace)),
                (store.bot_id, "conversation/onebot/group/start-atomic", digest),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index,kind,"
                    "source_component_id,source_kind,source_id,payload_id,payload_digest,"
                    "payload_owner_component_id,payload_owner_kind,payload_owner_id) VALUES ("
                    "%s,%s,'context/initial',1,0,'AGENT_STEP','agent.runtime','model.turn',"
                    "'turn/initial','payload/context/initial',%s,'agent.runtime',"
                    "'model.turn','turn/initial')"
                ).format(sql.Identifier(conversation_namespace)),
                (store.bot_id, "conversation/onebot/group/933179609", digest),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.active_context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index) "
                    "VALUES (%s,%s,'context/initial',1,0)"
                ).format(sql.Identifier(conversation_namespace)),
                (store.bot_id, "conversation/onebot/group/933179609"),
            )
            for ingress_seq, event_id, decision in (
                (2, "event/package/2", "NO_TRIGGER"),
                (3, "event/package/3", "TRIGGER"),
            ):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.inbox_messages("
                        "bot_id,conversation_id,event_id,ingress_seq,admission_operation_id,"
                        "input_digest,source_component_id,source_event_ref,event_kind,"
                        "platform_message_ref,author_subject_ref,content_payload_id,"
                        "content_payload_digest,content_owner_component_id,content_owner_kind,"
                        "content_owner_id,author_payload_id,author_payload_digest,"
                        "author_owner_component_id,author_owner_kind,author_owner_id,"
                        "context_disposition,classification_state,trigger_decision,"
                        "classification_operation_id,classification_input_digest,"
                        "classifier_component_id,classifier_generation_id,rule_revision,"
                        "reason_code,received_at,classification_deadline,classified_at) VALUES ("
                        "%s,%s,%s,%s,%s,%s,'adapter.onebot',%s,'message.group',%s,'qq:43',"
                        "%s,%s,'adapter.onebot','onebot.normalized-event',%s,%s,%s,"
                        "'adapter.onebot','onebot.author-snapshot',%s,'MATERIALIZE','FINAL',%s,"
                        "%s,%s,'conversation.trigger-policy','generation/trigger/test',"
                        "'rule/1','test',clock_timestamp(),clock_timestamp()+interval '1 minute',"
                        "clock_timestamp())"
                    ).format(sql.Identifier(conversation_namespace)),
                    (
                        store.bot_id,
                        "conversation/onebot/group/933179609",
                        event_id,
                        ingress_seq,
                        f"admit/{event_id}",
                        digest,
                        f"source/{event_id}",
                        f"message/{ingress_seq}",
                        f"payload/content/{event_id}",
                        digest,
                        event_id,
                        f"payload/author/{event_id}",
                        digest,
                        event_id,
                        decision,
                        f"classify/{event_id}",
                        digest,
                    ),
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.conversation_triggers("
                    "bot_id,conversation_id,trigger_id,event_id,ingress_seq,principal_ref,"
                    "input_digest) VALUES (%s,%s,'trigger/2','event/package/3',3,'qq:43',%s)"
                ).format(sql.Identifier(conversation_namespace)),
                (store.bot_id, "conversation/onebot/group/933179609", digest),
            )

        acquisition_declaration = next(
            item
            for item in instance.component["unit_of_work_plans"]
            if item["name"] == "agent.acquire-model-input"
        )
        binding = instance._prepared_binding
        acquisition = await instance._unit_of_work_coordinator.execute(
            PackageCallScope(
                bot_id=store.bot_id,
                component_id="agent.runtime",
                generation_id=generation_id,
                instance_id=binding.instance_id,
                worker_incarnation=binding.worker_incarnation,
                call_id="call:agent-package/acquire-model-input",
                call_operation_id="agent-acquire-model-input/package/1",
                causal_root_kind="agent.model-safe-point",
                causal_root_id="run/agent-package/1",
            ),
            acquisition_declaration,
            {
                "conversation": {
                    "actor": {
                        "conversation_id": "conversation/onebot/group/933179609",
                        "owner_id": "conversation-owner/package/1",
                        "epoch": 1,
                    },
                    "boundary": "MODEL_SAFE_POINT",
                    "selection_mode": "ANY",
                    "session_ref": "session/agent-package/1",
                    "run_ref": "run/agent-package/1",
                    "next_run_ref": None,
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "selection_step_id": "conversation",
                    "deletion_epoch_step_id": "payload_epoch",
                },
                "agent": {
                    "owner": {
                        "run_id": owner["run_id"],
                        "owner_id": owner["owner_id"],
                        "epoch": owner["epoch"],
                    },
                    "selection_step_id": "conversation",
                    "memory_step_id": "memory",
                },
            },
            parent_call_id="call:agent-package/acquire-model-input",
            timeout_seconds=20,
        )
        assert acquisition == {
            "protocol": "forest.agent-input-acquisition/1",
            "status": "BATCH",
            "run_id": "run/agent-package/1",
            "input_batch_ref": acquisition["input_batch_ref"],
            "input_revision": 2,
            "selected_input_end_seq": 3,
            "context_revision": 2,
            "context_view_revision": 1,
            "memory_qualified_count": 0,
            "memory_pending_count": 2,
        }
        appended = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "agent-run-query/package/after-acquisition",
                10,
                "test.agent-runtime-package",
                "run/agent-package/1",
            ),
            ({"run_id": "run/agent-package/1"},),
            {},
        )
        assert appended["progress"] == {
            "input_revision": 2,
            "selected_input_end_seq": 3,
            "context_revision": 2,
            "context_view_revision": 1,
            "next_step_seq": 1,
            "checkpoint_seq": 0,
            "repair_count": 0,
            "tool_count": 0,
            "thinking_count": 0,
        }
        assert [item["last_trigger_ref"] for item in appended["inputs"]] == [
            "trigger/1", "trigger/2"
        ]
        assert [item["last_trigger_ingress_seq"] for item in appended["inputs"]] == [1, 3]

        start_declaration = next(
            item
            for item in instance.component["unit_of_work_plans"]
            if item["name"] == "agent.start-from-conversation"
        )
        atomic_start = await instance._unit_of_work_coordinator.execute(
            PackageCallScope(
                bot_id=store.bot_id,
                component_id="agent.runtime",
                generation_id=generation_id,
                instance_id=binding.instance_id,
                worker_incarnation=binding.worker_incarnation,
                call_id="call:agent-package/start-from-conversation",
                call_operation_id="agent-start-from-conversation/package/1",
                causal_root_kind="conversation.agent-run",
                causal_root_id="conversation/onebot/group/start-atomic",
            ),
            start_declaration,
            {
                "conversation": {
                    "actor": {
                        "conversation_id": "conversation/onebot/group/start-atomic",
                        "owner_id": "conversation-owner/start-atomic",
                        "epoch": 1,
                    },
                    "boundary": "IDLE",
                    "selection_mode": "ANY",
                    "session_ref": "session/start-atomic",
                    "run_ref": "run/start-atomic",
                    "next_run_ref": None,
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "selection_step_id": "conversation",
                    "deletion_epoch_step_id": "payload_epoch",
                },
                "agent": {
                    "conversation_ref": "conversation/onebot/group/start-atomic",
                    "session_id": "session/start-atomic",
                    "run_id": "run/start-atomic",
                    "agent_spec_revision": "agent-spec/1",
                    "config": config.canonical(),
                    "preset_revision": "preset/1",
                    "preset": preset.canonical(),
                    "absolute_deadline": (
                        datetime.now(timezone.utc) + timedelta(minutes=5)
                    ).isoformat(),
                    "selection_step_id": "conversation",
                    "memory_step_id": "memory",
                },
            },
            parent_call_id="call:agent-package/start-from-conversation",
            timeout_seconds=20,
        )
        assert atomic_start["protocol"] == "forest.agent-start-selection/1"
        assert atomic_start["status"] == "STARTED"
        assert atomic_start["run"]["run_id"] == "run/start-atomic"
        assert atomic_start["run"]["progress"]["selected_input_end_seq"] == 1
        assert atomic_start["run"]["inputs"][0]["last_trigger_ref"] == (
            "trigger/start-atomic"
        )
        assert atomic_start["run"]["inputs"][0]["last_trigger_ingress_seq"] == 1
        assert atomic_start["memory_qualified_count"] == 0
        assert atomic_start["memory_pending_count"] == 1

        checkpoint = await _payload(
            kernel,
            owner_kind="checkpoint.state",
            owner_id="checkpoint/reload/agent-package/1",
            value=b'{"phase":"RELOAD_WAIT","resume_phase":"PREPARING_MODEL"}',
        )
        async with store.pool.connection() as conn, conn.transaction():
            parked = await participant.invoke_in(
                conn,
                "agent.park-reload-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": caller,
                    "input": {
                        "owner": {
                            "run_id": owner["run_id"],
                            "owner_id": owner["owner_id"],
                            "epoch": owner["epoch"],
                        },
                        "checkpoint_id": checkpoint.owner_id,
                        "state": checkpoint.canonical(),
                        "reload_operation_id": "runtime-reload/agent-package/1",
                        "resume_phase": "PREPARING_MODEL",
                    },
                    "prior": {},
                },
                operation_id="agent-run-park-reload/package/1",
                causal_root_kind="runtime.reload",
                causal_root_id="runtime-reload/agent-package/1",
                parent_call_id="call:agent-package/park-reload",
                timeout_seconds=10,
            )
        assert parked["state"] == "RELOAD_WAIT"
        assert parked["progress"]["checkpoint_seq"] == 1
        assert parked["current"] == {
            "model_request_id": None,
            "tool_call_id": None,
            "reload_operation_id": "runtime-reload/agent-package/1",
            "reload_resume_phase": "PREPARING_MODEL",
            "reload_generation_id": generation_id,
        }

        release_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": {
                    "run_id": owner["run_id"],
                    "owner_id": owner["owner_id"],
                    "epoch": owner["epoch"],
                }
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            released = await participant.invoke_in(
                conn,
                "agent.release-run-record",
                release_envelope,
                operation_id="agent-run-release/package/1",
                causal_root_kind="agent.execute",
                causal_root_id="run/agent-package/1",
                parent_call_id="call:agent-package/release",
                timeout_seconds=10,
            )
        assert released == {"run_id": "run/agent-package/1", "released": True}

        async with store.pool.connection() as conn, conn.transaction():
            with pytest.raises(ForestError) as stale:
                await participant.invoke_in(
                    conn,
                    "agent.release-run-record",
                    release_envelope,
                    operation_id="agent-run-release/package/stale",
                    causal_root_kind="agent.execute",
                    causal_root_id="run/agent-package/1",
                    parent_call_id="call:agent-package/stale",
                    timeout_seconds=10,
                )
        assert stale.value.code == "STALE_AGENT_RUN_OWNER"

        # A failed publication restores the old Graph. Its original Agent
        # Generation must claim its own parked Run only after that recovery.
        await registry.drain_owned_work_for_replacement(
            "runtime-reload/agent-package/rollback"
        )
        await registry.resume_owned_work_after_failed_replacement()
        for poll in range(200):
            rolled_back = await endpoint.invoke_routed(
                route,
                RuntimeServiceInvocation(
                    f"agent-run-query/package/rollback/{poll}",
                    10,
                    "test.agent-runtime-package",
                    "run/agent-package/1",
                ),
                ({"run_id": "run/agent-package/1"},),
                {},
            )
            if rolled_back["state"] == "PREPARING_MODEL" and rolled_back["owner"] is None:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("restored Agent Generation did not resume its parked Run")
        assert rolled_back["current"]["reload_operation_id"] is None

        owner = await _lease_operation(
            store, participant, generation_id,
            "agent.acquire-run-record",
            {"run_id": "run/agent-package/1", "owner_id": "agent-worker/repark/1",
             "lease_seconds": 120},
            "agent-acquire/package/repark",
        )
        checkpoint = await _payload(
            kernel,
            owner_kind="checkpoint.state",
            owner_id="checkpoint/reload/agent-package/repark",
            value=b'{"phase":"RELOAD_WAIT","resume_phase":"PREPARING_MODEL"}',
        )
        reparking = await _lease_operation(
            store, participant, generation_id,
            "agent.park-reload-record",
            {
                "owner": _owner_ref(owner),
                "checkpoint_id": checkpoint.owner_id,
                "state": checkpoint.canonical(),
                "reload_operation_id": "runtime-reload/agent-package/repark",
                "resume_phase": "PREPARING_MODEL",
            },
            "agent-run-park-reload/package/repark",
        )
        assert reparking["state"] == "RELOAD_WAIT"
        assert reparking["progress"]["checkpoint_seq"] == 2
        assert await _lease_operation(
            store, participant, generation_id,
            "agent.release-run-record",
            {"owner": _owner_ref(owner)},
            "agent-run-release/package/repark",
        ) == {"run_id": "run/agent-package/1", "released": True}

        candidate = RuntimeModuleRegistry(host, isolated_descriptors)
        await candidate.prewarm()
        await registry.drain_owned_work_for_replacement(candidate.activation_id)
        await candidate.start()
        replacement = candidate.get("agent.runtime")
        replacement_generation = replacement.generation_identity.generation_id
        assert replacement_generation != generation_id
        assert await registry.close() == []
        registry = candidate

        replacement_endpoint = replacement.runtime_service_handlers()[
            ("agent.run.query", 1)
        ]
        replacement_route = FrozenServiceRoute(
            service_id="agent.run.query",
            version=1,
            caller_component_id="kernel.management",
            provider_component_id="agent.runtime",
            provider_generation_id=replacement_generation,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="CONTROL",
            owned_work=False,
        )
        for poll in range(200):
            resumed = await replacement_endpoint.invoke_routed(
                replacement_route,
                RuntimeServiceInvocation(
                    f"agent-run-query/package/replacement/{poll}",
                    10,
                    "test.agent-runtime-package",
                    "run/agent-package/1",
                ),
                ({"run_id": "run/agent-package/1"},),
                {},
            )
            if resumed["state"] == "PREPARING_MODEL" and resumed["owner"] is None:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("replacement Agent Generation did not resume parked Run")
        assert resumed["current"] == {
            "model_request_id": None,
            "tool_call_id": None,
            "reload_operation_id": None,
            "reload_resume_phase": None,
            "reload_generation_id": None,
        }

        conversation_caller = {
            "component_id": "conversation.scheduler",
            "generation_id": "generation/conversation/package-test",
        }
        replacement_participant = replacement.unit_of_work_participant()
        first_owner = await _lease_operation(
            store, replacement_participant, replacement_generation,
            "agent.acquire-run-record",
            {"run_id": "run/agent-package/1", "owner_id": "agent-worker/finalize/1",
             "lease_seconds": 120},
            "agent-acquire/package/first",
        )
        await _seed_final_ready(store, package.configuration.namespace, first_owner["run_id"])
        first_completed_state = await _payload(
            kernel,
            owner_kind="checkpoint.state",
            owner_id="checkpoint/package/first/completed",
            value=b'{"phase":"COMPLETED"}',
        )
        first_complete_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": conversation_caller,
            "input": {
                "owner": {
                    "run_id": first_owner["run_id"],
                    "owner_id": first_owner["owner_id"],
                    "epoch": first_owner["epoch"],
                },
                "checkpoint_id": first_completed_state.owner_id,
                "state": first_completed_state.canonical(),
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            completed = await replacement_participant.invoke_in(
                conn,
                "agent.complete-run-record",
                first_complete_envelope,
                operation_id="agent-run-complete/package/1",
                causal_root_kind="conversation.final-reply",
                causal_root_id="run/agent-package/1",
                parent_call_id="call:agent-package/complete-first",
                timeout_seconds=10,
            )
        assert completed["state"] == "COMPLETED"
        assert completed["session"]["active_run_id"] == "run/agent-package/1"
        assert await _lease_operation(
            store, replacement_participant, replacement_generation,
            "agent.release-run-record", {"owner": _owner_ref(first_owner)},
            "agent-release/package/first",
        ) == {"run_id": first_owner["run_id"], "released": True}

        successor_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": conversation_caller,
            "input": {
                "session_id": "session/agent-package/1",
                "completed_run_id": "run/agent-package/1",
                "run_id": "run/agent-package/2",
                "input_batch": {
                    "input_batch_ref": "input-batch/agent-package/3",
                    "input_batch_digest": digest_json({"batch": 3}),
                    "selected_input_end_seq": 4,
                    "context_revision": 3,
                    "context_view_revision": 1,
                    "source_manifest_digest": digest_json({"source": 3}),
                    "trigger_claim": {
                        "ref": "claim:" + "3" * 64, "count": 1,
                        "last_trigger_ref": "trigger/3", "last_trigger_ingress_seq": 4,
                        "digest": digest_json({"claim": 3}),
                    },
                },
                "absolute_deadline": (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat(),
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            successor = await replacement_participant.invoke_in(
                conn,
                "agent.start-successor-record",
                successor_envelope,
                operation_id="agent-run-successor/package/1",
                causal_root_kind="conversation.session-handoff",
                causal_root_id="session/agent-package/1",
                parent_call_id="call:agent-package/successor",
                timeout_seconds=10,
            )
        assert successor["state"] == "PREPARING_MODEL"
        assert successor["session"]["active_run_id"] == "run/agent-package/2"
        assert successor["session"]["session_epoch"] == 2
        assert successor["profile"] == completed["profile"]

        second_owner = await _lease_operation(
            store, replacement_participant, replacement_generation,
            "agent.acquire-run-record",
            {"run_id": "run/agent-package/2", "owner_id": "agent-worker/finalize/2",
             "lease_seconds": 120},
            "agent-acquire/package/second",
        )
        await _seed_final_ready(store, package.configuration.namespace, second_owner["run_id"])
        second_completed_state = await _payload(
            kernel,
            owner_kind="checkpoint.state",
            owner_id="checkpoint/package/second/completed",
            value=b'{"phase":"COMPLETED"}',
        )
        async with store.pool.connection() as conn, conn.transaction():
            second_completed = await replacement_participant.invoke_in(
                conn,
                "agent.complete-run-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": conversation_caller,
                    "input": {
                        "owner": {
                            "run_id": second_owner["run_id"],
                            "owner_id": second_owner["owner_id"],
                            "epoch": second_owner["epoch"],
                        },
                        "checkpoint_id": second_completed_state.owner_id,
                        "state": second_completed_state.canonical(),
                    },
                    "prior": {},
                },
                operation_id="agent-run-complete/package/2",
                causal_root_kind="conversation.final-reply",
                causal_root_id="run/agent-package/2",
                parent_call_id="call:agent-package/complete-second",
                timeout_seconds=10,
            )
        assert second_completed["state"] == "COMPLETED"
        assert await _lease_operation(
            store, replacement_participant, replacement_generation,
            "agent.release-run-record", {"owner": _owner_ref(second_owner)},
            "agent-release/package/second",
        ) == {"run_id": second_owner["run_id"], "released": True}

        close_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": conversation_caller,
            "input": {
                "session_id": "session/agent-package/1",
                "completed_run_id": "run/agent-package/2",
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            closed = await replacement_participant.invoke_in(
                conn,
                "agent.close-session-record",
                close_envelope,
                operation_id="agent-session-close/package/1",
                causal_root_kind="conversation.session-handoff",
                causal_root_id="session/agent-package/1",
                parent_call_id="call:agent-package/close-session",
                timeout_seconds=10,
            )
        assert closed["session_id"] == "session/agent-package/1"
        assert closed["completed_run_id"] == "run/agent-package/2"
        final_snapshot = await replacement_endpoint.invoke_routed(
            replacement_route,
            RuntimeServiceInvocation(
                "agent-run-query/package/final", 10,
                "test.agent-runtime-package", "run/agent-package/2",
            ),
            ({"run_id": "run/agent-package/2"},),
            {},
        )
        assert final_snapshot["session"]["state"] == "CLOSED"
        assert final_snapshot["session"]["active_run_id"] is None
    finally:
        assert await registry.close() == []
