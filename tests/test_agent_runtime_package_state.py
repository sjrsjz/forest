import asyncio
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.agent_runtime_authority import AgentPayloadRef, AgentRuntimeRepository


async def _payload(
    kernel,
    *,
    owner_kind: str,
    owner_id: str,
    value: bytes,
    owner_component_id: str = "agent.runtime",
) -> AgentPayloadRef:
    saved = await kernel.payload.put_bytes(
        owner_component_id=owner_component_id,
        owner_kind=owner_kind,
        owner_id=owner_id,
        media_type="application/json",
        content=value,
        expected_deletion_epoch=await kernel.payload.epoch(),
    )
    return AgentPayloadRef(
        saved.payload_id,
        saved.content_digest,
        owner_component_id,
        owner_kind,
        owner_id,
    )


async def _advance_to_final_ready(repository, kernel, owner, prefix: str):
    phase = "PREPARING_MODEL"
    for index, target in enumerate(("MODEL_PENDING", "PARSING", "SAFE_BOUNDARY"), 1):
        state = await _payload(
            kernel,
            owner_kind="checkpoint.state",
            owner_id=f"{prefix}/phase/{index}",
            value=(f'{{"phase":"{target}"}}').encode(),
        )
        await repository.transition(
            owner,
            checkpoint_id=state.owner_id,
            expected_phase=phase,
            target_phase=target,
            state=state,
        )
        phase = target
    ready = await _payload(
        kernel,
        owner_kind="checkpoint.state",
        owner_id=f"{prefix}/final-ready",
        value=b'{"phase":"FINAL_READY"}',
    )
    candidate = await _payload(
        kernel,
        owner_kind="final.candidate",
        owner_id=f"{prefix}/candidate",
        value=b'{"assistant":"done"}',
    )
    await repository.transition(
        owner,
        checkpoint_id=ready.owner_id,
        expected_phase="SAFE_BOUNDARY",
        target_phase="FINAL_READY",
        state=ready,
        final_candidate=candidate,
    )


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
    registry = RuntimeModuleRegistry(
        host,
        (
            *kernel.descriptors,
            conversation_package.descriptor,
            memory_package.descriptor,
            package.descriptor,
        ),
    )
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
                    "triggers": [
                        {
                            "trigger_ref": "trigger/1",
                            "principal_ref": "qq:42",
                            "ingress_seq": 1,
                        }
                    ],
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
        assert snapshot["triggers"] == [
            {
                "input_revision": 1,
                "trigger_index": 0,
                "trigger_ref": "trigger/1",
                "principal_ref": "qq:42",
                "ingress_seq": 1,
            }
        ]

        recovery_declaration = next(
            item
            for item in instance.component["domain_queries"]
            if item["name"] == "agent.run-recovery"
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
                    "triggers": [
                        {
                            "trigger_ref": "trigger/model-port",
                            "principal_ref": "qq:43",
                            "ingress_seq": 1,
                        }
                    ],
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
            owner_component_id="provider.chat",
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
        attempt_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request_id": "model-request/package/1",
                "attempt_id": "model-attempt/package/1",
                "effect_operation_ref": "model-effect/package/1",
                "provider_service_id": "provider.model.dispatch",
                "wire": wire.canonical(),
            },
            "prior": {
                "effect": {
                    "protocol": "forest.effect-permit/1",
                    "effect_operation_id": "model-effect/package/1",
                    "attempt_id": "model-attempt/package/1",
                    "wire_payload_id": wire.payload_id,
                    "wire_payload_digest": wire.digest,
                    "executor_component_id": "provider.chat",
                    "executor_generation_id": "generation/provider/package-test",
                    "fence_kind": "runtime.graph",
                }
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
            "state": "DISPATCHING",
        }
        result_payload = await _payload(
            kernel,
            owner_component_id="provider.chat",
            owner_kind="model.result",
            owner_id="model-attempt/package/1",
            value=b'{"status":"RESPONDED","text":"<broken>"}',
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
                "payloads": [
                    {"role": "RESULT", "payload": result_payload.canonical()}
                ],
            },
            "prior": {
                "effect": {
                    "attempt_id": "model-attempt/package/1",
                    "observation_operation_id": "model-observation/package/1",
                    "observed_status": "SUCCEEDED",
                    "evidence_kind": "executor",
                    "accepted": True,
                    "resulting_state": "SUCCEEDED",
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
            owner_component_id="provider.chat",
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
        repair_attempt_envelope = {
            **attempt_envelope,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "request_id": "model-request/package/repair",
                "attempt_id": "model-attempt/package/repair",
                "effect_operation_ref": "model-effect/package/repair",
                "provider_service_id": "provider.model.dispatch",
                "wire": repair_wire.canonical(),
            },
            "prior": {
                "effect": {
                    "protocol": "forest.effect-permit/1",
                    "effect_operation_id": "model-effect/package/repair",
                    "attempt_id": "model-attempt/package/repair",
                    "wire_payload_id": repair_wire.payload_id,
                    "wire_payload_digest": repair_wire.digest,
                    "executor_component_id": "provider.chat",
                    "executor_generation_id": "generation/provider/package-test",
                    "fence_kind": "runtime.graph",
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
                "payloads": [{"role": "RESULT", "payload": repair_result.canonical()}],
            },
            "prior": {
                "effect": {
                    "attempt_id": "model-attempt/package/repair",
                    "observation_operation_id": "model-observation/package/repair",
                    "observed_status": "SUCCEEDED",
                    "evidence_kind": "executor",
                    "accepted": True,
                    "resulting_state": "SUCCEEDED",
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
            "prior": {},
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
        wait_tool_envelope = {
            "protocol": "forest.uow-step/1",
            "caller": caller,
            "input": {
                "owner": request_envelope["input"]["owner"],
                "observation_operation_id": "tool-observation/package/wait",
                "tool_call_id": "tool-call/package/repair",
                "observed_state": "WAIT_APPROVAL",
                "evidence_kind": "AUTHORIZATION",
                "effect_observation_ref": None,
                "approval_ref": "approval/package/repair",
                "error_code": None,
                "results": [],
            },
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            waiting_tool = await participant.invoke_in(
                conn, "agent.observe-tool-result-record", wait_tool_envelope,
                operation_id="agent-tool-observation/package/wait",
                causal_root_kind="agent.execute", causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/tool-wait", timeout_seconds=10,
            )
        assert waiting_tool["accepted"] is True
        dispatch_tool_envelope = {
            **wait_tool_envelope,
            "input": {
                **wait_tool_envelope["input"],
                "observation_operation_id": "tool-observation/package/dispatch",
                "observed_state": "DISPATCHING",
                "effect_observation_ref": "tool-handoff/package/repair",
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            await participant.invoke_in(
                conn, "agent.observe-tool-result-record", dispatch_tool_envelope,
                operation_id="agent-tool-observation/package/dispatch",
                causal_root_kind="agent.execute", causal_root_id="run/agent-package/model-port",
                parent_call_id="call:agent-package/model-port/tool-dispatch", timeout_seconds=10,
            )
        tool_result = await _payload(
            kernel, owner_component_id="tool.echo", owner_kind="tool.result",
            owner_id="tool-call/package/repair", value=b'{"value":"A"}',
        )
        settle_tool_envelope = {
            **wait_tool_envelope,
            "input": {
                **wait_tool_envelope["input"],
                "observation_operation_id": "tool-observation/package/success",
                "observed_state": "SUCCEEDED", "evidence_kind": "EXECUTOR",
                "effect_observation_ref": "tool-result/package/repair",
                "error_code": None, "results": [tool_result.canonical()],
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            settled_tool = await participant.invoke_in(
                conn, "agent.observe-tool-result-record", settle_tool_envelope,
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
        assert [item["principal_ref"] for item in appended["triggers"]] == [
            "qq:42",
            "qq:43",
        ]

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

        candidate = RuntimeModuleRegistry(
            host,
            (
                *kernel.descriptors,
                conversation_package.descriptor,
                memory_package.descriptor,
                package.descriptor,
            ),
        )
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

        repository = AgentRuntimeRepository(
            store,
            namespace=package.configuration.namespace,
            payload_verifier=kernel.payload.verify_in,
        )
        conversation_caller = {
            "component_id": "conversation.scheduler",
            "generation_id": "generation/conversation/package-test",
        }
        replacement_participant = replacement.unit_of_work_participant()
        first_owner = await repository.acquire_run(
            "run/agent-package/1",
            "agent-worker/finalize/1",
            lease_seconds=120,
        )
        await _advance_to_final_ready(
            repository,
            kernel,
            first_owner,
            "checkpoint/package/first",
        )
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
                    "run_id": first_owner.run_id,
                    "owner_id": first_owner.owner_id,
                    "epoch": first_owner.epoch,
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
        await repository.release_run(first_owner)

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
                    "triggers": [
                        {
                            "trigger_ref": "trigger/3",
                            "principal_ref": "qq:44",
                            "ingress_seq": 4,
                        }
                    ],
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

        second_owner = await repository.acquire_run(
            "run/agent-package/2",
            "agent-worker/finalize/2",
            lease_seconds=120,
        )
        await _advance_to_final_ready(
            repository,
            kernel,
            second_owner,
            "checkpoint/package/second",
        )
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
                            "run_id": second_owner.run_id,
                            "owner_id": second_owner.owner_id,
                            "epoch": second_owner.epoch,
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
        await repository.release_run(second_owner)

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
        final_snapshot = await repository.load("run/agent-package/2")
        assert final_snapshot["session"]["state"] == "CLOSED"
        assert final_snapshot["session"]["active_run_id"] is None
    finally:
        assert await registry.close() == []
