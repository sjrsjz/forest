from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.agent_runtime_authority import (
    AgentInputBatch,
    AgentPayloadRef,
    AgentRuntimeRepository,
    AgentTriggerInput,
)


async def _payload(kernel, *, owner_kind: str, owner_id: str, value: bytes) -> AgentPayloadRef:
    saved = await kernel.payload.put_bytes(
        owner_component_id="agent.runtime",
        owner_kind=owner_kind,
        owner_id=owner_id,
        media_type="application/json",
        content=value,
        expected_deletion_epoch=await kernel.payload.epoch(),
    )
    return AgentPayloadRef(
        saved.payload_id,
        saved.content_digest,
        "agent.runtime",
        owner_kind,
        owner_id,
    )


@pytest.mark.postgres
async def test_agent_package_owns_run_snapshot_and_epoch_fenced_lease(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
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
    lease = await kernel.runtime.claim_host("agent-package-host", "agent-package-incarnation")
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
        instance = registry.get("agent.runtime")
        generation_id = instance.generation_identity.generation_id
        config = await _payload(
            kernel,
            owner_kind="module.configuration",
            owner_id="configuration/agent-package/1",
            value=b'{"protocol":"forest.agent.runtime/1"}',
        )
        repository = AgentRuntimeRepository(
            store,
            namespace=package.configuration.namespace,
            payload_verifier=kernel.payload.verify_in,
        )
        batch = AgentInputBatch(
            "input-batch/agent-package/1",
            digest_json({"batch": 1}),
            1,
            1,
            0,
            digest_json({"source": 1}),
            (AgentTriggerInput("trigger/1", "qq:42", 1),),
        )
        await repository.start_run(
            operation_id="agent-run-start/package/1",
            conversation_ref="conversation/onebot/group/933179609",
            session_id="session/agent-package/1",
            run_id="run/agent-package/1",
            input_batch=batch,
            agent_spec_revision="agent-spec/1",
            config=config,
            preset_revision="preset/1",
            preset=preset,
            absolute_deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
        )

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

        participant = instance.unit_of_work_participant()
        caller = {"component_id": "agent.runtime", "generation_id": generation_id}
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
    finally:
        assert await registry.close() == []
