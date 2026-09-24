import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from test_schema_bundles import clean_authority as clean_authority

from forest.core_runtime import bootstrap_clean_kernel
from forest.packages import digest_json
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.authorization_authority import CapabilityGrant


def _jsonable(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


@pytest.mark.postgres
async def test_kernel_effect_freezes_command_and_permit_in_one_transaction(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    lease = await kernel.runtime.claim_host(
        "effect-command-host",
        "effect-command-incarnation",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(host, kernel.descriptors)
    await registry.start()
    try:
        caller = registry.get("kernel.runtime").generation_identity
        executor = registry.get("kernel.payload").generation_identity
        grant = await kernel.authorization.create_grant_set(
            grantee_kind="component",
            grantee_id="kernel.runtime",
            entries=(
                CapabilityGrant(
                    "provider.invoke",
                    "bot",
                    store.bot_id,
                    delegable=True,
                ),
            ),
            expected_policy_revision=await kernel.authorization.revision(),
        )
        parent = await kernel.payload.put_bytes(
            owner_component_id="kernel.runtime",
            owner_kind="test.command-parent",
            owner_id="effect-command/parent/1",
            media_type="application/json",
            content=b'{"prepared":true}',
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        input_digest = digest_json({"command": 1})
        plan_digest = digest_json({"plan": 1})
        deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
        async with store.pool.connection() as conn, conn.transaction():
            ticket = await kernel.authorization.issue_ticket_in(
                conn,
                ticket_id="ticket/effect-command/1",
                issuance_operation_id="ticket-issue/effect-command/1",
                issuer_kind="component.policy",
                issuer_id="kernel.runtime",
                subject_kind="component",
                subject_id="kernel.runtime",
                action="provider.invoke",
                resource_kind="bot",
                resource_id=store.bot_id,
                operation_id="permit/effect-command/1",
                input_digest=input_digest,
                plan_digest=plan_digest,
                grant_set=grant,
                expires_at=deadline,
                audience_component_id="kernel.payload",
                audience_generation_id=executor.generation_id,
            )
            result = await registry.get("kernel.effect").unit_of_work_participant().invoke_in(
                conn,
                "effect.prepare-command",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "kernel.runtime",
                        "generation_id": caller.generation_id,
                    },
                    "input": {
                        "protocol": "forest.effect-command-prepare/1",
                        "effect_operation_id": "effect-command/1",
                        "input_digest": input_digest,
                        "effect_kind": "test.command",
                        "target_kind": "component.service",
                        "target_id": "kernel.payload/test.command",
                        "attempt_id": "effect-command-attempt/1",
                        "permit_operation_id": "permit/effect-command/1",
                        "authorization_step_id": "authorization",
                        "authorization": {
                            "action": "provider.invoke",
                            "resource_kind": "bot",
                            "resource_id": store.bot_id,
                            "plan_digest": plan_digest,
                        },
                        "executor": {
                            "component_id": "kernel.payload",
                            "generation_id": executor.generation_id,
                            "fence_kind": "runtime.graph",
                            "fence_id": executor.graph_id,
                            "fence_epoch": executor.graph_revision,
                        },
                        "deadline_at": deadline.isoformat(),
                        "command": {
                            "value": {"protocol": "forest.test-command/1", "value": "A"},
                            "parents": [
                                {
                                    "payload_id": parent.payload_id,
                                    "digest": parent.content_digest,
                                    "owner_component_id": "kernel.runtime",
                                    "owner_kind": "test.command-parent",
                                    "owner_id": "effect-command/parent/1",
                                }
                            ],
                        },
                    },
                    "prior": {
                        "authorization": {
                            "protocol": "forest.authorization-issued/1",
                            "ticket": _jsonable(ticket),
                        }
                    },
                },
                operation_id="uow-step/effect-command/1",
                causal_root_kind="test.effect-command",
                causal_root_id="effect-command/1",
                parent_call_id="call/effect-command/1",
                timeout_seconds=10,
            )
        assert result["protocol"] == "forest.effect-command-permit/1"
        assert result["wire_payload"]["owner_component_id"] == "kernel.effect"
        assert result["wire_payload"]["owner_kind"] == "effect.wire"
        assert result["wire_payload"]["owner_id"] == "effect-command-attempt/1"
        assert result["permit"]["wire_payload_id"] == result["wire_payload"]["payload_id"]
        assert json.loads(
            await kernel.payload.read_bytes(result["wire_payload"]["payload_id"])
        ) == {
            "protocol": "forest.test-command/1",
            "value": "A",
        }
    finally:
        assert await registry.close() == []
