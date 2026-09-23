from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from forest.adapter_onebot_runtime import CleanOneBotRuntime, PreparedAdapterSend
from forest.adapters.onebot import PreparedSend, SendResult
from forest.kernel_authorization_participant import (
    KernelAuthorizationUnitOfWorkParticipant,
)
from forest.kernel_payload_effect_driver import KernelPayloadJsonEffectDriver


@pytest.mark.asyncio
async def test_onebot_delivery_json_abi_preserves_exact_prepared_bytes():
    runtime = object.__new__(CleanOneBotRuntime)
    prepared = PreparedSend(
        "send_group_msg",
        b'{"group_id":123,"message":[{"data":{"text":"A"},"type":"text"}],"auto_escape":true}',
        "77426935a2351da33e63d672c3b1a9cb9de43349cf07e422c6e80697c644d609",
    )
    runtime.prepare_delivery = AsyncMock(
        return_value=PreparedAdapterSend(
            "binding/main", "client/1", 2, 3, "generation/adapter", "sha256:fence", prepared
        )
    )
    output = await runtime.prepare_delivery_json(
        {
            "binding_ref": "binding/main",
            "channel_kind": "GROUP",
            "channel_ref": "123",
            "parts": [{"type": "text", "text": "A"}],
            "operation_id": "prepare/1",
        }
    )
    assert PreparedSend.from_codec_result(output["prepared"]) == prepared

    runtime.dispatch = AsyncMock(return_value=SendResult("SENT", message_id="456"))
    result = await runtime.dispatch_json(
        {
            "binding_ref": output["binding_ref"],
            "client_incarnation": output["client_incarnation"],
            "binding_epoch": output["binding_epoch"],
            "connection_epoch": output["connection_epoch"],
            "adapter_generation_id": output["adapter_generation_id"],
            "executor_fence_digest": output["executor_fence_digest"],
            "prepared": output["prepared"],
            "timeout_seconds": 10,
        }
    )
    assert result == {
        "protocol": "forest.onebot-delivery-result/1",
        "status": "SENT",
        "message_id": "456",
        "error_code": None,
    }


@pytest.mark.asyncio
async def test_authorization_participant_derives_grants_from_caller_generation():
    runtime_repository = SimpleNamespace(
        generation_capability_grants_digest_in=AsyncMock(return_value="sha256:" + "1" * 64)
    )
    runtime = SimpleNamespace(
        generation_identity=SimpleNamespace(generation_id="generation/auth"),
        service=runtime_repository,
    )
    grant_set = SimpleNamespace(grant_set_id="grant/derived")
    authority = SimpleNamespace(
        grant_set_by_digest_in=AsyncMock(return_value=grant_set),
        issue_ticket_in=AsyncMock(return_value={"ticket_id": "ticket/1", "expires_at": "sealed"}),
    )
    participant = KernelAuthorizationUnitOfWorkParticipant(runtime, authority)
    request = {
        "protocol": "forest.authorization-issue/1",
        "ticket_id": "ticket/1",
        "issuance_operation_id": "ticket-issue/1",
        "operation_id": "permit/1",
        "action": "delivery.send",
        "resource_kind": "bot",
        "resource_id": "bot/1",
        "input_digest": "sha256:" + "2" * 64,
        "plan_digest": "sha256:" + "3" * 64,
        "expires_at": "2030-01-01T00:00:00+00:00",
        "audience_component_id": "adapter.onebot",
        "audience_generation_id": "generation/adapter",
    }
    result = await participant.invoke_in(
        object(),
        "authorization.issue",
        {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "delivery",
                "generation_id": "generation/delivery",
            },
            "input": request,
            "prior": {},
        },
    )
    assert result["ticket"]["ticket_id"] == "ticket/1"
    runtime_repository.generation_capability_grants_digest_in.assert_awaited_once_with(
        ANY,
        component_id="delivery",
        generation_id="generation/delivery",
    )
    authority.grant_set_by_digest_in.assert_awaited_once_with(
        ANY,
        grantee_kind="component",
        grantee_id="delivery",
        grant_set_digest="sha256:" + "1" * 64,
    )
    assert authority.issue_ticket_in.await_args.kwargs["grant_set"] is grant_set


@pytest.mark.asyncio
async def test_effect_payload_materializer_seals_kernel_effect_ownership():
    payloads = SimpleNamespace(
        epoch=AsyncMock(return_value=7),
        put_bytes=AsyncMock(
            return_value=SimpleNamespace(
                payload_id="payload/1", content_digest="sha256:" + "4" * 64
            )
        ),
    )
    runtime = SimpleNamespace(verify_generation_member=AsyncMock())
    identity = SimpleNamespace(component_id="delivery", generation_id="generation/delivery")
    driver = KernelPayloadJsonEffectDriver(
        {},
        1,
        identity,
        {"kind": "kernel.payload-json", "name": "kernel.effect.json-materialize"},
        runtime=runtime,
        payloads=payloads,
    )
    await driver.start()
    result = await driver.invoke(
        {
            "protocol": "forest.effect-json-materialize/1",
            "owner_kind": "effect.wire",
            "owner_id": "attempt/1",
            "value": {"message": "A"},
        },
        scope=SimpleNamespace(component_id="delivery", generation_id="generation/delivery"),
        invocation=SimpleNamespace(operation_id="materialize/1"),
    )
    assert result["payload"]["owner_component_id"] == "kernel.effect"
    payloads.put_bytes.assert_awaited_once_with(
        owner_component_id="kernel.effect",
        owner_kind="effect.wire",
        owner_id="attempt/1",
        media_type="application/json",
        content=b'{"message":"A"}',
        expected_deletion_epoch=7,
    )
