import hashlib
import json
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from forest.builtin.delivery.python.delivery import Delivery
from forest.errors import ForestError
from forest.kernel_authorization_participant import (
    KernelAuthorizationUnitOfWorkParticipant,
)
from forest.kernel_payload_effect_driver import KernelPayloadJsonEffectDriver
from forest.package_domain_authority import PostgresPackageDomainAuthority
from forest.packages import PackageCatalog


def test_delivery_builtin_is_an_immutable_worker_package(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(
        Path(str(files("forest").joinpath("builtin/delivery")))
    )
    assert snapshot.component["id"] == "delivery"
    assert snapshot.manifest["runtime"] == "python-worker"
    assert {item["name"] for item in snapshot.component["unit_of_work_participants"]} == {
        "delivery.create-chat-intent",
        "delivery.create-chat-intent-uow",
        "delivery.create-control-intent",
        "delivery.create-control-intent-uow",
        "delivery.create-core-rescue-intent-uow",
        "delivery.record-prepare",
        "delivery.record-handoff",
        "delivery.record-observe",
        "delivery.release-claim",
        "delivery.reject-prepare-record",
        "delivery.record-abandon",
    }
    assert [item["name"] for item in snapshot.component["owned_work_handlers"]] == [
        "delivery.dispatch"
    ]
    assert [item["name"] for item in snapshot.component["unit_of_work_plans"]] == [
        "delivery.prepare",
        "delivery.handoff",
        "delivery.observe",
        "delivery.reconcile",
        "delivery.release",
        "delivery.abandon",
        "delivery.reject-prepare",
    ]
    assert {item["id"] for item in snapshot.component["management_queries"]} == {
        "delivery.unknown.list", "delivery.unknown.get",
    }
    assert [item["port"] for item in snapshot.component["management_actions"]] == [
        "delivery.reconcile",
    ]
    assert snapshot.management_ui() is not None


@pytest.mark.asyncio
async def test_core_revoke_at_adapter_boundary_is_settled_as_unsent():
    wire_body = json.dumps({
        "action": "send_group_msg", "params": {"group_id": 933179609, "message": "ok"},
    }).encode()
    wire_digest = hashlib.sha256(wire_body).hexdigest()
    adapter = {
        "status": "READY", "error_code": None,
        "binding_ref": "binding/main", "binding_epoch": 1,
        "connection_epoch": 1, "client_incarnation": "client/1",
        "adapter_generation_id": "adapter/1", "graph_id": "graph/1",
        "graph_revision": 1, "transport_digest": "sha256:" + "b" * 64,
        "prepared": {
            "wire_payload": {"payload_id": "wire/1", "digest": "sha256:" + wire_digest},
            "wire_digest": wire_digest,
        },
    }
    work = {
        "mode": "NEW", "intent_id": "core-intent:" + "a" * 64,
        "cause_component_id": "kernel.runtime", "cause_kind": "core.rescue.request",
        "cause_id": "core:" + "a" * 64,
        "payload": {"payload_id": "payload/1"},
        "binding_ref": "binding/main", "channel_kind": "GROUP",
        "channel_ref": "933179609", "semantic_digest": "sha256:" + "c" * 64,
        "attempt_id": "attempt/1", "effect_operation_ref": "effect/1",
    }
    calls = []

    async def service_call(name, _request, **_options):
        calls.append(name)
        if name == "kernel.payload.json-read":
            return {"value": {"text": "ok"}}
        if name == "adapter.onebot.delivery.prepare":
            return adapter
        if name == "kernel.effect.json-materialize":
            return {"payload": {"payload_id": "wire/1", "digest": "sha256:" + wire_digest}}
        if name == "core.rescue.delivery-permit":
            return {"allowed": True}
        if name == "adapter.onebot.delivery.dispatch":
            raise ForestError(
                "CORE_RESCUE_DELIVERY_REVOKED", "Authorization was revoked", 409
            )
        pytest.fail(f"Unexpected service {name}")

    async def uow_call(name, _request, **_options):
        calls.append(name)
        return {"permit": {"effect_operation_id": "effect/1"}}

    delivery = Delivery()
    observed = AsyncMock()
    delivery._observe = observed
    ctx = SimpleNamespace(
        services=SimpleNamespace(call=service_call),
        uow=SimpleNamespace(call=uow_call),
        operations=SimpleNamespace(stable=lambda suffix: "op/" + suffix),
        identity={"bot_id": "bot/1"},
    )
    result = await delivery.dispatch({"payload": work}, ctx)
    assert result == {"intent_id": work["intent_id"], "state": "FAILED"}
    assert calls.index("delivery.handoff") < calls.index("adapter.onebot.delivery.dispatch")
    observed.assert_awaited_once_with(
        work, ctx, "REJECTED_NOT_APPLIED", None, "CORE_RESCUE_DELIVERY_REVOKED"
    )


@pytest.mark.asyncio
async def test_authorization_participant_derives_grants_from_caller_generation():
    runtime_repository = SimpleNamespace(
        generation_capability_grants_digest_in=AsyncMock(return_value="sha256:" + "1" * 64)
    )
    runtime = SimpleNamespace(
        generation_identity=SimpleNamespace(generation_id="generation/auth"),
        generation_authority=runtime_repository,
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
async def test_authorization_participant_owns_approval_scope_and_requester_identity():
    runtime_repository = SimpleNamespace(
        generation_capability_grants_digest_in=AsyncMock(
            return_value="sha256:" + "1" * 64
        )
    )
    runtime = SimpleNamespace(
        generation_identity=SimpleNamespace(generation_id="generation/auth"),
        generation_authority=runtime_repository,
    )
    grant_set = SimpleNamespace(policy_revision=7)
    approval = {
        "approval_id": "approval/tool/1",
        "request_kind": "agent.tool",
        "request_id": "tool/1",
        "action": "tool.invoke",
        "input_digest": "sha256:" + "2" * 64,
        "plan_digest": "sha256:" + "3" * 64,
        "requester_kind": "component",
        "requester_id": "agent.runtime",
        "state": "PENDING",
        "expires_at": "2030-01-01T00:00:00+00:00",
    }
    authority = SimpleNamespace(
        store=SimpleNamespace(bot_id="bot/1"),
        grant_set_by_digest_in=AsyncMock(return_value=grant_set),
        request_approval_in=AsyncMock(return_value=approval),
    )
    participant = KernelAuthorizationUnitOfWorkParticipant(runtime, authority)
    result = await participant.invoke_in(
        object(),
        "authorization.approval.request",
        {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "agent.runtime",
                "generation_id": "generation/agent",
            },
            "input": {
                "protocol": "forest.approval-request/1",
                "approval_id": "approval/tool/1",
                "request_kind": "agent.tool",
                "request_id": "tool/1",
                "action": "tool.invoke",
                "input_digest": "sha256:" + "2" * 64,
                "plan_digest": "sha256:" + "3" * 64,
                "expires_at": "2030-01-01T00:00:00+00:00",
            },
            "prior": {},
        },
    )
    assert result == {
        "protocol": "forest.approval-state/1",
        "approval_id": "approval/tool/1",
        "request_kind": "agent.tool",
        "request_id": "tool/1",
        "action": "tool.invoke",
        "input_digest": "sha256:" + "2" * 64,
        "plan_digest": "sha256:" + "3" * 64,
        "state": "PENDING",
        "expires_at": "2030-01-01T00:00:00+00:00",
    }
    assert authority.request_approval_in.await_args.kwargs["requester_id"] == "agent.runtime"
    assert authority.request_approval_in.await_args.kwargs["approver_scope_id"] == "bot/1"
    assert authority.request_approval_in.await_args.kwargs["expected_policy_revision"] == 7


@pytest.mark.asyncio
async def test_authorization_participant_consumes_only_exact_caller_approval():
    approval = {
        "approval_id": "approval/tool/1",
        "request_kind": "agent.tool",
        "request_id": "tool/1",
        "action": "tool.invoke",
        "input_digest": "sha256:" + "2" * 64,
        "plan_digest": "sha256:" + "3" * 64,
        "requester_kind": "component",
        "requester_id": "agent.runtime",
        "state": "CONSUMED",
        "expires_at": "2030-01-01T00:00:00+00:00",
    }
    authority = SimpleNamespace(
        consume_approval_in=AsyncMock(return_value=approval),
    )
    participant = KernelAuthorizationUnitOfWorkParticipant(
        SimpleNamespace(generation_identity=SimpleNamespace(generation_id="generation/auth")),
        authority,
    )
    result = await participant.invoke_in(
        object(),
        "authorization.approval.consume",
        {
            "protocol": "forest.uow-step/1",
            "caller": {
                "component_id": "agent.runtime",
                "generation_id": "generation/agent",
            },
            "input": {
                "protocol": "forest.approval-consume/1",
                "approval_id": "approval/tool/1",
                "request_kind": "agent.tool",
                "request_id": "tool/1",
                "action": "tool.invoke",
                "input_digest": "sha256:" + "2" * 64,
                "plan_digest": "sha256:" + "3" * 64,
                "consume_operation_id": "consume/tool/1",
            },
            "prior": {},
        },
    )
    assert result["state"] == "CONSUMED"
    assert authority.consume_approval_in.await_args.kwargs["requester_kind"] == "component"
    assert authority.consume_approval_in.await_args.kwargs["requester_id"] == "agent.runtime"


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


@pytest.mark.asyncio
async def test_domain_authority_verifies_every_payload_in_declared_array():
    payloads = SimpleNamespace(store=SimpleNamespace(), verify_in=AsyncMock())
    authority = PostgresPackageDomainAuthority(
        SimpleNamespace(),
        SimpleNamespace(),
        component_id="delivery",
        runtime_package_digest="sha256:" + "1" * 64,
        storage_spec=SimpleNamespace(),
        storage_package_digest="sha256:" + "2" * 64,
        storage_execution_profile="official",
        payloads=payloads,
    )
    references = [
        {
            "payload_id": f"payload/{index}",
            "digest": "sha256:" + str(index) * 64,
            "owner_component_id": "agent.runtime",
            "owner_kind": "agent.reply",
            "owner_id": f"reply/{index}",
        }
        for index in (3, 4)
    ]
    conn = object()
    await authority._verify_payload_refs(
        conn,
        {
            "payload_refs": [
                {
                    "path": "/parts/*/payload",
                    "owner_id_path": "/parts/*/reply_ref",
                    "owner_component": "agent.runtime",
                    "owner_kind": "agent.reply",
                }
            ]
        },
        {
            "parts": [
                {"reply_ref": f"reply/{index}", "payload": reference}
                for index, reference in zip((3, 4), references, strict=True)
            ]
        },
        scope=SimpleNamespace(generation_id="generation/delivery"),
    )
    assert [call.kwargs["payload_id"] for call in payloads.verify_in.await_args_list] == [
        "payload/3",
        "payload/4",
    ]


@pytest.mark.asyncio
async def test_domain_authority_resolves_payload_owner_from_the_port_input():
    payloads = SimpleNamespace(store=SimpleNamespace(), verify_in=AsyncMock())
    authority = PostgresPackageDomainAuthority(
        SimpleNamespace(),
        SimpleNamespace(),
        component_id="conversation.scheduler",
        runtime_package_digest="sha256:" + "1" * 64,
        storage_spec=SimpleNamespace(),
        storage_package_digest="sha256:" + "2" * 64,
        storage_execution_profile="official",
        payloads=payloads,
    )
    reference = {
        "payload_id": "payload/event",
        "digest": "sha256:" + "3" * 64,
        "owner_component_id": "adapter.onebot",
        "owner_kind": "onebot.normalized-event",
        "owner_id": "event/1",
    }
    conn = object()
    await authority._verify_payload_refs(
        conn,
        {
            "payload_refs": [
                {
                    "path": "/input/content",
                    "owner_id_path": "/input/event_id",
                    "owner_component_path": "/input/source_component_id",
                    "owner_kind_path": "/input/content_owner_kind",
                }
            ]
        },
        {
            "input": {
                "event_id": "event/1",
                "source_component_id": "adapter.onebot",
                "content_owner_kind": "onebot.normalized-event",
                "content": reference,
            }
        },
        scope=SimpleNamespace(generation_id="generation/conversation"),
    )
    payloads.verify_in.assert_awaited_once_with(
        conn,
        payload_id="payload/event",
        digest="sha256:" + "3" * 64,
        owner_component_id="adapter.onebot",
        owner_kind="onebot.normalized-event",
        owner_id="event/1",
    )
