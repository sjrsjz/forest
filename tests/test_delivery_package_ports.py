from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

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
        "delivery.create-control-intent",
        "delivery.create-control-intent-uow",
        "delivery.record-prepare",
        "delivery.record-handoff",
        "delivery.record-observe",
        "delivery.release-claim",
        "delivery.record-abandon",
    }
    assert [item["name"] for item in snapshot.component["owned_work_handlers"]] == [
        "delivery.dispatch"
    ]
    assert [item["name"] for item in snapshot.component["unit_of_work_plans"]] == [
        "delivery.prepare",
        "delivery.handoff",
        "delivery.observe",
        "delivery.release",
        "delivery.abandon",
    ]
    assert snapshot.management_ui() is not None


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
    payloads = SimpleNamespace(verify_in=AsyncMock())
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
    await authority._verify_payload_refs(
        object(),
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
    )
    assert [call.kwargs["payload_id"] for call in payloads.verify_in.await_args_list] == [
        "payload/3",
        "payload/4",
    ]
