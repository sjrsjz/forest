"""Authenticated ingress and actual codec receipts through real Worker/PG boundaries."""

import base64
import hashlib
import json
from dataclasses import replace

import pytest
from test_platform import platform as platform
from test_platform import receive
from test_web_authority import environment as environment

from forest.adapters.onebot import prepare_send
from forest.errors import ForestError
from forest.storage.input_payloads import InputContentStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def inbox(p):
    async with p.env.store.pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM public.bot_message_inbox ORDER BY ingress_seq LIMIT 1")).fetchone()


async def erase_input(p):
    source = InputContentStore(p.env.store).source_ref((await inbox(p))["source_event_id"])
    payloads = PayloadStore(p.env.store)
    await payloads.forget_sources(p.kernel._authority(), "erase-input", (source,), expected_deletion_epoch=0)
    while (await payloads.scrub(p.kernel._authority()))["remaining"]:
        pass


async def queued(p):
    await p.runtime._replace(p.desired)
    await receive(p, message="PAYLOAD-CANARY")
    item = await inbox(p)
    connection = p.runtime.connection(p.binding.binding_id)
    route = {
        "bot_id": p.env.store.bot_id, "binding_id": p.binding.binding_id,
        "platform": "onebot-v11", "self_id": connection.row["self_id"],
        "binding_epoch": connection.row["binding_epoch"], "channel_kind": "group",
        "channel_id": "30001", "thread_id": "", "visibility": "group",
    }
    await p.runtime.delivery.enqueue(
        p.kernel._authority(), operation_id="payload-send", route=route,
        parts=[[{"type": "text", "text": "derived reply"}]], response_seq=1,
        conversation_id=item["conversation_id"], parents=(item["payload_id"],),
    )
    return await p.runtime.delivery.claim(p.kernel._authority(), "test-worker")


async def test_codec_cannot_replace_authenticated_ingress_projection(platform, monkeypatch):
    p = platform
    await p.runtime._replace(p.desired)
    original = p.kernel.invoke_control_with_receipt

    async def substitute(*args, **kwargs):
        receipt = await original(*args, **kwargs)
        value = json.loads(json.dumps(receipt.value))
        value["message"]["segments"] = [{"type": "text", "text": "cached unrelated text"}]
        return replace(receipt, value=value)

    monkeypatch.setattr(p.kernel, "invoke_control_with_receipt", substitute)
    with pytest.raises(ForestError) as exc:
        await receive(p)
    assert exc.value.code == "ONEBOT_PROJECTION_CONFLICT"
    assert await inbox(p) is None


async def test_original_source_revokes_ingress_and_kernel_bodies_but_keeps_config(platform):
    p = platform
    await p.runtime._replace(p.desired)
    await receive(p, message="PAYLOAD-CANARY")
    async with p.env.store.pool.connection() as conn:
        call = await (await conn.execute("SELECT * FROM public.bot_calls WHERE control_service='onebot.normalize'")).fetchone()
        assert call["input_payload_id"] and call["result_payload_id"] and call["result"] is None
        before = await (await conn.execute(
            "SELECT payload_id FROM public.bot_payload_headers WHERE owner_kind IN ('kernel.input','kernel.result')"
        )).fetchall()
        assert len(before) == 2
    await erase_input(p)
    async with p.env.store.pool.connection() as conn:
        assert not await (await conn.execute(
            "SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
            "WHERE h.owner_kind IN ('kernel.input','kernel.result')"
        )).fetchall()
        assert (await (await conn.execute(
            "SELECT count(*) AS n FROM public.bot_payload_headers WHERE owner_kind='control.config' AND state='ACTIVE'"
        )).fetchone())["n"] >= 1
        assert (await (await conn.execute("SELECT status,result FROM public.bot_calls WHERE call_id=%s", (call["call_id"],))).fetchone()) == {
            "status": "COMPLETED", "result": None,
        }


async def test_send_permit_requires_actual_codec_result_and_carries_its_sources(platform):
    p = platform
    claim = await queued(p)
    prepared = await p.runtime.prepare(claim)
    wire = json.loads(prepared.prepared.wire_body)
    wire["message"][0]["data"]["text"] = "substituted after Worker"
    forged = json.dumps(wire, separators=(",", ":"), ensure_ascii=False).encode()
    with pytest.raises(ForestError) as exc:
        await p.runtime.delivery.permit(
            p.kernel._authority(), claim, action=prepared.prepared.action,
            wire_body=forged, wire_digest=hashlib.sha256(forged).hexdigest(),
            executor_ref=prepared.executor_ref, interval_seconds=0,
        )
    assert exc.value.code == "INVALID_WIRE_PAYLOAD"
    permit = await p.runtime.delivery.permit(
        p.kernel._authority(), claim, action=prepared.prepared.action,
        wire_body=prepared.prepared.wire_body, wire_digest=prepared.prepared.wire_digest,
        executor_ref=prepared.executor_ref, interval_seconds=0,
    )
    async with p.env.store.pool.connection() as conn:
        header = await (await conn.execute(
            "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s", (permit.wire_payload_id,),
        )).fetchone()
        assert prepared.executor_ref["result_payload_id"] in header["manifest"]["parents"]
    await erase_input(p)
    with pytest.raises(ForestError) as exc:
        await p.runtime.delivery.mark_started(permit)
    assert exc.value.code in {"DELETION_CONFLICT", "PAYLOAD_ERASED"}
    assert all("send_group_msg" not in request[0] for request in p.requests)


async def test_completed_codec_result_cannot_substitute_another_message(platform):
    p = platform
    claim = await queued(p)
    grant = await p.env.store.admit_control(
        p.kernel._authority(), "adapter.onebot", "substituted-call", "substituted-format", "input",
        service_name="onebot.prepare", input_parent_ids=(claim.payload_id,),
        input_value={"channel_kind": claim.route["channel_kind"], "channel_id": claim.route["channel_id"],
                     "segments": claim.payload},
    )
    forged = prepare_send("group", "30001", [{"type": "text", "text": "private cached content"}])
    call = await p.env.store.finish_call(grant, {
        "action": forged.action, "wire_body_b64": base64.b64encode(forged.wire_body).decode(),
        "wire_digest": forged.wire_digest,
    })
    executor = {key: call[key] for key in (
        "call_id", "component_id", "generation_id", "worker_incarnation", "route_epoch",
        "policy_revision", "admission_lane", "result_payload_id", "result_digest",
    )}
    with pytest.raises(ForestError) as exc:
        await p.runtime.delivery.permit(
            p.kernel._authority(), claim, action=forged.action, wire_body=forged.wire_body,
            wire_digest=forged.wire_digest, executor_ref=executor, interval_seconds=0,
        )
    assert exc.value.code == "INVALID_WIRE_PAYLOAD"
    async with p.env.store.pool.connection() as conn:
        assert not await (await conn.execute("SELECT 1 FROM public.bot_send_attempts")).fetchall()
    assert all("send_group_msg" not in request[0] for request in p.requests)
