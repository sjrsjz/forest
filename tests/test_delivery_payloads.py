"""Real delivery content cutover and erasure fences; no live QQ credentials."""

import asyncio
import hashlib
from importlib.resources import files

import psycopg
import pytest
from legacy_022 import FrozenPayloadStore
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_delivery import delivery as delivery
from test_delivery import permit, route
from test_messages import messages as messages
from test_messages import owner, receive
from test_onebot import response, socket_api
from test_p3_upgrade import p2_authority
from test_upgrade import upgrade_database as upgrade_database

from forest.adapters.onebot import OneBotHTTPClient
from forest.delivery import DeliveryWorker, PreparedDelivery
from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore
from forest.storage.input_payloads import InputContentStore

pytestmark = pytest.mark.postgres


async def rows(store, query, parameters=()):
    async with store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


async def delivery_bodies(store):
    return await rows(
        store,
        "SELECT h.payload_id FROM public.bot_payload_headers h "
        "JOIN public.bot_payload_bodies b USING(bot_id,payload_id) "
        "WHERE h.owner_kind IN ('delivery.payload','delivery.request','delivery.receipt') "
        "OR (h.owner_kind='context.parts' AND h.owner_id='input-source')",
    )


async def sourced(d, lease, *, operation="send", sequence=1):
    body = await d.contents.payloads.put_json(
        lease,
        owner_kind="context.parts",
        owner_id="input-source",
        value={"text": "SOURCE-CANARY"},
        sources=(SourceRef("input-source"),),
        expected_deletion_epoch=0,
    )
    intent = await d.enqueue(
        lease,
        operation_id=operation,
        route=route(),
        parts=[[{"type": "text", "text": "SEND-CANARY"}]],
        conversation_id="conversation",
        response_seq=sequence,
        parents=(body.payload_id,),
    )
    return intent, body


async def erase(d, lease, *, scrub=True):
    await d.contents.payloads.forget_sources(
        lease, "erase", (SourceRef("input-source"),), expected_deletion_epoch=0
    )
    if scrub:
        while (await d.contents.payloads.scrub(lease))["remaining"]:
            pass


@pytest.mark.parametrize("boundary", ["queued", "claimed", "permitted"])
async def test_erasure_prevents_each_new_transport_boundary(delivery, boundary):
    d, lease = delivery
    original, source = await sourced(d, lease)
    claim = await d.claim(lease, "worker") if boundary != "queued" else None
    attempt = await permit(d, lease, claim) if boundary == "permitted" else None
    await erase(d, lease)
    if boundary == "queued":
        assert await d.claim(lease, "worker") is None
        assert (await d.list(lease))[0]["state"] == "CANCELLED"
    elif boundary == "claimed":
        with pytest.raises(ForestError) as exc:
            await permit(d, lease, claim)
        assert exc.value.code in {"DELETION_CONFLICT", "PAYLOAD_ERASED"}
        assert not await rows(d.store, "SELECT 1 FROM public.bot_send_attempts")
    else:
        with pytest.raises(ForestError) as exc:
            await d.mark_started(attempt)
        assert exc.value.code in {"DELETION_CONFLICT", "PAYLOAD_ERASED"}
        assert (await rows(d.store, "SELECT started_at FROM public.bot_send_attempts")) == [
            {"started_at": None}
        ]
    # The immutable idempotency receipt stays available after all content is gone.
    assert (
        await d.enqueue(
            lease,
            operation_id="send",
            route=route(),
            parts=[[{"type": "text", "text": "SEND-CANARY"}]],
            conversation_id="conversation",
            response_seq=1,
            parents=(source.payload_id,),
        )
        == original
    )
    assert not await delivery_bodies(d.store)


@pytest.mark.parametrize("status", ["SENT", "UNKNOWN"])
async def test_late_effect_observation_preserves_metadata_without_reviving_body(delivery, status):
    d, lease = delivery
    await sourced(d, lease)
    claim = await d.claim(lease, "worker")
    attempt = await permit(d, lease, claim)
    await d.mark_started(attempt)
    await erase(d, lease)
    receipt = {
        "message_id": "9001",
        "status": status,
        "debug": {"body": "LATE-CANARY"},
        "token": "RECEIPT-CANARY",
    }
    observed = await d.complete(attempt, status, receipt)
    assert observed["receipt"] == {"message_id": "9001", "status": status}
    assert observed["state"] == status
    row = (await rows(d.store, "SELECT * FROM public.bot_send_attempts"))[0]
    assert row["wire_body"] is None and row["receipt_payload_id"] is None
    assert row["wire_digest"] == attempt.wire_digest and row["started_at"] is not None
    assert not await delivery_bodies(d.store)
    if status == "UNKNOWN":
        with pytest.raises(ForestError) as exc:
            await d.reconcile(
                lease,
                claim.intent_id,
                0,
                operation_id="replay",
                decision="NOT_SENT",
                expected_revision=observed["revision"],
                reason="operator correction",
            )
        assert exc.value.code == "PAYLOAD_ERASED"
        skipped = await d.reconcile(
            lease,
            claim.intent_id,
            0,
            operation_id="skip",
            decision="SKIP",
            expected_revision=observed["revision"],
            reason="DELETED-REASON-CANARY",
        )
        assert skipped["state"] == "SKIPPED"
        assert (
            await d.reconcile(
                lease,
                claim.intent_id,
                0,
                operation_id="skip",
                decision="SKIP",
                expected_revision=observed["revision"],
                reason="DELETED-REASON-CANARY",
            )
            == skipped
        )
        assert not await delivery_bodies(d.store)
        assert (
            await rows(
                d.store, "SELECT reason,result,payload_id FROM public.bot_delivery_resolutions"
            )
        ) == [{"reason": None, "result": None, "payload_id": None}]
    assert await d.claim(lease, "other") is None


async def test_full_receipt_and_resolution_are_payloads_then_physically_erased(delivery):
    d, lease = delivery
    await sourced(d, lease)
    claim = await d.claim(lease, "worker")
    attempt = await permit(d, lease, claim)
    receipt = {"status": "UNKNOWN", "debug": {"original": "RECEIPT-CANARY"}}
    result = await d.complete(attempt, "UNKNOWN", receipt)
    result = await d.reconcile(
        lease,
        claim.intent_id,
        0,
        operation_id="skip",
        decision="SKIP",
        expected_revision=result["revision"],
        reason="REASON-CANARY",
    )
    part = (await rows(d.store, "SELECT * FROM public.bot_send_parts"))[0]
    saved = (await rows(d.store, "SELECT * FROM public.bot_send_attempts"))[0]
    resolution = (await rows(d.store, "SELECT * FROM public.bot_delivery_resolutions"))[0]
    assert part["payload"] is None and saved["wire_body"] is None
    assert resolution["reason"] is None and resolution["result"] is None
    assert resolution["result_metadata"] == result
    assert "CANARY" not in str((part, saved, resolution))
    async with d.store.pool.connection() as conn, conn.transaction():
        body = await d.contents.read_attempt_in(conn, saved, expected_deletion_epoch=0)
        assert body == attempt.wire_body
        assert (
            await d.contents.read_receipt_in(
                conn, saved["receipt_payload_id"], expected_deletion_epoch=0
            )
            == receipt
        )
        resolved = await d.contents.read_receipt_in(
            conn, resolution["payload_id"], expected_deletion_epoch=0
        )
        assert resolved["reason"] == "REASON-CANARY"
    await erase(d, lease)
    assert not await delivery_bodies(d.store)
    assert (await d.list(lease))[0]["state"] == "SKIPPED"


async def test_real_socket_receipt_after_erasure_does_not_restore_transmitted_bytes(delivery):
    d, lease = delivery
    await sourced(d, lease)
    release = asyncio.Event()

    async def reply(writer):
        await release.wait()
        writer.write(response({"status": "ok", "retcode": 0, "data": {"message_id": 123}}))
        await writer.drain()

    async with socket_api(reply) as (origin, requests, entered):
        client = OneBotHTTPClient(origin, "private-test-token")

        async def prepare(claim):
            return PreparedDelivery(
                client.prepare(claim.route, claim.payload), client.send, {"driver": "onebot-v11:1"}
            )

        worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
        task = asyncio.create_task(worker.run_once())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            await erase(d, lease)
            release.set()
            assert await asyncio.wait_for(task, 5)
            assert len(requests) == 1 and b"SEND-CANARY" in requests[0][2]
            assert (await d.list(lease))[0]["receipt"] == {"status": "SENT", "message_id": "123"}
            assert not await delivery_bodies(d.store)
            assert not await worker.run_once()
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client.aclose()


async def test_worker_never_hands_removed_permitted_body_to_transport(delivery, monkeypatch):
    d, lease = delivery
    await sourced(d, lease)
    calls = []
    original = d.mark_started

    async def remove_before_start(attempt):
        await erase(d, lease)
        await original(attempt)

    async def send(prepared, *, timeout):
        calls.append(prepared)
        raise AssertionError("Erased bytes must not enter the transport")

    async def prepare(claim):
        from forest.adapters.onebot import prepare_send

        return PreparedDelivery(
            prepare_send(claim.route["channel_kind"], claim.route["channel_id"], claim.payload),
            send,
            {"driver": "onebot-v11:1"},
        )

    monkeypatch.setattr(d, "mark_started", remove_before_start)
    worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
    assert await worker.run_once()
    assert calls == []
    assert (await d.list(lease))[0]["state"] == "FAILED"
    assert (
        await rows(
            d.store, "SELECT state,started_at,receipt_payload_id FROM public.bot_send_attempts"
        )
    ) == [{"state": "NOT_SENT", "started_at": None, "receipt_payload_id": None}]


async def test_same_delivery_operation_cannot_replace_its_source_parents(delivery):
    d, lease = delivery
    await sourced(d, lease)
    with pytest.raises(ForestError) as exc:
        await d.enqueue(
            lease,
            operation_id="send",
            route=route(),
            parts=[[{"type": "text", "text": "SEND-CANARY"}]],
            conversation_id="conversation",
            response_seq=1,
        )
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"


@pytest.mark.parametrize("lane", ["CHAT", "CONTROL"])
async def test_actual_reply_and_control_send_parts_inherit_input_sources(messages, lane):
    repository, lease, _ = messages
    d = DeliveryStore(repository.store)
    incoming = await receive(
        repository,
        lease,
        1,
        group="42",
        text="#forest help" if lane == "CONTROL" else "trigger",
        trigger=lane == "CHAT",
    )
    if lane == "CONTROL":
        claim = await repository.claim_control(lease, incoming["command_id"], "control")
        await repository.complete_control(
            lease,
            claim,
            "answer",
            reply_parts=[[{"type": "text", "text": "DERIVED-CANARY"}]],
            outcome={"code": "HELP"},
            delivery=d,
        )
    else:
        actor = await owner(repository, lease, incoming["conversation_id"])
        batch = await repository.select_input(actor, "start")
        await repository.finalize_reply(
            actor,
            "answer",
            run_id=batch["run_id"],
            expected_input_revision=1,
            reply_parts=[[{"type": "text", "text": "DERIVED-CANARY"}]],
            delivery=d,
        )
    inbox = (await rows(d.store, "SELECT * FROM public.bot_message_inbox"))[0]
    source = InputContentStore(d.store).source_ref(inbox["source_event_id"])
    dependencies = await rows(
        d.store,
        "SELECT s.source_unit_id FROM public.bot_payload_sources s JOIN public.bot_send_parts p USING(bot_id,payload_id)",
    )
    assert source.source_unit_id in {row["source_unit_id"] for row in dependencies}
    await d.contents.payloads.forget_sources(lease, "forget", (source,), expected_deletion_epoch=0)
    assert await d.claim(lease, "sender") is None
    assert (await d.list(lease))[0]["state"] == "CANCELLED"


@pytest.mark.parametrize("previously_suppressed", [False, True])
async def test_actual_028_upgrade_preserves_exact_wire_and_unknown_without_plaintext_fallback(
    upgrade_database, previously_suppressed
):
    await p2_authority(upgrade_database)
    payload = [{"type": "text", "text": "OLD-SEND-CANARY"}]
    wire = (
        b'{ "message" : [{"type":"text","data":{"text":"OLD-SEND-CANARY"}}], "group_id" : 20000 }'
    )
    digest = hashlib.sha256(wire).hexdigest()
    old_route = {**route(), "binding_id": "binding", "self_id": "1000"}
    old_receipt = {"status": "UNKNOWN", "debug": {"body": "OLD-RECEIPT-CANARY"}}
    async with (
        await psycopg.AsyncConnection.connect(
            upgrade_database, autocommit=True, row_factory=dict_row
        ) as conn,
        conn.transaction(),
    ):
        for path in sorted(
            files("forest.storage").joinpath("bot_migrations").iterdir(), key=lambda p: p.name
        ):
            if not path.name.endswith(".sql") or not 13 <= int(path.name[:3]) <= 28:
                continue
            text = path.read_text(encoding="utf-8")
            await conn.execute(text)
            await conn.execute(
                "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                (path.name, hashlib.sha256(text.encode()).hexdigest()),
            )
        await conn.execute(
            "INSERT INTO public.bot_delivery_roots(bot_id,root_operation_id) VALUES ('bot-a','old-root')"
        )
        await conn.execute(
            "INSERT INTO public.bot_delivery_streams(bot_id,stream_id,binding_id,lane,last_seq) VALUES ('bot-a','old-stream','binding','CHAT',1)"
        )
        await conn.execute(
            "INSERT INTO public.bot_send_intents(bot_id,intent_id,operation_id,input_digest,root_operation_id,cause_kind,cause_id,binding_id,binding_epoch,policy_revision,storage_generation,stream_id,send_lane,conversation_id,response_seq,sequence,reply_route,adapter_contract) VALUES ('bot-a','old-intent','old-operation','old-digest','old-root','DELIVERY_OPERATION','old-operation','binding',1,1,1,'old-stream','CHAT','conversation',1,1,%s,'onebot-v11:1')",
            (Jsonb(old_route),),
        )
        await conn.execute(
            "INSERT INTO public.bot_send_parts(bot_id,intent_id,part_index,part_operation_id,payload,payload_digest,state,owner_epoch,attempt_id,attempt_count,receipt) VALUES ('bot-a','old-intent',0,'old-part',%s,%s,'UNKNOWN',1,'old-attempt',1,%s)",
            (Jsonb(payload), digest_json(payload).removeprefix("sha256:"), Jsonb(old_receipt)),
        )
        await conn.execute(
            "INSERT INTO public.bot_send_attempts(bot_id,attempt_id,intent_id,part_index,owner_epoch,host_incarnation,host_owner_epoch,policy_revision,binding_epoch,connection_epoch,binding_revision,cancel_epoch,action,wire_body,wire_digest,executor_ref,permitted_at,deadline_at,started_at,state,receipt) VALUES ('bot-a','old-attempt','old-intent',0,1,'old-host',1,1,1,1,1,0,'send_group_msg',%s,%s,'{}',clock_timestamp(),clock_timestamp(),clock_timestamp(),'UNKNOWN',%s)",
            (wire, digest, Jsonb(old_receipt)),
        )
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    d = DeliveryStore(store)
    try:
        if previously_suppressed:
            lease = await store.acquire_host("old", "old", ttl_seconds=30)
            await FrozenPayloadStore(store).forget_sources(
                lease, "old-forget", (SourceRef("old-untracked-source"),), expected_deletion_epoch=0
            )
            await store.release_host(lease)
        await store.migrate()
        lease = await store.acquire_host("new", "new", ttl_seconds=120)
        part = (await rows(store, "SELECT * FROM public.bot_send_parts"))[0]
        attempt = (await rows(store, "SELECT * FROM public.bot_send_attempts"))[0]
        assert part["payload"] is None and attempt["wire_body"] is None
        assert part["state"] == attempt["state"] == "UNKNOWN"
        assert attempt["wire_digest"] == digest and attempt["started_at"] is not None
        assert attempt["receipt"] == {"status": "UNKNOWN"}
        async with store.pool.connection() as conn, conn.transaction():
            clock = await d.contents.payloads.lock_in(conn)
            if previously_suppressed:
                with pytest.raises(ForestError) as exc:
                    await d.contents.read_attempt_in(
                        conn, attempt, expected_deletion_epoch=clock["deletion_epoch"]
                    )
                assert exc.value.code == "PAYLOAD_ERASED"
            else:
                assert (
                    await d.contents.read_attempt_in(conn, attempt, expected_deletion_epoch=0)
                    == wire
                )
                assert (
                    await d.contents.read_part_in(conn, part, expected_deletion_epoch=0) == payload
                )
                assert (
                    await d.contents.read_receipt_in(
                        conn, attempt["receipt_payload_id"], expected_deletion_epoch=0
                    )
                    == old_receipt
                )
        await store.migrate()
        assert await d.claim(lease, "worker") is None
        if not previously_suppressed:
            await d.contents.payloads.forget_sources(
                lease, "erase-old", (SourceRef("a-source"),), expected_deletion_epoch=0
            )
        while (await d.contents.payloads.scrub(lease))["remaining"]:
            pass
        assert not await rows(
            store,
            "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
            (b"OLD-SEND-CANARY",),
        )
        assert not await rows(
            store,
            "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
            (b"OLD-RECEIPT-CANARY",),
        )
        assert (await d.list(lease))[0]["state"] == "UNKNOWN"
    finally:
        await store.close()
