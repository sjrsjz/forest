"""Real PostgreSQL arbitration and fault-injected external sends."""

import asyncio
import base64
import hashlib
import json
import os
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from binding_seed import create_online_binding, reconnect_binding
from psycopg import sql

from forest.adapters.onebot import SendResult, prepare_send
from forest.delivery import DeliveryWorker, PreparedDelivery
from forest.errors import ForestError
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def delivery():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_delivery_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-a")
    try:
        await store.open()
        await store.migrate()
        lease = await store.acquire_host("host", "inc-a", ttl_seconds=120)
        await create_online_binding(store, lease, "binding-a", "10000")
        yield DeliveryStore(store), lease
    finally:
        await store.close()
        assert name.startswith("forest_delivery_") and len(name) == 48
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def route(group="20000", epoch=2):
    return {
        "bot_id": "bot-a",
        "binding_id": "binding-a",
        "platform": "onebot-v11",
        "self_id": "10000",
        "binding_epoch": epoch,
        "channel_kind": "group",
        "channel_id": group,
        "thread_id": "",
        "visibility": "group",
    }


async def enqueue(
    d,
    lease,
    *,
    group="20000",
    sequence=1,
    lane="CHAT",
    command="command-a",
    operation=None,
    root=None,
    text="hello",
    parts=None,
):
    return await d.enqueue(
        lease,
        operation_id=operation or f"{group}-{lane}-{command}-{sequence}",
        route=route(group),
        parts=parts or [[{"type": "text", "text": text}]],
        conversation_id="conversation-" + group,
        lane=lane,
        response_seq=sequence if lane == "CHAT" else None,
        command_id=command if lane == "CONTROL" else None,
        control_seq=sequence if lane == "CONTROL" else None,
        root_operation_id=root,
    )


async def permit(d, lease, claim, **kwargs):
    prepared = prepare_send(claim.route["channel_kind"], claim.route["channel_id"], claim.payload)
    return await d.permit(
        lease,
        claim,
        action=prepared.action,
        wire_body=prepared.wire_body,
        wire_digest=prepared.wire_digest,
        interval_seconds=0,
        **kwargs,
    )


async def expect(code, awaitable):
    with pytest.raises(ForestError) as exc:
        await awaitable
    assert exc.value.code == code


async def codec_receipt(d, lease, claim):
    prepared = prepare_send(claim.route["channel_kind"], claim.route["channel_id"], claim.payload)
    grant = await d.store.admit_control(
        lease, "adapter.onebot", "format-call", "format-operation", "format-input",
        service_name="onebot.prepare",
        input_value={"channel_kind": claim.route["channel_kind"], "channel_id": claim.route["channel_id"],
                     "segments": claim.payload}, input_parent_ids=(claim.payload_id,),
    )
    call = await d.store.finish_call(grant, {
        "action": prepared.action, "wire_body_b64": base64.b64encode(prepared.wire_body).decode(),
        "wire_digest": prepared.wire_digest,
    })
    return {key: call[key] for key in ("call_id", "component_id", "generation_id", "route_epoch",
                                     "worker_incarnation", "policy_revision", "admission_lane",
                                     "result_payload_id", "result_digest")}


async def test_intent_idempotency_contiguous_stream_and_immutable_attempt(delivery):
    d, lease = delivery
    original = await enqueue(d, lease)
    assert await enqueue(d, lease) == original
    await expect("IDEMPOTENCY_CONFLICT", enqueue(d, lease, text="modified"))
    await expect("DELIVERY_SEQUENCE_CONFLICT", enqueue(d, lease, sequence=3))
    claim = await d.claim(lease, "worker")
    p = await permit(d, lease, claim)
    async with d.store.pool.connection() as conn:
        for statement in (
            "UPDATE public.bot_send_intents SET reply_route='{}'::jsonb",
            "UPDATE public.bot_send_parts SET payload='[]'::jsonb",
            "UPDATE public.bot_send_attempts SET wire_digest='changed'",
        ):
            with pytest.raises(psycopg.errors.RaiseException):
                await conn.execute(statement)
    result = await d.complete(p, "SENT", {"message_id": "40000"})
    assert result["state"] == "SENT"
    assert (await d.complete(p, "UNKNOWN"))["state"] == "SENT"


async def test_unknown_blocks_only_same_order_chain(delivery):
    d, lease = delivery
    chat = await enqueue(
        d, lease, parts=[[{"type": "text", "text": "a"}], [{"type": "text", "text": "b"}]]
    )
    await enqueue(d, lease, sequence=2)
    first = await d.claim(lease, "w")
    assert first.intent_id == chat["intent_id"] and first.part_index == 0
    p = await permit(d, lease, first)
    await d.complete(p, "UNKNOWN", {"error_code": "RECEIPT_LOST"})
    assert await d.claim(lease, "w", lanes=("CHAT",)) is None
    command_a = await enqueue(d, lease, lane="CONTROL", command="a")
    await enqueue(d, lease, lane="CONTROL", command="a", sequence=2)
    command_b = await enqueue(d, lease, lane="CONTROL", command="b")
    control = await d.claim(lease, "w")
    assert control.intent_id == command_a["intent_id"]
    await d.complete(await permit(d, lease, control), "UNKNOWN")
    control = await d.claim(lease, "w")
    assert control.intent_id == command_b["intent_id"]
    await d.complete(await permit(d, lease, control), "SENT", {"message_id": "1"})
    other_group = await enqueue(d, lease, group="30000")
    other = await d.claim(lease, "w")
    assert other.intent_id == other_group["intent_id"]


async def test_concurrent_claim_has_one_owner_and_scope_fails_closed(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claims = await asyncio.gather(*(d.claim(lease, f"w-{i}") for i in range(20)))
    winners = [claim for claim in claims if claim]
    assert len(winners) == 1
    await expect("BOT_SCOPE_MISMATCH", permit(d, lease, replace(winners[0], bot_id="other")))
    await expect("STALE_DELIVERY_OWNER", permit(d, lease, replace(winners[0], worker_id="forged")))
    assert (await d.list(lease))[0]["state"] == "CLAIMED"


async def test_cancel_and_permit_share_effect_root_boundary(delivery):
    d, lease = delivery
    await enqueue(d, lease, root="run-a")
    claim = await d.claim(lease, "w")
    assert (await d.cancel_root(lease, "run-a"))["cancelled_parts"] == 1
    await expect("STALE_DELIVERY_OWNER", permit(d, lease, claim))
    await enqueue(d, lease, group="30000", root="run-b")
    claim = await d.claim(lease, "w")
    p = await permit(d, lease, claim)
    result = await d.cancel_root(lease, "run-b")
    assert result == {"cancelled_parts": 0, "inflight_or_unknown": 1}
    assert (await d.complete(p, "SENT", {"message_id": "known-success"}))["state"] == "SENT"


async def test_guard_rechecks_host_time_after_waiting_for_cancel_root(delivery):
    d, lease = delivery
    await enqueue(d, lease, root="held-root")
    claim = await d.claim(lease, "w")
    lease = await d.store.renew_host(lease, ttl_seconds=0.35)
    async with d.store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT * FROM public.bot_delivery_roots WHERE root_operation_id='held-root' FOR UPDATE"
        )
        task = asyncio.create_task(permit(d, lease, claim))
        await asyncio.sleep(0.45)
    await expect("STALE_HOST", task)
    async with d.store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_send_attempts")
            ).fetchone()
        )["n"] == 0


async def test_policy_change_and_wrong_target_deny_final_permit(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    body = json.dumps({"group_id": 999, "message": []}).encode()
    await expect(
        "INVALID_WIRE_PAYLOAD",
        d.permit(
            lease,
            claim,
            action="send_group_msg",
            wire_body=body,
            wire_digest=hashlib.sha256(body).hexdigest(),
        ),
    )
    await d.store.revoke_policy(lease, 1)
    await expect("POLICY_CHANGED", permit(d, lease, claim))
    await d.release(lease, claim, delay_seconds=0)
    assert await d.claim(lease, "w") is None
    assert (await d.list(lease))[0]["state"] == "CANCELLED"


async def test_same_account_reconnect_rebases_pending_but_not_unknown(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    await reconnect_binding(d.store, lease)
    await expect("STALE_BINDING", permit(d, lease, claim))
    await d.release(lease, claim, delay_seconds=0)
    newer = await d.claim(lease, "w")
    assert newer.binding_epoch == 3 and newer.route["binding_epoch"] == 2
    p = await permit(d, lease, newer)
    assert p.binding_epoch == 3 and p.connection_epoch == 2
    await d.complete(p, "UNKNOWN")
    await reconnect_binding(d.store, lease)
    assert await d.claim(lease, "w") is None
    assert await d.recover(lease) == {"unknown": 0, "requeued": 0}


async def test_host_replacement_keeps_unknown_and_accepts_original_late_receipt(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    p = await permit(d, lease, claim)
    await d.store.release_host(lease)
    newer = await d.store.acquire_host("host", "inc-b", ttl_seconds=120)
    await reconnect_binding(d.store, newer)
    assert await d.recover(newer) == {"unknown": 1, "requeued": 0}
    assert await d.claim(newer, "new") is None
    assert (await d.complete(p, "SENT", {"message_id": "late"}))["state"] == "SENT"
    assert (await d.list(newer))[0]["receipt"] == {"message_id": "late"}


async def test_manual_resolution_is_cas_idempotent_and_never_changes_body(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    first = await permit(d, lease, claim)
    unknown = await d.complete(first, "UNKNOWN")
    kwargs = dict(
        operation_id="operator-resolution",
        decision="NOT_SENT",
        expected_revision=unknown["revision"],
        reason="Operator verified platform history",
    )
    result = await d.reconcile(lease, claim.intent_id, 0, **kwargs)
    assert await d.reconcile(lease, claim.intent_id, 0, **kwargs) == result
    await expect(
        "IDEMPOTENCY_CONFLICT",
        d.reconcile(lease, claim.intent_id, 0, **{**kwargs, "reason": "changed"}),
    )
    retried = await d.claim(lease, "w")
    assert retried.part_operation_id == claim.part_operation_id
    second = await permit(d, lease, retried)
    assert second.attempt_id != first.attempt_id and second.wire_body == first.wire_body
    # Old receipt can settle the old attempt but cannot replace the newer part.
    await d.complete(first, "SENT", {"message_id": "old-late"})
    assert (await d.list(lease))[0]["state"] == "DISPATCHING"
    await d.complete(second, "UNKNOWN")
    current = (await d.list(lease))[0]
    skipped = await d.reconcile(
        lease,
        claim.intent_id,
        0,
        operation_id="skip",
        decision="SKIP",
        expected_revision=current["revision"],
        reason="Continue while retaining unknown original effect",
    )
    assert skipped["state"] == "SKIPPED"
    async with d.store.pool.connection() as conn:
        state = await (
            await conn.execute(
                "SELECT state FROM public.bot_send_attempts WHERE attempt_id=%s",
                (second.attempt_id,),
            )
        ).fetchone()
    assert state["state"] == "UNKNOWN"


async def test_account_network_slot_and_control_burst_are_bounded(delivery):
    d, lease = delivery
    for i in range(5):
        await enqueue(d, lease, lane="CONTROL", command=f"cmd-{i}")
    chat = await enqueue(d, lease)
    for i in range(4):
        control = await d.claim(lease, "w", lanes=("CONTROL",))
        p = await permit(d, lease, control)
        if i == 0:
            parallel = await d.claim(lease, "other", lanes=("CHAT",))
            await expect("DELIVERY_RATE_LIMITED", permit(d, lease, parallel))
            await d.release(lease, parallel, delay_seconds=0)
        await d.complete(p, "SENT", {"message_id": str(i)})
    fifth = await d.claim(lease, "control", lanes=("CONTROL",))
    await expect("DELIVERY_RATE_LIMITED", permit(d, lease, fifth))
    next_chat = await d.claim(lease, "w")
    assert next_chat.intent_id == chat["intent_id"]
    await d.complete(await permit(d, lease, next_chat), "SENT", {"message_id": "chat"})
    await d.complete(await permit(d, lease, fifth), "SENT", {"message_id": "control"})


async def test_worker_does_network_outside_transaction_and_retains_lost_receipt(delivery):
    d, lease = delivery
    await enqueue(d, lease, root="external-root")
    sends = []

    async def prepare(claim):
        prepared = prepare_send("group", claim.route["channel_id"], claim.payload)

        async def send(prepared, *, timeout):
            async with d.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    "SELECT * FROM public.bot_delivery_roots WHERE root_operation_id='external-root' FOR UPDATE NOWAIT"
                )
                row = await (
                    await conn.execute("SELECT state FROM public.bot_send_parts")
                ).fetchone()
                assert row["state"] == "DISPATCHING"
            sends.append(prepared.wire_digest)
            raise ConnectionError("Platform accepted; response socket was lost")

        return PreparedDelivery(prepared, send, {"driver": "onebot-v11:1"})

    worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
    assert await worker.run_once()
    assert not await worker.run_once()
    assert len(sends) == 1 and (await d.list(lease))[0]["state"] == "UNKNOWN"


async def test_control_slot_survives_slow_chat_preparation(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    preparing = asyncio.Event()
    unblock = asyncio.Event()
    control_sent = asyncio.Event()

    async def prepare(claim):
        if claim.payload[0]["text"] == "hello":
            preparing.set()
            await unblock.wait()
        prepared = prepare_send("group", claim.route["channel_id"], claim.payload)

        async def send(prepared, *, timeout):
            control_sent.set()
            return SendResult("SENT", "1")

        return PreparedDelivery(prepared, send, {"driver": "onebot-v11:1"})

    worker = DeliveryWorker(d, lease, prepare, parallelism=2, interval_seconds=0)
    task = asyncio.create_task(worker.run_forever())
    try:
        await asyncio.wait_for(preparing.wait(), 3)
        await enqueue(d, lease, lane="CONTROL", text="status")
        await asyncio.wait_for(control_sent.wait(), 3)
        assert not unblock.is_set()
    finally:
        unblock.set()
        await worker.close()
        await task


async def test_cancellation_after_transport_start_is_unknown(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    started = asyncio.Event()

    async def prepare(claim):
        async def send(prepared, *, timeout):
            started.set()
            await asyncio.Event().wait()

        return PreparedDelivery(
            prepare_send("group", "20000", claim.payload), send, {"driver": "onebot-v11:1"}
        )

    worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
    task = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(started.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await d.list(lease))[0]["state"] == "UNKNOWN"


async def test_adapter_generation_is_checked_before_binding_permit(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    await d.store.register_generation(
        lease,
        "adapter.onebot",
        "gen",
        "package",
        "config",
        "worker-inc",
        config={},
        instance_id="instance",
    )
    await d.store.mark_ready(lease, "gen")
    op = await d.store.begin_operation(lease, "adapter.onebot", "install-adapter", "plan", "gen", 0)
    await d.store.freeze(lease, op["operation_id"], op["op_epoch"])
    await d.store.publish(lease, op["operation_id"], op["op_epoch"])
    ref = {
        "component_id": "adapter.onebot",
        "generation_id": "gen",
        "route_epoch": 1,
        "worker_incarnation": "worker-inc",
        "policy_revision": 1,
    }
    ref = await codec_receipt(d, lease, claim)
    async with d.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_component_routes SET admission=false,control_admission=false")
    await expect("STALE_ADAPTER", permit(d, lease, claim, executor_ref=ref))
    async with d.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_component_routes SET admission=true,control_admission=true")
    assert (await permit(d, lease, claim, executor_ref=ref)).intent_id == claim.intent_id


async def test_transport_handoff_is_consumed_once_and_deadline_is_final(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    p = await permit(d, lease, claim)
    await d.mark_started(p)
    await expect("DELIVERY_ALREADY_STARTED", d.mark_started(p))
    async with d.store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.RaiseException):
            await conn.execute("UPDATE public.bot_send_attempts SET started_at=clock_timestamp()")
    await d.complete(p, "SENT")
    await enqueue(d, lease, sequence=2)
    claim = await d.claim(lease, "w")
    expired = await permit(d, lease, claim, timeout_seconds=0.1)
    await asyncio.sleep(0.15)
    await expect("DELIVERY_PERMIT_EXPIRED", d.mark_started(expired))
    assert (await d.recover(lease))["unknown"] == 1


async def test_final_claim_deadline_and_reconnect_fence_cannot_be_forged(delivery):
    d, lease = delivery
    await enqueue(d, lease, root="held")
    claim = await d.claim(lease, "w", ttl_seconds=0.2)
    async with d.store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT * FROM public.bot_delivery_roots WHERE root_operation_id='held' FOR UPDATE"
        )
        task = asyncio.create_task(permit(d, lease, claim))
        await asyncio.sleep(0.25)
    await expect("STALE_DELIVERY_OWNER", task)
    assert (await d.recover(lease))["requeued"] == 1
    claim = await d.claim(lease, "new")
    await reconnect_binding(d.store, lease)
    await expect(
        "STALE_DELIVERY_OWNER",
        permit(d, lease, replace(claim, binding_epoch=3, connection_epoch=2)),
    )


async def test_reconciliation_joins_outer_authority_transaction(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    claim = await d.claim(lease, "w")
    unknown = await d.complete(await permit(d, lease, claim), "UNKNOWN")
    with pytest.raises(RuntimeError, match="rollback"):
        async with d.store.pool.connection() as conn, conn.transaction():
            await d.store._host(conn, lease)
            await d.reconcile_in(
                conn,
                lease,
                claim.intent_id,
                0,
                operation_id="atomic-resolution",
                decision="SKIP",
                expected_revision=unknown["revision"],
                reason="Authorized test",
            )
            raise RuntimeError("rollback authority transaction")
    assert (await d.list(lease))[0]["state"] == "UNKNOWN"
    async with d.store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_delivery_resolutions")
            ).fetchone()
        )["n"] == 0


async def test_known_not_sent_is_terminal_without_automatic_retry(delivery):
    d, lease = delivery
    await enqueue(d, lease)
    sends = 0

    async def prepare(claim):
        async def send(prepared, *, timeout):
            nonlocal sends
            sends += 1
            return SendResult("NOT_SENT", error_code="ONEBOT_REJECTED")

        return PreparedDelivery(
            prepare_send("group", "20000", claim.payload), send, {"driver": "onebot-v11:1"}
        )

    worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
    assert await worker.run_once()
    assert not await worker.run_once()
    assert sends == 1 and (await d.list(lease))[0]["state"] == "FAILED"


async def test_confirmed_echo_matches_fixed_account_group_and_control_origin(delivery):
    d, lease = delivery
    await enqueue(d, lease, lane="CONTROL")
    claim = await d.claim(lease, "w")
    await d.complete(await permit(d, lease, claim), "SENT", {"message_id": "-123"})
    echo = await d.find_echo(lease, "binding-a", "-123", "group", "20000")
    assert echo["origin"] == "control_output" and echo["intent_id"] == claim.intent_id
    assert await d.find_echo(lease, "binding-a", "-123", "group", "30000") is None
    assert await d.find_echo(lease, "binding-a", "-123", "private", "20000") is None
    assert await d.find_echo(lease, "binding-a", "missing", "group", "20000") is None
    await d.store.set_maintenance(lease, 1, False)
    assert await d.find_echo(lease, "binding-a", "-123", "group", "20000") == echo
    await expect(
        "BOT_SCOPE_MISMATCH",
        d.find_echo(replace(lease, bot_id="other"), "binding-a", "-123", "group", "20000"),
    )


async def test_control_output_permit_survives_pause_but_respects_reload_freeze(delivery):
    d, lease = delivery
    await d.store.set_maintenance(lease, 1, False)
    await d.store.register_generation(
        lease,
        "adapter.onebot",
        "codec",
        "package",
        "config",
        "worker-codec",
        config={},
        instance_id="codec-instance",
    )
    await d.store.mark_ready(lease, "codec")
    op = await d.store.begin_operation(lease, "adapter.onebot", "install-codec", "plan", "codec", 0)
    await d.store.freeze(lease, op["operation_id"], op["op_epoch"])
    await d.store.publish(lease, op["operation_id"], op["op_epoch"])
    await enqueue(d, lease, lane="CONTROL")
    claim = await d.claim(lease, "w")
    ref = {
        "component_id": "adapter.onebot",
        "generation_id": "codec",
        "route_epoch": 1,
        "worker_incarnation": "worker-codec",
        "policy_revision": 1,
        "admission_lane": "CONTROL",
    }
    ref = await codec_receipt(d, lease, claim)
    async with d.store.pool.connection() as conn:
        route_row = await (
            await conn.execute(
                "SELECT * FROM public.bot_component_routes WHERE component_id='adapter.onebot'"
            )
        ).fetchone()
    assert not route_row["admission"] and route_row["control_admission"]
    sent = await permit(d, lease, claim, executor_ref=ref)
    await d.complete(sent, "SENT", {"message_id": "control"})
    await enqueue(d, lease, lane="CONTROL", sequence=2)
    claim = await d.claim(lease, "w")
    await d.store.register_generation(
        lease,
        "adapter.onebot",
        "next-codec",
        "package2",
        "config",
        "worker-next",
        config={},
        instance_id="next-instance",
    )
    await d.store.mark_ready(lease, "next-codec")
    reload_op = await d.store.begin_operation(
        lease, "adapter.onebot", "reload-codec", "plan2", "next-codec", 1
    )
    await d.store.freeze(lease, reload_op["operation_id"], reload_op["op_epoch"])
    await expect("STALE_ADAPTER", permit(d, lease, claim, executor_ref=ref))


async def test_offline_prefix_cannot_hide_another_binding_beyond_candidate_limit(delivery):
    d, lease = delivery
    for index in range(130):
        await enqueue(d, lease, group=str(20000 + index))
    async with d.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_bindings SET state='OFFLINE'")
    await create_online_binding(d.store, lease, "binding-b", "10001", name="Other")
    other = await d.enqueue(
        lease,
        operation_id="other-binding",
        route={**route("30000"), "binding_id": "binding-b", "self_id": "10001"},
        parts=[[{"type": "text", "text": "live"}]],
        conversation_id="live-conversation",
        response_seq=1,
    )
    claim = await d.claim(lease, "w")
    assert claim.intent_id == other["intent_id"]


async def test_bad_canonical_payload_is_a_visible_terminal_failure(delivery):
    d, lease = delivery
    await enqueue(d, lease, parts=[[{"type": "not-a-segment"}]])

    async def prepare(claim):
        return PreparedDelivery(prepare_send("group", "20000", claim.payload), None, {})

    worker = DeliveryWorker(d, lease, prepare, interval_seconds=0)
    assert await worker.run_once()
    assert not await worker.run_once()
    assert (await d.list(lease))[0]["state"] == "FAILED"


@pytest.mark.parametrize("lost_stage", ["permit", "complete"])
async def test_database_ack_loss_does_not_replay_effect(delivery, monkeypatch, lost_stage):
    d, lease = delivery
    await enqueue(d, lease)
    sends = 0
    original = getattr(d, lost_stage)
    once = True

    async def acknowledge_lost(*args, **kwargs):
        nonlocal once
        result = await original(*args, **kwargs)
        if once:
            once = False
            raise ConnectionError("Database commit acknowledgement lost")
        return result

    monkeypatch.setattr(d, lost_stage, acknowledge_lost)

    async def prepare(claim):
        async def send(prepared, *, timeout):
            nonlocal sends
            sends += 1
            return SendResult("SENT", "123")

        return PreparedDelivery(
            prepare_send("group", "20000", claim.payload), send, {"driver": "onebot-v11:1"}
        )

    worker = DeliveryWorker(d, lease, prepare, send_timeout=0.1, interval_seconds=0)
    assert await worker.run_once()
    assert not await worker.run_once()
    await asyncio.sleep(0.15)
    await d.recover(lease)
    assert sends == (1 if lost_stage == "complete" else 0)
    assert (await d.list(lease))[0]["state"] == ("SENT" if lost_stage == "complete" else "UNKNOWN")


async def test_unexpected_internal_task_failure_terminates_supervisor(delivery, monkeypatch):
    d, lease = delivery

    async def failed_claim(*args, **kwargs):
        raise RuntimeError("Injected store failure")

    def failed_error_sink(code):
        raise RuntimeError("Injected monitoring failure")

    async def unused_prepare(claim):
        raise AssertionError("No call can be claimed")

    monkeypatch.setattr(d, "claim", failed_claim)
    worker = DeliveryWorker(d, lease, unused_prepare, on_error=failed_error_sink)
    await asyncio.wait_for(worker.run_forever(), 3)
    assert worker.failure == "DELIVERY_TASK_FAILED" and not worker._tasks


async def test_enqueue_freezes_proposal_before_waiting_for_binding_authority(delivery, monkeypatch):
    d, lease = delivery
    entered = asyncio.Event()
    original = d.bindings._guard

    async def guarded(*args, **kwargs):
        entered.set()
        return await original(*args, **kwargs)

    monkeypatch.setattr(d.bindings, "_guard", guarded)
    parts = [[{"type": "text", "text": "original proposal"}]]
    async with d.store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT * FROM public.bot_bindings WHERE binding_id='binding-a' FOR UPDATE"
        )
        task = asyncio.create_task(enqueue(d, lease, parts=parts))
        await asyncio.wait_for(entered.wait(), 2)
        parts[0][0]["text"] = "mutated while blocked"
    await task
    claim = await d.claim(lease, "worker")
    assert claim.payload == [[{"type": "text", "text": "original proposal"}]][0]
    await expect("IDEMPOTENCY_CONFLICT", enqueue(d, lease, parts=parts))
