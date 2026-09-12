"""Independent real PostgreSQL review of timer cuts and competing owners."""

import asyncio
from datetime import timedelta

import pytest
from test_jobs import configure, enqueue, error, manual, proof
from test_jobs import environment as environment
from test_jobs import jobs as jobs
from test_messages import event, owner

from forest.storage.jobs import JobStore

pytestmark = pytest.mark.postgres


async def test_expired_last_job_does_not_expand_real_trigger_cut_or_cancel_user(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation, max_overlap=2)
    await manual(env, store, operation="before-user")
    earlier = await enqueue(env, store)
    user = await store.messages.ingest(env.lease, "binding-a", event(2), decision="TRIGGER")
    await manual(env, store, operation="after-user")
    later = await enqueue(env, store)
    tail = await store.messages.ingest(env.lease, "binding-a", event(3), decision="NO_TRIGGER")
    async with env.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_web_issuers SET enabled=false")
    actor = await owner(store.messages, env.lease, conversation)
    selected = await store.messages.select_input(actor, "real-user-only")
    assert selected["selected_input_end_seq"] == user["ingress_seq"]
    assert len(selected["trigger_ids"]) == 1
    assert earlier["trigger_id"] not in selected["trigger_ids"]
    assert later["trigger_id"] not in selected["trigger_ids"]
    context = await store.messages.context(actor, run_id=selected["run_id"])
    assert max(u["ingress_seq"] for u in context["units"]) == user["ingress_seq"]
    assert all(u["event_id"] != tail["event_id"] for u in context["units"])
    async with env.store.pool.connection() as conn:
        triggers = await (
            await conn.execute(
                "SELECT event_id,status FROM public.bot_conversation_triggers ORDER BY ingress_seq"
            )
        ).fetchall()
    assert triggers == [
        {"event_id": earlier["event_id"], "status": "CANCELLED"},
        {"event_id": user["event_id"], "status": "CLAIMED"},
        {"event_id": later["event_id"], "status": "CANCELLED"},
    ]


async def test_two_scanners_only_one_owns_nonoverlapping_occurrence(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store, operation="one")
    await manual(env, store, operation="two")
    other = JobStore(store.messages)
    claimed = await asyncio.gather(
        store.claim_pending(env.lease, "scanner-one"), other.claim_pending(env.lease, "scanner-two")
    )
    assert sum(claim is not None for claim in claimed) == 1
    claim = next(claim for claim in claimed if claim)
    authorization = await proof(env, store, claim)
    receipts = await asyncio.gather(
        store.dispatch(env.lease, claim, authorization=authorization),
        other.dispatch(env.lease, claim, authorization=authorization),
    )
    assert receipts[0]["event_id"] == receipts[1]["event_id"]
    assert sum(receipt.get("replayed", False) for receipt in receipts) == 1
    assert await other.claim_pending(env.lease, "third") is None
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_message_inbox WHERE origin='job'"
            )
        ).fetchone()
    assert row["n"] == 1


async def test_expiry_after_trigger_claim_rolls_back_then_later_user_progresses(jobs, monkeypatch):
    env, store, conversation = jobs
    async with env.store.pool.connection() as conn:
        now = await env.store._now(conn)
    expiry = now + timedelta(seconds=2)
    await configure(
        env,
        store,
        conversation,
        schedule={"kind": "interval", "interval_seconds": 60, "start_at": now.isoformat()},
        delegation_expires_at=expiry.isoformat(),
    )
    await manual(env, store)
    job = await enqueue(env, store)
    user = await store.messages.ingest(env.lease, "binding-a", event(2), decision="TRIGGER")
    actor = await owner(store.messages, env.lease, conversation)
    final = store.messages._actor_final

    async def delayed_final(conn, actor, binding):
        await final(conn, actor, binding)
        await conn.execute(
            "SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM %s::timestamptz-clock_timestamp()))+0.04)",
            (expiry,),
        )

    monkeypatch.setattr(store.messages, "_actor_final", delayed_final)
    await error("JOB_DELEGATION_EXPIRED", store.messages.select_input(actor, "late-commit"))
    monkeypatch.setattr(store.messages, "_actor_final", final)
    async with env.store.pool.connection() as conn:
        rows = await (
            await conn.execute("SELECT * FROM public.bot_context_input_batches")
        ).fetchall()
    assert rows == []
    selected = await store.messages.select_input(actor, "retry-user")
    assert selected["selected_input_end_seq"] == user["ingress_seq"]
    assert len(selected["trigger_ids"]) == 1 and job["trigger_id"] not in selected["trigger_ids"]
