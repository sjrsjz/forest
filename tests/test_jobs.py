"""Real PostgreSQL jobs, signed authority and the actual conversation ingress."""

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from test_messages import event, owner
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.storage.jobs import JobStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def jobs(environment):
    env = environment
    async with env.store.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,"
            "host_incarnation,host_owner_epoch,enabled,receive_enabled,state) "
            "VALUES (%s,'binding-a','onebot-v11','1000','test','registry',%s,%s,true,true,'ONLINE')",
            (env.store.bot_id, env.lease.incarnation, env.lease.owner_epoch),
        )
    messages = MessageStore(env.store)
    receipt = await messages.ingest(env.lease, "binding-a", event(1), decision="NO_TRIGGER")
    yield env, JobStore(messages), receipt["conversation_id"]


async def request(env, store, action, method, payload, *, operation=None):
    op = operation or str(uuid4())
    ticket = await env.ticket(
        "job." + action, op, payload, plan_digest=payload.get("plan_digest", "")
    )
    return await getattr(store, method)(ticket, op, payload)


async def configure(env, store, conversation_id, *, job_id="reminder", revision=0, **changes):
    async with env.store.pool.connection() as conn:
        now = await env.store._now(conn)
    values = {
        "name": "每日提醒",
        "conversation_id": conversation_id,
        "prompt": "提醒大家完成任务",
        "schedule": {
            "kind": "interval",
            "interval_seconds": 60,
            "start_at": (now + timedelta(seconds=60)).isoformat(),
        },
        "delegation_expires_at": (now + timedelta(hours=1)).isoformat(),
        **changes,
    }
    plan = await request(
        env,
        store,
        "plan",
        "plan",
        {"job_id": job_id, "expected_revision": revision, "values": values},
    )
    return await request(
        env,
        store,
        "apply",
        "apply",
        {
            "job_id": job_id,
            "expected_revision": revision,
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
        },
    )


async def manual(env, store, *, job_id="reminder", revision=1, operation=None):
    return await request(
        env,
        store,
        "run",
        "run_now",
        {"job_id": job_id, "expected_revision": revision},
        operation=operation,
    )


async def proof(env, store, claim):
    target = store.authorization_request(claim)
    grant = claim.delegation
    return await env.authority.issue_delegation(
        subject_id=grant["subject_id"],
        bot_id=env.store.bot_id,
        operation_id=target["operation_id"],
        payload=target["payload"],
        policy_revision=grant["policy_revision"],
        delegation_id=grant["delegation_id"],
        expected_account_revision=grant["account_revision"],
        expected_key_revision=grant["key_revision"],
    )


async def enqueue(env, store, claim=None):
    claim = claim or await store.claim_pending(env.lease, "dispatcher")
    assert claim
    return await store.dispatch(env.lease, claim, authorization=await proof(env, store, claim))


async def error(code, awaitable):
    with pytest.raises(ForestError) as caught:
        await awaitable
    assert caught.value.code == code
    return caught.value


async def test_signed_management_cas_and_internal_ingress_are_idempotent(jobs):
    env, store, conversation = jobs
    config = await configure(env, store, conversation)
    assert config["delegation"]["tool_capabilities"] == []
    first = await manual(env, store, operation="manual-once")
    assert first == await manual(env, store, operation="manual-once")
    await error("REVISION_CONFLICT", manual(env, store, revision=0))
    claim = await store.claim_pending(env.lease, "host-worker")
    receipt = await enqueue(env, store, claim)
    repeated = await enqueue(env, store, claim)
    assert repeated["event_id"] == receipt["event_id"] and repeated["replayed"]
    async with env.store.pool.connection() as conn:
        rows = await (
            await conn.execute("SELECT * FROM public.bot_message_inbox WHERE origin='job'")
        ).fetchall()
    assert len(rows) == 1 and rows[0]["author"]["identity_origin"] == "host_job"
    actor = await owner(store.messages, env.lease, conversation)
    selected = await store.messages.select_input(actor, "select")
    assert selected["trigger_ids"] == [receipt["trigger_id"]]
    view = await store.messages.context(actor, run_id=selected["run_id"])
    unit = view["units"][-1]
    assert unit["source"]["source_kind"] == "SYSTEM" and unit["source"]["platform"] == "forest"
    assert unit["source"]["channel_id"] == "group-1"
    result = await request(env, store, "read", "read", {})
    assert result["occurrences"][0]["run_id"] == selected["run_id"]
    assert not result["occurrences"][0]["replay_allowed"]


async def test_no_overlap_persists_until_trigger_terminal_not_just_enqueue(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    await manual(env, store)
    first = await enqueue(env, store)
    assert await store.claim_pending(env.lease, "second") is None
    actor = await owner(store.messages, env.lease, conversation)
    selected = await store.messages.select_input(actor, "run")
    assert await store.claim_pending(env.lease, "second") is None
    await store.messages.end_run(
        actor, run_id=selected["run_id"], reason="test-done", status="FAILED"
    )
    assert await store.reconcile(env.lease) == 1
    claim = await store.claim_pending(env.lease, "second")
    assert claim and claim.occurrence_id != first["occurrence_id"]


async def test_missing_proof_is_recoverable_and_old_claim_cannot_dispatch(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    first = await store.claim_pending(env.lease, "first", ttl_seconds=0.08)
    await error("JOB_AUTHORIZATION_REQUIRED", store.dispatch(env.lease, first))
    await asyncio.sleep(0.1)
    second = await JobStore(store.messages).claim_pending(env.lease, "restarted")
    assert second.occurrence_id == first.occurrence_id and second.lease_epoch > first.lease_epoch
    await error("STALE_JOB", enqueue(env, store, first))
    await store.defer(env.lease, second, "AUTHORITY_UNAVAILABLE", delay_seconds=1)
    assert await store.claim_pending(env.lease, "third") is None


async def test_cancel_scopes_preserve_unrelated_triggers_and_cas_active_runs(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    receipt = await enqueue(env, store)
    await store.messages.ingest(env.lease, "binding-a", event(2), decision="TRIGGER")
    result = await request(
        env,
        store,
        "cancel",
        "cancel",
        {
            "job_id": "reminder",
            "expected_revision": 1,
            "scope": "pending",
            "reason": "stop pending",
        },
    )
    assert result["enabled"] and result["cancelled_occurrences"] == [receipt["occurrence_id"]]
    await manual(env, store, revision=2)
    active = await enqueue(env, store)
    actor = await owner(store.messages, env.lease, conversation)
    selected = await store.messages.select_input(actor, "shared-run")
    assert len(selected["trigger_ids"]) == 2 and active["trigger_id"] in selected["trigger_ids"]
    exc = await error(
        "JOB_RUN_ALREADY_ACTIVE",
        request(
            env,
            store,
            "cancel",
            "cancel",
            {
                "job_id": "reminder",
                "expected_revision": 2,
                "scope": "all",
                "reason": "cancel",
            },
        ),
    )
    assert exc.details["runs"][0]["run_id"] == selected["run_id"]


async def test_local_revocation_cancels_pending_trigger_before_actor_claim(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    receipt = await enqueue(env, store)
    async with env.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_web_issuers SET enabled=false")
    actor = await owner(store.messages, env.lease, conversation)
    selected = await store.messages.select_input(actor, "after-revoke")
    assert selected["kind"] == "SILENT_DRAIN"
    async with env.store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT status FROM public.bot_job_occurrences WHERE occurrence_id=%s",
                (receipt["occurrence_id"],),
            )
        ).fetchone()
    assert row["status"] == "CANCELLED"


async def test_never_enqueued_failure_replay_keeps_occurrence_identity(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    first = await manual(env, store)
    claim = await store.claim_pending(env.lease, "worker")
    await store.fail(env.lease, claim, "JOB_TARGET_INVALID")
    view = await request(env, store, "read", "read", {})
    row = view["occurrences"][0]
    assert row["replay_allowed"]
    replayed = await request(
        env,
        store,
        "replay",
        "replay",
        {"occurrence_id": row["occurrence_id"], "expected_revision": row["revision"]},
    )
    assert replayed["occurrence_id"] == first["occurrence_id"]
    await enqueue(env, store)
    view = await request(env, store, "read", "read", {})
    row = view["occurrences"][0]
    await error(
        "JOB_REPLAY_CONFLICT",
        request(
            env,
            store,
            "replay",
            "replay",
            {"occurrence_id": row["occurrence_id"], "expected_revision": row["revision"]},
        ),
    )


async def test_dispatch_final_time_fence_after_business_lock(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    claim = await store.claim_pending(env.lease, "worker", ttl_seconds=0.25)
    authorization = await proof(env, store, claim)
    async with env.store.pool.connection() as blocker, blocker.transaction():
        await blocker.execute("SELECT * FROM public.bot_jobs WHERE job_id='reminder' FOR UPDATE")
        task = asyncio.create_task(store.dispatch(env.lease, claim, authorization=authorization))
        await asyncio.sleep(0.35)
    await error("STALE_JOB", task)
    async with env.store.pool.connection() as conn:
        count = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_message_inbox WHERE origin='job'"
            )
        ).fetchone()
    assert count["n"] == 0


async def test_expansion_coalesces_outage_once_and_restart_keeps_ids(jobs):
    env, store, conversation = jobs
    async with env.store.pool.connection() as conn:
        now = await env.store._now(conn)
    start = now - timedelta(days=30)
    await configure(
        env,
        store,
        conversation,
        schedule={"kind": "interval", "interval_seconds": 1, "start_at": start.isoformat()},
        misfire="catch_up",
        misfire_grace_seconds=0,
        catch_up_limit=3,
    )
    async with env.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_jobs SET next_due_at=%s", (start,))
    assert await store.expand_due(env.lease) == 3
    async with env.store.pool.connection() as conn:
        before = await (
            await conn.execute(
                "SELECT occurrence_id,scheduled_for FROM public.bot_job_occurrences ORDER BY scheduled_for"
            )
        ).fetchall()
        expansion = await (await conn.execute("SELECT * FROM public.bot_job_expansions")).fetchone()
    assert len(expansion["selected_occurrences"]) == 3 and expansion["misfired"]
    assert (before[-1]["scheduled_for"] - before[0]["scheduled_for"]).total_seconds() == 2
    restart = JobStore(store.messages)
    claimed = await restart.claim_pending(env.lease, "new-dispatcher")
    assert claimed.occurrence_id == before[0]["occurrence_id"]
    async with env.store.pool.connection() as conn:
        after = await (
            await conn.execute(
                "SELECT occurrence_id,scheduled_for FROM public.bot_job_occurrences ORDER BY scheduled_for"
            )
        ).fetchall()
    assert before == after


async def test_definition_replacement_revokes_pending_and_does_not_move_group(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    old = await store.claim_pending(env.lease, "old-owner")
    await configure(env, store, conversation, revision=1, prompt="new definition")
    await error("STALE_JOB", enqueue(env, store, old))
    other = await store.messages.ingest(
        env.lease, "binding-a", event(2, group="other"), decision="NO_TRIGGER"
    )
    await error("JOB_TARGET_IMMUTABLE", configure(env, store, other["conversation_id"], revision=2))


async def test_revoked_pending_job_does_not_poison_scanner(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    async with env.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_runtime_state SET policy_revision=policy_revision+1")
    await configure(env, store, conversation, job_id="current")
    await manual(env, store, job_id="current")
    claim = await store.claim_pending(env.lease, "worker")
    assert claim.job_id == "current"
    assert await store.reconcile(env.lease) == 1


async def test_current_account_revision_required_until_durable_enqueue(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    accepted = await enqueue(env, store)
    async with env.deployment.pool.connection() as conn:
        await conn.execute(
            "UPDATE web_accounts SET revision=revision+1 WHERE id=%s", (env.subject,)
        )
    # Account revision blocks fresh proof, but does not revoke already accepted
    # conversation input. Cancellation is a separate explicit management action.
    actor = await owner(store.messages, env.lease, conversation)
    selected = await store.messages.select_input(actor, "accepted-run")
    assert accepted["trigger_id"] in selected["trigger_ids"]
    await store.messages.end_run(actor, run_id=selected["run_id"], reason="done")
    await store.reconcile(env.lease)
    async with env.store.pool.connection() as conn:
        definition = await store._definition(conn, "reminder", 1)
        head = await store._head(conn, "reminder")
        await store._emit(
            conn,
            head,
            definition,
            await env.store._now(conn),
            origin="MANUAL",
            identity="queued-before-revision",
        )
    pending = await store.claim_pending(env.lease, "new")
    await error("JOB_DELEGATION_REVOKED", proof(env, store, pending))


async def test_read_and_mutation_scope_is_bound_to_shell_conversation(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    other = await store.messages.ingest(
        env.lease, "binding-a", event(2, group="other"), decision="NO_TRIGGER"
    )
    await configure(env, store, other["conversation_id"], job_id="other")
    from forest.storage.control import BotControl

    class ScopedControl(BotControl):
        async def _guard(self, *args, **kwargs):
            return {**await super()._guard(*args, **kwargs), "source_conversation_id": conversation}

    scoped = JobStore(store.messages, control=ScopedControl(env.store))
    view = await request(env, scoped, "read", "read", {})
    assert [row["job_id"] for row in view["jobs"]] == ["reminder"]
    async with env.store.pool.connection() as conn:
        await error(
            "JOB_SCOPE_DENIED",
            JobStore.authorize_shell_scope_in(
                conn, env.store.bot_id, conversation, {"job_id": "other"}
            ),
        )


async def test_host_incarnation_takeover_preserves_occurrence_and_fences_old_owner(jobs):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    old = await store.claim_pending(env.lease, "old-worker")
    old_lease = env.lease
    await env.store.release_host(old_lease)
    env.lease = await env.store.acquire_host(
        "replacement", "replacement-incarnation", ttl_seconds=120
    )
    new = await JobStore(store.messages).claim_pending(env.lease, "new-worker")
    assert new.occurrence_id == old.occurrence_id
    assert new.host_incarnation != old.host_incarnation and new.lease_epoch > old.lease_epoch
    await error("STALE_HOST", store.defer(old_lease, old, "late", delay_seconds=1))
    await error("STALE_JOB", store.defer(env.lease, old, "wrong-owner", delay_seconds=1))


async def test_dispatch_expiry_after_ingress_audit_rolls_back_every_fact(jobs, monkeypatch):
    env, store, conversation = jobs
    await configure(env, store, conversation)
    await manual(env, store)
    claim = await store.claim_pending(env.lease, "worker", ttl_seconds=0.2)
    authorization = await proof(env, store, claim)
    original = env.store._event

    async def delayed(conn, kind, operation_id, payload):
        await original(conn, kind, operation_id, payload)
        if kind == "job.trigger_enqueued":
            await asyncio.sleep(0.3)

    monkeypatch.setattr(env.store, "_event", delayed)
    await error("STALE_JOB", store.dispatch(env.lease, claim, authorization=authorization))
    async with env.store.pool.connection() as conn:
        inbox = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_message_inbox WHERE origin='job'"
            )
        ).fetchone()
        trigger = await (
            await conn.execute("SELECT count(*) AS n FROM public.bot_conversation_triggers")
        ).fetchone()
        cursor = await (
            await conn.execute("SELECT next_ingress_seq FROM public.bot_conversations")
        ).fetchone()
        occurrence = await (
            await conn.execute("SELECT status,event_id FROM public.bot_job_occurrences")
        ).fetchone()
    assert inbox["n"] == trigger["n"] == 0 and cursor["next_ingress_seq"] == 2
    assert occurrence == {"status": "CLAIMED", "event_id": None}


@pytest.mark.parametrize("was_claimed", [False, True])
async def test_cancelled_job_context_preserves_source_but_removes_instruction(jobs, was_claimed):
    env, store, conversation = jobs
    await configure(env, store, conversation, prompt="OLD CANCELLED TASK INSTRUCTION")
    await manual(env, store)
    receipt = await enqueue(env, store)
    actor = await owner(store.messages, env.lease, conversation)
    if was_claimed:
        prior = await store.messages.select_input(actor, "first-run")
        await store.messages.end_run(
            actor, run_id=prior["run_id"], reason="cancelled", status="CANCELLED"
        )
    else:
        await request(
            env,
            store,
            "cancel",
            "cancel",
            {
                "job_id": "reminder",
                "expected_revision": 1,
                "scope": "pending",
                "reason": "cancelled",
            },
        )
    await store.messages.ingest(
        env.lease, "binding-a", event(2, text="new user question"), decision="TRIGGER"
    )
    selected = await store.messages.select_input(actor, "next-user-run")
    context = await store.messages.context(actor, run_id=selected["run_id"])
    unit = next(u for u in context["units"] if u["event_id"] == receipt["event_id"])
    assert unit["source"]["task_status"] == "CANCELLED"
    assert unit["source"]["occurrence_id"] == receipt["occurrence_id"]
    assert unit["author"]["identity_origin"] == "host_job"
    assert unit["segments"] == [{"type": "text", "text": "该计划任务已取消；此处仅保留历史记录。"}]
    async with env.store.pool.connection() as conn:
        raw = await (
            await conn.execute(
                "SELECT segments FROM public.bot_message_inbox WHERE event_id=%s",
                (receipt["event_id"],),
            )
        ).fetchone()
    assert raw["segments"] == [{"type": "text", "text": "OLD CANCELLED TASK INSTRUCTION"}]
