"""Live PostgreSQL Job supervision, deployment authority and group queue integration."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from test_group_scheduler import replies, row, scheduler, until
from test_messages import receive
from test_web_authority import environment as environment

from forest.job_scheduler import JobDispatcher
from forest.schedules import JobSettings, ScheduleSpec
from forest.storage.jobs import JobStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


async def configured(env):
    async with env.store.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,"
            "registry_operation_id,host_incarnation,host_owner_epoch,enabled,receive_enabled,state) "
            "VALUES (%s,'binding-a','onebot-v11','1000','test','test',%s,%s,true,true,'ONLINE')",
            (env.store.bot_id, env.lease.incarnation, env.lease.owner_epoch),
        )
    messages = MessageStore(env.store)
    incoming = await receive(messages, env.lease, "first", trigger=True)
    now = datetime.now(UTC)
    settings = JobSettings(
        name="test schedule", conversation_id=incoming["conversation_id"], prompt="scheduled request",
        schedule=ScheduleSpec(kind="interval", interval_seconds=60, start_at=(now + timedelta(hours=1)).isoformat()),
        delegation_expires_at=(now + timedelta(days=1)).isoformat(),
    ).model_dump()
    jobs = JobStore(messages)
    payload = {"job_id": "test-job", "expected_revision": 0, "values": settings}
    plan = await jobs.plan(await env.ticket("job.plan", "plan", payload), "plan", payload)
    payload = {"job_id": "test-job", "expected_revision": 0,
               "plan_id": plan["plan_id"], "plan_digest": plan["plan_digest"]}
    await jobs.apply(await env.ticket("job.apply", "apply", payload, plan_digest=plan["plan_digest"]), "apply", payload)

    async def authorize(claim):
        request, grant = jobs.authorization_request(claim), claim.delegation
        return await env.authority.issue_delegation(
            subject_id=grant["subject_id"], bot_id=env.store.bot_id,
            operation_id=request["operation_id"], payload=request["payload"],
            policy_revision=grant["policy_revision"], delegation_id=grant["delegation_id"],
            expected_account_revision=grant["account_revision"], expected_key_revision=grant["key_revision"],
        )

    return messages, jobs, authorize


async def run_now(env, jobs):
    payload = {"job_id": "test-job", "expected_revision": 1}
    return await jobs.run_now(await env.ticket("job.run", "manual", payload), "manual", payload)


async def test_two_dispatchers_enqueue_once_and_busy_group_obeys_the_job_trigger_cut(environment):
    env = environment
    messages, jobs, authorize = await configured(env)
    entered, release = asyncio.Event(), asyncio.Event()
    inputs, active, maximum = [], 0, 0

    async def execute(actor, selected, context):
        nonlocal active, maximum
        active += 1
        maximum = max(active, maximum)
        try:
            inputs.append((selected, context))
            if len(inputs) == 1:
                entered.set()
                await release.wait()
            return [[{"type": "text", "text": "reply"}]]
        finally:
            active -= 1

    conversations = scheduler(messages, env.lease, execute)
    workers = [JobDispatcher(jobs, env.lease, authorize=authorize, on_change=conversations.notify,
                             poll_interval=0.03) for _ in range(2)]
    try:
        await conversations.start()
        await asyncio.wait_for(entered.wait(), 5)
        occurrence = await run_now(env, jobs)
        for worker in workers:
            await worker.start()

        async def enqueued():
            current = await row(messages, "SELECT status FROM public.bot_job_occurrences WHERE occurrence_id=%s", (occurrence["occurrence_id"],))
            return current["status"] == "ENQUEUED"

        await until(enqueued)
        await receive(messages, env.lease, "tail", text="later ordinary tail")
        release.set()
        conversations.notify()
        await until(lambda: replies(messages, 2))
        assert maximum == 1 and len(inputs) == 2
        assert inputs[1][0]["end_seq"] == 2
        assert "later ordinary tail" not in str(inputs[1][1])
        assert "host_job" in str(inputs[1][1])
        assert (await row(messages, "SELECT count(*) AS n FROM public.bot_message_inbox WHERE author->>'identity_origin'='host_job'"))["n"] == 1

        async def finished():
            return (await row(messages, "SELECT status FROM public.bot_job_occurrences"))["status"] == "SUCCEEDED"

        await until(finished)
        assert all(not worker.failure and not worker._jobs for worker in workers)
    finally:
        release.set()
        await asyncio.gather(*(worker.close() for worker in workers))
        await conversations.close()


async def test_revoked_deployment_account_defers_without_hot_retry_or_ingress(environment):
    env = environment
    messages, jobs, authorize = await configured(env)
    await run_now(env, jobs)
    async with env.deployment.pool.connection() as conn:
        await conn.execute("UPDATE web_accounts SET disabled=true WHERE id=%s", (env.subject,))
    dispatcher = JobDispatcher(jobs, env.lease, authorize=authorize, poll_interval=0.02)
    try:
        await dispatcher.start()

        async def deferred():
            current = await row(messages, "SELECT error_code,status FROM public.bot_job_occurrences")
            return current["status"] == "QUEUED" and current["error_code"] == "FORBIDDEN"

        await until(deferred)
        await asyncio.sleep(0.12)
        current = await row(messages, "SELECT attempt_count,event_id,next_attempt_at>clock_timestamp() AS deferred FROM public.bot_job_occurrences")
        assert current["attempt_count"] == 1 and current["event_id"] is None and current["deferred"]
        assert not dispatcher.failure
    finally:
        await dispatcher.close()
    assert not dispatcher._jobs and dispatcher.task.done()
