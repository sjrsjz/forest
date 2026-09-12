"""Real durable mailbox scheduling with controllable host callback failures."""

import asyncio

import pytest
from test_messages import event, receive
from test_messages import messages as messages

from forest.errors import ForestError
from forest.group_scheduler import GroupScheduler

pytestmark = pytest.mark.postgres


async def until(check, *, timeout=8):
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.02)


async def row(repository, query, params=()):
    async with repository.store.pool.connection() as conn:
        return await (await conn.execute(query, params)).fetchone()


async def replies(repository, expected):
    return (await row(repository, "SELECT count(*) AS n FROM public.bot_conversation_replies"))["n"] == expected


def scheduler(repository, lease, execute, **kwargs):
    return GroupScheduler(repository, lease=lease, execute=execute, poll_interval=0.02, **kwargs)


async def test_silent_input_is_drained_without_execution_or_send(messages):
    repository, lease, _ = messages
    executed = []

    async def execute(actor, selected, context):
        executed.append(selected)
        return [[{"type": "text", "text": "unexpected"}]]

    for n in range(20):
        await receive(repository, lease, n)
    instance = scheduler(repository, lease, execute)
    try:
        await instance.start()

        async def drained():
            current = await row(repository, "SELECT context_applied_seq,processed_mailbox_revision,mailbox_revision FROM public.bot_conversations")
            return current["context_applied_seq"] == 20 and current["processed_mailbox_revision"] == current["mailbox_revision"]

        await until(drained)
        assert not executed
        assert await replies(repository, 0)
        assert not instance.failure
    finally:
        await instance.close()


async def test_same_group_serial_other_group_parallel_and_queued_tail_stays_out(messages):
    repository, lease, _ = messages
    first_started, other_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    invocations = []

    async def execute(actor, selected, context):
        group = context["conversation"]["channel_id"]
        invocations.append((group, selected["run_id"], [u["ingress_seq"] for u in context["units"] if u["kind"] == "INPUT"]))
        assert all("source" in u and u["author"]["principal_ref"] for u in context["units"] if u["kind"] == "INPUT")
        if group == "group-1" and not first_started.is_set():
            first_started.set()
            await release.wait()
        elif group == "group-2":
            other_started.set()
        return [[{"type": "text", "text": "reply " + group}]]

    await receive(repository, lease, 1, trigger=True)
    instance = scheduler(repository, lease, execute, max_concurrency=2)
    try:
        await instance.start()
        await asyncio.wait_for(first_started.wait(), 5)
        await receive(repository, lease, 2, trigger=True)
        await receive(repository, lease, 3)
        await receive(repository, lease, 1, trigger=True, group="group-2")
        instance.notify()
        await asyncio.wait_for(other_started.wait(), 5)
        assert [entry[0] for entry in invocations].count("group-1") == 1
        release.set()
        await until(lambda: replies(repository, 3))
        group1 = [entry for entry in invocations if entry[0] == "group-1"]
        assert len(group1) == 2
        assert group1[0][2] == [1]
        assert group1[1][2] == [1, 2]
        assert group1[0][1] != group1[1][1]
    finally:
        release.set()
        await instance.close()


async def test_busy_group_yields_to_already_queued_groups_after_one_run(messages):
    repository, lease, _ = messages
    started, release = asyncio.Event(), asyncio.Event()
    order = []

    async def execute(actor, selected, context):
        group = context["conversation"]["channel_id"]
        order.append(group)
        if len(order) == 1:
            started.set()
            await release.wait()
        return [[{"type": "text", "text": "done"}]]

    for group in ("a", "b", "c"):
        await receive(repository, lease, 1, trigger=True, group=group)
    instance = scheduler(repository, lease, execute, max_concurrency=1)
    try:
        await instance.start()
        await asyncio.wait_for(started.wait(), 5)
        await receive(repository, lease, 2, trigger=True, group=order[0])
        release.set()
        await until(lambda: replies(repository, 4))
        assert len(set(order[:3])) == 3
        assert order[-1] == order[0]
    finally:
        release.set()
        await instance.close()


async def test_scan_cursor_reaches_conversations_beyond_unresolved_head_page(messages):
    repository, lease, _ = messages
    identities = []
    for n in range(8):
        first = await repository.ingest(lease, "binding-a", event(1, group=str(n)))
        await receive(repository, lease, 2, trigger=True, group=str(n))
        identities.append((first["conversation_id"], str(n), first["event_id"]))
    _, last_group, first_event = sorted(identities)[-1]
    conversation_id = sorted(identities)[-1][0]
    await repository.classify(lease, conversation_id, first_event, "NO_TRIGGER")
    called = []

    async def execute(actor, selected, context):
        called.append(context["conversation"]["channel_id"])
        return [[{"type": "text", "text": "reachable"}]]

    instance = scheduler(repository, lease, execute, scan_page_size=2, max_concurrency=2)
    try:
        await instance.start()
        await until(lambda: replies(repository, 1))
        assert called == [last_group]
    finally:
        await instance.close()


async def test_actor_renewal_keeps_long_callback_valid(messages):
    repository, lease, _ = messages
    started, release = asyncio.Event(), asyncio.Event()

    async def execute(actor, selected, context):
        started.set()
        await release.wait()
        return [[{"type": "text", "text": "after renewals"}]]

    await receive(repository, lease, 1, trigger=True)
    instance = scheduler(repository, lease, execute, actor_ttl=0.3)
    try:
        await instance.start()
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.sleep(0.8)
        current = await row(repository, "SELECT actor_expires_at>clock_timestamp() AS live FROM public.bot_conversations")
        assert current["live"]
        release.set()
        await until(lambda: replies(repository, 1))
    finally:
        release.set()
        await instance.close()


async def test_close_cancels_callback_and_restart_resumes_same_run_without_duplicate_input(messages):
    repository, lease, _ = messages
    started, cancelled = asyncio.Event(), asyncio.Event()
    original = []

    async def unfinished(actor, selected, context):
        original.append(selected["run_id"])
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    await receive(repository, lease, 1, trigger=True)
    first = scheduler(repository, lease, unfinished)
    await first.start()
    await asyncio.wait_for(started.wait(), 5)
    await first.close()
    assert cancelled.is_set()
    assert await replies(repository, 0)
    assert (await row(repository, "SELECT status FROM public.bot_conversation_triggers"))["status"] == "CLAIMED"

    async def recovered(actor, selected, context):
        assert selected["status"] == "RESUME"
        assert selected["run_id"] == original[0]
        assert len(context["units"]) == 1
        return [[{"type": "text", "text": "recovered"}]]

    second = scheduler(repository, lease, recovered)
    try:
        await second.start()
        await until(lambda: replies(repository, 1))
        assert (await row(repository, "SELECT count(*) AS n FROM public.bot_context_input_batches"))["n"] == 1
    finally:
        await second.close()


async def test_pause_cancels_local_work_and_new_epoch_resumes_without_waiting_old_ttl(messages):
    repository, lease, _ = messages
    started, cancelled = asyncio.Event(), asyncio.Event()
    calls = []

    async def execute(actor, selected, context):
        calls.append(actor.actor_epoch)
        if len(calls) == 1:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return [[{"type": "text", "text": "resumed"}]]

    await receive(repository, lease, 1, trigger=True)
    instance = scheduler(repository, lease, execute)
    try:
        await instance.start()
        await asyncio.wait_for(started.wait(), 5)
        await repository.store.set_maintenance(lease, 1, data_admission=False)
        instance.notify()
        await asyncio.wait_for(cancelled.wait(), 5)
        await receive(repository, lease, 2)
        assert await replies(repository, 0)
        await repository.store.set_maintenance(lease, 2, data_admission=True)
        instance.notify()
        await until(lambda: replies(repository, 1))
        assert calls[1] > calls[0]
    finally:
        await instance.close()


async def test_budget_failure_and_callback_failure_are_durable_terminal_claims(messages):
    repository, lease, _ = messages
    repository.context_token_budget = 512
    await receive(repository, lease, 1, trigger=True, text="oversized" * 1000)
    called = []

    async def execute(actor, selected, context):
        called.append(context)
        raise RuntimeError("private provider credential")

    instance = scheduler(repository, lease, execute)
    try:
        await instance.start()

        async def failed():
            return (await row(repository, "SELECT status FROM public.bot_conversation_triggers"))["status"] == "FAILED"

        await until(failed)
        assert called == []
        assert await replies(repository, 0)
        assert "CONTEXT_BUDGET_EXCEEDED" in instance.errors.values()
        repository.context_token_budget = 32768
        await receive(repository, lease, 2, trigger=True)
        instance.notify()

        async def failed_twice():
            return (await row(repository, "SELECT count(*) AS n FROM public.bot_conversation_triggers WHERE status='FAILED'"))["n"] == 2

        await until(failed_twice)
        assert len(called) == 1
        assert "private" not in str(instance.errors)
    finally:
        await instance.close()


async def test_shared_execution_semaphore_bounds_two_independent_schedulers(messages):
    repository, lease, _ = messages
    # Two schedulers stand in for different Bot supervisors sharing the same
    # host capacity. Actor fencing still excludes double same-group execution.
    active = 0
    maximum = 0
    seen = []

    async def execute(actor, selected, context):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        seen.append(selected["run_id"])
        try:
            await asyncio.sleep(0.08)
            return [[{"type": "text", "text": "bounded"}]]
        finally:
            active -= 1

    for group in ("one", "two", "three", "four"):
        await receive(repository, lease, 1, trigger=True, group=group)
    slots = asyncio.Semaphore(1)
    first = scheduler(repository, lease, execute, global_slots=slots, max_concurrency=2)
    second = scheduler(repository, lease, execute, global_slots=slots, max_concurrency=2)
    try:
        await asyncio.gather(first.start(), second.start())
        await until(lambda: replies(repository, 4))
        assert maximum == 1
        assert len(set(seen)) == 4 and len(seen) == 4
    finally:
        await asyncio.gather(first.close(), second.close())


async def test_health_detects_unexpected_scheduler_loop_stop(messages):
    repository, lease, _ = messages

    async def execute(actor, selected, context):
        return [[{"type": "text", "text": "unused"}]]

    instance = scheduler(repository, lease, execute)
    await instance.start()
    instance.task.cancel()
    await asyncio.gather(instance.task, return_exceptions=True)
    assert instance.failure.code == "SCHEDULER_STOPPED"
    await instance.close()
    with pytest.raises(ForestError, match="closed"):
        await instance.start()


async def test_uncooperative_callback_retains_capacity_and_cannot_publish_after_close(messages):
    repository, lease, _ = messages
    started, release = asyncio.Event(), asyncio.Event()

    async def stubborn(actor, selected, context):
        started.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue
        return [[{"type": "text", "text": "late discarded output"}]]

    await receive(repository, lease, 1, trigger=True)
    capacity = asyncio.Semaphore(1)
    instance = scheduler(repository, lease, stubborn, global_slots=capacity,
                         cancellation_timeout=0.05, actor_ttl=0.3)
    try:
        await instance.start()
        await asyncio.wait_for(started.wait(), 5)
        with pytest.raises(ForestError) as stopped:
            await instance.close()
        assert stopped.value.code == "SCHEDULER_CLEANUP_FAILED"
        assert instance.failure.code == "SCHEDULER_CLEANUP_FAILED"
        assert capacity.locked()
        assert await replies(repository, 0)
    finally:
        release.set()

        async def cleaned():
            return not instance._orphaned

        await until(cleaned)
        await instance.close()
    assert not capacity.locked()
    assert await replies(repository, 0)
    assert (await row(repository, "SELECT status FROM public.bot_conversation_triggers"))["status"] == "CLAIMED"
