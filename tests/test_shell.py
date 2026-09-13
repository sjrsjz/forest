"""Independent group control, current allowlist checks and output fencing."""

import asyncio
import hashlib
from uuid import uuid4

import pytest
from binding_seed import _ticket_factory
from test_group_scheduler import row, until
from test_messages import event, receive
from test_messages import messages as messages

from forest.group_scheduler import GroupScheduler
from forest.shell import ShellDispatcher
from forest.storage.delivery import DeliveryStore

pytestmark = pytest.mark.postgres


async def allow(repository, *users, binding_id="binding-a"):
    async with repository.store.pool.connection() as conn:
        owner = await (await conn.execute("SELECT * FROM public.bot_host_lease WHERE bot_id=%s",
            (repository.store.bot_id,))).fetchone()
    lease = repository.store._host_value(owner)
    ticket = await _ticket_factory(repository.store, lease)
    bindings = repository.bindings
    current = await bindings.current(lease, binding_id)
    secret = await bindings.credentials(lease, binding_id)
    operation = "allow-plan-" + uuid4().hex
    payload = {"binding_id": binding_id, "expected_revision": current["revision"],
               "values": {**current["config"], "admin_user_ids": list(users)},
               "credentials_changed": False, "secret_digest": hashlib.sha256(secret.encode()).hexdigest()}
    plan = await bindings.plan(await ticket("binding.plan", operation, payload), operation, payload, secret)
    operation = "allow-apply-" + uuid4().hex
    payload = {"binding_id": binding_id, **{key: plan[key] for key in ("expected_revision", "plan_id", "plan_digest")}}
    await bindings.apply(await ticket("binding.apply", operation, payload, plan_digest=payload["plan_digest"]), operation, payload)
    await bindings.activate(lease, binding_id, current["revision"] + 1, enabled=True,
        client_incarnation="allow-client-" + uuid4().hex, expected_binding_epoch=current["binding_epoch"])


def dispatcher(repository, lease):
    return ShellDispatcher(repository, repository.bindings, DeliveryStore(repository.store),
                           lease, poll_interval=0.02, command_timeout=1)


async def completed(repository, number):
    return (await row(repository, "SELECT count(*) AS n FROM public.bot_shell_requests WHERE status='COMPLETED'"))["n"] == number


async def test_unauthorized_platform_admin_and_anonymous_cannot_use_control_allowlist(messages):
    repository, lease, _ = messages
    await allow(repository, "trusted", "anonymous-shared")
    forged = event(1, user="not-listed", text="#forest status")
    forged["author"]["role"] = "owner"
    anonymous = event(2, user="anonymous-shared", text="#forest status")
    anonymous["author"]["anonymous"] = {"id": "anon-id", "name": "trusted"}
    await repository.ingest(lease, "binding-a", forged)
    await repository.ingest(lease, "binding-a", anonymous)
    await receive(repository, lease, 3, user="trusted", text="#forest help")
    instance = dispatcher(repository, lease)
    try:
        await instance.start()
        await until(lambda: completed(repository, 3))
        async with repository.store.pool.connection() as conn:
            outcomes = await (await conn.execute("SELECT outcome FROM public.bot_shell_requests ORDER BY created_at")).fetchall()
        assert [r["outcome"]["code"] for r in outcomes] == ["DENIED", "DENIED", "HELP"]
        assert not instance.failure
    finally:
        await instance.close()


async def test_control_status_works_while_chat_execution_slot_is_occupied_and_bot_paused(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    await receive(repository, lease, 1, trigger=True)
    callback_started = asyncio.Event()

    async def execute(actor, selected, context):
        callback_started.set()
        await asyncio.Event().wait()

    chats = GroupScheduler(repository, lease=lease, execute=execute, max_concurrency=1, poll_interval=0.02)
    controls = dispatcher(repository, lease)
    try:
        await chats.start()
        await asyncio.wait_for(callback_started.wait(), 5)
        await repository.store.set_maintenance(lease, 1, data_admission=False)
        await receive(repository, lease, 2, text="#forest status")
        await controls.start()
        await until(lambda: completed(repository, 1))
        result = await row(repository, "SELECT outcome,delivery FROM public.bot_shell_requests")
        assert result["outcome"]["code"] == "STATUS" and result["outcome"]["paused"]
        assert result["outcome"]["maintenance_epoch"] == 2
        assert result["delivery"]["send_lane"] == "CONTROL"
        assert (await row(repository, "SELECT count(*) AS n FROM public.bot_conversation_replies"))["n"] == 0
    finally:
        await asyncio.gather(chats.close(), controls.close())


async def test_unknown_sensitive_command_produces_only_fixed_safe_reply(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    await receive(repository, lease, 1, text="#forest confirm token SUPER-SECRET")
    instance = dispatcher(repository, lease)
    try:
        await instance.start()
        await until(lambda: completed(repository, 1))
        outcome = await row(repository, "SELECT * FROM public.bot_shell_requests")
        assert outcome["outcome"]["code"] == "UNSUPPORTED"
        async with repository.store.pool.connection() as conn:
            parts = await (await conn.execute("SELECT payload FROM public.bot_send_parts")).fetchall()
        assert "SUPER-SECRET" not in str(parts) and "SUPER-SECRET" not in str(outcome)
    finally:
        await instance.close()


async def test_allowlist_replacement_before_complete_prevents_stale_status_receipt(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    command = await receive(repository, lease, 1, text="#forest status")
    before_complete, resume = asyncio.Event(), asyncio.Event()
    original = repository.complete_control
    first = True

    async def held_complete(*args, **kwargs):
        nonlocal first
        if first:
            first = False
            before_complete.set()
            await resume.wait()
        return await original(*args, **kwargs)

    repository.complete_control = held_complete
    instance = dispatcher(repository, lease)
    try:
        await instance.start()
        await asyncio.wait_for(before_complete.wait(), 5)
        await allow(repository)
        resume.set()
        await until(lambda: completed(repository, 1))
        result = await row(repository, "SELECT outcome FROM public.bot_shell_requests WHERE command_id=%s", (command["command_id"],))
        assert result["outcome"]["code"] == "DENIED"
        assert (await row(repository, "SELECT count(*) AS n FROM public.bot_send_intents"))["n"] == 1
    finally:
        resume.set()
        await instance.close()


async def test_one_control_failure_does_not_block_another_binding(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    await allow(repository, "user-1", binding_id="binding-b")
    failed = await receive(repository, lease, 1, text="#forest status")
    second = await repository.ingest(lease, "binding-b", event(2, self_id="2000", text="#forest help"))
    original = repository.complete_control

    async def partial_failure(lease, claim, *args, **kwargs):
        if claim["command_id"] == failed["command_id"]:
            raise RuntimeError("simulated one binding storage failure")
        return await original(lease, claim, *args, **kwargs)

    repository.complete_control = partial_failure
    instance = dispatcher(repository, lease)
    try:
        await instance.start()
        await until(lambda: completed(repository, 1))
        result = await row(repository, "SELECT status FROM public.bot_shell_requests WHERE command_id=%s", (second["command_id"],))
        assert result["status"] == "COMPLETED"
    finally:
        await instance.close()
