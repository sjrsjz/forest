"""Conversation Actor ordering and atomic business-command result publication."""

import asyncio
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from binding_seed import _ticket_factory, create_online_binding
from psycopg import sql
from test_messages import event

from forest.group_scheduler import BusinessCommandExecution, GroupScheduler
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.command_policies import CommandPolicyStore
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


def command_source(tmp_path: Path) -> Path:
    root = tmp_path / "actor-command-source"
    module = root / "src" / "actor_command"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname="actor-command"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import Consumed, Pass, Reply, command, plugin

@plugin(id="actor.command", version="0.1.0")
class ActorCommand:
    @command("/actor", "command.actor", id="actor", args="tail_text")
    async def actor(self, event, ctx):
        if event["argument_text"] == "consume":
            return Consumed()
        if event["argument_text"] == "pass":
            return Pass()
        return Reply([[{"type": "text", "text": "command:" + event["argument_text"]}]])
''',
        encoding="utf-8",
    )
    return root


async def set_policy(messages, lease):
    policies = CommandPolicyStore(messages.store)
    tickets = await _ticket_factory(messages.store)
    proposal = {
        "binding_id": "binding-a",
        "command_id": "actor",
        "expected_revision": 0,
        "values": {"mode": "EVERYONE", "user_ids": []},
    }
    plan = await policies.plan(
        await tickets("command.policy.plan", "actor-policy-plan", proposal),
        "actor-policy-plan",
        proposal,
    )
    apply = {
        "binding_id": "binding-a",
        "command_id": "actor",
        "expected_revision": 0,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    await policies.apply(
        await tickets(
            "command.policy.apply",
            "actor-policy-apply",
            apply,
            plan_digest=plan["plan_digest"],
        ),
        "actor-policy-apply",
        apply,
    )


@pytest_asyncio.fixture
async def command_runtime(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private test PostgreSQL instance")
    name = "forest_command_actor_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-a")
    kernel = Kernel(store)
    try:
        await store.open()
        await store.migrate()
        await kernel.start()
        await kernel.install(PackageCatalog(tmp_path / "catalog").build(command_source(tmp_path)))
        await create_online_binding(store, kernel.lease, "binding-a", "1000", name="binding-a")
        messages = MessageStore(store, BindingStore(store))
        await set_policy(messages, kernel.lease)
        yield messages, kernel
    finally:
        await kernel.close()
        await store.close()
        assert name.startswith("forest_command_actor_") and len(name) == 53
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def ingest(messages, kernel, number, text, *, group="group-1", trigger=False):
    return await messages.ingest(
        kernel.lease,
        "binding-a",
        event(number, group=group, user="20001", text=text),
        decision="TRIGGER" if trigger else "NO_TRIGGER",
    )


async def actor(messages, kernel, conversation_id, name="actor-a"):
    return await messages.acquire_actor(kernel.lease, conversation_id, name, ttl_seconds=120)


async def execute_selected(kernel, selected):
    operation_id = "business-command-call:" + selected["request"]["request_id"]
    receipt = await kernel.invoke_business_command_with_receipt(
        selected["request"],
        selected["event"],
        operation_id=operation_id,
        input_parent_ids=tuple(selected["input_parent_ids"]),
    )
    return operation_id, receipt


async def finalize(messages, actor_lease, selected, operation_id, receipt, delivery=None):
    return await messages.finalize_business_command(
        actor_lease,
        "business-command-final:" + selected["request"]["request_id"],
        request_id=selected["request"]["request_id"],
        call_operation_id=operation_id,
        result=receipt.value,
        result_payload_id=receipt.result_payload_id,
        result_payload_digest=receipt.result_digest,
        delivery=delivery or DeliveryStore(messages.store),
    )


async def until(check, *, timeout=8):
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.02)


async def test_command_is_an_ingress_barrier_and_publishes_one_atomic_chat_result(
    command_runtime,
):
    messages, kernel = command_runtime
    first = await ingest(messages, kernel, 1, "ordinary")
    command = await ingest(messages, kernel, 2, "/actor hello")
    await ingest(messages, kernel, 3, "trigger", trigger=True)
    owner = await actor(messages, kernel, first["conversation_id"])

    selected = await messages.select_input(owner, "select-command")
    assert selected["status"] == "BUSINESS_COMMAND"
    assert selected["selected_input_end_seq"] == 2
    assert selected["event"]["author"]["external_user_id"] == "20001"
    assert selected["event"]["argument_text"] == "hello"
    assert (await messages.select_input(owner, "select-command"))["request"]["request_id"] == command[
        "business_request_id"
    ]

    operation_id, receipt = await execute_selected(kernel, selected)
    result = await finalize(messages, owner, selected, operation_id, receipt)
    assert result["disposition"] == "REPLY"
    assert result["response_seq"] == 1
    replay = await finalize(messages, owner, selected, operation_id, receipt)
    assert replay["result_id"] == result["result_id"]

    context = await messages.context(owner)
    assert [unit["kind"] for unit in context["units"]] == [
        "INPUT",
        "INPUT",
        "COMMAND_RESULT",
    ]
    assert context["units"][1]["author"]["external_user_id"] == "20001"
    assert context["units"][2]["context_role"] == "assistant"
    assert context["units"][2]["reply_parts"] == [
        [{"type": "text", "text": "command:hello"}]
    ]
    next_work = await messages.select_input(owner, "select-agent")
    assert next_work["status"] == "INPUT"
    assert next_work["trigger_cut_seq"] == 3

    async with messages.store.pool.connection() as conn:
        request = await (
            await conn.execute(
                "SELECT status FROM public.bot_business_command_requests "
                "WHERE bot_id=%s AND request_id=%s",
                (messages.store.bot_id, command["business_request_id"]),
            )
        ).fetchone()
        intents = await (
            await conn.execute(
                "SELECT send_lane,response_seq FROM public.bot_send_intents WHERE bot_id=%s",
                (messages.store.bot_id,),
            )
        ).fetchall()
    assert request["status"] == "COMPLETED"
    assert intents == [{"send_lane": "CHAT", "response_seq": 1}]


@pytest.mark.parametrize(
    ("argument", "disposition", "request_status"),
    [("consume", "CONSUMED", "COMPLETED"), ("pass", "PASS", "FAILED")],
)
async def test_non_reply_dispositions_are_contextual_and_do_not_consume_reply_sequence(
    command_runtime, argument, disposition, request_status
):
    messages, kernel = command_runtime
    accepted = await ingest(messages, kernel, 1, "/actor " + argument)
    owner = await actor(messages, kernel, accepted["conversation_id"])
    selected = await messages.select_input(owner, "select-" + argument)
    operation_id, receipt = await execute_selected(kernel, selected)
    result = await finalize(messages, owner, selected, operation_id, receipt)
    assert result["disposition"] == disposition
    assert result["response_seq"] is None
    context = await messages.context(owner)
    assert context["units"][-1]["kind"] == "COMMAND_RESULT"
    async with messages.store.pool.connection() as conn:
        request = await (
            await conn.execute(
                "SELECT status,terminal_reason FROM public.bot_business_command_requests "
                "WHERE bot_id=%s AND request_id=%s",
                (messages.store.bot_id, accepted["business_request_id"]),
            )
        ).fetchone()
        conversation = await (
            await conn.execute(
                "SELECT next_response_seq FROM public.bot_conversations "
                "WHERE bot_id=%s AND conversation_id=%s",
                (messages.store.bot_id, accepted["conversation_id"]),
            )
        ).fetchone()
    assert request["status"] == request_status
    assert request["terminal_reason"] == (
        "COMMAND_NOT_HANDLED" if disposition == "PASS" else None
    )
    assert conversation["next_response_seq"] == 1


async def test_active_agent_run_is_not_interrupted_by_a_later_command(command_runtime):
    messages, kernel = command_runtime
    trigger = await ingest(messages, kernel, 1, "trigger", trigger=True)
    owner = await actor(messages, kernel, trigger["conversation_id"])
    run = await messages.select_input(owner, "select-run")
    await ingest(messages, kernel, 2, "/actor after")
    boundary = await messages.select_input(owner, "agent-safe-point", mode="SAFE_POINT")
    assert boundary["status"] == "CONTINUE"
    assert boundary["run_id"] == run["run_id"]
    resumed = await messages.select_input(owner, "while-agent-active")
    assert resumed["status"] == "RESUME"
    assert resumed["run_id"] == run["run_id"]
    await messages.finalize_reply(
        owner,
        "finish-agent",
        run_id=run["run_id"],
        expected_input_revision=run["input_revision"],
        reply_parts=[[{"type": "text", "text": "agent reply"}]],
        delivery=DeliveryStore(messages.store),
    )
    command = await messages.select_input(owner, "after-agent")
    assert command["status"] == "BUSINESS_COMMAND"
    assert command["event"]["argument_text"] == "after"


async def test_claim_recovery_and_delivery_failure_keep_exactly_one_result(command_runtime):
    messages, kernel = command_runtime
    accepted = await ingest(messages, kernel, 1, "/actor retry")
    first_actor = await actor(messages, kernel, accepted["conversation_id"])
    selected = await messages.select_input(first_actor, "first-claim")
    await messages.release_actor(first_actor)
    successor = await actor(messages, kernel, accepted["conversation_id"], "actor-b")
    recovered = await messages.select_input(successor, "recover-claim")
    assert recovered["replayed"]
    assert recovered["input_batch_id"] == selected["input_batch_id"]
    operation_id, receipt = await execute_selected(kernel, recovered)

    class FailAfterIntent(DeliveryStore):
        async def enqueue_in(self, *args, **kwargs):
            await super().enqueue_in(*args, **kwargs)
            raise RuntimeError("lost before command commit")

    with pytest.raises(RuntimeError, match="lost before command commit"):
        await finalize(
            messages,
            successor,
            recovered,
            operation_id,
            receipt,
            FailAfterIntent(messages.store),
        )
    async with messages.store.pool.connection() as conn:
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_business_command_results) AS results,"
                "(SELECT count(*) FROM public.bot_send_intents) AS intents"
            )
        ).fetchone()
    assert counts == {"results": 0, "intents": 0}
    committed = await finalize(messages, successor, recovered, operation_id, receipt)
    assert committed["response_seq"] == 1


async def test_scheduler_serializes_each_group_but_runs_command_groups_in_parallel(
    command_runtime,
):
    messages, kernel = command_runtime
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release = asyncio.Event()

    async def execute(actor_lease, selected, context):
        assert context is None
        group = selected["event"]["conversation"]["channel_id"]
        if group == "group-1":
            first_started.set()
            await release.wait()
        else:
            second_started.set()
        operation_id, receipt = await execute_selected(kernel, selected)
        return BusinessCommandExecution(
            operation_id,
            receipt.value,
            receipt.result_payload_id,
            receipt.result_digest,
        )

    await ingest(messages, kernel, 1, "/actor first", group="group-1")
    await ingest(messages, kernel, 1, "/actor second", group="group-2")
    scheduler = GroupScheduler(
        messages,
        lease=lambda: kernel.lease,
        execute=execute,
        poll_interval=0.02,
        max_concurrency=2,
    )
    try:
        await scheduler.start()
        await asyncio.wait_for(first_started.wait(), 5)
        await asyncio.wait_for(second_started.wait(), 5)
        release.set()

        async def completed():
            async with messages.store.pool.connection() as conn:
                count = await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM public.bot_business_command_results"
                    )
                ).fetchone()
            return count["n"] == 2

        await until(completed)
        assert not scheduler.failure
        async with messages.store.pool.connection() as conn:
            intents = await (
                await conn.execute(
                    "SELECT conversation_id,response_seq FROM public.bot_send_intents "
                    "ORDER BY conversation_id"
                )
            ).fetchall()
        assert len(intents) == 2
        assert [intent["response_seq"] for intent in intents] == [1, 1]
    finally:
        release.set()
        await scheduler.close()
