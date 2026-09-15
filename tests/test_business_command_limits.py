"""Database-serialized business-command capacity and principal rate limits."""

import asyncio

import pytest
from binding_seed import _ticket_factory
from test_business_command_ingress import install_command, set_policy
from test_messages import event
from test_messages import messages as messages

from forest.errors import ForestError
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.command_limits import DEFAULT_COMMAND_LIMITS, CommandLimitStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


async def set_limits(repository, values, *, revision=0):
    tickets = await _ticket_factory(repository.store)
    limits = CommandLimitStore(repository.store)
    plan_payload = {"expected_revision": revision, "values": values}
    plan_op = f"command-limit-plan-{revision}"
    plan = await limits.plan(
        await tickets("command.limit.plan", plan_op, plan_payload),
        plan_op,
        plan_payload,
    )
    apply_payload = {
        "expected_revision": revision,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    apply_op = f"command-limit-apply-{revision}"
    await limits.apply(
        await tickets(
            "command.limit.apply",
            apply_op,
            apply_payload,
            plan_digest=plan["plan_digest"],
        ),
        apply_op,
        apply_payload,
    )


async def prepared(messages, values):
    repository, lease, _ = messages
    await install_command(repository, lease)
    await set_policy(repository, lease, {"mode": "EVERYONE", "user_ids": []})
    await set_limits(repository, values)
    return repository, lease


async def requests(repository):
    async with repository.store.pool.connection() as conn:
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_business_command_requests "
                "WHERE bot_id=%s ORDER BY event_id",
                (repository.store.bot_id,),
            )
        ).fetchall()


async def test_concurrent_groups_cannot_pierce_principal_queue_limit(messages):
    values = {
        **DEFAULT_COMMAND_LIMITS,
        "max_pending_bot": 10,
        "max_pending_binding": 10,
        "max_pending_principal": 1,
        "principal_burst": 10,
        "principal_refill_seconds": 3600,
    }
    repository, lease = await prepared(messages, values)
    second_store = BotStore(messages[2], repository.store.bot_id)
    await second_store.open()
    second = MessageStore(second_store, BindingStore(second_store))
    try:
        accepted = await asyncio.gather(
            *(
                (repository if number % 2 else second).ingest(
                    lease,
                    "binding-a",
                    event(number, group=f"group-{number}", user="20001", text="/hello work"),
                    decision="TRIGGER",
                )
                for number in range(1, 9)
            )
        )
    finally:
        await second_store.close()
    rows = await requests(repository)
    assert len(rows) == 8
    assert sum(row["status"] == "PENDING" for row in rows) == 1
    assert all(row["admission_limit_revision"] == 1 for row in rows)
    assert {
        row["terminal_reason"] for row in rows if row["status"] == "FAILED"
    } == {"BUSINESS_COMMAND_PRINCIPAL_QUEUE_FULL"}
    assert all(row["trigger_decision"] == "BUSINESS_COMMAND" for row in accepted)
    async with repository.store.pool.connection() as conn:
        counts = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM public.bot_message_inbox WHERE bot_id=%s) AS inbox,"
                "(SELECT count(*) FROM public.bot_memory_source_occurrences WHERE bot_id=%s) AS sources,"
                "(SELECT count(*) FROM public.bot_conversation_triggers WHERE bot_id=%s) AS triggers,"
                "(SELECT count(*) FROM public.bot_conversation_notices WHERE bot_id=%s "
                "AND kind='BUSINESS_COMMAND_FAILED') AS notices",
                (repository.store.bot_id,) * 4,
            )
        ).fetchone()
    assert counts == {"inbox": 8, "sources": 8, "triggers": 0, "notices": 7}


async def test_bot_queue_cap_and_principal_bucket_are_durable(messages):
    values = {
        **DEFAULT_COMMAND_LIMITS,
        "max_pending_bot": 2,
        "max_pending_binding": 2,
        "max_pending_principal": 2,
        "principal_burst": 1,
        "principal_refill_seconds": 3600,
    }
    repository, lease = await prepared(messages, values)
    first = await repository.ingest(
        lease, "binding-a", event(1, group="group-a", user="20001", text="/hello one"),
        decision="NO_TRIGGER",
    )
    async with repository.store.pool.connection() as conn:
        before = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_principal_buckets WHERE bot_id=%s",
                (repository.store.bot_id,),
            )
        ).fetchone()
    duplicate = await repository.ingest(
        lease, "binding-a", event(1, group="group-a", user="20001", text="/hello one"),
        decision="TRIGGER",
    )
    async with repository.store.pool.connection() as conn:
        after = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_principal_buckets WHERE bot_id=%s",
                (repository.store.bot_id,),
            )
        ).fetchone()
    assert duplicate["duplicate"] is True
    assert first["business_request_id"] == duplicate["business_request_id"]
    assert before == after

    await repository.ingest(
        lease, "binding-a", event(2, group="group-b", user="20001", text="/hello two"),
        decision="TRIGGER",
    )
    async with repository.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_command_principal_buckets "
            "SET updated_at=updated_at-interval '3601 seconds' WHERE bot_id=%s",
            (repository.store.bot_id,),
        )
    await repository.ingest(
        lease, "binding-a", event(3, group="group-c", user="20001", text="/hello three"),
        decision="TRIGGER",
    )
    await repository.ingest(
        lease, "binding-a", event(4, group="group-d", user="20003", text="/hello four"),
        decision="TRIGGER",
    )
    rows = await requests(repository)
    by_event = {row["event_id"]: row for row in rows}
    assert by_event[first["event_id"]]["status"] == "PENDING"
    assert [row["terminal_reason"] for row in rows].count("BUSINESS_COMMAND_RATE_LIMITED") == 1
    assert [row["terminal_reason"] for row in rows].count("BUSINESS_COMMAND_BOT_QUEUE_FULL") == 1
    assert sum(row["status"] == "PENDING" for row in rows) == 2


async def test_oversized_arguments_fail_before_consuming_a_rate_token(messages):
    values = {
        **DEFAULT_COMMAND_LIMITS,
        "max_argument_bytes": 4,
        "max_pending_bot": 10,
        "max_pending_binding": 10,
        "max_pending_principal": 10,
        "principal_burst": 1,
        "principal_refill_seconds": 3600,
    }
    repository, lease = await prepared(messages, values)
    oversized = await repository.ingest(
        lease,
        "binding-a",
        event(1, group="large", user="20001", text="/hello 12345"),
        decision="TRIGGER",
    )
    small = await repository.ingest(
        lease,
        "binding-a",
        event(2, group="small", user="20001", text="/hello 1234"),
        decision="TRIGGER",
    )
    rows = {row["event_id"]: row for row in await requests(repository)}
    assert rows[oversized["event_id"]]["terminal_reason"] == (
        "BUSINESS_COMMAND_ARGUMENT_TOO_LARGE"
    )
    assert rows[small["event_id"]]["status"] == "PENDING"


async def test_concurrent_limit_applies_have_one_revision_winner(messages):
    repository, _, _ = messages
    tickets = await _ticket_factory(repository.store)
    limits = CommandLimitStore(repository.store)
    plans = []
    for index, pending in enumerate((20, 30)):
        values = {
            **DEFAULT_COMMAND_LIMITS,
            "max_pending_bot": pending,
            "max_pending_binding": 10,
        }
        payload = {"expected_revision": 0, "values": values}
        operation = f"racing-limit-plan-{index}"
        plans.append(
            await limits.plan(
                await tickets("command.limit.plan", operation, payload),
                operation,
                payload,
            )
        )

    async def apply(index, plan):
        payload = {
            "expected_revision": 0,
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
        }
        operation = f"racing-limit-apply-{index}"
        return await limits.apply(
            await tickets(
                "command.limit.apply",
                operation,
                payload,
                plan_digest=plan["plan_digest"],
            ),
            operation,
            payload,
        )

    outcomes = await asyncio.gather(
        *(apply(index, plan) for index, plan in enumerate(plans)),
        return_exceptions=True,
    )
    assert sum(isinstance(value, dict) for value in outcomes) == 1
    failures = [value for value in outcomes if isinstance(value, ForestError)]
    assert len(failures) == 1 and failures[0].code == "REVISION_CONFLICT"
