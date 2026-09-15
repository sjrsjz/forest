"""Database-serialized ordinary-message Handler capacity governance."""

import asyncio
from uuid import uuid4

import pytest
from test_kernel import bot_database as bot_database
from test_message_handler_chains import event as base_event
from test_message_handler_chains import prepared, register, set_policy

from forest.storage.bindings import BindingStore
from forest.storage.message_handler_limits import (
    DEFAULT_MESSAGE_HANDLER_LIMITS,
    MessageHandlerLimitStore,
)
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


def event(number, *, group=None, user="20001", text="ordinary group message"):
    value = base_event(number, user=user)
    value["channel_id"] = group or f"group-{number}"
    value["segments"] = [{"type": "text", "text": text}]
    return value


async def set_limits(store, ticket, values, *, revision=0):
    limits = MessageHandlerLimitStore(store)
    operation = "handler-limit-plan-" + uuid4().hex
    payload = {"expected_revision": revision, "values": values}
    plan = await limits.plan(
        await ticket("message.handler.limit.plan", operation, payload),
        operation,
        payload,
    )
    operation = "handler-limit-apply-" + uuid4().hex
    payload = {
        "expected_revision": revision,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    result = await limits.apply(
        await ticket(
            "message.handler.limit.apply",
            operation,
            payload,
            plan_digest=plan["plan_digest"],
        ),
        operation,
        payload,
    )
    return result


async def configured(bot_database, values):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "capacity", "gen-6", "ordinary", 10)
    await set_policy(
        store,
        ticket,
        "binding-a",
        "capacity",
        "ordinary",
        {"mode": "EVERYONE", "user_ids": []},
    )
    await set_limits(store, ticket, values)
    return store, lease, ticket, MessageStore(store, BindingStore(store))


async def test_concurrent_groups_cannot_pierce_principal_pending_limit(bot_database):
    values = {
        **DEFAULT_MESSAGE_HANDLER_LIMITS,
        "max_pending_bot": 10,
        "max_pending_binding": 10,
        "max_pending_principal": 1,
        "principal_burst": 10,
        "principal_refill_seconds": 3600,
    }
    store, lease, _, messages = await configured(bot_database, values)
    accepted = await asyncio.gather(
        *(
            messages.ingest(
                lease,
                "binding-a",
                event(number),
                decision="TRIGGER",
                rule_revision="binding-trigger-v1",
            )
            for number in range(1, 9)
        )
    )
    async with store.pool.connection() as conn:
        chains = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s",
                (store.bot_id,),
            )
        ).fetchall()
        counts = await (
            await conn.execute(
                "SELECT "
                "(SELECT count(*) FROM public.bot_message_inbox WHERE bot_id=%s) AS inbox,"
                "(SELECT count(*) FROM public.bot_memory_source_occurrences "
                " WHERE bot_id=%s) AS sources,"
                "(SELECT count(*) FROM public.bot_conversation_triggers "
                " WHERE bot_id=%s) AS triggers,"
                "(SELECT count(*) FROM public.bot_conversation_notices WHERE bot_id=%s "
                " AND kind='MESSAGE_HANDLER_REJECTED') AS notices",
                (store.bot_id,) * 4,
            )
        ).fetchone()
    assert sum(row["status"] == "PENDING" for row in chains) == 1
    assert {
        row["terminal_reason"] for row in chains if row["status"] == "FAILED"
    } == {"MESSAGE_HANDLER_PRINCIPAL_QUEUE_FULL"}
    assert all(row["admission_limit_revision"] == 1 for row in chains)
    assert sum(row["trigger_decision"] == "PENDING" for row in accepted) == 1
    assert sum(row["trigger_decision"] == "FAILED" for row in accepted) == 7
    assert counts == {"inbox": 8, "sources": 8, "triggers": 0, "notices": 7}


async def test_event_size_rejection_does_not_consume_token_and_duplicate_is_stable(
    bot_database,
):
    values = {
        **DEFAULT_MESSAGE_HANDLER_LIMITS,
        "max_event_bytes": 1,
        "principal_burst": 1,
        "principal_refill_seconds": 3600,
    }
    store, lease, _, messages = await configured(bot_database, values)
    first = await messages.ingest(
        lease, "binding-a", event(20), decision="TRIGGER", rule_revision="trigger-v1"
    )
    duplicate = await messages.ingest(
        lease, "binding-a", event(20), decision="TRIGGER", rule_revision="trigger-v1"
    )
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, first["event_id"]),
            )
        ).fetchone()
        buckets = (
            await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_message_handler_principal_buckets "
                    "WHERE bot_id=%s",
                    (store.bot_id,),
                )
            ).fetchone()
        )["n"]
    assert first["trigger_decision"] == "FAILED"
    assert duplicate["duplicate"] is True
    assert duplicate["message_handler_chain_id"] == first["message_handler_chain_id"]
    assert chain["status"] == "FAILED"
    assert chain["terminal_reason"] == "MESSAGE_HANDLER_EVENT_TOO_LARGE"
    assert chain["event_bytes"] > values["max_event_bytes"]
    assert buckets == 0


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {"max_pending_bot": 1, "max_pending_binding": 1},
            "MESSAGE_HANDLER_BOT_QUEUE_FULL",
        ),
        (
            {"max_pending_bot": 10, "max_pending_binding": 1},
            "MESSAGE_HANDLER_BINDING_QUEUE_FULL",
        ),
    ],
)
async def test_bot_and_binding_pending_caps_are_enforced(bot_database, changes, reason):
    values = {
        **DEFAULT_MESSAGE_HANDLER_LIMITS,
        **changes,
        "max_pending_principal": 1,
        "principal_burst": 10,
        "principal_refill_seconds": 3600,
    }
    store, lease, _, messages = await configured(bot_database, values)
    first = await messages.ingest(
        lease, "binding-a", event(21, user="21001"), decision="NO_TRIGGER"
    )
    second = await messages.ingest(
        lease, "binding-a", event(22, user="21002"), decision="NO_TRIGGER"
    )
    async with store.pool.connection() as conn:
        rejected = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, second["event_id"]),
            )
        ).fetchone()
    assert first["trigger_decision"] == "PENDING"
    assert second["trigger_decision"] == "FAILED"
    assert rejected["terminal_reason"] == reason


async def test_principal_token_bucket_refills_durably(bot_database):
    values = {
        **DEFAULT_MESSAGE_HANDLER_LIMITS,
        "max_pending_bot": 10,
        "max_pending_binding": 10,
        "max_pending_principal": 10,
        "principal_burst": 1,
        "principal_refill_seconds": 3600,
    }
    store, lease, _, messages = await configured(bot_database, values)
    first = await messages.ingest(
        lease, "binding-a", event(23), decision="NO_TRIGGER"
    )
    limited = await messages.ingest(
        lease, "binding-a", event(24), decision="NO_TRIGGER"
    )
    async with store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_message_handler_principal_buckets "
            "SET updated_at=updated_at-interval '3601 seconds' WHERE bot_id=%s",
            (store.bot_id,),
        )
    refilled = await messages.ingest(
        lease, "binding-a", event(25), decision="NO_TRIGGER"
    )
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT event_id,status,terminal_reason FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s ORDER BY created_at,chain_id",
                (store.bot_id,),
            )
        ).fetchall()
    assert first["trigger_decision"] == refilled["trigger_decision"] == "PENDING"
    assert limited["trigger_decision"] == "FAILED"
    assert [row["terminal_reason"] for row in rows].count("MESSAGE_HANDLER_RATE_LIMITED") == 1


async def test_configured_deadline_is_frozen_on_admitted_chain(bot_database):
    values = {**DEFAULT_MESSAGE_HANDLER_LIMITS, "deadline_seconds": 7}
    store, lease, _, messages = await configured(bot_database, values)
    accepted = await messages.ingest(
        lease, "binding-a", event(30), decision="NO_TRIGGER", rule_revision="trigger-v1"
    )
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
    assert chain["status"] == "PENDING"
    assert chain["admission_limit_revision"] == 1
    assert 6.9 <= (chain["deadline_at"] - chain["created_at"]).total_seconds() <= 7.1


async def test_route_cap_rejects_before_consuming_principal_token(bot_database):
    values = {
        **DEFAULT_MESSAGE_HANDLER_LIMITS,
        "max_handlers_per_chain": 1,
        "principal_burst": 1,
        "principal_refill_seconds": 3600,
    }
    store, lease, ticket = await prepared(bot_database)
    for component, generation, handler in (
        ("capacity-a", "gen-7", "first"),
        ("capacity-b", "gen-8", "second"),
    ):
        await register(store, lease, component, generation, handler, 10)
        await set_policy(
            store,
            ticket,
            "binding-a",
            component,
            handler,
            {"mode": "EVERYONE", "user_ids": []},
        )
    await set_limits(store, ticket, values)
    messages = MessageStore(store, BindingStore(store))
    accepted = await messages.ingest(
        lease, "binding-a", event(40), decision="TRIGGER", rule_revision="trigger-v1"
    )
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
        item_count = (
            await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_message_handler_chain_items "
                    "WHERE bot_id=%s AND chain_id=%s",
                    (store.bot_id, chain["chain_id"]),
                )
            ).fetchone()
        )["n"]
        bucket_count = (
            await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_message_handler_principal_buckets "
                    "WHERE bot_id=%s",
                    (store.bot_id,),
                )
            ).fetchone()
        )["n"]
    assert accepted["trigger_decision"] == "FAILED"
    assert chain["status"] == "FAILED" and chain["route_count"] == item_count == 2
    assert chain["terminal_reason"] == "MESSAGE_HANDLER_CHAIN_TOO_LARGE"
    assert bucket_count == 0


async def test_classification_barrier_is_admitted_before_handler_work(
    bot_database, monkeypatch
):
    values = {
        **DEFAULT_MESSAGE_HANDLER_LIMITS,
        "max_event_bytes": 1,
        "principal_burst": 1,
        "principal_refill_seconds": 3600,
    }
    store, lease, _, messages = await configured(bot_database, values)
    calls = 0

    async def transient_then_ordinary(conn, binding, value):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"failed": True, "reason": "BUSINESS_COMMAND_POLICY_UNAVAILABLE"}
        return None

    monkeypatch.setattr(messages, "_business_plan_in", transient_then_ordinary)
    accepted = await messages.ingest(
        lease,
        "binding-a",
        event(41),
        decision="TRIGGER",
        rule_revision="trigger-v1",
    )
    async with store.pool.connection() as conn:
        barrier = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains "
                "WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
        bucket_count = (
            await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_message_handler_principal_buckets "
                    "WHERE bot_id=%s",
                    (store.bot_id,),
                )
            ).fetchone()
        )["n"]
    assert accepted["trigger_decision"] == "PENDING"
    assert barrier["status"] == "PENDING" and barrier["admission_exempt"] is True
    assert barrier["admission_limit_revision"] is None
    assert bucket_count == 0

    assert await messages.retry_business_classifications(lease) == 1
    async with store.pool.connection() as conn:
        facts = await (
            await conn.execute(
                "SELECT i.trigger_decision,i.business_classification_pending,"
                "h.status,h.admission_exempt,h.admission_limit_revision,h.terminal_reason,"
                "(SELECT count(*) FROM public.bot_conversation_notices n "
                " WHERE n.bot_id=i.bot_id AND n.event_id=i.event_id "
                " AND n.kind='MESSAGE_HANDLER_REJECTED') AS notices,"
                "(SELECT count(*) FROM public.bot_conversation_triggers t "
                " WHERE t.bot_id=i.bot_id AND t.event_id=i.event_id) AS triggers,"
                "(SELECT count(*) FROM public.bot_memory_source_occurrences m "
                " WHERE m.bot_id=i.bot_id AND m.event_id=i.event_id) AS memory_sources "
                "FROM public.bot_message_inbox i JOIN public.bot_message_handler_chains h "
                "ON h.bot_id=i.bot_id AND h.event_id=i.event_id "
                "WHERE i.bot_id=%s AND i.event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
    assert facts == {
        "trigger_decision": "FAILED",
        "business_classification_pending": False,
        "status": "FAILED",
        "admission_exempt": False,
        "admission_limit_revision": 1,
        "terminal_reason": "MESSAGE_HANDLER_EVENT_TOO_LARGE",
        "notices": 1,
        "triggers": 0,
        "memory_sources": 1,
    }


async def test_classification_barrier_uses_unversioned_defaults(bot_database, monkeypatch):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "capacity", "gen-9", "ordinary", 10)
    await set_policy(
        store,
        ticket,
        "binding-a",
        "capacity",
        "ordinary",
        {"mode": "EVERYONE", "user_ids": []},
    )
    messages = MessageStore(store, BindingStore(store))
    calls = 0

    async def transient_then_ordinary(conn, binding, value):
        nonlocal calls
        calls += 1
        return (
            {"failed": True, "reason": "BUSINESS_COMMAND_POLICY_UNAVAILABLE"}
            if calls == 1
            else None
        )

    monkeypatch.setattr(messages, "_business_plan_in", transient_then_ordinary)
    accepted = await messages.ingest(
        lease, "binding-a", event(42), decision="NO_TRIGGER", rule_revision="trigger-v1"
    )
    assert accepted["trigger_decision"] == "PENDING"
    assert await messages.retry_business_classifications(lease) == 1
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT status,admission_exempt,admission_limit_revision "
                "FROM public.bot_message_handler_chains WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
    assert chain == {
        "status": "PENDING",
        "admission_exempt": False,
        "admission_limit_revision": None,
    }
