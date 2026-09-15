"""Ordinary group messages freeze immutable routes before Agent classification."""

import asyncio
from uuid import uuid4

import psycopg
import pytest
from binding_seed import create_online_binding, signed_ticket_factory
from test_kernel import bot_database as bot_database

from forest.storage.bindings import BindingStore
from forest.storage.message_handler_policies import MessageHandlerPolicyStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


def event(number, *, user="20001"):
    return {
        "platform": "onebot-v11",
        "self_id": "10001",
        "channel_kind": "group",
        "channel_id": "933179609",
        "thread_id": "",
        "platform_message_id": str(number),
        "sent_at": "2026-09-15T00:00:00Z",
        "author": {
            "external_user_id": user,
            "display_name": "消息链测试用户",
            "is_self": False,
        },
        "segments": [{"type": "text", "text": "普通群消息"}],
    }


async def register(store, lease, component, generation, handler_id, priority):
    definitions = [{"handler_id": handler_id, "handler": handler_id, "priority": priority}]
    await store.register_generation(
        lease,
        component,
        generation,
        "sha256:" + generation[-1] * 64,
        "sha256:" + "0" * 64,
        "worker-" + generation,
        message_handler_definitions=definitions,
    )
    await store.mark_ready(lease, generation)
    operation = await store.begin_operation(
        lease, component, "activate-" + generation, "plan-" + generation,
        generation, 0,
    )
    await store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    await store.publish(lease, operation["operation_id"], operation["op_epoch"])


async def set_policy(store, ticket, binding_id, component_id, handler_id, values):
    policies = MessageHandlerPolicyStore(store)
    operation = "handler-plan-" + uuid4().hex
    payload = {
        "binding_id": binding_id,
        "component_id": component_id,
        "handler_id": handler_id,
        "expected_revision": 0,
        "values": values,
    }
    plan = await policies.plan(
        await ticket("message.handler.policy.plan", operation, payload),
        operation,
        payload,
    )
    operation = "handler-apply-" + uuid4().hex
    payload = {
        "binding_id": binding_id,
        "component_id": component_id,
        "handler_id": handler_id,
        "expected_revision": 0,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    return await policies.apply(
        await ticket(
            "message.handler.policy.apply",
            operation,
            payload,
            plan_digest=plan["plan_digest"],
        ),
        operation,
        payload,
    )


async def prepared(bot_database):
    store, _ = bot_database
    lease = await store.acquire_host("handler-chain-host", "handler-chain-incarnation")
    ticket = await signed_ticket_factory(store, lease)
    await create_online_binding(
        store,
        lease,
        "binding-a",
        "10001",
        ticket=ticket,
        values={
            "enabled": True,
            "api_base_url": "http://127.0.0.1:5700",
            "response_mode": "disabled",
            "response_template": "",
            "trigger_mentions": True,
            "trigger_prefix": "",
            "private_trigger": True,
            "admin_user_ids": ["20002"],
        },
    )
    return store, lease, ticket


async def test_allowed_routes_are_frozen_in_stable_order_and_block_agent(bot_database):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "zeta", "gen-1", "late", 10)
    await register(store, lease, "alpha", "gen-2", "early", 20)
    for component, handler in (("zeta", "late"), ("alpha", "early")):
        await set_policy(
            store,
            ticket,
            "binding-a",
            component,
            handler,
            {"mode": "EVERYONE", "user_ids": []},
        )
    messages = MessageStore(store, BindingStore(store))
    accepted = await messages.ingest(
        lease, "binding-a", event(1), decision="TRIGGER", rule_revision="binding-trigger-v1"
    )
    assert accepted["trigger_decision"] == "PENDING"
    assert accepted["message_handler_chain_id"].startswith("message-handler-chain:sha256:")
    duplicate = await messages.ingest(
        lease, "binding-a", event(1), decision="TRIGGER", rule_revision="binding-trigger-v1"
    )
    assert duplicate["duplicate"]
    assert duplicate["message_handler_chain_id"] == accepted["message_handler_chain_id"]
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s AND chain_id=%s",
                (store.bot_id, accepted["message_handler_chain_id"]),
            )
        ).fetchone()
        items = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chain_items WHERE bot_id=%s "
                "AND chain_id=%s ORDER BY route_index",
                (store.bot_id, chain["chain_id"]),
            )
        ).fetchall()
        trigger_count = (
            await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_conversation_triggers WHERE bot_id=%s",
                    (store.bot_id,),
                )
            ).fetchone()
        )["n"]
        conversation = await (
            await conn.execute(
                "SELECT classified_through_seq FROM public.bot_conversations "
                "WHERE bot_id=%s AND conversation_id=%s",
                (store.bot_id, accepted["conversation_id"]),
            )
        ).fetchone()
    assert chain["status"] == "PENDING" and chain["route_count"] == 2
    assert chain["fallback_decision"] == "TRIGGER"
    assert [(row["component_id"], row["handler_id"], row["priority"])
            for row in items] == [("alpha", "early", 20), ("zeta", "late", 10)]
    assert [row["policy_revision"] for row in items] == [1, 1]
    assert trigger_count == 0
    assert conversation["classified_through_seq"] == 0
    async with store.pool.connection() as conn:
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState,
            match="MESSAGE_HANDLER_CHAIN_ITEM_IMMUTABLE",
        ):
            await conn.execute(
                "UPDATE public.bot_message_handler_chain_items SET handler='forged' "
                "WHERE bot_id=%s AND chain_id=%s AND route_index=0",
                (store.bot_id, chain["chain_id"]),
            )


async def test_denied_route_records_empty_passed_chain_and_preserves_fallback(bot_database):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "admin-only", "gen-3", "ordinary", 5)
    await set_policy(
        store,
        ticket,
        "binding-a",
        "admin-only",
        "ordinary",
        {"mode": "ADMINS", "user_ids": []},
    )
    messages = MessageStore(store, BindingStore(store))
    accepted = await messages.ingest(
        lease, "binding-a", event(2, user="20001"), decision="TRIGGER"
    )
    assert accepted["trigger_decision"] == "TRIGGER"
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
        trigger = await (
            await conn.execute(
                "SELECT * FROM public.bot_conversation_triggers WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
    assert chain["status"] == "PASSED" and chain["route_count"] == 0
    assert chain["fallback_decision"] == "TRIGGER"
    assert trigger["principal_ref"] == chain["principal_ref"]


async def test_late_fallback_resolution_does_not_advance_a_pending_chain(bot_database):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "deferred", "gen-4", "ordinary", 0)
    await set_policy(
        store,
        ticket,
        "binding-a",
        "deferred",
        "ordinary",
        {"mode": "EVERYONE", "user_ids": []},
    )
    messages = MessageStore(store, BindingStore(store))
    accepted = await messages.ingest(lease, "binding-a", event(3))
    resolved = await messages.classify(
        lease,
        accepted["conversation_id"],
        accepted["event_id"],
        "TRIGGER",
        rule_revision="late-trigger-v1",
    )
    assert resolved["trigger_decision"] == "PENDING"
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
        trigger = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_conversation_triggers WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
    assert chain["status"] == "PENDING"
    assert chain["fallback_decision"] == "TRIGGER" and chain["revision"] == 2
    assert trigger is None


async def test_concurrent_policy_publication_and_ingress_converge_to_one_revision(bot_database):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "concurrent", "gen-5", "ordinary", 0)
    policies = MessageHandlerPolicyStore(store)
    operation = "concurrent-handler-plan"
    proposal = {
        "binding_id": "binding-a",
        "component_id": "concurrent",
        "handler_id": "ordinary",
        "expected_revision": 0,
        "values": {"mode": "EVERYONE", "user_ids": []},
    }
    plan = await policies.plan(
        await ticket("message.handler.policy.plan", operation, proposal),
        operation,
        proposal,
    )
    operation = "concurrent-handler-apply"
    apply = {
        "binding_id": "binding-a",
        "component_id": "concurrent",
        "handler_id": "ordinary",
        "expected_revision": 0,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    apply_ticket = await ticket(
        "message.handler.policy.apply",
        operation,
        apply,
        plan_digest=plan["plan_digest"],
    )
    messages = MessageStore(store, BindingStore(store))
    accepted, applied = await asyncio.gather(
        messages.ingest(lease, "binding-a", event(4), decision="NO_TRIGGER"),
        policies.apply(apply_ticket, operation, apply),
    )
    assert applied["revision"] == 1
    async with store.pool.connection() as conn:
        chain = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s AND event_id=%s",
                (store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
        items = await (
            await conn.execute(
                "SELECT policy_revision FROM public.bot_message_handler_chain_items "
                "WHERE bot_id=%s AND chain_id=%s",
                (store.bot_id, chain["chain_id"]),
            )
        ).fetchall()
    observed = (
        chain["route_count"],
        chain["status"],
        tuple(row["policy_revision"] for row in items),
    )
    assert observed in {
        (0, "PASSED", ()),
        (1, "PENDING", (1,)),
    }
