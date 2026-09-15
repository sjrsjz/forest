"""Conversation Actor execution of immutable ordinary-message handler chains."""

from pathlib import Path
from uuid import uuid4

import pytest
from binding_seed import create_online_binding, signed_ticket_factory
from test_kernel import bot_database as bot_database
from test_message_handler_chains import event, set_policy

from forest.group_scheduler import GroupScheduler, MessageHandlerExecution
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.bindings import BindingStore
from forest.storage.delivery import DeliveryStore
from forest.storage.message_handler_policies import MessageHandlerPolicyStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


def handler_source(tmp_path: Path) -> Path:
    root = tmp_path / "actor-message-handlers"
    module = root / "src" / "actor_handlers"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname="actor-handlers"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import Pass, Reply, message_handler, plugin

@plugin(id="actor.handlers", version="0.1.0")
class ActorHandlers:
    @message_handler("first", priority=20)
    async def first(self, event, ctx):
        return Pass()

    @message_handler("second", priority=10)
    async def second(self, event, ctx):
        if event["platform_message_id"] == "8104":
            return Pass()
        user = event["author"]["external_user_id"]
        return Reply([[{"type": "text", "text": "handled:" + user}]])
''',
        encoding="utf-8",
    )
    return root


async def prepare(bot_database, tmp_path, *, policy_values=None):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    lease = kernel.lease
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
            "response_mode": "agent",
            "response_template": "",
            "trigger_mentions": True,
            "trigger_prefix": "",
            "private_trigger": True,
            "admin_user_ids": [],
        },
    )
    catalog = PackageCatalog(tmp_path / "catalog")
    await kernel.install(catalog.build(handler_source(tmp_path)))
    policy_values = policy_values or {"mode": "EVERYONE", "user_ids": []}
    for handler_id in ("first", "second"):
        await set_policy(
            store,
            ticket,
            "binding-a",
            "actor.handlers",
            handler_id,
            policy_values,
        )
    return store, lease, kernel


async def replace_policy(store, lease, handler_id, values):
    ticket = await signed_ticket_factory(store, lease)
    policies = MessageHandlerPolicyStore(store)
    operation = "handler-replace-plan-" + uuid4().hex
    payload = {
        "binding_id": "binding-a",
        "component_id": "actor.handlers",
        "handler_id": handler_id,
        "expected_revision": 1,
        "values": values,
    }
    plan = await policies.plan(
        await ticket("message.handler.policy.plan", operation, payload), operation, payload
    )
    operation = "handler-replace-apply-" + uuid4().hex
    payload = {
        "binding_id": "binding-a",
        "component_id": "actor.handlers",
        "handler_id": handler_id,
        "expected_revision": 1,
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


async def test_actor_runs_one_frozen_item_per_quantum_and_reply_suppresses_agent(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    delivery = DeliveryStore(store, BindingStore(store))
    actor = None
    try:
        accepted = await messages.ingest(
            lease,
            "binding-a",
            event(8101),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        actor = await messages.acquire_actor(
            lease, accepted["conversation_id"], "handler-test-owner", ttl_seconds=30
        )

        first = await messages.select_input(actor, "handler-select:" + uuid4().hex)
        assert first["status"] == "MESSAGE_HANDLER"
        assert first["request"]["handler_id"] == "first"
        first_receipt = await kernel.invoke_message_handler_with_receipt(
            first["request"],
            first["event"],
            operation_id=first["attempt"]["call_operation_id"],
            input_parent_ids=tuple(first["input_parent_ids"]),
        )
        await messages.finalize_message_handler(
            actor,
            "message-handler-final:" + first["attempt"]["operation_id"],
            chain_id=first["chain"]["chain_id"],
            call_operation_id=first["attempt"]["call_operation_id"],
            result=first_receipt.value,
            result_payload_id=first_receipt.result_payload_id,
            result_payload_digest=first_receipt.result_digest,
            delivery=delivery,
        )

        second = await messages.select_input(actor, "handler-select:" + uuid4().hex)
        assert second["status"] == "MESSAGE_HANDLER"
        assert second["request"]["handler_id"] == "second"
        assert second["attempt"]["attempt_no"] == 2
        second_receipt = await kernel.invoke_message_handler_with_receipt(
            second["request"],
            second["event"],
            operation_id=second["attempt"]["call_operation_id"],
            input_parent_ids=tuple(second["input_parent_ids"]),
        )
        await messages.finalize_message_handler(
            actor,
            "message-handler-final:" + second["attempt"]["operation_id"],
            chain_id=second["chain"]["chain_id"],
            call_operation_id=second["attempt"]["call_operation_id"],
            result=second_receipt.value,
            result_payload_id=second_receipt.result_payload_id,
            result_payload_digest=second_receipt.result_digest,
            delivery=delivery,
        )

        drained = await messages.select_input(actor, "handler-drain:" + uuid4().hex)
        assert drained["kind"] == "SILENT_DRAIN"
        context = await messages.context(actor)
        assert [unit["kind"] for unit in context["units"]] == [
            "INPUT",
            "MESSAGE_HANDLER_RESULT",
        ]
        assert context["units"][0]["author"]["external_user_id"] == "20001"
        assert context["units"][1]["reply_parts"] == [
            [{"type": "text", "text": "handled:20001"}]
        ]
        async with store.pool.connection() as conn:
            chain = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                    "AND chain_id=%s",
                    (store.bot_id, accepted["message_handler_chain_id"]),
                )
            ).fetchone()
            inbox = await (
                await conn.execute(
                    "SELECT trigger_decision FROM public.bot_message_inbox WHERE bot_id=%s "
                    "AND event_id=%s",
                    (store.bot_id, accepted["event_id"]),
                )
            ).fetchone()
            trigger_count = (
                await (
                    await conn.execute(
                        "SELECT count(*) AS count FROM public.bot_conversation_triggers "
                        "WHERE bot_id=%s AND event_id=%s",
                        (store.bot_id, accepted["event_id"]),
                    )
                ).fetchone()
            )["count"]
        assert chain["status"] == "REPLIED" and chain["current_index"] == 2
        assert inbox["trigger_decision"] == "NO_TRIGGER"
        assert trigger_count == 0
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_scheduler_rebinds_one_attempt_and_executes_one_item_per_quantum(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    accepted = await messages.ingest(
        lease,
        "binding-a",
        event(8102),
        decision="TRIGGER",
        rule_revision="binding-trigger-v1",
    )
    first_actor = await messages.acquire_actor(
        lease, accepted["conversation_id"], "crashed-handler-owner", ttl_seconds=30
    )
    first = await messages.select_input(first_actor, "handler-select:" + uuid4().hex)
    await messages.release_actor(first_actor)
    calls = []

    async def execute(actor, selected, context):
        assert context is None
        calls.append((selected["request"]["handler_id"], selected["attempt"]["call_operation_id"]))
        receipt = await kernel.invoke_message_handler_with_receipt(
            selected["request"],
            selected["event"],
            operation_id=selected["attempt"]["call_operation_id"],
            input_parent_ids=tuple(selected["input_parent_ids"]),
        )
        return MessageHandlerExecution(
            selected["attempt"]["call_operation_id"],
            receipt.value,
            receipt.result_payload_id,
            receipt.result_digest,
        )

    scheduler = GroupScheduler(messages, lease=lease, execute=execute)
    try:
        await scheduler._conversation(accepted["conversation_id"])
        assert calls == [("first", first["attempt"]["call_operation_id"])]
        async with store.pool.connection() as conn:
            after_first = await (
                await conn.execute(
                    "SELECT status,current_index,attempt_count FROM "
                    "public.bot_message_handler_chains WHERE bot_id=%s AND chain_id=%s",
                    (store.bot_id, accepted["message_handler_chain_id"]),
                )
            ).fetchone()
        assert after_first == {"status": "PENDING", "current_index": 1, "attempt_count": 1}

        await scheduler._conversation(accepted["conversation_id"])
        assert [name for name, _ in calls] == ["first", "second"]
        assert calls[0][1] != calls[1][1]
        async with store.pool.connection() as conn:
            final = await (
                await conn.execute(
                    "SELECT status,current_index,attempt_count FROM "
                    "public.bot_message_handler_chains WHERE bot_id=%s AND chain_id=%s",
                    (store.bot_id, accepted["message_handler_chain_id"]),
                )
            ).fetchone()
        assert final == {"status": "REPLIED", "current_index": 2, "attempt_count": 2}
    finally:
        await scheduler.close()
        await kernel.close()


async def test_terminal_handler_failure_is_safe_output_and_never_agent_fallback(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    delivery = DeliveryStore(store, BindingStore(store))
    actor = None
    try:
        accepted = await messages.ingest(
            lease,
            "binding-a",
            event(8103),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        actor = await messages.acquire_actor(
            lease, accepted["conversation_id"], "failure-owner", ttl_seconds=30
        )
        selected = await messages.select_input(actor, "handler-select:" + uuid4().hex)
        failure = await messages.record_message_handler_failure(
            actor,
            "message-handler-failure:" + selected["attempt"]["operation_id"],
            chain_id=selected["chain"]["chain_id"],
            error_code="MESSAGE_HANDLER_FENCED",
            retryable=False,
            retry_delay_seconds=0,
            max_attempts=3,
            delivery=delivery,
        )
        assert not failure["retryable"] and failure["response_seq"] == 1
        drained = await messages.select_input(actor, "handler-drain:" + uuid4().hex)
        assert drained["kind"] == "SILENT_DRAIN"
        context = await messages.context(actor)
        assert [unit["kind"] for unit in context["units"]] == [
            "INPUT",
            "MESSAGE_HANDLER_RESULT",
        ]
        async with store.pool.connection() as conn:
            facts = await (
                await conn.execute(
                    "SELECT h.status,i.trigger_decision,count(t.trigger_id) AS triggers "
                    "FROM public.bot_message_handler_chains h "
                    "JOIN public.bot_message_inbox i ON (i.bot_id,i.event_id)=(h.bot_id,h.event_id) "
                    "LEFT JOIN public.bot_conversation_triggers t ON "
                    "(t.bot_id,t.event_id)=(i.bot_id,i.event_id) "
                    "WHERE h.bot_id=%s AND h.chain_id=%s GROUP BY h.status,i.trigger_decision",
                    (store.bot_id, accepted["message_handler_chain_id"]),
                )
            ).fetchone()
        assert facts == {"status": "FAILED", "trigger_decision": "FAILED", "triggers": 0}
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_all_passes_release_the_frozen_agent_trigger_only_after_last_item(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    delivery = DeliveryStore(store, BindingStore(store))
    actor = None
    try:
        accepted = await messages.ingest(
            lease,
            "binding-a",
            event(8104),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        actor = await messages.acquire_actor(
            lease, accepted["conversation_id"], "all-pass-owner", ttl_seconds=30
        )
        for expected in ("first", "second"):
            selected = await messages.select_input(actor, "handler-select:" + uuid4().hex)
            assert selected["request"]["handler_id"] == expected
            receipt = await kernel.invoke_message_handler_with_receipt(
                selected["request"],
                selected["event"],
                operation_id=selected["attempt"]["call_operation_id"],
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            assert receipt.value == {"kind": "PASS"}
            await messages.finalize_message_handler(
                actor,
                "message-handler-final:" + selected["attempt"]["operation_id"],
                chain_id=selected["chain"]["chain_id"],
                call_operation_id=selected["attempt"]["call_operation_id"],
                result=receipt.value,
                result_payload_id=receipt.result_payload_id,
                result_payload_digest=receipt.result_digest,
                delivery=delivery,
            )
            async with store.pool.connection() as conn:
                count = (
                    await (
                        await conn.execute(
                            "SELECT count(*) AS count FROM public.bot_conversation_triggers "
                            "WHERE bot_id=%s AND event_id=%s",
                            (store.bot_id, accepted["event_id"]),
                        )
                    ).fetchone()
                )["count"]
            assert count == (1 if expected == "second" else 0)
        selected = await messages.select_input(actor, "agent-fallback:" + uuid4().hex)
        assert selected["status"] == "INPUT"
        assert selected["trigger_ids"] == [
            "trigger:" + accepted["message_handler_chain_id"].removeprefix(
                "message-handler-chain:"
            )
        ]
        async with store.pool.connection() as conn:
            chain = await (
                await conn.execute(
                    "SELECT status,current_index FROM public.bot_message_handler_chains "
                    "WHERE bot_id=%s AND chain_id=%s",
                    (store.bot_id, accepted["message_handler_chain_id"]),
                )
            ).fetchone()
        assert chain == {"status": "PASSED", "current_index": 2}
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_retry_releases_the_claim_and_allocates_a_new_stable_attempt(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    delivery = DeliveryStore(store, BindingStore(store))
    actor = None
    try:
        accepted = await messages.ingest(
            lease,
            "binding-a",
            event(8105),
            decision="NO_TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        actor = await messages.acquire_actor(
            lease, accepted["conversation_id"], "retry-owner", ttl_seconds=30
        )
        first = await messages.select_input(actor, "handler-select:" + uuid4().hex)
        failure = await messages.record_message_handler_failure(
            actor,
            "message-handler-failure:" + first["attempt"]["operation_id"],
            chain_id=first["chain"]["chain_id"],
            error_code="WORKER_UNAVAILABLE",
            retryable=True,
            retry_delay_seconds=0,
            max_attempts=3,
            delivery=delivery,
        )
        assert failure["retryable"] and failure["retry_at"] is not None
        second = await messages.select_input(actor, "handler-select:" + uuid4().hex)
        assert second["attempt"]["attempt_no"] == 2
        assert second["attempt"]["call_operation_id"] != first["attempt"]["call_operation_id"]
        await messages.record_message_handler_failure(
            actor,
            "message-handler-failure:" + second["attempt"]["operation_id"],
            chain_id=second["chain"]["chain_id"],
            error_code="MESSAGE_HANDLER_FENCED",
            retryable=False,
            retry_delay_seconds=0,
            max_attempts=3,
            delivery=delivery,
        )
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_handler_output_stays_between_its_source_and_a_later_agent_trigger(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(
        bot_database,
        tmp_path,
        policy_values={"mode": "USER_SET", "user_ids": ["20001"]},
    )
    messages = MessageStore(store, BindingStore(store))
    delivery = DeliveryStore(store, BindingStore(store))
    actor = None
    try:
        handled = await messages.ingest(
            lease,
            "binding-a",
            event(8106, user="20001"),
            decision="NO_TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        later = await messages.ingest(
            lease,
            "binding-a",
            event(8107, user="20002"),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        assert later["trigger_decision"] == "TRIGGER"
        actor = await messages.acquire_actor(
            lease, handled["conversation_id"], "ordered-cut-owner", ttl_seconds=30
        )
        for expected in ("first", "second"):
            selected = await messages.select_input(actor, "handler-select:" + uuid4().hex)
            assert selected["request"]["handler_id"] == expected
            receipt = await kernel.invoke_message_handler_with_receipt(
                selected["request"],
                selected["event"],
                operation_id=selected["attempt"]["call_operation_id"],
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            await messages.finalize_message_handler(
                actor,
                "message-handler-final:" + selected["attempt"]["operation_id"],
                chain_id=selected["chain"]["chain_id"],
                call_operation_id=selected["attempt"]["call_operation_id"],
                result=receipt.value,
                result_payload_id=receipt.result_payload_id,
                result_payload_digest=receipt.result_digest,
                delivery=delivery,
            )
        selection = await messages.select_input(actor, "combined-agent-cut:" + uuid4().hex)
        assert selection["status"] == "INPUT" and selection["selected_input_end_seq"] == 2
        context = await messages.context(actor, run_id=selection["run_id"])
        assert [unit["kind"] for unit in context["units"]] == [
            "INPUT",
            "MESSAGE_HANDLER_RESULT",
            "INPUT",
        ]
        assert [
            context["units"][0]["author"]["external_user_id"],
            context["units"][2]["author"]["external_user_id"],
        ] == ["20001", "20002"]
        assert context["triggers"][0]["event_id"] == later["event_id"]
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_policy_contraction_before_claim_cancels_without_agent_fallback(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    actor = None
    try:
        accepted = await messages.ingest(
            lease,
            "binding-a",
            event(8108),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        await replace_policy(
            store,
            lease,
            "first",
            {"mode": "USER_SET", "user_ids": ["29999"]},
        )
        actor = await messages.acquire_actor(
            lease, accepted["conversation_id"], "revoked-policy-owner", ttl_seconds=30
        )
        cancelled = await messages.select_input(actor, "handler-select:" + uuid4().hex)
        assert cancelled["kind"] == "MESSAGE_HANDLER_POLICY_REVOKED"
        drained = await messages.select_input(actor, "handler-drain:" + uuid4().hex)
        assert drained["kind"] == "SILENT_DRAIN"
        async with store.pool.connection() as conn:
            facts = await (
                await conn.execute(
                    "SELECT h.status,h.terminal_reason,i.trigger_decision "
                    "FROM public.bot_message_handler_chains h JOIN public.bot_message_inbox i "
                    "ON (i.bot_id,i.event_id)=(h.bot_id,h.event_id) "
                    "WHERE h.bot_id=%s AND h.chain_id=%s",
                    (store.bot_id, accepted["message_handler_chain_id"]),
                )
            ).fetchone()
        assert facts == {
            "status": "CANCELLED",
            "terminal_reason": "POLICY_REVOKED",
            "trigger_decision": "NO_TRIGGER",
        }
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_active_run_waits_at_safe_point_then_actor_claims_the_handler(
    bot_database, tmp_path
):
    store, lease, kernel = await prepare(bot_database, tmp_path)
    messages = MessageStore(store, BindingStore(store))
    delivery = DeliveryStore(store, BindingStore(store))
    actor = None
    try:
        initial = await messages.ingest(
            lease,
            "binding-a",
            event(8104),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        actor = await messages.acquire_actor(
            lease, initial["conversation_id"], "active-run-owner", ttl_seconds=30
        )
        for _ in range(2):
            selected = await messages.select_input(actor, "handler-select:" + uuid4().hex)
            receipt = await kernel.invoke_message_handler_with_receipt(
                selected["request"],
                selected["event"],
                operation_id=selected["attempt"]["call_operation_id"],
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            await messages.finalize_message_handler(
                actor,
                "message-handler-final:" + selected["attempt"]["operation_id"],
                chain_id=selected["chain"]["chain_id"],
                call_operation_id=selected["attempt"]["call_operation_id"],
                result=receipt.value,
                result_payload_id=receipt.result_payload_id,
                result_payload_digest=receipt.result_digest,
                delivery=delivery,
            )
        run = await messages.select_input(actor, "initial-agent-cut:" + uuid4().hex)
        assert run["status"] == "INPUT" and run["run_id"]
        queued = await messages.ingest(
            lease,
            "binding-a",
            event(8109),
            decision="NO_TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        waiting = await messages.select_input(
            actor, "active-safe-point:" + uuid4().hex, mode="SAFE_POINT"
        )
        assert waiting["status"] == "WAIT_INPUT" and waiting["run_id"] == run["run_id"]
        claimed = await messages.select_input(actor, "active-handler:" + uuid4().hex)
        assert claimed["status"] == "MESSAGE_HANDLER"
        assert claimed["chain"]["event_id"] == queued["event_id"]
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()
