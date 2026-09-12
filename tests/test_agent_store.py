"""Real PostgreSQL transactions for Agent cuts, recovery, budgets and authority."""

import asyncio
import json
from uuid import uuid4

import psycopg
import pytest
from test_messages import event, fails, owner, receive
from test_messages import messages as messages
from test_web_authority import environment as environment

from forest.models import ModelCapabilities, ModelResult, prepare_model_request
from forest.storage.agent import AgentStore
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


def specification(*, budget=1000000, approval="never"):
    return {
        "config_revision": 0,
        "values": {
            "max_steps": 8,
            "run_timeout": 240,
            "model_timeout": 30,
            "daily_token_budget": budget,
            "max_output_tokens": 64,
            "allowed_tools": [
                {
                    "name": "lookup",
                    "component_id": "counter",
                    "service": "get",
                    "contract_major": 1,
                    "authority": "trigger",
                    "approval": approval,
                    "effect": "read",
                }
            ],
        },
    }


async def start(messages, *, group="group-1", spec=None):
    repo, lease, _ = messages
    incoming = await receive(repo, lease, uuid4().hex, group=group, trigger=True)
    actor = await owner(repo, lease, incoming["conversation_id"], uuid4().hex)
    batch = await repo.select_input(actor, "start:" + uuid4().hex)
    state = AgentStore(repo)
    head = await state.ensure_run(actor, batch["run_id"], spec or specification())
    return state, actor, head


async def prepare(state, actor, head):
    snapshot = await state.snapshot(actor, head["run_id"])
    request = prepare_model_request(
        snapshot,
        ModelCapabilities("test", "test", "v1"),
        request_id=uuid4().hex,
        transport_attempt_id=uuid4().hex,
        output_tokens=64,
        stream=False,
    )
    permit = await state.prepare_model(actor, head["run_id"], "permit:" + uuid4().hex, request)
    return snapshot, request, permit


async def respond(state, actor, permit, request, *, text="final", calls=None, status="RESPONDED"):
    if status != "NOT_SENT":
        await state.check_model_permit(actor, permit, request)
    return await state.complete_model(
        actor,
        permit,
        ModelResult(
            status,
            request.request_id,
            request.transport_attempt_id,
            request.payload_digest,
            text=text,
            tool_calls_json=json.dumps(calls or []),
            usage_json=json.dumps(
                {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            ),
        ),
    )


def tool_calls(snapshot, count=1):
    return [
        {
            "id": "call-" + str(i),
            "name": "lookup",
            "arguments": {
                "authority_trigger_id": snapshot["trigger_ids"][0],
                "arguments": {"key": str(i)},
            },
        }
        for i in range(count)
    ]


async def query(state, sql, args=()):
    async with state.store.pool.connection() as conn:
        return await (await conn.execute(sql, args)).fetchall()


async def test_final_ready_recovers_before_new_input_and_commits_once(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    head = await respond(state, actor, permit, request)
    assert head["phase"] == "FINAL_READY"
    repo, lease, _ = messages
    await receive(repo, lease, "next", trigger=True)
    await receive(repo, lease, "tail")
    await repo.release_actor(actor)
    successor = await owner(repo, lease, actor.conversation_id, "successor")
    recovered = await state.ensure_run(successor, head["run_id"], specification(budget=123))
    assert recovered["phase"] == "FINAL_READY" and recovered["spec"] == head["spec"]
    await fails(
        "STALE_ACTOR",
        state.commit_final(actor, head["run_id"], "old-final", DeliveryStore(repo.store)),
    )
    reply = await state.commit_final(successor, head["run_id"], "final", DeliveryStore(repo.store))
    replay = await state.commit_final(
        successor, head["run_id"], "lost-ack-recovery", DeliveryStore(repo.store)
    )
    assert reply["reply_id"] == replay["reply_id"]
    assert reply["logical_step_id"] == snapshot["logical_step_id"]
    selected = await repo.select_input(successor, "next-run")
    assert selected["end_seq"] == 2 and selected["run_id"] != head["run_id"]
    rows = await query(
        state,
        "SELECT status,count(*) AS count FROM public.bot_conversation_triggers GROUP BY status",
    )
    assert {r["status"]: r["count"] for r in rows} == {"ANSWERED": 1, "CLAIMED": 1}
    assert len(await query(state, "SELECT * FROM public.bot_conversation_replies")) == 1


async def test_closed_tool_group_flushes_before_last_trigger_cut_and_excludes_tail(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, text="working", calls=tool_calls(snapshot, 2))
    await fails("AGENT_BOUNDARY_CLOSED", state.boundary(actor, head["run_id"], "too-early"))
    grants = [
        await state.permit_tool(
            actor, head["run_id"], call["id"], authority_trigger_id=snapshot["trigger_ids"][0]
        )
        for call in tool_calls(snapshot, 2)
    ]
    await state.observe_tool(actor, grants[1], status="SUCCEEDED", result={"value": 2})
    await fails("TOOL_GROUP_OPEN", state.close_tools(actor, head["run_id"]))
    await state.observe_tool(actor, grants[0], status="SUCCEEDED", result={"value": 1})
    await state.close_tools(actor, head["run_id"])
    repo, lease, _ = messages
    await receive(repo, lease, "ordinary")
    await receive(repo, lease, "trigger-2", trigger=True, user="user-2")
    await receive(repo, lease, "tail")
    cut = await state.boundary(actor, head["run_id"], "cut-2")
    assert cut["selected_input_end_seq"] == 3 and cut["input_revision"] == 2
    context = await repo.context(actor, run_id=head["run_id"])
    assert [u["kind"] for u in context["units"]] == ["INPUT", "RUN_STEP", "INPUT", "INPUT"]
    group = context["units"][1]["step_parts"]
    assert [u["role"] for u in group] == ["assistant", "tool", "tool"]
    assert [u.get("tool_call_id") for u in group[1:]] == ["call-0", "call-1"]
    assert context["units"][-1]["author"]["external_user_id"] == "user-2"
    frozen = await state.snapshot(actor, head["run_id"])
    assert all(unit["unit_id"] in frozen["required_unit_ids"] for unit in group)
    assert len(frozen["trigger_ids"]) == 2


async def test_registered_run_rejects_legacy_boundary_final_and_end(messages):
    state, actor, head = await start(messages)
    repo, _, _ = messages
    await fails("AGENT_BOUNDARY_REQUIRED", repo.select_input(actor, "bypass", mode="SAFE_POINT"))
    await fails(
        "AGENT_BOUNDARY_REQUIRED",
        repo.finalize_reply(
            actor,
            "bypass",
            run_id=head["run_id"],
            expected_input_revision=1,
            reply_parts=[[{"type": "text", "text": "fake"}]],
            delivery=DeliveryStore(repo.store),
        ),
    )
    await fails(
        "AGENT_BOUNDARY_REQUIRED", repo.end_run(actor, run_id=head["run_id"], reason="bypass")
    )


async def test_not_sent_retries_exact_request_and_unknown_keeps_reservation(messages):
    state, actor, head = await start(messages)
    _, request, permit = await prepare(state, actor, head)
    head = await respond(state, actor, permit, request, status="NOT_SENT")
    assert head["phase"] == "RETRY_MODEL"
    await receive(messages[0], messages[1], "new-trigger", trigger=True)
    request2 = await state.replay_prepared(actor, head["run_id"])
    assert request2.body == request.body and request2.request_id == request.request_id
    assert request2.transport_attempt_id != request.transport_attempt_id
    permit2 = await state.prepare_model(actor, head["run_id"], "retry", request2)
    head = await respond(state, actor, permit2, request2, status="UNKNOWN")
    assert head["phase"] == "BLOCKED_UNKNOWN"
    budget = (await query(state, "SELECT * FROM public.bot_agent_budget_days"))[0]
    assert budget["reserved_tokens"] == request.reserved_tokens and budget["charged_tokens"] == 0
    assert await messages[0].pending(messages[1]) == []
    await fails("MODEL_RETRY_FORBIDDEN", state.replay_prepared(actor, head["run_id"]))


async def test_bot_daily_budget_reservations_serialize_different_groups(messages):
    first = await start(messages, group="one")
    second = await start(messages, group="two")
    prepared = []
    for state, actor, head in (first, second):
        snapshot = await state.snapshot(actor, head["run_id"])
        request = prepare_model_request(
            snapshot,
            ModelCapabilities("test", "test", "v1"),
            request_id=uuid4().hex,
            transport_attempt_id=uuid4().hex,
            output_tokens=64,
            stream=False,
        )
        prepared.append(request)
    # Spend remaining daily capacity before racing independent conversation permits.
    room = max(p.reserved_tokens for p in prepared)
    async with first[0].store.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO public.bot_agent_budget_days(bot_id,budget_day,charged_tokens) VALUES (%s,(clock_timestamp() AT TIME ZONE 'UTC')::date,%s)",
            (first[0].store.bot_id, 1000000 - room),
        )
    results = await asyncio.gather(
        *[
            state.prepare_model(actor, head["run_id"], "race:" + str(i), prepared[i])
            for i, (state, actor, head) in enumerate((first, second))
        ],
        return_exceptions=True,
    )
    assert sum(hasattr(r, "transport_attempt_id") for r in results) == 1
    assert [r.code for r in results if isinstance(r, Exception)] == ["DAILY_TOKEN_BUDGET_EXCEEDED"]


async def test_takeover_unknown_transport_blocks_old_observation(messages):
    state, actor, head = await start(messages)
    _, request, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, request)
    await messages[0].release_actor(actor)
    successor = await owner(messages[0], messages[1], actor.conversation_id, "replacement")
    recovered = await state.ensure_run(successor, head["run_id"], specification())
    assert recovered["phase"] == "BLOCKED_UNKNOWN" and recovered["owner_epoch"] == 2
    await fails(
        "MODEL_ATTEMPT_STALE",
        state.complete_model(
            successor,
            permit,
            ModelResult(
                "RESPONDED",
                request.request_id,
                request.transport_attempt_id,
                request.payload_digest,
                text="late",
            ),
        ),
    )
    assert (await query(state, "SELECT status FROM public.bot_model_budget_reservations"))[0][
        "status"
    ] == "UNKNOWN"


async def test_snapshot_and_canonical_facts_are_immutable(messages):
    state, actor, head = await start(messages)
    snapshot = await state.snapshot(actor, head["run_id"])
    assert await state.snapshot(actor, head["run_id"]) == snapshot
    for statement in (
        "UPDATE public.bot_agent_steps SET snapshot='{}'::jsonb",
        "UPDATE public.bot_agent_runs SET spec='{}'::jsonb",
        "DELETE FROM public.bot_agent_checkpoints",
    ):
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            async with state.store.pool.connection() as conn:
                await conn.execute(statement)


async def test_pending_classification_wait_does_not_consume_unclassified_trigger(messages):
    state, actor, head = await start(messages)
    repo, lease, _ = messages
    gap = await repo.ingest(lease, "binding-a", event("gap"))
    await receive(repo, lease, "trigger", trigger=True)
    blocked = await state.boundary(actor, head["run_id"], "waiting")
    assert blocked["phase"] == "WAIT_INPUT"
    await fails("AGENT_INPUT_NOT_READY", state.snapshot(actor, head["run_id"]))
    assert await repo.pending(lease) == []
    await repo.classify(
        lease,
        actor.conversation_id,
        gap["event_id"],
        "NO_TRIGGER",
        rule_revision="test",
        reason="done",
    )
    assert (await repo.pending(lease))[0]["agent_phase"] == "WAIT_INPUT"
    resumed = await state.boundary(actor, head["run_id"], "classified")
    assert resumed["selected_input_end_seq"] == 3 and resumed["input_revision"] == 2


async def test_failed_final_send_intent_rolls_back_candidate_publication(messages):
    state, actor, head = await start(messages)
    _, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request)

    class BrokenDelivery:
        async def enqueue_in(self, *args, **kwargs):
            raise RuntimeError("disk unavailable")

    with pytest.raises(RuntimeError, match="disk unavailable"):
        await state.commit_final(actor, head["run_id"], "final", BrokenDelivery())
    assert (await state.load(actor, head["run_id"]))["phase"] == "FINAL_READY"
    assert await query(state, "SELECT * FROM public.bot_conversation_replies") == []
    assert (await query(state, "SELECT status FROM public.bot_conversation_triggers"))[0][
        "status"
    ] == "CLAIMED"


async def test_tool_final_guard_checks_exact_args_and_current_actor(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    grant = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    async with state.store.pool.connection() as conn, conn.transaction():
        await state.store._runtime(conn)
        result = await state.guard_tool_call_in(conn, state.store.bot_id, grant.operation_id)
        assert result["tool_def"]["name"] == "lookup"
        await fails(
            "TOOL_INPUT_CONFLICT",
            state.guard_tool_call_in(
                conn, state.store.bot_id, grant.operation_id, input_digest="wrong"
            ),
        )
    await messages[0].release_actor(actor)
    async with state.store.pool.connection() as conn, conn.transaction():
        await state.store._runtime(conn)
        await fails(
            "AGENT_TOOL_REVOKED",
            state.guard_tool_call_in(conn, state.store.bot_id, grant.operation_id),
        )


async def authorized_messages(env):
    async with env.store.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,"
            "host_incarnation,host_owner_epoch,enabled,receive_enabled,state) "
            "VALUES (%s,'binding-a','onebot-v11','1000','test','register-a',%s,%s,true,true,'ONLINE')",
            (env.store.bot_id, env.lease.incarnation, env.lease.owner_epoch),
        )
    return MessageStore(env.store), env.lease, ""


async def test_signed_approval_binds_digest_and_once_only_decision(environment):
    env = environment
    state, actor, head = await start(
        await authorized_messages(env), spec=specification(approval="always")
    )
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    waiting = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    assert waiting["status"] == "WAIT_APPROVAL"
    approval = (await query(state, "SELECT * FROM public.bot_agent_tool_approvals"))[0]
    displayed = await state.inspect(await env.ticket("agent.read", "inspect", {}), "inspect", {})
    assert displayed["runs"][0]["channel_id"] == "group-1"
    assert displayed["approvals"][0]["author"]["external_user_id"] == "user-1"
    assert displayed["approvals"][0]["event_id"] == snapshot["triggers"][0]["event_id"]
    wrong = {
        "approval_id": approval["approval_id"],
        "decision": "APPROVE",
        "arguments_digest": "wrong",
    }
    await fails(
        "APPROVAL_CONFLICT",
        state.decide_approval(await env.ticket("agent.approval", "wrong", wrong), "wrong", wrong),
    )
    payload = {**wrong, "arguments_digest": approval["arguments_digest"]}
    receipt = await state.decide_approval(
        await env.ticket("agent.approval", "approve", payload), "approve", payload
    )
    assert (
        await state.decide_approval(
            await env.ticket("agent.approval", "approve", payload), "approve", payload
        )
        == receipt
    )
    opposite = {**payload, "decision": "REJECT"}
    await fails(
        "APPROVAL_CONFLICT",
        state.decide_approval(
            await env.ticket("agent.approval", "opposite", opposite), "opposite", opposite
        ),
    )
    grant = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    async with state.store.pool.connection() as conn, conn.transaction():
        await state.store._runtime(conn)
        assert (await state.guard_tool_call_in(conn, state.store.bot_id, grant.operation_id))[
            "requires_approval"
        ]
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        async with state.store.pool.connection() as conn:
            await conn.execute("UPDATE public.bot_agent_tool_approvals SET decision='REJECT'")


async def test_control_cancel_revokes_current_actor_and_unknown_budget_stays(environment):
    env = environment
    state, actor, head = await start(await authorized_messages(env))
    _, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, status="UNKNOWN")
    saved = await state.load(actor, head["run_id"])
    payload = {
        "run_id": head["run_id"],
        "expected_checkpoint_seq": saved["checkpoint_seq"] - 1,
        "reason": "operator",
    }
    await fails(
        "AGENT_CANCEL_CONFLICT",
        state.cancel(await env.ticket("agent.cancel", "stale", payload), "stale", payload),
    )
    payload["expected_checkpoint_seq"] = saved["checkpoint_seq"]
    receipt = await state.cancel(
        await env.ticket("agent.cancel", "cancel", payload), "cancel", payload
    )
    assert receipt["phase"] == "CANCELLED"
    assert (
        await state.cancel(await env.ticket("agent.cancel", "cancel", payload), "cancel", payload)
        == receipt
    )
    assert (await query(state, "SELECT reserved_tokens FROM public.bot_agent_budget_days"))[0][
        "reserved_tokens"
    ] == request.reserved_tokens
    await fails("STALE_ACTOR", state.load(actor, head["run_id"]))
    assert (await query(state, "SELECT status FROM public.bot_conversation_triggers"))[0][
        "status"
    ] == "CANCELLED"


async def test_unknown_tool_resolution_preserves_observation_history(environment):
    env = environment
    state, actor, head = await start(await authorized_messages(env))
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot, 2))
    grants = [
        await state.permit_tool(
            actor, head["run_id"], call["id"], authority_trigger_id=snapshot["trigger_ids"][0]
        )
        for call in tool_calls(snapshot, 2)
    ]
    await state.observe_tool(actor, grants[0], status="UNKNOWN", result=None)
    saved = await state.observe_tool(actor, grants[1], status="SUCCEEDED", result={"value": 2})
    assert saved["phase"] == "BLOCKED_UNKNOWN"
    payload = {
        "run_id": head["run_id"],
        "tool_call_id": "call-0",
        "expected_checkpoint_seq": saved["checkpoint_seq"],
        "arguments_digest": grants[0].arguments_digest,
        "status": "SUCCEEDED",
        "result": {"value": 1},
    }
    await state.resolve(await env.ticket("agent.resolve", "resolve", payload), "resolve", payload)
    assert (await state.close_tools(actor, head["run_id"]))["phase"] == "READY_MODEL"
    observations = await query(
        state,
        "SELECT status FROM public.bot_agent_tool_observations WHERE tool_call_id='call-0' ORDER BY observation_id",
    )
    assert [r["status"] for r in observations] == ["DISPATCHING", "UNKNOWN", "SUCCEEDED"]


async def test_late_model_output_is_observed_and_charged_without_publication(messages):
    spec = specification()
    spec["values"]["model_timeout"] = 0.1
    state, actor, head = await start(messages, spec=spec)
    _, request, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, request)
    await asyncio.sleep(0.15)
    saved = await state.complete_model(
        actor,
        permit,
        ModelResult(
            "RESPONDED",
            request.request_id,
            request.transport_attempt_id,
            request.payload_digest,
            text="too late",
            usage_json='{"total_tokens":11}',
        ),
    )
    assert saved["phase"] == "FAILED" and saved["error_code"] == "MODEL_DEADLINE_EXCEEDED"
    assert (await query(state, "SELECT status FROM public.bot_model_transport_attempts"))[0][
        "status"
    ] == "RESPONDED"
    assert (await query(state, "SELECT charged_tokens FROM public.bot_agent_budget_days"))[0][
        "charged_tokens"
    ] == 11
    assert await query(state, "SELECT * FROM public.bot_conversation_replies") == []


async def test_expired_approval_wakes_and_ends_without_executing_tool(messages):
    spec = specification(approval="always")
    spec["values"]["run_timeout"] = 1
    state, actor, head = await start(messages, spec=spec)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    assert await messages[0].pending(messages[1]) == []
    await asyncio.sleep(1.05)
    assert (await messages[0].pending(messages[1]))[0]["agent_phase"] == "WAIT_APPROVAL"
    saved = await state.ensure_run(actor, head["run_id"], spec)
    assert saved["phase"] == "FAILED" and saved["tools"][0]["status"] == "CANCELLED"


async def test_model_budget_projection_never_discards_trigger_or_partial_tool_group(messages):
    repo, lease, _ = messages
    await receive(repo, lease, "long-history", text="x" * 10000)
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    grant = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(actor, grant, status="SUCCEEDED", result={"value": "done"})
    await state.close_tools(actor, head["run_id"])
    frozen = await state.snapshot(actor, head["run_id"])
    projected = prepare_model_request(
        frozen,
        ModelCapabilities("test", "test", "v1", context_limit=4000, max_output_tokens=64),
        request_id="small",
        transport_attempt_id="small-attempt",
        output_tokens=64,
        stream=False,
    )
    assert projected.manifest["omitted_unit_ids"]
    assert not set(projected.manifest["omitted_unit_ids"]) & set(frozen["required_unit_ids"])
    trigger_unit = next(
        u["unit_id"]
        for u in (await repo.context(actor, run_id=head["run_id"]))["units"]
        if u["event_id"] == frozen["triggers"][0]["event_id"]
    )
    assert trigger_unit in frozen["required_unit_ids"]
    roles = [m["role"] for m in json.loads(projected.body)["messages"]]
    assert roles[-2:] == ["assistant", "tool"]


async def test_not_sent_tool_retry_rejects_old_attempt_observation(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    first = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(actor, first, status="NOT_SENT", result=None)
    second = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    assert (
        first.operation_id == second.operation_id
        and second.attempt_count == first.attempt_count + 1
    )
    await fails(
        "AGENT_TOOL_STALE",
        state.observe_tool(actor, first, status="SUCCEEDED", result={"late": True}),
    )
    await state.observe_tool(actor, second, status="SUCCEEDED", result={"current": True})
