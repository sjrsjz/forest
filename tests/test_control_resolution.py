"""Manual UNKNOWN reconciliation preserves the actual administrative input dependency."""

import pytest
from test_agent_store import authorized_messages, prepare, query, respond, start, tool_calls
from test_control_receipts import forget, operation
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.packages import digest_json

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("value", [{"answer": "MANUAL-RESULT-CANARY"}, None])
async def test_resolved_tool_and_following_snapshot_are_revoked_with_management_input_but_cancel_still_finishes(
    environment, value
):
    env = environment
    state, actor, head = await start(await authorized_messages(env))
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    tool = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    saved = await state.observe_tool(actor, tool, status="UNKNOWN", result=None)
    payload = {
        "run_id": head["run_id"],
        "tool_call_id": "call-0",
        "expected_checkpoint_seq": saved["checkpoint_seq"],
        "arguments_digest": tool.arguments_digest,
        "status": "SUCCEEDED",
        "result": value,
    }
    result = await state.resolve(
        await env.ticket("agent.resolve", "resolution", payload), "resolution", payload
    )
    assert (
        await state.resolve(
            await env.ticket("agent.resolve", "resolution", payload), "resolution", payload
        )
        == result
    )
    record = await operation(env, "resolution")
    stored = (await query(state, "SELECT * FROM public.bot_agent_tools"))[0]
    assert stored["result"] is None and stored["result_digest"] == digest_json(["SUCCEEDED", value])
    header = (
        await query(
            state,
            "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
            (stored["result_payload_id"],),
        )
    )[0]
    assert record["input_payload_id"] in header["manifest"]["parents"]
    assert record["result_payload_id"] not in header["manifest"]["parents"]
    assert (await state.load(actor, head["run_id"]))["tools"][0]["result"] == value
    await state.close_tools(actor, head["run_id"])
    second = await state.snapshot(actor, head["run_id"])
    assert second["logical_step_id"] != snapshot["logical_step_id"]
    before = await query(
        state, "SELECT status,result_digest,result_payload_id FROM public.bot_agent_tools"
    )
    observations = await query(
        state,
        "SELECT * FROM public.bot_agent_tool_observations ORDER BY observation_id",
    )
    assert [row["status"] for row in observations] == ["DISPATCHING", "UNKNOWN", "SUCCEEDED"]
    await forget(env, "resolution")
    for attempt in (state.load(actor, head["run_id"]), state.snapshot(actor, head["run_id"])):
        with pytest.raises(ForestError) as caught:
            await attempt
        assert caught.value.code == "AGENT_MEMORY_REVOKED"
    assert (
        await query(
            state, "SELECT status,result_digest,result_payload_id FROM public.bot_agent_tools"
        )
        == before
    )
    assert (
        await query(
            state,
            "SELECT * FROM public.bot_agent_tool_observations ORDER BY observation_id",
        )
        == observations
    )
    replay = await state.resolve(
        await env.ticket("agent.resolve", "resolution", payload), "resolution", payload
    )
    assert (
        replay["result_payload_state"] == "UNAVAILABLE" and replay["operation_status"] == "RECORDED"
    )
    current = (await query(state, "SELECT checkpoint_seq FROM public.bot_agent_runs"))[0]
    cancel = {
        "run_id": head["run_id"],
        "expected_checkpoint_seq": current["checkpoint_seq"],
        "reason": "stop revoked run",
    }
    terminal = await state.cancel(
        await env.ticket("agent.cancel", "cancel-after-forget", cancel),
        "cancel-after-forget",
        cancel,
    )
    assert terminal["phase"] == "CANCELLED"
    assert (await query(state, "SELECT reserved_tokens FROM public.bot_agent_budget_days"))[0][
        "reserved_tokens"
    ] == 0
    assert (await query(state, "SELECT active_run_id FROM public.bot_conversation_sessions"))[0][
        "active_run_id"
    ] is None


async def test_manual_resolution_freezes_result_before_signature_clock_wait(
    environment, monkeypatch
):
    import asyncio

    env = environment
    state, actor, head = await start(await authorized_messages(env))
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    tool = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    saved = await state.observe_tool(actor, tool, status="UNKNOWN", result=None)
    payload = {
        "run_id": head["run_id"],
        "tool_call_id": "call-0",
        "expected_checkpoint_seq": saved["checkpoint_seq"],
        "arguments_digest": tool.arguments_digest,
        "status": "SUCCEEDED",
        "result": {"value": 1},
    }
    signed = await env.ticket("agent.resolve", "stable-resolution", payload)
    original_digest = signed["claims"]["input_digest"]
    entered, resume = asyncio.Event(), asyncio.Event()
    actual_now, first = env.store._now, True

    async def gated(conn):
        nonlocal first
        if first:
            first = False
            entered.set()
            await resume.wait()
        return await actual_now(conn)

    monkeypatch.setattr(env.store, "_now", gated)
    task = asyncio.create_task(state.resolve(signed, "stable-resolution", payload))
    await asyncio.wait_for(entered.wait(), 3)
    payload["result"]["value"] = 999
    signed["claims"]["input_digest"] = digest_json(payload)
    resume.set()
    await task
    record = await operation(env, "stable-resolution")
    assert record["input_digest"] == record["ticket_basis"]["input_digest"] == original_digest
    assert (await state.load(actor, head["run_id"]))["tools"][0]["result"] == {"value": 1}
    async with env.store.pool.connection() as conn, conn.transaction():
        body = await env.control.content.hydrate_input_in(conn, record, epoch=0)
    assert body["input"]["result"] == {"value": 1} and digest_json(body["input"]) == original_digest
