"""Existing ACTIVE content can become unreadable when its source closure grows."""

import json

import pytest
from test_agent_memory import bound
from test_agent_runner import agent_env as agent_env
from test_agent_runner import begin
from test_agent_store import prepare, query, respond, start, tool_calls
from test_children import acquire, family
from test_memory_config import environment as environment
from test_memory_media import media as media
from test_messages import fails, owner, receive
from test_messages import messages as messages

from forest.models import ModelResult
from forest.storage import payloads as payloads_module
from forest.storage.agent_memory import AgentMemoryStore

pytestmark = pytest.mark.postgres


async def history(repository, lease):
    await receive(repository, lease, "earlier", group="group-1", text="earlier source")


async def assert_drained(state, actor):
    session = (
        await query(state, "SELECT status,active_run_id FROM public.bot_conversation_sessions")
    )[0]
    assert session == {"status": "DRAINING", "active_run_id": None}
    await state.messages.select_input(actor, "drain-source-overflow")
    assert (await query(state, "SELECT active_session_id FROM public.bot_conversations"))[0][
        "active_session_id"
    ] is None
    assert not await query(state, "SELECT * FROM public.bot_conversation_replies")


@pytest.mark.parametrize("phase", ["PREPARING_MODEL", "WAIT_TOOL"])
async def test_new_actor_ends_existing_over_limit_snapshot_without_dispatch(
    agent_env, monkeypatch, phase
):
    repository, lease, kernel, runner, client, spec, _ = agent_env
    await history(repository, lease)
    actor, selected, _ = await begin(agent_env)
    state = runner.state
    head = await state.ensure_run(actor, selected["run_id"], spec)
    if phase == "WAIT_TOOL":
        snapshot, request, permit = await prepare(state, actor, head)
        await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    else:
        snapshot = await state.snapshot(actor, head["run_id"])
    step = (await query(state, "SELECT * FROM public.bot_agent_steps"))[0]
    assert len(snapshot["units"]) == 2
    await repository.release_actor(actor)
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    async with state.store.pool.connection() as conn, conn.transaction():
        epoch = await state.payloads.epoch_in(conn)
        await fails(
            "SOURCE_LINK_SCOPE_LIMIT",
            state.payloads.payloads.read_json_in(
                conn, step["snapshot_payload_id"], expected_deletion_epoch=epoch
            ),
        )
        assert (
            await state.payloads.read_in(conn, step["snapshot_payload_id"], required=False) is None
        )
        await fails(
            "AGENT_MEMORY_REVOKED",
            AgentMemoryStore.guard_run_in(conn, state.store.bot_id, head["run_id"]),
        )
    assert not await query(state, "SELECT 1 FROM public.bot_payload_headers WHERE state<>'ACTIVE'")
    assert not await query(state, "SELECT 1 FROM public.bot_payload_suppression")
    successor = await owner(repository, lease, actor.conversation_id, "limit-successor")
    resumed = await repository.select_input(successor, "resume-over-limit")
    assert successor.actor_epoch != actor.actor_epoch and resumed["run_id"] == head["run_id"]
    result = await runner.advance(
        successor, resumed, await repository.context(successor, run_id=head["run_id"])
    )
    assert result.status == "FAILED" and result.error_code == "AGENT_MEMORY_REVOKED"
    assert not client.prepared and not kernel._calls
    await assert_drained(state, successor)


async def test_late_model_over_limit_source_records_known_cost_without_new_body(
    messages, monkeypatch
):
    repository, lease, _ = messages
    await history(repository, lease)
    state, actor, head = await start(messages)
    _, request, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, request)
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    result = ModelResult(
        "RESPONDED",
        request.request_id,
        request.transport_attempt_id,
        request.payload_digest,
        text="OVERFLOW-LATE-SECRET",
        usage_json='{"total_tokens":19}',
    )
    finished = await state.complete_model(actor, permit, result)
    assert finished["phase"] == "FAILED" and finished["error_code"] == "AGENT_MEMORY_REVOKED"
    attempt = (await query(state, "SELECT * FROM public.bot_model_transport_attempts"))[0]
    assert attempt["status"] == "RESPONDED" and attempt["result_payload_id"] is None
    assert attempt["result_metadata"]["usage"] == {"total_tokens": 19}
    assert "OVERFLOW-LATE-SECRET" not in json.dumps(attempt["result_metadata"])
    budget = (
        await query(
            state, "SELECT reserved_tokens,charged_tokens FROM public.bot_agent_budget_days"
        )
    )[0]
    assert budget == {"reserved_tokens": 0, "charged_tokens": 19}
    assert not await query(state, "SELECT 1 FROM public.bot_agent_tools")
    assert not await query(
        state,
        "SELECT 1 FROM public.bot_payload_bodies WHERE position('OVERFLOW-LATE-SECRET'::bytea IN content)>0",
    )
    await assert_drained(state, actor)


async def test_late_tool_over_limit_source_keeps_effect_receipt_without_publication(
    messages, monkeypatch
):
    await history(messages[0], messages[1])
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    tool = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    finished = await state.observe_tool(
        actor, tool, status="SUCCEEDED", result={"text": "OVERFLOW-TOOL-SECRET"}
    )
    assert finished["phase"] == "FAILED" and finished["error_code"] == "AGENT_MEMORY_REVOKED"
    row = (await query(state, "SELECT * FROM public.bot_agent_tools"))[0]
    assert row["status"] == "SUCCEEDED" and row["result_outcome"] == "SUCCEEDED"
    assert row["result_payload_id"] is None and row["result_digest"]
    assert row["result_metadata"] == {"discarded_reason": "AGENT_MEMORY_REVOKED"}
    assert not await query(
        state, "SELECT 1 FROM public.bot_agent_steps WHERE closed_parts_payload_id IS NOT NULL"
    )
    await assert_drained(state, actor)


async def test_child_over_limit_source_settles_then_parent_can_end(messages, monkeypatch):
    await history(messages[0], messages[1])
    children, actor, head, _ = await family(messages)
    lease, child, request, attempt_id = await acquire(children, messages[1])
    await children.check_permit(lease, attempt_id, request)
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    async with children.store.pool.connection() as conn, conn.transaction():
        await fails("AGENT_MEMORY_REVOKED", children.payloads.guard_in(conn, child))
        await fails("AGENT_MEMORY_REVOKED", children.payloads.hydrate_in(conn, child))
    finished = await children.complete(
        lease,
        attempt_id,
        ModelResult(
            "RESPONDED",
            request.request_id,
            attempt_id,
            request.payload_digest,
            text="OVERFLOW-CHILD-SECRET",
            usage_json='{"total_tokens":21}',
        ),
    )
    assert finished["status"] == "FAILED" and finished["result_payload_id"] is None
    observation = (await query(children.agent, "SELECT * FROM public.bot_child_model_attempts"))[0]
    assert (
        observation["status"] == "RESPONDED"
        and observation["settled"]
        and observation["charged_tokens"] == 21
    )
    assert (
        observation["result_payload_id"] is None
        and observation["result_metadata"]["discarded_reason"] == "AGENT_MEMORY_REVOKED"
    )
    await fails("AGENT_MEMORY_REVOKED", children.adopt(actor, head["run_id"]))
    assert (await children.agent.fail_revoked(actor, head["run_id"]))["phase"] == "FAILED"
    budget = (
        await query(
            children.agent,
            "SELECT reserved_tokens,charged_tokens FROM public.bot_agent_budget_days",
        )
    )[0]
    assert budget == {"reserved_tokens": 0, "charged_tokens": 36}
    await assert_drained(children.agent, actor)


async def test_bound_memory_scope_limit_never_replaces_bound_evidence_and_can_end(
    media, monkeypatch
):
    state, actor, snapshot, memory, bundle = await bound(media)
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    async with state.store.pool.connection() as conn, conn.transaction():
        await fails(
            "SOURCE_LINK_SCOPE_LIMIT",
            state.payloads.payloads.read_json_in(
                conn, bundle["payload_id"], expected_deletion_epoch=bundle["deletion_epoch"]
            ),
        )
    await fails("AGENT_MEMORY_REVOKED", memory.finish(actor, snapshot))
    row = (await query(state, "SELECT status,bundle_id FROM public.bot_agent_memory_bindings"))[0]
    assert row == {"status": "BOUND", "bundle_id": bundle["bundle_id"]}
    assert (await state.fail_revoked(actor, snapshot["run_id"]))["phase"] == "FAILED"
    # This fixture also contains the old evidence conversation; inspect only
    # this Agent's conversation instead of claiming the entire Bot was drained.
    await state.messages.select_input(actor, "drain-memory-overflow")
    assert (
        await query(
            state,
            "SELECT active_session_id FROM public.bot_conversations WHERE conversation_id=%s",
            (actor.conversation_id,),
        )
    )[0]["active_session_id"] is None
    assert not await query(state, "SELECT 1 FROM public.bot_conversation_replies")
