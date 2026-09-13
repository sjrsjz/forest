"""Existing ACTIVE content can become unreadable when its source closure grows."""

import json

import pytest
from test_agent_memory import bound
from test_agent_runner import agent_env as agent_env
from test_agent_runner import begin
from test_agent_store import prepare, query, respond, start, tool_calls
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


async def configuration_source_limit(state, actor, head, memory_payload_id):
    refs = [head["spec_payload_id"]]
    binding = await query(
        state,
        "SELECT r.values_payload_id FROM public.bot_bindings b "
        "JOIN public.bot_binding_revisions r ON (r.bot_id,r.binding_id,r.revision)="
        "(b.bot_id,b.binding_id,b.effective_revision) "
        "WHERE b.bot_id=%s AND b.binding_id=%s",
        (state.store.bot_id, actor.binding_id),
    )
    if binding:
        refs.append(binding[0]["values_payload_id"])
    revision = head["spec_metadata"]["config_revision"]
    if revision:
        agent = await query(
            state,
            "SELECT values_payload_id FROM public.bot_agent_config_revisions "
            "WHERE bot_id=%s AND revision=%s",
            (state.store.bot_id, revision),
        )
        refs.append(agent[0]["values_payload_id"])
    counts = []
    for payload_id in refs:
        rows = await query(
            state,
            "SELECT count(*) AS n FROM public.forest_payload_effective_sources "
            "WHERE bot_id=%s AND payload_id=%s",
            (state.store.bot_id, payload_id),
        )
        counts.append(rows[0]["n"])
    limit = max(counts)
    memory = await query(
        state,
        "SELECT count(*) AS n FROM public.forest_payload_effective_sources "
        "WHERE bot_id=%s AND payload_id=%s",
        (state.store.bot_id, memory_payload_id),
    )
    assert memory[0]["n"] > limit
    return limit


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
    attempt = (await query(state, "SELECT * FROM public.bot_model_transport_attempts"))[0]
    limit = await configuration_source_limit(state, actor, head, attempt["request_payload_id"])
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", limit)
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
    stored_tool = (await query(state, "SELECT * FROM public.bot_agent_tools"))[0]
    limit = await configuration_source_limit(
        state, actor, head, stored_tool["arguments_payload_id"]
    )
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", limit)
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


async def test_late_tool_configuration_over_limit_settles_effect_without_body(
    messages, monkeypatch
):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    tool = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    monkeypatch.setattr(payloads_module, "MAX_SOURCES", 1)
    finished = await state.observe_tool(
        actor, tool, status="SUCCEEDED", result={"text": "CONFIG-OVERFLOW-TOOL-SECRET"}
    )
    assert finished["phase"] == "FAILED"
    assert finished["error_code"] == "AGENT_CONFIGURATION_REVOKED"
    row = (await query(state, "SELECT * FROM public.bot_agent_tools"))[0]
    assert row["status"] == "SUCCEEDED" and row["result_payload_id"] is None
    assert row["result_metadata"] == {"discarded_reason": "AGENT_MEMORY_REVOKED"}
    assert not await query(
        state,
        "SELECT 1 FROM public.bot_payload_bodies "
        "WHERE position('CONFIG-OVERFLOW-TOOL-SECRET'::bytea IN content)>0",
    )
    await assert_drained(state, actor)


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
