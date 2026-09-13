"""Real Agent payload storage, source revocation and released-schema migration."""

import hashlib
import json
from importlib.resources import files

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_agent_runner import agent_env as agent_env
from test_agent_runner import begin, install_tool, proposed
from test_agent_store import prepare, query, respond, start, tool_calls
from test_context_payloads import forget_event
from test_messages import fails, owner, receive
from test_messages import messages as messages
from test_p4_upgrade import p3_authority
from test_upgrade import upgrade_database as upgrade_database

from forest.models import ModelResult
from forest.packages import digest_json
from forest.storage.agent_payloads import AgentPayloadStore
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def first_receipt(state, snapshot):
    return (
        await query(
            state,
            "SELECT event_id FROM public.bot_message_inbox WHERE event_id=%s",
            (snapshot["units"][0]["unit_id"],),
        )
    )[0]


async def test_native_source_erases_snapshot_wire_output_tool_and_closed_parts(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    receipt = await first_receipt(state, snapshot)
    await respond(state, actor, permit, request, text="MODEL-CANARY", calls=tool_calls(snapshot))
    tool = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(actor, tool, status="SUCCEEDED", result={"text": "TOOL-CANARY"})
    await state.close_tools(actor, head["run_id"])
    second = await state.snapshot(actor, head["run_id"])
    assert "MODEL-CANARY" in str(second) and "TOOL-CANARY" in str(second)
    assert not await query(state, "SELECT * FROM public.bot_agent_memory_bindings")
    for table, fields in (
        ("bot_agent_steps", "snapshot,output,closed_parts"),
        ("bot_model_request_manifests", "manifest"),
        ("bot_model_transport_attempts", "request_body,result"),
        ("bot_agent_tools", "arguments,result"),
        ("bot_agent_tool_observations", "observation"),
    ):
        values = await query(state, f"SELECT {fields} FROM public.{table}")
        assert values and all(value is None for row in values for value in row.values())
    before = await query(
        state,
        "SELECT logical_step_id,snapshot_digest,output_digest,snapshot_payload_id,output_payload_id,closed_parts_payload_id "
        "FROM public.bot_agent_steps ORDER BY step_seq",
    )
    await forget_event(state.messages, messages[1], receipt)
    payloads = PayloadStore(state.store)
    while (await payloads.scrub(messages[1]))["remaining"]:
        pass
    await fails("AGENT_MEMORY_REVOKED", state.snapshot(actor, head["run_id"]))
    assert (await state.fail(actor, head["run_id"], "SOURCE_REMOVED"))["phase"] == "FAILED"
    assert before == await query(
        state,
        "SELECT logical_step_id,snapshot_digest,output_digest,snapshot_payload_id,output_payload_id,closed_parts_payload_id "
        "FROM public.bot_agent_steps ORDER BY step_seq",
    )
    assert not await query(
        state,
        "SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
        "WHERE h.owner_kind IN ('model.snapshot','model.request','model.manifest','model.result','agent.tool')",
    )
    assert not await query(state, "SELECT * FROM public.bot_conversation_replies")


async def test_late_native_model_observation_keeps_known_cost_without_content(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, request)
    await forget_event(state.messages, messages[1], await first_receipt(state, snapshot))
    observation = ModelResult(
        "RESPONDED",
        request.request_id,
        request.transport_attempt_id,
        request.payload_digest,
        text="LATE-CANARY",
        usage_json=json.dumps({"total_tokens": 19, "details": {"text": "USAGE-CANARY"}}),
    )
    closed = await state.complete_model(actor, permit, observation)
    assert closed["phase"] == "FAILED" and closed["error_code"] == "AGENT_MEMORY_REVOKED"
    attempt = (await query(state, "SELECT * FROM public.bot_model_transport_attempts"))[0]
    assert attempt["status"] == "RESPONDED" and attempt["result_digest"]
    assert attempt["result"] is None and attempt["result_payload_id"] is None
    assert attempt["result_metadata"]["usage"] == {"total_tokens": 19}
    assert "LATE-CANARY" not in str(attempt)
    assert "USAGE-CANARY" not in str(attempt)
    budget = (await query(state, "SELECT * FROM public.bot_agent_budget_days"))[0]
    assert (budget["reserved_tokens"], budget["charged_tokens"]) == (0, 19)
    assert not await query(state, "SELECT * FROM public.bot_agent_tools")
    assert not await query(state, "SELECT * FROM public.bot_conversation_replies")


async def test_erased_agent_reply_cannot_enter_a_new_run_and_receipt_stays_committed(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, text="OLD-ANSWER-CANARY")
    delivery = DeliveryStore(state.store)
    reply = await state.commit_final(actor, head["run_id"], "reply", delivery)
    await forget_event(state.messages, messages[1], await first_receipt(state, snapshot))
    repeated = await state.commit_final(actor, head["run_id"], "reply-lost-ack", delivery)
    assert repeated["reply_id"] == reply["reply_id"] and repeated["parts"] is None
    await receive(state.messages, messages[1], "new", trigger=True, text="new visible input")
    await receive(state.messages, messages[1], "tail", text="TAIL-CANARY")
    batch = await state.messages.select_input(actor, "next-run")
    new = await state.ensure_run(actor, batch["run_id"], head["spec"])
    frozen = await state.snapshot(actor, new["run_id"])
    assert "OLD-ANSWER-CANARY" not in str(frozen) and "TAIL-CANARY" not in str(frozen)
    assert "new visible input" in str(frozen)
    assert frozen["selected_input_end_seq"] == 2


async def test_raw_body_columns_and_frozen_references_cannot_be_rewritten(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    statements = (
        "UPDATE public.bot_agent_steps SET snapshot='{}'::jsonb",
        "UPDATE public.bot_agent_steps SET snapshot_payload_id=output_payload_id",
        "UPDATE public.bot_model_request_manifests SET manifest='{}'::jsonb",
        "UPDATE public.bot_model_transport_attempts SET request_body='x'::bytea",
        "UPDATE public.bot_model_transport_attempts SET result_payload_id=NULL",
        "UPDATE public.bot_agent_tools SET arguments='{}'::jsonb",
    )
    for statement in statements:
        async with state.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error):
                await conn.execute(statement)


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED"])
async def test_terminal_tool_result_reference_is_fixed_and_rejected_changes_leave_no_trace(
    messages, status
):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    tool = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    original = {"answer": "ORIGINAL-RESULT-CANARY"}
    await state.observe_tool(actor, tool, status=status, result=original)
    before = await query(state, "SELECT * FROM public.bot_agent_tools")
    observations = await query(
        state, "SELECT * FROM public.bot_agent_tool_observations ORDER BY observation_id"
    )
    for mutation in (
        "result_payload_id=arguments_payload_id",
        "result_payload_id=NULL",
        "result_digest='different'",
        "result_outcome='UNKNOWN'",
        "result_metadata='{}'::jsonb,result_payload_id=arguments_payload_id",
        "status='DISPATCHING',attempt_count=attempt_count+1",
    ):
        async with state.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error):
                await conn.execute("UPDATE public.bot_agent_tools SET " + mutation)
    assert await query(state, "SELECT * FROM public.bot_agent_tools") == before
    assert (
        await query(
            state, "SELECT * FROM public.bot_agent_tool_observations ORDER BY observation_id"
        )
        == observations
    )
    assert (await state.load(actor, head["run_id"]))["tools"][0]["result"] == original
    await state.close_tools(actor, head["run_id"])
    assert "ORIGINAL-RESULT-CANARY" in str(await state.snapshot(actor, head["run_id"]))


async def test_tool_hydration_and_close_reject_mismatched_first_result_payload(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    async with state.store.pool.connection() as conn, conn.transaction():
        with pytest.raises(psycopg.Error):
            await conn.execute("UPDATE public.bot_agent_tools SET status='SUCCEEDED'")
    # Simulate a trusted writer attaching the wrong still-readable payload at
    # first publication. It must never enter a subsequent model tool group.
    async with state.store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE public.bot_agent_tools SET status='SUCCEEDED',result_payload_id=arguments_payload_id,result_digest=%s",
            (digest_json(["SUCCEEDED", {"answer": "intended"}]),),
        )
    await fails("AGENT_PAYLOAD_IDENTITY_CONFLICT", state.load(actor, head["run_id"]))
    await fails("AGENT_PAYLOAD_IDENTITY_CONFLICT", state.close_tools(actor, head["run_id"]))
    assert not await query(
        state, "SELECT 1 FROM public.bot_agent_steps WHERE closed_parts_payload_id IS NOT NULL"
    )
    assert not await query(state, "SELECT 1 FROM public.bot_conversation_replies")


async def test_tool_retry_retains_observation_outcome_and_audits_replacement(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    first = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(actor, first, status="NOT_SENT", result={"reason": "unsent"})
    second = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    retry = (await state.load(actor, head["run_id"]))["tools"][0]
    assert (retry["status"], retry["result_outcome"], retry["result"]) == (
        "DISPATCHING",
        "NOT_SENT",
        {"reason": "unsent"},
    )
    # Identical NOT_SENT data from another attempt remains a valid observation.
    await state.observe_tool(actor, second, status="NOT_SENT", result={"reason": "unsent"})
    third = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(actor, third, status="SUCCEEDED", result={"answer": "known"})
    observations = await query(
        state,
        "SELECT status,observation_refs FROM public.bot_agent_tool_observations ORDER BY observation_id",
    )
    assert [row["status"] for row in observations] == [
        "DISPATCHING",
        "NOT_SENT",
        "DISPATCHING",
        "NOT_SENT",
        "DISPATCHING",
        "SUCCEEDED",
    ]
    assert [row["observation_refs"]["result_outcome"] for row in observations] == [
        None,
        "NOT_SENT",
        "NOT_SENT",
        "NOT_SENT",
        "NOT_SENT",
        "SUCCEEDED",
    ]
    assert (
        observations[1]["observation_refs"]["result_payload_id"]
        != observations[-1]["observation_refs"]["result_payload_id"]
    )
    assert (await state.close_tools(actor, head["run_id"]))["phase"] == "READY_MODEL"


@pytest.mark.parametrize("first_status", ["SUCCEEDED", "NOT_SENT"])
async def test_live_json_null_tool_result_is_a_readable_payload_until_revoked(
    messages, first_status
):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    first = await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    await state.observe_tool(actor, first, status=first_status, result=None)
    async with state.store.pool.connection() as conn, conn.transaction():
        tool = await (await conn.execute("SELECT * FROM public.bot_agent_tools")).fetchone()
        assert tool["result_payload_id"] is not None
        assert tool["result_digest"] == digest_json([first_status, None])
        assert await state.payloads.read_in(conn, tool["result_payload_id"], binary=True) == b"null"
        assert (await state.payloads.hydrate_tool_in(conn, tool))["result"] is None
    if first_status == "NOT_SENT":
        retry = await state.permit_tool(
            actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
        )
        await state.observe_tool(actor, retry, status="SUCCEEDED", result=None)
    assert (await state.close_tools(actor, head["run_id"]))["phase"] == "READY_MODEL"
    next_snapshot = await state.snapshot(actor, head["run_id"])
    assert next(unit for unit in next_snapshot["units"] if unit["role"] == "tool")["parts"] == [
        {"type": "json", "value": None}
    ]
    await forget_event(state.messages, messages[1], await first_receipt(state, snapshot))
    async with state.store.pool.connection() as conn, conn.transaction():
        await fails("AGENT_MEMORY_REVOKED", state.payloads.hydrate_tool_in(conn, tool))


async def test_observation_metadata_rejects_plaintext_and_unbounded_or_nested_usage(messages):
    state, actor, head = await start(messages)
    snapshot, request, permit = await prepare(state, actor, head)
    attempt = (await query(state, "SELECT * FROM public.bot_model_transport_attempts"))[0]
    metadata = {
        "status": "RESPONDED",
        "request_id": attempt["model_request_id"],
        "transport_attempt_id": attempt["transport_attempt_id"],
        "payload_digest": attempt["wire_digest"],
        "usage": {"total_tokens": 19},
        "discarded_reason": "AGENT_MEMORY_REVOKED",
    }
    for poisoned in (
        {"text": "CANARY"},
        {**metadata, "text": "CANARY"},
        {**metadata, "usage": {"total_tokens": "CANARY"}},
        {**metadata, "usage": {"total_tokens": {"text": "CANARY"}}},
        {**metadata, "usage": {"total_tokens": 19, "text": "CANARY"}},
        {**metadata, "usage": {"total_tokens": -1}},
        {**metadata, "usage": {"total_tokens": True}},
        {**metadata, "usage": {"total_tokens": 1000000001}},
    ):
        async with state.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error):
                await conn.execute(
                    "UPDATE public.bot_model_transport_attempts SET status='RESPONDED',result_digest=%s,result_metadata=%s",
                    (digest_json({"discarded": True}), Jsonb(poisoned)),
                )
    await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    await state.permit_tool(
        actor, head["run_id"], "call-0", authority_trigger_id=snapshot["trigger_ids"][0]
    )
    for poisoned in (
        {"text": "CANARY"},
        {"discarded_reason": "CANARY"},
        {"usage": {"total_tokens": 19}},
    ):
        async with state.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error):
                await conn.execute(
                    "UPDATE public.bot_agent_tools SET status='SUCCEEDED',result_digest=%s,result_metadata=%s",
                    (digest_json(["SUCCEEDED", {"text": "discarded"}]), Jsonb(poisoned)),
                )
    assert (await query(state, "SELECT result_metadata FROM public.bot_agent_tools"))[0][
        "result_metadata"
    ] == {}


async def test_native_kernel_final_state_commit_rechecks_raw_source_without_rag(
    agent_env, tmp_path, monkeypatch
):
    repository, lease, kernel, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    actor, selected, context = await begin(agent_env)
    client.responses = [proposed(selected["trigger_ids"][0], increment=29)]
    commit = kernel.store.commit_state
    reached = []

    async def erase_then_commit(*args, **kwargs):
        reached.append(True)
        await forget_event(repository, lease, {"event_id": context["units"][0]["event_id"]})
        return await commit(*args, **kwargs)

    monkeypatch.setattr(kernel.store, "commit_state", erase_then_commit)
    outcome = await runner.advance(actor, selected, context)
    assert reached and outcome.status == "FAILED"
    async with repository.store.pool.connection() as conn:
        assert not await (
            await conn.execute("SELECT * FROM public.bot_agent_memory_bindings")
        ).fetchall()
        assert not await (await conn.execute("SELECT * FROM public.bot_state_steps")).fetchall()
        assert not await (
            await conn.execute("SELECT * FROM public.bot_conversation_replies")
        ).fetchall()
        run = await (
            await conn.execute("SELECT phase,error_code FROM public.bot_agent_runs")
        ).fetchone()
        assert run == {"phase": "FAILED", "error_code": "AGENT_MEMORY_REVOKED"}


@pytest.mark.parametrize("phase", ["PREPARING_MODEL", "WAIT_TOOL"])
async def test_new_actor_terminates_erased_checkpoint_without_hydration_or_dispatch(
    agent_env, phase
):
    repository, lease, kernel, runner, client, spec, _ = agent_env
    actor, selected, _ = await begin(agent_env)
    state = runner.state
    head = await state.ensure_run(actor, selected["run_id"], spec)
    if phase == "WAIT_TOOL":
        snapshot, request, permit = await prepare(state, actor, head)
        await respond(state, actor, permit, request, calls=tool_calls(snapshot))
    else:
        snapshot = await state.snapshot(actor, head["run_id"])
    await repository.release_actor(actor)
    await forget_event(repository, lease, await first_receipt(state, snapshot))
    successor = await owner(repository, lease, actor.conversation_id, "after-erasure")
    resumed = await repository.select_input(successor, "resume-erased")
    assert resumed["run_id"] == head["run_id"] and successor.actor_epoch != actor.actor_epoch
    disposition = await runner.advance(
        successor, resumed, await repository.context(successor, run_id=head["run_id"])
    )
    assert disposition.status == "FAILED" and disposition.error_code == "AGENT_MEMORY_REVOKED"
    assert not client.prepared and not kernel._calls
    persisted = (await query(state, "SELECT phase,error_code FROM public.bot_agent_runs"))[0]
    assert persisted == {"phase": "FAILED", "error_code": "AGENT_MEMORY_REVOKED"}
    session = (
        await query(state, "SELECT status,active_run_id FROM public.bot_conversation_sessions")
    )[0]
    assert session == {"status": "DRAINING", "active_run_id": None}
    # The real scheduler performs the final silent drain after a Run ends.
    await repository.select_input(successor, "drain-after-revocation")
    assert (await query(state, "SELECT active_session_id FROM public.bot_conversations"))[0][
        "active_session_id"
    ] is None
    assert not await query(state, "SELECT * FROM public.bot_conversation_replies")


async def test_revoked_recovery_requires_actual_erasure_and_exact_current_conversation(messages):
    state, actor, head = await start(messages)
    snapshot = await state.snapshot(actor, head["run_id"])
    await fails("AGENT_CONTENT_NOT_REVOKED", state.fail_revoked(actor, head["run_id"]))
    other, other_actor, other_head = await start(messages, group="other")
    await fails("AGENT_SCOPE_MISMATCH", state.fail_revoked(other_actor, head["run_id"]))
    await forget_event(state.messages, messages[1], await first_receipt(state, snapshot))
    await state.messages.release_actor(actor)
    successor = await owner(state.messages, messages[1], actor.conversation_id, "new-owner")
    await fails("STALE_ACTOR", state.fail_revoked(actor, head["run_id"]))
    assert (await state.fail_revoked(successor, head["run_id"]))["phase"] == "FAILED"
    assert (await other.load(other_actor, other_head["run_id"]))["phase"] == "READY_MODEL"
    # A legacy Run without an Agent checkpoint is never terminalized here.
    legacy = await receive(state.messages, messages[1], "legacy", group="legacy", trigger=True)
    legacy_actor = await owner(state.messages, messages[1], legacy["conversation_id"], "legacy")
    legacy_run = await state.messages.select_input(legacy_actor, "legacy-start")
    assert await state.fail_revoked(legacy_actor, legacy_run["run_id"]) is None
    assert (
        await query(
            state,
            "SELECT status FROM public.bot_conversation_runs WHERE run_id=%s",
            (legacy_run["run_id"],),
        )
    )[0]["status"] == "RUNNING"


@pytest.mark.parametrize(
    "legacy_outcome,invalid_digest,legacy_value",
    [
        ("SUCCEEDED", False, {"text": "LEGACY-TOOL-CANARY"}),
        ("NOT_SENT", False, {"text": "LEGACY-TOOL-CANARY"}),
        ("SUCCEEDED", True, {"text": "LEGACY-TOOL-CANARY"}),
        pytest.param("SUCCEEDED", False, None, id="succeeded-json-null"),
        pytest.param("NOT_SENT", False, None, id="not-sent-json-null"),
    ],
)
async def test_released_028_agent_facts_import_exact_bodies_with_conservative_source(
    upgrade_database,
    legacy_outcome,
    invalid_digest,
    legacy_value,
):
    await p3_authority(upgrade_database)
    async with await psycopg.AsyncConnection.connect(
        upgrade_database, autocommit=True, row_factory=dict_row
    ) as conn:
        async with conn.transaction():
            for path in sorted(
                files("forest.storage").joinpath("bot_migrations").iterdir(), key=lambda p: p.name
            ):
                if path.suffix == ".sql" and 21 <= int(path.name[:3]) <= 28:
                    body = path.read_text(encoding="utf-8")
                    await conn.execute(body)
                    await conn.execute(
                        "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                        (path.name, hashlib.sha256(body.encode()).hexdigest()),
                    )
            await conn.execute(
                "UPDATE public.bot_agent_steps SET output=%s,output_digest='old-result',closed_parts=%s WHERE logical_step_id='step:unsent'",
                (Jsonb({"text": "LEGACY-MODEL-CANARY"}), Jsonb([{"text": "LEGACY-CLOSED-CANARY"}])),
            )
            await conn.execute(
                "INSERT INTO public.bot_agent_tools(bot_id,run_id,logical_step_id,tool_call_id,call_index,tool_name,"
                "arguments,arguments_digest,operation_id,status,attempt_count) VALUES "
                "('bot-a','open-run','step:unsent','legacy-tool',0,'lookup',%s,%s,'legacy-tool-op','DISPATCHING',1)",
                (
                    Jsonb({"text": "LEGACY-ARGS-CANARY"}),
                    digest_json({"text": "LEGACY-ARGS-CANARY"}),
                ),
            )
            await conn.execute(
                "UPDATE public.bot_agent_tools SET status=%s,result=%s,result_digest=%s WHERE operation_id='legacy-tool-op'",
                (
                    legacy_outcome,
                    Jsonb(legacy_value),
                    "legacy-result"
                    if invalid_digest
                    else digest_json([legacy_outcome, legacy_value]),
                ),
            )
            if legacy_outcome == "NOT_SENT":
                await conn.execute(
                    "UPDATE public.bot_agent_tools SET status='DISPATCHING',attempt_count=2 WHERE operation_id='legacy-tool-op'"
                )
            original_tool = await (
                await conn.execute(
                    "SELECT result,result IS NOT NULL AS result_present FROM public.bot_agent_tools WHERE operation_id='legacy-tool-op'"
                )
            ).fetchone()
            assert original_tool == {"result": legacy_value, "result_present": True}
        old_attempts = await (
            await conn.execute(
                "SELECT *,result IS NOT NULL AS result_present FROM public.bot_model_transport_attempts ORDER BY transport_attempt_id"
            )
        ).fetchall()
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        if invalid_digest:
            with pytest.raises(ValueError, match="tool observation digest does not match"):
                await store.migrate()
            async with store.pool.connection() as conn:
                row = await (
                    await conn.execute(
                        "SELECT result,result_digest FROM public.bot_agent_tools WHERE operation_id='legacy-tool-op'"
                    )
                ).fetchone()
                assert row == {
                    "result": legacy_value,
                    "result_digest": "legacy-result",
                }
                assert not await (
                    await conn.execute(
                        "SELECT 1 FROM public.forest_bot_schema_migrations WHERE version='031_agent_payloads.sql'"
                    )
                ).fetchall()
                attempts = await (
                    await conn.execute(
                        "SELECT transport_attempt_id,request_body,wire_digest FROM public.bot_model_transport_attempts ORDER BY transport_attempt_id"
                    )
                ).fetchall()
                assert [
                    (r["transport_attempt_id"], bytes(r["request_body"]), r["wire_digest"])
                    for r in attempts
                ] == [
                    (r["transport_attempt_id"], bytes(r["request_body"]), r["wire_digest"])
                    for r in old_attempts
                ]
            return
        await store.migrate()
        payloads = AgentPayloadStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            tool = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_tools WHERE operation_id='legacy-tool-op'"
                )
            ).fetchone()
            restored_tool = await payloads.hydrate_tool_in(conn, tool)
            assert restored_tool["result"] == legacy_value
            assert tool["result_payload_id"] is not None
            if legacy_value is None:
                assert (
                    await payloads.read_in(conn, tool["result_payload_id"], binary=True) == b"null"
                )
            assert tool["result"] is None and tool["result_outcome"] == legacy_outcome
            assert tool["result_digest"] == digest_json([legacy_outcome, restored_tool["result"]])
            assert (tool["status"], tool["attempt_count"]) == (
                ("DISPATCHING", 2) if legacy_outcome == "NOT_SENT" else ("SUCCEEDED", 1)
            )
            attempts = await (
                await conn.execute(
                    "SELECT a.*,m.manifest_payload_id FROM public.bot_model_transport_attempts a "
                    "JOIN public.bot_model_request_manifests m USING(bot_id,model_request_id) ORDER BY transport_attempt_id"
                )
            ).fetchall()
            for before, row in zip(old_attempts, attempts, strict=True):
                restored = await payloads.hydrate_attempt_in(conn, row)
                assert restored["request_body"] == bytes(before["request_body"])
                for key in (
                    "wire_digest",
                    "status",
                    "operation_id",
                    "model_call_epoch",
                    "owner_epoch",
                ):
                    assert restored[key] == before[key]
                assert row["request_body"] is None and row["result"] is None
                if not before["result_present"]:
                    assert row["result_payload_id"] is None
            observation = (
                await (
                    await conn.execute("SELECT * FROM public.bot_agent_tool_observations")
                ).fetchall()
            )[0]
            assert observation["observation"] is None
            assert "LEGACY-ARGS-CANARY" in str(
                await payloads.read_in(conn, observation["observation_payload_id"])
            )
            step = await (
                await conn.execute(
                    "SELECT * FROM public.bot_agent_steps WHERE logical_step_id='step:unsent'"
                )
            ).fetchone()
            restored = await payloads.hydrate_step_in(conn, step)
            assert restored["output"] == {"text": "LEGACY-MODEL-CANARY"}
            assert restored["closed_parts"] == [{"text": "LEGACY-CLOSED-CANARY"}]
        # A current Host can still read exact imported wire until a deletion.
        lease = await store.acquire_host("new", "new-incarnation", ttl_seconds=120)
        raw = PayloadStore(store)
        from forest.memory_types import SourceRef

        await raw.forget_sources(
            lease,
            "first-forget",
            (SourceRef("some-unrelated-raw-source"),),
            expected_deletion_epoch=0,
        )
        async with store.pool.connection() as conn, conn.transaction():
            await fails("AGENT_MEMORY_REVOKED", payloads.hydrate_step_in(conn, step))
        while (await raw.scrub(lease))["remaining"]:
            pass
        await store.migrate()
        async with store.pool.connection() as conn:
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                    "WHERE h.owner_id LIKE 'legacy-agent:%'"
                )
            ).fetchall()
    finally:
        await store.close()
