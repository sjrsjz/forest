"""Shell Context controls queue view work without owning the group Agent."""

import pytest
from test_agent_store import specification, start
from test_messages import messages as messages
from test_messages import owner, receive
from test_shell import allow, dispatcher
from test_shell_management import claim_command

from forest.storage.agent import AgentStore

pytestmark = pytest.mark.postgres


async def test_context_inspect_is_origin_group_metadata_without_message_content(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = await receive(
        repository, lease, "secret-input", trigger=True, text="SECRET-CONTEXT-BODY"
    )
    actor = await owner(repository, lease, incoming["conversation_id"])
    selected = await repository.select_input(actor, "context-inspect-input")
    assert selected["status"] == "INPUT"

    claim = await claim_command(
        repository, lease, "#forest context inspect", number="context-inspect"
    )
    text, outcome = await dispatcher(repository, lease)._typed(lease, claim)
    assert outcome["code"] == "CONTEXT_STATUS"
    assert outcome["visible_units"] == 1
    assert outcome["pending_triggers"] == 0
    assert "SECRET-CONTEXT-BODY" not in text
    assert "无活跃 Run" in text


async def test_context_compaction_waits_for_actor_safe_point_and_resolves_same_view_race(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = await receive(repository, lease, "trigger", trigger=True)
    actor = await owner(repository, lease, incoming["conversation_id"])
    selected = await repository.select_input(actor, "context-compact-input")
    baseline = await repository.context(actor, run_id=selected["run_id"])
    expected = baseline["context_view_revision"]

    controls = dispatcher(repository, lease)
    first = await claim_command(
        repository,
        lease,
        f"#forest context compact --expect-view {expected}",
        number="compact-first",
    )
    second = await claim_command(
        repository,
        lease,
        f"#forest context compact --expect-view {expected}",
        number="compact-second",
    )
    first_text, first_outcome = await controls._typed(lease, first)
    _, second_outcome = await controls._typed(lease, second)
    assert first_outcome["code"] == second_outcome["code"] == "CONTEXT_COMPACTION_QUEUED"
    assert "不会调用摘要模型" in first_text

    async with repository.store.pool.connection() as conn:
        before = await (await conn.execute(
            "SELECT context_view_revision FROM public.bot_conversations "
            "WHERE bot_id=%s AND conversation_id=%s",
            (repository.store.bot_id, incoming["conversation_id"]),
        )).fetchone()
        requests = await (await conn.execute(
            "SELECT status FROM public.bot_context_management_requests ORDER BY requested_at,request_id"
        )).fetchall()
    assert before["context_view_revision"] == expected
    assert [row["status"] for row in requests] == ["PENDING", "PENDING"]

    boundary = await repository.select_input(
        actor, "context-compact-boundary", mode="SAFE_POINT"
    )
    assert boundary["status"] == "CONTINUE"
    async with repository.store.pool.connection() as conn:
        after = await (await conn.execute(
            "SELECT context_view_revision FROM public.bot_conversations "
            "WHERE bot_id=%s AND conversation_id=%s",
            (repository.store.bot_id, incoming["conversation_id"]),
        )).fetchone()
        requests = await (await conn.execute(
            "SELECT request_id,status,applied_view_revision,terminal_reason "
            "FROM public.bot_context_management_requests ORDER BY requested_at,request_id"
        )).fetchall()
        compactions = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_context_compactions "
            "WHERE bot_id=%s AND conversation_id=%s AND view_revision=%s",
            (repository.store.bot_id, incoming["conversation_id"], expected + 1),
        )).fetchone()
        model_attempts = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_model_transport_attempts"
        )).fetchone()
    assert after["context_view_revision"] == expected + 1
    requests = {row.pop("request_id"): row for row in requests}
    assert requests[first_outcome["request_id"]] == {
        "status": "APPLIED",
        "applied_view_revision": expected + 1,
        "terminal_reason": None,
    }
    assert requests[second_outcome["request_id"]] == {
        "status": "CONFLICT",
        "applied_view_revision": None,
        "terminal_reason": "context_view_revision_changed",
    }
    assert compactions["count"] == 1
    assert model_attempts["count"] == 0


async def test_idle_agent_recovery_does_not_cross_the_model_tool_boundary(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    state, actor, head = await start(messages)
    before = await repository.context(actor, run_id=head["run_id"])
    expected = before["context_view_revision"]
    claim = await claim_command(
        repository,
        lease,
        f"#forest context compact --expect-view {expected}",
        number="compact-agent-resume",
    )
    _, queued = await dispatcher(repository, lease)._typed(lease, claim)

    resumed = await repository.select_input(actor, "context-idle-resume", mode="IDLE")
    assert resumed["status"] == "RESUME"
    async with repository.store.pool.connection() as conn:
        pending = await (await conn.execute(
            "SELECT status FROM public.bot_context_management_requests WHERE request_id=%s",
            (queued["request_id"],),
        )).fetchone()
        view = await (await conn.execute(
            "SELECT context_view_revision FROM public.bot_conversations "
            "WHERE bot_id=%s AND conversation_id=%s",
            (repository.store.bot_id, actor.conversation_id),
        )).fetchone()
    assert pending["status"] == "PENDING"
    assert view["context_view_revision"] == expected

    boundary = await state.boundary(
        actor, head["run_id"], "context-agent-safe-point"
    )
    assert boundary["phase"] == "READY_MODEL"
    async with repository.store.pool.connection() as conn:
        applied = await (await conn.execute(
            "SELECT status,applied_view_revision FROM public.bot_context_management_requests "
            "WHERE request_id=%s",
            (queued["request_id"],),
        )).fetchone()
    assert applied == {"status": "APPLIED", "applied_view_revision": expected + 1}


async def test_context_reset_archives_closed_history_but_keeps_active_input_batch(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    old = await receive(
        repository, lease, "reset-old-trigger", trigger=True, text="OLD-CLOSED-CONTEXT"
    )
    actor = await owner(repository, lease, old["conversation_id"])
    old_batch = await repository.select_input(actor, "context-reset-old-input")
    await repository.end_run(
        actor, run_id=old_batch["run_id"], reason="OLD_RUN_CLOSED", status="FAILED"
    )
    leading = await receive(
        repository,
        lease,
        "reset-current-observation",
        trigger=False,
        text="CURRENT-LEADING-OBSERVATION",
    )
    trigger = await receive(
        repository, lease, "reset-current-trigger", trigger=True, text="CURRENT-TRIGGER"
    )
    batch = await repository.select_input(actor, "context-reset-input")
    state = AgentStore(repository)
    head = await state.ensure_run(actor, batch["run_id"], specification())
    expected = (await repository.context(actor, run_id=head["run_id"]))[
        "context_view_revision"
    ]

    claim = await claim_command(
        repository,
        lease,
        f"#forest context reset --expect-view {expected}",
        number="context-reset",
    )
    text, queued = await dispatcher(repository, lease)._typed(lease, claim)
    assert queued == {
        "code": "CONTEXT_RESET_QUEUED",
        "request_id": queued["request_id"],
        "status": "PENDING",
        "expected_view_revision": expected,
        "mode": "safe-point",
    }
    assert "不会调用摘要模型" in text

    resumed = await repository.select_input(actor, "context-reset-idle", mode="IDLE")
    assert resumed["status"] == "RESUME"
    async with repository.store.pool.connection() as conn:
        still_pending = await (await conn.execute(
            "SELECT status FROM public.bot_context_management_requests WHERE request_id=%s",
            (queued["request_id"],),
        )).fetchone()
    assert still_pending["status"] == "PENDING"

    boundary = await state.boundary(actor, head["run_id"], "context-reset-boundary")
    assert boundary["phase"] == "READY_MODEL"
    async with repository.store.pool.connection() as conn:
        units = await (await conn.execute(
            "SELECT event_id,visible FROM public.bot_context_units "
            "WHERE bot_id=%s AND conversation_id=%s AND event_id=ANY(%s) ORDER BY event_id",
            (
                repository.store.bot_id,
                trigger["conversation_id"],
                [old["event_id"], leading["event_id"], trigger["event_id"]],
            ),
        )).fetchall()
        request = await (await conn.execute(
            "SELECT kind,mode,status,applied_view_revision FROM "
            "public.bot_context_management_requests WHERE request_id=%s",
            (queued["request_id"],),
        )).fetchone()
        publication = await (await conn.execute(
            "SELECT policy_revision,removed_unit_ids,retained_unit_ids "
            "FROM public.bot_context_compactions WHERE bot_id=%s AND conversation_id=%s "
            "AND view_revision=%s",
            (repository.store.bot_id, trigger["conversation_id"], expected + 1),
        )).fetchone()
        model_attempts = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_model_transport_attempts"
        )).fetchone()
    by_event = {row["event_id"]: row["visible"] for row in units}
    assert by_event == {
        old["event_id"]: False,
        leading["event_id"]: True,
        trigger["event_id"]: True,
    }
    assert request == {
        "kind": "RESET",
        "mode": "SAFE_POINT",
        "status": "APPLIED",
        "applied_view_revision": expected + 1,
    }
    assert publication["policy_revision"] == "explicit-safe-point-reset-v1"
    assert publication["removed_unit_ids"] and publication["retained_unit_ids"]
    assert model_attempts["count"] == 0
