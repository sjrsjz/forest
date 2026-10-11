"""An expired Run must still expose uncertain external effects for reconciliation."""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from forest.errors import ForestError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/agent_runtime/python"))
from execution import AgentExecution


@pytest.mark.asyncio
async def test_expired_unknown_effects_stay_reconcilable(monkeypatch):
    calls = []

    async def uow_call(name, _body, **_kwargs):
        calls.append(name)
        if name == "agent.acquire-run-lease":
            return {"run_id": "run/1", "owner_id": "owner/1", "epoch": 1}
        assert name == "agent.release-run-lease"
        return {}

    ctx = SimpleNamespace(
        identity={"graph_id": "graph/1", "graph_revision": 1},
        operations=SimpleNamespace(stable=lambda value: value),
        uow=SimpleNamespace(call=uow_call),
        tasks=SimpleNamespace(
            spawn_owned=lambda coroutine, *, name: asyncio.create_task(
                coroutine, name=name
            )
        ),
        agent_tools=SimpleNamespace(
            list=AsyncMock(return_value={
                "protocol": "forest.contribution-directory/1",
                "kind": "agent-tools", "graph_id": "graph/1",
                "graph_revision": 1, "items": [],
            })
        ),
    )
    executor = AgentExecution(ctx, {}, lambda: None)
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    run = {
        "run_id": "run/1", "state": "BLOCKED_UNKNOWN",
        "absolute_deadline": expired, "error_code": "AGENT_MODEL_RESULT_UNKNOWN",
        "final_candidate": None,
    }
    recovery = {
        "state": "BLOCKED_UNKNOWN", "cancel_request": None,
        "model_request": {
            "request_id": "request/1", "initial_wire_payload_digest": "sha256:wire",
        },
        "model_attempt": {
            "attempt_id": "attempt/1", "effect_operation_ref": "effect/1",
            "state": "UNKNOWN",
        },
        "tool_call": None,
    }
    executor._load_or_start = AsyncMock(return_value=run)
    executor._run = AsyncMock(return_value=run)
    executor._recovery = AsyncMock(return_value=recovery)
    executor._preset_context = AsyncMock(return_value=[])
    executor._renew_owner_until_done = AsyncMock()
    executor.model.reconcile_committed = AsyncMock(return_value=False)
    request = {
        "actor": {"conversation_id": "conversation/1"},
        "conversation_id": "conversation/1", "session_ref": "session/1",
        "run_ref": "run/1", "resume": True, "absolute_deadline": None,
    }

    result = await executor.execute(request)
    assert result["status"] == "BLOCKED_UNKNOWN"
    assert result["error_code"] == "AGENT_MODEL_RESULT_UNKNOWN"
    executor.model.reconcile_committed.assert_awaited_once()

    # A handed-off tool query has the same rule; the deadline cannot suppress
    # lookup of its original operation.
    recovery.update({
        "state": "WAIT_TOOL", "model_request": None, "model_attempt": None,
        "tool_call": {"tool_name": "diagnostic.echo", "state": "UNKNOWN"},
    })
    monkeypatch.setattr(
        "execution.tool_directory",
        lambda _directory, *, identity: ([], {"diagnostic.echo": {}}),
    )
    executor.tools.invoke = AsyncMock(return_value={
        "status": "UNKNOWN", "error_code": "AGENT_TOOL_RESULT_UNKNOWN",
    })
    tool_result = await executor.execute(request)
    assert tool_result["status"] == "BLOCKED_UNKNOWN"
    executor.tools.invoke.assert_awaited_once()

    # The same expired Run must still reject a state that could start new work.
    recovery["state"] = "PREPARING_MODEL"
    with pytest.raises(ForestError) as error:
        await executor.execute(request)
    assert error.value.code == "AGENT_DEADLINE_EXCEEDED"
    assert calls.count("agent.release-run-lease") == 3
