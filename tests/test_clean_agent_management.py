from __future__ import annotations

from datetime import datetime, timezone

import pytest

from forest.clean_agent_management import CleanAgentManagementContract
from forest.errors import ForestError


class FakeRuntime:
    def __init__(self):
        self.query_values = None
        self.cancel_values = None

    async def query_runs(self, **values):
        self.query_values = values
        return {"items": [], "has_more": False}

    async def load_recovery(self, run_id):
        assert run_id == "run/one"
        return {
            "run_id": run_id,
            "session_id": "session/one",
            "state": "WAIT_APPROVAL",
            "checkpoint_seq": 4,
            "input_revision": 2,
            "selected_input_end_seq": 8,
            "next_step_seq": 3,
            "current_model_request_id": None,
            "current_tool_call_id": "tool/one",
            "absolute_deadline": datetime.now(timezone.utc),
            "error_code": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "terminal_at": None,
            "session": {"conversation_ref": "onebot:group:933179609"},
            "cancel_request": None,
            "triggers": [
                {
                    "trigger_ref": "trigger/one",
                    "principal_ref": "qq:42",
                    "ingress_seq": 8,
                    "input_revision": 2,
                }
            ],
            "current_request": None,
            "current_attempt": None,
            "current_tool": {"tool_call_id": "tool/one", "state": "WAIT_APPROVAL"},
            "latest_checkpoint": {"checkpoint_seq": 4, "phase": "WAIT_APPROVAL"},
        }

    async def execution_history(self, run_id):
        assert run_id == "run/one"
        return ({"step_seq": 1, "terminal": False},)

    async def request_cancel(self, **values):
        self.cancel_values = values
        return {
            **values,
            "created_at": datetime.now(timezone.utc),
        }


def _contract(runtime):
    return CleanAgentManagementContract(
        object(),
        payloads=object(),
        runtime=runtime,
        storage_schema={"type": "object"},
    )


@pytest.mark.asyncio
async def test_agent_management_queries_are_bounded_and_project_run_history():
    runtime = FakeRuntime()
    contract = _contract(runtime)
    snapshot = "2026-09-20T08:00:00+00:00"
    result = await contract.query(
        "agent.runs.query",
        {
            "states": ["WAIT_APPROVAL"],
            "conversation_ref": "onebot:group:933179609",
            "snapshot_created_at": snapshot,
            "limit": 25,
        },
    )
    assert result == {"items": [], "has_more": False}
    assert runtime.query_values == {
        "states": ("WAIT_APPROVAL",),
        "conversation_ref": "onebot:group:933179609",
        "snapshot_created_at": datetime.fromisoformat(snapshot),
        "limit": 25,
    }

    detail = await contract.query("agent.run.read", {"run_id": "run/one"})
    assert detail["conversation_ref"] == "onebot:group:933179609"
    assert detail["triggers"][0]["principal_ref"] == "qq:42"
    assert detail["current_tool"]["state"] == "WAIT_APPROVAL"
    assert detail["history"] == [{"step_seq": 1, "terminal": False}]

    with pytest.raises(ForestError) as caught:
        await contract.query("agent.runs.query", {"limit": 10, "unknown": True})
    assert caught.value.code == "MODULE_MANAGEMENT_INPUT_INVALID"


@pytest.mark.asyncio
async def test_agent_management_cancel_uses_checkpoint_cas_and_stable_operation():
    runtime = FakeRuntime()
    result = await _contract(runtime).action(
        "agent.run.cancel",
        operation_id="web:cancel/one",
        payload={"run_id": "run/one", "expected_checkpoint_seq": 4},
    )

    assert result.replace_runtime_graph is False
    assert result.value["run_id"] == "run/one"
    assert runtime.cancel_values == {
        "operation_id": "web:cancel/one",
        "run_id": "run/one",
        "expected_checkpoint_seq": 4,
        "requested_by": "management.web",
        "reason_code": "ADMIN_REQUESTED",
    }
