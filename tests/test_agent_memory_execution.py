"""The Agent must keep one Memory query bound to each durable model input."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/agent_runtime/python"))
from execution import AgentExecution


class MemoryServices:
    def __init__(self, deadline: str):
        self.calls = []
        self.deadline = deadline
        self.capture_pending = False
        self.state = "PENDING"

    async def call(self, name, body, **_kwargs):
        self.calls.append((name, body))
        if name == "memory.query.begin":
            if self.capture_pending:
                raise ForestError("MEMORY_QUERY_CAPTURE_PENDING", "capture pending", 409)
            return {**body, "protocol": "forest.memory-query/1", "space_id": "space",
                    "query_mode": "TEXT", "part_count": 1}
        if name == "memory.query.request":
            return {**body, "protocol": "forest.memory-recall-requested/1"}
        if name == "memory.query.work-status":
            return {"query_id": body["query_id"], "deadline_at": self.deadline,
                    "state": self.state, "evidence_ref": {"payload_id": "evidence"}
                    if self.state == "READY" else None,
                    "error_code": None}
        if name == "memory.query.cancel":
            return {"query_id": body["query_id"], "status": "CANCELLED"}
        raise AssertionError(name)


def run():
    created = datetime.now(UTC) - timedelta(seconds=10)
    deadline = created + timedelta(minutes=15)
    return {
        "run_id": "run-one", "conversation_ref": "onebot:group:933179609",
        "created_at": created.isoformat(), "absolute_deadline": deadline.isoformat(),
        "progress": {"input_revision": 2},
        "inputs": [{"input_revision": 1, "input_batch_ref": "batch-old",
                    "context_revision": 4, "created_at": created.isoformat()},
                   {"input_revision": 2, "input_batch_ref": "batch-new",
                    "context_revision": 5,
                    "created_at": (created + timedelta(seconds=5)).isoformat()}],
    }


@pytest.mark.asyncio
async def test_recall_waits_for_capture_then_replays_one_frozen_query():
    durable = run()
    frozen_deadline = (datetime.fromisoformat(durable["created_at"])
                       + timedelta(minutes=9)).isoformat()
    services = MemoryServices(frozen_deadline)
    agent = AgentExecution(
        SimpleNamespace(services=services,
                        operations=SimpleNamespace(stable=lambda value: value)),
        {},
    )
    services.capture_pending = True
    waiting = await agent._recall(durable, 3)
    assert waiting[0] == "PENDING"
    assert [name for name, _ in services.calls] == ["memory.query.begin"]
    services.capture_pending = False
    first = await agent._recall(durable, 3)
    assert first == ("PENDING", waiting[1], None)
    services.state = "READY"
    ready = await agent._recall(durable, 3)
    assert ready == ("READY", waiting[1], {"payload_id": "evidence"})
    requested = [body for name, body in services.calls if name == "memory.query.request"]
    assert len(requested) == 2
    assert requested[0] == requested[1]
    assert requested[0]["deadline_at"] == frozen_deadline
    assert all(body["input_batch_ref"] == "batch-new" for name, body in services.calls
               if name == "memory.query.begin")
    await agent._cancel_superseded_recall(durable, 3)
    assert services.calls[-1] == (
        "memory.query.cancel", {"query_id": waiting[1], "reason_code": "SUPERSEDED"}
    )
