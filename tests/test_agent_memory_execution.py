"""The Agent must keep one Memory query bound to each durable model input."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.model_contract import ModelCapabilities

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/forest/builtin/agent_runtime/python"))
from execution import AgentExecution
from model_execution import ModelExecution, _digest


class MemoryServices:
    def __init__(self, deadline: str):
        self.calls = []
        self.deadline = deadline
        self.capture_pending = False
        self.state = "PENDING"
        self.registered = False

    async def call(self, name, body, **_kwargs):
        self.calls.append((name, body))
        if name == "memory.query.begin":
            if self.capture_pending:
                raise ForestError("MEMORY_QUERY_CAPTURE_PENDING", "capture pending", 409)
            return {**body, "protocol": "forest.memory-query/1", "space_id": "space",
                    "query_mode": "TEXT", "part_count": 1}
        if name == "memory.query.request":
            self.registered = True
            return {**body, "protocol": "forest.memory-recall-requested/1"}
        if name == "memory.query.work-status":
            if not self.registered:
                raise ForestError("MEMORY_RECALL_NOT_REQUESTED", "not requested", 404)
            return {"query_id": body["query_id"], "deadline_at": self.deadline,
                    "state": self.state, "evidence_ref": {"payload_id": "evidence"}
                    if self.state == "READY" else None,
                    "error_code": None}
        if name == "memory.query.cancel":
            return {"query_id": body["query_id"], "status": "CANCELLED"}
        raise AssertionError(name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_error", "expected"),
    [("PAYLOAD_REVOKED", "AGENT_FROZEN_MODEL_INVALID"),
     ("PAYLOAD_DIGEST_MISMATCH", "AGENT_FROZEN_MODEL_INVALID"),
     ("PAYLOAD_AUTHORITY_MISSING", "PAYLOAD_AUTHORITY_MISSING")],
)
async def test_frozen_model_payload_failure_is_terminal_only_when_definitive(
    monkeypatch, payload_error, expected
):
    async def profile(_request_id):
        return ({"profile_ref": "profile/1", "capability_digest": "digest/1"},
                SimpleNamespace(model="model/1"))

    async def frozen(_name, _body):
        return {"request_id": "request/1", "provider_profile_ref": "profile/1",
                "capability_digest": "digest/1", "model_ref": "model/1",
                "projection": {"owner_id": "request/1"}, "wire": {"owner_id": "attempt/1"}}

    async def unreadable(_reference):
        raise ForestError(payload_error, "payload unavailable", 409)

    model = ModelExecution(SimpleNamespace(
        identity={"graph_id": "graph/1", "graph_revision": 1, "bot_id": "bot/1"},
        queries=SimpleNamespace(call=frozen),
        payloads=SimpleNamespace(read_bytes=unreadable),
    ))
    monkeypatch.setattr(model, "_profile", profile)
    with pytest.raises(ForestError) as failure:
        await model.resume_frozen(
            owner={"run_id": "run/1"}, request_id="request/1",
            attempt_id="attempt/1", effect_operation_id="effect/1",
        )
    assert failure.value.code == expected


@pytest.mark.asyncio
async def test_inheritance_transaction_failure_keeps_its_own_error():
    async def put_bytes(_content, **kwargs):
        return {"digest": "sha256:wire" if kwargs["owner_kind"] == "model.wire"
                else "sha256:projection"}

    async def fail_inheritance(_name, _body, **_kwargs):
        raise ForestError("PAYLOAD_REVOKED", "effect participant failure", 409)

    model = ModelExecution(SimpleNamespace(
        payloads=SimpleNamespace(put_bytes=put_bytes),
        uow=SimpleNamespace(call=fail_inheritance),
    ))
    frozen = {
        "request_id": "request/1", "projection": {"payload_id": "projection/1"},
        "wire": {"payload_id": "wire/1", "digest": "sha256:wire"},
        "attempts": [], "projected_content_digest": "sha256:content",
        "source_manifest_digest": "sha256:manifest", "output_token_budget": 256,
    }
    profile = {
        "generation_id": "provider/2", "component_id": "provider.component",
        "profile_ref": "profile/1", "capability_digest": "sha256:capability",
    }
    with pytest.raises(ForestError) as failure:
        await model._inherit_frozen(
            owner={"run_id": "run/1"}, frozen=frozen, profile=profile,
            target=ModelCapabilities("deployment/1", "model/1", "revision/1"),
            manifest={}, wire_bytes=b"wire",
        )
    assert failure.value.code == "PAYLOAD_REVOKED"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("write_error", "expected"),
    [("PAYLOAD_PARENT_UNAVAILABLE", "AGENT_FROZEN_MODEL_INVALID"),
     ("PAYLOAD_AUTHORITY_MISSING", "PAYLOAD_AUTHORITY_MISSING")],
)
async def test_frozen_unsent_retry_classifies_payload_write(write_error, expected):
    request_id = "request/unsent"
    source_manifest = {"input_batch": "batch/1"}
    manifest = {
        "request_id": request_id,
        "projected_content_digest": "sha256:content",
        "source_manifest": source_manifest,
        "serializer_revision": "serializer/1",
    }
    body = {"model": "model/1", "stream": False, "max_completion_tokens": 256}
    frozen = {
        "request_id": request_id, "provider_profile_ref": "profile/1",
        "capability_digest": "sha256:capability", "model_ref": "model/1",
        "provider_generation_id": "provider/1", "state": "NOT_APPLIED",
        "projection": {"owner_id": request_id, "owner_kind": "model.projection"},
        "wire": {"owner_id": "attempt/old", "owner_kind": "model.wire",
                 "digest": "sha256:wire"},
        "projected_content_digest": "sha256:content",
        "source_manifest_digest": _digest(source_manifest),
        "output_token_budget": 256,
    }

    async def read_bytes(ref):
        return json.dumps(manifest if ref["owner_kind"] == "model.projection"
                          else body).encode()

    async def fail_write(_content, **_kwargs):
        raise ForestError(write_error, "retry wire unavailable", 409)

    async def frozen_query(_name, _body):
        return frozen

    model = ModelExecution(SimpleNamespace(
        identity={"graph_id": "graph/1", "graph_revision": 1, "bot_id": "bot/1"},
        queries=SimpleNamespace(call=frozen_query),
        payloads=SimpleNamespace(read_bytes=read_bytes, put_bytes=fail_write),
    ))

    async def profile(_request_id):
        return ({"profile_ref": "profile/1", "capability_digest": "sha256:capability",
                 "generation_id": "provider/1"},
                ModelCapabilities("deployment/1", "model/1", "revision/1"))

    model._profile = profile
    with pytest.raises(ForestError) as failure:
        await model.resume_frozen(
            owner={"run_id": "run/1"}, request_id=request_id,
            attempt_id="attempt/new", effect_operation_id="effect/new",
        )
    assert failure.value.code == expected


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
        lambda: None,
    )
    services.capture_pending = True
    waiting = await agent._recall(durable, 3)
    assert waiting[0] == "PENDING"
    assert [name for name, _ in services.calls] == [
        "memory.query.work-status", "memory.query.begin"
    ]
    services.capture_pending = False
    first = await agent._recall(durable, 3)
    assert first == ("PENDING", waiting[1], None)
    services.state = "READY"
    ready = await agent._recall(durable, 3)
    assert ready == ("READY", waiting[1], {"payload_id": "evidence"})
    requested = [body for name, body in services.calls if name == "memory.query.request"]
    assert len(requested) == 1
    assert requested[0]["deadline_at"] == frozen_deadline
    assert all(body["input_batch_ref"] == "batch-new" for name, body in services.calls
               if name == "memory.query.begin")
    await agent._cancel_superseded_recall(durable, 3)
    assert services.calls[-1] == (
        "memory.query.cancel", {"query_id": waiting[1], "reason_code": "SUPERSEDED"}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_during_recall", [False, True])
async def test_replacement_parks_before_next_model_call(
    monkeypatch, signal_during_recall
):
    durable = run()
    durable["state"] = "PREPARING_MODEL"
    durable["progress"].update(checkpoint_seq=0, next_step_seq=1)
    operation_id = "runtime-reload/agent/1"
    signal = [not signal_during_recall]
    calls = []
    payloads = []

    async def uow_call(name, body, **_kwargs):
        calls.append(name)
        if name == "agent.acquire-run-lease":
            return {"run_id": durable["run_id"], "owner_id": body["owner_id"],
                    "epoch": 1}
        if name == "agent.park-reload":
            assert body["reload_operation_id"] == operation_id
            assert body["resume_phase"] == "PREPARING_MODEL"
            return durable
        assert name == "agent.release-run-lease"

    async def put_json(body, **_kwargs):
        payloads.append(body)
        return {"payload_id": "checkpoint", "digest": "sha256:" + "0" * 64,
                "owner_component_id": "agent.runtime", "owner_kind": "checkpoint.state",
                "owner_id": "checkpoint"}

    async def read_json(_ref):
        return {"system": ["You are an Agent."]}

    async def tools_list():
        return {"protocol": "forest.contribution-directory/1", "kind": "agent-tools",
                "graph_id": "graph/1", "graph_revision": 1, "items": []}

    context = SimpleNamespace(
        uow=SimpleNamespace(call=uow_call),
        payloads=SimpleNamespace(put_json=put_json, read_json=read_json),
        agent_tools=SimpleNamespace(list=tools_list),
        identity={"graph_id": "graph/1", "graph_revision": 1},
        operations=SimpleNamespace(stable=lambda value: value),
    )
    agent = AgentExecution(
        context,
        {"preset": {"payload_id": "preset", "digest": "sha256:" + "0" * 64,
                    "owner_id": "preset/1"}, "max_steps": 32},
        lambda: operation_id if signal[0] else None,
    )

    async def load_or_start(_request):
        return durable

    async def recovery(_run_id):
        return {"state": "PREPARING_MODEL", "next_step_seq": 1}

    async def current_run(_run_id):
        return durable

    async def forbidden_model_call(**_kwargs):
        raise AssertionError("replacement must park before dispatching another model call")

    monkeypatch.setattr(agent, "_load_or_start", load_or_start)
    monkeypatch.setattr(agent, "_recovery", recovery)
    monkeypatch.setattr(agent, "_run", current_run)
    monkeypatch.setattr(agent.model, "invoke", forbidden_model_call)
    if signal_during_recall:
        async def acquire_input(*_args):
            return {"status": "READY"}

        async def recall(*_args):
            signal[0] = True
            return "READY", "query/1", None

        async def model_snapshot(*_args):
            return {}

        monkeypatch.setattr(agent, "_acquire_input", acquire_input)
        monkeypatch.setattr(agent, "_recall", recall)
        monkeypatch.setattr(agent, "_model_snapshot", model_snapshot)
    result = await agent.execute({
        "actor": {"conversation_id": durable["conversation_ref"]},
        "conversation_id": durable["conversation_ref"],
        "absolute_deadline": None,
        "resume": True,
        "session_ref": "session/1",
        "run_ref": durable["run_id"],
    })
    assert result["status"] == "RELOAD_WAIT"
    assert payloads[0]["reload_operation_id"] == operation_id
    assert calls == ["agent.acquire-run-lease", "agent.park-reload",
                     "agent.release-run-lease"]
