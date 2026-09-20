from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from forest.agent_execution import DurableSingleAgentExecution
from forest.conversation_worker import AgentExecutionRequest, AgentReloadRequested
from forest.errors import ForestError


class _Agent:
    bot_id = "bot/test"

    def __init__(self):
        self.owner = SimpleNamespace(run_id="run/one", owner_id="worker/one", epoch=1)
        self.transition_values = None
        self.released = False

    async def load(self, _run_id):
        return {
            "run_id": "run/one",
            "session_id": "session/one",
            "session": {"conversation_ref": "group/one"},
            "state": "PREPARING_MODEL",
            "next_step_seq": 1,
            "selected_input_end_seq": 1,
            "inputs": [
                {
                    "input_revision": 1,
                    "input_batch_ref": "batch/one",
                    "context_revision": 1,
                }
            ],
        }

    async def acquire_run(self, *_args, **_values):
        return self.owner

    async def load_recovery(self, _run_id):
        return {
            "run_id": "run/one",
            "state": "PREPARING_MODEL",
            "next_step_seq": 1,
            "current_request": None,
        }

    def profile(self):
        return SimpleNamespace(
            max_steps=8,
            preset_context=(),
            output_tokens=256,
            stream=False,
        )

    async def transition(self, owner, **values):
        assert owner is self.owner
        self.transition_values = values
        return {"phase": values["target_phase"]}

    async def release_run(self, owner):
        assert owner is self.owner
        self.released = True


class _Payloads:
    def __init__(self):
        self.content = None

    async def epoch(self):
        return 7

    async def put_bytes(self, **values):
        assert values["expected_deletion_epoch"] == 7
        self.content = json.loads(values["content"])
        return SimpleNamespace(
            payload_id="payload/reload",
            content_digest="sha256:" + "a" * 64,
        )


class _Projector:
    async def project(self, **_values):
        return {"units": []}


class _Memory:
    async def recall(self, **_values):
        return None


class _Media:
    async def resolve_context(self, _snapshot):
        return {}


class _Model:
    async def execute(self, *_args, **_values):
        raise AssertionError("quiescing Agent must not dispatch a new model call")


@pytest.mark.asyncio
async def test_agent_parks_durable_reload_checkpoint_before_external_model_call():
    agent = _Agent()
    payloads = _Payloads()
    execution = object.__new__(DurableSingleAgentExecution)
    execution.agent = agent
    execution.payloads = payloads
    execution.projector = _Projector()
    execution.memory = _Memory()
    execution.media = _Media()
    execution.model = _Model()
    execution.routes = {}
    execution.owner_id = "agent-execution/test"

    async def context_snapshot():
        return {"conversation_id": "group/one"}

    async def next_model_input():
        return None

    async def before_external_step():
        raise AgentReloadRequested("replacement/one")

    request = AgentExecutionRequest(
        conversation_id="group/one",
        session_ref="session/one",
        run_ref="run/one",
        initial_batch=None,
        recovery=True,
        context_snapshot=context_snapshot,
        next_model_input=next_model_input,
        before_external_step=before_external_step,
    )

    with pytest.raises(ForestError) as caught:
        await execution.execute(request)

    assert caught.value.code == "AGENT_RELOAD_WAIT"
    assert caught.value.details == {"operation_id": "replacement/one"}
    assert payloads.content == {
        "phase": "RELOAD_WAIT",
        "operation_id": "replacement/one",
        "resume_phase": "PREPARING_MODEL",
    }
    assert agent.transition_values["expected_phase"] == "PREPARING_MODEL"
    assert agent.transition_values["target_phase"] == "RELOAD_WAIT"
    assert agent.transition_values["reload_operation_id"] == "replacement/one"
    assert agent.transition_values["reload_resume_phase"] == "PREPARING_MODEL"
    assert agent.released is True
