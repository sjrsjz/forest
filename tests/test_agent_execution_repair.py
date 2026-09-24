from types import SimpleNamespace

import pytest

from forest.agent_execution import DurableSingleAgentExecution
from forest.builtin.agent_runtime.python.agent_protocol import ProtocolRepairView
from forest.conversation_worker import AgentExecutionRequest


class RepairDispatched(Exception):
    pass


class Agent:
    bot_id = "bot/one"

    def __init__(self):
        self.owner = SimpleNamespace(run_id="run/one")
        self.released = False
        self.failure = {
            "resulting_state": "REPAIR_READY",
            "repair_step_seq": 2,
            "failed_request_id": "request/primary",
            "error_code": "AGENT_PROTOCOL_XML",
            "candidate_digest": "sha256:" + "a" * 64,
            "feedback_payload_id": "payload/repair",
        }

    async def load(self, _run_id):
        return {
            "run_id": "run/one",
            "session_id": "session/one",
            "session": {"conversation_ref": "conversation/one"},
            "state": "REPAIR_READY",
            "next_step_seq": 2,
            "input_revision": 1,
            "context_revision": 4,
            "context_view_revision": 3,
            "selected_input_end_seq": 9,
            "triggers": [{"trigger_ref": "trigger/one"}],
            "inputs": [{"input_batch_ref": "batch/one"}],
        }

    async def load_recovery(self, _run_id):
        return {
            "run_id": "run/one",
            "state": "REPAIR_READY",
            "next_step_seq": 2,
            "repair_count": 1,
            "current_request": None,
            "current_protocol_failure": self.failure,
            "cancel_request": None,
        }

    async def acquire_run(self, *_args, **_kwargs):
        return self.owner

    async def release_run(self, owner):
        assert owner is self.owner
        self.released = True

    def profile(self):
        return SimpleNamespace(
            max_steps=8,
            preset_context=("must not enter repair",),
            output_tokens=256,
            stream=False,
        )


class Payloads:
    async def read_bytes(self, payload_id):
        assert payload_id == "payload/repair"
        return (
            b'{"candidate_digest":"sha256:'
            + b"a" * 64
            + b'","error_code":"AGENT_PROTOCOL_XML","feedback":"plain answer",'
            b'"repair_count":1,"validation_reason":"not XML"}'
        )


class ForbiddenPort:
    def __getattr__(self, name):
        async def forbidden(*_args, **_kwargs):
            raise AssertionError(f"repair called forbidden port: {name}")

        return forbidden


class Model:
    async def execute(self, _owner, _step, snapshot, **values):
        assert snapshot["units"] == []
        assert snapshot["context_revision"] == 4
        assert values["preset_context"] == ()
        assert values["media"] is None
        assert values["evidence"] is None
        assert values["request_kind"] == "REPAIR"
        assert values["repair_of_request_id"] == "request/primary"
        assert type(values["repair_view"]) is ProtocolRepairView
        assert values["repair_view"].invalid_candidate == "plain answer"
        raise RepairDispatched


@pytest.mark.asyncio
async def test_repair_dispatch_never_reads_context_memory_or_media():
    agent = Agent()
    execution = object.__new__(DurableSingleAgentExecution)
    execution.agent = agent
    execution.model = Model()
    execution.tools = ForbiddenPort()
    execution.memory = ForbiddenPort()
    execution.media = ForbiddenPort()
    execution.payloads = Payloads()
    execution.projector = ForbiddenPort()
    execution.owner_id = "worker/one"
    execution.routes = {}

    async def forbidden_input():
        raise AssertionError("repair read a Conversation input port")

    request = AgentExecutionRequest(
        conversation_id="conversation/one",
        session_ref="session/one",
        run_ref="run/one",
        initial_batch=None,
        recovery=True,
        context_snapshot=forbidden_input,
        next_model_input=forbidden_input,
        before_external_step=lambda: _completed(),
    )
    with pytest.raises(RepairDispatched):
        await execution.execute(request)
    assert agent.released is True


async def _completed():
    return None
