"""The kernel-owned tool uses the same receipt protocol as package tools."""

from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.kernel_runtime_tool import runtime_status_tool_service


@pytest.mark.asyncio
async def test_runtime_status_tool_returns_agent_receipt_with_owned_payload():
    writes = []

    class Runtime:
        async def status(self):
            return {
                "bot_id": "bot-1",
                "status": "ACTIVE",
                "maintenance_epoch": 0,
                "data_admission": "OPEN",
                "control_admission": "OPEN",
                "graph_id": "graph-1",
                "graph_revision": 1,
                "graph_digest": "sha256:" + "a" * 64,
                "graph_state": "ACTIVE",
                "members": [],
            }

    class Payloads:
        async def epoch(self):
            return 4

        async def put_bytes(self, **values):
            writes.append(values)
            return SimpleNamespace(
                payload_id="payload-1", content_digest="sha256:" + "b" * 64
            )

    runtime = SimpleNamespace(service=Runtime())
    payloads = SimpleNamespace(service=Payloads())
    context = SimpleNamespace(
        participant=lambda name: {
            "kernel.runtime": runtime,
            "kernel.payload": payloads,
        }[name]
    )
    dispatch = runtime_status_tool_service(context)
    command = {
        "protocol": "forest.agent-tool-execute/1",
        "component_id": "kernel.runtime",
        "service_id": "agent.tool.runtime-status",
        "arguments": {},
        "tool_call_id": "call-1",
        "effect_operation_id": "effect-1",
        "effect_attempt_id": "attempt-1",
        "authority_principal_ref": "qq/42",
    }

    receipt = await dispatch(command)

    assert receipt == {
        "protocol": "forest.agent-tool-receipt/1",
        "status": "SUCCEEDED",
        "tool_call_id": "call-1",
        "effect_operation_id": "effect-1",
        "effect_attempt_id": "attempt-1",
        "result": {
            "payload_id": "payload-1",
            "digest": "sha256:" + "b" * 64,
            "owner_component_id": "kernel.runtime",
            "owner_kind": "tool.result",
            "owner_id": "call-1",
        },
        "error_code": None,
        "observation_ref": "runtime-status:call-1",
    }
    assert writes[0]["expected_deletion_epoch"] == 4
    assert writes[0]["sources"][0].author_subject_id == "qq/42"
    assert writes[0]["sources"][0].source_component_id == "agent.runtime"

    with pytest.raises(ForestError) as caught:
        await dispatch({**command, "arguments": {"unexpected": True}})
    assert caught.value.code == "RUNTIME_STATUS_TOOL_INPUT_INVALID"
    assert len(writes) == 1
