from types import SimpleNamespace

import pytest

from forest.builtin.shell_command.python.shell_command import ShellCommand
from forest.errors import ForestError


@pytest.mark.parametrize(
    ("error_code", "expected_state"),
    [
        ("CALL_TIMEOUT", "UNKNOWN"),
        ("SERVICE_OPERATION_UNKNOWN", "UNKNOWN"),
        ("SERVICE_OPERATION_IN_PROGRESS", "UNKNOWN"),
        ("SHELL_ROUTE_STALE", "FAILED"),
    ],
)
async def test_shell_does_not_report_an_uncertain_command_as_failed(
    error_code, expected_state
):
    calls = []

    async def uow_call(name, payload, **kwargs):
        calls.append((name, payload))
        return {}

    async def service_call(name, payload, **kwargs):
        assert name == "kernel.payload.json-write"
        return {"payload": {"payload_id": "shell-result", "digest": "sha256:" + "a" * 64}}

    async def command_execute(**kwargs):
        raise ForestError(error_code, "command outcome unavailable", 504)

    context = SimpleNamespace(
        identity={"bot_id": "bot/test", "generation_id": "shell/generation"},
        operations=SimpleNamespace(stable=lambda key: "operation:" + key),
        uow=SimpleNamespace(call=uow_call),
        services=SimpleNamespace(call=service_call),
        shell_commands=SimpleNamespace(execute=command_execute),
    )
    task = {
        "attempt": 1,
        "payload": {
            "request_id": "request/1",
            "claim_token": "claim/1",
            "command_id": "core.status",
            "command_version": 1,
            "target_component_id": "component.worker-host",
            "target_generation_id": "worker/generation",
            "target_service_id": "core.status",
            "target_service_version": 1,
            "arguments": {},
            "principal_ref": "qq:1",
            "conversation_ref": "onebot:group:933179609",
            "timeout_seconds": 10,
            "binding_ref": "binding/1",
        },
    }

    result = await ShellCommand().execute(task, context)

    assert result == {"disposition": "SUCCEEDED", "request_id": "request/1"}
    assert [name for name, _ in calls] == ["shell.authorize-execution", "shell.complete"]
    shell_result = calls[-1][1]["shell"]
    assert shell_result["resulting_state"] == expected_state
    assert shell_result["error_code"] == error_code
    assert calls[-1][1]["delivery"]["lane_kind"] == "CONTROL"
