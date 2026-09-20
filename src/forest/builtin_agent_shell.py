"""Single-Agent Run control commands contributed by ``agent.runtime``."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_modules import RuntimeShellCommandContribution
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)


def _schema(command_id: str, properties: dict, required: tuple[str, ...]) -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.arguments/1"},
            "command_id": {"const": command_id},
            "arguments": {
                "type": "object",
                "properties": properties,
                "required": list(required),
                "additionalProperties": False,
            },
        },
        "required": ["protocol", "command_id", "arguments"],
        "additionalProperties": False,
    }


_RUN_ID = {"type": "string", "minLength": 1, "maxLength": 256}

AGENT_SHELL_COMMANDS = (
    RuntimeShellCommandContribution(
        "run.inspect",
        1,
        "shell.agent.execute",
        1,
        _schema("run.inspect", {"run_id": _RUN_ID}, ("run_id",)),
        "agent.run.read",
        "BOT",
        "P1",
        ("agent.run.read",),
    ),
    RuntimeShellCommandContribution(
        "run.cancel",
        1,
        "shell.agent.execute",
        1,
        _schema(
            "run.cancel",
            {
                "run_id": _RUN_ID,
                "expected_checkpoint_seq": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 9_223_372_036_854_775_807,
                },
                "reason_code": {"type": "string", "minLength": 1, "maxLength": 128},
            },
            ("run_id", "expected_checkpoint_seq"),
        ),
        "agent.run.cancel",
        "BOT",
        "P0",
        ("agent.run.cancel",),
        (
            ("expect-checkpoint", "expected_checkpoint_seq"),
            ("reason", "reason_code"),
        ),
    ),
)


def _cancel_status(run: dict) -> str:
    request = run.get("cancel_request")
    return "none" if request is None else f"requested:{request['operation_id']}"


async def execute_agent_shell(runtime, request: ShellCommandExecutionRequest):
    if type(request) is not ShellCommandExecutionRequest:
        raise ForestError("SHELL_EXECUTION_INPUT_INVALID", "Shell request is invalid", 422)
    if request.command_id == "run.inspect":
        if set(request.arguments) != {"run_id"}:
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Run query is invalid", 422)
        run = await runtime.load(request.arguments["run_id"])
        return ShellCommandExecutionResult(
            f"Forest Agent Run {run['run_id']}: state={run['state']} "
            f"checkpoint={run['checkpoint_seq']} input=r{run['input_revision']} "
            f"session={run['session_id']} conversation={run['session']['conversation_ref']} "
            f"cancel={_cancel_status(run)}"
        )
    if request.command_id == "run.cancel":
        if set(request.arguments) - {
            "run_id",
            "expected_checkpoint_seq",
            "reason_code",
        } or not {"run_id", "expected_checkpoint_seq"} <= set(request.arguments):
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Run cancellation is invalid", 422)
        saved = await runtime.request_cancel(
            operation_id="shell-cancel:" + request.request_id,
            run_id=request.arguments["run_id"],
            expected_checkpoint_seq=request.arguments["expected_checkpoint_seq"],
            requested_by=request.principal_ref,
            reason_code=request.arguments.get("reason_code", "ADMIN_REQUESTED"),
        )
        return ShellCommandExecutionResult(
            f"Forest Agent Run cancellation accepted: run={saved['run_id']} "
            f"checkpoint={saved['expected_checkpoint_seq']} "
            f"operation={saved['operation_id']}"
        )
    raise ForestError("SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409)
