"""Job commands contributed by ``job.scheduler`` to the group Shell."""

from __future__ import annotations

import json

from forest.errors import ForestError
from forest.runtime_modules import RuntimeShellCommandContribution
from forest.shell_command_protocol import ShellCommandExecutionRequest, ShellCommandExecutionResult


def _schema(command_id: str, properties: dict, required=()) -> dict:
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


_ID = {"type": "string", "minLength": 1, "maxLength": 256}
_TYPE = {"type": "string", "pattern": "^[a-z0-9][a-z0-9._-]{0,127}$"}
_STATE = {
    "enum": ["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"]
}

JOB_SHELL_COMMANDS = (
    RuntimeShellCommandContribution(
        "jobs.list",
        1,
        "shell.jobs.execute",
        1,
        _schema(
            "jobs.list",
            {
                "limit": {"type": "integer", "minimum": 1, "maximum": 10},
                "state": _STATE,
                "job_type": _TYPE,
            },
        ),
        "job.read",
        "BOT",
        "P1",
        ("job.read",),
        (("type", "job_type"),),
    ),
    RuntimeShellCommandContribution(
        "jobs.inspect",
        1,
        "shell.jobs.execute",
        1,
        _schema("jobs.inspect", {"job_id": _ID}, ("job_id",)),
        "job.read",
        "BOT",
        "P1",
        ("job.read",),
    ),
    RuntimeShellCommandContribution(
        "jobs.run",
        1,
        "shell.jobs.execute",
        1,
        _schema(
            "jobs.run",
            {
                "job_type": _TYPE,
                "input_json": {"type": "string", "minLength": 2, "maxLength": 512},
            },
            ("job_type",),
        ),
        "job.run",
        "BOT",
        "P1",
        ("job.run",),
        (("type", "job_type"), ("input", "input_json")),
    ),
    RuntimeShellCommandContribution(
        "jobs.cancel",
        1,
        "shell.jobs.execute",
        1,
        _schema(
            "jobs.cancel",
            {
                "job_id": _ID,
                "expected_revision": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 9_223_372_036_854_775_807,
                },
                "reason_code": {
                    "type": "string",
                    "pattern": "^[A-Z][A-Z0-9_]{0,127}$",
                },
            },
            ("job_id", "expected_revision"),
        ),
        "job.cancel",
        "BOT",
        "P0",
        ("job.cancel",),
        (("expect-revision", "expected_revision"), ("reason", "reason_code")),
    ),
)


async def execute_job_shell(runtime, request: ShellCommandExecutionRequest):
    if type(request) is not ShellCommandExecutionRequest:
        raise ForestError("SHELL_EXECUTION_INPUT_INVALID", "Shell request is invalid", 422)
    arguments = request.arguments
    if request.command_id == "jobs.list":
        if set(arguments) - {"limit", "state", "job_type"}:
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Job query is invalid", 422)
        rows = await runtime.list_jobs(
            limit=arguments.get("limit", 10),
            state=arguments.get("state"),
            job_type=arguments.get("job_type"),
        )
        if not rows:
            return ShellCommandExecutionResult("Forest Jobs：没有匹配记录。")
        return ShellCommandExecutionResult(
            "Forest Jobs：\n"
            + "\n".join(
                f"{row['job_id']} {row['job_type']} {row['state']} r{row['revision']}"
                for row in rows
            )
        )
    if request.command_id == "jobs.inspect":
        if set(arguments) != {"job_id"}:
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Job inspection is invalid", 422)
        row = await runtime.inspect(arguments["job_id"])
        suffix = f" error={row['error_code']}" if row["error_code"] else ""
        return ShellCommandExecutionResult(
            f"Forest Job {row['job_id']}: type={row['job_type']} state={row['state']} "
            f"revision={row['revision']} attempt={row['attempt_count']}/{row['max_attempts']}"
            + suffix
        )
    if request.command_id == "jobs.run":
        if set(arguments) - {"job_type", "input_json"} or "job_type" not in arguments:
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Job request is invalid", 422)
        try:
            input_value = json.loads(arguments.get("input_json", "{}"))
        except json.JSONDecodeError as exc:
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Job input JSON is invalid", 422) from exc
        if not isinstance(input_value, dict):
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Job input must be an object", 422)
        saved = await runtime.enqueue(
            job_type=arguments["job_type"],
            operation_id="shell-job:" + request.request_id,
            requested_by=request.principal_ref,
            source_conversation_ref=request.conversation_ref,
            input_value=input_value,
        )
        runtime.notify()
        return ShellCommandExecutionResult(
            f"Forest Job {saved['state']}: id={saved['job_id']} "
            f"type={saved['job_type']} revision={saved['revision']}"
        )
    if request.command_id == "jobs.cancel":
        if set(arguments) - {"job_id", "expected_revision", "reason_code"} or not {
            "job_id",
            "expected_revision",
        } <= set(arguments):
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Job cancellation is invalid", 422)
        saved = await runtime.cancel(
            operation_id="shell-job-cancel:" + request.request_id,
            job_id=arguments["job_id"],
            expected_revision=arguments["expected_revision"],
            requested_by=request.principal_ref,
            reason_code=arguments.get("reason_code", "ADMIN_REQUESTED"),
        )
        runtime.notify()
        return ShellCommandExecutionResult(
            f"Forest Job cancellation accepted: id={saved['job_id']} "
            f"state={saved['resulting_state']} revision={saved['resulting_revision']}"
        )
    raise ForestError("SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409)
