"""Bot audit queries contributed by ``kernel.audit``."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_modules import RuntimeShellCommandContribution
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)


def _tail_schema() -> dict:
    text = {"type": "string", "minLength": 1, "maxLength": 512}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.arguments/1"},
            "command_id": {"const": "audit.tail"},
            "arguments": {
                "type": "object",
                "properties": {
                    "action": {**text, "maxLength": 128},
                    "producer_component_id": {**text, "maxLength": 128},
                    "origin_kind": {**text, "maxLength": 64},
                    "resource_kind": {**text, "maxLength": 64},
                    "resource_id": text,
                    "operation_kind": {**text, "maxLength": 64},
                    "operation_id": {**text, "maxLength": 256},
                    "before": {**text, "maxLength": 128},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 10},
                },
                "additionalProperties": False,
            },
        },
        "required": ["protocol", "command_id", "arguments"],
        "additionalProperties": False,
    }


AUDIT_SHELL_COMMANDS = (
    RuntimeShellCommandContribution(
        "audit.tail",
        1,
        "shell.audit.execute",
        1,
        _tail_schema(),
        "audit.query",
        "BOT",
        "P1",
        ("audit.query",),
    ),
)


def _field(value: object) -> str:
    return "-" if value is None else str(value)


def audit_shell_service(audit):

    async def execute(request: ShellCommandExecutionRequest):
        if type(request) is not ShellCommandExecutionRequest or request.command_id != "audit.tail":
            raise ForestError("SHELL_EXECUTION_INPUT_INVALID", "Shell request is invalid", 422)
        arguments = dict(request.arguments)
        for kind, identity in (
            ("resource_kind", "resource_id"),
            ("operation_kind", "operation_id"),
        ):
            if (kind in arguments) != (identity in arguments):
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID",
                    f"{kind} and {identity} must be supplied together",
                    422,
                )
        page = await audit.scan(limit=arguments.pop("limit", 10), **arguments)
        if not page["records"]:
            return ShellCommandExecutionResult("Forest audit: no matching records")
        lines = []
        for item in page["records"]:
            actor = f"{_field(item['actor_kind'])}:{_field(item['actor_id'])}"
            resource = f"{_field(item['resource_kind'])}:{_field(item['resource_id'])}"
            operation = f"{_field(item['operation_kind'])}:{_field(item['operation_id'])}"
            lines.append(
                f"{item['created_at']} {item['severity']} {item['action']} "
                f"producer={item['producer_component_id']} actor={actor} "
                f"resource={resource} operation={operation} id={item['audit_id']}"
            )
        if page["next_before"] is not None:
            lines.append(f"next: --before {page['next_before']}")
        return ShellCommandExecutionResult("Forest audit:\n" + "\n".join(lines))

    return execute
