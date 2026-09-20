"""Runtime control commands contributed by ``kernel.runtime``."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_modules import RuntimeShellCommandContribution
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)


def _empty_schema(command_id: str) -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.arguments/1"},
            "command_id": {"const": command_id},
            "arguments": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        "required": ["protocol", "command_id", "arguments"],
        "additionalProperties": False,
    }


def _maintenance_schema(command_id: str) -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.arguments/1"},
            "command_id": {"const": command_id},
            "arguments": {
                "type": "object",
                "properties": {
                    "expected_maintenance_epoch": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 9_223_372_036_854_775_807,
                    }
                },
                "required": ["expected_maintenance_epoch"],
                "additionalProperties": False,
            },
        },
        "required": ["protocol", "command_id", "arguments"],
        "additionalProperties": False,
    }


def _reload_schema() -> dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.arguments/1"},
            "command_id": {"const": "core.reload"},
            "arguments": {
                "type": "object",
                "properties": {
                    "expected_graph_revision": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 9_223_372_036_854_775_807,
                    }
                },
                "required": ["expected_graph_revision"],
                "additionalProperties": False,
            },
        },
        "required": ["protocol", "command_id", "arguments"],
        "additionalProperties": False,
    }


RUNTIME_SHELL_COMMANDS = (
    RuntimeShellCommandContribution(
        "core.status",
        1,
        "shell.core-runtime.execute",
        1,
        _empty_schema("core.status"),
        "runtime.status",
        "BOT",
        "P0",
        ("runtime.status.read",),
    ),
    RuntimeShellCommandContribution(
        "core.pause",
        1,
        "shell.core-runtime.execute",
        1,
        _maintenance_schema("core.pause"),
        "runtime.maintenance",
        "BOT",
        "P0",
        ("runtime.maintenance",),
        (("expect-maintenance", "expected_maintenance_epoch"),),
    ),
    RuntimeShellCommandContribution(
        "core.resume",
        1,
        "shell.core-runtime.execute",
        1,
        _maintenance_schema("core.resume"),
        "runtime.maintenance",
        "BOT",
        "P0",
        ("runtime.maintenance",),
        (("expect-maintenance", "expected_maintenance_epoch"),),
    ),
    RuntimeShellCommandContribution(
        "module.list",
        1,
        "shell.core-runtime.execute",
        1,
        _empty_schema("module.list"),
        "runtime.status",
        "BOT",
        "P1",
        ("runtime.status.read",),
    ),
    RuntimeShellCommandContribution(
        "core.reload",
        1,
        "shell.core-runtime.execute",
        1,
        _reload_schema(),
        "runtime.graph.reload",
        "BOT",
        "P1",
        ("runtime.graph.reload",),
        (("expect-graph", "expected_graph_revision"),),
    ),
)


def runtime_shell_service(context):
    runtime = context.participant("kernel.runtime").service

    async def execute(request: ShellCommandExecutionRequest):
        if type(request) is not ShellCommandExecutionRequest:
            raise ForestError("SHELL_EXECUTION_INPUT_INVALID", "Shell request is invalid", 422)
        if request.command_id == "core.status":
            status = await runtime.status()
            member_failures = [
                item for item in status["members"] if item["status"] != "ACTIVE"
            ]
            text = (
                f"Forest {status['bot_id']}：{status['status']} / {status['graph_state']}；"
                f"Graph r{status['graph_revision']}，{len(status['members'])} 个模块；"
                f"DATA={'open' if status['data_admission'] else 'closed'}，"
                f"CONTROL={'open' if status['control_admission'] else 'closed'}"
            )
            if member_failures:
                text += f"；异常模块 {len(member_failures)} 个"
            replacement = status.get("latest_replacement")
            if replacement is not None:
                text += (
                    f"；最近重载={replacement['state']}"
                    f" operation={replacement['operation_id']}"
                )
                if replacement.get("error_code"):
                    text += f" error={replacement['error_code']}"
            return ShellCommandExecutionResult(text)
        if request.command_id == "module.list":
            status = await runtime.status()
            return ShellCommandExecutionResult(
                "Forest RuntimeGraph 模块：\n"
                + "\n".join(
                    f"{item['component_id']} {item['status']} "
                    f"r{item['config_revision']} {item['generation_id']}"
                    for item in status["members"]
                )
            )
        if request.command_id == "core.reload":
            if set(request.arguments) != {"expected_graph_revision"} or type(
                request.arguments["expected_graph_revision"]
            ) is not int:
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID", "Shell reload arguments are invalid", 422
                )
            saved = await runtime.request_graph_replacement(
                context.host_lease,
                operation_id="shell-reload:" + request.request_id,
                expected_graph_revision=request.arguments["expected_graph_revision"],
                requested_by=request.principal_ref,
            )
            return ShellCommandExecutionResult(
                f"Forest RuntimeGraph reload {saved['state']}: "
                f"operation={saved['operation_id']} activation={saved['activation_id']}"
            )
        if request.command_id in {"core.pause", "core.resume"}:
            if set(request.arguments) != {"expected_maintenance_epoch"} or type(
                request.arguments["expected_maintenance_epoch"]
            ) is not int:
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID", "Shell maintenance arguments are invalid", 422
                )
            data_admission = request.command_id == "core.resume"
            changed = await runtime.set_data_admission(
                context.host_lease,
                expected_maintenance_epoch=request.arguments[
                    "expected_maintenance_epoch"
                ],
                data_admission=data_admission,
            )
            return ShellCommandExecutionResult(
                f"Forest DATA admission 已{'恢复' if data_admission else '暂停'}；"
                f"maintenance_epoch={changed['maintenance_epoch']}"
            )
        raise ForestError("SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409)

    return execute
