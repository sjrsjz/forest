"""Bounded group-Shell inspection commands owned by ``memory.graph``."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_modules import RuntimeShellCommandContribution
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)


def _arguments_schema(command_id: str, properties: dict, required=()) -> dict:
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


_FILTER_STRING = {"type": "string", "minLength": 1, "maxLength": 256}
_LIMIT = {"type": "integer", "minimum": 1, "maximum": 10}

MEMORY_SHELL_COMMANDS = (
    RuntimeShellCommandContribution(
        command_id="memory.trunks",
        command_version=1,
        service_id="shell.memory.inspect",
        service_version=1,
        argument_schema=_arguments_schema("memory.trunks", {}),
        management_operation_id="memory.trunks.list",
        target_scope="BOT",
        service_class="P1",
        required_capabilities=("memory.read",),
    ),
    RuntimeShellCommandContribution(
        command_id="memory.sources",
        command_version=1,
        service_id="shell.memory.inspect",
        service_version=1,
        argument_schema=_arguments_schema(
            "memory.sources",
            {
                "trunk_id": dict(_FILTER_STRING),
                "author_subject_ref": dict(_FILTER_STRING),
                "modality": {"enum": ["TEXT", "IMAGE", "MIXED", "CONTROL"]},
                "unit_kind": {"type": "string", "minLength": 1, "maxLength": 128},
                "limit": _LIMIT,
            },
        ),
        management_operation_id="memory.sources.query",
        target_scope="BOT",
        service_class="P1",
        required_capabilities=("memory.read",),
    ),
    RuntimeShellCommandContribution(
        command_id="memory.relations",
        command_version=1,
        service_id="shell.memory.inspect",
        service_version=1,
        argument_schema=_arguments_schema(
            "memory.relations",
            {
                "endpoint_kind": {
                    "enum": ["SOURCE_UNIT", "ASSERTION", "ENTITY", "ARTIFACT"]
                },
                "endpoint_id": dict(_FILTER_STRING),
                "limit": _LIMIT,
            },
            ("endpoint_kind", "endpoint_id"),
        ),
        management_operation_id="memory.relations.query",
        target_scope="BOT",
        service_class="P1",
        required_capabilities=("memory.read",),
    ),
)


def _short(text: str, limit: int = 3500) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[: limit - 3].decode("utf-8", errors="ignore") + "..."


async def execute_memory_shell(memory, request: ShellCommandExecutionRequest):
    if type(request) is not ShellCommandExecutionRequest:
        raise ForestError("SHELL_EXECUTION_INPUT_INVALID", "Shell request is invalid", 422)
    arguments = request.arguments
    if request.command_id == "memory.trunks":
        rows = await memory.repository.list_trunks()
        lines = [f"Memory 主干（{len(rows)}）"]
        lines.extend(
            f"{row['trunk_id']} | 群 {row['external_group_id']} | "
            f"来源 {row['registered_sources']} / 可召回 {row['eligible_sources']}"
            for row in rows[:10]
        )
        if len(rows) > 10:
            lines.append(f"另有 {len(rows) - 10} 条，请在 WebUI 筛选。")
        return ShellCommandExecutionResult(_short("\n".join(lines)))
    if request.command_id == "memory.sources":
        page = await memory.repository.query_sources(**arguments)
        lines = [f"Memory 来源（本页 {len(page['items'])}）"]
        lines.extend(
            f"{row['source_unit_id']} | 群 {row['external_group_id']} | "
            f"{row['author_subject_ref']} | {row['modality']} | {row['unit_kind']}"
            for row in page["items"]
        )
        if page["has_more"]:
            lines.append("仍有后续结果，请在 WebUI 使用稳定游标继续查询。")
        return ShellCommandExecutionResult(_short("\n".join(lines)))
    if request.command_id == "memory.relations":
        page = await memory.repository.query_relations(**arguments)
        lines = [f"Memory 关系（本页 {len(page['items'])}）"]
        for row in page["items"]:
            groups = sorted(
                {
                    evidence["external_group_id"]
                    for evidence in row["evidence"]
                    if evidence.get("external_group_id")
                }
            )
            source = ",".join(groups) if groups else "无可见群来源"
            lines.append(
                f"{row['relation_id']} {row['relation_kind']} | "
                f"{row['left_kind']}:{row['left_id']} -> "
                f"{row['right_kind']}:{row['right_id']} | 来源群 {source}"
            )
        if page["has_more"]:
            lines.append("仍有后续结果，请在 WebUI 使用稳定游标继续查询。")
        return ShellCommandExecutionResult(_short("\n".join(lines)))
    raise ForestError("SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409)
