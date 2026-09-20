from __future__ import annotations

from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.runtime_modules import (
    RuntimeAgentToolContribution,
    RuntimeArtifactIdentity,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
    RuntimeShellCommandContribution,
)

_ARTIFACT = RuntimeArtifactIdentity(
    "test.tools",
    "1.0.0",
    "sha256:" + "a" * 64,
    "bot",
    "test.in-process/1",
    "installed",
)


def _tool(name: str, service_id: str) -> RuntimeAgentToolContribution:
    return RuntimeAgentToolContribution(
        name,
        service_id,
        1,
        {
            "name": name,
            "description": f"Invoke {name}",
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "strict": True,
        },
    )


def _runtime():
    return SimpleNamespace(
        scope_kind="bot",
        generation_authority=None,
        bot=SimpleNamespace(store=None),
    )


def _command(service_id: str) -> RuntimeShellCommandContribution:
    return RuntimeShellCommandContribution(
        "module.inspect",
        1,
        service_id,
        1,
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "protocol": {"const": "forest.shell.arguments/1"},
                "command_id": {"const": "module.inspect"},
                "arguments": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
            "required": ["protocol", "command_id", "arguments"],
            "additionalProperties": False,
        },
        "module.inspect",
        "BOT",
        "P1",
        ("runtime.status.read",),
    )


def test_agent_tool_contribution_freezes_contract_identity():
    tool = _tool("inspect", "module.inspect")

    assert tool.as_fact() == {
        "protocol": "forest.agent-tool-contribution/1",
        "name": "inspect",
        "service_id": "module.inspect",
        "version": 1,
        "declaration": tool.declaration,
        "contract_digest": tool.contract_digest,
        "approval_required": True,
        "supports_query": False,
    }
    assert tool.contract_digest.startswith("sha256:")


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (_tool("inspect", "module.inspect-a"), _tool("inspect", "module.inspect-b")),
        (_tool("inspect_a", "module.inspect"), _tool("inspect_b", "module.inspect")),
    ],
)
def test_runtime_graph_rejects_ambiguous_agent_tool_directory(left, right):
    descriptors = (
        RuntimeModuleDescriptor(
            "module.a",
            (),
            lambda *_: object(),
            _ARTIFACT,
            agent_tools=(left,),
        ),
        RuntimeModuleDescriptor(
            "module.b",
            (),
            lambda *_: object(),
            _ARTIFACT,
            agent_tools=(right,),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_AGENT_TOOL_CONFLICT"


def test_agent_tool_declaration_must_match_its_directory_name():
    with pytest.raises(ValueError, match="Agent tool contribution"):
        RuntimeAgentToolContribution(
            "inspect",
            "module.inspect",
            1,
            {
                "name": "different",
                "description": "Mismatch",
                "parameters": {"type": "object"},
            },
        )


def test_runtime_graph_rejects_ambiguous_shell_command_directory():
    descriptors = (
        RuntimeModuleDescriptor(
            "module.a",
            (),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.inspect-a"),),
        ),
        RuntimeModuleDescriptor(
            "module.b",
            (),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.inspect-b"),),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_SHELL_COMMAND_CONFLICT"


def test_contribution_consumers_follow_provider_lifecycle_order():
    descriptors = (
        RuntimeModuleDescriptor(
            "agent.runtime",
            (),
            lambda *_: object(),
            _ARTIFACT,
        ),
        RuntimeModuleDescriptor(
            "shell.command",
            (),
            lambda *_: object(),
            _ARTIFACT,
        ),
        RuntimeModuleDescriptor(
            "module.provider",
            (),
            lambda *_: object(),
            _ARTIFACT,
            agent_tools=(_tool("inspect", "module.inspect"),),
            shell_commands=(_command("module.shell-inspect"),),
        ),
    )

    registry = RuntimeModuleRegistry(_runtime(), descriptors)

    assert registry.order.index("module.provider") < registry.order.index("agent.runtime")
    assert registry.order.index("module.provider") < registry.order.index("shell.command")
    assert registry.describe()[0]["requires"] == ["module.provider"]
    assert registry.describe()[1]["requires"] == ["module.provider"]


def test_dynamic_command_dependency_participates_in_cycle_rejection():
    descriptors = (
        RuntimeModuleDescriptor(
            "shell.command",
            (),
            lambda *_: object(),
            _ARTIFACT,
        ),
        RuntimeModuleDescriptor(
            "module.provider",
            ("shell.command",),
            lambda *_: object(),
            _ARTIFACT,
            shell_commands=(_command("module.shell-inspect"),),
        ),
    )

    with pytest.raises(ForestError) as caught:
        RuntimeModuleRegistry(_runtime(), descriptors)
    assert caught.value.code == "RUNTIME_MODULE_DEPENDENCY_CYCLE"
