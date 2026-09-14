"""Frozen v10 Shell contract with admission-time P0 qualification."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from forest.shell_authority import P0_PRIORITY_POLICY
from forest.shell_registry import _canonical, _digest
from forest.shell_registry_v9 import BUILTIN_GATE_V9, DOMAIN_ALIASES_V9
from forest.shell_registry_v9 import BUILTIN_GATES as PRIOR_GATES
from forest.shell_registry_v9 import DOMAIN_ALIASES as PRIOR_ALIASES

PRIORITY_GATE_VERSION = "shell-gate-v10"
CURRENT_GATE_VERSION = PRIORITY_GATE_VERSION

BUILTIN_GATE_V10 = MappingProxyType({
    key: replace(spec, gate_version=PRIORITY_GATE_VERSION)
    for key, spec in BUILTIN_GATE_V9.items()
})
DOMAIN_ALIASES_V10 = DOMAIN_ALIASES_V9
SIMPLE_COMMANDS_V10 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V10.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({**PRIOR_GATES, PRIORITY_GATE_VERSION: BUILTIN_GATE_V10})
DOMAIN_ALIASES = MappingProxyType({**PRIOR_ALIASES, PRIORITY_GATE_VERSION: DOMAIN_ALIASES_V10})


def get_command_spec(command_key: str, gate_version: str = CURRENT_GATE_VERSION):
    gate = BUILTIN_GATES.get(gate_version)
    return gate.get(command_key) if gate is not None else None


def command_action(command_key: str, gate_version: str = CURRENT_GATE_VERSION):
    spec = get_command_spec(command_key, gate_version)
    return spec.action if spec is not None else None


def registry_manifest(gate_version: str = CURRENT_GATE_VERSION) -> dict:
    gate = BUILTIN_GATES.get(gate_version)
    if gate is None:
        raise ValueError("Unknown shell gate")
    value = {
        "gate_version": gate_version,
        "commands": [spec.manifest() for spec in gate.values()],
        "domain_aliases": dict(DOMAIN_ALIASES[gate_version]),
    }
    if gate_version == PRIORITY_GATE_VERSION:
        value["priority_admission"] = {
            "policy": P0_PRIORITY_POLICY,
            "p0_requires_current_binding_admin": True,
            "unqualified_p0_priority": 1,
            "final_authorization_required": True,
        }
    return {**value, "registry_digest": _digest(_canonical(value))}


def render_help(gate_version: str = CURRENT_GATE_VERSION) -> str:
    gate = BUILTIN_GATES.get(gate_version)
    if gate is None:
        raise ValueError("Unknown shell gate")
    commands, notes = [], []
    for spec in gate.values():
        schema = spec.args_schema
        commands.extend(form.usage(schema) for form in spec.forms if form.show_in_help)
        if spec.note:
            notes.append(spec.note)
    aliases = "；".join(
        f"{alias} 等同 {name}" for alias, name in DOMAIN_ALIASES[gate_version].items()
    )
    return (
        "Forest 本群管理：" + "；".join(commands) + "。" + aliases + "。"
        + "".join(notes) + "计划配置在 Web 后台编辑。"
        "命令参数不会进入 Agent 上下文；请勿在群中发送密钥。"
    )
