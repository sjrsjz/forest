"""Frozen v12 Shell contract for explicit conversation Context reset."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from forest.shell_registry import CommandForm, _canonical, _digest, _revision, _spec
from forest.shell_registry_v11 import BUILTIN_GATE_V11, DOMAIN_ALIASES_V11
from forest.shell_registry_v11 import BUILTIN_GATES as PRIOR_GATES
from forest.shell_registry_v11 import DOMAIN_ALIASES as PRIOR_ALIASES

CONTEXT_RESET_GATE_VERSION = "shell-gate-v12"
CURRENT_GATE_VERSION = CONTEXT_RESET_GATE_VERSION


def _build_v12():
    table = {
        key: replace(spec, gate_version=CONTEXT_RESET_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V11.items()
    }
    reset = replace(
        _spec(
            "context.reset",
            forms=(CommandForm(
                ("context", "reset"),
                options=(("--expect-view", "expected_view_revision"),),
                fixed_arguments=(("mode", "safe-point"),),
            ),),
            properties={
                "expected_view_revision": _revision("上下文视图版本"),
                "mode": {"type": "string", "enum": ["safe-point"], "title": "模式"},
            },
            required=("expected_view_revision", "mode"),
            action="context.reset",
            scope="origin_conversation_context_view",
            kind="context_view_request",
            target="context",
            method="reset",
            note=(
                "重置请求由会话 Actor 在模型调用安全点发布新视图；保留当前未闭合因果组，"
                "不删除消息，也不调用摘要模型。"
            ),
        ),
        gate_version=CONTEXT_RESET_GATE_VERSION,
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    if reset.command_key in table or any(form.words in forms for form in reset.forms):
        raise ValueError("Duplicate builtin v12 command")
    table[reset.command_key] = reset
    return MappingProxyType(table)


BUILTIN_GATE_V12 = _build_v12()
DOMAIN_ALIASES_V12 = DOMAIN_ALIASES_V11
SIMPLE_COMMANDS_V12 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V12.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({**PRIOR_GATES, CONTEXT_RESET_GATE_VERSION: BUILTIN_GATE_V12})
DOMAIN_ALIASES = MappingProxyType({**PRIOR_ALIASES, CONTEXT_RESET_GATE_VERSION: DOMAIN_ALIASES_V12})


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
    if gate_version in {"shell-gate-v10", "shell-gate-v11", CONTEXT_RESET_GATE_VERSION}:
        value["priority_admission"] = {
            "policy": "current-binding-admin-v1",
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
        commands.extend(form.usage(spec.args_schema) for form in spec.forms if form.show_in_help)
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
