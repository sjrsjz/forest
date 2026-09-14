"""Frozen v7 Shell contract layered over the already released v2--v6 gates."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from forest.shell_registry import (
    BUILTIN_GATE_V6,
    DOMAIN_ALIASES_V6,
    CommandForm,
    _canonical,
    _digest,
    _revision,
    _spec,
)
from forest.shell_registry import BUILTIN_GATES as PRIOR_GATES
from forest.shell_registry import DOMAIN_ALIASES as PRIOR_ALIASES

CONTEXT_GATE_VERSION = "shell-gate-v7"
CURRENT_GATE_VERSION = CONTEXT_GATE_VERSION


def _build_v7():
    table = {
        key: replace(spec, gate_version=CONTEXT_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V6.items()
    }
    additions = (
        replace(
            _spec(
                "context.read",
                forms=(CommandForm(("context", "inspect")),),
                action="context.read",
                scope="origin_conversation_context_metadata",
                target="context",
                method="read",
                note="上下文查询只返回本群视图和队列元数据，不向群聊输出消息正文。",
            ),
            gate_version=CONTEXT_GATE_VERSION,
        ),
        replace(
            _spec(
                "context.compact",
                forms=(CommandForm(
                    ("context", "compact"),
                    options=(("--expect-view", "expected_view_revision"),),
                ),),
                properties={"expected_view_revision": _revision("上下文视图版本")},
                required=("expected_view_revision",),
                action="context.compact",
                scope="origin_conversation_context_view",
                kind="context_view_request",
                target="context",
                method="compact",
                note="裁剪请求由会话 Actor 在模型调用安全点执行，不调用摘要模型。",
            ),
            gate_version=CONTEXT_GATE_VERSION,
        ),
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    for spec in additions:
        if spec.command_key in table or any(form.words in forms for form in spec.forms):
            raise ValueError("Duplicate builtin v7 command")
        table[spec.command_key] = spec
        forms.update(form.words for form in spec.forms)
    return MappingProxyType(table)


BUILTIN_GATE_V7 = _build_v7()
DOMAIN_ALIASES_V7 = DOMAIN_ALIASES_V6
SIMPLE_COMMANDS_V7 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V7.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({**PRIOR_GATES, CONTEXT_GATE_VERSION: BUILTIN_GATE_V7})
DOMAIN_ALIASES = MappingProxyType({**PRIOR_ALIASES, CONTEXT_GATE_VERSION: DOMAIN_ALIASES_V7})


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
