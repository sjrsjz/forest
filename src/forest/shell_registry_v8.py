"""Frozen v8 Shell contract layered over the released v2--v7 gates."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from forest.shell_registry import CommandForm, _canonical, _digest, _id, _revision, _spec
from forest.shell_registry_v7 import BUILTIN_GATE_V7, DOMAIN_ALIASES_V7
from forest.shell_registry_v7 import BUILTIN_GATES as PRIOR_GATES
from forest.shell_registry_v7 import DOMAIN_ALIASES as PRIOR_ALIASES

MODULE_GATE_VERSION = "shell-gate-v8"
CURRENT_GATE_VERSION = MODULE_GATE_VERSION


def _build_v8():
    table = {
        key: replace(spec, gate_version=MODULE_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V7.items()
    }
    additions = (
        replace(
            _spec(
                "module.read",
                forms=(
                    CommandForm(("module", "list")),
                    CommandForm(("module", "status"), ("component_id",)),
                ),
                properties={"component_id": _id("component_id")},
                action="module.read",
                scope="bot_component_lifecycle_metadata",
                target="module",
                method="read",
                note=(
                    "Module queries expose revision, route, generation and lifecycle metadata only; "
                    "configuration bodies and capability grants stay private."
                ),
            ),
            gate_version=MODULE_GATE_VERSION,
        ),
        replace(
            _spec(
                "module.reload",
                forms=(CommandForm(
                    ("module", "reload"),
                    ("component_id",),
                    (("--expect-revision", "expected_revision"),),
                ),),
                properties={
                    "component_id": _id("component_id"),
                    "expected_revision": _revision("desired revision"),
                },
                required=("component_id", "expected_revision"),
                action="module.reload",
                scope="bot_component_configuration",
                kind="submit_reload",
                target="module",
                method="reload",
                note=(
                    "Reload copies the current desired configuration into a new revision and lets "
                    "the normal lifecycle reconciler publish it; acceptance is not publication."
                ),
            ),
            gate_version=MODULE_GATE_VERSION,
        ),
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    for spec in additions:
        if spec.command_key in table or any(form.words in forms for form in spec.forms):
            raise ValueError("Duplicate builtin v8 command")
        table[spec.command_key] = spec
        forms.update(form.words for form in spec.forms)
    return MappingProxyType(table)


BUILTIN_GATE_V8 = _build_v8()
DOMAIN_ALIASES_V8 = MappingProxyType({**DOMAIN_ALIASES_V7, "modules": "module"})
SIMPLE_COMMANDS_V8 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V8.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({**PRIOR_GATES, MODULE_GATE_VERSION: BUILTIN_GATE_V8})
DOMAIN_ALIASES = MappingProxyType({**PRIOR_ALIASES, MODULE_GATE_VERSION: DOMAIN_ALIASES_V8})


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
