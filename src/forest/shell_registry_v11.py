"""Frozen v11 Shell contract for durable module validation and state changes."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from forest.shell_registry import CommandForm, _canonical, _digest, _id, _reason, _revision, _spec
from forest.shell_registry_v10 import BUILTIN_GATE_V10, DOMAIN_ALIASES_V10
from forest.shell_registry_v10 import BUILTIN_GATES as PRIOR_GATES
from forest.shell_registry_v10 import DOMAIN_ALIASES as PRIOR_ALIASES

MODULE_STATE_GATE_VERSION = "shell-gate-v11"
CURRENT_GATE_VERSION = MODULE_STATE_GATE_VERSION


def _build_v11():
    table = {
        key: replace(spec, gate_version=MODULE_STATE_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V10.items()
    }
    additions = (
        _spec(
            "module.validate",
            forms=(CommandForm(
                ("module", "validate"),
                ("component_id",),
                (
                    ("--expect-config-revision", "expected_config_revision"),
                    ("--expect-state-revision", "expected_state_revision"),
                ),
            ),),
            properties={
                "component_id": _id("component_id"),
                "expected_config_revision": _revision("desired config revision"),
                "expected_state_revision": _revision("module state revision"),
            },
            required=("component_id", "expected_config_revision", "expected_state_revision"),
            action="module.validate",
            scope="bot_component_desired_snapshot",
            target="module",
            method="validate",
            note=(
                "Module validation checks the Host-pinned desired package and dependency graph; "
                "configuration bodies and grants stay private."
            ),
        ),
        _spec(
            "module.enable",
            forms=(CommandForm(
                ("module", "enable"),
                ("component_id",),
                (("--expect-state-revision", "expected_state_revision"), ("--reason", "reason")),
                (("enabled", True),),
            ),),
            properties={
                "component_id": _id("component_id"),
                "expected_state_revision": _revision("module state revision"),
                "enabled": {"type": "boolean", "const": True},
                "reason": _reason(),
            },
            required=("component_id", "expected_state_revision", "enabled", "reason"),
            action="module.enable",
            scope="bot_component_lifecycle",
            kind="module_state_change",
            target="module",
            method="set_state",
        ),
        _spec(
            "module.disable",
            forms=(CommandForm(
                ("module", "disable"),
                ("component_id",),
                (("--expect-state-revision", "expected_state_revision"), ("--reason", "reason")),
                (("enabled", False),),
            ),),
            properties={
                "component_id": _id("component_id"),
                "expected_state_revision": _revision("module state revision"),
                "enabled": {"type": "boolean", "const": False},
                "reason": _reason(),
            },
            required=("component_id", "expected_state_revision", "enabled", "reason"),
            action="module.disable",
            scope="bot_component_lifecycle",
            priority=0,
            kind="module_state_change",
            target="module",
            method="set_state",
            note=(
                "Module disable is admitted as P0 only for a current Binding administrator; "
                "the lifecycle coordinator drains calls before publication."
            ),
        ),
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    for spec in additions:
        spec = replace(spec, gate_version=MODULE_STATE_GATE_VERSION)
        if spec.command_key in table or any(form.words in forms for form in spec.forms):
            raise ValueError("Duplicate builtin v11 command")
        table[spec.command_key] = spec
        forms.update(form.words for form in spec.forms)
    return MappingProxyType(table)


BUILTIN_GATE_V11 = _build_v11()
DOMAIN_ALIASES_V11 = DOMAIN_ALIASES_V10
SIMPLE_COMMANDS_V11 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V11.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({**PRIOR_GATES, MODULE_STATE_GATE_VERSION: BUILTIN_GATE_V11})
DOMAIN_ALIASES = MappingProxyType({**PRIOR_ALIASES, MODULE_STATE_GATE_VERSION: DOMAIN_ALIASES_V11})


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
    if gate_version in {"shell-gate-v10", MODULE_STATE_GATE_VERSION}:
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
