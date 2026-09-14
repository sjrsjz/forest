"""Frozen v9 Shell contract with a minimal host-owned rescue grammar."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from forest.shell_registry import CommandForm, _canonical, _digest
from forest.shell_registry_v8 import BUILTIN_GATE_V8, DOMAIN_ALIASES_V8
from forest.shell_registry_v8 import BUILTIN_GATES as PRIOR_GATES
from forest.shell_registry_v8 import DOMAIN_ALIASES as PRIOR_ALIASES

CORE_GATE_VERSION = "shell-gate-v9"
CURRENT_GATE_VERSION = CORE_GATE_VERSION


def _build_v9():
    table = {
        key: replace(spec, gate_version=CORE_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V8.items()
    }
    table["core.status"] = replace(table["core.status"], priority=0)
    table["run.cancel"] = replace(
        table["run.cancel"],
        forms=table["run.cancel"].forms + (
            CommandForm(
                ("core", "cancel"),
                ("run_id",),
                (("--expect-checkpoint", "expected_checkpoint_seq"), ("--reason", "reason")),
            ),
        ),
        note=(
            "core cancel is the host-owned P0 spelling of run cancel; both target the same "
            "durable Run checkpoint and cancellation guard."
        ),
    )
    table["bot.pause"] = replace(
        table["bot.pause"],
        forms=table["bot.pause"].forms + (
            CommandForm(
                ("core", "disable"),
                (),
                (("--expect-maintenance", "expected_maintenance_epoch"), ("--reason", "reason")),
                (("data_admission", False),),
            ),
        ),
        note=(
            "core disable is the host-owned P0 spelling of Bot maintenance pause; it closes "
            "business admission without disabling CONTROL."
        ),
    )
    table["module.reload"] = replace(
        table["module.reload"],
        forms=table["module.reload"].forms + (
            CommandForm(
                ("core", "reload"),
                ("component_id",),
                (("--expect-revision", "expected_revision"),),
            ),
        ),
        note=(
            "core reload is the fixed P1 spelling of module reload and uses the same config "
            "head CAS and lifecycle reconciler."
        ),
    )
    forms = [form.words for spec in table.values() for form in spec.forms]
    if len(forms) != len(set(forms)):
        raise ValueError("Duplicate builtin v9 command form")
    return MappingProxyType(table)


BUILTIN_GATE_V9 = _build_v9()
DOMAIN_ALIASES_V9 = DOMAIN_ALIASES_V8
SIMPLE_COMMANDS_V9 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V9.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({**PRIOR_GATES, CORE_GATE_VERSION: BUILTIN_GATE_V9})
DOMAIN_ALIASES = MappingProxyType({**PRIOR_ALIASES, CORE_GATE_VERSION: DOMAIN_ALIASES_V9})


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
