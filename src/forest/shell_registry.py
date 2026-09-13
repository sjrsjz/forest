"""Frozen contracts for the builtin v2, v3, and v4 Shell command languages.

This is a host-owned table, not a plugin registration or authorization API.
Queued requests already contain typed arguments: executors select this fixed
contract by persisted gate and command key and never parse or validate their
original text again. Per-command digests are exported metadata, not a claim
that the database stores a dynamic registry snapshot.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from types import MappingProxyType

GATE_VERSION = "shell-gate-v2"
MEMORY_GATE_VERSION = "shell-gate-v3"
MAINTENANCE_GATE_VERSION = "shell-gate-v4"
CURRENT_GATE_VERSION = "shell-gate-v5"


def _canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DeadlinePolicy:
    """The existing configurable Host timeout, not a per-request deadline."""

    policy_ref: str = "host.shell.command_timeout"
    default_seconds: float = 5
    minimum_seconds: float = 0.1
    maximum_seconds: float = 30

    def manifest(self) -> dict:
        return dict(vars(self))


CONTROL_DEADLINE = DeadlinePolicy()


@dataclass(frozen=True)
class CommandForm:
    words: tuple[str, ...]
    positional: tuple[str, ...] = ()
    options: tuple[tuple[str, str], ...] = ()
    fixed_arguments: tuple[tuple[str, str], ...] = ()
    show_in_help: bool = True

    def usage(self, schema: dict) -> str:
        def argument(name):
            prop = schema["properties"][name]
            if prop.get("type") == "array":
                prop = prop["items"]
            if "enum" in prop:
                return "|".join(prop["enum"])
            title = prop["title"]
            return f'"{title}"' if prop.get("x-shell-quoted") else f"<{title}>"

        words = ["#forest", *self.words, *(argument(name) for name in self.positional)]
        for option, name in self.options:
            words.extend((option, argument(name)))
        return " ".join(words)


@dataclass(frozen=True)
class CommandSpec:
    command_key: str
    action: str | None
    resource_scope: str
    priority: int
    execution_kind: str
    execution_target: str
    execution_method: str
    _args_json: str
    forms: tuple[CommandForm, ...]
    note: str = ""
    command_version: int = 1
    gate_version: str = GATE_VERSION
    required_authority: str = "current_binding_admin"
    service_class: str = "CONTROL"
    output_visibility: str = "origin_conversation_metadata"
    confirmation_policy: str = "none"
    timeout: DeadlinePolicy = CONTROL_DEADLINE

    @property
    def args_schema(self) -> dict:
        # A frozen dataclass containing mutable JSON would not freeze the contract.
        return json.loads(self._args_json)

    @property
    def schema_digest(self) -> str:
        return _digest(self._args_json)

    def manifest(self) -> dict:
        return {
            "command_key": self.command_key, "command_version": self.command_version,
            "gate_version": self.gate_version, "schema_digest": self.schema_digest,
            "args_schema": self.args_schema, "action": self.action,
            "required_authority": self.required_authority, "resource_scope": self.resource_scope,
            "service_class": self.service_class, "priority": self.priority,
            "execution_kind": self.execution_kind, "execution_target": self.execution_target,
            "execution_method": self.execution_method, "output_visibility": self.output_visibility,
            "confirmation_policy": self.confirmation_policy, "timeout": self.timeout.manifest(),
            "forms": [{"words": list(form.words), "positional": list(form.positional),
                       "options": [list(option) for option in form.options],
                       "fixed_arguments": dict(form.fixed_arguments),
                       "show_in_help": form.show_in_help} for form in self.forms],
            "note": self.note,
        }


def _id(title: str) -> dict:
    return {"type": "string", "title": title, "minLength": 1, "maxLength": 128,
            "pattern": r"^[A-Za-z0-9_.:-]{1,128}(?![\s\S])"}


def _revision(title: str) -> dict:
    return {"type": "integer", "title": title, "minimum": 0, "maximum": 999999999999999999}


def _ids(title: str) -> dict:
    return {"type": "array", "minItems": 1, "maxItems": 32, "uniqueItems": True,
            "items": _id(title)}


def _reason() -> dict:
    return {"type": "string", "title": "原因", "minLength": 1, "maxLength": 256,
            "pattern": r"^(?=[\s\S]*\S)[^\x00-\x1f]*(?![\s\S])", "x-shell-quoted": True}


def _spec(key, *, forms, properties=None, required=(), action=None, scope="conversation",
          priority=1, kind="query", target="builtin", method="status", note=""):
    schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
              "properties": properties or {}, "additionalProperties": False,
              "required": list(required)}
    return CommandSpec(key, action, scope, priority, kind, target, method, _canonical(schema),
                       tuple(forms), note)


def _build_v2():
    specs = (
        _spec("help", forms=(CommandForm((), show_in_help=False), CommandForm(("help",))),
              scope="builtin_registry", method="help"),
        _spec("status", forms=(CommandForm(("status",)),), scope="bot_status_metadata"),
        _spec("bot.status", forms=(CommandForm(("bot", "status")),), scope="bot_status_metadata"),
        _spec("core.status", forms=(CommandForm(("core", "status")),), scope="bot_status_metadata"),
        _spec("run.read", forms=(CommandForm(("run", "list")),), action="agent.read",
              target="agent", method="inspect"),
        _spec("job.read", forms=(CommandForm(("job", "list")),
                                 CommandForm(("job", "status"), ("job_id",))),
              properties={"job_id": _id("job_id")}, action="job.read", target="job", method="read"),
        _spec("job.run", forms=(CommandForm(("job", "run"), ("job_id",),
                                           (("--expect-revision", "expected_revision"),)),),
              properties={"job_id": _id("job_id"), "expected_revision": _revision("修订")},
              required=("job_id", "expected_revision"), action="job.run", priority=2,
              kind="submit_run", target="job", method="run_now"),
        _spec("job.cancel", forms=(CommandForm(("job", "cancel"), ("job_id",), (
            ("--expect-revision", "expected_revision"), ("--scope", "scope"), ("--reason", "reason"))),),
              properties={"job_id": _id("job_id"), "expected_revision": _revision("修订"),
                          "scope": {"type": "string", "enum": ["future", "pending", "all"]},
                          "reason": _reason()},
              required=("job_id", "expected_revision", "scope", "reason"), action="job.cancel",
              priority=0, kind="submit_cancel", target="job", method="cancel",
              note="future 停止未来触发，pending 取消未领取任务，all 合并两者；已开始的回答需通过 run cancel 取消。"),
        _spec("job.replay", forms=(CommandForm(("job", "replay"), ("occurrence_id",),
                                              (("--expect-revision", "expected_revision"),)),),
              properties={"occurrence_id": _id("occurrence_id"),
                          "expected_revision": _revision("执行修订")},
              required=("occurrence_id", "expected_revision"), action="job.replay", priority=2,
              kind="submit_run", target="job", method="replay"),
        _spec("run.cancel", forms=(CommandForm(("run", "cancel"), ("run_id",), (
            ("--expect-checkpoint", "expected_checkpoint_seq"), ("--reason", "reason"))),),
              properties={"run_id": _id("run_id"),
                          "expected_checkpoint_seq": _revision("检查点"), "reason": _reason()},
              required=("run_id", "expected_checkpoint_seq", "reason"), action="agent.cancel",
              priority=0, kind="submit_cancel", target="agent", method="cancel"),
        _spec("run.approval", forms=tuple(CommandForm(("run", verb), ("approval_id",),
            (("--digest", "arguments_digest"),), (("decision", decision),))
            for verb, decision in (("approve", "APPROVE"), ("reject", "REJECT"))),
              properties={"approval_id": _id("approval_id"), "arguments_digest": _id("参数摘要"),
                          "decision": {"type": "string", "enum": ["APPROVE", "REJECT"]}},
              required=("approval_id", "arguments_digest", "decision"), action="agent.approval",
              priority=0, kind="approval", target="agent", method="decide_approval"),
    )
    table, forms = {}, set()
    for spec in specs:
        if spec.command_key in table:
            raise ValueError("Duplicate builtin command key")
        table[spec.command_key] = spec
        for form in spec.forms:
            if form.words in forms:
                raise ValueError("Duplicate builtin command form")
            forms.add(form.words)
    return MappingProxyType(table)


BUILTIN_GATE_V2 = _build_v2()
DOMAIN_ALIASES_V2 = MappingProxyType({"jobs": "job", "runs": "run"})
SIMPLE_COMMANDS_V2 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V2.values()
    if spec.execution_target == "builtin" for form in spec.forms
})


def _build_v3():
    table = {
        key: replace(spec, gate_version=MEMORY_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V2.items()
    }
    additions = (
        replace(
            _spec("memory.read", forms=(CommandForm(("memory", "list")),),
                  action="memory.forget.read", scope="group_memory_trunk",
                  target="memory", method="read"),
            gate_version=MEMORY_GATE_VERSION,
        ),
        replace(
            _spec(
                "memory.forget.plan",
                forms=(CommandForm(("memory", "forget-plan"), ("source_unit_ids",),
                                   (("--expect-deletion-epoch", "expected_deletion_epoch"),)),),
                properties={"source_unit_ids": _ids("source_unit_id"),
                            "expected_deletion_epoch": _revision("删除版本")},
                required=("source_unit_ids", "expected_deletion_epoch"),
                action="memory.forget.plan", scope="group_memory_trunk",
                target="memory", method="plan",
            ),
            gate_version=MEMORY_GATE_VERSION,
        ),
        replace(
            _spec(
                "memory.forget.apply",
                forms=(CommandForm(("memory", "forget-apply"), ("plan_id",), (
                    ("--digest", "plan_digest"),
                    ("--expect-deletion-epoch", "expected_deletion_epoch"),
                )),),
                properties={"plan_id": _id("plan_id"), "plan_digest": _id("计划摘要"),
                            "expected_deletion_epoch": _revision("删除版本")},
                required=("plan_id", "plan_digest", "expected_deletion_epoch"),
                action="memory.forget.apply", scope="group_memory_trunk", priority=0,
                kind="forget_apply", target="memory", method="apply",
            ),
            gate_version=MEMORY_GATE_VERSION,
        ),
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    for spec in additions:
        if spec.command_key in table or any(form.words in forms for form in spec.forms):
            raise ValueError("Duplicate builtin v3 command")
        table[spec.command_key] = spec
        forms.update(form.words for form in spec.forms)
    return MappingProxyType(table)


BUILTIN_GATE_V3 = _build_v3()
DOMAIN_ALIASES_V3 = DOMAIN_ALIASES_V2
SIMPLE_COMMANDS_V3 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V3.values()
    if spec.execution_target == "builtin" for form in spec.forms
})


def _build_v4():
    table = {
        key: replace(spec, gate_version=MAINTENANCE_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V3.items()
    }
    additions = (
        replace(
            _spec(
                "bot.pause",
                forms=(CommandForm(
                    ("bot", "pause"), (),
                    (("--expect-maintenance", "expected_maintenance_epoch"),
                     ("--reason", "reason")),
                    (("data_admission", False),),
                ),),
                properties={
                    "expected_maintenance_epoch": _revision("维护版本"),
                    "data_admission": {"type": "boolean", "const": False},
                    "reason": _reason(),
                },
                required=("expected_maintenance_epoch", "data_admission", "reason"),
                action="bot.maintenance", scope="bot_runtime", priority=0,
                kind="maintenance_pause", target="bot", method="set_maintenance",
            ),
            gate_version=MAINTENANCE_GATE_VERSION,
        ),
        replace(
            _spec(
                "bot.resume",
                forms=(CommandForm(
                    ("bot", "resume"), (),
                    (("--expect-maintenance", "expected_maintenance_epoch"),
                     ("--reason", "reason")),
                    (("data_admission", True),),
                ),),
                properties={
                    "expected_maintenance_epoch": _revision("维护版本"),
                    "data_admission": {"type": "boolean", "const": True},
                    "reason": _reason(),
                },
                required=("expected_maintenance_epoch", "data_admission", "reason"),
                action="bot.maintenance", scope="bot_runtime", priority=1,
                kind="maintenance_resume", target="bot", method="set_maintenance",
            ),
            gate_version=MAINTENANCE_GATE_VERSION,
        ),
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    for spec in additions:
        if spec.command_key in table or any(form.words in forms for form in spec.forms):
            raise ValueError("Duplicate builtin v4 command")
        table[spec.command_key] = spec
        forms.update(form.words for form in spec.forms)
    return MappingProxyType(table)


BUILTIN_GATE_V4 = _build_v4()
DOMAIN_ALIASES_V4 = DOMAIN_ALIASES_V3
SIMPLE_COMMANDS_V4 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V4.values()
    if spec.execution_target == "builtin" for form in spec.forms
})


def _build_v5():
    table = {
        key: replace(spec, gate_version=CURRENT_GATE_VERSION)
        for key, spec in BUILTIN_GATE_V4.items()
    }
    additions = (
        _spec(
            "event.read",
            forms=(CommandForm(("event", "list")),),
            action="event.read",
            scope="bot_component_events",
            target="event",
            method="read",
        ),
        _spec(
            "event.replay",
            forms=(CommandForm(
                ("event", "replay"),
                ("delivery_id",),
                (("--expect-revision", "expected_revision"),),
            ),),
            properties={
                "delivery_id": _id("delivery_id"),
                "expected_revision": _revision("管理修订"),
            },
            required=("delivery_id", "expected_revision"),
            action="event.replay",
            scope="bot_component_events",
            priority=2,
            kind="submit_run",
            target="event",
            method="replay",
        ),
        _spec(
            "event.retire.plan",
            forms=(CommandForm(
                ("event", "retire-plan"),
                ("subscriber_id",),
                (
                    ("--contract-major", "contract_major"),
                    ("--expect-revision", "expected_revision"),
                ),
            ),),
            properties={
                "subscriber_id": _id("subscriber_id"),
                "contract_major": {
                    "type": "integer", "title": "契约主版本", "minimum": 1,
                    "maximum": 2147483647,
                },
                "expected_revision": _revision("管理修订"),
            },
            required=("subscriber_id", "contract_major", "expected_revision"),
            action="event.retire.plan",
            scope="bot_component_events",
            target="event",
            method="plan_retirement",
        ),
        _spec(
            "event.retire.apply",
            forms=(CommandForm(
                ("event", "retire-apply"),
                ("plan_id",),
                (
                    ("--digest", "plan_digest"),
                    ("--expect-revision", "expected_revision"),
                ),
            ),),
            properties={
                "plan_id": _id("plan_id"),
                "plan_digest": _id("计划摘要"),
                "expected_revision": _revision("管理修订"),
            },
            required=("plan_id", "plan_digest", "expected_revision"),
            action="event.retire.apply",
            scope="bot_component_events",
            target="event",
            method="apply_retirement",
        ),
        _spec(
            "component_job.read",
            forms=(CommandForm(("component-job", "list")),),
            action="component_job.read",
            scope="bot_component_jobs",
            target="component_job",
            method="read",
        ),
        _spec(
            "component_job.replay",
            forms=(CommandForm(
                ("component-job", "replay"),
                ("job_id",),
                (("--expect-revision", "expected_revision"),),
            ),),
            properties={
                "job_id": _id("component_job_id"),
                "expected_revision": _revision("管理修订"),
            },
            required=("job_id", "expected_revision"),
            action="component_job.replay",
            scope="bot_component_jobs",
            priority=2,
            kind="submit_run",
            target="component_job",
            method="replay",
        ),
        _spec(
            "component_job.retire.plan",
            forms=(CommandForm(
                ("component-job", "retire-plan"),
                ("component_id", "handler_id"),
                (("--expect-revision", "expected_revision"),),
            ),),
            properties={
                "component_id": _id("component_id"),
                "handler_id": _id("handler_id"),
                "expected_revision": _revision("管理修订"),
            },
            required=("component_id", "handler_id", "expected_revision"),
            action="component_job.retire.plan",
            scope="bot_component_jobs",
            target="component_job",
            method="plan_retirement",
        ),
        _spec(
            "component_job.retire.apply",
            forms=(CommandForm(
                ("component-job", "retire-apply"),
                ("plan_id",),
                (
                    ("--digest", "plan_digest"),
                    ("--expect-revision", "expected_revision"),
                ),
            ),),
            properties={
                "plan_id": _id("plan_id"),
                "plan_digest": _id("计划摘要"),
                "expected_revision": _revision("管理修订"),
            },
            required=("plan_id", "plan_digest", "expected_revision"),
            action="component_job.retire.apply",
            scope="bot_component_jobs",
            target="component_job",
            method="apply_retirement",
        ),
    )
    forms = {form.words for spec in table.values() for form in spec.forms}
    for spec in additions:
        spec = replace(spec, gate_version=CURRENT_GATE_VERSION)
        if spec.command_key in table or any(form.words in forms for form in spec.forms):
            raise ValueError("Duplicate builtin v5 command")
        table[spec.command_key] = spec
        forms.update(form.words for form in spec.forms)
    return MappingProxyType(table)


BUILTIN_GATE_V5 = _build_v5()
DOMAIN_ALIASES_V5 = MappingProxyType({
    **DOMAIN_ALIASES_V4,
    "events": "event",
    "component-jobs": "component-job",
})
SIMPLE_COMMANDS_V5 = MappingProxyType({
    form.words: spec.command_key for spec in BUILTIN_GATE_V5.values()
    if spec.execution_target == "builtin" for form in spec.forms
})
BUILTIN_GATES = MappingProxyType({
    GATE_VERSION: BUILTIN_GATE_V2,
    MEMORY_GATE_VERSION: BUILTIN_GATE_V3,
    MAINTENANCE_GATE_VERSION: BUILTIN_GATE_V4,
    CURRENT_GATE_VERSION: BUILTIN_GATE_V5,
})
DOMAIN_ALIASES = MappingProxyType({
    GATE_VERSION: DOMAIN_ALIASES_V2,
    MEMORY_GATE_VERSION: DOMAIN_ALIASES_V3,
    MAINTENANCE_GATE_VERSION: DOMAIN_ALIASES_V4,
    CURRENT_GATE_VERSION: DOMAIN_ALIASES_V5,
})


def get_command_spec(command_key: str, gate_version: str = GATE_VERSION) -> CommandSpec | None:
    """Resolve a durable command only under the gate that admitted it."""
    gate = BUILTIN_GATES.get(gate_version)
    return gate.get(command_key) if gate is not None else None


def command_action(command_key: str, gate_version: str = GATE_VERSION) -> str | None:
    spec = get_command_spec(command_key, gate_version)
    return spec.action if spec is not None else None


def registry_manifest(gate_version: str = GATE_VERSION) -> dict:
    """Detached metadata for inspection; this grants no execution capability."""
    gate = BUILTIN_GATES.get(gate_version)
    if gate is None:
        raise ValueError("Unknown shell gate")
    value = {"gate_version": gate_version, "commands": [spec.manifest() for spec in gate.values()],
             "domain_aliases": dict(DOMAIN_ALIASES[gate_version])}
    return {**value, "registry_digest": _digest(_canonical(value))}


def render_help(gate_version: str = GATE_VERSION) -> str:
    gate = BUILTIN_GATES.get(gate_version)
    if gate is None:
        raise ValueError("Unknown shell gate")
    commands, notes = [], []
    for spec in gate.values():
        schema = spec.args_schema
        commands.extend(form.usage(schema) for form in spec.forms if form.show_in_help)
        if spec.note:
            notes.append(spec.note)
    aliases = "；".join(f"{alias} 等同 {name}" for alias, name in DOMAIN_ALIASES[gate_version].items())
    return ("Forest 本群管理：" + "；".join(commands) + "。" + aliases + "。"
            + "".join(notes) + "计划配置在 Web 后台编辑。"
            "命令参数不会进入 Agent 上下文；请勿在群中发送密钥。")
