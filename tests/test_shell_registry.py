"""Builtin contracts describe the existing language without extending it."""

import hashlib
import json
import shlex
from dataclasses import FrozenInstanceError

import pytest
from jsonschema import Draft202012Validator
from test_shell_commands import command

from forest.shell_registry import (
    BUILTIN_GATE_V2,
    BUILTIN_GATE_V3,
    BUILTIN_GATE_V4,
    BUILTIN_GATE_V5,
    BUILTIN_GATE_V6,
    COMPONENT_GATE_VERSION,
    CONTROL_DEADLINE,
    CURRENT_GATE_VERSION,
    DOMAIN_ALIASES_V2,
    MAINTENANCE_GATE_VERSION,
    MEMORY_GATE_VERSION,
    command_action,
    get_command_spec,
    registry_manifest,
    render_help,
)
from forest.shell_registry_v10 import get_command_spec as get_current_command_spec


def test_contract_export_is_detached_and_cannot_change_execution_or_help():
    before, help_text = registry_manifest(), render_help()
    exported = registry_manifest()
    exported["commands"][0]["args_schema"]["properties"]["injected"] = {"type": "string"}
    exported["commands"][0]["timeout"]["maximum_seconds"] = 99999
    exported["domain_aliases"]["ops"] = "run"
    spec = get_command_spec("job.cancel")
    schema = spec.args_schema
    schema["properties"]["scope"]["enum"].append("everything")
    with pytest.raises(TypeError):
        BUILTIN_GATE_V2["ops.exec"] = spec
    with pytest.raises(FrozenInstanceError):
        spec.priority = 2
    assert registry_manifest() == before and render_help() == help_text
    assert command('#forest job cancel x --expect-revision 0 --scope everything --reason x').name == "unknown"


def test_only_existing_actions_can_resolve_and_metadata_matches_current_policy():
    assert {key: command_action(key) for key, spec in BUILTIN_GATE_V2.items() if spec.action} == {
        "run.read": "agent.read", "run.cancel": "agent.cancel", "run.approval": "agent.approval",
        "job.read": "job.read", "job.run": "job.run", "job.cancel": "job.cancel", "job.replay": "job.replay",
    }
    assert len(BUILTIN_GATE_V2) == 11
    for spec in BUILTIN_GATE_V2.values():
        assert spec.gate_version == "shell-gate-v2" and spec.command_version == 1
        assert spec.required_authority == "current_binding_admin"
        assert spec.service_class == "CONTROL" and spec.output_visibility == "origin_conversation_metadata"
        assert spec.timeout == CONTROL_DEADLINE
        if spec.action:
            assert spec.resource_scope == "conversation"
        assert spec.priority == (0 if spec.execution_kind in {"approval", "submit_cancel"}
                                 else 2 if spec.execution_kind == "submit_run" else 1)
    assert CONTROL_DEADLINE.manifest() == {
        "policy_ref": "host.shell.command_timeout", "default_seconds": 5,
        "minimum_seconds": 0.1, "maximum_seconds": 30,
    }
    for name in ("unknown", "ops.exec", "bot.pause", "core.reload", "config.apply", "ext.echo"):
        assert get_command_spec(name) is None and command_action(name) is None


def _value(name, schema):
    if "enum" in schema:
        return schema["enum"][0]
    if schema["type"] == "integer":
        return schema.get("maximum", 999999999999999999)
    return "中 文 原因" if name == "reason" else "sha256:abc" if name == "arguments_digest" else "id:1"


@pytest.mark.parametrize("spec", list(BUILTIN_GATE_V2.values()), ids=lambda spec: spec.command_key)
def test_every_advertised_form_and_alias_is_accepted_with_exact_schema(spec):
    schema = spec.args_schema
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    schema_json = json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert spec.schema_digest == "sha256:" + hashlib.sha256(schema_json.encode()).hexdigest()
    for form in spec.forms:
        payload = dict(form.fixed_arguments)
        words = ["#forest", *form.words]
        for name in form.positional:
            payload[name] = _value(name, schema["properties"][name])
            words.append(str(payload[name]))
        for option, name in form.options:
            payload[name] = _value(name, schema["properties"][name])
            words.extend((option, str(payload[name])))
        validator.validate(payload)
        rendered = "#forest " + shlex.join(words[1:])
        parsed = command(rendered)
        assert (parsed.name, parsed.payload) == (spec.command_key, payload)
        current = get_current_command_spec(parsed.name, parsed.gate_version)
        assert current is not None and parsed.priority == current.priority
        for alias, canonical in DOMAIN_ALIASES_V2.items():
            if form.words and form.words[0] == canonical:
                aliased = command("#forest " + shlex.join([alias, *words[2:]]))
                assert aliased == parsed
        if form.show_in_help:
            assert form.usage(schema) in render_help()


@pytest.mark.parametrize(("key", "payload"), [
    ("job.read", {"job_id": "x\n"}),
    ("job.run", {"job_id": "x", "expected_revision": True}),
    ("job.run", {"job_id": "x", "expected_revision": 1000000000000000000}),
    ("job.run", {"job_id": "x", "expected_revision": -1}),
    ("run.read", {"other_conversation": "x"}),
    ("run.cancel", {"run_id": "x", "expected_checkpoint_seq": 1, "reason": "\t"}),
    ("run.cancel", {"run_id": "x", "expected_checkpoint_seq": 1, "reason": "x\ny"}),
    ("run.cancel", {"run_id": "x", "expected_checkpoint_seq": 1, "reason": "\u2003"}),
    ("run.approval", {"approval_id": "x", "arguments_digest": "x", "decision": "LATER"}),
])
def test_schema_does_not_advertise_malformed_or_broader_typed_inputs(key, payload):
    assert not Draft202012Validator(get_command_spec(key).args_schema).is_valid(payload)


def test_gate_v2_keeps_posix_quoting_and_does_not_silently_add_new_syntax():
    base = "#forest run cancel r --expect-checkpoint 0 --reason "
    assert command(base + r"C:\logs").payload["reason"] == "C:logs"
    assert command(base + '"C:\\logs"').payload["reason"] == r"C:\logs"
    assert command(base + "'single quoted'").payload["reason"] == "single quoted"
    assert command(base + '"x; y"').payload["reason"] == "x; y"
    assert command(base + "x --").name == "unknown"
    assert command("#forest job run x --expect-revision=1").name == "unknown"
    assert "#forest ops" not in render_help() and "#forest core reload" not in render_help()


def test_fixed_gate_v2_registry_digest():
    # A contract change needs an explicit compatibility decision, not silent drift
    # of a table that still resolves already-admitted shell-gate-v2 requests.
    assert registry_manifest()["registry_digest"] == "sha256:560d434e08393a4cf0ccf548ca427d469c55bae4fae6ead76052e0334cf027e9"


def test_gate_v3_adds_only_group_memory_administration_and_keeps_v2_resolvable():
    assert MEMORY_GATE_VERSION == "shell-gate-v3"
    assert set(BUILTIN_GATE_V3) - set(BUILTIN_GATE_V2) == {
        "memory.read", "memory.forget.plan", "memory.forget.apply"
    }
    assert get_command_spec("memory.read") is None
    assert command_action("memory.read") is None
    assert get_command_spec("memory.read", MEMORY_GATE_VERSION).action == "memory.forget.read"
    assert command_action("memory.forget.apply", MEMORY_GATE_VERSION) == "memory.forget.apply"
    assert all(spec.gate_version == MEMORY_GATE_VERSION for spec in BUILTIN_GATE_V3.values())
    assert "#forest memory list" in render_help(MEMORY_GATE_VERSION)
    assert registry_manifest(MEMORY_GATE_VERSION)["gate_version"] == MEMORY_GATE_VERSION


def test_gate_v4_adds_bot_maintenance_without_changing_old_gates():
    assert MAINTENANCE_GATE_VERSION == "shell-gate-v4"
    assert set(BUILTIN_GATE_V4) - set(BUILTIN_GATE_V3) == {"bot.pause", "bot.resume"}
    pause, resume = BUILTIN_GATE_V4["bot.pause"], BUILTIN_GATE_V4["bot.resume"]
    assert pause.action == resume.action == "bot.maintenance"
    assert pause.priority == 0 and resume.priority == 1
    assert pause.forms[0].fixed_arguments == (("data_admission", False),)
    assert resume.forms[0].fixed_arguments == (("data_admission", True),)
    assert all(
        spec.gate_version == MAINTENANCE_GATE_VERSION
        for spec in BUILTIN_GATE_V4.values()
    )
    assert get_command_spec("bot.pause", MEMORY_GATE_VERSION) is None
    assert "#forest bot pause" in render_help(MAINTENANCE_GATE_VERSION)


def test_gate_v5_adds_component_operations_without_changing_old_gates():
    assert COMPONENT_GATE_VERSION == "shell-gate-v5"
    additions = set(BUILTIN_GATE_V5) - set(BUILTIN_GATE_V4)
    assert additions == {
        "event.read",
        "event.replay",
        "event.retire.plan",
        "event.retire.apply",
        "component_job.read",
        "component_job.replay",
        "component_job.retire.plan",
        "component_job.retire.apply",
    }
    assert all(
        spec.gate_version == COMPONENT_GATE_VERSION for spec in BUILTIN_GATE_V5.values()
    )
    assert get_command_spec("event.read", MAINTENANCE_GATE_VERSION) is None
    assert command_action("component_job.replay", COMPONENT_GATE_VERSION) == (
        "component_job.replay"
    )
    assert BUILTIN_GATE_V5["event.replay"].priority == 2
    assert BUILTIN_GATE_V5["component_job.replay"].priority == 2
    assert "#forest event retire-plan" in render_help(COMPONENT_GATE_VERSION)
    assert "#forest component-job retire-plan" in render_help(COMPONENT_GATE_VERSION)
    for key in additions:
        spec = BUILTIN_GATE_V5[key]
        Draft202012Validator.check_schema(spec.args_schema)
        validator = Draft202012Validator(spec.args_schema)
        for form in spec.forms:
            payload = dict(form.fixed_arguments)
            words = ["#forest", *form.words]
            for name in form.positional:
                payload[name] = _value(name, spec.args_schema["properties"][name])
                words.append(str(payload[name]))
            for option, name in form.options:
                payload[name] = _value(name, spec.args_schema["properties"][name])
                words.extend((option, str(payload[name])))
            validator.validate(payload)
            parsed = command("#forest " + shlex.join(words[1:]))
            assert (parsed.name, parsed.payload) == (key, payload)


def test_gate_v6_adds_only_redacted_bot_audit_query_and_keeps_v5_resolvable():
    assert CURRENT_GATE_VERSION == "shell-gate-v6"
    assert set(BUILTIN_GATE_V6) - set(BUILTIN_GATE_V5) == {"audit.read"}
    assert all(spec.gate_version == CURRENT_GATE_VERSION for spec in BUILTIN_GATE_V6.values())
    assert get_command_spec("audit.read", COMPONENT_GATE_VERSION) is None
    spec = get_command_spec("audit.read", CURRENT_GATE_VERSION)
    assert (spec.action, spec.resource_scope, spec.priority) == (
        "audit.read", "bot_audit_metadata", 1,
    )
    assert spec.output_visibility == "origin_conversation_metadata"
    Draft202012Validator.check_schema(spec.args_schema)
    assert Draft202012Validator(spec.args_schema).is_valid(
        {"action": "agent.cancel", "before": "audit:1", "limit": 20}
    )
    assert not Draft202012Validator(spec.args_schema).is_valid({"limit": 21})
    assert "#forest audit list" in render_help(CURRENT_GATE_VERSION)
