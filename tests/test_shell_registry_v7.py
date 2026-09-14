"""The v7 context-control language extends, rather than reinterprets, v6."""

from jsonschema import Draft202012Validator

from forest.shell_registry import BUILTIN_GATE_V6
from forest.shell_registry_v7 import (
    BUILTIN_GATE_V7,
    CONTEXT_GATE_VERSION,
    command_action,
    get_command_spec,
    registry_manifest,
    render_help,
)


def test_gate_v7_adds_only_origin_group_context_commands():
    assert CONTEXT_GATE_VERSION == "shell-gate-v7"
    assert set(BUILTIN_GATE_V7) - set(BUILTIN_GATE_V6) == {
        "context.read", "context.compact"
    }
    assert all(spec.gate_version == CONTEXT_GATE_VERSION for spec in BUILTIN_GATE_V7.values())
    assert get_command_spec("context.read", "shell-gate-v6") is None
    inspect = get_command_spec("context.read")
    compact = get_command_spec("context.compact")
    assert (inspect.action, inspect.execution_target, inspect.resource_scope) == (
        "context.read", "context", "origin_conversation_context_metadata"
    )
    assert (compact.action, compact.execution_target, compact.priority) == (
        "context.compact", "context", 1
    )
    assert command_action("context.compact") == "context.compact"
    for spec in (inspect, compact):
        Draft202012Validator.check_schema(spec.args_schema)
    assert Draft202012Validator(compact.args_schema).is_valid({"expected_view_revision": 0})
    assert not Draft202012Validator(compact.args_schema).is_valid(
        {"expected_view_revision": -1}
    )
    assert "#forest context inspect" in render_help()
    assert "#forest context compact --expect-view" in render_help()
    manifest = registry_manifest()
    assert manifest["gate_version"] == CONTEXT_GATE_VERSION
    assert any(row["command_key"] == "context.compact" for row in manifest["commands"])
