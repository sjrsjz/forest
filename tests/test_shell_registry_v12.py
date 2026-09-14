from jsonschema import Draft202012Validator

from forest.shell_registry_v11 import BUILTIN_GATE_V11
from forest.shell_registry_v12 import (
    BUILTIN_GATE_V12,
    CURRENT_GATE_VERSION,
    get_command_spec,
    registry_manifest,
    render_help,
)


def test_v12_adds_only_safe_point_context_reset():
    assert CURRENT_GATE_VERSION == "shell-gate-v12"
    assert set(BUILTIN_GATE_V12) - set(BUILTIN_GATE_V11) == {"context.reset"}
    spec = get_command_spec("context.reset")
    Draft202012Validator.check_schema(spec.args_schema)
    assert spec.gate_version == CURRENT_GATE_VERSION
    assert spec.action == "context.reset"
    assert spec.priority == 1
    assert spec.args_schema["properties"]["mode"]["enum"] == ["safe-point"]
    assert "#forest context reset --expect-view" in render_help()
    assert registry_manifest()["priority_admission"]["p0_requires_current_binding_admin"]
