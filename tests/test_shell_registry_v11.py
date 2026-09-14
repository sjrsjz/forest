from jsonschema import Draft202012Validator

from forest.shell_registry_v10 import BUILTIN_GATE_V10
from forest.shell_registry_v11 import (
    BUILTIN_GATE_V11,
    CURRENT_GATE_VERSION,
    get_command_spec,
    registry_manifest,
    render_help,
)


def test_v11_adds_only_module_validation_and_state_commands():
    assert CURRENT_GATE_VERSION == "shell-gate-v11"
    assert set(BUILTIN_GATE_V11) - set(BUILTIN_GATE_V10) == {
        "module.validate", "module.enable", "module.disable"
    }
    for name in ("module.validate", "module.enable", "module.disable"):
        spec = get_command_spec(name)
        Draft202012Validator.check_schema(spec.args_schema)
        assert spec.gate_version == CURRENT_GATE_VERSION
    assert get_command_spec("module.disable").priority == 0
    assert get_command_spec("module.enable").priority == 1
    assert "#forest module validate" in render_help()
    assert "#forest module disable" in render_help()
    assert registry_manifest()["priority_admission"]["p0_requires_current_binding_admin"]
