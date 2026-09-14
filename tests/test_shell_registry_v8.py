"""The v8 module-control language extends, rather than reinterprets, v7."""

from jsonschema import Draft202012Validator

from forest.shell_registry_v7 import BUILTIN_GATE_V7
from forest.shell_registry_v8 import (
    BUILTIN_GATE_V8,
    MODULE_GATE_VERSION,
    command_action,
    get_command_spec,
    registry_manifest,
    render_help,
)


def test_gate_v8_adds_only_module_metadata_and_reload_commands():
    assert MODULE_GATE_VERSION == "shell-gate-v8"
    assert set(BUILTIN_GATE_V8) - set(BUILTIN_GATE_V7) == {
        "module.read", "module.reload"
    }
    assert all(spec.gate_version == MODULE_GATE_VERSION for spec in BUILTIN_GATE_V8.values())
    assert get_command_spec("module.read", "shell-gate-v7") is None
    read = get_command_spec("module.read")
    reload = get_command_spec("module.reload")
    assert (read.action, read.execution_target, read.resource_scope) == (
        "module.read", "module", "bot_component_lifecycle_metadata"
    )
    assert (reload.action, reload.execution_target, reload.execution_method) == (
        "module.reload", "module", "reload"
    )
    assert command_action("module.reload") == "module.reload"
    for spec in (read, reload):
        Draft202012Validator.check_schema(spec.args_schema)
    assert Draft202012Validator(reload.args_schema).is_valid({
        "component_id": "forest.weather", "expected_revision": 1
    })
    assert not Draft202012Validator(reload.args_schema).is_valid({
        "component_id": "forest.weather", "expected_revision": -1
    })
    assert "#forest module list" in render_help()
    assert "#forest module reload" in render_help()
    assert registry_manifest()["gate_version"] == MODULE_GATE_VERSION
