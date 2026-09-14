"""The v10 registry freezes admission-time P0 qualification."""

from forest.shell_authority import P0_PRIORITY_POLICY
from forest.shell_registry_v9 import BUILTIN_GATE_V9
from forest.shell_registry_v9 import registry_manifest as v9_manifest
from forest.shell_registry_v10 import (
    BUILTIN_GATE_V10,
    PRIORITY_GATE_VERSION,
    registry_manifest,
    render_help,
)


def test_v10_changes_only_gate_and_declares_priority_admission_policy():
    assert PRIORITY_GATE_VERSION == "shell-gate-v10"
    assert set(BUILTIN_GATE_V10) == set(BUILTIN_GATE_V9)
    for key, current in BUILTIN_GATE_V10.items():
        prior = BUILTIN_GATE_V9[key]
        assert current.gate_version == PRIORITY_GATE_VERSION
        assert (
            current.command_key,
            current.action,
            current.priority,
            current.forms,
            current.args_schema,
        ) == (
            prior.command_key,
            prior.action,
            prior.priority,
            prior.forms,
            prior.args_schema,
        )
    policy = registry_manifest()["priority_admission"]
    assert policy == {
        "policy": P0_PRIORITY_POLICY,
        "p0_requires_current_binding_admin": True,
        "unqualified_p0_priority": 1,
        "final_authorization_required": True,
    }
    assert "priority_admission" not in v9_manifest()
    assert registry_manifest("shell-gate-v9") == v9_manifest()
    assert registry_manifest()["registry_digest"].startswith("sha256:")
    assert "#forest core cancel" in render_help()
