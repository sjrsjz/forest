"""The v9 Core rescue grammar aliases existing durable control authorities."""

from forest.shell_registry_v8 import BUILTIN_GATE_V8
from forest.shell_registry_v9 import (
    BUILTIN_GATE_V9,
    CORE_GATE_VERSION,
    registry_manifest,
    render_help,
)


def _forms(gate, key):
    return {form.words for form in gate[key].forms}


def test_v9_adds_only_fixed_core_forms_and_p0_status():
    assert CORE_GATE_VERSION == "shell-gate-v9"
    assert set(BUILTIN_GATE_V9) == set(BUILTIN_GATE_V8)
    assert all(spec.gate_version == CORE_GATE_VERSION for spec in BUILTIN_GATE_V9.values())
    assert BUILTIN_GATE_V8["core.status"].priority == 1
    assert BUILTIN_GATE_V9["core.status"].priority == 0
    assert _forms(BUILTIN_GATE_V9, "run.cancel") - _forms(BUILTIN_GATE_V8, "run.cancel") == {
        ("core", "cancel")
    }
    assert _forms(BUILTIN_GATE_V9, "bot.pause") - _forms(BUILTIN_GATE_V8, "bot.pause") == {
        ("core", "disable")
    }
    assert _forms(BUILTIN_GATE_V9, "module.reload") - _forms(
        BUILTIN_GATE_V8, "module.reload"
    ) == {("core", "reload")}
    help_text = render_help()
    for command in ("core status", "core cancel", "core disable", "core reload"):
        assert "#forest " + command in help_text
    manifest = registry_manifest()
    assert manifest["gate_version"] == CORE_GATE_VERSION
    assert manifest["registry_digest"].startswith("sha256:")
