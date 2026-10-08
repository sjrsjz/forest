import json
import re
import shutil
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.packages import PackageCatalog
from forest.runtime_modules import RuntimeShellCommandContribution


def test_package_rejects_long_p0_command_before_publication(tmp_path):
    official = Path(__file__).resolve().parents[1] / (
        "src/forest/builtin/component_worker_host"
    )
    source = tmp_path / "component_worker_host"
    shutil.copytree(official, source)
    manifest = source / "module.toml"
    original = manifest.read_text(encoding="utf-8")
    altered, count = re.subn(
        r'(command_id = "core.status"[\s\S]*?timeout_s = )10\b',
        r"\g<1>21",
        original,
        count=1,
    )
    assert count == 1
    manifest.write_text(altered, encoding="utf-8")

    with pytest.raises(ForestError) as rejected:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert rejected.value.code == "INVALID_SHELL_COMMAND"


def test_runtime_shell_contribution_keeps_p0_short_and_p1_long():
    fields = {
        "command_id": "core.status",
        "command_version": 1,
        "service_id": "shell.component-host.core-status",
        "service_version": 1,
        "argument_schema": {
            "properties": {
                "protocol": {"const": "forest.shell.arguments/1"},
                "command_id": {"const": "core.status"},
                "arguments": {"type": "object", "properties": {}},
            },
        },
        "management_operation_id": "runtime.status",
        "target_scope": "BOT",
        "required_capabilities": ("runtime.status.read",),
    }
    RuntimeShellCommandContribution(**fields, service_class="P0", timeout_seconds=20)
    with pytest.raises(ValueError):
        RuntimeShellCommandContribution(
            **fields, service_class="P0", timeout_seconds=21,
        )
    p1 = RuntimeShellCommandContribution(
        **fields, service_class="P1", timeout_seconds=900,
    )
    schema_path = Path(__file__).resolve().parents[1] / (
        "src/forest/builtin/shell_command/schemas/directory-input.json"
    )
    item_schema = json.loads(schema_path.read_text(encoding="utf-8"))[
        "properties"
    ]["items"]["items"]
    contribution = p1.as_fact() | {
        "owner_component_id": "component.worker-host",
        "target_generation_id": "generation/target",
    }
    validator = Draft202012Validator(item_schema)
    assert list(validator.iter_errors(contribution)) == []
    contribution["service_class"] = "P0"
    assert list(validator.iter_errors(contribution))
