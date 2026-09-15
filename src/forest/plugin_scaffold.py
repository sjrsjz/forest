"""Transactional source scaffold for auto-declared native plugins."""

from __future__ import annotations

import keyword
import re
import tempfile
from pathlib import Path

from forest.errors import ForestError
from forest.packages import validate_source_package

IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,121}\Z")
VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?\Z")


def initialize_plugin(target: Path, package_id: str, version: str, module: str | None) -> dict:
    if not isinstance(package_id, str) or not IDENTIFIER.fullmatch(package_id):
        raise ForestError(
            "PLUGIN_ID_INVALID",
            "Plugin ID must be a bounded identifier with room for its example service",
            422,
        )
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise ForestError("PLUGIN_VERSION_INVALID", "Plugin version must be SemVer-like", 422)
    selected_module = module or package_id.rsplit(".", 1)[-1].replace("-", "_").lower()
    if (
        not isinstance(selected_module, str)
        or not selected_module.isidentifier()
        or keyword.iskeyword(selected_module)
        or len(selected_module) > 64
    ):
        raise ForestError(
            "PLUGIN_MODULE_INVALID", "Python module must be a bounded non-keyword identifier", 422
        )
    target = target.absolute()
    if target.exists():
        raise ForestError("PLUGIN_PATH_EXISTS", "Plugin destination already exists", 409)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target.parent, prefix=".forest-init-") as temporary:
            root = Path(temporary)
            source = root / "source"
            package = source / "src" / selected_module
            package.mkdir(parents=True)
            (source / "pyproject.toml").write_text(
                f'''[project]
name = "{package_id}"
version = "{version}"
requires-python = ">=3.11"

[tool.forest]
module-root = "src"
''',
                encoding="utf-8",
            )
            service_name = package_id + ".echo"
            (package / "__init__.py").write_text(
                f'''from forest.sdk import plugin, service


@plugin(
    id="{package_id}",
    version="{version}",
    requests=[],
    config_schema={{"type": "object", "additionalProperties": False}},
)
class Plugin:
    @service(
        "{service_name}",
        contract="1.0",
        input_schema={{
            "type": "object",
            "required": ["text"],
            "additionalProperties": False,
            "properties": {{"text": {{"type": "string", "maxLength": 4096}}}},
        }},
        output_schema={{
            "type": "object",
            "required": ["text"],
            "additionalProperties": False,
            "properties": {{"text": {{"type": "string"}}}},
        }},
    )
    async def echo(self, payload, ctx):
        return {{"text": payload["text"]}}
''',
                encoding="utf-8",
            )
            validation = validate_source_package(source)
            source.rename(target)
    except ForestError:
        raise
    except OSError as exc:
        raise ForestError("PLUGIN_INIT_FAILED", "Unable to create plugin source tree", 422) from exc
    return {
        "status": "initialized",
        "path": str(target),
        "package_id": package_id,
        "version": version,
        "module": selected_module,
        "service": service_name,
        "validated_digest": validation["digest"],
    }
