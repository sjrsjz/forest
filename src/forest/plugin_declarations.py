"""Static SDK declaration synthesis for source packages without ``module.toml``.

The builder only evaluates Python literals from the AST.  Plugin modules are
never imported in the host process, and the generated files become ordinary
members of the immutable package snapshot.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import tomllib
from pathlib import PurePosixPath
from typing import Any

from forest.command_contract import (
    command_identifier,
    normalize_command_name,
    validate_argument_mode,
    validate_command_permission,
    validate_command_priority,
)
from forest.errors import ForestError

_DECORATORS = {"plugin", "service", "subscribe", "job", "command", "message_handler"}
_PLUGIN_POSITIONAL = ("id", "version", "scope")
_HANDLER_POSITIONAL = {
    "service": ("name",),
    "subscribe": ("subscriber_id", "topic", "contract"),
    "job": ("handler_id",),
    "command": ("name", "permission"),
    "message_handler": ("handler_id",),
}
_IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")


def _error(message: str, path: str | None = None, node: ast.AST | None = None) -> None:
    details: dict[str, Any] = {"phase": "declaration"}
    if path is not None:
        details.update(
            file=path,
            line=getattr(node, "lineno", None),
            column=getattr(node, "col_offset", None) + 1 if node is not None else None,
        )
    raise ForestError("PLUGIN_DECLARATION_INVALID", message, 422, details)


def _json_value(value: Any, path: str, node: ast.AST) -> Any:
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        return [_json_value(item, path, node) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            _error("Declaration objects require string keys", path, node)
        return {key: _json_value(item, path, node) for key, item in value.items()}
    if value is None or isinstance(value, (str, bool, int, float)):
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError):
            _error("Declaration values must be finite JSON literals", path, node)
        return value
    _error("Declaration values must be JSON literals", path, node)


def _literal(node: ast.AST, path: str) -> Any:
    try:
        return _json_value(ast.literal_eval(node), path, node)
    except ForestError:
        raise
    except (ValueError, TypeError, RecursionError):
        _error("Decorator arguments must be statically evaluable literals", path, node)


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted(node.value)
        return f"{parent}.{node.attr}" if parent else None
    return None


def _sdk_bindings(tree: ast.Module) -> tuple[dict[str, str], set[str]]:
    direct: dict[str, str] = {}
    modules: set[str] = set()
    for statement in tree.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                if alias.name == "forest.sdk":
                    modules.add(alias.asname or "forest.sdk")
        elif isinstance(statement, ast.ImportFrom):
            if statement.module == "forest.sdk":
                for alias in statement.names:
                    if alias.name in _DECORATORS:
                        direct[alias.asname or alias.name] = alias.name
            elif statement.module == "forest":
                for alias in statement.names:
                    if alias.name == "sdk":
                        modules.add(alias.asname or "sdk")
    return direct, modules


def _decorator_name(
    node: ast.AST, direct: dict[str, str], modules: set[str]
) -> str | None:
    target = node.func if isinstance(node, ast.Call) else node
    if isinstance(target, ast.Name):
        return direct.get(target.id)
    if (
        isinstance(target, ast.Attribute)
        and target.attr in _DECORATORS
        and _dotted(target.value) in modules
    ):
        return target.attr
    return None


def _call(decorator: ast.AST, name: str, positional: tuple[str, ...], path: str) -> dict:
    if not isinstance(decorator, ast.Call):
        _error(f"@{name} must be called with literal arguments", path, decorator)
    if len(decorator.args) > len(positional):
        _error(f"@{name} has too many positional arguments", path, decorator)
    values = {
        key: _literal(value, path) for key, value in zip(positional, decorator.args, strict=False)
    }
    for keyword in decorator.keywords:
        if keyword.arg is None:
            _error("Expanded decorator keyword arguments are not static declarations", path, keyword)
        if keyword.arg in values:
            _error(f"@{name} repeats argument {keyword.arg}", path, keyword)
        values[keyword.arg] = _literal(keyword.value, path)
    return values


def _module_root(files: dict[str, bytes]) -> str:
    configured = None
    if "pyproject.toml" in files:
        try:
            project = tomllib.loads(files["pyproject.toml"].decode("utf-8"))
            configured = project.get("tool", {}).get("forest", {}).get("module-root")
        except (ValueError, UnicodeError, AttributeError, RecursionError) as exc:
            raise ForestError(
                "PLUGIN_DECLARATION_INVALID",
                "Auto-declared packages require a valid pyproject.toml",
                422,
                {"file": "pyproject.toml", "line": None, "column": None, "phase": "declaration"},
            ) from exc
    root = (
        configured
        if configured is not None
        else ("src" if any(name.startswith("src/") for name in files) else "python")
    )
    if not isinstance(root, str) or not root or len(root) > 128:
        _error("tool.forest.module-root must be a bounded relative directory")
    pure = PurePosixPath(root)
    if pure.is_absolute() or root != pure.as_posix() or any(part in {"", ".", ".."} for part in pure.parts):
        _error("tool.forest.module-root must be a portable relative directory")
    return root


def _parse(path: str, source: bytes) -> ast.Module:
    try:
        return ast.parse(source, filename=path)
    except (SyntaxError, ValueError) as exc:
        raise ForestError(
            "PACKAGE_SYNTAX_ERROR",
            f"Invalid Python in {path}",
            422,
            {
                "file": path,
                "line": getattr(exc, "lineno", None),
                "column": getattr(exc, "offset", None),
                "phase": "syntax",
            },
        ) from exc


def _schema(
    files: dict[str, bytes], value: Any, generated_name: str, path: str, node: ast.AST
) -> str:
    if value is None:
        value = {}
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        _error("Schema declarations must be a package path or JSON object", path, node)
    reference = f"forest_generated/{generated_name}.json"
    files[reference] = (json.dumps(value, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    return reference


def _string(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _array(values: list[str]) -> str:
    return "[" + ", ".join(_string(value) for value in values) + "]"


def _emit_table(lines: list[str], heading: str, values: dict[str, Any]) -> None:
    if not values:
        return
    lines.extend(("", heading))
    for key in sorted(values):
        value = values[key]
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            rendered = str(value)
        elif isinstance(value, str):
            rendered = _string(value)
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            rendered = _array(value)
        else:
            _error(f"Generated manifest field {key} has an unsupported value")
        lines.append(f"{key} = {rendered}")


def _toml(manifest: dict) -> bytes:
    component = manifest["components"][0]
    lines = [
        "# Generated from literal forest.sdk decorators; do not edit this artifact.",
        "manifest_version = 1",
        f'package_id = {_string(manifest["package_id"])}',
        f'version = {_string(manifest["version"])}',
        f'kernel_api = {_string(manifest["kernel_api"])}',
        'runtime = "python-worker"',
        f'entrypoint = {_string(manifest["entrypoint"])}',
        f'module_root = {_string(manifest["module_root"])}',
    ]
    if manifest.get("dependency_lock"):
        lines.append(f'dependency_lock = {_string(manifest["dependency_lock"])}')
    lines.extend(
        (
            "",
            "[[components]]",
            f'id = {_string(component["id"])}',
            f'scope = {_string(component["scope"])}',
            f'state_schema = {component["state_schema"]}',
            f'reload_mode = {_string(component["reload_mode"])}',
            f'config_schema = {_string(component["config_schema"])}',
        )
    )
    if component.get("message_handler_takeover"):
        lines.append(
            f'message_handler_takeover = {_string(component["message_handler_takeover"])}'
        )
    if not component.get("provides"):
        lines.append("provides = []")
    for key in ("state_value_schema", "state_mode", "checkpoint_schema"):
        if key in component:
            value = component[key]
            lines.append(f"{key} = {_string(value) if isinstance(value, str) else value}")
    _emit_table(lines, "[components.capabilities]", component.get("capabilities", {}))
    _emit_table(lines, "[components.resources]", component.get("resources", {}))
    _emit_table(lines, "[components.lifecycle]", component.get("lifecycle", {}))
    for section, items in (
        ("requires", component.get("requires", [])),
        ("provides", component.get("provides", [])),
        ("publishes", component.get("publishes", [])),
        ("subscribes", component.get("subscribes", [])),
        ("jobs", component.get("jobs", [])),
        ("commands", component.get("commands", [])),
        ("message_handlers", component.get("message_handlers", [])),
        ("state_migrations", component.get("state_migrations", [])),
    ):
        for item in items:
            _emit_table(lines, f"[[components.{section}]]", item)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _items(value: Any, field: str, path: str, node: ast.AST) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        _error(f"@plugin {field} must be a literal list of objects", path, node)
    return value


def synthesize_manifest(source_files: dict[str, bytes]) -> dict[str, bytes]:
    """Return captured files plus generated declaration artifacts when needed."""

    if "module.toml" in source_files:
        return source_files
    if "checksums.json" in source_files or any(
        name.startswith("forest_generated/") for name in source_files
    ):
        _error(
            "Auto-declared source packages reserve checksums.json and forest_generated/"
        )
    files = dict(source_files)
    root = _module_root(files)
    candidates: list[tuple[str, ast.ClassDef, dict]] = []
    prefix = root + "/"
    for path, source in sorted(files.items()):
        if not path.startswith(prefix) or not path.endswith(".py"):
            continue
        tree = _parse(path, source)
        direct, modules = _sdk_bindings(tree)
        for statement in tree.body:
            if not isinstance(statement, ast.ClassDef):
                continue
            declarations = [
                decorator
                for decorator in statement.decorator_list
                if _decorator_name(decorator, direct, modules) == "plugin"
            ]
            if len(declarations) > 1:
                _error("A class may have only one @plugin declaration", path, statement)
            if declarations:
                candidates.append(
                    (path, statement, _call(declarations[0], "plugin", _PLUGIN_POSITIONAL, path))
                )
    if len(candidates) != 1:
        _error("A source package without module.toml requires exactly one literal @plugin class")
    path, class_node, declaration = candidates[0]
    selected_tree = _parse(path, files[path])
    direct, modules = _sdk_bindings(selected_tree)
    allowed = {
        "id", "version", "scope", "requests", "kernel_api", "config_schema",
        "state_schema", "state_value_schema", "state_migrations", "reload_mode",
        "requires", "publishes", "resources", "lifecycle", "dependency_lock",
        "message_handler_takeover",
    }
    unknown = sorted(set(declaration) - allowed)
    if unknown:
        _error(f"Unsupported @plugin arguments: {', '.join(unknown)}", path, class_node)
    for required in ("id", "version"):
        if required not in declaration:
            _error(f"@plugin requires {required}", path, class_node)
    relative = path.removeprefix(prefix).removesuffix(".py")
    module = relative.removesuffix("/__init__").replace("/", ".")
    if not module:
        _error("The @plugin class must be inside an importable module", path, class_node)
    handlers: dict[str, list[tuple[dict, ast.AST, str]]] = {
        "service": [], "subscribe": [], "job": [], "command": [], "message_handler": []
    }
    seen: dict[str, set[str]] = {name: set() for name in handlers}
    for statement in class_node.body:
        if not isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in statement.decorator_list:
            kind = _decorator_name(decorator, direct, modules)
            if kind not in handlers:
                continue
            values = _call(decorator, kind, _HANDLER_POSITIONAL[kind], path)
            if not isinstance(statement, ast.AsyncFunctionDef):
                _error(f"@{kind} requires an async method", path, statement)
            identifier_key = (
                "name" if kind == "service" else
                "subscriber_id" if kind == "subscribe" else
                "handler_id" if kind in {"job", "message_handler"} else "id"
            )
            if kind == "command":
                try:
                    identifier = command_identifier(values.get("name"), values.get("id"))
                except ValueError as exc:
                    _error(str(exc), path, decorator)
            else:
                identifier = values.get(identifier_key)
                if not isinstance(identifier, str) or not _IDENTIFIER.fullmatch(identifier):
                    _error(f"@{kind} requires a bounded {identifier_key}", path, decorator)
            if identifier in seen[kind]:
                _error(f"Duplicate @{kind} declaration {identifier}", path, decorator)
            seen[kind].add(identifier)
            handlers[kind].append((values, decorator, statement.name))
    services = []
    for index, (values, node, _) in enumerate(
        sorted(handlers["service"], key=lambda item: item[0]["name"])
    ):
        unknown = sorted(set(values) - {"name", "contract", "input_schema", "output_schema"})
        if unknown:
            _error(f"Unsupported @service arguments: {', '.join(unknown)}", path, node)
        services.append({
            "name": values["name"],
            "contract": values.get("contract", "1.0"),
            "input_schema": _schema(files, values.get("input_schema"), f"service-{index}-input", path, node),
            "output_schema": _schema(files, values.get("output_schema"), f"service-{index}-output", path, node),
        })
    subscriptions = []
    for index, (values, node, _) in enumerate(
        sorted(handlers["subscribe"], key=lambda item: item[0]["subscriber_id"])
    ):
        unknown = sorted(set(values) - {"subscriber_id", "topic", "contract", "schema", "max_attempts"})
        if unknown:
            _error(f"Unsupported @subscribe arguments: {', '.join(unknown)}", path, node)
        item = {
            "subscriber_id": values["subscriber_id"], "topic": values.get("topic"),
            "contract": values.get("contract"),
            "schema": _schema(files, values.get("schema"), f"subscription-{index}", path, node),
        }
        if "max_attempts" in values:
            item["max_attempts"] = values["max_attempts"]
        subscriptions.append(item)
    jobs = []
    for index, (values, node, _) in enumerate(
        sorted(handlers["job"], key=lambda item: item[0]["handler_id"])
    ):
        unknown = sorted(set(values) - {"handler_id", "input_schema", "max_attempts", "timeout_s", "max_concurrency"})
        if unknown:
            _error(f"Unsupported @job arguments: {', '.join(unknown)}", path, node)
        item = {
            "handler_id": values["handler_id"],
            "input_schema": _schema(files, values.get("input_schema"), f"job-{index}-input", path, node),
        }
        for key in ("max_attempts", "timeout_s", "max_concurrency"):
            if key in values:
                item[key] = values[key]
        jobs.append(item)
    commands = []
    command_names = set()
    command_handlers = set()
    command_items = sorted(
        handlers["command"],
        key=lambda item: command_identifier(item[0].get("name"), item[0].get("id")),
    )
    for index, (values, node, handler) in enumerate(command_items):
        unknown = sorted(set(values) - {"name", "permission", "id", "args", "schema", "priority"})
        if unknown:
            _error(f"Unsupported @command arguments: {', '.join(unknown)}", path, node)
        try:
            name = normalize_command_name(values.get("name"))
            command_id = command_identifier(name, values.get("id"))
            permission = validate_command_permission(values.get("permission"))
            argument_mode = validate_argument_mode(values.get("args", "none"))
            priority = validate_command_priority(values.get("priority", 0))
        except ValueError as exc:
            _error(str(exc), path, node)
        if name in command_names:
            _error(f"Duplicate @command name {name}", path, node)
        if handler in command_handlers:
            _error(f"Duplicate @command handler {handler}", path, node)
        command_names.add(name)
        command_handlers.add(handler)
        commands.append({
            "command_id": command_id,
            "name": name,
            "handler": handler,
            "permission": permission,
            "argument_mode": argument_mode,
            "input_schema": _schema(
                files, values.get("schema"), f"command-{index}-input", path, node
            ),
            "priority": priority,
        })
    message_handlers = []
    message_methods = set()
    for values, node, handler in sorted(
        handlers["message_handler"], key=lambda item: item[0]["handler_id"]
    ):
        unknown = sorted(set(values) - {"handler_id", "priority"})
        if unknown:
            _error(f"Unsupported @message_handler arguments: {', '.join(unknown)}", path, node)
        priority = values.get("priority", 0)
        if type(priority) is not int or not -100 <= priority <= 100:
            _error("Message handler priority must be an integer from -100 to 100", path, node)
        if handler in message_methods:
            _error(f"Duplicate @message_handler method {handler}", path, node)
        message_methods.add(handler)
        message_handlers.append({
            "handler_id": values["handler_id"],
            "handler": handler,
            "priority": priority,
        })
    config_schema = _schema(
        files, declaration.get("config_schema"), "config", path, class_node
    )
    component: dict[str, Any] = {
        "id": declaration["id"],
        "scope": declaration.get("scope", "bot"),
        "state_schema": declaration.get("state_schema", 1),
        "reload_mode": declaration.get("reload_mode", "drain"),
        "config_schema": config_schema,
        "capabilities": {"requests": declaration.get("requests", [])},
        "provides": services,
    }
    for key in ("resources", "lifecycle"):
        if key in declaration:
            component[key] = declaration[key]
    if "message_handler_takeover" in declaration:
        component["message_handler_takeover"] = declaration["message_handler_takeover"]
    for key in ("state_value_schema",):
        if key in declaration:
            component[key] = _schema(files, declaration[key], key.replace("_", "-"), path, class_node)
    for key in ("requires", "publishes", "state_migrations"):
        values = _items(declaration.get(key), key, path, class_node)
        if values:
            component[key] = values
    for index, publication in enumerate(component.get("publishes", [])):
        publication["schema"] = _schema(
            files,
            publication.get("schema"),
            f"publication-{index}",
            path,
            class_node,
        )
    if subscriptions:
        component["subscribes"] = subscriptions
    if jobs:
        component["jobs"] = jobs
    if commands:
        component["commands"] = commands
    if message_handlers:
        component["message_handlers"] = message_handlers
    manifest = {
        "manifest_version": 1,
        "package_id": declaration["id"],
        "version": declaration["version"],
        "kernel_api": declaration.get("kernel_api", ">=1.0,<2.0"),
        "runtime": "python-worker",
        "entrypoint": f"{module}:{class_node.name}",
        "module_root": root,
        "components": [component],
    }
    if "dependency_lock" in declaration:
        manifest["dependency_lock"] = declaration["dependency_lock"]
    files["module.toml"] = _toml(manifest)
    return with_checksums(files)


def with_checksums(files: dict[str, bytes]) -> dict[str, bytes]:
    """Create or refresh the deterministic self-excluding checksum manifest."""

    result = dict(files)
    result.pop("checksums.json", None)
    checksums = {
        "algorithm": "sha256",
        "files": {name: hashlib.sha256(value).hexdigest() for name, value in sorted(result.items())},
    }
    result["checksums.json"] = (
        json.dumps(checksums, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    return result


def verify_checksums(files: dict[str, bytes]) -> None:
    if "checksums.json" not in files:
        return
    try:
        value = json.loads(files["checksums.json"])
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError("PACKAGE_CHECKSUM_INVALID", "Invalid checksums.json", 422) from exc
    expected = {
        name: hashlib.sha256(content).hexdigest()
        for name, content in sorted(files.items())
        if name != "checksums.json"
    }
    if value != {"algorithm": "sha256", "files": expected}:
        raise ForestError(
            "PACKAGE_CHECKSUM_MISMATCH",
            "checksums.json does not bind every immutable package file",
            422,
        )
