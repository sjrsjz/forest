"""Static package validation and immutable, content-addressed Python snapshots.

The catalog is an artifact cache, never a routing or Bot state authority. Package
validation does not import user code. Runtime identity is allocated by the kernel.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import stat
import sys
import tomllib
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from forest.errors import ForestError
from forest.plugin_declarations import synthesize_manifest, verify_checksums, with_checksums
from forest.security import private_directory

SKIPPED = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"}
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_WHEEL_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_FILES = 512
MAX_PACKAGE_ENTRIES = 4096
MAX_PACKAGE_DEPTH = 32
IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
ENTRYPOINT = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*:[a-zA-Z_]\w*\Z")
VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?\Z")
MANIFEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "manifest_version",
        "runtime",
        "kernel_api",
        "package_id",
        "version",
        "entrypoint",
        "components",
    ],
    "properties": {
        "manifest_version": {"type": "integer", "const": 1},
        **{
            key: {"type": "string"}
            for key in ("runtime", "kernel_api", "package_id", "version", "entrypoint")
        },
        "dependency_lock": {"type": "string"},
        "module_root": {"type": "string"},
        "components": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "id",
                    "scope",
                    "state_schema",
                    "reload_mode",
                    "config_schema",
                    "provides",
                ],
                "properties": {
                    **{
                        key: {"type": "string"}
                        for key in ("id", "scope", "reload_mode", "config_schema")
                    },
                    "state_schema": {"type": "integer", "minimum": 1, "maximum": 2147483647},
                    "state_value_schema": {"type": "string"},
                    "state_migrations": {
                        "type": "array", "maxItems": 64,
                        "items": {
                            "type": "object", "additionalProperties": False,
                            "required": ["from_schema", "to_schema"],
                            "properties": {
                                key: {"type": "integer", "minimum": 1, "maximum": 2147483647}
                                for key in ("from_schema", "to_schema")
                            },
                        },
                    },
                    "state_mode": {"const": "durable"},
                    "checkpoint_schema": {"type": "integer", "const": 1},
                    "requires": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "contract"],
                            "properties": {
                                "name": {"type": "string"},
                                "contract": {"type": "string"},
                                "required": {"type": "boolean"},
                                "startup_dependency": {"type": "boolean"},
                            },
                        },
                    },
                    "resources": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "max_frame_bytes": {
                                "type": "integer",
                                "minimum": 1024,
                                "maximum": 262144,
                            },
                            "max_queue_items": {"type": "integer", "minimum": 0, "maximum": 256},
                            **{
                                key: {"type": "integer", "minimum": 1, "maximum": 256}
                                for key in (
                                    "max_inflight",
                                    "max_state_inflight",
                                    "max_owned_tasks",
                                    "max_owned_resources",
                                )
                            },
                        },
                    },
                    "lifecycle": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            key: {"type": "number", "exclusiveMinimum": 0, "maximum": 120}
                            for key in (
                                "prepare_timeout_s",
                                "start_timeout_s",
                                "drain_timeout_s",
                                "stop_timeout_s",
                            )
                        },
                    },
                    "capabilities": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"requests": {"type": "array", "items": {"type": "string"}}},
                    },
                    "provides": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "contract", "input_schema", "output_schema"],
                            "properties": {
                                key: {"type": "string"}
                                for key in ("name", "contract", "input_schema", "output_schema")
                            },
                        },
                    },
                    "publishes": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["topic", "contract", "schema"],
                            "properties": {
                                key: {"type": "string"}
                                for key in ("topic", "contract", "schema")
                            },
                        },
                    },
                    "subscribes": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["subscriber_id", "topic", "contract", "schema"],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in ("subscriber_id", "topic", "contract", "schema")
                                },
                                "max_attempts": {
                                    "type": "integer", "minimum": 1, "maximum": 32
                                },
                            },
                        },
                    },
                    "jobs": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["handler_id", "input_schema"],
                            "properties": {
                                "handler_id": {"type": "string"},
                                "input_schema": {"type": "string"},
                                "max_attempts": {
                                    "type": "integer", "minimum": 1, "maximum": 32
                                },
                                "timeout_s": {
                                    "type": "number", "exclusiveMinimum": 0, "maximum": 120
                                },
                                "max_concurrency": {
                                    "type": "integer", "minimum": 1, "maximum": 16
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


def digest_json(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _fail(code: str, message: str) -> None:
    raise ForestError(code, message, 422)


def contract_range(contract: str) -> tuple[tuple[int, int], tuple[int, int] | None]:
    """Explicit major.minor or >=major.minor,<major.minor, without implicit coercion."""
    if not isinstance(contract, str) or len(contract) > 64:
        _fail("INVALID_DEPENDENCY", "Dependency contract must be a version or bounded range")
    exact = re.fullmatch(r"([1-9]\d*)\.(\d+)", contract)
    if exact:
        return (int(exact[1]), int(exact[2])), None
    interval = re.fullmatch(r">=([1-9]\d*)\.(\d+),<([1-9]\d*)\.(\d+)", contract)
    if interval:
        lower, upper = (int(interval[1]), int(interval[2])), (int(interval[3]), int(interval[4]))
        if lower < upper:
            return lower, upper
    _fail("INVALID_DEPENDENCY", "Supported contracts are major.minor or >=major.minor,<major.minor")


def contract_matches(contract: str, provided: str) -> bool:
    lower, upper = contract_range(contract)
    version = tuple(map(int, provided.split(".")))
    return version == lower if upper is None else lower <= version < upper


def _signature(item: os.stat_result) -> tuple:
    return (
        item.st_dev,
        item.st_ino,
        item.st_mode,
        item.st_size,
        item.st_mtime_ns,
        item.st_ctime_ns,
        item.st_nlink,
        getattr(item, "st_reparse_tag", 0),
    )


def _reject_link(info: os.stat_result) -> None:
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0):
        _fail("PACKAGE_LINK_REJECTED", "Package links and reparse points are not supported")


def _capture(root: Path) -> dict[str, bytes]:
    try:
        return _capture_files(root)
    except OSError as exc:
        raise ForestError(
            "PACKAGE_CHANGED", "Package became unavailable during capture", 422
        ) from exc


def source_fingerprint(source: Path) -> str:
    """Watch/build share one complete, portable capture and exclusion policy."""
    return digest_json({
        name: hashlib.sha256(value).hexdigest() for name, value in _capture(source).items()
    })


def _prepared_source(source_files: dict[str, bytes]) -> dict[str, bytes]:
    files = synthesize_manifest(source_files)
    if len(files) > MAX_PACKAGE_FILES or sum(map(len, files.values())) > MAX_PACKAGE_BYTES:
        _fail("PACKAGE_TOO_LARGE", "Generated package exceeds file or byte limits")
    if any(
        len(content) > (MAX_WHEEL_BYTES if name.endswith(".whl") else MAX_FILE_BYTES)
        for name, content in files.items()
    ):
        _fail("PACKAGE_TOO_LARGE", "Generated package file exceeds its byte limit")
    return files


def _capture_files(root: Path) -> dict[str, bytes]:
    captured: dict[str, bytes] = {}
    names: set[str] = set()
    total = 0
    root_info = root.lstat()
    _reject_link(root_info)
    if not stat.S_ISDIR(root_info.st_mode):
        _fail("INVALID_PACKAGE_PATH", "Package source must be a regular directory")
    pending = [(root, root_info)]
    directories = []
    entry_count = 0
    while pending:
        current, current_info = pending.pop()
        if _signature(current.lstat()) != _signature(current_info):
            _fail("PACKAGE_CHANGED", "Package directory changed during capture")
        directories.append((current, current_info))
        # scandir errors must propagate; os.walk's default would silently omit unreadable trees.
        with os.scandir(current) as entries:
            children = []
            for entry in entries:
                entry_count += 1
                if entry_count > MAX_PACKAGE_ENTRIES:
                    _fail("PACKAGE_TOO_LARGE", "Package exceeds directory entry limits")
                children.append(entry.name)
        for name in sorted(children):
            path = current / name
            info = path.lstat()
            _reject_link(info)
            if stat.S_ISDIR(info.st_mode) and name in SKIPPED:
                continue
            rel = path.relative_to(root).as_posix()
            stem = name.split(".", 1)[0].upper()
            if (
                name.startswith(".env")
                or re.search(r'[<>:"\\|?*\x00-\x1f]', name)
                or name.endswith((".", " "))
                or stem in {"CON", "PRN", "AUX", "NUL"}
                or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)
            ):
                _fail("PACKAGE_FILE_REJECTED", "Package contains a private or nonportable path")
            if rel.casefold() in names:
                _fail("PACKAGE_PATH_COLLISION", "Package has case-insensitive path collisions")
            names.add(rel.casefold())
            if len(path.relative_to(root).parts) > MAX_PACKAGE_DEPTH:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds directory depth limits")
            if stat.S_ISDIR(info.st_mode):
                pending.append((path, info))
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                _fail("PACKAGE_FILE_REJECTED", "Package entries must be ordinary single-link files")
            file_limit = MAX_WHEEL_BYTES if rel.endswith(".whl") else MAX_FILE_BYTES
            if info.st_size > file_limit or len(captured) >= MAX_PACKAGE_FILES:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds file limits")
            flags = (
                os.O_RDONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            with os.fdopen(os.open(path, flags), "rb") as stream:
                before = os.fstat(stream.fileno())
                if _signature(info) != _signature(before):
                    _fail("PACKAGE_CHANGED", "Package entry changed before being read")
                content = stream.read(file_limit + 1)
                after = os.fstat(stream.fileno())
            if (
                _signature(before) != _signature(after)
                or _signature(after) != _signature(path.lstat())
                or len(content) != after.st_size
            ):
                _fail("PACKAGE_CHANGED", "Package changed while being captured")
            total += len(content)
            if len(content) > file_limit or total > MAX_PACKAGE_BYTES:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds byte limits")
            captured[rel] = content
    for directory, initial in directories:
        if _signature(directory.lstat()) != _signature(initial):
            _fail("PACKAGE_CHANGED", "Package directory changed during capture")
    return captured


def _schema(files: dict[str, bytes], reference: str) -> dict:
    if not isinstance(reference, str):
        _fail("INVALID_SCHEMA", "Schema reference must name a package JSON file")
    parts = PurePosixPath(reference)
    if parts.is_absolute() or ".." in parts.parts or reference not in files:
        _fail("INVALID_SCHEMA", "Schema must be contained in the immutable package")
    try:

        def reject_constant(value):
            raise ValueError(f"Non-finite JSON number: {value}")

        value = json.loads(files[reference], parse_constant=reject_constant)
        Draft202012Validator.check_schema(value)

        # Resolve no network or external filesystem references during admission.
        def references(node):
            if isinstance(node, dict):
                for key, item in node.items():
                    if key in {"$id"} and item:
                        _fail("INVALID_SCHEMA", "Schema base URI changes are not supported")
                    if key in {"$ref", "$dynamicRef"} and (
                        not isinstance(item, str) or not item.startswith("#")
                    ):
                        _fail("INVALID_SCHEMA", "Schema references must be local fragments")
                    references(item)
            elif isinstance(node, list):
                for item in node:
                    references(item)

        references(value)
        if not isinstance(value, dict):
            _fail("INVALID_SCHEMA", "Schema must be a JSON object")
        return value
    except (ValueError, UnicodeError, SchemaError, RecursionError) as exc:
        raise ForestError("INVALID_SCHEMA", "Invalid package JSON schema", 422) from exc


def _manifest(files: dict[str, bytes]) -> dict:
    verify_checksums(files)
    try:
        manifest = tomllib.loads(files["module.toml"].decode("utf-8"))
    except (KeyError, ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError("INVALID_MANIFEST", "A valid module.toml is required", 422) from exc
    if not Draft202012Validator(MANIFEST_SCHEMA).is_valid(manifest):
        _fail("INVALID_MANIFEST", "Manifest fields do not match the supported schema")
    try:
        digest_json(manifest)
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError(
            "INVALID_MANIFEST", "Manifest must contain finite JSON values", 422
        ) from exc
    if manifest.get("manifest_version") != 1 or manifest.get("runtime") != "python-worker":
        _fail("UNSUPPORTED_MANIFEST", "Only manifest v1 and python-worker are implemented")
    if manifest.get("kernel_api") != ">=1.0,<2.0":
        _fail("KERNEL_API_INCOMPATIBLE", "Package must target kernel API >=1.0,<2.0")
    for key in ("package_id",):
        if not isinstance(manifest.get(key), str) or not IDENTIFIER.fullmatch(manifest[key]):
            _fail("INVALID_MANIFEST", f"Invalid {key}")
    if not isinstance(manifest.get("version"), str) or not VERSION.fullmatch(manifest["version"]):
        _fail("INVALID_MANIFEST", "Invalid package version")
    entrypoint = manifest.get("entrypoint", "")
    if not isinstance(entrypoint, str) or not ENTRYPOINT.fullmatch(entrypoint):
        _fail("INVALID_MANIFEST", "Entrypoint must be a Python module:Class")
    module_root = manifest.get("module_root", "python")
    root = PurePosixPath(module_root)
    if (
        not isinstance(module_root, str)
        or not module_root
        or len(module_root) > 128
        or root.is_absolute()
        or module_root != root.as_posix()
        or any(part in {"", ".", ".."} for part in root.parts)
    ):
        _fail("INVALID_MANIFEST", "module_root must be a portable relative directory")
    module = module_root + "/" + entrypoint.split(":")[0].replace(".", "/")
    if module + ".py" not in files and module + "/__init__.py" not in files:
        _fail("ENTRYPOINT_MISSING", "Entrypoint is absent from the declared module_root")
    components = manifest.get("components")
    if not isinstance(components, list) or len(components) != 1:
        _fail(
            "UNSUPPORTED_COMPONENT_SET", "This runtime currently supports one component per package"
        )
    component = components[0]
    if not IDENTIFIER.fullmatch(component.get("id", "")) or component.get("scope") != "bot":
        _fail("INVALID_COMPONENT", "A valid Bot-scoped component is required")
    if component.get("reload_mode") != "drain":
        _fail("UNSUPPORTED_STATE_SCHEMA", "Only drain reload is supported")
    if "state_value_schema" in component:
        _schema(files, component["state_value_schema"])
    migration_sources = set()
    for migration in component.get("state_migrations", []):
        if (migration["to_schema"] != component["state_schema"]
                or migration["from_schema"] == migration["to_schema"]
                or migration["from_schema"] in migration_sources):
            _fail("INVALID_STATE_MIGRATION", "Migration sources must uniquely target candidate state")
        migration_sources.add(migration["from_schema"])
    dependencies = component.get("requires", [])
    dependency_names = set()
    for dependency in dependencies:
        name = dependency["name"]
        if not IDENTIFIER.fullmatch(name) or name in dependency_names:
            _fail("INVALID_DEPENDENCY", "Dependency service names must be valid and unique")
        contract_range(dependency["contract"])
        dependency_names.add(name)
    _schema(files, component.get("config_schema"))
    services = component.get("provides", [])
    if not services or len(services) > 32:
        _fail("INVALID_SERVICES", "Declare between one and 32 versioned services")
    names = set()
    for service in services:
        name = service.get("name", "")
        if not IDENTIFIER.fullmatch(name) or name in names:
            _fail("INVALID_SERVICES", "Service names must be valid and unique")
        names.add(name)
        if len(service.get("contract", "")) > 64 or not re.fullmatch(
            r"[1-9]\d*\.\d+", service.get("contract", "")
        ):
            _fail("INVALID_SERVICES", "A service contract major.minor is required")
        _schema(files, service.get("input_schema"))
        _schema(files, service.get("output_schema"))
    published_topics = set()
    for publication in component.get("publishes", []):
        topic = publication.get("topic", "")
        if not IDENTIFIER.fullmatch(topic) or topic in published_topics:
            _fail("INVALID_EVENTS", "Published event topics must be valid and unique")
        published_topics.add(topic)
        if not re.fullmatch(r"[1-9]\d*\.\d+", publication.get("contract", "")):
            _fail("INVALID_EVENTS", "Published events require an exact major.minor contract")
        _schema(files, publication.get("schema"))
    subscriber_ids = set()
    for subscription in component.get("subscribes", []):
        subscriber_id = subscription.get("subscriber_id", "")
        if not IDENTIFIER.fullmatch(subscriber_id) or subscriber_id in subscriber_ids:
            _fail("INVALID_EVENTS", "Durable subscriber identities must be valid and unique")
        subscriber_ids.add(subscriber_id)
        if not IDENTIFIER.fullmatch(subscription.get("topic", "")) or not re.fullmatch(
            r"[1-9]\d*\.\d+", subscription.get("contract", "")
        ):
            _fail("INVALID_EVENTS", "Subscriptions require a valid topic and exact contract")
        _schema(files, subscription.get("schema"))
    job_ids = set()
    for definition in component.get("jobs", []):
        handler_id = definition.get("handler_id", "")
        if not IDENTIFIER.fullmatch(handler_id) or handler_id in job_ids:
            _fail("INVALID_JOBS", "Durable job handler identities must be valid and unique")
        job_ids.add(handler_id)
        _schema(files, definition.get("input_schema"))
    capabilities = component.get("capabilities", {}).get("requests", [])
    allowed_capabilities = {"state.read", "state.write"} | {
        "service.call:" + name for name in dependency_names
    } | {"event.publish:" + topic for topic in published_topics} | {
        "job.schedule:" + handler_id for handler_id in job_ids
    }
    if any(item not in allowed_capabilities for item in capabilities) or len(capabilities) != len(
        set(capabilities)
    ):
        _fail(
            "UNSUPPORTED_CAPABILITY",
            "Capabilities must name an explicitly declared state, service, event, or job operation",
        )
    for relative, source in files.items():
        if relative.endswith(".py"):
            try:
                compile(source, relative, "exec", dont_inherit=True)
            except (SyntaxError, ValueError) as exc:
                raise ForestError(
                    "PACKAGE_SYNTAX_ERROR", f"Invalid Python in {relative}", 422,
                    details={"file": relative, "line": getattr(exc, "lineno", None),
                             "column": getattr(exc, "offset", None), "phase": "syntax"},
                ) from exc
    if "dependency_lock" in manifest:
        from forest.environments import validate_lock

        validate_lock(files, manifest["dependency_lock"])
    elif any(path.endswith(".whl") for path in files):
        _fail("INVALID_DEPENDENCY_LOCK", "Vendored wheels require an explicit dependency lock")
    return manifest


@dataclass(frozen=True)
class PackageSnapshot:
    root: Path
    digest: str
    manifest: dict

    @property
    def module_root(self) -> Path:
        return self.root / self.manifest.get("module_root", "python")

    @property
    def entrypoint(self) -> str:
        return self.manifest["entrypoint"]

    @property
    def component(self) -> dict:
        return self.manifest["components"][0]

    def schema(self, reference: str) -> dict:
        return _schema(self._verified_files(), reference)

    def _verified_files(self) -> dict[str, bytes]:
        files = _capture(self.root)
        actual = digest_json(
            {name: hashlib.sha256(value).hexdigest() for name, value in files.items()}
        )
        if actual != self.digest or _manifest(files) != self.manifest:
            _fail("PACKAGE_TAMPERED", "The immutable package no longer matches its captured digest")
        return files

    def verify(self) -> None:
        self._verified_files()


class PackageCatalog:
    def __init__(self, root: Path) -> None:
        self.root = private_directory(root)

    def build(self, source: Path, *, development: bool = False) -> PackageSnapshot:
        try:
            _reject_link(source.lstat())
            source = source.resolve(strict=True)
        except OSError as exc:
            raise ForestError("INVALID_PACKAGE_PATH", "Package source is unavailable", 422) from exc
        catalog_root = self.root.resolve()
        if catalog_root == source or catalog_root.is_relative_to(source):
            _fail("INVALID_PACKAGE_PATH", "Artifact catalog cannot be inside the source package")
        source_files = _capture(source)
        captured = _prepared_source(source_files)
        _manifest(captured)
        if captured != _prepared_source(_capture(source)):
            _fail("PACKAGE_CHANGED", "Package must remain stable throughout capture")
        if development:
            source_digest = digest_json(
                {name: hashlib.sha256(value).hexdigest() for name, value in source_files.items()}
            ).removeprefix("sha256:")
            # The manifest has already passed TOML/schema validation. Replace the
            # top-level version only; no source file is changed by a dev build.
            lines = captured["module.toml"].decode("utf-8").splitlines(keepends=True)
            replaced = False
            for index, line in enumerate(lines):
                if line.lstrip().startswith("["):
                    break
                if re.match(r"\s*version\s*=", line):
                    base = _manifest(captured)["version"].split("+", 1)[0]
                    lines[index] = f'version = "{base}+dev.{source_digest}"\n'
                    replaced = True
                    break
            if not replaced:
                _fail("INVALID_MANIFEST", "Development builds require a top-level version")
            captured["module.toml"] = "".join(lines).encode("utf-8")
            if "checksums.json" in captured:
                captured = with_checksums(captured)
        return self._publish(captured)

    def import_archive(
        self, data: bytes, *, check_manifest: Callable[[dict], None] | None = None,
        bind_version: bool = True,
    ) -> PackageSnapshot:
        """Validate a complete ZIP before publishing any package file."""
        if not isinstance(data, bytes) or len(data) > MAX_ARCHIVE_BYTES:
            _fail("PACKAGE_TOO_LARGE", "Package ZIP exceeds the upload limit")
        captured = archive_files(data)
        manifest = _manifest(captured)
        if check_manifest is not None:
            # Deployment namespace policy must reject before the local artifact
            # registry reserves an immutable package_id/version pair.
            check_manifest(manifest)
        return self._publish(captured, bind_version=bind_version)

    def export_archive(self, digest: str) -> bytes:
        files = self.get(digest)._verified_files()
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in sorted(files.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | 0o400) << 16
                archive.writestr(entry, content)
        result = output.getvalue()
        if len(result) > MAX_ARCHIVE_BYTES:
            _fail("PACKAGE_TOO_LARGE", "Exported ZIP exceeds the archive limit")
        return result

    def get(self, digest: str) -> PackageSnapshot:
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            _fail("INVALID_PACKAGE_DIGEST", "Package digest must be SHA-256")
        rows = self._rows("SELECT digest FROM packages WHERE digest=?", (digest,))
        if not rows:
            rows = self._rows("SELECT digest FROM artifacts WHERE digest=?", (digest,),
                              table="artifacts")
        if not rows:
            raise ForestError("PACKAGE_NOT_FOUND", "Package artifact is not registered", 404)
        root = self.root / digest.removeprefix("sha256:")
        files = _capture(root)
        snapshot = PackageSnapshot(root, digest, _manifest(files))
        snapshot.verify()
        return snapshot

    def find(self, package_id: str, version: str) -> PackageSnapshot:
        rows = self._rows(
            "SELECT digest FROM packages WHERE package_id=? AND version=?", (package_id, version)
        )
        if not rows:
            raise ForestError("PACKAGE_NOT_FOUND", "Package version is not registered", 404)
        return self.get(rows[0][0])

    def list(self) -> list[PackageSnapshot]:
        return [self.get(row[0]) for row in self._rows(
            "SELECT DISTINCT digest FROM packages ORDER BY package_id,version", ()
        )]

    def _rows(self, query: str, parameters: tuple, *, table: str = "packages") -> list[tuple]:
        path = self.root / "catalog.sqlite3"
        if not path.exists():
            return []
        with sqlite3.connect(path, timeout=10) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                              (table,)).fetchone():
                return []
            return db.execute(query, parameters).fetchall()

    def _publish(self, captured: dict[str, bytes], *, bind_version: bool = True) -> PackageSnapshot:
        manifest = _manifest(captured)
        digest = digest_json(
            {name: hashlib.sha256(value).hexdigest() for name, value in captured.items()}
        )
        destination = self.root / digest.removeprefix("sha256:")
        with sqlite3.connect(self.root / "catalog.sqlite3", timeout=10) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS packages (package_id TEXT, version TEXT, "
                "digest TEXT NOT NULL, PRIMARY KEY(package_id,version))"
            )
            db.execute("CREATE TABLE IF NOT EXISTS artifacts (digest TEXT PRIMARY KEY)")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT digest FROM packages WHERE package_id=? AND version=?",
                (manifest["package_id"], manifest["version"]),
            ).fetchone()
            if bind_version and row is not None and row[0] != digest:
                _fail(
                    "PACKAGE_VERSION_CONFLICT", "A package version cannot silently change content"
                )
            if not destination.exists():
                staging = self.root / (".capture-" + uuid4().hex)
                staging.mkdir(mode=0o700)
                try:
                    for name, data in captured.items():
                        target = staging / name
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with target.open("xb") as stream:
                            stream.write(data)
                            stream.flush()
                            os.fsync(stream.fileno())
                        if sys.platform != "win32":
                            target.chmod(0o400)
                    staging.rename(destination)
                except OSError as exc:
                    raise ForestError(
                        "PACKAGE_STORAGE_ERROR", "Unable to publish package artifact", 500
                    ) from exc
                finally:
                    # Only this build's still-unpublished directory may be removed. Check the
                    # resolved parent and reparse status before any recursive Windows cleanup.
                    if staging.exists():
                        info = staging.lstat()
                        if (
                            not stat.S_ISLNK(info.st_mode)
                            and not getattr(info, "st_reparse_tag", 0)
                            and staging.resolve().parent == self.root.resolve()
                        ):
                            shutil.rmtree(staging)
            snapshot = PackageSnapshot(destination, digest, manifest)
            snapshot.verify()
            db.execute("INSERT OR IGNORE INTO artifacts VALUES (?)", (digest,))
            if bind_version:
                db.execute(
                    "INSERT OR IGNORE INTO packages VALUES (?,?,?)",
                    (manifest["package_id"], manifest["version"], digest),
                )
        return snapshot


def archive_files(
    data: bytes, *, max_files: int = MAX_PACKAGE_FILES, max_bytes: int = MAX_PACKAGE_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES, allow_wheels: bool = True,
) -> dict[str, bytes]:
    """Bounded portable ZIP decoder, shared by package and wheel admission.

    Never uses extract()/extractall(). Paths are validated before inflation;
    links, ambiguous paths, duplicate members and unsupported compression fail.
    """
    files: dict[str, bytes] = {}
    seen: set[str] = set()
    kinds: dict[str, bool] = {}
    spellings: dict[str, str] = {}
    total = 0
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            if len(entries) > MAX_PACKAGE_ENTRIES:
                _fail("PACKAGE_TOO_LARGE", "ZIP exceeds entry limits")
            for entry in entries:
                name = entry.filename
                directory = entry.is_dir()
                relative = name[:-1] if directory else name
                parts = relative.split("/")
                if (not relative or len(name) > 1024 or entry.orig_filename != name
                        or len(parts) > MAX_PACKAGE_DEPTH):
                    _fail("PACKAGE_FILE_REJECTED", "ZIP contains an invalid path")
                for part in parts:
                    stem = part.split(".", 1)[0].upper()
                    if (not part or part in {".", ".."} or part.startswith(".env")
                            or re.search(r'[<>:"\\|?*\x00-\x1f]', part)
                            or part.endswith((".", " "))
                            or stem in {"CON", "PRN", "AUX", "NUL"}
                            or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)):
                        _fail("PACKAGE_FILE_REJECTED", "ZIP contains a nonportable/private path")
                if any(part in SKIPPED for part in (parts if directory else parts[:-1])):
                    _fail("PACKAGE_FILE_REJECTED", "ZIP contains excluded source/cache directories")
                key = relative.casefold()
                if key in seen:
                    _fail("PACKAGE_PATH_COLLISION", "ZIP contains duplicate or colliding paths")
                seen.add(key)
                # Implicit parent directories also participate in collision checks.
                for length in range(1, len(parts)):
                    original_parent = "/".join(parts[:length])
                    parent = original_parent.casefold()
                    if parent in spellings and spellings[parent] != original_parent:
                        _fail("PACKAGE_PATH_COLLISION", "ZIP has case-colliding directory names")
                    spellings[parent] = original_parent
                    if parent in kinds and not kinds[parent]:
                        _fail("PACKAGE_PATH_COLLISION", "ZIP file shadows a directory")
                    kinds[parent] = True
                if key in kinds and kinds[key] != directory:
                    _fail("PACKAGE_PATH_COLLISION", "ZIP file shadows a directory")
                kinds[key] = directory
                if key in spellings and spellings[key] != relative:
                    _fail("PACKAGE_PATH_COLLISION", "ZIP has case-colliding directory names")
                spellings[key] = relative
                mode = (entry.external_attr >> 16) & 0xFFFF
                file_type = stat.S_IFMT(mode)
                if file_type not in ({0, stat.S_IFDIR} if directory else {0, stat.S_IFREG}):
                    _fail("PACKAGE_LINK_REJECTED", "ZIP links and special files are rejected")
                if entry.flag_bits & 1 or entry.compress_type not in {
                    zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED,
                }:
                    _fail("INVALID_PACKAGE_ARCHIVE", "Unsupported ZIP encryption/compression")
                if directory:
                    if entry.file_size:
                        _fail("INVALID_PACKAGE_ARCHIVE", "ZIP directory contains file content")
                    continue
                limit = MAX_WHEEL_BYTES if allow_wheels and name.endswith(".whl") else max_file_bytes
                if len(files) >= max_files or entry.file_size > limit:
                    _fail("PACKAGE_TOO_LARGE", "ZIP exceeds file limits")
                total += entry.file_size
                if total > max_bytes:
                    _fail("PACKAGE_TOO_LARGE", "ZIP exceeds expanded byte limit")
                with archive.open(entry) as stream:
                    value = stream.read(limit + 1)
                if len(value) != entry.file_size or len(value) > limit:
                    _fail("INVALID_PACKAGE_ARCHIVE", "ZIP entry size does not match content")
                files[name] = value
        return files
    except (zipfile.BadZipFile, NotImplementedError, OSError, ValueError, EOFError) as exc:
        raise ForestError("INVALID_PACKAGE_ARCHIVE", "Invalid complete package ZIP", 422) from exc
