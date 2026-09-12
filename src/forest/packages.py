"""Static package validation and immutable, content-addressed Python snapshots.

The catalog is an artifact cache, never a routing or Bot state authority. Package
validation does not import user code. Runtime identity is allocated by the kernel.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from forest.errors import ForestError
from forest.security import private_directory

SKIPPED = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"}
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_PACKAGE_BYTES = 32 * 1024 * 1024
MAX_PACKAGE_FILES = 512
MAX_PACKAGE_ENTRIES = 4096
MAX_PACKAGE_DEPTH = 32
IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
ENTRYPOINT = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*:[a-zA-Z_]\w*\Z")
VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?\Z")
MANIFEST_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["manifest_version", "runtime", "kernel_api", "package_id",
                                    "version", "entrypoint", "components"],
    "properties": {
        "manifest_version": {"type": "integer", "const": 1},
        **{key: {"type": "string"} for key in
           ("runtime", "kernel_api", "package_id", "version", "entrypoint")},
        "components": {"type": "array", "minItems": 1, "maxItems": 1, "items": {
            "type": "object", "additionalProperties": False,
            "required": ["id", "scope", "state_schema", "reload_mode",
                                            "config_schema", "provides"],
            "properties": {
                **{key: {"type": "string"} for key in
                   ("id", "scope", "reload_mode", "config_schema")},
                "state_schema": {"type": "integer"},
                "state_mode": {"const": "durable"},
                "checkpoint_schema": {"type": "integer", "const": 1},
                "requires": {"type": "array"},
                "resources": {"type": "object", "additionalProperties": False,
                              "properties": {
                                  "max_frame_bytes": {"type": "integer", "minimum": 1024,
                                                      "maximum": 262144},
                                  "max_queue_items": {"type": "integer", "minimum": 0,
                                                      "maximum": 256},
                                  **{key: {"type": "integer", "minimum": 1, "maximum": 256}
                                     for key in ("max_inflight", "max_state_inflight",
                                                 "max_owned_tasks")},
                              }},
                "lifecycle": {"type": "object", "additionalProperties": False,
                              "properties": {
                                  key: {"type": "number", "exclusiveMinimum": 0, "maximum": 120}
                                  for key in ("prepare_timeout_s", "start_timeout_s",
                                              "drain_timeout_s", "stop_timeout_s")}},
                "capabilities": {"type": "object", "additionalProperties": False,
                                 "properties": {
                    "requests": {"type": "array", "items": {"type": "string"}}}},
                "provides": {"type": "array", "items": {
                    "type": "object", "additionalProperties": False, "required":
                    ["name", "contract", "input_schema", "output_schema"], "properties": {
                        key: {"type": "string"} for key in
                        ("name", "contract", "input_schema", "output_schema")}}},
            },
        }},
    },
}


def digest_json(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                         allow_nan=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _fail(code: str, message: str) -> None:
    raise ForestError(code, message, 422)


def _signature(item: os.stat_result) -> tuple:
    return (item.st_dev, item.st_ino, item.st_mode, item.st_size, item.st_mtime_ns,
            item.st_ctime_ns, item.st_nlink, getattr(item, "st_reparse_tag", 0))


def _reject_link(info: os.stat_result) -> None:
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0):
        _fail("PACKAGE_LINK_REJECTED", "Package links and reparse points are not supported")


def _capture(root: Path) -> dict[str, bytes]:
    try:
        return _capture_files(root)
    except OSError as exc:
        raise ForestError("PACKAGE_CHANGED", "Package became unavailable during capture", 422) from exc


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
            if (name.startswith(".env") or re.search(r'[<>:"\\|?*\x00-\x1f]', name)
                    or name.endswith((".", " ")) or stem in {"CON", "PRN", "AUX", "NUL"}
                    or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)):
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
            if info.st_size > MAX_FILE_BYTES or len(captured) >= MAX_PACKAGE_FILES:
                _fail("PACKAGE_TOO_LARGE", "Package exceeds file limits")
            flags = (os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
                     | getattr(os, "O_NONBLOCK", 0))
            with os.fdopen(os.open(path, flags), "rb") as stream:
                before = os.fstat(stream.fileno())
                if _signature(info) != _signature(before):
                    _fail("PACKAGE_CHANGED", "Package entry changed before being read")
                content = stream.read(MAX_FILE_BYTES + 1)
                after = os.fstat(stream.fileno())
            if (_signature(before) != _signature(after)
                    or _signature(after) != _signature(path.lstat())
                    or len(content) != after.st_size):
                _fail("PACKAGE_CHANGED", "Package changed while being captured")
            total += len(content)
            if len(content) > MAX_FILE_BYTES or total > MAX_PACKAGE_BYTES:
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
    try:
        manifest = tomllib.loads(files["module.toml"].decode("utf-8"))
    except (KeyError, ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError("INVALID_MANIFEST", "A valid module.toml is required", 422) from exc
    if not Draft202012Validator(MANIFEST_SCHEMA).is_valid(manifest):
        _fail("INVALID_MANIFEST", "Manifest fields do not match the supported schema")
    try:
        digest_json(manifest)
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ForestError("INVALID_MANIFEST", "Manifest must contain finite JSON values", 422) from exc
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
    module = "python/" + entrypoint.split(":")[0].replace(".", "/")
    if module + ".py" not in files and module + "/__init__.py" not in files:
        _fail("ENTRYPOINT_MISSING", "Entrypoint is absent from package python/ directory")
    components = manifest.get("components")
    if not isinstance(components, list) or len(components) != 1:
        _fail("UNSUPPORTED_COMPONENT_SET", "This runtime currently supports one component per package")
    component = components[0]
    if not IDENTIFIER.fullmatch(component.get("id", "")) or component.get("scope") != "bot":
        _fail("INVALID_COMPONENT", "A valid Bot-scoped component is required")
    if component.get("state_schema") != 1 or component.get("reload_mode") != "drain":
        _fail("UNSUPPORTED_STATE_SCHEMA", "This slice supports state schema 1 and drain only")
    if component.get("requires"):
        _fail("DEPENDENCIES_NOT_IMPLEMENTED", "Cross-component service dependencies are not available yet")
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
        if not re.fullmatch(r"[1-9]\d*\.\d+", service.get("contract", "")):
            _fail("INVALID_SERVICES", "A service contract major.minor is required")
        _schema(files, service.get("input_schema"))
        _schema(files, service.get("output_schema"))
    capabilities = component.get("capabilities", {}).get("requests", [])
    if not isinstance(capabilities, list) or any(
        item not in {"state.read", "state.write"} for item in capabilities
    ) or len(capabilities) != len(set(capabilities)):
        _fail("UNSUPPORTED_CAPABILITY", "This SDK slice supports explicit state.read/state.write requests")
    for relative, source in files.items():
        if relative.endswith(".py"):
            try:
                compile(source, relative, "exec", dont_inherit=True)
            except (SyntaxError, ValueError) as exc:
                raise ForestError("PACKAGE_SYNTAX_ERROR", f"Invalid Python in {relative}", 422) from exc
    return manifest


@dataclass(frozen=True)
class PackageSnapshot:
    root: Path
    digest: str
    manifest: dict

    @property
    def module_root(self) -> Path:
        return self.root / "python"

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
        actual = digest_json({name: hashlib.sha256(value).hexdigest()
                              for name, value in files.items()})
        if actual != self.digest or _manifest(files) != self.manifest:
            _fail("PACKAGE_TAMPERED", "The immutable package no longer matches its captured digest")
        return files

    def verify(self) -> None:
        self._verified_files()


class PackageCatalog:
    def __init__(self, root: Path) -> None:
        self.root = private_directory(root)

    def build(self, source: Path) -> PackageSnapshot:
        try:
            _reject_link(source.lstat())
            source = source.resolve(strict=True)
        except OSError as exc:
            raise ForestError("INVALID_PACKAGE_PATH", "Package source is unavailable", 422) from exc
        catalog_root = self.root.resolve()
        if catalog_root == source or catalog_root.is_relative_to(source):
            _fail("INVALID_PACKAGE_PATH", "Artifact catalog cannot be inside the source package")
        captured = _capture(source)
        manifest = _manifest(captured)
        if captured != _capture(source):
            _fail("PACKAGE_CHANGED", "Package must remain stable throughout capture")
        digest = digest_json({name: hashlib.sha256(value).hexdigest()
                              for name, value in captured.items()})
        destination = self.root / digest.removeprefix("sha256:")
        with sqlite3.connect(self.root / "catalog.sqlite3", timeout=10) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("CREATE TABLE IF NOT EXISTS packages (package_id TEXT, version TEXT, "
                       "digest TEXT NOT NULL, PRIMARY KEY(package_id,version))")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT digest FROM packages WHERE package_id=? AND version=?",
                             (manifest["package_id"], manifest["version"])).fetchone()
            if row is not None and row[0] != digest:
                _fail("PACKAGE_VERSION_CONFLICT", "A package version cannot silently change content")
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
                    raise ForestError("PACKAGE_STORAGE_ERROR", "Unable to publish package artifact",
                                      500) from exc
                finally:
                    # Only this build's still-unpublished directory may be removed. Check the
                    # resolved parent and reparse status before any recursive Windows cleanup.
                    if staging.exists():
                        info = staging.lstat()
                        if (not stat.S_ISLNK(info.st_mode)
                                and not getattr(info, "st_reparse_tag", 0)
                                and staging.resolve().parent == self.root.resolve()):
                            shutil.rmtree(staging)
            snapshot = PackageSnapshot(destination, digest, manifest)
            snapshot.verify()
            db.execute("INSERT OR IGNORE INTO packages VALUES (?,?,?)",
                       (manifest["package_id"], manifest["version"], digest))
        return snapshot
