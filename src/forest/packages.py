"""Static package validation and immutable, content-addressed Python snapshots.

The catalog is an artifact cache, never a routing or Bot state authority. Package
validation does not import user code. Runtime identity is allocated by the kernel.
"""

from __future__ import annotations

import hashlib
import io
import json
import keyword
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

from forest.command_contract import (
    IDENTIFIER as COMMAND_IDENTIFIER,
)
from forest.command_contract import (
    normalize_command_name,
    validate_argument_mode,
    validate_command_permission,
    validate_command_priority,
)
from forest.errors import ForestError
from forest.plugin_declarations import synthesize_manifest, verify_checksums, with_checksums
from forest.schema_bundles import (
    MAX_STORAGE_BASELINE_BYTES,
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)
from forest.security import private_directory

SKIPPED = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"}
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_WHEEL_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_FILES = 512
MAX_PACKAGE_ENTRIES = 4096
MAX_PACKAGE_DEPTH = 32
MAX_MANAGEMENT_UI_BYTES = 256 * 1024
IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
SQL_FUNCTION = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
ENTRYPOINT = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*:[a-zA-Z_]\w*\Z")
VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?\Z")


def _valid_json_pointer(value: object) -> bool:
    if not isinstance(value, str) or not 2 <= len(value.encode("utf-8")) <= 1024:
        return False
    if not value.startswith("/"):
        return False
    for token in value[1:].split("/"):
        if not token or re.search(r"~(?![01])", token):
            return False
    return True


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
                    "message_handler_takeover": {
                        "type": "string",
                        "enum": ["none", "compatible"],
                    },
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
                    "effect_services": {
                        "type": "array",
                        "maxItems": 16,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "contract",
                                "kind",
                                "semantics",
                                "input_schema",
                                "output_schema",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "name",
                                        "contract",
                                        "kind",
                                        "input_schema",
                                        "output_schema",
                                    )
                                },
                                "semantics": {
                                    "enum": ["at-most-once", "idempotent", "read"]
                                },
                            },
                        },
                    },
                    **{
                        kind: {
                            "type": "array",
                            "maxItems": 64,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "name",
                                    "contract",
                                    "function",
                                    "input_schema",
                                    "output_schema",
                                ],
                                "properties": {
                                    **{
                                        key: {"type": "string"}
                                        for key in (
                                            "name",
                                            "contract",
                                            "function",
                                            "input_schema",
                                            "output_schema",
                                        )
                                    },
                                    "payload_refs": {
                                        "type": "array",
                                        "maxItems": 32,
                                        "items": {
                                            "type": "object",
                                            "additionalProperties": False,
                                            "required": [
                                                "path",
                                                "owner_component",
                                                "owner_kind",
                                                "owner_id_path",
                                            ],
                                            "properties": {
                                                "path": {"type": "string"},
                                                "owner_component": {"type": "string"},
                                                "owner_kind": {"type": "string"},
                                                "owner_id_path": {"type": "string"},
                                            },
                                        },
                                    },
                                },
                            },
                        }
                        for kind in (
                            "domain_queries",
                            "domain_commands",
                            "unit_of_work_participants",
                        )
                    },
                    "job_types": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "job_type",
                                "job_version",
                                "name",
                                "contract",
                                "handler",
                                "input_schema",
                                "output_schema",
                                "max_attempts",
                                "timeout_s",
                                "max_concurrency",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "job_type",
                                        "name",
                                        "contract",
                                        "handler",
                                        "input_schema",
                                        "output_schema",
                                    )
                                },
                                "job_version": {
                                    "type": "integer", "minimum": 1, "maximum": 2147483647
                                },
                                "max_attempts": {
                                    "type": "integer", "minimum": 1, "maximum": 32
                                },
                                "timeout_s": {
                                    "type": "integer", "minimum": 1, "maximum": 86400
                                },
                                "max_concurrency": {
                                    "type": "integer", "minimum": 1, "maximum": 128
                                },
                            },
                        },
                    },
                    "owned_work_handlers": {
                        "type": "array",
                        "maxItems": 32,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "name",
                                "contract",
                                "handler",
                                "claim_function",
                                "settle_function",
                                "input_schema",
                                "output_schema",
                                "max_concurrency",
                                "poll_interval_ms",
                                "claim_timeout_s",
                                "handler_timeout_s",
                                "settle_timeout_s",
                                "lease_seconds",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "name",
                                        "contract",
                                        "handler",
                                        "claim_function",
                                        "settle_function",
                                        "input_schema",
                                        "output_schema",
                                    )
                                },
                                "max_concurrency": {
                                    "type": "integer", "minimum": 1, "maximum": 32
                                },
                                "poll_interval_ms": {
                                    "type": "integer", "minimum": 50, "maximum": 60000
                                },
                                "claim_timeout_s": {
                                    "type": "integer", "minimum": 1, "maximum": 60
                                },
                                "handler_timeout_s": {
                                    "type": "integer", "minimum": 1, "maximum": 86400
                                },
                                "settle_timeout_s": {
                                    "type": "integer", "minimum": 1, "maximum": 60
                                },
                                "lease_seconds": {
                                    "type": "integer", "minimum": 3, "maximum": 86400
                                },
                            },
                        },
                    },
                    "management_ui": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["entrypoint", "protocol"],
                        "properties": {
                            "entrypoint": {"type": "string"},
                            "protocol": {"const": "forest.management-ui/1"},
                            "label": {
                                "type": "string", "minLength": 1, "maxLength": 128
                            },
                            "group": {
                                "type": "string",
                                "pattern": "^[a-z0-9][a-z0-9._-]{0,63}$",
                            },
                            "order": {
                                "type": "integer", "minimum": -10000, "maximum": 10000
                            },
                        },
                    },
                    "storage": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "schema_id",
                            "schema_version",
                            "baseline",
                            "baseline_sha256",
                            "scope",
                            "repository_contract",
                        ],
                        "properties": {
                            "schema_id": {"type": "string"},
                            "schema_version": {"type": "string"},
                            "baseline": {"type": "string"},
                            "baseline_sha256": {"type": "string"},
                            "scope": {"const": "bot"},
                            "repository_contract": {"type": "string"},
                            "execution_profile": {
                                "enum": ["declarative", "official"]
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
                    "commands": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "command_id",
                                "name",
                                "handler",
                                "permission",
                                "argument_mode",
                                "input_schema",
                                "priority",
                            ],
                            "properties": {
                                **{
                                    key: {"type": "string"}
                                    for key in (
                                        "command_id",
                                        "name",
                                        "handler",
                                        "permission",
                                        "argument_mode",
                                        "input_schema",
                                    )
                                },
                                "priority": {
                                    "type": "integer", "minimum": -100, "maximum": 100
                                },
                            },
                        },
                    },
                    "message_handlers": {
                        "type": "array",
                        "maxItems": 64,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["handler_id", "handler", "priority"],
                            "properties": {
                                "handler_id": {"type": "string"},
                                "handler": {"type": "string"},
                                "priority": {
                                    "type": "integer", "minimum": -100, "maximum": 100
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


def _validated_source(source: Path) -> tuple[Path, dict[str, bytes], dict[str, bytes], dict]:
    try:
        _reject_link(source.lstat())
        source = source.resolve(strict=True)
    except OSError as exc:
        raise ForestError("INVALID_PACKAGE_PATH", "Package source is unavailable", 422) from exc
    source_files = _capture(source)
    captured = _prepared_source(source_files)
    manifest = _manifest(captured)
    if captured != _prepared_source(_capture(source)):
        _fail("PACKAGE_CHANGED", "Package must remain stable throughout capture")
    return source, source_files, captured, manifest


def validate_source_package(source: Path) -> dict:
    """Validate a stable source capture without publishing an artifact catalog."""

    _, _, captured, manifest = _validated_source(source)
    return {
        "manifest": manifest,
        "digest": digest_json(
            {name: hashlib.sha256(value).hexdigest() for name, value in captured.items()}
        ),
    }


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


def _management_ui(files: dict[str, bytes], component: dict) -> dict | None:
    declaration = component.get("management_ui")
    if declaration is None:
        return None
    reference = declaration["entrypoint"]
    path = PurePosixPath(reference)
    if (
        not isinstance(reference, str)
        or not reference.startswith("ui/")
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix.lower() != ".html"
        or reference not in files
    ):
        _fail("INVALID_MANAGEMENT_UI", "Management UI must be a packaged ui/*.html file")
    content = files[reference]
    if not content or len(content) > MAX_MANAGEMENT_UI_BYTES:
        _fail("INVALID_MANAGEMENT_UI", "Management UI exceeds its byte limit")
    try:
        html = content.decode("utf-8")
    except UnicodeError as exc:
        raise ForestError("INVALID_MANAGEMENT_UI", "Management UI must be UTF-8 HTML", 422) from exc
    if "\x00" in html or not re.search(r"<!doctype\s+html|<html(?:\s|>)", html, re.IGNORECASE):
        _fail("INVALID_MANAGEMENT_UI", "Management UI must be a complete HTML document")
    return {
        "entrypoint": reference,
        "protocol": declaration["protocol"],
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }


def _storage_bundle(files: dict[str, bytes], component: dict) -> dict | None:
    declaration = component.get("storage")
    if declaration is None:
        return None
    schema_id = declaration["schema_id"]
    reference = declaration["baseline"]
    path = PurePosixPath(reference)
    if (
        not isinstance(reference, str)
        or not reference.startswith("schema/")
        or path.is_absolute()
        or ".." in path.parts
        or path.suffix.lower() != ".sql"
        or reference not in files
    ):
        _fail(
            "INVALID_STORAGE_BUNDLE",
            "Storage baseline must be a packaged schema/*.sql file",
        )
    content = files[reference]
    if not content or len(content) > MAX_STORAGE_BASELINE_BYTES:
        _fail("INVALID_STORAGE_BUNDLE", "Storage baseline exceeds its byte limit")
    spec = StorageBundleSpec(
        component["id"],
        schema_id,
        declaration["schema_version"],
        declaration["baseline_sha256"],
        declaration["repository_contract"],
    )
    execution_profile = declaration.get("execution_profile", "declarative")
    if execution_profile == "official":
        validate_official_storage_baseline(content, spec.baseline_sha256)
    else:
        validate_storage_baseline(content, spec.baseline_sha256)
    return {
        "schema_id": spec.schema_id,
        "schema_version": spec.schema_version,
        "namespace": spec.namespace,
        "baseline": reference,
        "baseline_sha256": spec.baseline_sha256,
        "scope": declaration["scope"],
        "repository_contract": spec.repository_contract,
        "execution_profile": execution_profile,
        "bytes": len(content),
    }


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
    _management_ui(files, component)
    _storage_bundle(files, component)
    services = component.get("provides", [])
    effect_services = component.get("effect_services", [])
    domain_queries = component.get("domain_queries", [])
    domain_commands = component.get("domain_commands", [])
    unit_of_work_participants = component.get("unit_of_work_participants", [])
    job_types = component.get("job_types", [])
    owned_work_handlers = component.get("owned_work_handlers", [])
    commands = component.get("commands", [])
    message_handlers = component.get("message_handlers", [])
    if component.get("message_handler_takeover", "none") == "compatible" and not message_handlers:
        _fail(
            "INVALID_MESSAGE_HANDLERS",
            "Compatible Handler takeover requires at least one message handler",
        )
    subscriptions = component.get("subscribes", [])
    component_jobs = component.get("jobs", [])
    if len(services) > 32 or (
        not services
        and not effect_services
        and not job_types
        and not owned_work_handlers
        and not commands
        and not message_handlers
        and not subscriptions
        and not component_jobs
    ):
        _fail(
            "INVALID_SERVICES",
            "Declare at least one service, subscription, job, command, or message handler",
        )
    names = set()
    for service in (
        *services,
        *effect_services,
        *domain_queries,
        *domain_commands,
        *unit_of_work_participants,
        *job_types,
        *owned_work_handlers,
    ):
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
        if service in effect_services and not IDENTIFIER.fullmatch(
            service.get("kind", "")
        ):
            _fail("INVALID_SERVICES", "Effect services require a valid driver kind")
        if service in (
            *domain_queries,
            *domain_commands,
            *unit_of_work_participants,
        ) and not SQL_FUNCTION.fullmatch(service.get("function", "")):
            _fail(
                "INVALID_DOMAIN_AUTHORITY",
                "Domain authority functions must be portable unqualified SQL identifiers",
            )
        payload_refs = service.get("payload_refs", [])
        if payload_refs and service not in (*domain_commands, *unit_of_work_participants):
            _fail(
                "INVALID_DOMAIN_AUTHORITY",
                "Managed payload references are only valid on write ports",
            )
        for reference in payload_refs:
            if (
                not _valid_json_pointer(reference.get("path"))
                or not _valid_json_pointer(reference.get("owner_id_path"))
                or not IDENTIFIER.fullmatch(reference.get("owner_component", ""))
                or not IDENTIFIER.fullmatch(reference.get("owner_kind", ""))
            ):
                _fail(
                    "INVALID_DOMAIN_AUTHORITY",
                    "Managed payload reference declarations must use exact JSON pointers and owners",
                )
        if service in owned_work_handlers and (
            not SQL_FUNCTION.fullmatch(service.get("claim_function", ""))
            or not SQL_FUNCTION.fullmatch(service.get("settle_function", ""))
            or not IDENTIFIER.fullmatch(service.get("handler", ""))
            or service["lease_seconds"]
            <= service["handler_timeout_s"] + service["settle_timeout_s"]
        ):
            _fail(
                "INVALID_OWNED_WORK",
                "Owned work requires valid functions, handler, and a lease beyond execution",
            )
    if (
        domain_queries
        or domain_commands
        or unit_of_work_participants
        or owned_work_handlers
    ) and component.get("storage") is None:
        _fail("INVALID_DOMAIN_AUTHORITY", "Domain authority ports require package storage")
    functions = [
        item["function"]
        for item in (*domain_queries, *domain_commands, *unit_of_work_participants)
    ] + [
        function
        for item in owned_work_handlers
        for function in (item["claim_function"], item["settle_function"])
    ]
    if len(functions) != len(set(functions)):
        _fail("INVALID_DOMAIN_AUTHORITY", "Domain authority functions must be unique")
    contributed_job_types = set()
    contributed_job_handlers = set()
    for definition in job_types:
        job_type = definition.get("job_type", "")
        handler = definition.get("handler", "")
        if (
            not IDENTIFIER.fullmatch(job_type)
            or job_type in contributed_job_types
            or not IDENTIFIER.fullmatch(handler)
            or handler in contributed_job_handlers
        ):
            _fail("INVALID_JOB_TYPES", "Job type identities must be valid and unique")
        contributed_job_types.add(job_type)
        contributed_job_handlers.add(handler)
    for definition in owned_work_handlers:
        handler = definition["handler"]
        if handler in contributed_job_handlers:
            _fail("INVALID_OWNED_WORK", "Owned work handlers must be package-unique")
        contributed_job_handlers.add(handler)
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
        if (
            not IDENTIFIER.fullmatch(handler_id)
            or handler_id in job_ids
            or handler_id in contributed_job_handlers
        ):
            _fail("INVALID_JOBS", "Durable job handler identities must be valid and unique")
        job_ids.add(handler_id)
        _schema(files, definition.get("input_schema"))
    command_ids = set()
    command_names = set()
    command_handlers = set()
    for definition in commands:
        command_id = definition.get("command_id", "")
        name = definition.get("name", "")
        handler = definition.get("handler", "")
        try:
            normalized = normalize_command_name(name, require_normalized=True)
            validate_command_permission(definition.get("permission"))
            validate_argument_mode(definition.get("argument_mode"))
            validate_command_priority(definition.get("priority"))
        except ValueError as exc:
            _fail("INVALID_COMMANDS", str(exc))
        if (
            not COMMAND_IDENTIFIER.fullmatch(command_id)
            or command_id in command_ids
            or normalized in command_names
            or not isinstance(handler, str)
            or not handler.isidentifier()
            or keyword.iskeyword(handler)
            or handler in command_handlers
        ):
            _fail("INVALID_COMMANDS", "Command identities, names, and handlers must be unique")
        command_ids.add(command_id)
        command_names.add(normalized)
        command_handlers.add(handler)
        _schema(files, definition.get("input_schema"))
    message_handler_ids = set()
    message_handler_methods = set()
    for definition in message_handlers:
        handler_id = definition.get("handler_id", "")
        handler = definition.get("handler", "")
        priority = definition.get("priority")
        if (
            not IDENTIFIER.fullmatch(handler_id)
            or handler_id in message_handler_ids
            or not isinstance(handler, str)
            or not handler.isidentifier()
            or keyword.iskeyword(handler)
            or handler in message_handler_methods
            or type(priority) is not int
            or not -100 <= priority <= 100
        ):
            _fail(
                "INVALID_MESSAGE_HANDLERS",
                "Message handler identities, methods, and priorities must be valid and unique",
            )
        message_handler_ids.add(handler_id)
        message_handler_methods.add(handler)
    capabilities = component.get("capabilities", {}).get("requests", [])
    allowed_capabilities = {"state.read", "state.write"} | {
        "service.call:" + name
        for name in dependency_names | {item["name"] for item in effect_services}
    } | {
        "domain.query:" + item["name"] for item in domain_queries
    } | {
        "domain.command:" + item["name"] for item in domain_commands
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

    def management_ui(self) -> tuple[dict, bytes] | None:
        files = self._verified_files()
        metadata = _management_ui(files, self.component)
        if metadata is None:
            return None
        return metadata, files[metadata["entrypoint"]]

    def storage_bundle(self) -> tuple[dict, bytes] | None:
        files = self._verified_files()
        metadata = _storage_bundle(files, self.component)
        if metadata is None:
            return None
        return {**metadata, "package_digest": self.digest}, files[metadata["baseline"]]

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
        source, source_files, captured, _ = _validated_source(source)
        catalog_root = self.root.resolve()
        if catalog_root == source or catalog_root.is_relative_to(source):
            _fail("INVALID_PACKAGE_PATH", "Artifact catalog cannot be inside the source package")
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
