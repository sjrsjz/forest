"""Offline, immutable Worker environments made from locked binary wheels.

This is an installation/compatibility boundary for trusted native plugins, not
an OS sandbox. No resolver, index lookup, build backend, setup.py, site hook or
package import runs during admission or materialization.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import platform
import shutil
import sqlite3
import stat
import sys
import sysconfig
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path
from uuid import uuid4

from packaging.markers import default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.tags import parse_tag, sys_tags
from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename
from packaging.version import InvalidVersion, Version

from forest.errors import ForestError
from forest.packages import _capture, archive_files, digest_json
from forest.security import private_directory

SDK_API = "1.0"
SDK_FILES = (
    "__init__.py", "command_contract.py", "errors.py", "sdk.py", "drivers/__init__.py",
    "drivers/protocol.py", "drivers/python_worker.py", "drivers/worker.py",
    "model_contract.py",
)
MAX_ENVIRONMENT_BYTES = 256 * 1024 * 1024
MAX_ENVIRONMENT_FILES = 8192


def _fail(message: str, code: str = "INVALID_DEPENDENCY_LOCK") -> None:
    raise ForestError(code, message, 422)


def current_target() -> dict:
    """Exact admission target; publish a different lock for another deployment."""
    return {
        "implementation": sys.implementation.name,
        "python_version": platform.python_version(),
        "platform": sysconfig.get_platform(),
        "abi": sysconfig.get_config_var("SOABI") or sys.implementation.cache_tag,
        "sdk_api": SDK_API,
    }


def create_dependency_lock(source: Path, requirements: list[str]) -> dict:
    """Author tool: hash a complete wheelhouse without resolving/installing it.

    The caller publishes the returned JSON as dependencies.lock.json only after
    this validation succeeds. Source files and existing environments stay intact.
    """
    captured = _capture(source)
    entries = []
    for path, content in sorted(captured.items()):
        if not path.endswith(".whl"):
            continue
        try:
            name, version, _, _ = parse_wheel_filename(Path(path).name)
        except InvalidWheelFilename as exc:
            raise ForestError("INVALID_DEPENDENCY_LOCK", "Invalid vendored wheel filename", 422) from exc
        entries.append({
            "file": path, "name": name, "version": str(version),
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    lock = {
        "lock_version": 1, "target": current_target(),
        "requirements": list(requirements), "wheels": entries,
    }
    validate_lock({**captured, "dependencies.lock.json": json.dumps(lock).encode()},
                  "dependencies.lock.json")
    if _capture(source) != captured:
        _fail("Wheelhouse changed while creating its lock", "PACKAGE_CHANGED")
    return lock


@dataclass(frozen=True)
class LockedDependencies:
    lock: dict
    files: dict[str, bytes]


def _json(data: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate lock key")
            result[key] = value
        return result

    try:
        result = json.loads(data, object_pairs_hook=pairs)
        digest_json(result)
        return result
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ForestError("INVALID_DEPENDENCY_LOCK", "Lock must contain finite unique JSON", 422) from exc


def _requirement(value: str, *, root: bool = False) -> Requirement:
    try:
        requirement = Requirement(value)
    except (InvalidRequirement, TypeError) as exc:
        raise ForestError("INVALID_DEPENDENCY_LOCK", "Invalid dependency requirement", 422) from exc
    if requirement.url:
        _fail("URL dependencies are not supported; vendor a locked wheel")
    if root and (len(requirement.specifier) != 1 or any(
        item.operator != "==" or "*" in item.version for item in requirement.specifier
    )):
        _fail("Root requirements must pin an exact == version")
    return requirement


def _wheel(entry: dict, content: bytes) -> tuple[str, Version, list[Requirement], dict]:
    try:
        name, version, _, tags = parse_wheel_filename(Path(entry["file"]).name)
        if name != canonicalize_name(entry["name"]) or version != Version(entry["version"]):
            _fail("Wheel filename and lock identity differ")
        if not tags.intersection(set(sys_tags())):
            _fail("Wheel tags do not support this Python/platform", "DEPENDENCY_TARGET_MISMATCH")
    except (InvalidWheelFilename, InvalidVersion) as exc:
        raise ForestError("INVALID_DEPENDENCY_LOCK", "Invalid wheel identity", 422) from exc
    files = archive_files(
        content, max_files=MAX_ENVIRONMENT_FILES, max_bytes=MAX_ENVIRONMENT_BYTES,
        max_file_bytes=64 * 1024 * 1024, allow_wheels=False,
    )
    infos = {path.split("/", 1)[0] for path in files if path.split("/", 1)[0].endswith(".dist-info")}
    if len(infos) != 1:
        _fail("A wheel must contain exactly one distribution metadata directory")
    info = next(iter(infos))
    try:
        info_name, info_version = info.removesuffix(".dist-info").rsplit("-", 1)
        metadata = BytesParser().parsebytes(files[f"{info}/METADATA"])
        wheel = BytesParser().parsebytes(files[f"{info}/WHEEL"])
        if (len(metadata.get_all("Name", [])) != 1 or len(metadata.get_all("Version", [])) != 1
                or canonicalize_name(metadata["Name"]) != name
                or Version(metadata["Version"]) != version
                or canonicalize_name(info_name) != name or Version(info_version) != version
                or wheel.get("Wheel-Version") != "1.0"
                or wheel.get("Root-Is-Purelib") not in {"true", "false"}):
            _fail("Wheel metadata does not match its locked identity/format")
        metadata_tags = set().union(*(parse_tag(value) for value in wheel.get_all("Tag", [])))
        if metadata_tags != tags:
            _fail("Wheel metadata tags differ from the locked artifact filename")
        requires_python = metadata.get("Requires-Python")
        if requires_python and Version(platform.python_version()) not in SpecifierSet(requires_python):
            _fail("Wheel Requires-Python excludes this interpreter", "DEPENDENCY_TARGET_MISMATCH")
        requirements = [_requirement(value) for value in metadata.get_all("Requires-Dist", [])]
        records = list(csv.reader(io.StringIO(files[f"{info}/RECORD"].decode("utf-8"))))
    except (KeyError, ValueError, TypeError, UnicodeError, csv.Error, InvalidSpecifier) as exc:
        raise ForestError("INVALID_DEPENDENCY_LOCK", "Malformed wheel metadata", 422) from exc
    seen = set()
    for row in records:
        if len(row) != 3 or row[0] not in files or row[0] in seen:
            _fail("Wheel RECORD has missing/duplicate/invalid entries")
        path, checksum, size = row
        seen.add(path)
        if path == f"{info}/RECORD" and checksum == size == "":
            continue
        value = files[path]
        try:
            algorithm, encoded = checksum.split("=", 1)
            if algorithm not in {"sha256", "sha384", "sha512"}:
                _fail("Wheel RECORD requires SHA-256 or stronger hashes")
            actual = base64.urlsafe_b64encode(hashlib.new(algorithm, value).digest()).rstrip(b"=")
            if actual.decode() != encoded or str(len(value)) != size:
                _fail("Wheel RECORD checksum/size mismatch", "DEPENDENCY_TAMPERED")
        except ValueError as exc:
            raise ForestError("INVALID_DEPENDENCY_LOCK", "Malformed wheel RECORD hash", 422) from exc
    if seen != set(files):
        _fail("Wheel RECORD must cover every installed file")
    installed = {}
    for path, value in files.items():
        parts = path.split("/")
        if parts[0].endswith(".data"):
            if len(parts) < 3 or parts[1] not in {"purelib", "platlib"}:
                _fail("Wheel scripts/headers/external data schemes are not supported")
            path = "/".join(parts[2:])
            parts = path.split("/")
        top = parts[0].split(".", 1)[0]
        if top == "forest" or top in sys.stdlib_module_names or path.endswith((".pth", ".pyc")):
            _fail("Wheel shadows the SDK/stdlib or installs unsupported startup code")
        if path in installed:
            _fail("Wheel installation paths collide")
        installed[path] = value
    return name, version, requirements, installed


def validate_lock(files: dict[str, bytes], reference: str) -> LockedDependencies:
    if not isinstance(reference, str) or reference not in files or not reference.endswith(".json"):
        _fail("dependency_lock must name a captured JSON file")
    lock = _json(files[reference])
    if (not isinstance(lock, dict) or set(lock) != {"lock_version", "target", "requirements", "wheels"}
            or type(lock["lock_version"]) is not int or lock["lock_version"] != 1
            or not isinstance(lock["requirements"], list) or len(lock["requirements"]) > 128
            or not isinstance(lock["wheels"], list) or len(lock["wheels"]) > 128):
        _fail("Unsupported dependency lock schema")
    if lock["target"] != current_target():
        _fail("Dependency lock Python/platform/SDK differs from deployment", "DEPENDENCY_TARGET_MISMATCH")
    wheels = {}
    installed = {}
    paths = set()
    installation_paths = {}
    total = 0
    for entry in lock["wheels"]:
        if (not isinstance(entry, dict) or set(entry) != {"file", "name", "version", "sha256"}
                or any(not isinstance(value, str) for value in entry.values())
                or entry["file"] not in files or not entry["file"].startswith("wheelhouse/")
                or entry["file"] in paths):
            _fail("Each locked wheel requires a unique vendored file and exact identity/hash")
        paths.add(entry["file"])
        content = files[entry["file"]]
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            _fail("Vendored wheel differs from locked SHA-256", "DEPENDENCY_TAMPERED")
        name, version, requirements, wheel_files = _wheel(entry, content)
        if name in wheels:
            _fail("Duplicate locked distribution")
        wheels[name] = (version, requirements)
        for path, value in wheel_files.items():
            key = path.casefold()
            if key in installed:
                _fail("Locked wheels have colliding installation paths")
            installed[key] = (path, value)
            parts = path.split("/")
            for index in range(1, len(parts) + 1):
                prefix = "/".join(parts[:index])
                description = (prefix, index != len(parts))
                previous = installation_paths.setdefault(prefix.casefold(), description)
                if previous != description:
                    _fail("Locked wheels have ambiguous file/directory installation paths")
            total += len(value)
        if len(installed) > MAX_ENVIRONMENT_FILES or total > MAX_ENVIRONMENT_BYTES:
            _fail("Expanded dependency environment exceeds limits")
    if {path for path in files if path.endswith(".whl")} != paths:
        _fail("Every vendored wheel must be named in the dependency lock")
    pending = [(_requirement(value, root=True), {""}) for value in lock["requirements"]]
    included: dict[str, set[str]] = {}
    marker_environment = default_environment()
    while pending:
        requirement, source_extras = pending.pop()
        if requirement.marker and not any(requirement.marker.evaluate(
            {**marker_environment, "extra": extra}
        ) for extra in source_extras):
            continue
        name = canonicalize_name(requirement.name)
        if name not in wheels or wheels[name][0] not in requirement.specifier:
            _fail("Lock is missing a compatible transitive dependency")
        extras = {""} | set(requirement.extras)
        previous = included.get(name, set())
        if extras.issubset(previous):
            continue
        included[name] = previous | extras
        pending.extend((child, included[name]) for child in wheels[name][1])
    if set(included) != set(wheels):
        _fail("Lock contains wheels outside the exact dependency closure")
    return LockedDependencies(lock, dict(installed.values()))


@dataclass(frozen=True)
class EnvironmentSnapshot:
    root: Path
    digest: str
    descriptor: dict

    @property
    def python_executable(self) -> str:
        return self.descriptor["interpreter"]

    @property
    def search_paths(self) -> list[str]:
        return [str(self.root / "sdk"), str(self.root / "lib")]

    def verify(self) -> None:
        try:
            root_info = self.root.lstat()
            if stat.S_ISLNK(root_info.st_mode) or getattr(root_info, "st_reparse_tag", 0):
                _fail("Environment root is a link", "ENVIRONMENT_TAMPERED")
            descriptor = json.loads((self.root / "environment.json").read_bytes())
            actual = {}
            def walk_error(error):
                raise error
            for directory, subdirs, names in os.walk(self.root, onerror=walk_error):
                for name in subdirs + names:
                    path = Path(directory) / name
                    info = path.lstat()
                    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0):
                        _fail("Environment contains a link", "ENVIRONMENT_TAMPERED")
                    if path.is_file() and path.relative_to(self.root).as_posix() != "environment.json":
                        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                            _fail("Environment contains a nonordinary file", "ENVIRONMENT_TAMPERED")
                        actual[path.relative_to(self.root).as_posix()] = hashlib.sha256(
                            path.read_bytes()
                        ).hexdigest()
            if (descriptor != self.descriptor or digest_json(descriptor) != self.digest
                    or actual != descriptor["files"] or descriptor["target"] != current_target()
                    or hashlib.sha256(Path(self.python_executable).read_bytes()).hexdigest()
                    != descriptor["interpreter_sha256"]):
                _fail("Immutable environment checksum/target changed", "ENVIRONMENT_TAMPERED")
        except (OSError, ValueError, KeyError) as exc:
            raise ForestError("ENVIRONMENT_TAMPERED", "Environment is unavailable/invalid", 422) from exc


class EnvironmentCatalog:
    def __init__(self, root: Path):
        self.root = private_directory(root)

    def materialize(self, snapshot) -> EnvironmentSnapshot:
        captured = snapshot._verified_files() if hasattr(snapshot, "_verified_files") else {}
        lock_file = snapshot.manifest.get("dependency_lock")
        dependencies = validate_lock(captured, lock_file) if lock_file else None
        sdk_root = Path(__file__).parent
        sdk = {"sdk/forest/" + name: (sdk_root / name).read_bytes() for name in SDK_FILES}
        # A source checkout update racing this capture must not publish a mixed SDK.
        if any((sdk_root / name).read_bytes() != sdk["sdk/forest/" + name] for name in SDK_FILES):
            _fail("SDK changed during environment capture", "ENVIRONMENT_CHANGED")
        content = {**sdk, **{"lib/" + name: value for name, value in (
            dependencies.files.items() if dependencies else []
        )}}
        executable = str(Path(getattr(sys, "_base_executable", sys.executable)).resolve())
        descriptor = {
            "environment_version": 1, "target": current_target(), "interpreter": executable,
            "interpreter_sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(),
            "lock_digest": digest_json(dependencies.lock) if dependencies else None,
            "files": {name: hashlib.sha256(value).hexdigest() for name, value in content.items()},
        }
        digest = digest_json(descriptor)
        destination = self.root / digest.removeprefix("sha256:")
        with sqlite3.connect(self.root / "environments.sqlite3", timeout=30) as db:
            db.execute("BEGIN IMMEDIATE")
            if not destination.exists():
                staging = self.root / (".environment-" + uuid4().hex)
                staging.mkdir(mode=0o700)
                try:
                    (staging / "lib").mkdir()
                    for name, value in {**content, "environment.json": json.dumps(
                        descriptor, ensure_ascii=False, sort_keys=True, allow_nan=False
                    ).encode()}.items():
                        path = staging / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with path.open("xb") as stream:
                            stream.write(value)
                            stream.flush()
                            os.fsync(stream.fileno())
                        if os.name != "nt":
                            path.chmod(0o400)
                    staging.rename(destination)
                finally:
                    if staging.exists():
                        info = staging.lstat()
                        if (not stat.S_ISLNK(info.st_mode) and not getattr(info, "st_reparse_tag", 0)
                                and staging.resolve().parent == self.root.resolve()):
                            shutil.rmtree(staging)
            result = EnvironmentSnapshot(destination, digest, descriptor)
            result.verify()
            return result
