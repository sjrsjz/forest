import json
import os
import shutil
from pathlib import Path

import pytest

from forest import packages
from forest.errors import ForestError
from forest.packages import PackageCatalog

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


def source(tmp_path):
    path = tmp_path / "source"
    shutil.copytree(EXAMPLE, path)
    return path


def test_build_never_imports_and_captures_complete_helpers(tmp_path):
    root = source(tmp_path)
    code = root / "python" / "counter.py"
    code.write_text("raise RuntimeError('must not run during static build')\n", encoding="utf-8")
    helper = root / "python" / "helper.py"
    helper.write_text("VALUE = 1\n", encoding="utf-8")
    catalog = PackageCatalog(tmp_path / "catalog")
    snapshot = catalog.build(root)
    assert snapshot.entrypoint == "counter:Counter"
    assert (snapshot.module_root / "helper.py").read_text() == "VALUE = 1\n"
    assert catalog.build(root).digest == snapshot.digest
    helper.write_text("VALUE = 2\n", encoding="utf-8")
    assert (snapshot.module_root / "helper.py").read_text() == "VALUE = 1\n"
    with pytest.raises(ForestError, match="version cannot silently"):
        catalog.build(root)


@pytest.mark.parametrize("change,code", [
    ("syntax", "PACKAGE_SYNTAX_ERROR"),
    ("schema", "INVALID_SCHEMA"),
    ("entrypoint", "ENTRYPOINT_MISSING"),
    ("private", "PACKAGE_FILE_REJECTED"),
    ("collision", "PACKAGE_PATH_COLLISION"),
])
def test_invalid_packages_fail_before_execution(tmp_path, change, code):
    root = source(tmp_path)
    if change == "syntax":
        (root / "python" / "broken.py").write_text("def broken(:\n")
    elif change == "schema":
        (root / "schemas" / "input.json").write_text('{"$ref":"https://example.invalid/s"}')
    elif change == "entrypoint":
        (root / "python" / "counter.py").unlink()
    elif change == "private":
        (root / ".env").write_text("PRIVATE=test-only")
    else:
        # Case collision is meaningful only on a case-sensitive filesystem.
        first = root / "python" / "helper.py"
        second = root / "python" / "HELPER.py"
        first.write_text("VALUE=1")
        second.write_text("VALUE=2")
        if first.samefile(second):
            pytest.skip("Case-insensitive filesystem")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == code


def test_tampered_snapshot_is_rejected(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(source(tmp_path))
    file = snapshot.module_root / "counter.py"
    file.chmod(0o600)
    file.write_text("# modified after build\n")
    with pytest.raises(ForestError) as caught:
        snapshot.verify()
    assert caught.value.code == "PACKAGE_TAMPERED"


@pytest.mark.parametrize("schema", [
    '{"type":42}',
    '{"minimum":"zero"}',
    '{"$dynamicRef":"file:///private/schema.json"}',
    '{"$id":"https://example.invalid/base","$ref":"#/missing"}',
    '{"const":NaN}',
    '{"const":Infinity}',
    '{"properties":' * 1200 + '{}' + '}' * 1200,
])
def test_invalid_schema_errors_are_normalized_without_resolving_references(tmp_path, schema):
    root = source(tmp_path)
    (root / "schemas" / "input.json").write_text(schema, encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "INVALID_SCHEMA"


@pytest.mark.parametrize("section,setting", [
    ("resources", "max_frame_bytes = 1023"),
    ("resources", "max_frame_bytes = 262145"),
    ("resources", "max_inflight = true"),
    ("resources", "max_inflight = 0"),
    ("resources", "max_state_inflight = 257"),
    ("resources", "max_owned_tasks = 1.5"),
    ("resources", "max_queue_items = -1"),
    ("resources", "max_queue_bytes = 4096"),
    ("resources", "memory_mb = 128"),
    ("lifecycle", "prepare_timeout_s = 0"),
    ("lifecycle", "start_timeout_s = 121"),
    ("lifecycle", "stop_timeout_s = nan"),
    ("lifecycle", "drain_timeout_s = '10'"),
    ("lifecycle", "timeout_s = 10"),
])
def test_unsupported_or_invalid_worker_constraints_are_not_silently_ignored(
        tmp_path, section, setting):
    root = source(tmp_path)
    manifest = root / "module.toml"
    with manifest.open("a", encoding="utf-8") as stream:
        stream.write(f"\n[components.{section}]\n{setting}\n")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "INVALID_MANIFEST"


def test_supported_resource_and_lifecycle_limits_survive_snapshot(tmp_path):
    root = source(tmp_path)
    manifest = root / "module.toml"
    with manifest.open("a", encoding="utf-8") as stream:
        stream.write("\n[components.resources]\nmax_frame_bytes = 1024\nmax_queue_items = 0\n"
                     "max_inflight = 2\nmax_state_inflight = 3\nmax_owned_tasks = 4\n"
                     "[components.lifecycle]\nprepare_timeout_s = 0.5\nstart_timeout_s = 120\n"
                     "drain_timeout_s = 10\nstop_timeout_s = 1\n")
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    assert snapshot.component["resources"]["max_queue_items"] == 0
    assert snapshot.component["lifecycle"]["prepare_timeout_s"] == 0.5


@pytest.mark.parametrize("extra", ['unknown = "ignored"', '[metadata]\nvalue = "unknown"'])
def test_unknown_manifest_properties_are_rejected(tmp_path, extra):
    root = source(tmp_path)
    manifest = root / "module.toml"
    text = manifest.read_text(encoding="utf-8")
    if extra.startswith("["):
        text += "\n" + extra
    else:
        text = extra + "\n" + text
    manifest.write_text(text, encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "INVALID_MANIFEST"


def test_capture_detects_file_disappearance_between_stat_and_open(tmp_path, monkeypatch):
    root = source(tmp_path)
    target = root / "python" / "counter.py"
    original = os.open

    def disappear(path, flags, *args, **kwargs):
        if Path(path) == target:
            target.unlink()
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(packages.os, "open", disappear)
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "PACKAGE_CHANGED"


def test_capture_does_not_silently_skip_unreadable_directories(tmp_path, monkeypatch):
    root = source(tmp_path)
    original = os.scandir

    def unreadable(path):
        if Path(path) == root / "python":
            raise PermissionError("test-only inaccessible source")
        return original(path)

    monkeypatch.setattr(packages.os, "scandir", unreadable)
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "PACKAGE_CHANGED"


def test_source_changing_between_captures_is_not_published(tmp_path, monkeypatch):
    root = source(tmp_path)
    original = packages._capture
    calls = 0

    def mutate_after_read(path):
        nonlocal calls
        result = original(path)
        calls += 1
        if calls == 1:
            (root / "python" / "late.py").write_text("VALUE = 2\n", encoding="utf-8")
        return result

    monkeypatch.setattr(packages, "_capture", mutate_after_read)
    catalog = PackageCatalog(tmp_path / "catalog")
    with pytest.raises(ForestError) as caught:
        catalog.build(root)
    assert caught.value.code == "PACKAGE_CHANGED"
    assert not list(catalog.root.glob(".capture-*"))
    assert not (catalog.root / "catalog.sqlite3").exists()


@pytest.mark.parametrize("limit", [
    "MAX_FILE_BYTES", "MAX_PACKAGE_BYTES", "MAX_PACKAGE_FILES",
    "MAX_PACKAGE_ENTRIES", "MAX_PACKAGE_DEPTH",
])
def test_package_resource_limits_are_enforced(tmp_path, monkeypatch, limit):
    root = source(tmp_path)
    files = [path for path in root.rglob("*") if path.is_file()]
    maximum = {
        "MAX_FILE_BYTES": max(path.stat().st_size for path in files) - 1,
        "MAX_PACKAGE_BYTES": sum(path.stat().st_size for path in files) - 1,
        "MAX_PACKAGE_FILES": len(files) - 1,
        "MAX_PACKAGE_ENTRIES": 1,
        "MAX_PACKAGE_DEPTH": 1,
    }[limit]
    monkeypatch.setattr(packages, limit, maximum)
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "PACKAGE_TOO_LARGE"


@pytest.mark.parametrize("directory", [False, True])
def test_links_cannot_import_outside_package_content(tmp_path, directory):
    root = source(tmp_path)
    outside = tmp_path / "outside"
    if directory:
        outside.mkdir()
        (outside / "private.py").write_text("VALUE = 42\n", encoding="utf-8")
    else:
        outside.write_text("VALUE = 42\n", encoding="utf-8")
    try:
        (root / "linked").symlink_to(outside, target_is_directory=directory)
    except OSError:
        pytest.skip("Creating symlinks is unavailable for this account")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "PACKAGE_LINK_REJECTED"


def test_reparse_source_is_rejected_before_directory_resolution(tmp_path, monkeypatch):
    root = source(tmp_path)
    original = Path.lstat

    def reparse(path):
        info = original(path)
        if path == root:
            class Reparse:
                st_mode = info.st_mode
                st_reparse_tag = 0xA0000003  # Windows mount-point/junction tag.
            return Reparse()
        return info

    monkeypatch.setattr(Path, "lstat", reparse)
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "PACKAGE_LINK_REJECTED"


def test_failed_artifact_write_only_cleans_its_own_capture(tmp_path, monkeypatch):
    root = source(tmp_path)
    catalog = PackageCatalog(tmp_path / "catalog")
    unrelated = catalog.root / ".capture-unrelated"
    unrelated.mkdir()
    (unrelated / "keep.json").write_text(json.dumps({"keep": True}), encoding="utf-8")

    def disk_failure(_fd):
        raise OSError("test-only fsync failure")

    monkeypatch.setattr(packages.os, "fsync", disk_failure)
    with pytest.raises(ForestError) as caught:
        catalog.build(root)
    assert caught.value.code == "PACKAGE_STORAGE_ERROR"
    assert list(catalog.root.glob(".capture-*")) == [unrelated]
    assert (unrelated / "keep.json").is_file()


def test_missing_source_returns_a_domain_error(tmp_path):
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(tmp_path / "missing")
    assert caught.value.code == "INVALID_PACKAGE_PATH"


def test_schema_read_rejects_changes_to_verified_artifact(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(source(tmp_path))
    schema = snapshot.root / "schemas" / "input.json"
    schema.chmod(0o600)
    schema.write_text('{"type":"string"}', encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        snapshot.schema("schemas/input.json")
    assert caught.value.code == "PACKAGE_TAMPERED"


def test_local_schema_fragments_remain_supported(tmp_path):
    root = source(tmp_path)
    (root / "schemas" / "input.json").write_text(json.dumps({
        "$defs": {"counter": {"type": "integer"}},
        "type": "object", "properties": {"amount": {"$ref": "#/$defs/counter"}},
    }), encoding="utf-8")
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    assert snapshot.schema("schemas/input.json")["properties"]["amount"]["$ref"] == (
        "#/$defs/counter"
    )
