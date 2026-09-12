import io
import json
import os
import shutil
import stat
import subprocess
import sys
import zipfile
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


def test_catalog_restart_archive_roundtrip_and_development_versions(tmp_path):
    root = source(tmp_path)
    original = (root / "module.toml").read_bytes()
    catalog = PackageCatalog(tmp_path / "catalog")
    released = catalog.build(root)
    dev = catalog.build(root, development=True)
    assert dev.manifest["version"].startswith("0.1.0+dev.")
    assert catalog.build(root, development=True).digest == dev.digest
    assert (root / "module.toml").read_bytes() == original
    before = packages.source_fingerprint(root)
    (root / "template.txt").write_text("new helper input", encoding="utf-8")
    assert packages.source_fingerprint(root) != before
    other_dev = catalog.build(root, development=True)
    assert other_dev.digest != dev.digest
    restarted = PackageCatalog(catalog.root)
    assert {item.digest for item in restarted.list()} == {released.digest, dev.digest, other_dev.digest}
    assert restarted.find("example.counter", "0.1.0").digest == released.digest
    archive = restarted.export_archive(dev.digest)
    assert archive == restarted.export_archive(dev.digest)
    imported = PackageCatalog(tmp_path / "imported").import_archive(archive)
    assert imported.digest == dev.digest
    assert imported._verified_files() == dev._verified_files()


@pytest.mark.parametrize("names,code", [
    (["../escape"], "PACKAGE_FILE_REJECTED"),
    (["/absolute"], "PACKAGE_FILE_REJECTED"),
    (["C:/absolute"], "PACKAGE_FILE_REJECTED"),
    (["folder\\escape"], "PACKAGE_FILE_REJECTED"),
    (["a//b"], "PACKAGE_FILE_REJECTED"),
    (["a/./b"], "PACKAGE_FILE_REJECTED"),
    ([".env.production"], "PACKAGE_FILE_REJECTED"),
    (["LPT1.txt"], "PACKAGE_FILE_REJECTED"),
    (["same", "same"], "PACKAGE_PATH_COLLISION"),
    (["same", "SAME"], "PACKAGE_PATH_COLLISION"),
    (["A/one", "a/two"], "PACKAGE_PATH_COLLISION"),
    (["a/b", "a"], "PACKAGE_PATH_COLLISION"),
    (["a", "a/b"], "PACKAGE_PATH_COLLISION"),
])
def test_archive_path_attacks_rejected_before_publication(tmp_path, names, code):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name in names:
            archive.writestr(name, b"data")
    data = output.getvalue()
    if names == ["folder\\escape"]:
        # ZipInfo normalizes the Windows separator while writing; construct
        # actual nonportable ZIP header bytes, as an upload from Unix could.
        data = data.replace(b"folder/escape", b"folder\\escape")
    catalog = PackageCatalog(tmp_path / "catalog")
    with pytest.raises(ForestError) as caught:
        catalog.import_archive(data)
    assert caught.value.code == code
    assert not (catalog.root / "catalog.sqlite3").exists()


def test_archive_links_bombs_truncation_and_version_conflicts(tmp_path, monkeypatch):
    catalog = PackageCatalog(tmp_path / "catalog")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        link = zipfile.ZipInfo("link")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, "outside")
    with pytest.raises(ForestError, match="links"):
        catalog.import_archive(output.getvalue())
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("bomb.txt", b"0" * (packages.MAX_FILE_BYTES + 1))
    with pytest.raises(ForestError) as caught:
        catalog.import_archive(output.getvalue())
    assert caught.value.code == "PACKAGE_TOO_LARGE"
    with pytest.raises(ForestError) as caught:
        catalog.import_archive(output.getvalue()[:-20])
    assert caught.value.code == "INVALID_PACKAGE_ARCHIVE"
    snapshot = catalog.build(source(tmp_path))
    files = snapshot._verified_files() | {"new.txt": b"different release"}
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, value in files.items():
            archive.writestr(name, value)
    with pytest.raises(ForestError) as caught:
        catalog.import_archive(output.getvalue())
    assert caught.value.code == "PACKAGE_VERSION_CONFLICT"
    assert catalog.get(snapshot.digest).digest == snapshot.digest


def test_state_migration_manifest_contract_is_static_and_explicit(tmp_path):
    root = source(tmp_path)
    path = root / "module.toml"
    original = path.read_text()
    path.write_text(original.replace("state_schema = 1", (
        'state_schema = 2\nstate_value_schema = "schemas/state.json"\n'
        'state_migrations = [{from_schema=1,to_schema=2}]'
    )))
    (root / "schemas/state.json").write_text('{"type":"object"}')
    assert PackageCatalog(tmp_path / "catalog").build(root).component["state_schema"] == 2
    path.write_text(path.read_text().replace("to_schema=2", "to_schema=1"))
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "bad").build(root)
    assert caught.value.code == "INVALID_STATE_MIGRATION"


def test_import_policy_rejection_does_not_poison_immutable_version_registry(tmp_path):
    source_catalog = PackageCatalog(tmp_path / "source-catalog")
    snapshot = source_catalog.build(source(tmp_path))
    archive = source_catalog.export_archive(snapshot.digest)
    destination = PackageCatalog(tmp_path / "destination")

    def reject(manifest):
        assert manifest["package_id"] == "example.counter"
        raise ForestError("PACKAGE_RESERVED", "Reserved namespace", 403)

    with pytest.raises(ForestError) as caught:
        destination.import_archive(archive, check_manifest=reject)
    assert caught.value.code == "PACKAGE_RESERVED"
    assert destination.list() == []
    assert not (destination.root / "catalog.sqlite3").exists()
    assert destination.import_archive(archive).digest == snapshot.digest


def test_unbound_web_artifacts_do_not_reserve_version_after_rejected_registration(tmp_path):
    root = source(tmp_path)
    source_catalog = PackageCatalog(tmp_path / "source-catalog")
    first = source_catalog.build(root)
    destination = PackageCatalog(tmp_path / "destination")
    staged = destination.import_archive(source_catalog.export_archive(first.digest), bind_version=False)
    assert destination.list() == []
    assert PackageCatalog(destination.root).get(staged.digest).digest == staged.digest
    (root / "new.txt").write_text("different content for same version")
    # A rejected Web registration left bytes, but cannot reserve the release name.
    accepted = destination.build(root)
    assert accepted.digest != staged.digest
    assert destination.find("example.counter", "0.1.0").digest == accepted.digest
    # Content-addressed Web staging also does not collide with an existing CLI name.
    assert destination.import_archive(source_catalog.export_archive(first.digest),
                                      bind_version=False).digest == staged.digest
    assert destination.get(accepted.digest).digest == accepted.digest
    with pytest.raises(ForestError) as caught:
        destination.import_archive(source_catalog.export_archive(first.digest))
    assert caught.value.code == "PACKAGE_VERSION_CONFLICT"


def test_competing_processes_cannot_publish_different_content_for_one_release(tmp_path):
    first = source(tmp_path)
    second = tmp_path / "second"
    shutil.copytree(first, second)
    (second / "different.txt").write_text("competing release content")
    catalog_path = tmp_path / "catalog"
    script = """
import json,sys
from pathlib import Path
from forest.packages import PackageCatalog
from forest.errors import ForestError
try:
    result=PackageCatalog(Path(sys.argv[2])).build(Path(sys.argv[1]))
    print(json.dumps({'digest':result.digest}))
except ForestError as exc:
    print(json.dumps({'error':exc.code}))
"""
    processes = []
    try:
        for root in (first, second):
            processes.append(subprocess.Popen(
                [sys.executable, "-c", script, str(root), str(catalog_path)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            ))
        results = []
        for process in processes:
            output, error = process.communicate(timeout=20)
            assert process.returncode == 0, error
            results.append(json.loads(output))
        assert sum("digest" in result for result in results) == 1
        assert [result["error"] for result in results if "error" in result] == [
            "PACKAGE_VERSION_CONFLICT"]
        accepted = PackageCatalog(catalog_path).find("example.counter", "0.1.0")
        assert accepted.digest == next(result["digest"] for result in results if "digest" in result)
        assert len(PackageCatalog(catalog_path).list()) == 1
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
