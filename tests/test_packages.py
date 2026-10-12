import hashlib
import io
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import zipfile
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from forest import packages
from forest.builtin_packages import bootstrap_installed_package
from forest.cli import run_plugin_command
from forest.component_selection import (
    ComponentSelectionEntry,
    ComponentSelectionSnapshot,
    resolve_official_package_snapshots,
)
from forest.errors import ForestError
from forest.kernel_package_catalog_snapshot import (
    package_catalog_artifact_service,
    package_catalog_page_service,
)
from forest.packages import PackageCatalog, auto_install_official_sources
from forest.schema_bundles import StorageBundleSpec, module_schema_namespace

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


def test_official_auto_install_catalogs_are_scope_separated(tmp_path):
    bot_sources = auto_install_official_sources("bot")
    deployment_sources = auto_install_official_sources("deployment")
    host_sources = auto_install_official_sources("host")
    assert bot_sources and deployment_sources
    assert set(bot_sources).isdisjoint(deployment_sources)
    assert set(host_sources).isdisjoint((*bot_sources, *deployment_sources))
    catalog = PackageCatalog(tmp_path / "official-auto-install")
    assert all(catalog.build(source).component["scope"] == "bot" for source in bot_sources)
    assert all(
        catalog.build(source).component["scope"] == "deployment"
        for source in deployment_sources
    )


@pytest.mark.asyncio
async def test_installed_package_requires_exact_capability_approval(tmp_path):
    catalog = PackageCatalog(tmp_path / "catalog")
    snapshot = catalog.build(EXAMPLE)
    with pytest.raises(ForestError) as rejected:
        await bootstrap_installed_package(
            snapshot.digest,
            catalog,
            None,
            None,
            kernel=None,
            initial_configuration={},
            approved_capabilities=(),
        )
    assert rejected.value.code == "PACKAGE_CAPABILITY_APPROVAL_REQUIRED"


@pytest.mark.asyncio
async def test_installed_package_rejects_official_sql_before_database_access(tmp_path):
    catalog = PackageCatalog(tmp_path / "catalog")
    source_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/counter"
    snapshot = catalog.build(source_root)
    requested = tuple(snapshot.component["capabilities"]["requests"])
    with pytest.raises(ForestError) as rejected:
        await bootstrap_installed_package(
            snapshot.digest,
            catalog,
            None,
            None,
            kernel=None,
            initial_configuration={"label": "untrusted", "multiplier": 1},
            approved_capabilities=requested,
        )
    assert rejected.value.code == "PACKAGE_STORAGE_TRUST_REQUIRED"


@pytest.mark.asyncio
async def test_installed_package_cannot_claim_official_bot_grants(tmp_path):
    catalog = PackageCatalog(tmp_path / "catalog")
    source_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/job_scheduler"
    snapshot = catalog.build(source_root)
    requested = tuple(snapshot.component["capabilities"]["requests"])
    with pytest.raises(ForestError) as rejected:
        await bootstrap_installed_package(
            snapshot.digest,
            catalog,
            None,
            None,
            kernel=None,
            initial_configuration={},
            approved_capabilities=requested,
        )
    assert rejected.value.code == "PACKAGE_OFFICIAL_GRANTS_FORBIDDEN"


def source(tmp_path):
    path = tmp_path / "source"
    shutil.copytree(EXAMPLE, path)
    return path


def test_deployment_official_provenance_is_not_a_manifest_claim(tmp_path):
    invalid = PackageCatalog(tmp_path / "invalid-official")
    with pytest.raises(ForestError) as wrong_scope:
        invalid.build_official(EXAMPLE)
    assert wrong_scope.value.code == "OFFICIAL_PACKAGE_SOURCE_INVALID"
    assert invalid.list() == []

    source_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/deployment_audit"
    catalog = PackageCatalog(tmp_path / "official")
    artifact = catalog.build(source_root)
    imported = PackageCatalog(tmp_path / "imported")
    imported.import_archive(catalog.export_archive(artifact.digest))

    for untrusted in (catalog, imported):
        with pytest.raises(ForestError) as rejected:
            untrusted.get_official_deployment(artifact.digest)
        assert rejected.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"

    assert catalog.build_official(source_root).digest == artifact.digest
    assert PackageCatalog(tmp_path / "official").get_official_deployment(artifact.digest).digest == artifact.digest


def test_host_official_provenance_is_scope_fenced(tmp_path):
    root = tmp_path / "host-source"
    shutil.copytree(
        Path(__file__).resolve().parents[1] / "src/forest/builtin/deployment_audit", root,
    )
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'scope = "deployment"', 'scope = "host"'
        ), encoding="utf-8",
    )
    catalog = PackageCatalog(tmp_path / "host-catalog")
    artifact = catalog.build(root)
    assert artifact.component["scope"] == "host"
    assert not catalog.is_official_host(artifact.digest)
    with pytest.raises(ForestError) as untrusted:
        catalog.get_official_host(artifact.digest)
    assert untrusted.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"
    with pytest.raises(ForestError) as wrong_scope:
        catalog.build_official(root)
    assert wrong_scope.value.code == "OFFICIAL_PACKAGE_SOURCE_INVALID"

    assert catalog.build_official(root, scope="host").digest == artifact.digest
    assert catalog.is_official_host(artifact.digest)
    assert not catalog.is_official_deployment(artifact.digest)
    with pytest.raises(ForestError) as wrong_registry:
        catalog.get_official_deployment(artifact.digest)
    assert wrong_registry.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"

    imported = PackageCatalog(tmp_path / "host-imported")
    imported.import_archive(catalog.export_archive(artifact.digest))
    assert not imported.is_official_host(artifact.digest)
    with pytest.raises(ForestError) as imported_untrusted:
        imported.get_official_host(artifact.digest)
    assert imported_untrusted.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"


def test_bot_official_provenance_requires_host_registration(tmp_path):
    source_root = (
        Path(__file__).resolve().parents[1]
        / "src/forest/builtin/memory_graph"
    )
    catalog = PackageCatalog(tmp_path / "bot-catalog")
    ordinary = catalog.build(source_root)
    imported = PackageCatalog(tmp_path / "imported-bot-catalog")
    imported.import_archive(catalog.export_archive(ordinary.digest))

    for untrusted in (catalog, imported):
        assert not untrusted.is_official_bot(ordinary.digest)
        assert untrusted.artifact(ordinary.digest)["official_registered"] is False
        with pytest.raises(ForestError) as rejected:
            untrusted.get_official_bot(ordinary.digest)
        assert rejected.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"

    with pytest.raises(ForestError) as wrong_scope:
        catalog.build_official(source_root, scope="deployment")
    assert wrong_scope.value.code == "OFFICIAL_PACKAGE_SOURCE_INVALID"
    official = catalog.build_official(source_root, scope="bot")
    assert official.digest == ordinary.digest
    assert catalog.artifact(official.digest)["official_registered"] is True
    assert PackageCatalog(tmp_path / "bot-catalog").get_official_bot(official.digest).digest == official.digest
    assert not catalog.is_official_deployment(official.digest)
    assert not imported.is_official_bot(official.digest)
    assert imported.artifact(official.digest)["official_registered"] is False


def test_official_registration_and_artifact_index_commit_together(tmp_path):
    catalog = PackageCatalog(tmp_path / "official-atomic")
    index = catalog.root / "catalog.sqlite3"
    with sqlite3.connect(index) as db:
        db.execute(
            "CREATE TABLE official_bot_artifacts "
            "(digest TEXT PRIMARY KEY, component_id TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TRIGGER reject_official BEFORE INSERT ON official_bot_artifacts "
            "BEGIN SELECT RAISE(ABORT, 'injected official registration failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        catalog.build_official(EXAMPLE, scope="bot")
    assert catalog.list() == []
    with sqlite3.connect(index) as db:
        db.execute("DROP TRIGGER reject_official")
    snapshot = catalog.build_official(EXAMPLE, scope="bot")
    assert catalog.get_official_bot(snapshot.digest).digest == snapshot.digest


def test_official_build_does_not_hold_catalog_write_lock_while_fsyncing(
    tmp_path, monkeypatch
):
    sources = auto_install_official_sources("deployment")[:2]
    assert len(sources) == 2
    catalog = PackageCatalog(tmp_path / "official-concurrent")
    entered = Event()
    release = Event()
    real_fsync = packages.os.fsync
    paused = False

    def delayed_fsync(fd):
        nonlocal paused
        if not paused:
            paused = True
            entered.set()
            assert release.wait(timeout=30)
        return real_fsync(fd)

    monkeypatch.setattr(packages.os, "fsync", delayed_fsync)
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(catalog.build_official, sources[0], scope="deployment")
        try:
            assert entered.wait(timeout=30)
            second = workers.submit(catalog.build_official, sources[1], scope="deployment")
            second_snapshot = second.result(timeout=30)
        finally:
            release.set()
        first_snapshot = first.result(timeout=30)
    assert first_snapshot.digest != second_snapshot.digest
    assert catalog.get_official_deployment(first_snapshot.digest).digest == first_snapshot.digest
    assert catalog.get_official_deployment(second_snapshot.digest).digest == second_snapshot.digest


def test_official_archive_requires_exact_declared_bytes_and_identity(tmp_path):
    staging = PackageCatalog(tmp_path / "staging")
    source = staging.build(EXAMPLE)
    archive = staging.export_archive(source.digest)
    expected = {
        "scope": "bot",
        "expected_archive_sha256": "sha256:" + hashlib.sha256(archive).hexdigest(),
        "expected_package_digest": source.digest,
        "expected_manifest_digest": packages.digest_json(source.manifest),
        "expected_package_id": source.manifest["package_id"],
        "expected_component_id": source.component["id"],
        "expected_runtime": source.manifest["runtime"],
    }
    catalog = PackageCatalog(tmp_path / "release")
    for changed, code in (
        ({"expected_archive_sha256": "sha256:" + "0" * 64}, "PACKAGE_ARCHIVE_DIGEST_MISMATCH"),
        ({"expected_package_digest": "sha256:" + "0" * 64}, "OFFICIAL_PACKAGE_DIGEST_MISMATCH"),
        ({"expected_manifest_digest": "sha256:" + "0" * 64}, "OFFICIAL_PACKAGE_DIGEST_MISMATCH"),
        ({"expected_component_id": "another.component"}, "OFFICIAL_PACKAGE_SOURCE_INVALID"),
        ({"expected_runtime": "another-runtime"}, "OFFICIAL_PACKAGE_SOURCE_INVALID"),
    ):
        with pytest.raises(ForestError) as rejected:
            catalog.publish_official_archive(archive, **{**expected, **changed})
        assert rejected.value.code == code
        assert catalog.list() == []
    released = catalog.publish_official_archive(archive, **expected)
    assert released.digest == source.digest
    assert catalog.get_official_bot(released.digest).digest == released.digest
    assert catalog.publish_official_archive(archive, **expected).digest == released.digest


def test_official_archive_registration_failure_rolls_back_index(tmp_path):
    staging = PackageCatalog(tmp_path / "staging")
    source = staging.build(EXAMPLE)
    archive = staging.export_archive(source.digest)
    catalog = PackageCatalog(tmp_path / "release")
    index = catalog.root / "catalog.sqlite3"
    with sqlite3.connect(index) as db:
        db.execute(
            "CREATE TABLE official_bot_artifacts "
            "(digest TEXT PRIMARY KEY, component_id TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TRIGGER reject_official BEFORE INSERT ON official_bot_artifacts "
            "BEGIN SELECT RAISE(ABORT, 'injected official registration failure'); END"
        )
    expected = {
        "scope": "bot",
        "expected_archive_sha256": "sha256:" + hashlib.sha256(archive).hexdigest(),
        "expected_package_digest": source.digest,
        "expected_manifest_digest": packages.digest_json(source.manifest),
        "expected_package_id": source.manifest["package_id"],
        "expected_component_id": source.component["id"],
        "expected_runtime": source.manifest["runtime"],
    }
    with pytest.raises(sqlite3.IntegrityError):
        catalog.publish_official_archive(archive, **expected)
    assert catalog.list() == []
    with sqlite3.connect(index) as db:
        db.execute("DROP TRIGGER reject_official")
    assert catalog.publish_official_archive(archive, **expected).digest == source.digest


@pytest.mark.asyncio
async def test_host_local_command_publishes_only_bundled_bot_identity(tmp_path, capsys):
    source_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/routing_graph"
    staging = PackageCatalog(tmp_path / "staging")
    default = staging.build(source_root)
    updated_root = tmp_path / "updated-routing"
    shutil.copytree(source_root, updated_root)
    manifest_path = updated_root / "module.toml"
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8").replace(
            f'version = "{default.manifest["version"]}"',
            'version = "99.0.0"', 1,
        ), encoding="utf-8",
    )
    source = staging.build(updated_root)
    assert source.digest != default.digest
    archive = staging.export_archive(source.digest)
    archive_path = tmp_path / "routing.zip"
    archive_path.write_bytes(archive)
    inspection_root = tmp_path / "inspection"
    await run_plugin_command(Namespace(
        command="plugin", plugin_command="import",
        archive=archive_path, cache_dir=inspection_root,
    ))
    inspected = json.loads(capsys.readouterr().out)
    assert inspected["package_digest"] == source.digest
    assert inspected["archive_sha256"] == "sha256:" + hashlib.sha256(archive).hexdigest()
    assert not PackageCatalog(inspection_root).is_official_bot(source.digest)
    catalog = PackageCatalog(tmp_path / "production")
    assert catalog.build_official(source_root, scope="bot").digest == default.digest
    args = Namespace(
        command="plugin", plugin_command="publish-official-bot",
        archive=archive_path, catalog_dir=catalog.root,
        archive_sha256=inspected["archive_sha256"],
        package_digest=inspected["package_digest"],
        manifest_digest=inspected["manifest_digest"],
        package_id=inspected["package_id"],
        component_id=inspected["component_id"],
    )
    with pytest.raises(ForestError) as relative:
        await run_plugin_command(Namespace(**{**vars(args), "catalog_dir": Path("relative")}))
    assert relative.value.code == "OFFICIAL_RELEASE_CATALOG_INVALID"
    with pytest.raises(ForestError) as unknown:
        await run_plugin_command(Namespace(**{**vars(args), "component_id": "unknown.component"}))
    assert unknown.value.code == "OFFICIAL_RELEASE_COMPONENT_UNKNOWN"
    assert len(catalog.list()) == 1
    assert catalog.get_official_bot(default.digest).digest == default.digest

    await run_plugin_command(args)
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "published-official-bot"
    assert result["package_digest"] == source.digest
    assert len(catalog.list()) == 2
    assert catalog.get_official_bot(default.digest).digest == default.digest
    assert catalog.get_official_bot(source.digest).digest == source.digest


@pytest.mark.asyncio
@pytest.mark.parametrize("scope,folder", (
    ("deployment", "deployment_audit"),
    ("host", "host_audit"),
))
async def test_host_local_command_publishes_bundled_control_package(
    tmp_path, capsys, scope, folder,
):
    source_root = Path(__file__).resolve().parents[1] / "src/forest/builtin" / folder
    staging = PackageCatalog(tmp_path / "staging")
    default = staging.build(source_root)
    updated_root = tmp_path / "updated"
    shutil.copytree(source_root, updated_root)
    manifest_path = updated_root / "module.toml"
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8").replace(
            f'version = "{default.manifest["version"]}"',
            'version = "99.0.0"', 1,
        ), encoding="utf-8",
    )
    updated = staging.build(updated_root)
    archive = staging.export_archive(updated.digest)
    archive_path = tmp_path / "updated.zip"
    archive_path.write_bytes(archive)
    catalog = PackageCatalog(tmp_path / "production")
    assert catalog.build_official(source_root, scope=scope).digest == default.digest
    args = Namespace(
        command="plugin", plugin_command=f"publish-official-{scope}",
        archive=archive_path, catalog_dir=catalog.root,
        archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
        package_digest=updated.digest,
        manifest_digest=packages.digest_json(updated.manifest),
        package_id=updated.manifest["package_id"],
        component_id=updated.component["id"],
    )
    with pytest.raises(ForestError) as wrong_scope:
        await run_plugin_command(Namespace(**{
            **vars(args), "plugin_command": "publish-official-" + (
                "host" if scope == "deployment" else "deployment"
            ),
        }))
    assert wrong_scope.value.code == "OFFICIAL_RELEASE_COMPONENT_UNKNOWN"
    with pytest.raises(ForestError) as wrong_digest:
        await run_plugin_command(Namespace(**{
            **vars(args), "package_digest": "sha256:" + "0" * 64,
        }))
    assert wrong_digest.value.code == "OFFICIAL_PACKAGE_DIGEST_MISMATCH"
    assert len(catalog.list()) == 1
    await run_plugin_command(args)
    released = json.loads(capsys.readouterr().out)
    assert released["status"] == f"published-official-{scope}"
    assert released["package_digest"] == updated.digest
    assert len(catalog.list()) == 2
    assert getattr(catalog, f"is_official_{scope}")(updated.digest)
    assert not getattr(catalog, "is_official_" + (
        "host" if scope == "deployment" else "deployment"
    ))(updated.digest)
    ordinary = PackageCatalog(tmp_path / "ordinary")
    ordinary.import_archive(archive)
    assert not getattr(ordinary, f"is_official_{scope}")(updated.digest)


def test_bot_official_selection_resolves_exact_registered_artifact(tmp_path):
    source_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/memory_graph"
    catalog = PackageCatalog(tmp_path / "catalog")
    default = catalog.build_official(source_root, scope="bot")
    updated_root = tmp_path / "updated-memory"
    shutil.copytree(source_root, updated_root)
    manifest_path = updated_root / "module.toml"
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8").replace(
            f'version = "{default.manifest["version"]}"',
            'version = "99.0.0"',
            1,
        ),
        encoding="utf-8",
    )
    updated = catalog.build_official(updated_root, scope="bot")
    entry = ComponentSelectionEntry(
        default.component["id"], "official", updated.digest,
        packages.digest_json(updated.manifest), (), True, 1,
    )
    frozen = {
        "protocol": "forest.component-selection-snapshot/1",
        "revision": 1,
        "entries": [entry.as_dict()],
    }
    selection = ComponentSelectionSnapshot(
        1, (entry,), packages.digest_json(frozen)
    )
    assert resolve_official_package_snapshots(
        selection, (default,), catalog
    ) == (updated,)
    untrusted = PackageCatalog(tmp_path / "untrusted")
    untrusted.import_archive(catalog.export_archive(default.digest))
    untrusted.import_archive(catalog.export_archive(updated.digest))
    with pytest.raises(ForestError) as rejected:
        resolve_official_package_snapshots(selection, (default,), untrusted)
    assert rejected.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"
    untrusted.build_official(source_root, scope="bot")
    with pytest.raises(ForestError) as rejected:
        resolve_official_package_snapshots(selection, (default,), untrusted)
    assert rejected.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ("deployment", "host"))
async def test_non_bot_package_artifact_cannot_enter_bot_runtime(tmp_path, scope):
    root = source(tmp_path)
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('scope = "bot"', f'scope = "{scope}"'),
        encoding="utf-8",
    )
    catalog = PackageCatalog(tmp_path / "catalog")
    snapshot = catalog.build(root)
    assert snapshot.component["scope"] == scope
    with pytest.raises(ForestError) as rejected:
        await bootstrap_installed_package(
            snapshot.digest, catalog, None, None, kernel=None,
            initial_configuration={}, approved_capabilities=("state.read", "state.write"),
        )
    assert rejected.value.code == "PACKAGE_SCOPE_UNSUPPORTED"


@pytest.mark.asyncio
async def test_deployment_package_passes_isolated_worker_check(tmp_path, capsys):
    root = source(tmp_path)
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('scope = "bot"', 'scope = "deployment"'),
        encoding="utf-8",
    )
    await run_plugin_command(Namespace(
        command="plugin", plugin_command="check", source=root,
        cache_dir=tmp_path / "catalog", config_json="{}",
    ))
    assert json.loads(capsys.readouterr().out)["status"] == "checked"


def test_package_storage_cannot_claim_another_scope(tmp_path):
    root = source(tmp_path)
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('scope = "bot"', 'scope = "deployment"'),
        encoding="utf-8",
    )
    schema = root / "schema" / "baseline.sql"
    schema.parent.mkdir()
    baseline = b"CREATE TABLE owned_state (key text PRIMARY KEY, value jsonb NOT NULL);\n"
    schema.write_bytes(baseline)
    with manifest.open("a", encoding="utf-8") as stream:
        stream.write(
            "\n[components.storage]\n"
            'schema_id = "counter.state"\n'
            'schema_version = "1.0.0"\n'
            'baseline = "schema/baseline.sql"\n'
            f'baseline_sha256 = "{hashlib.sha256(baseline).hexdigest()}"\n'
            'scope = "bot"\n'
            'repository_contract = "counter.repository/1"\n'
        )
    with pytest.raises(ForestError) as rejected:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert rejected.value.code == "INVALID_STORAGE_BUNDLE"

    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'scope = "bot"', 'scope = "deployment"'
        ),
        encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "deployment-catalog").build(root)
    storage, _ = snapshot.storage_bundle()
    spec = StorageBundleSpec(
        component_id=snapshot.component["id"],
        schema_id="counter.state",
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract="counter.repository/1",
        scope="deployment",
    )
    assert storage["scope"] == "deployment"
    assert storage["package_digest"] == packages.storage_bundle_digest(spec, "declarative")


@pytest.mark.asyncio
async def test_bot_package_directory_fences_deployment_artifacts(tmp_path):
    catalog = PackageCatalog(tmp_path / "catalog")
    bot = catalog.build(EXAMPLE)
    bot_revision = catalog.page(scope="bot")["catalog_revision"]
    deployment_root = source(tmp_path)
    manifest = deployment_root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8")
        .replace('package_id = "example.counter"', 'package_id = "example.deployment-counter"')
        .replace('scope = "bot"', 'scope = "deployment"'),
        encoding="utf-8",
    )
    deployment = catalog.build(deployment_root)
    assert catalog.page(scope="bot")["catalog_revision"] == bot_revision
    assert [item["scope"] for item in catalog.page(scope="bot")["items"]] == ["bot"]
    assert [item["scope"] for item in catalog.page(scope="deployment")["items"]] == [
        "deployment"
    ]
    calls = []

    async def read_registry(query_id, payload):
        calls.append((query_id, payload))
        if query_id == "packages":
            return catalog.page(**payload)
        return catalog.artifact(payload["package_digest"])

    context = SimpleNamespace(package_registry_reader=read_registry)
    page = await package_catalog_page_service(context)({
        "protocol": "forest.package-catalog-page-request/1", "limit": 1,
    })
    assert [item["package_digest"] for item in page["items"]] == [bot.digest]
    assert page["next_after"] is None
    assert calls[0] == ("packages", {"limit": 1, "scope": "bot"})
    with pytest.raises(ForestError) as rejected:
        await package_catalog_artifact_service(context)({
            "protocol": "forest.package-catalog-artifact-request/1",
            "package_digest": deployment.digest,
        })
    assert rejected.value.code == "PACKAGE_SCOPE_UNSUPPORTED"


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


def test_catalog_page_verifies_artifacts_and_fences_concurrent_imports(tmp_path):
    first_source = source(tmp_path)
    catalog = PackageCatalog(tmp_path / "catalog")
    first = catalog.build(first_source)
    page = catalog.page(limit=1)
    assert page["items"] == [{
        "package_id": first.manifest["package_id"],
        "package_version": first.manifest["version"],
        "package_digest": first.digest,
        "manifest_digest": packages.digest_json(first.manifest),
        "component_id": first.component["id"],
        "scope": first.component["scope"],
        "runtime": first.manifest["runtime"],
        "kernel_api": first.manifest["kernel_api"],
        "requested_capabilities": first.component["capabilities"]["requests"],
    }]
    assert page["next_after"] is None
    artifact = catalog.artifact(first.digest)
    assert artifact["protocol"] == "forest.package-catalog-artifact/2"
    for owner in ("component_worker_host", "deployment_package_registry"):
        output_schema = json.loads((
            Path(__file__).resolve().parents[1]
            / f"src/forest/builtin/{owner}/schemas/artifact-output.json"
        ).read_text(encoding="utf-8"))
        Draft202012Validator(output_schema).validate(artifact)
    assert artifact["package_digest"] == page["items"][0]["package_digest"]
    assert artifact["manifest_digest"] == page["items"][0]["manifest_digest"]
    assert artifact["configuration_schema_digest"] == packages.digest_json(
        first.schema(first.component["config_schema"])
    )
    assert artifact["provides"] == [
        {"name": item["name"], "contract": item["contract"]}
        for item in first.component["provides"]
    ]
    with pytest.raises(ForestError) as missing:
        catalog.artifact("sha256:" + "0" * 64)
    assert missing.value.code == "PACKAGE_NOT_FOUND"
    second_source = tmp_path / "second"
    shutil.copytree(EXAMPLE, second_source)
    manifest = second_source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace('version = "0.1.0"', 'version = "0.1.1"'),
        encoding="utf-8",
    )
    second = catalog.build(second_source)
    with pytest.raises(ForestError) as changed:
        catalog.page(catalog_revision=page["catalog_revision"])
    assert changed.value.code == "PACKAGE_CATALOG_CHANGED"
    first_page = catalog.page(limit=1)
    assert first_page["next_after"] == {
        "package_id": first.manifest["package_id"], "version": first.manifest["version"],
    }
    with pytest.raises(ForestError) as unfenced:
        catalog.page(
            after_package_id=first_page["next_after"]["package_id"],
            after_version=first_page["next_after"]["version"],
            limit=1,
        )
    assert unfenced.value.code == "PACKAGE_CATALOG_PAGE_INVALID"
    second_page = catalog.page(
        after_package_id=first_page["next_after"]["package_id"],
        after_version=first_page["next_after"]["version"],
        catalog_revision=first_page["catalog_revision"], limit=1,
    )
    assert second_page["items"][0]["package_digest"] == second.digest
    assert second_page["next_after"] is None


@pytest.mark.parametrize("grant", [False, True])
def test_service_call_grants_require_manifest_dependencies(tmp_path, grant):
    root = source(tmp_path)
    manifest = root / "module.toml"
    value = manifest.read_text(encoding="utf-8")
    if grant:
        value = value.replace(
            'requests = ["state.read", "state.write"]',
            'requests = ["state.read", "state.write", "service.call:memory.query.begin"]',
        )
    else:
        value += (
            '\n[[components.requires]]\nname = "memory.query.begin"\n'
            'contract = ">=1.0,<2.0"\nrequired = true\nstartup_dependency = false\n'
        )
    manifest.write_text(value, encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "INVALID_DEPENDENCY"


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


@pytest.mark.parametrize("reference,body", [
    ("../outside.json", "{}"),
    ("missing.json", None),
    ("bootstrap.json", "[]"),
    ("bootstrap.json", '{"value":NaN}'),
])
def test_bootstrap_seed_must_be_a_bounded_package_json_object(tmp_path, reference, body):
    root = source(tmp_path)
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'config_schema = "schemas/config.json"',
            f'config_schema = "schemas/config.json"\nbootstrap_config = "{reference}"',
            1,
        ),
        encoding="utf-8",
    )
    if body is not None:
        (root / "bootstrap.json").write_text(body, encoding="utf-8")
    with pytest.raises(ForestError) as rejected:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert rejected.value.code == "INVALID_BOOTSTRAP_CONFIGURATION"


@pytest.mark.parametrize("section,setting", [
    ("resources", "max_frame_bytes = 1023"),
    ("resources", "max_frame_bytes = 262145"),
    ("resources", "max_inflight = true"),
    ("resources", "max_inflight = 0"),
    ("resources", "max_state_inflight = 257"),
    ("resources", "max_owned_tasks = 1.5"),
    ("resources", "max_owned_resources = 0"),
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
                     "max_owned_resources = 5\n"
                     "[components.lifecycle]\nprepare_timeout_s = 0.5\nstart_timeout_s = 120\n"
                     "drain_timeout_s = 10\nstop_timeout_s = 1\n")
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    assert snapshot.component["resources"]["max_queue_items"] == 0
    assert snapshot.component["resources"]["max_owned_resources"] == 5
    assert snapshot.component["lifecycle"]["prepare_timeout_s"] == 0.5


def test_management_ui_is_immutable_package_content(tmp_path):
    root = source(tmp_path)
    (root / "ui").mkdir()
    html = b"<!doctype html><html><body><script>parent.postMessage('ignored','*')</script></body></html>"
    (root / "ui" / "index.html").write_bytes(html)
    with (root / "module.toml").open("a", encoding="utf-8") as stream:
        stream.write(
            '\n[components.management_ui]\nentrypoint = "ui/index.html"\n'
            'protocol = "forest.management-ui/1"\n'
        )
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    metadata, content = snapshot.management_ui()
    assert content == html
    assert metadata == {
        "entrypoint": "ui/index.html",
        "protocol": "forest.management-ui/1",
        "sha256": __import__("hashlib").sha256(html).hexdigest(),
        "bytes": len(html),
    }
    path = snapshot.root / "ui" / "index.html"
    path.chmod(0o600)
    path.write_text("<!doctype html><html>tampered</html>", encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        snapshot.management_ui()
    assert caught.value.code == "PACKAGE_TAMPERED"


def test_storage_bundle_is_content_addressed_package_content(tmp_path):
    root = source(tmp_path)
    schema = root / "schema" / "baseline.sql"
    schema.parent.mkdir()
    sql = b"CREATE TABLE owned_state (key text PRIMARY KEY, value jsonb NOT NULL);\n"
    schema.write_bytes(sql)
    digest = hashlib.sha256(sql).hexdigest()
    with (root / "module.toml").open("a", encoding="utf-8") as stream:
        stream.write(
            "\n[components.storage]\n"
            'schema_id = "counter.state"\n'
            'schema_version = "1.0.0"\n'
            'baseline = "schema/baseline.sql"\n'
            f'baseline_sha256 = "{digest}"\n'
            'scope = "bot"\n'
            'repository_contract = "counter.repository/1"\n'
        )
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    metadata, content = snapshot.storage_bundle()
    assert content == sql
    assert metadata == {
        "schema_id": "counter.state",
        "schema_version": "1.0.0",
        "namespace": module_schema_namespace("counter.state"),
        "baseline": "schema/baseline.sql",
        "baseline_sha256": digest,
        "scope": "bot",
        "repository_contract": "counter.repository/1",
        "execution_profile": "declarative",
        "bytes": len(sql),
        "package_digest": packages.storage_bundle_digest(
            StorageBundleSpec(
                "example.counter", "counter.state", "1.0.0", digest, "counter.repository/1"
            ),
            "declarative",
        ),
    }
    copied = snapshot.root / "schema" / "baseline.sql"
    copied.chmod(0o600)
    copied.write_text("CREATE TABLE changed(id int);", encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        snapshot.storage_bundle()
    assert caught.value.code == "PACKAGE_TAMPERED"


def test_build_cache_rechecks_source_bytes_and_artifact(tmp_path):
    root = source(tmp_path)
    catalog = PackageCatalog(tmp_path / "catalog")
    snapshot = catalog.build(root)
    assert catalog.build(root).digest == snapshot.digest

    source_file = root / "python" / "counter.py"
    original = source_file.read_bytes()
    source_file.write_bytes(original + b"\n# changed without a version bump\n")
    with pytest.raises(ForestError) as changed:
        catalog.build(root)
    assert changed.value.code == "PACKAGE_VERSION_CONFLICT"

    source_file.write_bytes(original)
    artifact_file = snapshot.module_root / "counter.py"
    artifact_file.chmod(0o600)
    artifact_file.write_text("# tampered after publication\n", encoding="utf-8")
    with pytest.raises(ForestError) as tampered:
        catalog.build(root)
    assert tampered.value.code == "PACKAGE_TAMPERED"


def test_generic_uow_cannot_claim_configuration_rebuild_authority(tmp_path):
    root = source(tmp_path)
    with (root / "module.toml").open("a", encoding="utf-8") as stream:
        stream.write(
            '\n[[components.unit_of_work_plans]]\nname = "forge.rebuild"\n'
            'contract = "1.0"\ninput_schema = "schemas/input.json"\n'
            'output_schema = "schemas/output.json"\ntimeout_s = 10\n'
            '[[components.unit_of_work_plans.steps]]\nid = "control"\n'
            'component_id = "component.worker-host"\n'
            'participant = "component.configuration-rebuild.record"\n'
        )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "INVALID_UNIT_OF_WORK_PLAN"


@pytest.mark.parametrize(
    "baseline,digest,contract,code",
    [
        ("../baseline.sql", "0" * 64, "counter.repository/1", "INVALID_STORAGE_BUNDLE"),
        ("schema/baseline.sql", "0" * 64, "counter.repository/1", "INVALID_STORAGE_BUNDLE"),
        ("schema/baseline.sql", None, "counter.repository/1", "INVALID_STORAGE_BUNDLE"),
        ("schema/baseline.sql", None, "counter repository", "INVALID_STORAGE_BUNDLE"),
    ],
)
def test_invalid_storage_bundle_declarations_are_rejected(
    tmp_path, baseline, digest, contract, code
):
    root = source(tmp_path)
    schema = root / "schema" / "baseline.sql"
    schema.parent.mkdir()
    sql = b"CREATE ROLE forbidden;\n" if digest is None and " " not in contract else b"SELECT 1;\n"
    schema.write_bytes(sql)
    actual = hashlib.sha256(sql).hexdigest()
    with (root / "module.toml").open("a", encoding="utf-8") as stream:
        stream.write(
            "\n[components.storage]\n"
            'schema_id = "counter.state"\n'
            'schema_version = "1.0.0"\n'
            f'baseline = "{baseline}"\n'
            f'baseline_sha256 = "{digest or actual}"\n'
            'scope = "bot"\n'
            f'repository_contract = "{contract}"\n'
        )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == code


def test_domain_authority_ports_require_package_owned_storage(tmp_path):
    root = source(tmp_path)
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'requests = ["state.read", "state.write"]',
            'requests = ["domain.query:counter.current"]',
        )
        + """

[[components.domain_queries]]
name = "counter.current"
contract = "1.0"
function = "counter_current"
input_schema = "schemas/input.json"
output_schema = "schemas/output.json"
""",
        encoding="utf-8",
    )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code == "INVALID_DOMAIN_AUTHORITY"


@pytest.mark.parametrize("entrypoint,protocol,body", [
    ("../index.html", "forest.management-ui/1", "<!doctype html><html></html>"),
    ("ui/index.js", "forest.management-ui/1", "<!doctype html><html></html>"),
    ("ui/index.html", "forest.management-ui/2", "<!doctype html><html></html>"),
    ("ui/index.html", "forest.management-ui/1", "fragment only"),
])
def test_invalid_management_ui_declarations_are_rejected(
        tmp_path, entrypoint, protocol, body):
    root = source(tmp_path)
    target = root / "ui" / "index.html"
    target.parent.mkdir()
    target.write_text(body, encoding="utf-8")
    with (root / "module.toml").open("a", encoding="utf-8") as stream:
        stream.write(
            f'\n[components.management_ui]\nentrypoint = "{entrypoint}"\n'
            f'protocol = "{protocol}"\n'
        )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert caught.value.code in {"INVALID_MANIFEST", "INVALID_MANAGEMENT_UI"}


def test_event_declarations_and_capabilities_survive_static_snapshot(tmp_path):
    root = source(tmp_path)
    manifest = root / "module.toml"
    text = manifest.read_text(encoding="utf-8").replace(
        'requests = ["state.read", "state.write"]',
        'requests = ["state.read", "state.write", "event.publish:counter.changed"]',
    )
    text += '''
[[components.publishes]]
topic = "counter.changed"
contract = "1.0"
schema = "schemas/input.json"

[[components.subscribes]]
subscriber_id = "counter.projection"
topic = "counter.changed"
contract = "1.0"
schema = "schemas/input.json"
max_attempts = 5
'''
    manifest.write_text(text, encoding="utf-8")
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    assert snapshot.component["publishes"][0]["topic"] == "counter.changed"
    assert snapshot.component["subscribes"][0]["max_attempts"] == 5


def test_event_capability_without_publication_is_rejected(tmp_path):
    root = source(tmp_path)
    manifest = root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'requests = ["state.read", "state.write"]',
            'requests = ["state.read", "state.write", "event.publish:undeclared"]',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ForestError) as error:
        PackageCatalog(tmp_path / "catalog").build(root)
    assert error.value.code == "UNSUPPORTED_CAPABILITY"


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


def test_concurrent_archive_import_replays_one_atomic_catalog_receipt(tmp_path):
    source_catalog = PackageCatalog(tmp_path / "source-catalog")
    snapshot = source_catalog.build(source(tmp_path))
    archive = source_catalog.export_archive(snapshot.digest)
    destination_root = tmp_path / "destination"
    operation_id = "web:sha256:" + "a" * 64
    archive_sha256 = "sha256:" + hashlib.sha256(archive).hexdigest()

    def import_once():
        return PackageCatalog(destination_root).import_archive_idempotent(
            archive, operation_id=operation_id, actor_ref="admin:one",
            archive_sha256=archive_sha256,
        ).digest

    with ThreadPoolExecutor(max_workers=2) as workers:
        assert list(workers.map(lambda _: import_once(), range(2))) == [
            snapshot.digest, snapshot.digest,
        ]
    destination = PackageCatalog(destination_root)
    assert [item.digest for item in destination.list()] == [snapshot.digest]
    assert destination.import_receipt(
        operation_id=operation_id, actor_ref="admin:one",
        archive_sha256=archive_sha256,
    )["package_digest"] == snapshot.digest
    assert destination.import_receipt(
        operation_id="web:sha256:" + "c" * 64, actor_ref="admin:one",
        archive_sha256=archive_sha256,
    ) is None
    assert import_once() == snapshot.digest
    with pytest.raises(ForestError) as actor_conflict:
        destination.import_archive_idempotent(
            archive, operation_id=operation_id, actor_ref="admin:two",
            archive_sha256=archive_sha256,
        )
    assert actor_conflict.value.code == "PACKAGE_IMPORT_OPERATION_CONFLICT"
    with pytest.raises(ForestError) as receipt_conflict:
        destination.import_receipt(
            operation_id=operation_id, actor_ref="admin:two",
            archive_sha256=archive_sha256,
        )
    assert receipt_conflict.value.code == "PACKAGE_IMPORT_OPERATION_CONFLICT"
    with pytest.raises(ForestError) as digest_conflict:
        destination.import_archive_idempotent(
            archive + b"different", operation_id=operation_id, actor_ref="admin:one",
            archive_sha256="sha256:" + hashlib.sha256(archive + b"different").hexdigest(),
        )
    assert digest_conflict.value.code == "PACKAGE_IMPORT_OPERATION_CONFLICT"
    with pytest.raises(ForestError) as wrong_digest:
        destination.import_archive_idempotent(
            archive, operation_id="web:sha256:" + "b" * 64,
            actor_ref="admin:one", archive_sha256="sha256:" + "0" * 64,
        )
    assert wrong_digest.value.code == "PACKAGE_ARCHIVE_DIGEST_MISMATCH"


def test_import_close_and_publish_choose_one_durable_outcome(tmp_path):
    source_catalog = PackageCatalog(tmp_path / "source-catalog")
    snapshot = source_catalog.build(source(tmp_path))
    archive = source_catalog.export_archive(snapshot.digest)
    root = tmp_path / "destination"
    identity = {
        "operation_id": "web:sha256:" + "d" * 64,
        "actor_ref": "admin:one",
        "archive_sha256": "sha256:" + hashlib.sha256(archive).hexdigest(),
    }
    closed = PackageCatalog(root).close_import_if_absent(**identity)
    assert closed == {"found": False, "package_digest": None, "closed": True}
    assert PackageCatalog(root).close_import_if_absent(**identity) == closed
    with pytest.raises(ForestError) as late:
        PackageCatalog(root).import_archive_idempotent(archive, **identity)
    assert late.value.code == "PACKAGE_IMPORT_CLOSED"
    with pytest.raises(ForestError) as conflict:
        PackageCatalog(root).close_import_if_absent(**{**identity, "actor_ref": "admin:two"})
    assert conflict.value.code == "PACKAGE_IMPORT_OPERATION_CONFLICT"

    racing = {**identity, "operation_id": "web:sha256:" + "e" * 64}
    ready = Event()

    def publish():
        ready.wait()
        try:
            return ("published", PackageCatalog(root).import_archive_idempotent(
                archive, **racing,
            ).digest)
        except ForestError as error:
            return ("rejected", error.code)

    def close():
        ready.wait()
        return PackageCatalog(root).close_import_if_absent(**racing)

    with ThreadPoolExecutor(max_workers=2) as workers:
        publication = workers.submit(publish)
        closure = workers.submit(close)
        ready.set()
        published, observed = publication.result(), closure.result()
    if observed["found"]:
        assert observed == {
            "found": True, "package_digest": snapshot.digest, "closed": False,
        }
        assert published == ("published", snapshot.digest)
    else:
        assert observed == {"found": False, "package_digest": None, "closed": True}
        assert published == ("rejected", "PACKAGE_IMPORT_CLOSED")
    assert PackageCatalog(root).close_import_if_absent(**racing) == observed


def test_import_close_can_win_while_archive_bytes_are_fsyncing(tmp_path, monkeypatch):
    source_catalog = PackageCatalog(tmp_path / "source-catalog")
    snapshot = source_catalog.build(source(tmp_path))
    archive = source_catalog.export_archive(snapshot.digest)
    destination = PackageCatalog(tmp_path / "destination")
    identity = {
        "operation_id": "web:sha256:" + "f" * 64,
        "actor_ref": "admin:one",
        "archive_sha256": "sha256:" + hashlib.sha256(archive).hexdigest(),
    }
    entered = Event()
    release = Event()
    real_fsync = packages.os.fsync
    paused = False

    def delayed_fsync(fd):
        nonlocal paused
        if not paused:
            paused = True
            entered.set()
            assert release.wait(timeout=30)
        return real_fsync(fd)

    monkeypatch.setattr(packages.os, "fsync", delayed_fsync)

    def publish():
        try:
            destination.import_archive_idempotent(archive, **identity)
        except ForestError as error:
            return error.code
        return "PUBLISHED"

    with ThreadPoolExecutor(max_workers=1) as workers:
        publication = workers.submit(publish)
        try:
            assert entered.wait(timeout=30)
            closed = destination.close_import_if_absent(**identity)
        finally:
            release.set()
        result = publication.result(timeout=30)
    assert closed == {"found": False, "package_digest": None, "closed": True}
    assert result == "PACKAGE_IMPORT_CLOSED"
    assert destination.list() == []
    assert not list(destination.root.glob(".capture-*"))


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
