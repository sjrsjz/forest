"""Recover a package import after its catalog close survives a real host death."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.deployment_package_controller import DeploymentPackageController
from forest.deployment_runtime import DeploymentRuntimeScope
from forest.packages import PackageCatalog
from forest.schema_bundles import module_schema_namespace
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_store import DeploymentStore

_SCHEMA = module_schema_namespace("forest.deployment.package-imports")
_OPERATION = "test-crash-after-physical-close"
_ARCHIVE_SHA256 = "sha256:" + "a" * 64


def _sources() -> tuple[Path, Path]:
    root = Path(__file__).resolve().parents[1] / "src/forest/builtin"
    return root / "deployment_audit", root / "deployment_package_registry"


async def _crash_child(spec_path: Path) -> None:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    store = DeploymentStore(spec["dsn"], "deployment/import-crash")
    await store.open()
    repository = DeploymentCatalogRepository(store)
    await repository.initialize()
    lease = await repository.claim_supervisor(
        "host/import-crash", "incarnation/import-crash", lease_seconds=10,
    )
    runtime = DeploymentRuntimeScope(
        store, SimpleNamespace(authority_lease=lambda **_values: lease),
        package_catalog=PackageCatalog(Path(spec["catalog"])),
    )
    controller = DeploymentPackageController(
        runtime, runtime.package_catalog, official_sources=_sources(),
    )
    try:
        registry = await controller.start()
        module = registry.get("deployment.package-registry")
        driver = module.management_contract.adapter._effect_drivers[
            "deployment.catalog.import-close"
        ]
        original_invoke = driver.invoke

        async def close_then_crash(payload, *, scope, invocation):
            result = await original_invoke(
                payload, scope=scope, invocation=invocation,
            )
            if payload["operation_id"] == _OPERATION:
                assert result == {
                    "found": False, "package_digest": None, "closed": True,
                }
                os._exit(137)
            return result

        driver.invoke = close_then_crash
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("INSERT INTO {}.package_import_requests "
                        "(operation_id,actor_ref,generation_id,content_length,"
                        "archive_sha256,status,created_at) VALUES "
                        "(%s,%s,%s,1,%s,'RESERVED',"
                        "clock_timestamp()-interval '2 minutes')")
                .format(sql.Identifier(_SCHEMA)),
                (
                    _OPERATION, "admin:crash",
                    registry.graph_identity.generations["deployment.package-registry"],
                    _ARCHIVE_SHA256,
                ),
            )
        await asyncio.sleep(30)
        raise AssertionError("Recovery worker did not reach the physical close")
    finally:
        await controller.close()
        await store.close()


@pytest.mark.postgres
async def test_import_recovery_reclaims_after_real_process_death(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    database = "forest_import_crash_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + database, "", ""))
    catalog_root = tmp_path / "packages"
    spec_path = tmp_path / "import-crash.json"
    spec_path.write_text(
        json.dumps({"dsn": uri, "catalog": str(catalog_root)}), encoding="utf-8",
    )
    store = None
    controller = None
    try:
        child = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(Path(__file__).resolve()), "--crash-child", str(spec_path)],
            capture_output=True, text=True, timeout=90, check=False,
        )
        assert child.returncode == 137, (child.returncode, child.stdout, child.stderr)
        catalog = PackageCatalog(catalog_root)
        with closing(sqlite3.connect(catalog_root / "catalog.sqlite3")) as db:
            assert db.execute(
                "SELECT actor_ref,archive_sha256 FROM package_import_closures "
                "WHERE operation_id=?", (_OPERATION,),
            ).fetchone() == ("admin:crash", _ARCHIVE_SHA256)
            assert db.execute(
                "SELECT digest FROM package_imports WHERE operation_id=?",
                (_OPERATION,),
            ).fetchone() is None
        store = DeploymentStore(uri, "deployment/import-crash")
        await store.open()
        repository = DeploymentCatalogRepository(store)
        async with store.pool.connection() as conn:
            row = await (await conn.execute(
                sql.SQL("SELECT status,recovery_attempt,recovery_lease_deadline "
                        "FROM {}.package_import_requests WHERE operation_id=%s")
                .format(sql.Identifier(_SCHEMA)), (_OPERATION,),
            )).fetchone()
            lease_row = await (await conn.execute(
                "SELECT expires_at FROM forest_deployment.supervisor_lease "
                "WHERE singleton"
            )).fetchone()
        assert row["status"] == "RESERVED" and row["recovery_attempt"] == 1
        delay = (lease_row["expires_at"] - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay + 0.5)
        new_lease = await repository.claim_supervisor(
            "host/import-crash-restarted", "incarnation/import-crash-restarted",
        )
        runtime = DeploymentRuntimeScope(
            store, SimpleNamespace(authority_lease=lambda **_values: new_lease),
            package_catalog=catalog,
        )
        controller = DeploymentPackageController(
            runtime, catalog, official_sources=_sources(),
        )
        await controller.start()
        async with asyncio.timeout(70):
            while True:
                async with store.pool.connection() as conn:
                    recovered = await (await conn.execute(
                        sql.SQL("SELECT status,recovery_attempt,package_digest "
                                "FROM {}.package_import_requests WHERE operation_id=%s")
                        .format(sql.Identifier(_SCHEMA)), (_OPERATION,),
                    )).fetchone()
                if recovered["status"] == "INTERRUPTED":
                    break
                await asyncio.sleep(0.2)
        assert recovered["recovery_attempt"] == 2
        assert recovered["package_digest"] is None
    finally:
        if controller is not None:
            await controller.close()
        if store is not None:
            await store.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(database)
        ))
        await admin.close()


if __name__ == "__main__" and len(sys.argv) == 3 and sys.argv[1] == "--crash-child":
    asyncio.run(_crash_child(Path(sys.argv[2])))
