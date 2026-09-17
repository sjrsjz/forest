"""Backup, restore, and storage management through scoped module contracts."""

import asyncio
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from test_management import ORIGIN, PASSWORD, create_account, initialize, login, login_as

from forest.backup_artifact import decrypt_file, inspect_zip
from forest.bootstrap import BootstrapState
from forest.management.app import WebSettings, create_app


@pytest_asyncio.fixture
async def backup_database():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must point to a private PostgreSQL test instance")
    name = "forest_backup_web_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    try:
        yield uri
    finally:
        restore_names = []
        try:
            async with await psycopg.AsyncConnection.connect(
                uri, row_factory=psycopg.rows.dict_row
            ) as deployment:
                exists = await (
                    await deployment.execute(
                        "SELECT to_regclass('restore_stage_targets') AS name"
                    )
                ).fetchone()
                if exists["name"] is not None:
                    rows = await (
                        await deployment.execute(
                            "SELECT database_name FROM restore_stage_targets"
                        )
                    ).fetchall()
                    restore_names = [row["database_name"] for row in rows]
        except psycopg.Error:
            pass
        for restore_name in restore_names:
            assert restore_name.startswith("forest_restore_") and len(restore_name) == 40
            await admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(restore_name)
                )
            )
        assert name.startswith("forest_backup_web_") and len(name) == 50
        await admin.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
        )
        await admin.close()


async def module_roots(client):
    session = (await client.get("/management/v1/session")).json()
    deployment = (
        f"/management/v1/scopes/deployment/{session['deployment_id']}/modules"
    )
    host = f"/management/v1/scopes/host/{session['host_id']}/modules"
    return {
        "deployment": deployment,
        "host": host,
        "backup": deployment + "/deployment.backup",
        "restore": deployment + "/deployment.restore",
        "storage": host + "/host.storage",
        "audit": deployment + "/deployment.audit",
    }


async def action(client, root, operation, body, key):
    return await client.post(
        f"{root}/actions/{operation}",
        json=body,
        headers={"idempotency-key": key},
    )


async def wait_for(client, url, terminal, *, attempts=160):
    row = None
    for _ in range(attempts):
        response = await client.get(url)
        assert response.status_code == 200, response.text
        row = response.json()
        if row["state"] in terminal:
            return row
        await asyncio.sleep(0.05)
    raise AssertionError(f"operation did not finish: {row}")


@pytest.mark.postgres
async def test_scoped_backup_restore_and_cleanup_contracts(tmp_path, backup_database):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN, managed_dsn=os.environ["FOREST_TEST_DSN"]))
    passphrase = "web backup recovery passphrase"
    create_body = {
        "current_password": PASSWORD,
        "recovery_passphrase": passphrase,
        "include_packages": False,
    }
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as admin,
    ):
        await initialize(admin, token, backup_database)
        await login(admin)
        roots = await module_roots(admin)

        html = await admin.get("/")
        assert 'id="nav-backups"' not in html.text
        assert 'id="storage-cleanup-form"' not in html.text
        assert (await admin.get("/static/backup-panel.js")).status_code == 404
        deployment_catalog = await admin.get(roots["deployment"])
        deployment_modules = {
            row["component_id"] for row in deployment_catalog.json()["modules"]
        }
        assert {"deployment.backup", "deployment.restore"} <= deployment_modules
        host_catalog = await admin.get(roots["host"])
        assert "host.storage" in {
            row["component_id"] for row in host_catalog.json()["modules"]
        }

        empty = await admin.get(roots["backup"] + "/queries/backups")
        assert empty.status_code == 200 and empty.json()["backups"] == []
        generations = await admin.get(roots["storage"] + "/queries/generations")
        assert generations.status_code == 200
        assert generations.json()["storage_generations"] == []

        rejected = await action(
            admin,
            roots["backup"],
            "backup.create",
            {**create_body, "current_password": "wrong-password-long-enough"},
            "wrong-password",
        )
        assert rejected.status_code == 403
        assert rejected.json()["code"] == "REAUTHENTICATION_FAILED"
        assert passphrase not in rejected.text

        accepted = await action(
            admin, roots["backup"], "backup.create", create_body, "web-backup"
        )
        assert accepted.status_code == 202, accepted.text
        queued = accepted.json()
        repeated = await action(
            admin, roots["backup"], "backup.create", create_body, "web-backup"
        )
        assert repeated.status_code == 202
        assert repeated.json()["backup_id"] == queued["backup_id"]

        backup_url = (
            roots["backup"] + "/queries/backup?backup_id=" + queued["backup_id"]
        )
        detail = await wait_for(admin, backup_url, {"READY", "INCONSISTENT", "FAILED"})
        assert detail["state"] == "READY", detail
        assert detail["database_total"] == detail["database_completed"] == 1
        assert detail["roster_start_digest"] == detail["roster_end_digest"]

        download = await admin.get(
            roots["backup"]
            + "/downloads/backup.archive?backup_id="
            + queued["backup_id"]
        )
        assert download.status_code == 200
        assert download.headers["cache-control"] == "no-store"
        assert download.headers["x-content-type-options"] == "nosniff"
        assert int(download.headers["content-length"]) == len(download.content)
        encrypted = tmp_path / "download.forest-backup"
        encrypted.write_bytes(download.content)
        restored = tmp_path / "download.zip"
        header = decrypt_file(encrypted, restored, passphrase)
        manifest = inspect_zip(restored)
        assert header["backup_id"] == manifest["backup_id"] == queued["backup_id"]
        assert manifest["roster"]["consistent"] is True

        reserved = await action(
            admin,
            roots["backup"],
            "import.reserve",
            {"expected_size": len(download.content), "current_password": PASSWORD},
            "web-external-import",
        )
        assert reserved.status_code == 201, reserved.text
        import_job = reserved.json()
        uploaded = await admin.post(
            roots["backup"]
            + "/uploads/import.artifact?import_id="
            + import_job["import_id"],
            content=download.content,
            headers={
                "content-type": "application/vnd.forest.backup",
                "idempotency-key": "web-external-artifact",
            },
        )
        assert uploaded.status_code == 200, uploaded.text
        assert uploaded.json()["state"] == "UPLOADED"
        finalized = await action(
            admin,
            roots["backup"],
            "import.finalize",
            {
                "import_id": import_job["import_id"],
                "recovery_passphrase": passphrase,
                "current_password": PASSWORD,
            },
            "web-external-finalize",
        )
        assert finalized.status_code == 202, finalized.text
        import_job = await wait_for(
            admin,
            roots["backup"]
            + "/queries/import?import_id="
            + import_job["import_id"],
            {"IMPORTED", "FAILED"},
        )
        assert import_job["state"] == "IMPORTED"
        assert import_job["backup_id"] == queued["backup_id"]
        assert "archive_key_ciphertext" not in import_job

        verification_response = await action(
            admin,
            roots["backup"],
            "verification.create",
            {
                "backup_id": queued["backup_id"],
                "current_password": PASSWORD,
                "recovery_passphrase": passphrase,
            },
            "web-backup-verification",
        )
        assert verification_response.status_code == 202, verification_response.text
        verification = verification_response.json()
        verification = await wait_for(
            admin,
            roots["backup"]
            + "/queries/verification?verification_id="
            + verification["verification_id"],
            {"VERIFIED", "FAILED"},
        )
        assert verification["state"] == "VERIFIED"
        assert verification["receipt_valid"]

        stages = await admin.get(roots["restore"] + "/queries/stages")
        assert stages.status_code == 200
        eligible = stages.json()["eligible_backups"]
        assert eligible[0]["backup_id"] == queued["backup_id"]
        assert eligible[0]["verification_id"] == verification["verification_id"]
        restore_response = await action(
            admin,
            roots["restore"],
            "stage.create",
            {
                "backup_id": queued["backup_id"],
                "verification_id": verification["verification_id"],
                "current_password": PASSWORD,
            },
            "web-restore-stage",
        )
        assert restore_response.status_code == 202, restore_response.text
        restore = restore_response.json()
        restore = await wait_for(
            admin,
            roots["restore"] + "/queries/stage?restore_id=" + restore["restore_id"],
            {"STAGED", "FAILED"},
        )
        assert restore["state"] == "STAGED", restore
        assert restore["verification_id"] == verification["verification_id"]
        assert restore["database_total"] == restore["database_completed"] == 1

        cleanup_plan = await action(
            admin,
            roots["storage"],
            "cleanup.plan",
            {"resource_kind": "RESTORE_STAGE", "resource_id": restore["restore_id"]},
            "web-restore-cleanup-plan",
        )
        assert cleanup_plan.status_code == 200, cleanup_plan.text
        plan = cleanup_plan.json()
        cleanup_response = await action(
            admin,
            roots["storage"],
            "cleanup.apply",
            {
                "plan_id": plan["plan_id"],
                "plan_digest": plan["plan_digest"],
                "current_password": PASSWORD,
            },
            "web-restore-cleanup",
        )
        assert cleanup_response.status_code == 202, cleanup_response.text
        cleanup = cleanup_response.json()
        cleanup = await wait_for(
            admin,
            roots["storage"]
            + "/queries/cleanup?cleanup_id="
            + cleanup["cleanup_id"],
            {"COMPLETE", "BLOCKED"},
        )
        assert cleanup["state"] == "COMPLETE", cleanup
        expired_restore = await admin.get(
            roots["restore"] + "/queries/stage?restore_id=" + restore["restore_id"]
        )
        assert expired_restore.json()["state"] == "EXPIRED"

        audit = await admin.get(
            roots["audit"] + "/queries/records?action=backup.create"
        )
        record = audit.json()["records"][0]
        assert record["details"] == {
            "backup_id": queued["backup_id"],
            "include_packages": False,
        }
        assert not record["details_redacted"]

        legacy = (
            "/management/v1/backups",
            "/management/v1/backup-imports",
            "/management/v1/backup-verifications",
            "/management/v1/restore-stages",
            "/management/v1/restore-activations",
            "/management/v1/storage-cleanups",
            "/management/v1/storage-generations",
        )
        for path in legacy:
            assert (await admin.get(path)).status_code == 404

        viewer_password = "viewer-backup-test-password"
        created = await create_account(
            admin,
            "backup-viewer",
            viewer_password,
            "viewer",
            PASSWORD,
            "backup-viewer",
        )
        assert created.status_code == 201, created.text
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as viewer:
            logged_in = await login_as(viewer, "backup-viewer", viewer_password)
            assert logged_in.status_code == 200
            viewer_roots = await module_roots(viewer)
            assert (
                await viewer.get(viewer_roots["backup"] + "/queries/backups")
            ).status_code == 403
            assert (
                await viewer.get(viewer_roots["restore"] + "/queries/stages")
            ).status_code == 403
            assert (
                await viewer.get(viewer_roots["storage"] + "/queries/generations")
            ).status_code == 403
