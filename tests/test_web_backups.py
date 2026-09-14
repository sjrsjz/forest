"""Backup HTTP admission, progress, download, and role boundaries."""

import asyncio
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from test_management import (
    ORIGIN,
    PASSWORD,
    create_account,
    initialize,
    login,
    login_as,
)

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
                exists = await (await deployment.execute(
                    "SELECT to_regclass('restore_stage_targets') AS name"
                )).fetchone()
                if exists["name"] is not None:
                    rows = await (await deployment.execute(
                        "SELECT database_name FROM restore_stage_targets"
                    )).fetchall()
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
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_web_backup_requires_reauthentication_and_streams_encrypted_artifact(
    tmp_path, backup_database
):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN, managed_dsn=os.environ["FOREST_TEST_DSN"]))
    passphrase = "web backup recovery passphrase"
    body = {
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
        html = await admin.get("/")
        script = await admin.get("/static/backup-panel.js")
        assert 'id="nav-backups"' in html.text and 'id="storage-cleanup-form"' in html.text
        assert script.status_code == 200 and "cleanup-plan" in script.text
        empty = await admin.get("/management/v1/backups")
        assert empty.status_code == 200 and empty.json()["backups"] == []

        rejected = await admin.post(
            "/management/v1/backups",
            json={**body, "current_password": "wrong-password-long-enough"},
            headers={"idempotency-key": "wrong-password"},
        )
        assert rejected.status_code == 403
        assert rejected.json()["code"] == "REAUTHENTICATION_FAILED"
        assert passphrase not in rejected.text

        accepted = await admin.post(
            "/management/v1/backups", json=body,
            headers={"idempotency-key": "web-backup"},
        )
        assert accepted.status_code == 202, accepted.text
        queued = accepted.json()
        assert queued["state"] == "QUEUED" and not queued["download_available"]
        repeated = await admin.post(
            "/management/v1/backups", json=body,
            headers={"idempotency-key": "web-backup"},
        )
        assert repeated.status_code == 202 and repeated.json() == queued

        detail = queued
        for _ in range(100):
            detail_response = await admin.get(
                f"/management/v1/backups/{queued['backup_id']}"
            )
            assert detail_response.status_code == 200, detail_response.text
            detail = detail_response.json()
            if detail["state"] in {"READY", "INCONSISTENT", "FAILED"}:
                break
            await asyncio.sleep(0.05)
        assert detail["state"] == "READY", detail
        assert detail["database_total"] == detail["database_completed"] == 1
        assert detail["roster_start_digest"] == detail["roster_end_digest"]

        download = await admin.get(
            f"/management/v1/backups/{queued['backup_id']}/download"
        )
        assert download.status_code == 200
        assert download.headers["cache-control"] == "no-store"
        encrypted = tmp_path / "download.forest-backup"
        encrypted.write_bytes(download.content)
        restored = tmp_path / "download.zip"
        header = decrypt_file(encrypted, restored, passphrase)
        manifest = inspect_zip(restored)
        assert header["backup_id"] == manifest["backup_id"] == queued["backup_id"]
        assert manifest["roster"]["consistent"] is True

        verification_response = await admin.post(
            f"/management/v1/backups/{queued['backup_id']}/verifications",
            json={
                "current_password": PASSWORD,
                "recovery_passphrase": passphrase,
            },
            headers={"idempotency-key": "web-backup-verification"},
        )
        assert verification_response.status_code == 202, verification_response.text
        verification = verification_response.json()
        assert verification["state"] == "QUEUED"
        for _ in range(100):
            response = await admin.get(
                f"/management/v1/backup-verifications/{verification['verification_id']}"
            )
            assert response.status_code == 200, response.text
            verification = response.json()
            if verification["state"] in {"VERIFIED", "FAILED"}:
                break
            await asyncio.sleep(0.05)
        assert verification["state"] == "VERIFIED" and verification["receipt_valid"]
        listed_verifications = await admin.get("/management/v1/backup-verifications")
        assert listed_verifications.status_code == 200
        assert listed_verifications.json()["verifications"][0] == verification
        verification_audit = await admin.get(
            "/management/v1/audit?action=backup.verify.completed"
        )
        verification_record = verification_audit.json()["records"][0]
        assert verification_record["details"] == {
            "verification_id": verification["verification_id"],
            "backup_id": queued["backup_id"],
            "manifest_digest": verification["manifest_digest"],
            "database_total": 1,
        }
        assert not verification_record["details_redacted"]

        restore_response = await admin.post(
            f"/management/v1/backups/{queued['backup_id']}/restore-stages",
            json={
                "current_password": PASSWORD,
                "verification_id": verification["verification_id"],
            },
            headers={"idempotency-key": "web-restore-stage"},
        )
        assert restore_response.status_code == 202, restore_response.text
        restore = restore_response.json()
        for _ in range(160):
            response = await admin.get(
                f"/management/v1/restore-stages/{restore['restore_id']}"
            )
            assert response.status_code == 200, response.text
            restore = response.json()
            if restore["state"] in {"STAGED", "FAILED"}:
                break
            await asyncio.sleep(0.05)
        assert restore["state"] == "STAGED", restore
        assert restore["verification_id"] == verification["verification_id"]
        assert restore["database_total"] == restore["database_completed"] == 1
        assert restore["targets"][0]["database_scope"] == "deployment"
        assert restore["targets"][0]["state"] == "STAGED"

        restore_audit = await admin.get(
            "/management/v1/audit?action=restore.stage.completed"
        )
        restore_record = restore_audit.json()["records"][0]
        assert restore_record["details"] == {
            "restore_id": restore["restore_id"],
            "backup_id": queued["backup_id"],
            "verification_id": verification["verification_id"],
            "database_total": 1,
            "manifest_digest": restore["manifest_digest"],
        }
        assert not restore_record["details_redacted"]

        restore_cleanup_plan = await admin.post(
            f"/management/v1/restore-stages/{restore['restore_id']}/cleanup-plan", json={}
        )
        assert restore_cleanup_plan.status_code == 200, restore_cleanup_plan.text
        restore_cleanup_plan = restore_cleanup_plan.json()
        restore_cleanup = await admin.post(
            "/management/v1/storage-cleanups",
            json={
                "plan_id": restore_cleanup_plan["plan_id"],
                "plan_digest": restore_cleanup_plan["plan_digest"],
                "current_password": PASSWORD,
            },
            headers={"idempotency-key": "web-restore-cleanup"},
        )
        assert restore_cleanup.status_code == 202, restore_cleanup.text
        restore_cleanup = restore_cleanup.json()
        expired_restore = await admin.get(
            f"/management/v1/restore-stages/{restore['restore_id']}"
        )
        assert expired_restore.json()["state"] == "EXPIRED"
        for _ in range(100):
            cleanup_status = await admin.get(
                f"/management/v1/storage-cleanups/{restore_cleanup['cleanup_id']}"
            )
            assert cleanup_status.status_code == 200
            if cleanup_status.json()["state"] in {"COMPLETE", "BLOCKED"}:
                break
            await asyncio.sleep(0.05)
        assert cleanup_status.json()["state"] == "COMPLETE", cleanup_status.text

        backup_cleanup_plan = await admin.post(
            f"/management/v1/backups/{queued['backup_id']}/cleanup-plan", json={}
        )
        assert backup_cleanup_plan.status_code == 200
        backup_cleanup_plan = backup_cleanup_plan.json()
        backup_cleanup = await admin.post(
            "/management/v1/storage-cleanups",
            json={
                "plan_id": backup_cleanup_plan["plan_id"],
                "plan_digest": backup_cleanup_plan["plan_digest"],
                "current_password": PASSWORD,
            },
            headers={"idempotency-key": "web-backup-cleanup"},
        )
        assert backup_cleanup.status_code == 202, backup_cleanup.text
        assert (await admin.get(
            f"/management/v1/backups/{queued['backup_id']}/download"
        )).status_code == 404

        audit = await admin.get("/management/v1/audit?action=backup.create")
        record = audit.json()["records"][0]
        assert record["details"] == {
            "backup_id": queued["backup_id"], "include_packages": False
        }
        assert not record["details_redacted"]

        viewer_password = "viewer-backup-test-password"
        created = await create_account(
            admin, "backup-viewer", viewer_password, "viewer", PASSWORD, "backup-viewer"
        )
        assert created.status_code == 201, created.text
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=ORIGIN,
            headers={"origin": ORIGIN},
        ) as viewer:
            assert (await login_as(viewer, "backup-viewer", viewer_password)).status_code == 200
            assert (await viewer.get("/management/v1/backups")).status_code == 403
            assert (
                await viewer.get("/management/v1/backup-verifications")
            ).status_code == 403
            assert (
                await viewer.get(
                    f"/management/v1/backups/{queued['backup_id']}/download"
                )
            ).status_code == 403
