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
        assert name.startswith("forest_backup_web_") and len(name) == 50
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


@pytest.mark.postgres
async def test_web_backup_requires_reauthentication_and_streams_encrypted_artifact(
    tmp_path, backup_database
):
    state = tmp_path / "state"
    token = BootstrapState(state).initialize()
    app = create_app(WebSettings(state, ORIGIN))
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
        assert 'id="nav-backups"' in html.text and script.status_code == 200
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
                await viewer.get(
                    f"/management/v1/backups/{queued['backup_id']}/download"
                )
            ).status_code == 403
