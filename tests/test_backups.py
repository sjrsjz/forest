"""Durable deployment and Bot database backup integration."""

import json

import psycopg
import pytest
from test_bot_catalog import fixture as fixture

from forest.backup_artifact import decrypt_file, inspect_zip
from forest.errors import ForestError
from forest.management.backups import BackupManager
from forest.management.provision import BotProvisioner
from forest.packages import PackageCatalog, digest_json

pytestmark = pytest.mark.postgres


async def test_encrypted_backup_captures_deployment_and_bot_consistent_cuts(
    fixture, tmp_path, monkeypatch
):
    bot = await fixture.create()
    assert (await BotProvisioner(fixture.catalog).run_once())["stage"] == "READY"
    data_dir = tmp_path / "vault"
    manager = BackupManager(
        fixture.catalog,
        PackageCatalog(data_dir / "artifacts"),
        data_dir,
        fixture.catalog.managed_dsn,
    )
    request = {
        **fixture.auth,
        "expected_password_hash": "unused-test-hash",
        "passphrase": "backup recovery phrase for tests",
        "passphrase_digest": "test-passphrase-hmac",
        "include_packages": False,
        "idempotency_key": "backup-once",
    }
    queued = await manager.create(**request)
    assert queued["state"] == "QUEUED" and not queued["download_available"]
    assert await manager.create(**request) == queued
    with pytest.raises(ForestError) as conflict:
        await manager.create(**{**request, "include_packages": True})
    assert conflict.value.code == "IDEMPOTENCY_CONFLICT"

    competitor = BackupManager(
        fixture.catalog,
        PackageCatalog(data_dir / "artifacts"),
        data_dir,
        fixture.catalog.managed_dsn,
    )
    claimed = await manager._claim()
    assert claimed["backup_id"] == queued["backup_id"]
    assert await competitor._claim() is None
    ready = await manager._capture(claimed)
    await competitor.close()
    assert ready["error_code"] is None, ready["error_code"]
    assert ready["state"] == "READY" and ready["database_completed"] == 2, ready
    detail = await manager.read(backup_id=queued["backup_id"], **fixture.auth)
    assert detail["download_available"] and len(detail["databases"]) == 2
    assert {cut["database_scope"] for cut in detail["databases"]} == {"deployment", "bot"}
    bot_cut = next(cut for cut in detail["databases"] if cut["database_scope"] == "bot")
    assert bot_cut["bot_id"] == bot["bot_id"] and bot_cut["deletion_epoch"] == 0

    path, metadata = await manager.artifact(
        backup_id=queued["backup_id"], **fixture.auth
    )
    restored = tmp_path / "restored.zip"
    header = decrypt_file(path, restored, request["passphrase"])
    manifest = inspect_zip(restored)
    assert header["backup_id"] == queued["backup_id"] == manifest["backup_id"]
    assert manifest["activation"] == "ISOLATED_RESTORE_REQUIRED"
    assert {database["scope"] for database in manifest["databases"]} == {
        "deployment", "bot"
    }
    assert any(
        member["name"].startswith("bots/" + bot["bot_id"] + "/tables/")
        for member in manifest["members"]
    )
    assert metadata["ciphertext_digest"].startswith("sha256:")

    plan, secret = await fixture.plan(bot["bot_id"])
    encrypted_bytes = path.read_bytes()
    assert request["passphrase"].encode() not in encrypted_bytes
    assert secret["dsn"].encode() not in encrypted_bytes
    async with fixture.store.pool.connection() as conn:
        stored = await (await conn.execute(
            "SELECT archive_key_ciphertext,archive_header::text FROM backup_jobs WHERE backup_id=%s",
            (queued["backup_id"],),
        )).fetchone()
        audit = await (await conn.execute(
            "SELECT details FROM audit_records WHERE action='backup.create' AND request_id=%s",
            (queued["backup_id"],),
        )).fetchone()
    assert request["passphrase"] not in json.dumps(stored)
    assert plan["secret_ciphertext"] not in json.dumps(audit)

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        async with fixture.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "UPDATE backup_jobs SET updated_at=clock_timestamp() WHERE backup_id=%s",
                (queued["backup_id"],),
            )

    original_anchor = manager._roster_anchor
    anchor_reads = 0

    async def changed_anchor():
        nonlocal anchor_reads
        anchor_reads += 1
        anchor, anchor_digest = await original_anchor()
        if anchor_reads == 2:
            anchor = [*anchor, {
                "bot_id": "concurrent-directory-change", "revision": 1,
                "job_id": "not-captured", "plan_digest": "sha256:" + "0" * 64,
            }]
            anchor_digest = digest_json(anchor)
        return anchor, anchor_digest

    monkeypatch.setattr(manager, "_roster_anchor", changed_anchor)
    drift_request = {**request, "idempotency_key": "backup-with-directory-drift"}
    drift = await manager.create(**drift_request)
    inconsistent = await manager.run_once()
    assert inconsistent["backup_id"] == drift["backup_id"]
    assert inconsistent["state"] == "INCONSISTENT"
    assert inconsistent["roster_start_digest"] != inconsistent["roster_end_digest"]
    drift_path, _ = await manager.artifact(backup_id=drift["backup_id"], **fixture.auth)
    drift_zip = tmp_path / "drift.zip"
    decrypt_file(drift_path, drift_zip, drift_request["passphrase"])
    assert inspect_zip(drift_zip)["roster"]["consistent"] is False
    await manager.close()


async def test_backup_job_rechecks_current_session_before_admission(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    manager = BackupManager(
        fixture.catalog,
        PackageCatalog(data_dir / "artifacts"),
        data_dir,
        fixture.catalog.managed_dsn,
    )
    await fixture.store.revoke_session("test-session")
    with pytest.raises(ForestError) as rejected:
        await manager.create(
            **fixture.auth,
            expected_password_hash="unused-test-hash",
            passphrase="backup recovery phrase for tests",
            passphrase_digest="test-passphrase-hmac",
            include_packages=False,
            idempotency_key="revoked",
        )
    assert rejected.value.code == "SESSION_INVALID"
