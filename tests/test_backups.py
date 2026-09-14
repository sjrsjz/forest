"""Durable deployment and Bot database backup integration."""

import asyncio
import json
import threading

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from test_bot_catalog import fixture as fixture

from forest.backup_artifact import decrypt_file, inspect_zip
from forest.errors import ForestError
from forest.management.backups import BackupManager
from forest.management.provision import BotProvisioner
from forest.management.restores import RestoreManager
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
    assert path.name == queued["backup_id"] + ".1.forest-backup"
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


async def test_backup_cancels_capture_when_lease_renewal_fails(
    fixture, tmp_path, monkeypatch
):
    data_dir = tmp_path / "vault"
    manager = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocked_capture(job):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async def lose_lease(job):
        await started.wait()
        raise ForestError("BACKUP_LEASE_LOST", "lease lost", 409)

    monkeypatch.setattr(manager, "_capture_owned", blocked_capture)
    monkeypatch.setattr(manager, "_renew_lease", lose_lease)
    try:
        with pytest.raises(ForestError) as lost:
            await manager._capture({"backup_id": "test", "owner_epoch": 1})
        assert lost.value.code == "BACKUP_LEASE_LOST"
        assert cancelled.is_set()
    finally:
        await manager.close()


async def test_backup_joins_owned_thread_io_before_propagating_cancellation(
    fixture, tmp_path
):
    data_dir = tmp_path / "vault"
    manager = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    entered = threading.Event()
    release = threading.Event()

    def blocked_io():
        entered.set()
        assert release.wait(10)
        return "done"

    task = asyncio.create_task(manager._run_blocking(blocked_io))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await manager.close()


async def test_restore_stage_rebuilds_unregistered_databases_and_quarantines_bot(
    fixture, tmp_path
):
    bot = await fixture.create()
    assert (await BotProvisioner(fixture.catalog).run_once())["stage"] == "READY"
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    passphrase = "isolated restore passphrase for tests"
    backup = await backups.create(
        **fixture.auth,
        expected_password_hash="unused-test-hash",
        passphrase=passphrase,
        passphrase_digest="restore-passphrase-hmac",
        include_packages=False,
        idempotency_key="restore-source",
    )
    assert (await backups.run_once())["state"] == "READY"
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    target_names = []
    try:
        with pytest.raises(ForestError) as wrong:
            await restores.create(
                backup_id=backup["backup_id"], **fixture.auth,
                expected_password_hash="unused-test-hash",
                passphrase="incorrect restore passphrase",
                passphrase_digest="wrong-hmac",
                idempotency_key="wrong-restore",
            )
        assert wrong.value.code == "BACKUP_PASSPHRASE_INVALID"
        queued = await restores.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            passphrase=passphrase,
            passphrase_digest="restore-passphrase-hmac",
            idempotency_key="restore-once",
        )
        assert queued["state"] == "QUEUED"
        assert await restores.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            passphrase=passphrase,
            passphrase_digest="restore-passphrase-hmac",
            idempotency_key="restore-once",
        ) == queued
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            async with fixture.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    "UPDATE restore_stage_targets SET state='STAGED' "
                    "WHERE restore_id=%s AND ordinal=0", (queued["restore_id"],)
                )
        staged = await restores.run_once()
        assert staged["error_code"] is None, staged["error_code"]
        assert staged["state"] == "STAGED", staged
        detail = await restores.read(restore_id=queued["restore_id"], **fixture.auth)
        assert detail["database_total"] == detail["database_completed"] == 2
        assert all(target["state"] == "STAGED" for target in detail["targets"])
        target_names = [target["database_name"] for target in detail["targets"]]
        admin_parts = conninfo_to_dict(fixture.admin_dsn)
        for target in detail["targets"]:
            target_dsn = make_conninfo(**{**admin_parts, "dbname": target["database_name"]})
            async with await psycopg.AsyncConnection.connect(
                target_dsn, row_factory=psycopg.rows.dict_row
            ) as conn:
                if target["database_scope"] == "deployment":
                    identity = await (await conn.execute(
                        "SELECT setup_id,plan_digest FROM web_setup_state WHERE singleton"
                    )).fetchone()
                    assert identity is not None
                else:
                    identity = await (await conn.execute(
                        "SELECT i.bot_id,r.status,r.data_admission,r.control_admission "
                        "FROM forest_bot_identity i JOIN bot_runtime_state r USING(bot_id) "
                        "WHERE i.singleton"
                    )).fetchone()
                    assert identity == {
                        "bot_id": bot["bot_id"], "status": "SEALED",
                        "data_admission": False, "control_admission": False,
                    }
            public_access = await (await fixture.admin.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_database d,aclexplode(d.datacl) a "
                "WHERE d.datname=%s AND a.grantee=0 AND a.privilege_type='CONNECT') AS access",
                (target["database_name"],),
            )).fetchone()
            assert not public_access["access"]
        deployment_target = next(
            target for target in detail["targets"] if target["database_scope"] == "deployment"
        )
        deployment_dsn = make_conninfo(
            **{**admin_parts, "dbname": deployment_target["database_name"]}
        )
        async with await psycopg.AsyncConnection.connect(deployment_dsn) as conn:
            await conn.execute("CREATE TABLE forest_fk_parent(id integer PRIMARY KEY)")
            await conn.execute(
                "CREATE TABLE forest_fk_child(parent_id integer REFERENCES forest_fk_parent(id))"
            )
            await conn.execute("SET session_replication_role='replica'")
            await conn.execute("INSERT INTO forest_fk_child(parent_id) VALUES (42)")
            await conn.execute("SET session_replication_role='origin'")
            with pytest.raises(ForestError) as broken_fk:
                await restores._validate_foreign_keys(conn)
            assert broken_fk.value.code == "RESTORE_FOREIGN_KEY_VIOLATION"
            await conn.execute("DROP TABLE forest_fk_child,forest_fk_parent")
        async with fixture.store.pool.connection() as conn:
            registered = await (await conn.execute(
                "SELECT count(*) AS n FROM bot_storage_plans WHERE database_name=ANY(%s)",
                (target_names,),
            )).fetchone()
            completion_audit = await (await conn.execute(
                "SELECT details FROM audit_records WHERE action='restore.stage.completed' "
                "AND request_id=%s", (queued["restore_id"],)
            )).fetchone()
        assert registered["n"] == 0
        assert completion_audit["details"]["manifest_digest"] == detail["manifest_digest"]
    finally:
        if not target_names:
            async with fixture.store.pool.connection() as conn:
                rows = await (await conn.execute(
                    "SELECT database_name FROM restore_stage_targets"
                )).fetchall()
                target_names = [row["database_name"] for row in rows]
        for name in target_names:
            assert name.startswith("forest_restore_") and len(name) == 40
            await fixture.admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )
        await restores.close()
        await backups.close()


async def test_restore_stage_resumes_after_completed_target(fixture, tmp_path, monkeypatch):
    await fixture.create()
    assert (await BotProvisioner(fixture.catalog).run_once())["stage"] == "READY"
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    passphrase = "resume restore passphrase for tests"
    backup = await backups.create(
        **fixture.auth,
        expected_password_hash="unused-test-hash",
        passphrase=passphrase,
        passphrase_digest="resume-passphrase-hmac",
        include_packages=False,
        idempotency_key="resume-source",
    )
    assert (await backups.run_once())["state"] == "READY"
    first = RestoreManager(fixture.catalog, backups, data_dir)
    second = RestoreManager(fixture.catalog, backups, data_dir)
    target_names = []
    try:
        queued = await first.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            passphrase=passphrase,
            passphrase_digest="resume-passphrase-hmac",
            idempotency_key="resume-restore",
        )
        claimed = await first._claim()
        assert claimed is not None
        original_heartbeat = first._heartbeat

        async def interrupt_after_first_target(job, *, state=None, completed=None):
            await original_heartbeat(job, state=state, completed=completed)
            if completed == 1:
                raise asyncio.CancelledError

        monkeypatch.setattr(first, "_heartbeat", interrupt_after_first_target)
        with pytest.raises(asyncio.CancelledError):
            await first._stage(claimed)
        interrupted = await first.read(restore_id=queued["restore_id"], **fixture.auth)
        assert interrupted["state"] == "RESTORING"
        assert interrupted["database_completed"] == 1
        assert [target["state"] for target in interrupted["targets"]].count("STAGED") == 1

        async with fixture.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "UPDATE restore_stage_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE restore_id=%s", (queued["restore_id"],)
            )
        resumed = await second.run_once()
        assert resumed["state"] == "STAGED" and resumed["owner_epoch"] == 2
        detail = await second.read(restore_id=queued["restore_id"], **fixture.auth)
        assert detail["database_total"] == detail["database_completed"] == 2
        assert all(target["state"] == "STAGED" for target in detail["targets"])
        target_names = [target["database_name"] for target in detail["targets"]]
    finally:
        if not target_names:
            async with fixture.store.pool.connection() as conn:
                rows = await (await conn.execute(
                    "SELECT database_name FROM restore_stage_targets"
                )).fetchall()
                target_names = [row["database_name"] for row in rows]
        for name in target_names:
            assert name.startswith("forest_restore_") and len(name) == 40
            await fixture.admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )
        await first.close()
        await second.close()
        await backups.close()


async def test_restore_stage_cancels_database_work_when_lease_renewal_fails(
    fixture, tmp_path, monkeypatch
):
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocked_work(job):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async def lose_lease(job):
        await started.wait()
        raise ForestError("RESTORE_LEASE_LOST", "lease lost", 409)

    monkeypatch.setattr(restores, "_stage_owned", blocked_work)
    monkeypatch.setattr(restores, "_renew_lease", lose_lease)
    try:
        with pytest.raises(ForestError) as lost:
            await restores._stage({"restore_id": "test", "owner_epoch": 1})
        assert lost.value.code == "RESTORE_LEASE_LOST"
        assert cancelled.is_set()
    finally:
        await restores.close()
        await backups.close()
