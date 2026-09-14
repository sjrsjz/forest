"""Durable deployment and Bot database backup integration."""

import asyncio
import json
import os
import threading
import zipfile
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.types.json import Jsonb
from test_bot_catalog import fixture as fixture

from forest.backup_artifact import (
    decrypt_file,
    encrypt_file,
    inspect_zip,
    prepare_header,
)
from forest.errors import ForestError
from forest.management.activations import RestoreActivationManager
from forest.management.backup_imports import BackupImportManager
from forest.management.backups import BackupManager
from forest.management.cleanup import StorageCleanupManager
from forest.management.provision import BotProvisioner
from forest.management.restores import RestoreManager
from forest.management.verifications import BackupVerificationManager
from forest.packages import PackageCatalog, digest_json

pytestmark = pytest.mark.postgres


async def _file_chunks(path, size=65536):
    with path.open("rb") as stream:
        while chunk := stream.read(size):
            yield chunk


async def _deployment_backup(fixture, data_dir, *, key):
    manager = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    queued = await manager.create(
        **fixture.auth, expected_password_hash="unused-test-hash",
        passphrase="cleanup recovery passphrase for tests",
        passphrase_digest="cleanup-passphrase-hmac", include_packages=False,
        idempotency_key=key,
    )
    ready = await manager.run_once()
    assert ready["state"] == "READY"
    return manager, queued


async def _verify_backup(
    fixture, backups, restores, data_dir, *, backup_id, passphrase, passphrase_digest, key,
):
    manager = BackupVerificationManager(fixture.catalog, backups, restores, data_dir)
    queued = await manager.create(
        backup_id=backup_id, **fixture.auth,
        expected_password_hash="unused-test-hash", passphrase=passphrase,
        passphrase_digest=passphrase_digest, idempotency_key=key,
    )
    assert queued["state"] == "QUEUED"
    verified = await manager.run_once()
    assert verified["state"] == "VERIFIED" and verified["receipt_valid"]
    return manager, verified


async def test_external_backup_import_streams_validates_and_deduplicates(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups, queued = await _deployment_backup(
        fixture, data_dir, key="backup-import-source"
    )
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    imports = BackupImportManager(fixture.catalog, backups, restores, data_dir)
    path, metadata = await backups.artifact(backup_id=queued["backup_id"], **fixture.auth)
    try:
        reserved = await imports.reserve(
            **fixture.auth, expected_password_hash="unused-test-hash",
            expected_size=path.stat().st_size, idempotency_key="external-import-once",
        )
        assert reserved["state"] == "RESERVED" and reserved["upload_available"]
        uploaded = await imports.upload(
            import_id=reserved["import_id"], **fixture.auth, chunks=_file_chunks(path)
        )
        assert uploaded["state"] == "UPLOADED"
        assert uploaded["ciphertext_digest"] == metadata["ciphertext_digest"]
        with pytest.raises(ForestError) as invalid:
            await imports.queue(
                import_id=reserved["import_id"], **fixture.auth,
                expected_password_hash="unused-test-hash",
                passphrase="wrong recovery passphrase for import",
                passphrase_digest="wrong-import-passphrase-digest",
            )
        assert invalid.value.code == "BACKUP_PASSPHRASE_INVALID"
        assert (await imports.read(import_id=reserved["import_id"], **fixture.auth))["state"] == "UPLOADED"

        finalized = await imports.queue(
            import_id=reserved["import_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            passphrase="cleanup recovery passphrase for tests",
            passphrase_digest="correct-import-passphrase-digest",
        )
        assert finalized["state"] == "QUEUED" and "archive_key_ciphertext" not in finalized
        imported = await imports.run_once()
        assert imported["state"] == "IMPORTED"
        assert imported["backup_id"] == queued["backup_id"]
        assert imported["database_total"] == 1
        assert not (imports.import_dir / f".job-{reserved['import_id']}-1.forest-backup-upload").exists()
        async with fixture.store.pool.connection() as conn:
            audit = await (await conn.execute(
                "SELECT details FROM audit_records WHERE action='backup.import.completed' "
                "AND request_id=%s", (reserved["import_id"],)
            )).fetchone()
            cleanup_receipt = await (await conn.execute(
                "SELECT details FROM backup_import_cleanup_receipts WHERE import_id=%s",
                (reserved["import_id"],),
            )).fetchone()
        assert audit["details"]["deduplicated"] is True
        assert cleanup_receipt["details"]["upload_result"] in {"DELETED", "ABSENT"}
    finally:
        await imports.close()
        await restores.close()
        await backups.close()


async def test_external_backup_import_publishes_new_standard_backup(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups, queued = await _deployment_backup(
        fixture, data_dir, key="backup-import-new-source"
    )
    source, _ = await backups.artifact(backup_id=queued["backup_id"], **fixture.auth)
    old_plain = tmp_path / "old.zip"
    decrypt_file(source, old_plain, "cleanup recovery passphrase for tests")
    new_id = str(uuid4())
    new_plain = tmp_path / "new.zip"
    with zipfile.ZipFile(old_plain) as incoming, zipfile.ZipFile(
        new_plain, "w", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as outgoing:
        for info in incoming.infolist():
            value = incoming.read(info)
            if info.filename == "manifest.json":
                manifest = json.loads(value)
                manifest["backup_id"] = new_id
                value = json.dumps(
                    manifest, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), allow_nan=False,
                ).encode("utf-8")
            outgoing.writestr(info.filename, value)
    passphrase = "external import recovery passphrase"
    header, data_key = prepare_header(new_id, passphrase)
    external = tmp_path / "external.forest-backup"
    encrypt_file(new_plain, external, header, data_key)

    restores = RestoreManager(fixture.catalog, backups, data_dir)
    imports = BackupImportManager(fixture.catalog, backups, restores, data_dir)
    verification = None
    try:
        reserved = await imports.reserve(
            **fixture.auth, expected_password_hash="unused-test-hash",
            expected_size=external.stat().st_size, idempotency_key="external-import-new",
        )
        await imports.upload(
            import_id=reserved["import_id"], **fixture.auth, chunks=_file_chunks(external)
        )
        await imports.queue(
            import_id=reserved["import_id"], **fixture.auth,
            expected_password_hash="unused-test-hash", passphrase=passphrase,
            passphrase_digest="external-import-passphrase-digest",
        )
        imported = await imports.run_once()
        assert imported["state"] == "IMPORTED" and imported["backup_id"] == new_id
        detail = await backups.read(backup_id=new_id, **fixture.auth)
        assert detail["state"] == "READY" and detail["download_available"]
        assert len(detail["databases"]) == 1
        verification, receipt = await _verify_backup(
            fixture, backups, restores, data_dir, backup_id=new_id,
            passphrase=passphrase, passphrase_digest="external-verification-passphrase",
            key="verify-external-import",
        )
        assert receipt["backup_id"] == new_id and receipt["receipt_valid"]
    finally:
        if verification is not None:
            await verification.close()
        await imports.close()
        await restores.close()
        await backups.close()


async def test_backup_import_rejects_partial_upload_and_cancels_on_lease_loss(
    fixture, tmp_path, monkeypatch
):
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    imports = BackupImportManager(fixture.catalog, backups, restores, data_dir)

    async def short_body():
        yield b"short"

    try:
        first = await imports.reserve(
            **fixture.auth, expected_password_hash="unused-test-hash",
            expected_size=10, idempotency_key="partial-import",
        )
        with pytest.raises(ForestError) as mismatch:
            await imports.upload(
                import_id=first["import_id"], **fixture.auth, chunks=short_body()
            )
        assert mismatch.value.code == "BACKUP_UPLOAD_SIZE_MISMATCH"
        assert (await imports.read(import_id=first["import_id"], **fixture.auth))["state"] == "RESERVED"

        second = await imports.reserve(
            **fixture.auth, expected_password_hash="unused-test-hash",
            expected_size=10, idempotency_key="lease-loss-import",
        )
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def blocked_body():
            started.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
            yield b"unreachable"

        async def lose_lease(job):
            await started.wait()
            raise ForestError("BACKUP_IMPORT_LEASE_LOST", "lease lost", 409)

        monkeypatch.setattr(imports, "_renew_lease", lose_lease)
        with pytest.raises(ForestError) as lost:
            await imports.upload(
                import_id=second["import_id"], **fixture.auth, chunks=blocked_body()
            )
        assert lost.value.code == "BACKUP_IMPORT_LEASE_LOST" and cancelled.is_set()
        assert (await imports.read(import_id=second["import_id"], **fixture.auth))["state"] == "RESERVED"
        assert list(imports.import_dir.iterdir()) == []
    finally:
        await imports.close()
        await restores.close()
        await backups.close()


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
    fixture, tmp_path, monkeypatch
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
    activations = RestoreActivationManager(fixture.catalog)
    cleanup = StorageCleanupManager(fixture.catalog, backups)
    verifications = BackupVerificationManager(fixture.catalog, backups, restores, data_dir)
    target_names = []
    try:
        with pytest.raises(ForestError) as wrong:
            await verifications.create(
                backup_id=backup["backup_id"], **fixture.auth,
                expected_password_hash="unused-test-hash",
                passphrase="incorrect restore passphrase",
                passphrase_digest="wrong-hmac",
                idempotency_key="wrong-verification",
            )
        assert wrong.value.code == "BACKUP_PASSPHRASE_INVALID"
        before = {
            row["datname"] for row in await (
                await fixture.admin.execute("SELECT datname FROM pg_database")
            ).fetchall()
        }
        verification = await verifications.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash", passphrase=passphrase,
            passphrase_digest="restore-passphrase-hmac",
            idempotency_key="verify-restore-source",
        )
        assert await verifications.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash", passphrase=passphrase,
            passphrase_digest="restore-passphrase-hmac",
            idempotency_key="verify-restore-source",
        ) == verification
        verified = await verifications.run_once()
        assert verified["state"] == "VERIFIED" and verified["receipt_valid"]
        after = {
            row["datname"] for row in await (
                await fixture.admin.execute("SELECT datname FROM pg_database")
            ).fetchall()
        }
        assert after == before
        with pytest.raises(ForestError) as unverified:
            await restores.create(
                backup_id=backup["backup_id"], **fixture.auth,
                expected_password_hash="unused-test-hash",
                verification_id="missing-verification",
                idempotency_key="unverified-restore",
            )
        assert unverified.value.code == "BACKUP_VERIFICATION_REQUIRED"
        queued = await restores.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            verification_id=verified["verification_id"],
            idempotency_key="restore-once",
        )
        assert queued["state"] == "QUEUED"
        assert queued["verification_id"] == verified["verification_id"]
        assert await restores.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            verification_id=verified["verification_id"],
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
        current_storage = await fixture.catalog.resolve_storage(bot["bot_id"])
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
                    owner = await (await conn.execute(
                        "SELECT r.rolname FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba "
                        "WHERE d.datname=current_database()"
                    )).fetchone()
                    assert owner["rolname"] == current_storage["role_name"]
                    restricted_dsn = make_conninfo(
                        current_storage["dsn"], dbname=target["database_name"]
                    )
                    async with await psycopg.AsyncConnection.connect(restricted_dsn) as restricted:
                        assert await (await restricted.execute(
                            "SELECT bot_id FROM forest_bot_identity"
                        )).fetchone()
            public_access = await (await fixture.admin.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_database d,aclexplode(d.datacl) a "
                "WHERE d.datname=%s AND a.grantee=0 AND a.privilege_type='CONNECT') AS access",
                (target["database_name"],),
            )).fetchone()
            assert not public_access["access"]
        bot_target = next(
            target for target in detail["targets"] if target["database_scope"] == "bot"
        )
        activation_plan = await activations.plan(
            restore_id=queued["restore_id"], target_ordinal=bot_target["ordinal"],
            **fixture.auth,
        )
        assert activation_plan["before"]["storage_generation"] == 1
        assert activation_plan["after"] == {
            "storage_generation": 2,
            "database_name": bot_target["database_name"],
            "status": "ACTIVE",
        }
        activation = await activations.apply(
            plan_id=activation_plan["plan_id"],
            plan_digest=activation_plan["plan_digest"],
            expected_password_hash="unused-test-hash",
            idempotency_key="activate-restore-once", **fixture.auth,
        )
        assert activation["state"] == "QUEUED"
        switch = activations._switch_head

        async def fail_after_fence(job):
            raise OSError("synthetic directory outage after durable fence")

        monkeypatch.setattr(activations, "_switch_head", fail_after_fence)
        blocked = await activations.run_once()
        assert blocked["state"] == "BLOCKED" and blocked["error_code"] == "ACTIVATION_FAILED"
        async with fixture.store.pool.connection() as conn:
            unchanged_head = await (await conn.execute(
                "SELECT storage_generation FROM bot_storage_heads WHERE bot_id=%s",
                (bot["bot_id"],),
            )).fetchone()
        assert unchanged_head["storage_generation"] == 1
        monkeypatch.setattr(activations, "_switch_head", switch)
        retried = await activations.retry(
            activation_id=activation["activation_id"],
            expected_password_hash="unused-test-hash",
            idempotency_key="retry-activation-after-fence", **fixture.auth,
        )
        assert retried["state"] == "QUEUED"
        activated = await activations.run_once()
        assert activated["state"] == "COMPLETE" and activated["error_code"] is None, activated
        selected = await fixture.catalog.resolve_storage(bot["bot_id"])
        assert selected["storage_generation"] == 2
        assert selected["database_name"] == bot_target["database_name"]
        async with await psycopg.AsyncConnection.connect(
            current_storage["dsn"], row_factory=psycopg.rows.dict_row
        ) as old_conn:
            old_runtime = await (await old_conn.execute(
                "SELECT status,storage_generation,data_admission,control_admission "
                "FROM bot_runtime_state"
            )).fetchone()
        assert old_runtime == {
            "status": "SEALED", "storage_generation": 2,
            "data_admission": False, "control_admission": False,
        }
        async with await psycopg.AsyncConnection.connect(
            selected["dsn"], row_factory=psycopg.rows.dict_row
        ) as new_conn:
            new_runtime = await (await new_conn.execute(
                "SELECT status,storage_generation,data_admission,control_admission "
                "FROM bot_runtime_state"
            )).fetchone()
        assert new_runtime == {
            "status": "ACTIVE", "storage_generation": 2,
            "data_admission": True, "control_admission": True,
        }
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
            registered_generation = await (await conn.execute(
                "SELECT count(*) AS n FROM bot_storage_generations "
                "WHERE database_name=ANY(%s)", (target_names,)
            )).fetchone()
        assert registered["n"] == 0
        assert registered_generation["n"] == 1
        assert completion_audit["details"]["manifest_digest"] == detail["manifest_digest"]
        assert completion_audit["details"]["verification_id"] == verified["verification_id"]
        cleanup_plan = await cleanup.plan(
            kind="RESTORE_STAGE", resource_id=queued["restore_id"], **fixture.auth
        )
        cleanup_job = await cleanup.apply(
            plan_id=cleanup_plan["plan_id"], plan_digest=cleanup_plan["plan_digest"],
            expected_password_hash="unused-test-hash", idempotency_key="cleanup-activated-stage",
            **fixture.auth,
        )
        assert cleanup_job["state"] == "QUEUED"
        cleaned = await cleanup.run_once()
        assert cleaned["state"] == "COMPLETE"
        databases = {
            row["datname"] for row in await (
                await fixture.admin.execute(
                    "SELECT datname FROM pg_database WHERE datname=ANY(%s)", (target_names,)
                )
            ).fetchall()
        }
        assert databases == {bot_target["database_name"]}

        generations = await cleanup.list_generations(**fixture.auth)
        assert [row["storage_generation"] for row in generations] == [2, 1]
        assert generations[0]["state"] == "CURRENT"
        assert generations[1]["state"] == "SUPERSEDED"
        with pytest.raises(ForestError) as current_generation:
            await cleanup.plan(
                kind="BOT_STORAGE_GENERATION",
                resource_id=f"{bot['bot_id']}:2", **fixture.auth,
            )
        assert current_generation.value.code == "STORAGE_GENERATION_CURRENT"

        old_database = current_storage["database_name"]
        old_marker = await (await fixture.admin.execute(
            "SELECT shobj_description(oid,'pg_database') AS marker "
            "FROM pg_database WHERE datname=%s", (old_database,)
        )).fetchone()
        generation_plan = await cleanup.plan(
            kind="BOT_STORAGE_GENERATION", resource_id=f"{bot['bot_id']}:1",
            **fixture.auth,
        )
        generation_cleanup = await cleanup.apply(
            plan_id=generation_plan["plan_id"], plan_digest=generation_plan["plan_digest"],
            expected_password_hash="unused-test-hash",
            idempotency_key="retire-old-storage-generation", **fixture.auth,
        )
        await fixture.admin.execute(
            sql.SQL("COMMENT ON DATABASE {} IS 'foreign-owner'").format(
                sql.Identifier(old_database)
            )
        )
        generation_blocked = await cleanup.run_once()
        assert generation_blocked["cleanup_id"] == generation_cleanup["cleanup_id"]
        assert generation_blocked["state"] == "BLOCKED"
        assert generation_blocked["error_code"] == "STORAGE_GENERATION_OWNERSHIP_CONFLICT"
        assert await (await fixture.admin.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (old_database,)
        )).fetchone()
        await fixture.admin.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                sql.Identifier(old_database), sql.Literal(old_marker["marker"])
            )
        )
        assert (await cleanup.retry(
            cleanup_id=generation_cleanup["cleanup_id"],
            expected_password_hash="unused-test-hash",
            idempotency_key="retry-old-storage-generation", **fixture.auth,
        ))["state"] == "QUEUED"
        retired = await cleanup.run_once()
        assert retired["state"] == "COMPLETE"
        assert await (await fixture.admin.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (old_database,)
        )).fetchone() is None
        generations = await cleanup.list_generations(**fixture.auth)
        assert generations[1]["state"] == "RETIRED"
        assert generations[1]["cleanup_state"] == "COMPLETE"
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
        await verifications.close()
        await cleanup.close()
        await activations.close()
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
    verifications, verified = await _verify_backup(
        fixture, backups, first, data_dir, backup_id=backup["backup_id"],
        passphrase=passphrase, passphrase_digest="resume-passphrase-hmac",
        key="verify-resume-source",
    )
    target_names = []
    try:
        queued = await first.create(
            backup_id=backup["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            verification_id=verified["verification_id"],
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
        await verifications.close()
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


async def test_backup_verification_cancels_read_only_work_when_lease_renewal_fails(
    fixture, tmp_path, monkeypatch
):
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    verifications = BackupVerificationManager(fixture.catalog, backups, restores, data_dir)
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
        raise ForestError("VERIFICATION_LEASE_LOST", "lease lost", 409)

    monkeypatch.setattr(verifications, "_verify_owned", blocked_work)
    monkeypatch.setattr(verifications, "_renew", lose_lease)
    try:
        with pytest.raises(ForestError) as lost:
            await verifications._verify({"verification_id": "test", "owner_epoch": 1})
        assert lost.value.code == "VERIFICATION_LEASE_LOST"
        assert cancelled.is_set()
    finally:
        await verifications.close()
        await restores.close()
        await backups.close()


async def test_backup_cleanup_tombstones_before_exact_file_deletion(fixture, tmp_path):
    backups, queued = await _deployment_backup(
        fixture, tmp_path / "vault", key="cleanup-backup-source"
    )
    cleanup = StorageCleanupManager(fixture.catalog, backups)
    path, _ = await backups.artifact(backup_id=queued["backup_id"], **fixture.auth)
    assert path.is_file()
    try:
        async with fixture.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                await conn.execute(
                    "UPDATE backup_jobs SET state='EXPIRED' WHERE backup_id=%s",
                    (queued["backup_id"],),
                )

        plan = await cleanup.plan(
            kind="BACKUP_ARTIFACT", resource_id=queued["backup_id"], **fixture.auth
        )
        applied = await cleanup.apply(
            plan_id=plan["plan_id"], plan_digest=plan["plan_digest"], **fixture.auth,
            expected_password_hash="unused-test-hash", idempotency_key="cleanup-backup-apply",
        )
        assert applied["state"] == "QUEUED" and path.is_file()
        with pytest.raises(ForestError) as unavailable:
            await backups.artifact(backup_id=queued["backup_id"], **fixture.auth)
        assert unavailable.value.code == "BACKUP_DOWNLOAD_UNAVAILABLE"
        tombstoned = await backups.read(backup_id=queued["backup_id"], **fixture.auth)
        assert tombstoned["state"] == "EXPIRED"
        assert tombstoned["cleanup_id"] == applied["cleanup_id"]

        complete = await cleanup.run_once()
        assert complete["state"] == "COMPLETE" and not path.exists()
        replay = await cleanup.apply(
            plan_id=plan["plan_id"], plan_digest=plan["plan_digest"], **fixture.auth,
            expected_password_hash="unused-test-hash", idempotency_key="cleanup-backup-apply",
        )
        assert replay == applied
    finally:
        await cleanup.close()
        await backups.close()


async def test_backup_cleanup_waits_for_active_read_only_verification(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups, queued = await _deployment_backup(
        fixture, data_dir, key="cleanup-verification-source"
    )
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    verifications = BackupVerificationManager(fixture.catalog, backups, restores, data_dir)
    cleanup = StorageCleanupManager(fixture.catalog, backups)
    try:
        await verifications.create(
            backup_id=queued["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            passphrase="cleanup recovery passphrase for tests",
            passphrase_digest="cleanup-passphrase-hmac",
            idempotency_key="active-verification",
        )
        plan = await cleanup.plan(
            kind="BACKUP_ARTIFACT", resource_id=queued["backup_id"], **fixture.auth
        )
        with pytest.raises(ForestError) as active:
            await cleanup.apply(
                plan_id=plan["plan_id"], plan_digest=plan["plan_digest"], **fixture.auth,
                expected_password_hash="unused-test-hash",
                idempotency_key="cleanup-during-verification",
            )
        assert active.value.code == "BACKUP_RESTORE_ACTIVE"

        verified = await verifications.run_once()
        assert verified["state"] == "VERIFIED"
        applied = await cleanup.apply(
            plan_id=plan["plan_id"], plan_digest=plan["plan_digest"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            idempotency_key="cleanup-after-verification",
        )
        assert applied["state"] == "QUEUED"
        assert (await cleanup.run_once())["state"] == "COMPLETE"
        receipt = await verifications.read(
            verification_id=verified["verification_id"], **fixture.auth
        )
        assert receipt["state"] == "VERIFIED"
        with pytest.raises(ForestError) as unavailable:
            await restores.create(
                backup_id=queued["backup_id"], verification_id=verified["verification_id"],
                **fixture.auth, expected_password_hash="unused-test-hash",
                idempotency_key="restore-after-backup-cleanup",
            )
        assert unavailable.value.code == "BACKUP_RESTORE_UNAVAILABLE"
    finally:
        await cleanup.close()
        await verifications.close()
        await restores.close()
        await backups.close()


async def test_expired_backup_is_automatically_tombstoned_and_cleaned(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    cleanup = StorageCleanupManager(fixture.catalog, backups)
    backup_id = str(uuid4())
    name = backup_id + ".1.forest-backup"
    path = backups.backup_dir / name
    path.write_bytes(b"expired encrypted artifact")
    digest = BackupManager._file_digest(path)
    now = datetime.now(UTC)
    try:
        async with fixture.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO backup_jobs(backup_id,subject_id,idempotency_key,input_digest,"
                "include_packages,state,archive_key_ciphertext,archive_header,database_total,"
                "database_completed,roster_start_digest,roster_end_digest) "
                "VALUES (%s,%s,%s,%s,false,'READY','sealed',%s,1,1,'same','same')",
                (backup_id, fixture.auth["subject_id"], "expired-source", "sha256:input",
                 Jsonb({"format": "test"})),
            )
            await conn.execute(
                "INSERT INTO backup_artifacts(backup_id,relative_path,ciphertext_digest,byte_size,"
                "manifest_digest,ready_at,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (backup_id, name, digest, path.stat().st_size, "sha256:manifest",
                 now - timedelta(days=2), now - timedelta(days=1)),
            )
        complete = await cleanup.run_once()
        assert complete["state"] == "COMPLETE" and complete["reason"] == "RETENTION"
        assert not path.exists()
        detail = await backups.read(backup_id=backup_id, **fixture.auth)
        assert detail["state"] == "EXPIRED" and detail["cleanup_state"] == "COMPLETE"
    finally:
        await cleanup.close()
        await backups.close()


async def test_expired_restore_stage_is_automatically_tombstoned(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    cleanup = StorageCleanupManager(fixture.catalog, backups)
    backup_id, restore_id = str(uuid4()), str(uuid4())
    target_name = "forest_restore_" + restore_id.replace("-", "")[:20] + "_0000"
    now = datetime.now(UTC)
    try:
        async with fixture.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO backup_jobs(backup_id,subject_id,idempotency_key,input_digest,"
                "include_packages,state,archive_key_ciphertext,archive_header,database_total,"
                "database_completed,roster_start_digest,roster_end_digest) "
                "VALUES (%s,%s,%s,'sha256:source',false,'READY','sealed',%s,1,1,'same','same')",
                (backup_id, fixture.auth["subject_id"], "restore-retention-source",
                 Jsonb({"format": "test"})),
            )
            await conn.execute(
                "INSERT INTO backup_artifacts(backup_id,relative_path,ciphertext_digest,byte_size,"
                "manifest_digest,ready_at,expires_at) VALUES (%s,%s,'sha256:cipher',1,"
                "'sha256:manifest',%s,%s)",
                (backup_id, backup_id + ".1.forest-backup", now, now + timedelta(days=1)),
            )
            await conn.execute(
                "INSERT INTO restore_stage_jobs(restore_id,backup_id,subject_id,idempotency_key,"
                "input_digest,artifact_digest,archive_key_ciphertext,state,database_total,"
                "database_completed,manifest_digest,expires_at) VALUES "
                "(%s,%s,%s,%s,'sha256:restore','sha256:cipher','sealed','STAGED',1,1,"
                "'sha256:manifest',%s)",
                (restore_id, backup_id, fixture.auth["subject_id"], "expired-restore",
                 now - timedelta(days=1)),
            )
            await conn.execute(
                "INSERT INTO restore_stage_targets(restore_id,ordinal,database_scope,bot_id,"
                "database_name,content_digest,migration_digest,state,restored_rows) "
                "VALUES (%s,0,'deployment','',%s,'sha256:content','sha256:migration','STAGED',0)",
                (restore_id, target_name),
            )
        complete = await cleanup.run_once()
        assert complete["state"] == "COMPLETE" and complete["reason"] == "RETENTION"
        detail = await cleanup.read(cleanup_id=complete["cleanup_id"], **fixture.auth)
        assert detail["resource_kind"] == "RESTORE_STAGE"
        async with fixture.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT state FROM restore_stage_jobs WHERE restore_id=%s", (restore_id,)
            )).fetchone()
        assert row["state"] == "EXPIRED"
    finally:
        await cleanup.close()
        await backups.close()


async def test_old_unpublished_backup_paths_require_job_epoch_proof(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups = BackupManager(
        fixture.catalog, PackageCatalog(data_dir / "artifacts"), data_dir,
        fixture.catalog.managed_dsn,
    )
    cleanup = StorageCleanupManager(fixture.catalog, backups, orphan_grace_seconds=60)
    backup_id = str(uuid4())
    candidate = backups.backup_dir / (backup_id + ".1.forest-backup")
    workspace = backups.backup_dir / (".job-" + backup_id + "-2")
    future = backups.backup_dir / (backup_id + ".3.forest-backup")
    candidate.write_bytes(b"unpublished-candidate")
    workspace.mkdir()
    (workspace / "payload.zip").write_bytes(b"sensitive plaintext workspace")
    future.write_bytes(b"unproven-future-epoch")
    old = datetime.now().timestamp() - 120
    os.utime(candidate, (old, old))
    os.utime(workspace / "payload.zip", (old, old))
    os.utime(workspace, (old, old))
    os.utime(future, (old, old))
    try:
        async with fixture.store.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO backup_jobs(backup_id,subject_id,idempotency_key,input_digest,"
                "include_packages,state,archive_key_ciphertext,archive_header,owner_epoch,error_code) "
                "VALUES (%s,%s,%s,'sha256:orphan',false,'FAILED','sealed',%s,2,'TEST_FAILURE')",
                (backup_id, fixture.auth["subject_id"], "orphan-source", Jsonb({"format": "test"})),
            )
        first = await cleanup.run_once()
        second = await cleanup.run_once()
        assert {first["path_kind"], second["path_kind"]} == {"CANDIDATE", "JOB_DIRECTORY"}
        assert not candidate.exists() and not workspace.exists()
        assert future.exists()
        async with fixture.store.pool.connection() as conn:
            receipts = await (await conn.execute(
                "SELECT path_kind,byte_size FROM backup_orphan_cleanup_receipts "
                "WHERE backup_id=%s ORDER BY path_kind", (backup_id,)
            )).fetchall()
        assert [row["path_kind"] for row in receipts] == ["CANDIDATE", "JOB_DIRECTORY"]
        assert all(row["byte_size"] > 0 for row in receipts)
    finally:
        future.unlink(missing_ok=True)
        await cleanup.close()
        await backups.close()


async def test_restore_cleanup_blocks_foreign_marker_then_retries_safely(fixture, tmp_path):
    data_dir = tmp_path / "vault"
    backups, queued = await _deployment_backup(
        fixture, data_dir, key="cleanup-restore-source"
    )
    restores = RestoreManager(fixture.catalog, backups, data_dir)
    verifications, verified = await _verify_backup(
        fixture, backups, restores, data_dir, backup_id=queued["backup_id"],
        passphrase="cleanup recovery passphrase for tests",
        passphrase_digest="cleanup-passphrase-hmac", key="verify-cleanup-source",
    )
    cleanup = StorageCleanupManager(fixture.catalog, backups)
    target_name = None
    try:
        restore = await restores.create(
            backup_id=queued["backup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash",
            verification_id=verified["verification_id"],
            idempotency_key="cleanup-restore-stage",
        )
        staged = await restores.run_once()
        assert staged["state"] == "STAGED"
        detail = await restores.read(restore_id=restore["restore_id"], **fixture.auth)
        target_name = detail["targets"][0]["database_name"]
        async with await psycopg.AsyncConnection.connect(
            fixture.catalog.managed_dsn, autocommit=True
        ) as conn:
            await conn.execute(sql.SQL("COMMENT ON DATABASE {} IS 'foreign-owner'").format(
                sql.Identifier(target_name)
            ))

        plan = await cleanup.plan(
            kind="RESTORE_STAGE", resource_id=restore["restore_id"], **fixture.auth
        )
        applied = await cleanup.apply(
            plan_id=plan["plan_id"], plan_digest=plan["plan_digest"], **fixture.auth,
            expected_password_hash="unused-test-hash", idempotency_key="cleanup-restore-apply",
        )
        assert (await restores.read(
            restore_id=restore["restore_id"], **fixture.auth
        ))["state"] == "EXPIRED"
        blocked = await cleanup.run_once()
        assert blocked["state"] == "BLOCKED"
        assert blocked["error_code"] == "RESTORE_CLEANUP_OWNERSHIP_CONFLICT"
        async with await psycopg.AsyncConnection.connect(
            fixture.catalog.managed_dsn, autocommit=True, row_factory=psycopg.rows.dict_row
        ) as conn:
            exists = await (await conn.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (target_name,)
            )).fetchone()
        assert exists is not None

        async with fixture.store.pool.connection() as conn:
            target = await (await conn.execute(
                "SELECT content_digest FROM restore_stage_targets "
                "WHERE restore_id=%s AND ordinal=0", (restore["restore_id"],)
            )).fetchone()
        marker = "forest:restore-stage:" + restore["restore_id"] + ":" + target["content_digest"]
        async with await psycopg.AsyncConnection.connect(
            fixture.catalog.managed_dsn, autocommit=True
        ) as conn:
            await conn.execute(sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                sql.Identifier(target_name), sql.Literal(marker)
            ))
        retried = await cleanup.retry(
            cleanup_id=applied["cleanup_id"], **fixture.auth,
            expected_password_hash="unused-test-hash", idempotency_key="cleanup-retry",
        )
        assert retried["state"] == "QUEUED"
        assert (await cleanup.run_once())["state"] == "COMPLETE"
        async with await psycopg.AsyncConnection.connect(
            fixture.catalog.managed_dsn, autocommit=True, row_factory=psycopg.rows.dict_row
        ) as conn:
            exists = await (await conn.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (target_name,)
            )).fetchone()
        assert exists is None
        target_name = None
    finally:
        if target_name is not None:
            async with await psycopg.AsyncConnection.connect(
                fixture.catalog.managed_dsn, autocommit=True
            ) as conn:
                await conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(target_name)
                ))
        await cleanup.close()
        await verifications.close()
        await restores.close()
        await backups.close()
