from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from forest.bootstrap import BootstrapState
from forest.deployment_recovery import DeploymentRecoveryManager
from forest.errors import ForestError
from forest.management.backups import BackupManager
from forest.management.catalog import BotCatalog
from forest.management.restores import RestoreManager
from forest.management.verifications import BackupVerificationManager
from forest.packages import PackageCatalog
from forest.security import SecretVault, hash_password
from forest.storage.postgres import PostgresStore

pytestmark = pytest.mark.postgres


async def test_offline_recovery_revalidates_content_and_switches_bootstrap(tmp_path: Path) -> None:
    admin_dsn = os.environ.get("FOREST_TEST_DSN")
    if not admin_dsn:
        pytest.skip("FOREST_TEST_DSN is required for private PostgreSQL integration tests")
    source_name = "forest_recovery_source_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(
        admin_dsn, autocommit=True, row_factory=dict_row
    )
    await admin.execute(
        sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(source_name))
    )
    connection = conninfo_to_dict(admin_dsn)
    source_dsn = (
        f"postgresql://{quote(connection['user'], safe='')}:"
        f"{quote(connection.get('password', ''), safe='')}@{connection.get('host', '127.0.0.1')}:"
        f"{connection.get('port', '5432')}/{source_name}"
    )
    data_dir = tmp_path / "deployment"
    state = BootstrapState(data_dir)
    token = state.initialize()
    password_hash = hash_password("deployment recovery test password")
    bootstrap_plan = state.freeze(token, source_dsn, "admin", password_hash)
    store = PostgresStore(source_dsn)
    backups = verification = restores = None
    target_name = None
    try:
        await store.open()
        await store.migrate()
        account = await store.setup(
            bootstrap_plan["setup_id"], bootstrap_plan["plan_digest"], "admin", password_hash
        )
        state.complete(bootstrap_plan["setup_id"], bootstrap_plan["plan_digest"])
        await store.create_session(
            "recovery-session",
            "recovery-csrf",
            account["id"],
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        auth = {"subject_id": account["id"], "session_token_hash": "recovery-session"}
        catalog = BotCatalog(store, SecretVault(data_dir), managed_dsn=source_dsn)
        backups = BackupManager(
            catalog, PackageCatalog(data_dir / "artifacts"), data_dir, source_dsn
        )
        passphrase = "offline recovery passphrase for integration tests"
        queued = await backups.create(
            **auth,
            expected_password_hash=password_hash,
            passphrase=passphrase,
            passphrase_digest="offline-recovery-passphrase-digest",
            include_packages=False,
            idempotency_key="offline-recovery-backup",
        )
        ready = await backups.run_once()
        assert ready["state"] == "READY"
        restores = RestoreManager(catalog, backups, data_dir)
        verification = BackupVerificationManager(catalog, backups, restores, data_dir)
        requested = await verification.create(
            backup_id=queued["backup_id"],
            **auth,
            expected_password_hash=password_hash,
            passphrase=passphrase,
            passphrase_digest="offline-recovery-passphrase-digest",
            idempotency_key="offline-recovery-verification",
        )
        verified = await verification.run_once()
        assert verified["state"] == "VERIFIED"
        restore = await restores.create(
            backup_id=queued["backup_id"],
            **auth,
            expected_password_hash=password_hash,
            verification_id=requested["verification_id"],
            idempotency_key="offline-recovery-stage",
        )
        staged = await restores.run_once()
        assert staged["state"] == "STAGED"
        detail = await restores.read(restore_id=restore["restore_id"], **auth)
        target = next(row for row in detail["targets"] if row["database_scope"] == "deployment")
        target_name = target["database_name"]
        target_dsn = source_dsn.rsplit("/", 1)[0] + "/" + target_name
        artifact, _ = await backups.artifact(backup_id=queued["backup_id"], **auth)
        passphrase_file = tmp_path / "recovery-passphrase"
        dsn_file = tmp_path / "recovery-dsn"
        passphrase_file.write_text(passphrase + "\n", encoding="utf-8")
        dsn_file.write_text(target_dsn + "\n", encoding="utf-8")
        manager = DeploymentRecoveryManager(data_dir)
        planned = await manager.plan(
            artifact_path=artifact,
            passphrase_file=passphrase_file,
            target_dsn_file=dsn_file,
        )
        assert planned["state"] == "PLANNED"
        assert state.load_plan()["target_identity"]["dbname"] == source_name
        async with await psycopg.AsyncConnection.connect(target_dsn, autocommit=True) as target_conn:
            await target_conn.execute("UPDATE web_accounts SET username='drifted-admin'")
        with pytest.raises(ForestError) as drifted:
            await manager.apply(
                recovery_id=planned["recovery_id"],
                plan_digest=planned["plan_digest"],
                passphrase_file=passphrase_file,
            )
        assert drifted.value.code == "RECOVERY_CONTENT_MISMATCH"
        assert state.load_plan()["target_identity"]["dbname"] == source_name
        async with await psycopg.AsyncConnection.connect(target_dsn, autocommit=True) as target_conn:
            await target_conn.execute("UPDATE web_accounts SET username='admin'")
        applied = await manager.apply(
            recovery_id=planned["recovery_id"],
            plan_digest=planned["plan_digest"],
            passphrase_file=passphrase_file,
        )
        assert applied["state"] == "APPLIED"
        effective = BootstrapState(data_dir).load_plan()
        assert effective["target_identity"]["dbname"] == target_name
        assert effective["setup_id"] == bootstrap_plan["setup_id"]
        assert effective["plan_digest"] == bootstrap_plan["plan_digest"]
    finally:
        if verification is not None:
            await verification.close()
        if restores is not None:
            await restores.close()
        if backups is not None:
            await backups.close()
        await store.close()
        if target_name is not None:
            assert target_name.startswith("forest_restore_")
            await admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(target_name)
                )
            )
        assert source_name.startswith("forest_recovery_source_")
        await admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(source_name)
            )
        )
        await admin.close()
