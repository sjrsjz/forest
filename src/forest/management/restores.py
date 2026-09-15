"""Durable verification and restore into unregistered PostgreSQL databases."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import re
import shutil
import zipfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forest.backup_artifact import decrypt_file_with_key, inspect_zip
from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import private_directory
from forest.storage.bot import BotStore
from forest.storage.migration_v21 import WRITER_GENERATION
from forest.storage.postgres import PostgresStore

_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_PACKAGE_MEMBER = re.compile(r"^artifacts/[a-f0-9]{64}\.zip$")
_DATABASE_KEYS = {
    "scope", "bot_id", "identity", "identity_digest", "migration_digest", "migrations",
    "database_name_digest", "server_version", "encoding", "collation", "character_type",
    "transaction_snapshot", "wal_lsn", "deletion_epoch", "captured_at", "tables",
    "constraints", "sequences", "member_count", "byte_size", "content_digest",
}
_TABLE_KEYS = {"name", "member", "rows", "columns", "copied_columns"}
_COLUMN_KEYS = {"name", "type", "not_null", "identity", "generated"}
_JOB_SELECT = (
    "SELECT j.*,c.cleanup_id,c.state AS cleanup_state,c.error_code AS cleanup_error_code,"
    "c.completed_at AS cleanup_completed_at FROM restore_stage_jobs j "
    "LEFT JOIN storage_cleanup_jobs c ON c.resource_kind='RESTORE_STAGE' "
    "AND c.resource_id=j.restore_id "
)
_PUBLIC_KEYS = (
    "restore_id", "backup_id", "verification_id", "state", "owner_epoch", "database_total",
    "database_completed", "manifest_digest", "error_code", "expires_at", "created_at", "updated_at",
    "cleanup_id", "cleanup_state", "cleanup_error_code", "cleanup_completed_at",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _public(row: dict[str, Any], targets: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    result = {
        key: row[key].isoformat() if isinstance(row.get(key), datetime) else row.get(key)
        for key in _PUBLIC_KEYS
    }
    if targets is not None:
        result["targets"] = [
            {
                key: value.isoformat() if isinstance(value, datetime) else value
                for key, value in target.items()
                if key not in {"content_digest", "migration_digest"}
            }
            for target in targets
        ]
    return result


class RestoreManager:
    """Restore verified local backup artifacts into databases absent from the Fleet catalog."""

    def __init__(self, catalog, backup_manager, data_dir: Path, *, lease_seconds: int = 3600):
        if not 60 <= lease_seconds <= 86400:
            raise ValueError("Restore lease must be between one minute and one day")
        self.catalog = catalog
        self.backups = backup_manager
        self.store = catalog.store
        self.vault = catalog.vault
        self.managed_dsn = catalog.managed_dsn
        self.restore_dir = private_directory(Path(data_dir) / "restore-stages")
        self.lease_seconds = lease_seconds
        self.owner_id = "restore-worker:" + str(uuid4())
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def _authorize(
        self, conn, subject_id: str, session_token_hash: str,
        expected_password_hash: str | None = None,
    ) -> None:
        account = await self.store._active_account(conn, subject_id, require_admin=True)
        await self.store._active_session(conn, session_token_hash, subject_id)
        if expected_password_hash is not None:
            row = await (await conn.execute(
                "SELECT password_hash FROM web_accounts WHERE id=%s FOR UPDATE", (subject_id,)
            )).fetchone()
            if row is None or row["password_hash"] != expected_password_hash or account["disabled"]:
                raise ForestError(
                    "REAUTHENTICATION_STALE", "Account credentials changed; authenticate again", 409
                )

    async def create(
        self, *, backup_id: str, subject_id: str, session_token_hash: str,
        expected_password_hash: str, verification_id: str, idempotency_key: str,
    ) -> dict[str, Any]:
        if self.managed_dsn is None:
            raise ForestError(
                "RESTORE_MANAGED_STORAGE_REQUIRED",
                "Isolated restore requires managed PostgreSQL storage", 409,
            )
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("RESTORE_REQUEST_INVALID", "Restore request is invalid", 422)
        semantic = digest_json({
            "backup_id": backup_id, "verification_id": verification_id,
            "mode": "isolated-managed-v2",
        })
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                _JOB_SELECT + "WHERE j.subject_id=%s AND j.idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Restore key was used for different input", 409
                    )
                return _public(previous)

        restore_id = str(uuid4())
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170790)")
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                _JOB_SELECT + "WHERE j.subject_id=%s AND j.idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Restore key was used for different input", 409
                    )
                return _public(previous)
            verification = await (await conn.execute(
                "SELECT * FROM backup_verification_jobs WHERE verification_id=%s "
                "AND backup_id=%s FOR SHARE", (verification_id, backup_id)
            )).fetchone()
            if (
                verification is None or verification["state"] != "VERIFIED"
                or verification["expires_at"] <= await self._now(conn)
                or verification["manifest_digest"] is None
            ):
                raise ForestError(
                    "BACKUP_VERIFICATION_REQUIRED",
                    "A current successful verification receipt is required", 409,
                )
            current = await (await conn.execute(
                "SELECT j.state,a.ciphertext_digest,a.manifest_digest,a.deleted_at,a.expires_at "
                "FROM backup_jobs j JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.backup_id=%s FOR UPDATE OF j,a", (backup_id,)
            )).fetchone()
            if (
                current is None or current["state"] != "READY" or current["deleted_at"] is not None
                or current["expires_at"] <= await self._now(conn)
                or current["ciphertext_digest"] != verification["artifact_digest"]
                or current["manifest_digest"] != verification["manifest_digest"]
            ):
                raise ForestError(
                    "BACKUP_RESTORE_UNAVAILABLE", "Backup cannot enter a restore plan", 409
                )
            cuts = await (await conn.execute(
                "SELECT database_scope,bot_id,content_digest,migration_digest,deletion_epoch "
                "FROM backup_database_cuts WHERE backup_id=%s "
                "ORDER BY database_scope,bot_id", (backup_id,)
            )).fetchall()
            if not 1 <= len(cuts) <= 1024:
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database roster is invalid", 409)
            row = await (await conn.execute(
                "INSERT INTO restore_stage_jobs(restore_id,backup_id,verification_id,subject_id,"
                "idempotency_key,input_digest,artifact_digest,archive_key_ciphertext,database_total) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (restore_id, backup_id, verification_id, subject_id, idempotency_key, semantic,
                 verification["artifact_digest"], verification["archive_key_ciphertext"], len(cuts)),
            )).fetchone()
            prefix = restore_id.replace("-", "")[:20]
            for ordinal, cut in enumerate(cuts):
                database_name = f"forest_restore_{prefix}_{ordinal:04d}"
                await conn.execute(
                    "INSERT INTO restore_stage_targets(restore_id,ordinal,database_scope,bot_id,"
                    "database_name,content_digest,migration_digest,deletion_epoch) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (restore_id, ordinal, cut["database_scope"], cut["bot_id"], database_name,
                     cut["content_digest"], cut["migration_digest"], cut["deletion_epoch"]),
                )
            await conn.execute(
                "INSERT INTO restore_stage_events(restore_id,owner_epoch,step) "
                "VALUES (%s,0,'QUEUED')", (restore_id,)
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'restore.stage.create',%s,%s,%s)",
                (str(uuid4()), subject_id, "restore:" + restore_id, restore_id,
                 Jsonb({"restore_id": restore_id, "backup_id": backup_id,
                        "verification_id": verification_id, "database_total": len(cuts)})),
            )
            return _public(row)

    async def list(
        self, *, subject_id: str, session_token_hash: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ForestError("RESTORE_QUERY_INVALID", "Restore limit is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                _JOB_SELECT + "ORDER BY j.created_at DESC,j.restore_id DESC LIMIT %s", (limit,)
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return [_public(row) for row in rows]

    async def read(
        self, *, restore_id: str, subject_id: str, session_token_hash: str
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                _JOB_SELECT + "WHERE j.restore_id=%s", (restore_id,)
            )).fetchone()
            if row is None:
                raise ForestError("RESTORE_NOT_FOUND", "Restore stage does not exist", 404)
            targets = await (await conn.execute(
                "SELECT ordinal,database_scope,bot_id,database_name,state,restored_rows,"
                "quarantine_digest,error_code,updated_at FROM restore_stage_targets "
                "WHERE restore_id=%s ORDER BY ordinal", (restore_id,)
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return _public(row, targets)

    async def _claim(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            locked = await (await conn.execute(
                "SELECT pg_try_advisory_xact_lock(679330170791) AS locked"
            )).fetchone()
            if not locked["locked"]:
                return None
            active = await (await conn.execute(
                "SELECT 1 FROM restore_stage_jobs "
                "WHERE state IN ('VERIFYING','RESTORING','VALIDATING') "
                "AND lease_expires_at>clock_timestamp() LIMIT 1"
            )).fetchone()
            if active is not None:
                return None
            job = await (await conn.execute(
                "SELECT * FROM restore_stage_jobs WHERE state='QUEUED' OR "
                "(state IN ('VERIFYING','RESTORING','VALIDATING') "
                "AND lease_expires_at<=clock_timestamp()) "
                "ORDER BY (state IN ('VERIFYING','RESTORING','VALIDATING')) DESC,"
                "created_at,restore_id FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if job is None:
                return None
            claimed = await (await conn.execute(
                "UPDATE restore_stage_jobs SET state='VERIFYING',owner_id=%s,"
                "owner_epoch=owner_epoch+1,lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),database_completed=0,error_code=NULL,"
                "manifest_digest=NULL,updated_at=clock_timestamp() "
                "WHERE restore_id=%s RETURNING *",
                (self.owner_id, self.lease_seconds, job["restore_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO restore_stage_events(restore_id,owner_epoch,step) "
                "VALUES (%s,%s,'VERIFYING')", (claimed["restore_id"], claimed["owner_epoch"])
            )
            return claimed

    async def _heartbeat(
        self, job: dict[str, Any], *, state: str | None = None, completed: int | None = None
    ) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            changed = await (await conn.execute(
                "UPDATE restore_stage_jobs SET state=COALESCE(%s,state),"
                "database_completed=COALESCE(%s,database_completed),"
                "lease_expires_at=clock_timestamp()+(%s*interval '1 second'),"
                "updated_at=clock_timestamp() WHERE restore_id=%s AND owner_id=%s "
                "AND owner_epoch=%s AND state IN ('VERIFYING','RESTORING','VALIDATING') "
                "AND lease_expires_at>clock_timestamp() RETURNING 1",
                (state, completed, self.lease_seconds, job["restore_id"],
                 self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if changed is None:
                raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)
            if state is not None:
                await conn.execute(
                    "INSERT INTO restore_stage_events(restore_id,owner_epoch,step) "
                    "VALUES (%s,%s,%s)", (job["restore_id"], job["owner_epoch"], state)
                )

    async def run_once(self) -> dict[str, Any] | None:
        job = await self._claim()
        if job is None:
            return None
        try:
            return await self._stage(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, ForestError) else "RESTORE_FAILED"
            async with self.store.pool.connection() as conn, conn.transaction():
                failed = await (await conn.execute(
                    "UPDATE restore_stage_jobs SET state='FAILED',owner_id=NULL,"
                    "lease_expires_at=NULL,error_code=%s,updated_at=clock_timestamp() "
                    "WHERE restore_id=%s AND owner_id=%s AND owner_epoch=%s "
                    "AND state IN ('VERIFYING','RESTORING','VALIDATING') RETURNING 1",
                    (code, job["restore_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if failed is not None:
                    await conn.execute(
                        "UPDATE restore_stage_targets SET state='FAILED',error_code=%s,"
                        "updated_at=clock_timestamp() WHERE restore_id=%s "
                        "AND state NOT IN ('STAGED','FAILED')",
                        (code, job["restore_id"]),
                    )
                    await conn.execute(
                        "INSERT INTO restore_stage_events(restore_id,owner_epoch,step,details) "
                        "VALUES (%s,%s,'FAILED',%s)",
                        (job["restore_id"], job["owner_epoch"], Jsonb({"error_code": code})),
                    )
            return await self._read_internal(job["restore_id"])

    async def run_forever(self) -> None:
        self.closed = False
        while not self.closed:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(1)

    async def _read_internal(self, restore_id: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn:
            return await (await conn.execute(
                _JOB_SELECT + "WHERE j.restore_id=%s", (restore_id,)
            )).fetchone()

    async def _stage(self, job: dict[str, Any]) -> dict[str, Any]:
        stage = asyncio.create_task(self._stage_owned(job), name="restore-stage-owned")
        renewal = asyncio.create_task(self._renew_lease(job), name="restore-stage-lease")
        try:
            done, _ = await asyncio.wait({stage, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if stage in done:
                return await stage
            error = renewal.exception()
            stage.cancel()
            await asyncio.gather(stage, return_exceptions=True)
            if error is not None:
                raise error
            raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)
        finally:
            stage.cancel()
            renewal.cancel()
            await asyncio.gather(stage, renewal, return_exceptions=True)

    async def _renew_lease(self, job: dict[str, Any]) -> None:
        interval = max(10.0, self.lease_seconds / 3)
        while True:
            await asyncio.sleep(interval)
            await self._heartbeat(job)

    async def _stage_owned(self, job: dict[str, Any]) -> dict[str, Any]:
        job_root = self.restore_dir / (
            ".job-" + job["restore_id"] + "-" + str(job["owner_epoch"])
        )
        await asyncio.to_thread(self._reset_job_root, job_root)
        archive_path = job_root / "verified.zip"
        try:
            source_path, artifact_digest, expected_manifest_digest = await self._source(
                job, permit_expired=True
            )
            key = base64.urlsafe_b64decode(self.vault.decrypt(job["archive_key_ciphertext"]))
            header = await asyncio.to_thread(
                decrypt_file_with_key, source_path, archive_path, key
            )
            manifest = await asyncio.to_thread(inspect_zip, archive_path)
            manifest_digest = await asyncio.to_thread(self._manifest_digest, archive_path)
            targets = await self._validate_manifest(
                job, header, manifest, manifest_digest, expected_manifest_digest, artifact_digest
            )
            await self._heartbeat(job, state="RESTORING")
            with zipfile.ZipFile(archive_path) as archive:
                for index, target in enumerate(targets):
                    if target["state"] == "STAGED":
                        await self._heartbeat(job, completed=index + 1)
                        continue
                    database = next(
                        item for item in manifest["databases"]
                        if item["scope"] == target["database_scope"]
                        and (item.get("bot_id") or "") == target["bot_id"]
                    )
                    await self._prepare_database(job, target)
                    rows, quarantine = await self._restore_database(
                        archive, database, target, job
                    )
                    async with self.store.pool.connection() as conn, conn.transaction():
                        changed = await (await conn.execute(
                            "UPDATE restore_stage_targets t SET state='STAGED',restored_rows=%s,"
                            "quarantine_digest=%s,error_code=NULL,updated_at=clock_timestamp() "
                            "FROM restore_stage_jobs j WHERE t.restore_id=%s AND t.ordinal=%s "
                            "AND t.state='VALIDATED' AND j.restore_id=t.restore_id "
                            "AND j.owner_id=%s AND j.owner_epoch=%s "
                            "AND j.lease_expires_at>clock_timestamp() RETURNING 1",
                            (rows, quarantine, job["restore_id"], target["ordinal"],
                             self.owner_id, job["owner_epoch"]),
                        )).fetchone()
                        if changed is None:
                            raise ForestError(
                                "RESTORE_TARGET_CONFLICT", "Restore target state changed", 409
                            )
                    await self._heartbeat(job, completed=index + 1)
            await self._heartbeat(job, state="VALIDATING")
            async with self.store.pool.connection() as conn, conn.transaction():
                current = await (await conn.execute(
                    "SELECT * FROM restore_stage_jobs WHERE restore_id=%s FOR UPDATE",
                    (job["restore_id"],),
                )).fetchone()
                now = await self._now(conn)
                if (
                    current is None or current["state"] != "VALIDATING"
                    or current["owner_id"] != self.owner_id
                    or current["owner_epoch"] != job["owner_epoch"]
                    or current["lease_expires_at"] <= now
                ):
                    raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)
                count = await (await conn.execute(
                    "SELECT count(*) AS n FROM restore_stage_targets "
                    "WHERE restore_id=%s AND state='STAGED'", (job["restore_id"],)
                )).fetchone()
                if count["n"] != job["database_total"]:
                    raise ForestError(
                        "RESTORE_VALIDATION_FAILED", "Restore target set is incomplete", 409
                    )
                ready = await (await conn.execute(
                    "UPDATE restore_stage_jobs SET state='STAGED',owner_id=NULL,"
                    "lease_expires_at=NULL,database_completed=database_total,"
                    "manifest_digest=%s,error_code=NULL,updated_at=clock_timestamp() "
                    "WHERE restore_id=%s RETURNING *",
                    (manifest_digest, job["restore_id"]),
                )).fetchone()
                await conn.execute(
                    "INSERT INTO restore_stage_events(restore_id,owner_epoch,step,details) "
                    "VALUES (%s,%s,'STAGED',%s)",
                    (job["restore_id"], job["owner_epoch"], Jsonb({
                        "manifest_digest": manifest_digest,
                        "database_total": job["database_total"],
                    })),
                )
                await conn.execute(
                    "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                    "VALUES (%s,%s,'restore.stage.completed',%s,%s,%s)",
                    (str(uuid4()), job["subject_id"], "restore:" + job["restore_id"],
                     job["restore_id"], Jsonb({
                         "restore_id": job["restore_id"],
                         "backup_id": job["backup_id"],
                         "verification_id": job["verification_id"],
                         "database_total": job["database_total"],
                         "manifest_digest": manifest_digest,
                     })),
                )
            return _public(ready)
        finally:
            await asyncio.to_thread(self._remove_job_root, job_root)

    async def _source(
        self, job: dict[str, Any], *, permit_expired: bool = False,
    ) -> tuple[Path, str, str]:
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT j.state,a.relative_path,a.ciphertext_digest,a.manifest_digest,"
                "a.deleted_at,a.expires_at "
                "FROM backup_jobs j JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.backup_id=%s", (job["backup_id"],)
            )).fetchone()
        if (
            row is None or row["state"] != "READY" or row["deleted_at"] is not None
            or not permit_expired and row["expires_at"] <= datetime.now(UTC)
            or row["ciphertext_digest"] != job["artifact_digest"]
        ):
            raise ForestError("BACKUP_RESTORE_UNAVAILABLE", "Backup is no longer restorable", 409)
        path = (self.backups.backup_dir / row["relative_path"]).resolve()
        if path.parent != self.backups.backup_dir.resolve() or not path.is_file() or path.is_symlink():
            raise ForestError("BACKUP_STORAGE_ERROR", "Backup artifact is unavailable", 503)
        actual = await asyncio.to_thread(self.backups._file_digest, path)
        if actual != row["ciphertext_digest"]:
            raise ForestError("BACKUP_INTEGRITY_FAILED", "Backup artifact integrity failed", 409)
        return path, actual, row["manifest_digest"]

    async def _validate_manifest(
        self, job: dict[str, Any], header: dict[str, Any], manifest: dict[str, Any],
        manifest_digest: str, expected_manifest_digest: str, artifact_digest: str,
        *, targets: list[dict[str, Any]] | None = None, publish_restore: bool = True,
    ) -> list[dict[str, Any]]:
        required = {
            "format", "backup_id", "created_at", "databases", "members", "includes",
            "roster", "activation",
        }
        if (
            set(manifest) != required or header["backup_id"] != job["backup_id"]
            or manifest["backup_id"] != job["backup_id"]
            or manifest["activation"] != "ISOLATED_RESTORE_REQUIRED"
            or not isinstance(manifest["roster"], dict)
            or manifest["roster"].get("consistent") is not True
            or artifact_digest != job["artifact_digest"]
            or manifest_digest != expected_manifest_digest
        ):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup manifest is incompatible", 409)
        roster = manifest["roster"]
        if (
            set(roster) != {"start", "end", "start_digest", "end_digest", "consistent"}
            or roster["start"] != roster["end"]
            or roster["start_digest"] != roster["end_digest"]
            or digest_json(roster["start"]) != roster["start_digest"]
        ):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup roster proof is invalid", 409)
        if targets is None:
            async with self.store.pool.connection() as conn:
                targets = await (await conn.execute(
                    "SELECT * FROM restore_stage_targets WHERE restore_id=%s ORDER BY ordinal",
                    (job["restore_id"],),
                )).fetchall()
        databases = manifest.get("databases")
        members = manifest.get("members")
        includes = manifest.get("includes")
        if (
            not isinstance(databases, list) or len(databases) != len(targets)
            or not isinstance(members, list)
            or not isinstance(includes, dict)
            or set(includes) != {"recovery_key", "packages"}
            or includes["recovery_key"] is not True
            or type(includes["packages"]) is not bool
        ):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database roster is invalid", 409)
        by_scope = {}
        member_map = {item["name"]: item for item in members}
        table_members: set[str] = set()
        for database in databases:
            if not isinstance(database, dict) or set(database) != _DATABASE_KEYS:
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database entry is invalid", 409)
            key = (database.get("scope"), database.get("bot_id") or "")
            if (
                key in by_scope or key[0] not in {"deployment", "bot"}
                or (key[0] == "deployment" and database["bot_id"] is not None)
                or (key[0] == "bot" and (
                    not isinstance(database["bot_id"], str)
                    or not 1 <= len(database["bot_id"]) <= 128
                ))
            ):
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database entry is duplicated", 409)
            tables = database.get("tables")
            migrations = database.get("migrations")
            identity = database.get("identity")
            if (
                not isinstance(tables, list) or not tables
                or not isinstance(migrations, list) or not migrations
                or not isinstance(identity, dict)
                or any(
                    not isinstance(row, dict) or set(row) != {"version", "checksum"}
                    or not isinstance(row["version"], str) or not row["version"]
                    or not isinstance(row["checksum"], str) or not row["checksum"]
                    for row in migrations
                )
                or not isinstance(database["constraints"], list)
                or any(
                    not isinstance(item, dict)
                    or set(item) != {
                        "conname", "contype", "table_name", "referenced_table", "definition",
                    }
                    or any(not isinstance(value, str) for value in item.values())
                    for item in database["constraints"]
                )
                or not isinstance(database["sequences"], list)
                or any(
                    not isinstance(item, dict)
                    or set(item) != {
                        "sequencename", "start_value", "min_value", "max_value",
                        "increment_by", "cycle", "last_value",
                    }
                    or not isinstance(item["sequencename"], str)
                    or not _NAME.fullmatch(item["sequencename"])
                    or type(item["cycle"]) is not bool
                    for item in database["sequences"]
                )
            ):
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database contract is invalid", 409)
            exported = []
            prefix = "deployment" if key[0] == "deployment" else "bots/" + key[1]
            for index, table in enumerate(tables):
                if (
                    not isinstance(table, dict) or set(table) != _TABLE_KEYS
                    or not isinstance(table["name"], str) or not _NAME.fullmatch(table["name"])
                    or table["member"] != f"{prefix}/tables/{index:04d}-{table['name']}.bin"
                    or type(table["rows"]) is not int or table["rows"] < 0
                    or not isinstance(table["columns"], list) or not table["columns"]
                    or not isinstance(table["copied_columns"], list)
                    or any(
                        not isinstance(column, dict) or set(column) != _COLUMN_KEYS
                        or not isinstance(column["name"], str)
                        or not _NAME.fullmatch(column["name"])
                        or not isinstance(column["type"], str)
                        or type(column["not_null"]) is not bool
                        or not isinstance(column["identity"], str)
                        or not isinstance(column["generated"], str)
                        for column in table["columns"]
                    )
                    or table["copied_columns"] != [
                        column["name"] for column in table["columns"] if not column["generated"]
                    ]
                ):
                    raise ForestError("BACKUP_MANIFEST_INVALID", "Backup table contract is invalid", 409)
                member = member_map.get(table["member"])
                if member is None or table["member"] in table_members:
                    raise ForestError("BACKUP_MANIFEST_INVALID", "Backup table member is invalid", 409)
                table_members.add(table["member"])
                exported.append([member["name"], member["digest"]])
            if (
                digest_json(exported) != database.get("content_digest")
                or digest_json(identity) != database.get("identity_digest")
                or digest_json([[row.get("version"), row.get("checksum")] for row in migrations])
                != database.get("migration_digest")
                or database.get("member_count") != len(tables)
                or database.get("byte_size") != sum(member_map[item["member"]]["size"] for item in tables)
            ):
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup database digest is invalid", 409)
            by_scope[key] = database
        package_members = {name for name in member_map if _PACKAGE_MEMBER.fullmatch(name)}
        allowed_members = table_members | {"recovery/master.key"} | package_members
        if (
            "recovery/master.key" not in member_map or set(member_map) != allowed_members
            or (bool(package_members) and includes["packages"] is not True)
        ):
            raise ForestError("BACKUP_MANIFEST_INVALID", "Backup member purpose is invalid", 409)
        for target in targets:
            database = by_scope.get((target["database_scope"], target["bot_id"]))
            if (
                database is None or database.get("content_digest") != target["content_digest"]
                or database.get("migration_digest") != target["migration_digest"]
                or database.get("deletion_epoch") != target["deletion_epoch"]
            ):
                raise ForestError("BACKUP_MANIFEST_INVALID", "Backup cut receipt is invalid", 409)
        if publish_restore:
            async with self.store.pool.connection() as conn, conn.transaction():
                changed = await (await conn.execute(
                    "UPDATE restore_stage_jobs SET manifest_digest=%s,updated_at=clock_timestamp() "
                    "WHERE restore_id=%s AND owner_id=%s AND owner_epoch=%s "
                    "AND state='VERIFYING' RETURNING 1",
                    (manifest_digest, job["restore_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if changed is None:
                    raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)
        return targets

    async def _prepare_database(self, job: dict[str, Any], target: dict[str, Any]) -> None:
        assert self.managed_dsn is not None
        marker = "forest:restore-stage:" + job["restore_id"] + ":" + target["content_digest"]
        runtime_storage = await self._runtime_storage(target)
        async with self.store.pool.connection() as conn, conn.transaction():
            current = await (await conn.execute(
                "SELECT t.state FROM restore_stage_targets t JOIN restore_stage_jobs j "
                "ON j.restore_id=t.restore_id WHERE t.restore_id=%s AND t.ordinal=%s "
                "AND j.owner_id=%s AND j.owner_epoch=%s "
                "AND j.state IN ('VERIFYING','RESTORING','VALIDATING') "
                "AND j.lease_expires_at>clock_timestamp() FOR UPDATE OF t,j",
                (job["restore_id"], target["ordinal"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if current is None:
                raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)
            if current["state"] not in {
                "PLANNED", "CREATING", "CREATED", "VALIDATED",
            }:
                raise ForestError("RESTORE_TARGET_CONFLICT", "Restore target state changed", 409)
            prior_state = current["state"]
            if prior_state == "PLANNED":
                await conn.execute(
                    "UPDATE restore_stage_targets SET state='CREATING',"
                    "updated_at=clock_timestamp() WHERE restore_id=%s AND ordinal=%s",
                    (job["restore_id"], target["ordinal"]),
                )
                prior_state = "CREATING"
        async with await psycopg.AsyncConnection.connect(
            self.managed_dsn, autocommit=True, row_factory=dict_row, connect_timeout=10
        ) as conn:
            expected_owner = None
            if runtime_storage is not None:
                owner = await (await conn.execute(
                    "SELECT oid FROM pg_roles WHERE rolname=%s", (runtime_storage["role_name"],)
                )).fetchone()
                if owner is None:
                    raise ForestError(
                        "RESTORE_TARGET_ROLE_MISSING", "Current Bot storage role is unavailable", 409
                    )
                expected_owner = owner["oid"]
            row = await (await conn.execute(
                "SELECT shobj_description(oid,'pg_database') AS marker,datistemplate,"
                "datallowconn,datdba FROM pg_database WHERE datname=%s", (target["database_name"],)
            )).fetchone()
            if row is None:
                if prior_state != "CREATING":
                    raise ForestError(
                        "RESTORE_TARGET_OWNERSHIP_CONFLICT",
                        "Restore target database disappeared", 409,
                    )
                create = sql.SQL("CREATE DATABASE {} TEMPLATE template0 ALLOW_CONNECTIONS false").format(
                    sql.Identifier(target["database_name"])
                )
                if runtime_storage is not None:
                    create = sql.SQL("{} OWNER {}").format(
                        create, sql.Identifier(runtime_storage["role_name"])
                    )
                await conn.execute(create)
                row = {
                    "marker": None, "datistemplate": False, "datallowconn": False,
                    "datdba": expected_owner,
                }
            if (
                row["datistemplate"] or row["marker"] not in {None, marker}
                or expected_owner is not None and row["datdba"] != expected_owner
            ):
                raise ForestError(
                    "RESTORE_TARGET_OWNERSHIP_CONFLICT", "Restore target ownership is invalid", 409
                )
            if row["marker"] is None:
                if prior_state != "CREATING" or row["datallowconn"]:
                    raise ForestError(
                        "RESTORE_TARGET_OWNERSHIP_CONFLICT",
                        "Unmarked restore target was not created by this job", 409,
                    )
                await conn.execute(sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(target["database_name"]), sql.Literal(marker)
                ))
            await conn.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(
                sql.Identifier(target["database_name"])
            ))
            if not row["datallowconn"]:
                await conn.execute(
                    sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(
                        sql.Identifier(target["database_name"])
                    )
                )
        async with self.store.pool.connection() as conn, conn.transaction():
            changed = await (await conn.execute(
                "UPDATE restore_stage_targets t SET state='CREATED',restored_rows=NULL,"
                "quarantine_digest=NULL,error_code=NULL,"
                "updated_at=clock_timestamp() FROM restore_stage_jobs j "
                "WHERE t.restore_id=%s AND t.ordinal=%s "
                "AND t.state IN ('CREATING','CREATED','VALIDATED') "
                "AND j.restore_id=t.restore_id AND j.owner_id=%s AND j.owner_epoch=%s "
                "AND j.lease_expires_at>clock_timestamp() RETURNING 1",
                (job["restore_id"], target["ordinal"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if changed is None:
                raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)

    async def _restore_database(
        self, archive: zipfile.ZipFile, database: dict[str, Any], target: dict[str, Any],
        job: dict[str, Any],
    ) -> tuple[int, str | None]:
        target_dsn = self._target_dsn(target["database_name"])
        runtime_storage = await self._runtime_storage(target)
        schema_dsn = target_dsn
        if runtime_storage is not None:
            schema_dsn = make_conninfo(
                runtime_storage["dsn"], dbname=target["database_name"], options=None
            )
        async with await psycopg.AsyncConnection.connect(
            target_dsn, autocommit=True, connect_timeout=10,
        ) as guard:
            await guard.execute("SELECT pg_advisory_lock(679330170792)")
            await self._heartbeat(job)
            return await self._restore_database_locked(
                archive, database, target, job, target_dsn, schema_dsn
            )

    async def _restore_database_locked(
        self, archive: zipfile.ZipFile, database: dict[str, Any], target: dict[str, Any],
        job: dict[str, Any], target_dsn: str, schema_dsn: str,
    ) -> tuple[int, str | None]:
        # Schema ownership belongs to the restricted Bot role, while the managed
        # administrator performs COPY with trigger suppression during isolation.
        await self._migrate(database, schema_dsn)
        async with await psycopg.AsyncConnection.connect(
            target_dsn, autocommit=True, row_factory=dict_row, connect_timeout=10,
            options=f"-cforest.writer_generation={WRITER_GENERATION}",
        ) as conn:
            await self._validate_environment(conn, database)
            await self._validate_schema(conn, database)
            tables = database.get("tables")
            if not isinstance(tables, list) or not tables:
                raise ForestError("BACKUP_SCHEMA_UNSUPPORTED", "Backup table roster is invalid", 409)
            await conn.execute("BEGIN")
            try:
                await conn.execute("SET LOCAL session_replication_role='replica'")
                names = [table.get("name") for table in tables]
                if any(not isinstance(name, str) or not _NAME.fullmatch(name) for name in names):
                    raise ForestError("BACKUP_SCHEMA_UNSUPPORTED", "Backup table name is invalid", 409)
                await conn.execute(sql.SQL("TRUNCATE {} CASCADE").format(
                    sql.SQL(",").join(
                        sql.SQL("public.{}").format(sql.Identifier(name)) for name in names
                    )
                ))
                restored_rows = 0
                for table in tables:
                    columns = table.get("copied_columns")
                    member = table.get("member")
                    if (
                        not isinstance(columns, list) or not columns
                        or any(not isinstance(name, str) or not _NAME.fullmatch(name) for name in columns)
                        or not isinstance(member, str)
                    ):
                        raise ForestError(
                            "BACKUP_SCHEMA_UNSUPPORTED", "Backup COPY contract is invalid", 409
                        )
                    query = sql.SQL("COPY public.{} ({}) FROM STDIN (FORMAT BINARY)").format(
                        sql.Identifier(table["name"]),
                        sql.SQL(",").join(sql.Identifier(name) for name in columns),
                    )
                    with archive.open(member) as source:
                        async with conn.cursor().copy(query) as copy:
                            while chunk := source.read(1024 * 1024):
                                await copy.write(chunk)
                    count = await (await conn.execute(
                        sql.SQL("SELECT count(*) AS n FROM public.{}").format(
                            sql.Identifier(table["name"])
                        )
                    )).fetchone()
                    if type(table.get("rows")) is not int or count["n"] != table["rows"]:
                        raise ForestError(
                            "RESTORE_ROW_COUNT_MISMATCH", "Restored table row count differs", 409
                        )
                    restored_rows += count["n"]
                for sequence in database.get("sequences", []):
                    name = sequence.get("sequencename")
                    if not isinstance(name, str) or not _NAME.fullmatch(name):
                        raise ForestError(
                            "BACKUP_SCHEMA_UNSUPPORTED", "Backup sequence name is invalid", 409
                        )
                    last = sequence.get("last_value")
                    value = sequence.get("start_value") if last is None else last
                    await conn.execute(
                        "SELECT setval(%s::regclass,%s,%s)",
                        ("public." + name, value, last is not None),
                    )
                await self._validate_foreign_keys(conn)
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
            await self._validate_identity(conn, database)
            quarantine = await self._quarantine(conn, database)
        async with self.store.pool.connection() as conn, conn.transaction():
            changed = await (await conn.execute(
                "UPDATE restore_stage_targets t SET state='VALIDATED',restored_rows=%s,"
                "quarantine_digest=%s,updated_at=clock_timestamp() "
                "FROM restore_stage_jobs j WHERE t.restore_id=%s AND t.ordinal=%s "
                "AND t.state='CREATED' AND j.restore_id=t.restore_id "
                "AND j.owner_id=%s AND j.owner_epoch=%s "
                "AND j.lease_expires_at>clock_timestamp() RETURNING 1",
                (restored_rows, quarantine, target["restore_id"], target["ordinal"],
                 self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if changed is None:
                raise ForestError("RESTORE_LEASE_LOST", "Restore ownership was lost", 409)
        return restored_rows, quarantine

    async def _migrate(self, database: dict[str, Any], target_dsn: str) -> None:
        if database["scope"] == "deployment":
            store = PostgresStore(target_dsn)
        else:
            store = BotStore(target_dsn, database["bot_id"])
        try:
            await store.open()
            await store.migrate()
        finally:
            await store.close()

    async def _validate_schema(self, conn, database: dict[str, Any]) -> None:
        migration_table = (
            "forest_schema_migrations" if database["scope"] == "deployment"
            else "forest_bot_schema_migrations"
        )
        migrations = _json_value(await (await conn.execute(
            sql.SQL("SELECT version,checksum FROM public.{} ORDER BY version").format(
                sql.Identifier(migration_table)
            )
        )).fetchall())
        if migrations != database.get("migrations") or digest_json(
            [[row["version"], row["checksum"]] for row in migrations]
        ) != database.get("migration_digest"):
            raise ForestError(
                "RESTORE_SCHEMA_VERSION_MISMATCH", "Restore schema migration set differs", 409
            )
        rows = await (await conn.execute(
            "SELECT c.relname AS table_name,a.attnum,a.attname AS column_name,"
            "format_type(a.atttypid,a.atttypmod) AS data_type,a.attnotnull,a.attidentity,a.attgenerated "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
            "WHERE n.nspname='public' AND c.relkind='r' AND c.relname<>%s "
            "ORDER BY c.relname,a.attnum", (migration_table,)
        )).fetchall()
        columns: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            columns.setdefault(row["table_name"], []).append({
                "name": row["column_name"], "type": row["data_type"],
                "not_null": row["attnotnull"], "identity": row["attidentity"],
                "generated": row["attgenerated"],
            })
        expected = {table["name"]: table["columns"] for table in database.get("tables", [])}
        if columns != expected:
            raise ForestError("RESTORE_SCHEMA_MISMATCH", "Restore table schema differs", 409)
        constraints = _json_value(await (await conn.execute(
            "SELECT c.conname,c.contype,r.relname AS table_name,"
            "COALESCE(fr.relname,'') AS referenced_table,pg_get_constraintdef(c.oid,true) AS definition "
            "FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid "
            "JOIN pg_namespace n ON n.oid=r.relnamespace "
            "LEFT JOIN pg_class fr ON fr.oid=c.confrelid "
            "WHERE n.nspname='public' ORDER BY r.relname,c.conname"
        )).fetchall())
        if constraints != database.get("constraints"):
            raise ForestError("RESTORE_SCHEMA_MISMATCH", "Restore constraints differ", 409)
        current_sequences = _json_value(await (await conn.execute(
            "SELECT sequencename,start_value,min_value,max_value,increment_by,cycle "
            "FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename"
        )).fetchall())
        expected_sequences = [
            {key: value for key, value in sequence.items() if key != "last_value"}
            for sequence in database.get("sequences", [])
        ]
        if current_sequences != expected_sequences:
            raise ForestError("RESTORE_SCHEMA_MISMATCH", "Restore sequences differ", 409)

    @staticmethod
    async def _validate_foreign_keys(conn) -> None:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                "SELECT c.conname,child.relname AS child_table,parent.relname AS parent_table,"
                "c.confmatchtype,"
                "array_agg(child_column.attname ORDER BY keys.ordinality) AS child_columns,"
                "array_agg(parent_column.attname ORDER BY keys.ordinality) AS parent_columns "
                "FROM pg_constraint c JOIN pg_class child ON child.oid=c.conrelid "
                "JOIN pg_namespace child_ns ON child_ns.oid=child.relnamespace "
                "JOIN pg_class parent ON parent.oid=c.confrelid "
                "JOIN pg_namespace parent_ns ON parent_ns.oid=parent.relnamespace "
                "JOIN LATERAL unnest(c.conkey,c.confkey) WITH ORDINALITY "
                "AS keys(child_attnum,parent_attnum,ordinality) ON true "
                "JOIN pg_attribute child_column ON child_column.attrelid=child.oid "
                "AND child_column.attnum=keys.child_attnum "
                "JOIN pg_attribute parent_column ON parent_column.attrelid=parent.oid "
                "AND parent_column.attnum=keys.parent_attnum "
                "WHERE c.contype='f' AND child_ns.nspname='public' "
                "AND parent_ns.nspname='public' "
                "GROUP BY c.oid,c.conname,child.relname,parent.relname,c.confmatchtype "
                "ORDER BY child.relname,c.conname"
            )
            constraints = await cursor.fetchall()
        for constraint in constraints:
            child_table = constraint["child_table"]
            parent_table = constraint["parent_table"]
            child_columns = constraint["child_columns"]
            parent_columns = constraint["parent_columns"]
            if (
                not _NAME.fullmatch(child_table) or not _NAME.fullmatch(parent_table)
                or not child_columns or len(child_columns) != len(parent_columns)
                or any(not _NAME.fullmatch(name) for name in child_columns + parent_columns)
                or constraint["confmatchtype"] not in {"s", "f"}
            ):
                raise ForestError(
                    "BACKUP_SCHEMA_UNSUPPORTED", "Backup foreign key contract is invalid", 409
                )
            child_ref = sql.Identifier("child")
            parent_ref = sql.Identifier("parent")
            all_non_null = sql.SQL(" AND ").join(
                sql.SQL("{}.{} IS NOT NULL").format(child_ref, sql.Identifier(name))
                for name in child_columns
            )
            all_null = sql.SQL(" AND ").join(
                sql.SQL("{}.{} IS NULL").format(child_ref, sql.Identifier(name))
                for name in child_columns
            )
            matches = sql.SQL(" AND ").join(
                sql.SQL("{}.{} = {}.{}").format(
                    parent_ref, sql.Identifier(parent), child_ref, sql.Identifier(child)
                )
                for child, parent in zip(child_columns, parent_columns, strict=True)
            )
            missing_parent = sql.SQL("({} AND NOT EXISTS (SELECT 1 FROM public.{} AS {} WHERE {}))").format(
                all_non_null, sql.Identifier(parent_table), parent_ref, matches
            )
            if constraint["confmatchtype"] == "f":
                violation = sql.SQL("((NOT ({}) AND NOT ({})) OR {})").format(
                    all_null, all_non_null, missing_parent
                )
            else:
                violation = missing_parent
            query = sql.SQL("SELECT 1 FROM public.{} AS {} WHERE {} LIMIT 1").format(
                sql.Identifier(child_table), child_ref, violation
            )
            if await (await conn.execute(query)).fetchone() is not None:
                raise ForestError(
                    "RESTORE_FOREIGN_KEY_VIOLATION", "Restored foreign key validation failed", 409
                )

    @staticmethod
    async def _validate_environment(conn, database: dict[str, Any]) -> None:
        row = await (await conn.execute(
            "SELECT current_setting('server_version_num') AS server_version,"
            "current_setting('server_encoding') AS encoding,"
            "(SELECT datcollate FROM pg_database WHERE datname=current_database()) AS collation,"
            "(SELECT datctype FROM pg_database WHERE datname=current_database()) AS character_type"
        )).fetchone()
        if (
            row["server_version"] != database.get("server_version")
            or row["encoding"] != database.get("encoding")
            or row["collation"] != database.get("collation")
            or row["character_type"] != database.get("character_type")
        ):
            raise ForestError(
                "RESTORE_DATABASE_ENVIRONMENT_MISMATCH",
                "Restore database environment differs from the backup", 409,
            )

    @staticmethod
    async def _validate_identity(conn, database: dict[str, Any]) -> None:
        if database["scope"] == "deployment":
            row = await (await conn.execute(
                "SELECT setup_id,plan_digest FROM web_setup_state WHERE singleton"
            )).fetchone()
        else:
            row = await (await conn.execute(
                "SELECT i.bot_id,i.created_at,r.storage_generation,r.maintenance_epoch,"
                "r.policy_revision FROM forest_bot_identity i "
                "JOIN bot_runtime_state r USING(bot_id) WHERE i.singleton"
            )).fetchone()
        if row is None or digest_json(_json_value(row)) != database.get("identity_digest"):
            raise ForestError("RESTORE_IDENTITY_MISMATCH", "Restored database identity differs", 409)

    @staticmethod
    async def _quarantine(conn, database: dict[str, Any]) -> str | None:
        if database["scope"] != "bot":
            return None
        async with conn.transaction():
            row = await (await conn.execute(
                "UPDATE bot_runtime_state SET status='SEALED',storage_generation=storage_generation+1,"
                "maintenance_epoch=maintenance_epoch+1,data_admission=false,control_admission=false "
                "WHERE bot_id=%s RETURNING bot_id,status,storage_generation,maintenance_epoch,"
                "policy_revision,data_admission,control_admission", (database["bot_id"],)
            )).fetchone()
            await conn.execute(
                "UPDATE bot_host_lease SET host_id=NULL,incarnation=NULL,owner_epoch=owner_epoch+1,"
                "expires_at='1970-01-01 UTC' WHERE bot_id=%s", (database["bot_id"],)
            )
        return digest_json(_json_value(row))

    def _target_dsn(self, database_name: str) -> str:
        assert self.managed_dsn is not None
        values = conninfo_to_dict(self.managed_dsn)
        values["dbname"] = database_name
        values.pop("options", None)
        return make_conninfo(**values)

    async def _runtime_storage(self, target: dict[str, Any]) -> dict[str, Any] | None:
        if target["database_scope"] != "bot":
            return None
        try:
            storage = await self.catalog.resolve_storage(target["bot_id"])
        except ForestError as exc:
            if exc.code == "BOT_NOT_READY":
                return None
            raise
        # An external Bot may live on another cluster and its role is not managed
        # by this restore service. It remains an isolated, administrator-owned copy.
        return storage if storage["mode"] == "managed" else None

    @staticmethod
    def _manifest_digest(path: Path) -> str:
        with zipfile.ZipFile(path) as archive:
            return "sha256:" + hashlib.sha256(archive.read("manifest.json")).hexdigest()

    @staticmethod
    async def _now(conn):
        return (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]

    @staticmethod
    def _reset_job_root(path: Path) -> None:
        parent = path.parent.resolve()
        if path.exists():
            info = path.lstat()
            if path.is_symlink() or getattr(info, "st_reparse_tag", 0) or path.resolve().parent != parent:
                raise ForestError("RESTORE_STORAGE_ERROR", "Restore staging path is unsafe", 500)
            shutil.rmtree(path)
        path.mkdir(mode=0o700)

    @staticmethod
    def _remove_job_root(path: Path) -> None:
        if not path.exists():
            return
        info = path.lstat()
        if path.is_symlink() or getattr(info, "st_reparse_tag", 0):
            return
        if path.resolve().parent != path.parent.resolve():
            return
        with suppress(OSError):
            shutil.rmtree(path)
