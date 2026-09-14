"""Durable encrypted logical backups across the deployment and Bot databases."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forest.backup_artifact import (
    FORMAT,
    MAX_ARCHIVE,
    MAX_MEMBER,
    MAX_MEMBERS,
    encrypt_file,
    prepare_header,
)
from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import private_directory, private_file

_TABLE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_PUBLIC_KEYS = (
    "backup_id", "state", "include_packages", "database_total", "database_completed",
    "roster_start_digest", "roster_end_digest", "error_code", "owner_epoch", "created_at",
    "updated_at", "cleanup_id", "cleanup_state", "cleanup_error_code", "cleanup_completed_at",
)
_JOB_SELECT = (
    "SELECT j.*,a.relative_path,a.ciphertext_digest,a.byte_size,a.manifest_digest,"
    "a.ready_at,a.expires_at,a.deleted_at,c.cleanup_id,c.state AS cleanup_state,"
    "c.error_code AS cleanup_error_code,c.completed_at AS cleanup_completed_at "
    "FROM backup_jobs j "
    "LEFT JOIN backup_artifacts a USING(backup_id) "
    "LEFT JOIN storage_cleanup_jobs c ON c.resource_kind='BACKUP_ARTIFACT' "
    "AND c.resource_id=j.backup_id "
)


def _public(row: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: row[key].isoformat() if isinstance(row.get(key), datetime) else row.get(key)
        for key in _PUBLIC_KEYS
    }
    for key in ("ciphertext_digest", "byte_size", "manifest_digest", "ready_at", "expires_at"):
        value = row.get(key)
        result[key] = value.isoformat() if isinstance(value, datetime) else value
    result["download_available"] = bool(
        row.get("relative_path") and row.get("deleted_at") is None
        and row.get("expires_at") and row["expires_at"] > datetime.now(UTC)
    )
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


class BackupManager:
    def __init__(
        self, catalog, artifacts, data_dir: Path, deployment_dsn: str, *, lease_seconds: int = 3600
    ):
        if not 60 <= lease_seconds <= 86400:
            raise ValueError("Backup lease must be between one minute and one day")
        self.catalog = catalog
        self.artifacts = artifacts
        self.store = catalog.store
        self.vault = catalog.vault
        self.data_dir = private_directory(data_dir)
        self.backup_dir = private_directory(self.data_dir / "backups")
        self.deployment_dsn = deployment_dsn
        self.lease_seconds = lease_seconds
        self.owner_id = "backup-worker:" + str(uuid4())
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def _authorize(
        self, conn, subject_id: str, session_token_hash: str, expected_password_hash: str | None = None
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
        self,
        *,
        subject_id: str,
        session_token_hash: str,
        expected_password_hash: str,
        passphrase: str,
        passphrase_digest: str,
        include_packages: bool,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if type(include_packages) is not bool or not 1 <= len(idempotency_key) <= 128:
            raise ForestError("BACKUP_REQUEST_INVALID", "Backup request is invalid", 422)
        semantic = digest_json({
            "format": FORMAT,
            "include_packages": include_packages,
            "passphrase_digest": passphrase_digest,
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
                        "IDEMPOTENCY_CONFLICT", "Backup key was used for different input", 409
                    )
                return _public(previous)

        backup_id = str(uuid4())
        header, archive_key = await asyncio.to_thread(prepare_header, backup_id, passphrase)
        encrypted_key = self.vault.encrypt(base64.urlsafe_b64encode(archive_key).decode("ascii"))
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170788)")
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                _JOB_SELECT + "WHERE j.subject_id=%s AND j.idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Backup key was used for different input", 409
                    )
                return _public(previous)
            row = await (await conn.execute(
                "INSERT INTO backup_jobs(backup_id,subject_id,idempotency_key,input_digest,"
                "include_packages,archive_key_ciphertext,archive_header) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (backup_id, subject_id, idempotency_key, semantic, include_packages,
                 encrypted_key, Jsonb(header)),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_events(backup_id,owner_epoch,step) VALUES (%s,0,'QUEUED')",
                (backup_id,),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'backup.create',%s,%s,%s)",
                (str(uuid4()), subject_id, "backup:" + backup_id, backup_id,
                 Jsonb({"backup_id": backup_id, "include_packages": include_packages})),
            )
            return _public(row)

    async def list(
        self, *, subject_id: str, session_token_hash: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ForestError("BACKUP_QUERY_INVALID", "Backup limit is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                _JOB_SELECT + "ORDER BY j.created_at DESC,j.backup_id DESC LIMIT %s", (limit,)
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return [_public(row) for row in rows]

    async def read(
        self, *, backup_id: str, subject_id: str, session_token_hash: str
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                _JOB_SELECT + "WHERE j.backup_id=%s", (backup_id,)
            )).fetchone()
            if row is None:
                raise ForestError("BACKUP_NOT_FOUND", "Backup does not exist", 404)
            cuts = await (await conn.execute(
                "SELECT database_scope,bot_id,identity_digest,migration_digest,transaction_snapshot,"
                "wal_lsn,deletion_epoch,member_count,byte_size,content_digest,captured_at "
                "FROM backup_database_cuts WHERE backup_id=%s "
                "ORDER BY database_scope,bot_id", (backup_id,)
            )).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            result = _public(row)
            result["databases"] = [
                {key: value.isoformat() if isinstance(value, datetime) else value
                 for key, value in cut.items()}
                for cut in cuts
            ]
            return result

    async def artifact(
        self, *, backup_id: str, subject_id: str, session_token_hash: str
    ) -> tuple[Path, dict[str, Any]]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                "SELECT j.*,a.relative_path,a.ciphertext_digest,a.byte_size,a.manifest_digest,"
                "a.ready_at,a.expires_at,a.deleted_at FROM backup_jobs j "
                "JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.backup_id=%s AND j.state IN ('READY','INCONSISTENT') "
                "AND a.deleted_at IS NULL "
                "AND a.expires_at>clock_timestamp()", (backup_id,)
            )).fetchone()
            await self._authorize(conn, subject_id, session_token_hash)
        if row is None:
            raise ForestError("BACKUP_DOWNLOAD_UNAVAILABLE", "Backup download is unavailable", 404)
        path = (self.backup_dir / row["relative_path"]).resolve()
        if path.parent != self.backup_dir.resolve() or not path.is_file() or path.is_symlink():
            raise ForestError("BACKUP_STORAGE_ERROR", "Backup artifact is unavailable", 503)
        digest = await asyncio.to_thread(self._file_digest, path)
        if digest != row["ciphertext_digest"] or path.stat().st_size != row["byte_size"]:
            raise ForestError("BACKUP_INTEGRITY_FAILED", "Backup artifact integrity check failed", 503)
        return path, _public(row)

    @staticmethod
    def _file_digest(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        return "sha256:" + digest.hexdigest()

    async def _claim(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            locked = await (await conn.execute(
                "SELECT pg_try_advisory_xact_lock(679330170789) AS locked"
            )).fetchone()
            if not locked["locked"]:
                return None
            active = await (await conn.execute(
                "SELECT 1 FROM backup_jobs WHERE state IN ('CAPTURING','SEALING') "
                "AND lease_expires_at>clock_timestamp() LIMIT 1"
            )).fetchone()
            if active is not None:
                return None
            row = await (await conn.execute(
                "SELECT * FROM backup_jobs WHERE state='QUEUED' OR "
                "(state IN ('CAPTURING','SEALING') AND lease_expires_at<=clock_timestamp()) "
                "ORDER BY (state IN ('CAPTURING','SEALING')) DESC,created_at,backup_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is None:
                return None
            claimed = await (await conn.execute(
                "UPDATE backup_jobs SET state='CAPTURING',owner_id=%s,owner_epoch=owner_epoch+1,"
                "lease_expires_at=clock_timestamp()+(%s*interval '1 second'),error_code=NULL,"
                "database_completed=0,updated_at=clock_timestamp() WHERE backup_id=%s RETURNING *",
                (self.owner_id, self.lease_seconds, row["backup_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO backup_events(backup_id,owner_epoch,step) VALUES (%s,%s,'CAPTURING')",
                (claimed["backup_id"], claimed["owner_epoch"]),
            )
            return claimed

    async def _heartbeat(self, job: dict[str, Any], *, completed: int | None = None) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            updated = await (await conn.execute(
                "UPDATE backup_jobs SET lease_expires_at=clock_timestamp()+(%s*interval '1 second'),"
                "database_completed=COALESCE(%s,database_completed),updated_at=clock_timestamp() "
                "WHERE backup_id=%s AND owner_id=%s AND owner_epoch=%s "
                "AND state IN ('CAPTURING','SEALING') AND lease_expires_at>clock_timestamp() RETURNING 1",
                (self.lease_seconds, completed, job["backup_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if updated is None:
                raise ForestError("BACKUP_LEASE_LOST", "Backup ownership was lost", 409)

    async def _renew_lease(self, job: dict[str, Any]) -> None:
        interval = max(10.0, self.lease_seconds / 3)
        while True:
            await asyncio.sleep(interval)
            await self._heartbeat(job)

    async def run_once(self) -> dict[str, Any] | None:
        job = await self._claim()
        if job is None:
            return None
        try:
            result = await self._capture(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, ForestError) else "BACKUP_FAILED"
            async with self.store.pool.connection() as conn, conn.transaction():
                failed = await (await conn.execute(
                    "UPDATE backup_jobs SET state='FAILED',owner_id=NULL,lease_expires_at=NULL,"
                    "error_code=%s,updated_at=clock_timestamp() WHERE backup_id=%s AND owner_id=%s "
                    "AND owner_epoch=%s AND state IN ('CAPTURING','SEALING') RETURNING 1",
                    (code, job["backup_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if failed is not None:
                    await conn.execute(
                        "INSERT INTO backup_events(backup_id,owner_epoch,step,details) "
                        "VALUES (%s,%s,'FAILED',%s)",
                        (job["backup_id"], job["owner_epoch"], Jsonb({"error_code": code})),
                    )
            return await self.read_internal(job["backup_id"])
        return result

    async def run_forever(self) -> None:
        self.closed = False
        while not self.closed:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(1)

    async def read_internal(self, backup_id: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn:
            return await (await conn.execute(
                _JOB_SELECT + "WHERE j.backup_id=%s", (backup_id,)
            )).fetchone()

    async def _roster_anchor(self) -> tuple[list[dict[str, Any]], str]:
        async with self.store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT b.bot_id,b.revision,b.job_id,h.storage_generation,g.plan_digest "
                "FROM bot_catalog b JOIN bot_storage_heads h USING(bot_id) "
                "JOIN bot_storage_generations g USING(bot_id,storage_generation) "
                "WHERE b.stage='READY' ORDER BY b.bot_id"
            )).fetchall()
        anchor = _json_value(rows)
        return anchor, digest_json(anchor)

    async def _roster(self) -> tuple[list[tuple[str, str, str]], list[dict[str, Any]], str]:
        anchor, anchor_digest = await self._roster_anchor()
        result = [("deployment", "", self.deployment_dsn)]
        for row in anchor:
            result.append(("bot", row["bot_id"], await self.catalog.resolve_dsn(row["bot_id"])))
        return result, anchor, anchor_digest

    async def _capture(self, job: dict[str, Any]) -> dict[str, Any]:
        capture = asyncio.create_task(self._capture_owned(job), name="backup-capture-owned")
        renewal = asyncio.create_task(self._renew_lease(job), name="backup-capture-lease")
        try:
            done, _ = await asyncio.wait({capture, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if capture in done:
                return await capture
            error = renewal.exception()
            capture.cancel()
            await asyncio.gather(capture, return_exceptions=True)
            if error is not None:
                raise error
            raise ForestError("BACKUP_LEASE_LOST", "Backup ownership was lost", 409)
        finally:
            capture.cancel()
            renewal.cancel()
            await asyncio.gather(capture, renewal, return_exceptions=True)

    async def _capture_owned(self, job: dict[str, Any]) -> dict[str, Any]:
        roster, roster_start, roster_start_digest = await self._roster()
        if len(roster) > 1024:
            raise ForestError("BACKUP_DATABASE_CAPACITY", "Backup database limit was exceeded", 429)
        async with self.store.pool.connection() as conn, conn.transaction():
            changed = await (await conn.execute(
                "UPDATE backup_jobs SET database_total=%s,roster_start_digest=%s,"
                "updated_at=clock_timestamp() "
                "WHERE backup_id=%s AND owner_id=%s AND owner_epoch=%s "
                "AND state='CAPTURING' RETURNING 1",
                (len(roster), roster_start_digest, job["backup_id"], self.owner_id,
                 job["owner_epoch"]),
            )).fetchone()
            if changed is None:
                raise ForestError("BACKUP_LEASE_LOST", "Backup ownership was lost", 409)
        job_root = self.backup_dir / (
            ".job-" + job["backup_id"] + "-" + str(job["owner_epoch"])
        )
        await self._run_blocking(self._reset_job_root, job_root)
        plain = job_root / "payload.zip"
        encrypted = job_root / "sealed.tmp"
        final = self.backup_dir / (
            job["backup_id"] + "." + str(job["owner_epoch"]) + ".forest-backup"
        )
        members: list[dict[str, Any]] = []
        databases: list[dict[str, Any]] = []
        try:
            with zipfile.ZipFile(plain, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for index, (scope, bot_id, dsn) in enumerate(roster):
                    cut = await self._capture_database(
                        archive, members, scope=scope, bot_id=bot_id or None, dsn=dsn
                    )
                    databases.append(cut)
                    await self._heartbeat(job, completed=index + 1)
                await self._member_file(archive, members, "recovery/master.key", self.data_dir / "master.key")
                if job["include_packages"]:
                    await self._packages(archive, members)
                roster_end, roster_end_digest = await self._roster_anchor()
                roster_consistent = roster_start_digest == roster_end_digest
                manifest = {
                    "format": FORMAT,
                    "backup_id": job["backup_id"],
                    "created_at": datetime.now(UTC).isoformat(),
                    "databases": databases,
                    "members": members,
                    "includes": {"recovery_key": True, "packages": job["include_packages"]},
                    "roster": {
                        "start": roster_start,
                        "end": roster_end,
                        "start_digest": roster_start_digest,
                        "end_digest": roster_end_digest,
                        "consistent": roster_consistent,
                    },
                    "activation": "ISOLATED_RESTORE_REQUIRED",
                }
                manifest_bytes = json.dumps(
                    manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode("utf-8")
                archive.writestr("manifest.json", manifest_bytes)
            manifest_digest = "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()
            async with self.store.pool.connection() as conn, conn.transaction():
                updated = await (await conn.execute(
                    "UPDATE backup_jobs SET state='SEALING',updated_at=clock_timestamp() "
                    "WHERE backup_id=%s AND owner_id=%s AND owner_epoch=%s AND state='CAPTURING' "
                    "AND lease_expires_at>clock_timestamp() RETURNING 1",
                    (job["backup_id"], self.owner_id, job["owner_epoch"]),
                )).fetchone()
                if updated is None:
                    raise ForestError("BACKUP_LEASE_LOST", "Backup ownership was lost", 409)
                await conn.execute(
                    "INSERT INTO backup_events(backup_id,owner_epoch,step) VALUES (%s,%s,'SEALING')",
                    (job["backup_id"], job["owner_epoch"]),
                )
            key = base64.urlsafe_b64decode(self.vault.decrypt(job["archive_key_ciphertext"]))
            sealed = await self._run_blocking(
                encrypt_file, plain, encrypted, job["archive_header"], key
            )
            cuts = [self._cut_row(job["backup_id"], database) for database in databases]
            async with self.store.pool.connection() as conn, conn.transaction():
                current = await (await conn.execute(
                    "SELECT * FROM backup_jobs WHERE backup_id=%s FOR UPDATE", (job["backup_id"],)
                )).fetchone()
                if (
                    current is None or current["state"] != "SEALING"
                    or current["owner_id"] != self.owner_id
                    or current["owner_epoch"] != job["owner_epoch"]
                    or current["lease_expires_at"] <= await self._now(conn)
                ):
                    raise ForestError("BACKUP_LEASE_LOST", "Backup ownership was lost", 409)
                await self._run_blocking(os.replace, encrypted, final)
                await self._run_blocking(self._sync_directory, self.backup_dir)
                if current["lease_expires_at"] <= await self._now(conn):
                    raise ForestError("BACKUP_LEASE_LOST", "Backup ownership was lost", 409)
                for values in cuts:
                    await conn.execute(
                        "INSERT INTO backup_database_cuts(backup_id,database_scope,bot_id,identity_digest,"
                        "migration_digest,transaction_snapshot,wal_lsn,deletion_epoch,member_count,"
                        "byte_size,content_digest,captured_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", values,
                    )
                expires = datetime.now(UTC) + timedelta(days=30)
                await conn.execute(
                    "INSERT INTO backup_artifacts(backup_id,relative_path,ciphertext_digest,byte_size,"
                    "manifest_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s)",
                    (job["backup_id"], final.name, sealed["ciphertext_digest"], sealed["byte_size"],
                     manifest_digest, expires),
                )
                final_state = "READY" if roster_consistent else "INCONSISTENT"
                ready = await (await conn.execute(
                    "UPDATE backup_jobs SET state=%s,owner_id=NULL,lease_expires_at=NULL,"
                    "database_completed=database_total,error_code=NULL,updated_at=clock_timestamp(),"
                    "roster_end_digest=%s WHERE backup_id=%s RETURNING *",
                    (final_state, roster_end_digest, job["backup_id"]),
                )).fetchone()
                await conn.execute(
                    "INSERT INTO backup_events(backup_id,owner_epoch,step,details) "
                    "VALUES (%s,%s,%s,%s)",
                    (job["backup_id"], job["owner_epoch"], final_state, Jsonb({
                        "byte_size": sealed["byte_size"],
                        "ciphertext_digest": sealed["ciphertext_digest"],
                        "roster_consistent": roster_consistent,
                    })),
                )
            return _public({**ready, **sealed, "manifest_digest": manifest_digest,
                            "expires_at": expires, "relative_path": final.name})
        finally:
            await self._run_blocking(self._remove_job_root, job_root)

    @staticmethod
    async def _now(conn):
        return (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]

    @staticmethod
    def _reset_job_root(path: Path) -> None:
        parent = path.parent.resolve()
        if path.exists():
            info = path.lstat()
            if path.is_symlink() or getattr(info, "st_reparse_tag", 0) or path.resolve().parent != parent:
                raise ForestError("BACKUP_STORAGE_ERROR", "Backup staging path is unsafe", 500)
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
        shutil.rmtree(path)

    @staticmethod
    def _sync_directory(path: Path) -> None:
        if os.name != "nt":
            descriptor = os.open(path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    @staticmethod
    def _cut_row(backup_id: str, cut: dict[str, Any]) -> tuple:
        return (
            backup_id, cut["scope"], cut.get("bot_id") or "", cut["identity_digest"],
            cut["migration_digest"], cut["transaction_snapshot"], cut["wal_lsn"],
            cut.get("deletion_epoch"), cut["member_count"], cut["byte_size"],
            cut["content_digest"], datetime.fromisoformat(cut["captured_at"]),
        )

    async def _capture_database(
        self, archive: zipfile.ZipFile, members: list[dict[str, Any]], *,
        scope: str, bot_id: str | None, dsn: str,
    ) -> dict[str, Any]:
        async with await psycopg.AsyncConnection.connect(
            dsn, row_factory=dict_row, autocommit=True, connect_timeout=10
        ) as conn:
            await conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            try:
                base = await (await conn.execute(
                    "SELECT current_database() AS database_name,current_setting('server_version_num') AS server_version,"
                    "current_setting('server_encoding') AS encoding,txid_current_snapshot()::text AS snapshot,"
                    "pg_current_wal_lsn()::text AS wal_lsn,clock_timestamp() AS captured_at,"
                    "(SELECT datcollate FROM pg_database WHERE datname=current_database()) AS collation,"
                    "(SELECT datctype FROM pg_database WHERE datname=current_database()) AS character_type"
                )).fetchone()
                if scope == "deployment":
                    identity = await (await conn.execute(
                        "SELECT setup_id,plan_digest FROM web_setup_state WHERE singleton"
                    )).fetchone()
                    migrations = await (await conn.execute(
                        "SELECT version,checksum FROM forest_schema_migrations ORDER BY version"
                    )).fetchall()
                    deletion_epoch = None
                    prefix = "deployment"
                    excluded = {"forest_schema_migrations"}
                else:
                    identity = await (await conn.execute(
                        "SELECT i.bot_id,i.created_at,r.storage_generation,r.maintenance_epoch,"
                        "r.policy_revision FROM public.forest_bot_identity i "
                        "JOIN public.bot_runtime_state r USING(bot_id) WHERE i.singleton"
                    )).fetchone()
                    if identity is None or identity["bot_id"] != bot_id:
                        raise ForestError("BACKUP_IDENTITY_MISMATCH", "Bot database identity is invalid", 409)
                    migrations = await (await conn.execute(
                        "SELECT version,checksum FROM public.forest_bot_schema_migrations "
                        "ORDER BY version"
                    )).fetchall()
                    clock = await (await conn.execute(
                        "SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s", (bot_id,)
                    )).fetchone()
                    deletion_epoch = clock["deletion_epoch"] if clock else 0
                    prefix = "bots/" + bot_id
                    excluded = {"forest_bot_schema_migrations"}
                if identity is None:
                    raise ForestError("BACKUP_IDENTITY_MISMATCH", "Database identity is unavailable", 409)
                identity = _json_value(identity)
                migrations = _json_value(migrations)
                table_rows = await (await conn.execute(
                    "SELECT c.relname AS table_name,a.attnum,a.attname AS column_name,"
                    "format_type(a.atttypid,a.atttypmod) AS data_type,a.attnotnull,a.attidentity,a.attgenerated "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
                    "WHERE n.nspname='public' AND c.relkind='r' ORDER BY c.relname,a.attnum"
                )).fetchall()
                tables: dict[str, list[dict[str, Any]]] = {}
                for row in table_rows:
                    if row["table_name"] in excluded:
                        continue
                    if not _TABLE.fullmatch(row["table_name"]) or not _TABLE.fullmatch(row["column_name"]):
                        raise ForestError("BACKUP_SCHEMA_UNSUPPORTED", "Database schema name is unsupported", 409)
                    tables.setdefault(row["table_name"], []).append({
                        "name": row["column_name"], "type": row["data_type"],
                        "not_null": row["attnotnull"], "identity": row["attidentity"],
                        "generated": row["attgenerated"],
                    })
                constraints = _json_value(await (await conn.execute(
                    "SELECT c.conname,c.contype,r.relname AS table_name,"
                    "COALESCE(fr.relname,'') AS referenced_table,pg_get_constraintdef(c.oid,true) AS definition "
                    "FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid "
                    "JOIN pg_namespace n ON n.oid=r.relnamespace "
                    "LEFT JOIN pg_class fr ON fr.oid=c.confrelid "
                    "WHERE n.nspname='public' ORDER BY r.relname,c.conname"
                )).fetchall())
                sequences = _json_value(await (await conn.execute(
                    "SELECT sequencename,start_value,min_value,max_value,increment_by,cycle,last_value "
                    "FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename"
                )).fetchall())
                exported, table_manifest = [], []
                for index, (table, columns) in enumerate(tables.items()):
                    name = f"{prefix}/tables/{index:04d}-{table}.bin"
                    count = (await (await conn.execute(
                        sql.SQL("SELECT count(*) AS n FROM public.{}").format(sql.Identifier(table))
                    )).fetchone())["n"]
                    copied_columns = [
                        column for column in columns if not column["generated"]
                    ]
                    digest, size = hashlib.sha256(), 0
                    query = sql.SQL("COPY public.{} ({}) TO STDOUT (FORMAT BINARY)").format(
                        sql.Identifier(table),
                        sql.SQL(",").join(
                            sql.Identifier(column["name"]) for column in copied_columns
                        ),
                    )
                    with archive.open(name, "w", force_zip64=True) as output:
                        previous_size = sum(item["size"] for item in members)
                        async with conn.cursor().copy(query) as copy:
                            async for chunk in copy:
                                value = bytes(chunk)
                                output.write(value)
                                digest.update(value)
                                size += len(value)
                                if size > MAX_MEMBER or previous_size + size > MAX_ARCHIVE:
                                    raise ForestError(
                                        "BACKUP_TOO_LARGE", "Backup archive exceeds its size limit", 413
                                    )
                    item = {"name": name, "size": size, "digest": "sha256:" + digest.hexdigest()}
                    if len(members) >= MAX_MEMBERS:
                        raise ForestError(
                            "BACKUP_MEMBER_CAPACITY", "Backup member limit was exceeded", 429
                        )
                    members.append(item)
                    exported.append(item)
                    table_manifest.append({
                        "name": table, "member": name, "rows": count, "columns": columns,
                        "copied_columns": [column["name"] for column in copied_columns],
                    })
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
        cut_digest = digest_json([[item["name"], item["digest"]] for item in exported])
        migration_digest = digest_json([[row["version"], row["checksum"]] for row in migrations])
        return {
            "scope": scope,
            "bot_id": bot_id,
            "identity": identity,
            "identity_digest": digest_json(identity),
            "migration_digest": migration_digest,
            "migrations": migrations,
            "database_name_digest": digest_json(base["database_name"]),
            "server_version": base["server_version"],
            "encoding": base["encoding"],
            "collation": base["collation"],
            "character_type": base["character_type"],
            "transaction_snapshot": base["snapshot"],
            "wal_lsn": base["wal_lsn"],
            "deletion_epoch": deletion_epoch,
            "captured_at": base["captured_at"].isoformat(),
            "tables": table_manifest,
            "constraints": constraints,
            "sequences": sequences,
            "member_count": len(exported),
            "byte_size": sum(item["size"] for item in exported),
            "content_digest": cut_digest,
        }

    async def _member_file(
        self, archive: zipfile.ZipFile, members: list[dict[str, Any]], name: str, path: Path
    ) -> None:
        if path.is_symlink() or not path.is_file():
            raise ForestError("BACKUP_RECOVERY_KEY_UNAVAILABLE", "Recovery key is unavailable", 503)
        private_file(path)
        digest, size = hashlib.sha256(), 0
        previous_size = sum(item["size"] for item in members)
        with path.open("rb") as source, archive.open(name, "w", force_zip64=True) as output:
            while chunk := source.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
                size += len(chunk)
                if size > MAX_MEMBER or previous_size + size > MAX_ARCHIVE:
                    raise ForestError(
                        "BACKUP_TOO_LARGE", "Backup archive exceeds its size limit", 413
                    )
        if len(members) >= MAX_MEMBERS:
            raise ForestError("BACKUP_MEMBER_CAPACITY", "Backup member limit was exceeded", 429)
        members.append({"name": name, "size": size, "digest": "sha256:" + digest.hexdigest()})

    async def _packages(self, archive: zipfile.ZipFile, members: list[dict[str, Any]]) -> None:
        async with self.store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT package_digest FROM web_packages ORDER BY package_digest"
            )).fetchall()
        for row in rows:
            digest = row["package_digest"]
            data = await self._run_blocking(self.artifacts.export_archive, digest)
            name = "artifacts/" + digest.removeprefix("sha256:") + ".zip"
            if (
                len(data) > MAX_MEMBER
                or sum(item["size"] for item in members) + len(data) > MAX_ARCHIVE
            ):
                raise ForestError("BACKUP_TOO_LARGE", "Backup archive exceeds its size limit", 413)
            if len(members) >= MAX_MEMBERS:
                raise ForestError("BACKUP_MEMBER_CAPACITY", "Backup member limit was exceeded", 429)
            await self._run_blocking(archive.writestr, name, data)
            members.append({
                "name": name, "size": len(data),
                "digest": "sha256:" + hashlib.sha256(data).hexdigest(),
            })

    @staticmethod
    async def _run_blocking(function, *arguments):
        task = asyncio.create_task(asyncio.to_thread(function, *arguments))
        try:
            return await asyncio.shield(task)
        except BaseException:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                task.exception()
            raise
