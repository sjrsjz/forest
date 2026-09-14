"""Plan, tombstone, and physically clean Forest-owned backup and restore resources."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import stat
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json

BACKUP = "BACKUP_ARTIFACT"
RESTORE = "RESTORE_STAGE"
_BACKUP_NAME = re.compile(
    r"^(?P<id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\."
    r"(?P<epoch>[1-9][0-9]*)\.forest-backup$"
)
_JOB_DIRECTORY = re.compile(
    r"^\.job-(?P<id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-"
    r"(?P<epoch>[1-9][0-9]*)$"
)


def _time(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _public(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _time(row.get(key))
        for key in (
            "cleanup_id", "resource_kind", "resource_id", "reason", "state", "owner_epoch",
            "attempt_count", "error_code", "next_attempt_at", "completed_at", "created_at",
            "updated_at",
        )
    }


class StorageCleanupManager:
    """A deployment worker whose only authority is already-tombstoned exact resources."""

    def __init__(
        self, catalog, backups, *, lease_seconds: int = 300, orphan_grace_seconds: int = 7200,
    ):
        if not 60 <= lease_seconds <= 3600:
            raise ValueError("Cleanup lease must be between one minute and one hour")
        if not 60 <= orphan_grace_seconds <= 604800:
            raise ValueError("Orphan grace must be between one minute and seven days")
        self.catalog = catalog
        self.backups = backups
        self.store = catalog.store
        self.managed_dsn = catalog.managed_dsn
        self.backup_dir = backups.backup_dir
        self.lease_seconds = lease_seconds
        self.orphan_grace_seconds = orphan_grace_seconds
        self.owner_id = "storage-cleanup:" + str(uuid4())
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

    async def _resource(self, conn, kind: str, resource_id: str, *, lock: bool) -> dict[str, Any]:
        suffix = " FOR UPDATE" if lock else ""
        if kind == BACKUP:
            row = await (await conn.execute(
                "SELECT j.backup_id AS resource_id,j.state,a.relative_path,a.ciphertext_digest,"
                "a.byte_size,a.manifest_digest,a.expires_at,a.deleted_at FROM backup_jobs j "
                "JOIN backup_artifacts a USING(backup_id) WHERE j.backup_id=%s" + suffix,
                (resource_id,),
            )).fetchone()
            if row is None:
                raise ForestError("BACKUP_NOT_FOUND", "Backup does not exist", 404)
            allowed = {"READY", "INCONSISTENT"}
        elif kind == RESTORE:
            row = await (await conn.execute(
                "SELECT j.restore_id AS resource_id,j.backup_id,j.verification_id,j.state,j.manifest_digest,"
                "j.database_total,j.database_completed,j.expires_at "
                "FROM restore_stage_jobs j WHERE j.restore_id=%s" + suffix,
                (resource_id,),
            )).fetchone()
            if row is None:
                raise ForestError("RESTORE_NOT_FOUND", "Restore stage does not exist", 404)
            targets = await (await conn.execute(
                "SELECT ordinal,database_name,content_digest FROM restore_stage_targets "
                "WHERE restore_id=%s ORDER BY ordinal", (resource_id,)
            )).fetchall()
            row = {**row, "targets": [list(target.values()) for target in targets]}
            allowed = {"STAGED", "FAILED"}
        else:
            raise ForestError("CLEANUP_RESOURCE_INVALID", "Cleanup resource is invalid", 422)
        if row["state"] not in allowed:
            raise ForestError(
                "CLEANUP_STATE_CONFLICT", "Resource cannot be cleaned in its current state", 409,
                {"current_state": row["state"]},
            )
        return row

    @staticmethod
    def _resource_digest(kind: str, row: dict[str, Any]) -> str:
        if kind == BACKUP:
            value = {
                key: _time(row[key]) for key in (
                    "resource_id", "state", "relative_path", "ciphertext_digest", "byte_size",
                    "manifest_digest", "expires_at", "deleted_at",
                )
            }
        else:
            value = {
                key: _time(row[key]) for key in (
                    "resource_id", "backup_id", "verification_id", "state", "manifest_digest", "database_total",
                    "database_completed", "expires_at", "targets",
                )
            }
        return digest_json(value)

    async def plan(
        self, *, kind: str, resource_id: str, subject_id: str, session_token_hash: str,
    ) -> dict[str, Any]:
        plan_id = str(uuid4())
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await self._resource(conn, kind, resource_id, lock=True)
            resource_digest = self._resource_digest(kind, row)
            now = (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]
            expires = now + timedelta(minutes=5)
            plan_digest = digest_json({
                "plan_id": plan_id, "subject_id": subject_id, "resource_kind": kind,
                "resource_id": resource_id, "resource_digest": resource_digest,
            })
            await conn.execute(
                "INSERT INTO storage_cleanup_plans(plan_id,subject_id,resource_kind,resource_id,"
                "resource_digest,plan_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (plan_id, subject_id, kind, resource_id, resource_digest, plan_digest, expires),
            )
            return {
                "plan_id": plan_id, "plan_digest": plan_digest, "resource_kind": kind,
                "resource_id": resource_id, "expires_at": expires.isoformat(),
                "before": {"state": row["state"]},
                "after": {"state": "EXPIRED", "visibility": "REVOKED", "cleanup": "QUEUED"},
            }

    async def apply(
        self, *, plan_id: str, plan_digest: str, subject_id: str, session_token_hash: str,
        expected_password_hash: str, idempotency_key: str,
    ) -> dict[str, Any]:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable cleanup key is required", 422)
        semantic = digest_json({"plan_id": plan_id, "plan_digest": plan_digest})
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170794)")
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                "SELECT input_digest,result FROM storage_cleanup_requests "
                "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Cleanup key was used for different input", 409
                    )
                return previous["result"]
            plan = await (await conn.execute(
                "SELECT *,expires_at>clock_timestamp() AS unexpired FROM storage_cleanup_plans "
                "WHERE plan_id=%s AND subject_id=%s FOR UPDATE", (plan_id, subject_id)
            )).fetchone()
            if plan is None or plan["plan_digest"] != plan_digest:
                raise ForestError("PLAN_NOT_FOUND", "Cleanup plan is unavailable", 404)
            if not plan["unexpired"] or plan["applied_cleanup_id"] is not None:
                raise ForestError("PLAN_EXPIRED", "Preview cleanup again", 409)
            resource = await self._resource(
                conn, plan["resource_kind"], plan["resource_id"], lock=True
            )
            if self._resource_digest(plan["resource_kind"], resource) != plan["resource_digest"]:
                raise ForestError("PLAN_CHANGED", "Cleanup resource changed; preview again", 409)
            if plan["resource_kind"] == BACKUP:
                active = await (await conn.execute(
                    "SELECT 1 FROM restore_stage_jobs WHERE backup_id=%s AND state IN "
                    "('QUEUED','VERIFYING','RESTORING','VALIDATING') UNION ALL "
                    "SELECT 1 FROM backup_verification_jobs WHERE backup_id=%s "
                    "AND state IN ('QUEUED','VERIFYING') LIMIT 1",
                    (plan["resource_id"], plan["resource_id"]),
                )).fetchone()
                if active is not None:
                    raise ForestError(
                        "BACKUP_RESTORE_ACTIVE",
                        "An active verification or restore still depends on this backup", 409,
                    )
            cleanup_id = str(uuid4())
            result = await self._tombstone(
                conn, kind=plan["resource_kind"], resource=resource,
                cleanup_id=cleanup_id, subject_id=subject_id, reason="EXPLICIT",
                input_digest=semantic,
            )
            await conn.execute(
                "UPDATE storage_cleanup_plans SET applied_cleanup_id=%s WHERE plan_id=%s",
                (cleanup_id, plan_id),
            )
            await conn.execute(
                "INSERT INTO storage_cleanup_requests(request_id,subject_id,idempotency_key,"
                "input_digest,result) VALUES (%s,%s,%s,%s,%s)",
                (str(uuid4()), subject_id, idempotency_key, semantic, Jsonb(result)),
            )
            return result

    async def _tombstone(
        self, conn, *, kind: str, resource: dict[str, Any], cleanup_id: str,
        subject_id: str, reason: str, input_digest: str,
    ) -> dict[str, Any]:
        resource_id = resource["resource_id"]
        job = await (await conn.execute(
            "INSERT INTO storage_cleanup_jobs(cleanup_id,resource_kind,resource_id,subject_id,"
            "reason,input_digest) VALUES (%s,%s,%s,%s,%s,%s) RETURNING *",
            (cleanup_id, kind, resource_id, subject_id, reason, input_digest),
        )).fetchone()
        if kind == BACKUP:
            await conn.execute(
                "UPDATE backup_artifacts SET deleted_at=clock_timestamp() "
                "WHERE backup_id=%s AND deleted_at IS NULL", (resource_id,)
            )
            await conn.execute(
                "UPDATE backup_jobs SET state='EXPIRED',updated_at=clock_timestamp() "
                "WHERE backup_id=%s", (resource_id,)
            )
            await conn.execute(
                "INSERT INTO backup_events(backup_id,owner_epoch,step,details) "
                "SELECT backup_id,owner_epoch,'EXPIRED',%s FROM backup_jobs WHERE backup_id=%s",
                (Jsonb({"reason": reason, "cleanup_id": cleanup_id}), resource_id),
            )
            action, key = "backup.cleanup.request", "backup:" + resource_id
        else:
            await conn.execute(
                "UPDATE restore_stage_jobs SET state='EXPIRED',updated_at=clock_timestamp() "
                "WHERE restore_id=%s", (resource_id,)
            )
            await conn.execute(
                "INSERT INTO restore_stage_events(restore_id,owner_epoch,step,details) "
                "SELECT restore_id,owner_epoch,'EXPIRED',%s FROM restore_stage_jobs "
                "WHERE restore_id=%s",
                (Jsonb({"reason": reason, "cleanup_id": cleanup_id}), resource_id),
            )
            action, key = "restore.cleanup.request", "restore:" + resource_id
        await conn.execute(
            "INSERT INTO storage_cleanup_events(cleanup_id,owner_epoch,step,details) "
            "VALUES (%s,0,'QUEUED',%s)",
            (cleanup_id, Jsonb({"reason": reason})),
        )
        await conn.execute(
            "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid4()), subject_id, action, key, cleanup_id,
             Jsonb({"cleanup_id": cleanup_id, "reason": reason})),
        )
        return _public(job)

    async def read(
        self, *, cleanup_id: str, subject_id: str, session_token_hash: str,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                "SELECT * FROM storage_cleanup_jobs WHERE cleanup_id=%s", (cleanup_id,)
            )).fetchone()
            if row is None:
                raise ForestError("CLEANUP_NOT_FOUND", "Cleanup job does not exist", 404)
            await self._authorize(conn, subject_id, session_token_hash)
            return _public(row)

    async def retry(
        self, *, cleanup_id: str, subject_id: str, session_token_hash: str,
        expected_password_hash: str, idempotency_key: str,
    ) -> dict[str, Any]:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable cleanup key is required", 422)
        semantic = digest_json({"action": "cleanup.retry", "cleanup_id": cleanup_id})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, expected_password_hash)
            previous = await (await conn.execute(
                "SELECT input_digest,result FROM storage_cleanup_requests "
                "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if previous is not None:
                if previous["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Cleanup key was used for different input", 409
                    )
                return previous["result"]
            row = await (await conn.execute(
                "UPDATE storage_cleanup_jobs SET state='QUEUED',error_code=NULL,"
                "next_attempt_at=clock_timestamp(),updated_at=clock_timestamp() "
                "WHERE cleanup_id=%s AND state='BLOCKED' RETURNING *", (cleanup_id,)
            )).fetchone()
            if row is None:
                current = await (await conn.execute(
                    "SELECT state FROM storage_cleanup_jobs WHERE cleanup_id=%s", (cleanup_id,)
                )).fetchone()
                if current is None:
                    raise ForestError("CLEANUP_NOT_FOUND", "Cleanup job does not exist", 404)
                raise ForestError(
                    "CLEANUP_RETRY_CONFLICT", "Only a blocked cleanup can be retried", 409,
                    {"current_state": current["state"]},
                )
            result = _public(row)
            await conn.execute(
                "INSERT INTO storage_cleanup_requests(request_id,subject_id,idempotency_key,"
                "input_digest,result) VALUES (%s,%s,%s,%s,%s)",
                (str(uuid4()), subject_id, idempotency_key, semantic, Jsonb(result)),
            )
            await conn.execute(
                "INSERT INTO storage_cleanup_events(cleanup_id,owner_epoch,step) "
                "VALUES (%s,%s,'RETRY_REQUESTED')", (cleanup_id, row["owner_epoch"]),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'storage.cleanup.retry',%s,%s,%s)",
                (str(uuid4()), subject_id, "cleanup:" + cleanup_id, cleanup_id,
                 Jsonb({"cleanup_id": cleanup_id})),
            )
            return result

    async def _admit_due(self) -> bool:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170795)")
            backup = await (await conn.execute(
                "SELECT j.backup_id AS resource_id,j.subject_id,j.state,a.relative_path,"
                "a.ciphertext_digest,a.byte_size,a.manifest_digest,a.expires_at,a.deleted_at "
                "FROM backup_jobs j JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.state IN ('READY','INCONSISTENT') AND a.deleted_at IS NULL "
                "AND a.expires_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM "
                "restore_stage_jobs r WHERE r.backup_id=j.backup_id AND r.state IN "
                "('QUEUED','VERIFYING','RESTORING','VALIDATING')) AND NOT EXISTS(SELECT 1 FROM "
                "backup_verification_jobs v WHERE v.backup_id=j.backup_id "
                "AND v.state IN ('QUEUED','VERIFYING')) "
                "ORDER BY a.expires_at,j.backup_id FOR UPDATE OF j,a SKIP LOCKED LIMIT 1"
            )).fetchone()
            if backup is not None:
                digest = self._resource_digest(BACKUP, backup)
                await self._tombstone(
                    conn, kind=BACKUP, resource=backup, cleanup_id=str(uuid4()),
                    subject_id=backup["subject_id"], reason="RETENTION", input_digest=digest,
                )
                return True
            restore = await (await conn.execute(
                "SELECT j.restore_id AS resource_id,j.subject_id,j.backup_id,j.verification_id,j.state,"
                "j.manifest_digest,j.database_total,j.database_completed,j.expires_at "
                "FROM restore_stage_jobs j "
                "WHERE j.state IN ('STAGED','FAILED') AND j.expires_at<=clock_timestamp() "
                "ORDER BY j.expires_at,j.restore_id "
                "FOR UPDATE OF j SKIP LOCKED LIMIT 1"
            )).fetchone()
            if restore is not None:
                targets = await (await conn.execute(
                    "SELECT ordinal,database_name,content_digest FROM restore_stage_targets "
                    "WHERE restore_id=%s ORDER BY ordinal", (restore["resource_id"],)
                )).fetchall()
                restore = {**restore, "targets": [list(target.values()) for target in targets]}
                digest = self._resource_digest(RESTORE, restore)
                await self._tombstone(
                    conn, kind=RESTORE, resource=restore, cleanup_id=str(uuid4()),
                    subject_id=restore["subject_id"], reason="RETENTION", input_digest=digest,
                )
                return True
            return False

    async def _claim(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "SELECT * FROM storage_cleanup_jobs WHERE "
                "(state IN ('QUEUED','RETRY') AND next_attempt_at<=clock_timestamp()) OR "
                "(state='CLEANING' AND lease_expires_at<=clock_timestamp()) "
                "ORDER BY created_at,cleanup_id FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is None:
                return None
            claimed = await (await conn.execute(
                "UPDATE storage_cleanup_jobs SET state='CLEANING',owner_id=%s,"
                "owner_epoch=owner_epoch+1,lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),attempt_count=attempt_count+1,error_code=NULL,"
                "updated_at=clock_timestamp() WHERE cleanup_id=%s RETURNING *",
                (self.owner_id, self.lease_seconds, row["cleanup_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO storage_cleanup_events(cleanup_id,owner_epoch,step) "
                "VALUES (%s,%s,'CLEANING')", (claimed["cleanup_id"], claimed["owner_epoch"]),
            )
            return claimed

    async def _heartbeat(self, job: dict[str, Any]) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "UPDATE storage_cleanup_jobs SET lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),updated_at=clock_timestamp() "
                "WHERE cleanup_id=%s AND state='CLEANING' AND owner_id=%s AND owner_epoch=%s "
                "AND lease_expires_at>clock_timestamp() RETURNING 1",
                (self.lease_seconds, job["cleanup_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if row is None:
                raise ForestError("CLEANUP_LEASE_LOST", "Cleanup ownership was lost", 409)

    async def _renew(self, job: dict[str, Any]) -> None:
        while True:
            await asyncio.sleep(max(10.0, self.lease_seconds / 3))
            await self._heartbeat(job)

    async def _owned(self, job: dict[str, Any]) -> None:
        work = asyncio.create_task(self._clean_resource(job))
        renew = asyncio.create_task(self._renew(job))
        done, pending = await asyncio.wait({work, renew}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()

    async def run_once(self) -> dict[str, Any] | None:
        await self._admit_due()
        job = await self._claim()
        if job is None:
            return await self._scavenge_once()
        try:
            await self._owned(job)
            return await self._finish(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, ForestError) else "CLEANUP_IO_FAILED"
            blocked = isinstance(exc, ForestError) and exc.code in {
                "CLEANUP_RESOURCE_NOT_TOMBSTONED", "BACKUP_CLEANUP_OWNERSHIP_CONFLICT",
                "RESTORE_CLEANUP_OWNERSHIP_CONFLICT", "RESTORE_DATABASE_REGISTERED",
            }
            return await self._fail(job, code, blocked or job["attempt_count"] >= 5)

    async def _clean_resource(self, job: dict[str, Any]) -> None:
        await self._heartbeat(job)
        if job["resource_kind"] == BACKUP:
            await self._clean_backup(job)
        elif job["resource_kind"] == RESTORE:
            await self._clean_restore(job)
        else:
            raise ForestError("CLEANUP_RESOURCE_INVALID", "Cleanup resource is invalid", 409)
        await self._heartbeat(job)

    async def _clean_backup(self, job: dict[str, Any]) -> None:
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT j.state,a.relative_path,a.ciphertext_digest,a.byte_size,a.deleted_at "
                "FROM backup_jobs j JOIN backup_artifacts a USING(backup_id) "
                "WHERE j.backup_id=%s", (job["resource_id"],)
            )).fetchone()
        if row is None or row["state"] != "EXPIRED" or row["deleted_at"] is None:
            raise ForestError(
                "CLEANUP_RESOURCE_NOT_TOMBSTONED", "Backup cleanup lacks a visibility barrier", 409
            )
        name = row["relative_path"]
        match = _BACKUP_NAME.fullmatch(name)
        if match is None or match.group("id") != job["resource_id"]:
            raise ForestError(
                "BACKUP_CLEANUP_OWNERSHIP_CONFLICT", "Backup path ownership is invalid", 409
            )
        path = self.backup_dir / name
        if not path.exists() and not path.is_symlink():
            return
        if path.is_symlink() or path.parent.resolve() != self.backup_dir.resolve():
            raise ForestError(
                "BACKUP_CLEANUP_OWNERSHIP_CONFLICT", "Backup path ownership is invalid", 409
            )
        stat = path.stat()
        digest = await asyncio.to_thread(self._file_digest, path)
        if stat.st_size != row["byte_size"] or digest != row["ciphertext_digest"]:
            raise ForestError(
                "BACKUP_CLEANUP_OWNERSHIP_CONFLICT", "Backup artifact changed before cleanup", 409
            )
        await asyncio.to_thread(path.unlink)
        await asyncio.to_thread(self.backups._sync_directory, self.backup_dir)

    @staticmethod
    def _file_digest(path: Path) -> str:
        value = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                value.update(chunk)
        return "sha256:" + value.hexdigest()

    async def _clean_restore(self, job: dict[str, Any]) -> None:
        if self.managed_dsn is None:
            raise ForestError(
                "RESTORE_CLEANUP_OWNERSHIP_CONFLICT", "Managed storage is unavailable", 409
            )
        async with self.store.pool.connection() as conn:
            restore = await (await conn.execute(
                "SELECT state FROM restore_stage_jobs WHERE restore_id=%s", (job["resource_id"],)
            )).fetchone()
            targets = await (await conn.execute(
                "SELECT ordinal,database_name,content_digest FROM restore_stage_targets "
                "WHERE restore_id=%s ORDER BY ordinal", (job["resource_id"],)
            )).fetchall()
        if restore is None or restore["state"] != "EXPIRED":
            raise ForestError(
                "CLEANUP_RESOURCE_NOT_TOMBSTONED", "Restore cleanup lacks a visibility barrier", 409
            )
        prefix = job["resource_id"].replace("-", "")[:20]
        for target in targets:
            expected_name = f"forest_restore_{prefix}_{target['ordinal']:04d}"
            if target["database_name"] != expected_name:
                raise ForestError(
                    "RESTORE_CLEANUP_OWNERSHIP_CONFLICT", "Restore database name is invalid", 409
                )
            async with self.store.pool.connection() as conn:
                registered = await (await conn.execute(
                    "SELECT 1 FROM bot_storage_plans WHERE database_name=%s LIMIT 1",
                    (expected_name,),
                )).fetchone()
            if registered is not None:
                raise ForestError(
                    "RESTORE_DATABASE_REGISTERED", "A registered database cannot be cleaned", 409
                )
            marker = (
                "forest:restore-stage:" + job["resource_id"] + ":" + target["content_digest"]
            )
            async with await psycopg.AsyncConnection.connect(
                self.managed_dsn, autocommit=True, row_factory=dict_row, connect_timeout=10
            ) as conn:
                database = await (await conn.execute(
                    "SELECT shobj_description(oid,'pg_database') AS marker,datistemplate "
                    "FROM pg_database WHERE datname=%s", (expected_name,)
                )).fetchone()
                if database is None:
                    continue
                if database["datistemplate"] or database["marker"] != marker:
                    raise ForestError(
                        "RESTORE_CLEANUP_OWNERSHIP_CONFLICT",
                        "Restore database marker is invalid", 409,
                    )
                await conn.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(expected_name))
                )
            await self._heartbeat(job)

    async def _finish(self, job: dict[str, Any]) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "UPDATE storage_cleanup_jobs SET state='COMPLETE',owner_id=NULL,"
                "lease_expires_at=NULL,error_code=NULL,completed_at=clock_timestamp(),"
                "updated_at=clock_timestamp() WHERE cleanup_id=%s AND state='CLEANING' "
                "AND owner_id=%s AND owner_epoch=%s AND lease_expires_at>clock_timestamp() "
                "RETURNING *",
                (job["cleanup_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if row is None:
                raise ForestError("CLEANUP_LEASE_LOST", "Cleanup ownership was lost", 409)
            await conn.execute(
                "INSERT INTO storage_cleanup_events(cleanup_id,owner_epoch,step) "
                "VALUES (%s,%s,'COMPLETE')", (job["cleanup_id"], job["owner_epoch"]),
            )
            action = (
                "backup.cleanup.completed" if job["resource_kind"] == BACKUP
                else "restore.cleanup.completed"
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (str(uuid4()), job["subject_id"], action,
                 ("backup:" if job["resource_kind"] == BACKUP else "restore:")
                 + job["resource_id"], job["cleanup_id"],
                 Jsonb({"cleanup_id": job["cleanup_id"], "reason": job["reason"]})),
            )
            return _public(row)

    async def _fail(self, job: dict[str, Any], code: str, blocked: bool) -> dict[str, Any]:
        state = "BLOCKED" if blocked else "RETRY"
        delay = min(3600, 2 ** min(job["attempt_count"], 10))
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "UPDATE storage_cleanup_jobs SET state=%s,owner_id=NULL,lease_expires_at=NULL,"
                "error_code=%s,next_attempt_at=clock_timestamp()+(%s*interval '1 second'),"
                "updated_at=clock_timestamp() WHERE cleanup_id=%s AND state='CLEANING' "
                "AND owner_id=%s AND owner_epoch=%s RETURNING *",
                (state, code, delay, job["cleanup_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if row is None:
                return job
            await conn.execute(
                "INSERT INTO storage_cleanup_events(cleanup_id,owner_epoch,step,details) "
                "VALUES (%s,%s,%s,%s)",
                (job["cleanup_id"], job["owner_epoch"], state,
                 Jsonb({"error_code": code})),
            )
            return _public(row)

    async def run_forever(self) -> None:
        self.closed = False
        while not self.closed:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(1)

    async def _scavenge_once(self) -> dict[str, Any] | None:
        """Remove one old, unreferenced file whose backup job proves its namespace."""
        entries = await asyncio.to_thread(lambda: sorted(self.backup_dir.iterdir(), key=lambda p: p.name))
        cutoff = datetime.now().timestamp() - self.orphan_grace_seconds
        for path in entries:
            match = _BACKUP_NAME.fullmatch(path.name)
            path_kind = "CANDIDATE"
            if match is None:
                match = _JOB_DIRECTORY.fullmatch(path.name)
                path_kind = "JOB_DIRECTORY"
            if match is None:
                continue
            try:
                byte_size, latest_mtime = await asyncio.to_thread(self._inspect_owned_path, path)
            except (FileNotFoundError, ForestError, OSError):
                continue
            if latest_mtime > cutoff:
                continue
            backup_id, epoch = match.group("id"), int(match.group("epoch"))
            async with self.store.pool.connection() as conn:
                row = await (await conn.execute(
                    "SELECT state,owner_epoch,lease_expires_at,subject_id,clock_timestamp() AS now "
                    "FROM backup_jobs WHERE backup_id=%s", (backup_id,)
                )).fetchone()
                referenced = await (await conn.execute(
                    "SELECT 1 FROM backup_artifacts WHERE relative_path=%s", (path.name,)
                )).fetchone()
            if row is None or referenced is not None or epoch > row["owner_epoch"]:
                continue
            if (
                row["state"] in {"CAPTURING", "SEALING"} and epoch == row["owner_epoch"]
                and row["lease_expires_at"] is not None and row["lease_expires_at"] > row["now"]
            ):
                continue
            try:
                await asyncio.to_thread(self._delete_owned_path, path, path_kind)
            except FileNotFoundError:
                continue
            except (ForestError, OSError):
                continue
            path_digest = digest_json({
                "backup_id": backup_id, "owner_epoch": epoch,
                "path_kind": path_kind, "relative_path": path.name,
            })
            async with self.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    "INSERT INTO backup_orphan_cleanup_receipts(receipt_id,backup_id,owner_epoch,"
                    "path_kind,relative_path_digest,byte_size) VALUES (%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(relative_path_digest) DO NOTHING",
                    (str(uuid4()), backup_id, epoch, path_kind, path_digest, byte_size),
                )
                await conn.execute(
                    "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,"
                    "details) VALUES (%s,%s,'backup.orphan.cleaned',%s,%s,%s)",
                    (str(uuid4()), row["subject_id"], "backup:" + backup_id, path_digest,
                     Jsonb({"owner_epoch": epoch, "path_kind": path_kind,
                            "byte_size": byte_size})),
                )
            return {
                "state": "ORPHAN_CLEANED", "backup_id": backup_id,
                "owner_epoch": epoch, "path_kind": path_kind, "byte_size": byte_size,
            }
        return None

    def _inspect_owned_path(self, path: Path) -> tuple[int, float]:
        root = self.backup_dir.resolve()
        if path.parent.resolve() != root:
            raise ForestError("BACKUP_ORPHAN_PATH_INVALID", "Orphan path left backup storage", 409)
        stack, total, latest = [path], 0, 0.0
        while stack:
            current = stack.pop()
            info = os.lstat(current)
            attributes = getattr(info, "st_file_attributes", 0)
            if stat.S_ISLNK(info.st_mode) or attributes & 0x400:
                raise ForestError("BACKUP_ORPHAN_PATH_INVALID", "Orphan path is a link", 409)
            latest = max(latest, info.st_mtime)
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
            elif stat.S_ISDIR(info.st_mode):
                with os.scandir(current) as children:
                    stack.extend(Path(child.path) for child in children)
            else:
                raise ForestError(
                    "BACKUP_ORPHAN_PATH_INVALID", "Orphan path has an unsupported entry", 409
                )
        return total, latest

    def _delete_owned_path(self, path: Path, path_kind: str) -> None:
        # Reinspect immediately before mutation; the directory is host-private, so no external
        # principal can legitimately replace entries between these operations.
        self._inspect_owned_path(path)
        mode = os.lstat(path).st_mode
        if path_kind == "CANDIDATE" and not stat.S_ISREG(mode):
            raise ForestError("BACKUP_ORPHAN_PATH_INVALID", "Candidate is not a file", 409)
        if path_kind == "JOB_DIRECTORY" and not stat.S_ISDIR(mode):
            raise ForestError("BACKUP_ORPHAN_PATH_INVALID", "Job workspace is not a directory", 409)
        if path_kind == "CANDIDATE":
            path.unlink()
        else:
            shutil.rmtree(path)
        self.backups._sync_directory(self.backup_dir)
