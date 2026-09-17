"""Plan, tombstone, and physically clean Forest-owned backup and restore resources."""

from __future__ import annotations

import asyncio
import hashlib
import json
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
GENERATION = "BOT_STORAGE_GENERATION"
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
        generation_retention_seconds: int = 604800,
    ):
        if not 60 <= lease_seconds <= 3600:
            raise ValueError("Cleanup lease must be between one minute and one hour")
        if not 60 <= orphan_grace_seconds <= 604800:
            raise ValueError("Orphan grace must be between one minute and seven days")
        if not 60 <= generation_retention_seconds <= 31536000:
            raise ValueError("Generation retention must be between one minute and one year")
        self.catalog = catalog
        self.backups = backups
        self.store = catalog.store
        self.managed_dsn = catalog.managed_dsn
        self.backup_dir = backups.backup_dir
        self.lease_seconds = lease_seconds
        self.orphan_grace_seconds = orphan_grace_seconds
        self.generation_retention_seconds = generation_retention_seconds
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
        elif kind == GENERATION:
            lock_clause = " FOR UPDATE OF h" if lock else ""
            row = await (await conn.execute(
                "SELECT g.bot_id||':'||g.storage_generation::text AS resource_id,g.bot_id,"
                "g.storage_generation,g.source_kind,g.source_restore_id,g.source_ordinal,"
                "g.plan_digest,g.cluster_id,g.database_name,g.role_name,g.created_at,"
                "b.mode,h.storage_generation AS current_storage_generation,h.revision AS head_revision,"
                "a.activation_id,a.subject_id,a.completed_at AS superseded_at,"
                "a.completed_at+(%s*interval '1 second') AS retire_after,"
                "r.cleanup_id,c.state AS cleanup_state "
                "FROM bot_storage_generations g JOIN bot_catalog b USING(bot_id) "
                "JOIN bot_storage_heads h USING(bot_id) "
                "LEFT JOIN restore_activation_jobs a ON a.bot_id=g.bot_id "
                "AND a.expected_storage_generation=g.storage_generation AND a.state='COMPLETE' "
                "LEFT JOIN bot_storage_generation_retirements r ON r.bot_id=g.bot_id "
                "AND r.storage_generation=g.storage_generation LEFT JOIN storage_cleanup_jobs c "
                "ON c.cleanup_id=r.cleanup_id WHERE g.bot_id||':'||g.storage_generation::text=%s"
                + lock_clause,
                (self.generation_retention_seconds, resource_id),
            )).fetchone()
            if row is None:
                raise ForestError(
                    "STORAGE_GENERATION_NOT_FOUND", "Bot storage generation does not exist", 404
                )
            if row["mode"] != "managed" or row["cluster_id"] is None:
                raise ForestError(
                    "STORAGE_GENERATION_EXTERNAL", "External Bot storage is not managed", 409
                )
            if row["cleanup_id"] is not None:
                raise ForestError(
                    "CLEANUP_STATE_CONFLICT", "Storage generation is already retired", 409,
                    {"current_state": row["cleanup_state"]},
                )
            if (
                row["storage_generation"] >= row["current_storage_generation"]
                or row["activation_id"] is None
            ):
                raise ForestError(
                    "STORAGE_GENERATION_CURRENT",
                    "Only a generation superseded by a completed activation can be cleaned",
                    409,
                )
            row = {**row, "state": "SUPERSEDED"}
            allowed = {"SUPERSEDED"}
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
        elif kind == RESTORE:
            value = {
                key: _time(row[key]) for key in (
                    "resource_id", "backup_id", "verification_id", "state", "manifest_digest", "database_total",
                    "database_completed", "expires_at", "targets",
                )
            }
        else:
            value = {
                key: _time(row[key]) for key in (
                    "resource_id", "bot_id", "storage_generation", "source_kind",
                    "source_restore_id", "source_ordinal", "plan_digest", "cluster_id",
                    "database_name", "role_name", "created_at", "current_storage_generation",
                    "head_revision", "activation_id", "superseded_at", "retire_after", "state",
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
                "after": {
                    "state": "RETIRED" if kind == GENERATION else "EXPIRED",
                    "visibility": "REVOKED", "cleanup": "QUEUED",
                },
            }

    async def list_generations(
        self, *, subject_id: str, session_token_hash: str,
    ) -> list[dict[str, Any]]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                "SELECT g.bot_id,b.display_name,g.storage_generation,g.source_kind,"
                "g.source_restore_id,g.source_ordinal,g.created_at,"
                "h.storage_generation=g.storage_generation AS current,"
                "a.completed_at AS superseded_at,"
                "a.completed_at+(%s*interval '1 second') AS retire_after,"
                "r.retired_at,c.cleanup_id,c.state AS cleanup_state,c.error_code,"
                "c.completed_at AS cleanup_completed_at,clock_timestamp()>=a.completed_at+"
                "(%s*interval '1 second') AS retention_due "
                "FROM bot_storage_generations g JOIN bot_catalog b USING(bot_id) "
                "JOIN bot_storage_heads h USING(bot_id) "
                "LEFT JOIN restore_activation_jobs a ON a.bot_id=g.bot_id "
                "AND a.expected_storage_generation=g.storage_generation AND a.state='COMPLETE' "
                "LEFT JOIN bot_storage_generation_retirements r ON r.bot_id=g.bot_id "
                "AND r.storage_generation=g.storage_generation LEFT JOIN storage_cleanup_jobs c "
                "ON c.cleanup_id=r.cleanup_id WHERE b.mode='managed' "
                "ORDER BY b.display_name,g.bot_id,g.storage_generation DESC",
                (self.generation_retention_seconds, self.generation_retention_seconds),
            )).fetchall()
        result = []
        for row in rows:
            if row["current"]:
                state = "CURRENT"
            elif row["cleanup_id"] is not None:
                state = "RETIRED"
            elif row["superseded_at"] is not None:
                state = "SUPERSEDED"
            else:
                state = "UNPROVEN"
            result.append({
                key: _time(value) for key, value in {
                    "bot_id": row["bot_id"], "display_name": row["display_name"],
                    "storage_generation": row["storage_generation"],
                    "source_kind": row["source_kind"],
                    "source_restore_id": row["source_restore_id"],
                    "source_ordinal": row["source_ordinal"], "state": state,
                    "current": row["current"], "created_at": row["created_at"],
                    "superseded_at": row["superseded_at"],
                    "retire_after": row["retire_after"],
                    "retention_due": bool(row["retention_due"]),
                    "retired_at": row["retired_at"], "cleanup_id": row["cleanup_id"],
                    "cleanup_state": row["cleanup_state"], "error_code": row["error_code"],
                    "cleanup_completed_at": row["cleanup_completed_at"],
                }.items()
            })
        return result

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
            elif plan["resource_kind"] == RESTORE:
                activation = await (await conn.execute(
                    "SELECT 1 FROM restore_activation_jobs WHERE restore_id=%s "
                    "AND state<>'COMPLETE' LIMIT 1", (plan["resource_id"],)
                )).fetchone()
                if activation is not None:
                    raise ForestError(
                        "RESTORE_ACTIVATION_ACTIVE",
                        "An unfinished activation still depends on this restore", 409,
                    )
            else:
                activation = await (await conn.execute(
                    "SELECT 1 FROM restore_activation_jobs WHERE bot_id=%s "
                    "AND state<>'COMPLETE' LIMIT 1", (resource["bot_id"],)
                )).fetchone()
                if activation is not None:
                    raise ForestError(
                        "STORAGE_GENERATION_ACTIVATION_ACTIVE",
                        "An unfinished activation still depends on this Bot storage history",
                        409,
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
        elif kind == RESTORE:
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
        else:
            await conn.execute(
                "INSERT INTO bot_storage_generation_retirements("
                "bot_id,storage_generation,cleanup_id) VALUES (%s,%s,%s)",
                (resource["bot_id"], resource["storage_generation"], cleanup_id),
            )
            action, key = "bot.storage-generation.retire.request", "bot:" + resource["bot_id"]
        await conn.execute(
            "INSERT INTO storage_cleanup_events(cleanup_id,owner_epoch,step,details) "
            "VALUES (%s,0,'QUEUED',%s)",
            (cleanup_id, Jsonb({"reason": reason})),
        )
        await conn.execute(
            "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid4()), subject_id, action, key, cleanup_id,
             Jsonb({
                 "cleanup_id": cleanup_id, "reason": reason,
                 **({"storage_generation": resource["storage_generation"]}
                    if kind == GENERATION else {}),
             })),
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

    async def list(
        self, *, subject_id: str, session_token_hash: str, limit: int = 100,
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ForestError("CLEANUP_QUERY_INVALID", "Cleanup limit is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (
                await conn.execute(
                    "SELECT * FROM storage_cleanup_jobs "
                    "ORDER BY created_at DESC,cleanup_id DESC LIMIT %s",
                    (limit,),
                )
            ).fetchall()
            await self._authorize(conn, subject_id, session_token_hash)
            return [_public(row) for row in rows]

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
                "AND NOT EXISTS(SELECT 1 FROM restore_activation_jobs a "
                "WHERE a.restore_id=j.restore_id AND a.state<>'COMPLETE') "
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
            generation = await (await conn.execute(
                "SELECT g.bot_id||':'||g.storage_generation::text AS resource_id,g.bot_id,"
                "g.storage_generation,g.source_kind,g.source_restore_id,g.source_ordinal,"
                "g.plan_digest,g.cluster_id,g.database_name,g.role_name,g.created_at,"
                "h.storage_generation AS current_storage_generation,h.revision AS head_revision,"
                "a.activation_id,a.subject_id,a.completed_at AS superseded_at,"
                "a.completed_at+(%s*interval '1 second') AS retire_after,'SUPERSEDED' AS state "
                "FROM bot_storage_generations g JOIN bot_catalog b USING(bot_id) "
                "JOIN bot_storage_heads h USING(bot_id) JOIN restore_activation_jobs a "
                "ON a.bot_id=g.bot_id AND a.expected_storage_generation=g.storage_generation "
                "AND a.state='COMPLETE' LEFT JOIN bot_storage_generation_retirements r "
                "ON r.bot_id=g.bot_id AND r.storage_generation=g.storage_generation "
                "WHERE b.mode='managed' "
                "AND g.storage_generation<h.storage_generation AND r.cleanup_id IS NULL "
                "AND a.completed_at+(%s*interval '1 second')<=clock_timestamp() "
                "AND NOT EXISTS(SELECT 1 FROM restore_activation_jobs active "
                "WHERE active.bot_id=g.bot_id AND active.state<>'COMPLETE') "
                "ORDER BY a.completed_at,g.bot_id,g.storage_generation "
                "FOR UPDATE OF h SKIP LOCKED LIMIT 1",
                (self.generation_retention_seconds, self.generation_retention_seconds),
            )).fetchone()
            if generation is not None:
                digest = self._resource_digest(GENERATION, generation)
                await self._tombstone(
                    conn, kind=GENERATION, resource=generation, cleanup_id=str(uuid4()),
                    subject_id=generation["subject_id"], reason="RETENTION",
                    input_digest=digest,
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
                "STORAGE_GENERATION_CURRENT", "STORAGE_GENERATION_OWNERSHIP_CONFLICT",
                "STORAGE_GENERATION_DATABASE_SHARED", "STORAGE_GENERATION_ACTIVATION_ACTIVE",
            }
            return await self._fail(job, code, blocked or job["attempt_count"] >= 5)

    async def _clean_resource(self, job: dict[str, Any]) -> None:
        await self._heartbeat(job)
        if job["resource_kind"] == BACKUP:
            await self._clean_backup(job)
        elif job["resource_kind"] == RESTORE:
            await self._clean_restore(job)
        elif job["resource_kind"] == GENERATION:
            await self._clean_generation(job)
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
                    "SELECT g.source_restore_id,g.source_ordinal,h.storage_generation IS NOT NULL "
                    "AS current,c.state AS retirement_state "
                    "FROM bot_storage_generations g LEFT JOIN bot_storage_heads h "
                    "ON h.bot_id=g.bot_id AND h.storage_generation=g.storage_generation "
                    "LEFT JOIN bot_storage_generation_retirements r ON r.bot_id=g.bot_id "
                    "AND r.storage_generation=g.storage_generation "
                    "LEFT JOIN storage_cleanup_jobs c "
                    "ON c.cleanup_id=r.cleanup_id "
                    "WHERE g.database_name=%s LIMIT 1",
                    (expected_name,),
                )).fetchone()
            if registered is not None:
                if (
                    registered["source_restore_id"] == job["resource_id"]
                    and registered["source_ordinal"] == target["ordinal"]
                    and registered["current"]
                ):
                    continue
                if registered["retirement_state"] != "COMPLETE":
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

    async def _clean_generation(self, job: dict[str, Any]) -> None:
        if self.managed_dsn is None:
            raise ForestError(
                "STORAGE_GENERATION_OWNERSHIP_CONFLICT", "Managed storage is unavailable", 409
            )
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT g.bot_id,g.storage_generation,g.source_kind,g.source_restore_id,"
                "g.source_ordinal,g.plan_digest,g.cluster_id,g.database_name,g.role_name,"
                "h.storage_generation AS current_storage_generation,r.cleanup_id,"
                "p.deployment_id,t.content_digest FROM bot_storage_generation_retirements r "
                "JOIN bot_storage_generations g USING(bot_id,storage_generation) "
                "JOIN bot_storage_heads h USING(bot_id) LEFT JOIN bot_storage_plans p "
                "ON p.bot_id=g.bot_id LEFT JOIN restore_stage_targets t "
                "ON t.restore_id=g.source_restore_id AND t.ordinal=g.source_ordinal "
                "WHERE r.cleanup_id=%s", (job["cleanup_id"],)
            )).fetchone()
            active = None
            shared = None
            if row is not None:
                active = await (await conn.execute(
                    "SELECT 1 FROM restore_activation_jobs WHERE bot_id=%s "
                    "AND state<>'COMPLETE' LIMIT 1", (row["bot_id"],)
                )).fetchone()
                shared = await (await conn.execute(
                    "SELECT 1 FROM bot_storage_generations WHERE cluster_id=%s "
                    "AND database_name=%s AND (bot_id,storage_generation)<>(%s,%s) LIMIT 1",
                    (
                        row["cluster_id"], row["database_name"], row["bot_id"],
                        row["storage_generation"],
                    ),
                )).fetchone()
        if row is None or (
            job["resource_id"] != f"{row['bot_id']}:{row['storage_generation']}"
        ):
            raise ForestError(
                "CLEANUP_RESOURCE_NOT_TOMBSTONED",
                "Storage generation cleanup lacks its retirement fact", 409,
            )
        if row["storage_generation"] >= row["current_storage_generation"]:
            raise ForestError(
                "STORAGE_GENERATION_CURRENT", "The current storage generation cannot be cleaned", 409
            )
        if active is not None:
            raise ForestError(
                "STORAGE_GENERATION_ACTIVATION_ACTIVE",
                "An unfinished activation still depends on this Bot storage history", 409,
            )
        if shared is not None:
            raise ForestError(
                "STORAGE_GENERATION_DATABASE_SHARED",
                "More than one storage generation names this database", 409,
            )
        if row["source_kind"] == "PROVISIONED":
            if row["deployment_id"] is None:
                raise ForestError(
                    "STORAGE_GENERATION_OWNERSHIP_CONFLICT",
                    "Provisioned storage ownership proof is missing", 409,
                )
            marker = "forest:bot-storage:" + json.dumps({
                "deployment": row["deployment_id"], "bot": row["bot_id"],
                "plan": row["plan_digest"],
            }, sort_keys=True, separators=(",", ":"))
        elif row["source_kind"] == "RESTORE" and row["content_digest"] is not None:
            marker = (
                "forest:restore-stage:" + row["source_restore_id"] + ":" + row["content_digest"]
            )
        else:
            raise ForestError(
                "STORAGE_GENERATION_OWNERSHIP_CONFLICT",
                "Restore storage ownership proof is missing", 409,
            )
        async with await psycopg.AsyncConnection.connect(
            self.managed_dsn, autocommit=True, row_factory=dict_row, connect_timeout=10
        ) as conn:
            cluster = await (await conn.execute(
                "SELECT system_identifier::text AS id FROM pg_control_system()"
            )).fetchone()
            if cluster is None or cluster["id"] != row["cluster_id"]:
                raise ForestError(
                    "STORAGE_GENERATION_OWNERSHIP_CONFLICT",
                    "Managed cluster differs from the generation record", 409,
                )
            database = await (await conn.execute(
                "SELECT d.datistemplate,r.rolname,shobj_description(d.oid,'pg_database') AS marker "
                "FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba WHERE d.datname=%s",
                (row["database_name"],),
            )).fetchone()
            if database is None:
                return
            if (
                database["datistemplate"] or database["rolname"] != row["role_name"]
                or database["marker"] != marker
            ):
                raise ForestError(
                    "STORAGE_GENERATION_OWNERSHIP_CONFLICT",
                    "Storage generation database ownership is invalid", 409,
                )
            await conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(row["database_name"])
                )
            )

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
            if job["resource_kind"] == BACKUP:
                action, key = "backup.cleanup.completed", "backup:" + job["resource_id"]
            elif job["resource_kind"] == RESTORE:
                action, key = "restore.cleanup.completed", "restore:" + job["resource_id"]
            else:
                bot_id, generation = job["resource_id"].rsplit(":", 1)
                action, key = "bot.storage-generation.retire.completed", "bot:" + bot_id
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (str(uuid4()), job["subject_id"], action, key, job["cleanup_id"],
                 Jsonb({
                     "cleanup_id": job["cleanup_id"], "reason": job["reason"],
                     **({"storage_generation": int(generation)}
                        if job["resource_kind"] == GENERATION else {}),
                 })),
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
