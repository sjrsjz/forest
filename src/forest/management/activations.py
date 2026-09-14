"""Crash-resumable activation of one isolated Bot restore generation."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.migration_v11 import WRITER_GENERATION

_ACTIVE = ("FENCING", "SWITCHING", "ACTIVATING")
_PUBLIC_KEYS = (
    "activation_id", "plan_id", "restore_id", "target_ordinal", "bot_id", "state",
    "expected_storage_generation", "target_storage_generation", "current_database_name",
    "target_database_name", "owner_epoch", "attempt_count", "error_code", "fenced_at",
    "switched_at", "activated_at", "completed_at", "created_at", "updated_at",
)

# These facts can authorize or record an externally visible effect. Message and
# memory content is intentionally absent: rollback may discard newer conversation
# content, while the deletion epoch separately prevents forgotten data revival.
_CRITICAL_TABLES = (
    "bot_bindings",
    "bot_binding_revisions",
    "bot_binding_credentials",
    "bot_binding_clients",
    "bot_calls",
    "bot_state_steps",
    "bot_state_revisions",
    "bot_outbox_events",
    "bot_delivery_roots",
    "bot_delivery_streams",
    "bot_send_intents",
    "bot_send_parts",
    "bot_send_attempts",
    "bot_delivery_resolutions",
    "bot_message_echo_aliases",
    "bot_agent_runs",
    "bot_agent_steps",
    "bot_model_request_manifests",
    "bot_model_transport_attempts",
    "bot_model_budget_reservations",
    "bot_agent_tools",
    "bot_agent_tool_approvals",
    "bot_agent_tool_observations",
    "bot_shell_requests",
    "bot_jobs",
    "bot_job_revisions",
    "bot_job_occurrences",
    "bot_job_expansions",
    "bot_component_events",
    "bot_event_deliveries",
    "bot_component_jobs",
)


def _time(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _public(row: dict[str, Any], events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    result = {key: _time(row.get(key)) for key in _PUBLIC_KEYS}
    if events is not None:
        result["events"] = [
            {key: _time(value) for key, value in event.items()} for event in events
        ]
    return result


class RestoreActivationManager:
    """Move one Bot directory head only after proving effect-ledger convergence."""

    def __init__(self, catalog, *, lease_seconds: int = 300):
        if not 60 <= lease_seconds <= 3600:
            raise ValueError("Activation lease must be between one minute and one hour")
        self.catalog = catalog
        self.store = catalog.store
        self.vault = catalog.vault
        self.managed_dsn = catalog.managed_dsn
        self.lease_seconds = lease_seconds
        self.owner_id = "restore-activation:" + str(uuid4())
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
            current = await (await conn.execute(
                "SELECT password_hash FROM web_accounts WHERE id=%s FOR UPDATE", (subject_id,)
            )).fetchone()
            if (
                current is None or current["password_hash"] != expected_password_hash
                or account["disabled"]
            ):
                raise ForestError(
                    "REAUTHENTICATION_STALE",
                    "Account credentials changed; authenticate again",
                    409,
                )

    @staticmethod
    async def _candidate(conn, restore_id: str, ordinal: int, *, lock: bool) -> dict:
        suffix = " FOR UPDATE OF r,t,h" if lock else ""
        row = await (await conn.execute(
            "SELECT r.restore_id,r.state AS restore_state,r.expires_at,"
            "r.expires_at>clock_timestamp() AS unexpired,t.ordinal,t.database_scope,"
            "t.bot_id,t.database_name AS target_database_name,t.content_digest,"
            "t.deletion_epoch,t.state AS target_state,t.quarantine_digest,"
            "b.mode,b.stage AS bot_stage,h.storage_generation,g.database_name AS "
            "current_database_name,g.role_name,g.cluster_id,g.secret_ciphertext,"
            "g.plan_digest AS current_plan_digest,c.cleanup_id,c.state AS cleanup_state "
            "FROM restore_stage_jobs r JOIN restore_stage_targets t USING(restore_id) "
            "JOIN bot_catalog b ON b.bot_id=t.bot_id "
            "JOIN bot_storage_heads h ON h.bot_id=b.bot_id "
            "JOIN bot_storage_generations g ON g.bot_id=b.bot_id "
            "AND g.storage_generation=h.storage_generation "
            "LEFT JOIN storage_cleanup_jobs c ON c.resource_kind='RESTORE_STAGE' "
            "AND c.resource_id=r.restore_id WHERE r.restore_id=%s AND t.ordinal=%s" + suffix,
            (restore_id, ordinal),
        )).fetchone()
        if row is None:
            raise ForestError("RESTORE_TARGET_NOT_FOUND", "Restore Bot target does not exist", 404)
        if row["database_scope"] != "bot":
            raise ForestError("ACTIVATION_SCOPE_INVALID", "Only a Bot restore can be activated", 422)
        if row["mode"] != "managed" or row["cluster_id"] is None:
            raise ForestError(
                "ACTIVATION_STORAGE_UNSUPPORTED",
                "Activation currently requires managed Bot storage",
                409,
            )
        if (
            row["restore_state"] != "STAGED" or row["target_state"] != "STAGED"
            or not row["unexpired"]
            or row["cleanup_id"] is not None or row["bot_stage"] != "READY"
            or row["quarantine_digest"] is None
        ):
            raise ForestError(
                "ACTIVATION_TARGET_UNAVAILABLE",
                "Restore target is not an unexpired staged Bot",
                409,
            )
        return row

    async def _database_identity(self, candidate: dict) -> None:
        if self.managed_dsn is None:
            raise ForestError("MANAGED_STORAGE_UNAVAILABLE", "Managed storage is unavailable", 503)
        marker = (
            "forest:restore-stage:" + candidate["restore_id"] + ":" + candidate["content_digest"]
        )
        async with await psycopg.AsyncConnection.connect(
            self.managed_dsn, row_factory=dict_row, connect_timeout=10
        ) as conn:
            cluster = await (await conn.execute(
                "SELECT system_identifier::text AS id FROM pg_control_system()"
            )).fetchone()
            database = await (await conn.execute(
                "SELECT d.datistemplate,shobj_description(d.oid,'pg_database') AS marker,"
                "r.rolname FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba "
                "WHERE d.datname=%s", (candidate["target_database_name"],)
            )).fetchone()
        if (
            cluster["id"] != candidate["cluster_id"] or database is None
            or database["datistemplate"] or database["marker"] != marker
            or database["rolname"] != candidate["role_name"]
        ):
            raise ForestError(
                "ACTIVATION_TARGET_OWNERSHIP_CONFLICT",
                "Restore target ownership proof is invalid",
                409,
            )

    @staticmethod
    async def _critical_digest(conn, bot_id: str) -> str:
        total_rows = 0
        total_bytes = 0
        value = hashlib.sha256(b"forest.restore-activation.safety.v1\0")
        for table in _CRITICAL_TABLES:
            count = await (await conn.execute(
                sql.SQL("SELECT count(*) AS n FROM public.{} WHERE bot_id=%s").format(
                    sql.Identifier(table)
                ),
                (bot_id,),
            )).fetchone()
            total_rows += count["n"]
            if total_rows > 1_000_000:
                raise ForestError(
                    "ACTIVATION_PROOF_TOO_LARGE", "Critical effect proof exceeds its row bound", 409
                )
            value.update(table.encode())
            value.update(b"\0")
            cursor = conn.cursor(name="activation_" + uuid4().hex)
            await cursor.execute(
                sql.SQL(
                    "SELECT to_jsonb(t)::text AS value FROM public.{} AS t "
                    "WHERE bot_id=%s ORDER BY to_jsonb(t)::text"
                ).format(sql.Identifier(table)),
                (bot_id,),
            )
            async for row in cursor:
                encoded = row["value"].encode()
                total_bytes += len(encoded)
                if total_bytes > 256 * 1024 * 1024:
                    await cursor.close()
                    raise ForestError(
                        "ACTIVATION_PROOF_TOO_LARGE",
                        "Critical effect proof exceeds its byte bound",
                        409,
                    )
                value.update(len(encoded).to_bytes(8, "big"))
                value.update(encoded)
            await cursor.close()
        return "sha256:" + value.hexdigest()

    @staticmethod
    async def _unresolved_effect(conn, bot_id: str) -> bool:
        row = await (await conn.execute(
            "SELECT EXISTS(SELECT 1 FROM public.bot_send_parts WHERE bot_id=%s "
            "AND state IN ('CLAIMED','DISPATCHING','UNKNOWN')) "
            "OR EXISTS(SELECT 1 FROM public.bot_send_attempts WHERE bot_id=%s "
            "AND state IN ('DISPATCHING','UNKNOWN')) "
            "OR EXISTS(SELECT 1 FROM public.bot_model_transport_attempts WHERE bot_id=%s "
            "AND status IN ('PERMITTED','DISPATCHING','UNKNOWN')) "
            "OR EXISTS(SELECT 1 FROM public.bot_agent_tools WHERE bot_id=%s "
            "AND status IN ('DISPATCHING','UNKNOWN')) "
            "OR EXISTS(SELECT 1 FROM public.bot_calls WHERE bot_id=%s AND status='ADMITTED') "
            "OR EXISTS(SELECT 1 FROM public.bot_job_occurrences WHERE bot_id=%s "
            "AND status='CLAIMED') "
            "OR EXISTS(SELECT 1 FROM public.bot_event_deliveries WHERE bot_id=%s "
            "AND status='INFLIGHT') AS present",
            (bot_id,) * 7,
        )).fetchone()
        return row["present"]

    async def _snapshot(self, dsn: str, bot_id: str) -> dict[str, Any]:
        async with await psycopg.AsyncConnection.connect(
            dsn, row_factory=dict_row, connect_timeout=10
        ) as conn, conn.transaction():
            identity = await (await conn.execute(
                "SELECT bot_id FROM public.forest_bot_identity WHERE singleton"
            )).fetchone()
            runtime = await (await conn.execute(
                "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s", (bot_id,)
            )).fetchone()
            clock = await (await conn.execute(
                "SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s", (bot_id,)
            )).fetchone()
            if identity is None or identity["bot_id"] != bot_id or runtime is None:
                raise ForestError("ACTIVATION_BOT_IDENTITY_MISMATCH", "Bot identity differs", 409)
            return {
                "runtime": runtime,
                "deletion_epoch": 0 if clock is None else clock["deletion_epoch"],
                "safety_digest": await self._critical_digest(conn, bot_id),
                "unresolved": await self._unresolved_effect(conn, bot_id),
            }

    async def _preflight(self, candidate: dict) -> tuple[dict, dict, str, str]:
        await self._database_identity(candidate)
        source_secret = json.loads(self.vault.decrypt(candidate["secret_ciphertext"]))
        source_dsn = source_secret["dsn"]
        target_dsn = make_conninfo(source_dsn, dbname=candidate["target_database_name"])
        current = await self._snapshot(source_dsn, candidate["bot_id"])
        target = await self._snapshot(target_dsn, candidate["bot_id"])
        if (
            current["runtime"]["status"] != "ACTIVE"
            or current["runtime"]["storage_generation"] != candidate["storage_generation"]
        ):
            raise ForestError(
                "ACTIVATION_CURRENT_STATE_CONFLICT", "Current Bot storage is not active", 409
            )
        if (
            target["runtime"]["status"] != "SEALED"
            or digest_json(target["runtime"]) != candidate["quarantine_digest"]
        ):
            raise ForestError(
                "ACTIVATION_TARGET_QUARANTINE_INVALID", "Restore target is not quarantined", 409
            )
        if (
            current["deletion_epoch"] != candidate["deletion_epoch"]
            or target["deletion_epoch"] != candidate["deletion_epoch"]
        ):
            raise ForestError(
                "ACTIVATION_DELETION_EPOCH_CHANGED",
                "Memory deletion history changed after this backup",
                409,
            )
        if (
            current["safety_digest"] != target["safety_digest"]
            or current["unresolved"] or target["unresolved"]
        ):
            raise ForestError(
                "ACTIVATION_EFFECT_LEDGER_DIVERGED",
                "Critical effect ledgers differ or contain an unresolved effect",
                409,
            )
        return current, target, source_dsn, target_dsn

    async def plan(
        self, *, restore_id: str, target_ordinal: int, subject_id: str,
        session_token_hash: str,
    ) -> dict[str, Any]:
        if type(target_ordinal) is not int or not 0 <= target_ordinal <= 1023:
            raise ForestError("VALIDATION_FAILED", "Target ordinal is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            candidate = await self._candidate(conn, restore_id, target_ordinal, lock=False)
        _, target, _, _ = await self._preflight(candidate)
        plan_id = str(uuid4())
        expires_at = datetime.now().astimezone() + timedelta(minutes=5)
        fields = {
            "plan_id": plan_id,
            "subject_id": subject_id,
            "restore_id": restore_id,
            "target_ordinal": target_ordinal,
            "bot_id": candidate["bot_id"],
            "expected_storage_generation": candidate["storage_generation"],
            "current_database_name": candidate["current_database_name"],
            "target_database_name": candidate["target_database_name"],
            "deletion_epoch": candidate["deletion_epoch"],
            "safety_digest": target["safety_digest"],
            "quarantine_digest": candidate["quarantine_digest"],
        }
        plan_digest = digest_json(fields)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            current = await self._candidate(conn, restore_id, target_ordinal, lock=True)
            if any(current[key] != candidate[key] for key in (
                "storage_generation", "current_database_name", "target_database_name",
                "deletion_epoch", "quarantine_digest",
            )):
                raise ForestError("PLAN_CHANGED", "Restore activation target changed", 409)
            await conn.execute(
                "INSERT INTO restore_activation_plans(plan_id,subject_id,restore_id,"
                "target_ordinal,bot_id,expected_storage_generation,current_database_name,"
                "target_database_name,deletion_epoch,safety_digest,quarantine_digest,"
                "plan_digest,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan_id, subject_id, restore_id, target_ordinal, candidate["bot_id"],
                    candidate["storage_generation"], candidate["current_database_name"],
                    candidate["target_database_name"], candidate["deletion_epoch"],
                    target["safety_digest"], candidate["quarantine_digest"], plan_digest,
                    expires_at,
                ),
            )
        return {
            "plan_id": plan_id,
            "plan_digest": plan_digest,
            "restore_id": restore_id,
            "target_ordinal": target_ordinal,
            "bot_id": candidate["bot_id"],
            "expires_at": expires_at.isoformat(),
            "before": {
                "storage_generation": candidate["storage_generation"],
                "database_name": candidate["current_database_name"],
                "status": "ACTIVE",
            },
            "after": {
                "storage_generation": candidate["storage_generation"] + 1,
                "database_name": candidate["target_database_name"],
                "status": "ACTIVE",
            },
            "safety": {
                "deletion_epoch": candidate["deletion_epoch"],
                "effect_ledger_converged": True,
                "target_quarantined": True,
            },
        }

    async def apply(
        self, *, plan_id: str, plan_digest: str, subject_id: str,
        session_token_hash: str, expected_password_hash: str, idempotency_key: str,
    ) -> dict[str, Any]:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable activation key is required", 422)
        semantic = digest_json({"plan_id": plan_id, "plan_digest": plan_digest})
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(679330170796)")
            await self._authorize(
                conn, subject_id, session_token_hash, expected_password_hash
            )
            replay = await (await conn.execute(
                "SELECT input_digest,result FROM restore_activation_requests "
                "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if replay is not None:
                if replay["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Activation key was used for different input", 409
                    )
                return replay["result"]
            plan = await (await conn.execute(
                "SELECT *,expires_at>clock_timestamp() AS unexpired "
                "FROM restore_activation_plans WHERE plan_id=%s AND subject_id=%s FOR UPDATE",
                (plan_id, subject_id),
            )).fetchone()
            if plan is None or plan["plan_digest"] != plan_digest:
                raise ForestError("PLAN_NOT_FOUND", "Activation plan is unavailable", 404)
            if not plan["unexpired"]:
                raise ForestError("PLAN_EXPIRED", "Preview activation again", 409)
            candidate = await self._candidate(
                conn, plan["restore_id"], plan["target_ordinal"], lock=True
            )
            comparisons = {
                "bot_id": "bot_id",
                "storage_generation": "expected_storage_generation",
                "current_database_name": "current_database_name",
                "target_database_name": "target_database_name",
                "deletion_epoch": "deletion_epoch",
                "quarantine_digest": "quarantine_digest",
            }
            if any(candidate[key] != plan[plan_key] for key, plan_key in comparisons.items()):
                raise ForestError("PLAN_CHANGED", "Restore activation target changed", 409)
            _, target, source_dsn, target_dsn = await self._preflight(candidate)
            if target["safety_digest"] != plan["safety_digest"]:
                raise ForestError("PLAN_CHANGED", "Restore activation proof changed", 409)
            activation_id = str(uuid4())
            target_generation = plan["expected_storage_generation"] + 1
            target_plan_digest = digest_json({
                "bot_id": plan["bot_id"],
                "storage_generation": target_generation,
                "source_restore_id": plan["restore_id"],
                "source_ordinal": plan["target_ordinal"],
                "cluster_id": candidate["cluster_id"],
                "database_name": plan["target_database_name"],
                "role_name": candidate["role_name"],
                "safety_digest": plan["safety_digest"],
            })
            source_ciphertext = self.vault.encrypt(json.dumps({"dsn": source_dsn}))
            target_ciphertext = self.vault.encrypt(json.dumps({"dsn": target_dsn}))
            await conn.execute(
                "INSERT INTO bot_storage_generations(bot_id,storage_generation,source_kind,"
                "source_restore_id,source_ordinal,plan_digest,cluster_id,database_name,"
                "role_name,secret_ciphertext) VALUES (%s,%s,'RESTORE',%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan["bot_id"], target_generation, plan["restore_id"],
                    plan["target_ordinal"], target_plan_digest, candidate["cluster_id"],
                    plan["target_database_name"], candidate["role_name"], target_ciphertext,
                ),
            )
            row = await (await conn.execute(
                "INSERT INTO restore_activation_jobs(activation_id,plan_id,subject_id,"
                "restore_id,target_ordinal,bot_id,expected_storage_generation,"
                "target_storage_generation,current_database_name,target_database_name,"
                "role_name,cluster_id,source_secret_ciphertext,target_secret_ciphertext,"
                "target_plan_digest,deletion_epoch,safety_digest,quarantine_digest,input_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    activation_id, plan_id, subject_id, plan["restore_id"],
                    plan["target_ordinal"], plan["bot_id"],
                    plan["expected_storage_generation"], target_generation,
                    plan["current_database_name"], plan["target_database_name"],
                    candidate["role_name"], candidate["cluster_id"], source_ciphertext,
                    target_ciphertext, target_plan_digest, plan["deletion_epoch"],
                    plan["safety_digest"], plan["quarantine_digest"], semantic,
                ),
            )).fetchone()
            result = _public(row)
            await conn.execute(
                "INSERT INTO restore_activation_events(activation_id,owner_epoch,step) "
                "VALUES (%s,0,'QUEUED')", (activation_id,)
            )
            await conn.execute(
                "INSERT INTO restore_activation_requests(request_id,subject_id,idempotency_key,"
                "input_digest,result) VALUES (%s,%s,%s,%s,%s)",
                (str(uuid4()), subject_id, idempotency_key, semantic, Jsonb(result)),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'restore.activation.request',%s,%s,%s)",
                (
                    str(uuid4()), subject_id, "bot:" + plan["bot_id"], activation_id,
                    Jsonb({
                        "activation_id": activation_id,
                        "restore_id": plan["restore_id"],
                        "target_ordinal": plan["target_ordinal"],
                        "from_generation": plan["expected_storage_generation"],
                        "to_generation": target_generation,
                    }),
                ),
            )
            return result

    async def list(self, *, subject_id: str, session_token_hash: str) -> list[dict[str, Any]]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            rows = await (await conn.execute(
                "SELECT * FROM restore_activation_jobs ORDER BY created_at DESC,activation_id"
            )).fetchall()
            return [_public(row) for row in rows]

    async def read(
        self, *, activation_id: str, subject_id: str, session_token_hash: str,
    ) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                "SELECT * FROM restore_activation_jobs WHERE activation_id=%s",
                (activation_id,),
            )).fetchone()
            if row is None:
                raise ForestError("ACTIVATION_NOT_FOUND", "Restore activation does not exist", 404)
            events = await (await conn.execute(
                "SELECT event_id,owner_epoch,step,details,created_at "
                "FROM restore_activation_events WHERE activation_id=%s ORDER BY event_id",
                (activation_id,),
            )).fetchall()
            return _public(row, events)

    async def retry(
        self, *, activation_id: str, subject_id: str, session_token_hash: str,
        expected_password_hash: str, idempotency_key: str,
    ) -> dict[str, Any]:
        if not 1 <= len(idempotency_key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable activation key is required", 422)
        semantic = digest_json({"action": "restore.activation.retry", "activation_id": activation_id})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(
                conn, subject_id, session_token_hash, expected_password_hash
            )
            replay = await (await conn.execute(
                "SELECT input_digest,result FROM restore_activation_requests "
                "WHERE subject_id=%s AND idempotency_key=%s",
                (subject_id, idempotency_key),
            )).fetchone()
            if replay is not None:
                if replay["input_digest"] != semantic:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Activation key was used for different input", 409
                    )
                return replay["result"]
            row = await (await conn.execute(
                "UPDATE restore_activation_jobs SET state='QUEUED',error_code=NULL,"
                "updated_at=clock_timestamp() WHERE activation_id=%s AND state='BLOCKED' "
                "RETURNING *", (activation_id,)
            )).fetchone()
            if row is None:
                current = await (await conn.execute(
                    "SELECT state FROM restore_activation_jobs WHERE activation_id=%s",
                    (activation_id,),
                )).fetchone()
                if current is None:
                    raise ForestError("ACTIVATION_NOT_FOUND", "Restore activation does not exist", 404)
                raise ForestError(
                    "ACTIVATION_RETRY_CONFLICT", "Only a blocked activation can be retried", 409,
                    {"current_state": current["state"]},
                )
            result = _public(row)
            await conn.execute(
                "INSERT INTO restore_activation_requests(request_id,subject_id,idempotency_key,"
                "input_digest,result) VALUES (%s,%s,%s,%s,%s)",
                (str(uuid4()), subject_id, idempotency_key, semantic, Jsonb(result)),
            )
            await conn.execute(
                "INSERT INTO restore_activation_events(activation_id,owner_epoch,step) "
                "VALUES (%s,%s,'RETRY_REQUESTED')", (activation_id, row["owner_epoch"])
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,"
                "request_id,details) VALUES (%s,%s,'restore.activation.retry',%s,%s,%s)",
                (
                    str(uuid4()), subject_id, "bot:" + row["bot_id"], activation_id,
                    Jsonb({"activation_id": activation_id}),
                ),
            )
            return result

    async def _claim(self) -> dict[str, Any] | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "SELECT * FROM restore_activation_jobs WHERE state='QUEUED' OR "
                "(state IN ('FENCING','SWITCHING','ACTIVATING') "
                "AND lease_expires_at<=clock_timestamp()) ORDER BY created_at,activation_id "
                "FOR UPDATE SKIP LOCKED LIMIT 1"
            )).fetchone()
            if row is None:
                return None
            claimed = await (await conn.execute(
                "UPDATE restore_activation_jobs SET state=CASE WHEN state='QUEUED' "
                "THEN 'FENCING' ELSE state END,owner_id=%s,owner_epoch=owner_epoch+1,"
                "lease_expires_at=clock_timestamp()+(%s*interval '1 second'),"
                "attempt_count=attempt_count+1,error_code=NULL,updated_at=clock_timestamp() "
                "WHERE activation_id=%s RETURNING *",
                (self.owner_id, self.lease_seconds, row["activation_id"]),
            )).fetchone()
            await conn.execute(
                "INSERT INTO restore_activation_events(activation_id,owner_epoch,step) "
                "VALUES (%s,%s,%s)",
                (claimed["activation_id"], claimed["owner_epoch"], claimed["state"]),
            )
            return claimed

    async def _guard(self, conn, job: dict) -> dict:
        row = await (await conn.execute(
            "SELECT * FROM restore_activation_jobs WHERE activation_id=%s FOR UPDATE",
            (job["activation_id"],),
        )).fetchone()
        now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
        if (
            row is None or row["state"] not in _ACTIVE or row["owner_id"] != self.owner_id
            or row["owner_epoch"] != job["owner_epoch"] or row["lease_expires_at"] <= now["now"]
        ):
            raise ForestError("ACTIVATION_LEASE_LOST", "Activation ownership was lost", 409)
        return row

    async def _heartbeat(self, job: dict) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, job)
            await conn.execute(
                "UPDATE restore_activation_jobs SET lease_expires_at=clock_timestamp()+"
                "(%s*interval '1 second'),updated_at=clock_timestamp() "
                "WHERE activation_id=%s",
                (self.lease_seconds, job["activation_id"]),
            )

    async def _renew(self, job: dict) -> None:
        while True:
            await asyncio.sleep(max(10.0, self.lease_seconds / 3))
            await self._heartbeat(job)

    async def _transition(self, job: dict, old: str, new: str, timestamp: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, job)
            row = await (await conn.execute(
                sql.SQL(
                    "UPDATE restore_activation_jobs SET state=%s,{}=clock_timestamp(),"
                    "updated_at=clock_timestamp() WHERE activation_id=%s AND state=%s RETURNING *"
                ).format(sql.Identifier(timestamp)),
                (new, job["activation_id"], old),
            )).fetchone()
            if row is None:
                raise ForestError("ACTIVATION_STATE_CHANGED", "Activation state changed", 409)
            await conn.execute(
                "INSERT INTO restore_activation_events(activation_id,owner_epoch,step) "
                "VALUES (%s,%s,%s)", (job["activation_id"], job["owner_epoch"], new)
            )
            return row

    async def _fence_current(self, job: dict) -> None:
        source = json.loads(self.vault.decrypt(job["source_secret_ciphertext"]))["dsn"]
        async with await psycopg.AsyncConnection.connect(
            source, row_factory=dict_row, connect_timeout=10,
            options=f"-cforest.writer_generation={WRITER_GENERATION}",
        ) as conn, conn.transaction():
            identity = await (await conn.execute(
                "SELECT bot_id FROM public.forest_bot_identity WHERE singleton"
            )).fetchone()
            runtime = await (await conn.execute(
                "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s FOR UPDATE",
                (job["bot_id"],),
            )).fetchone()
            if identity is None or identity["bot_id"] != job["bot_id"] or runtime is None:
                raise ForestError("ACTIVATION_BOT_IDENTITY_MISMATCH", "Current Bot identity differs", 409)
            if runtime["status"] == "SEALED" and (
                runtime["storage_generation"] == job["target_storage_generation"]
            ):
                return
            if (
                runtime["status"] not in {"ACTIVE", "SEALED"}
                or runtime["storage_generation"] != job["expected_storage_generation"]
            ):
                raise ForestError(
                    "ACTIVATION_CURRENT_STATE_CONFLICT", "Current Bot fence differs", 409
                )
            clock = await (await conn.execute(
                "SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s",
                (job["bot_id"],),
            )).fetchone()
            if (
                (0 if clock is None else clock["deletion_epoch"]) != job["deletion_epoch"]
                or await self._critical_digest(conn, job["bot_id"]) != job["safety_digest"]
                or await self._unresolved_effect(conn, job["bot_id"])
            ):
                raise ForestError(
                    "ACTIVATION_EFFECT_LEDGER_DIVERGED",
                    "Current critical effects changed before fencing",
                    409,
                )
            await conn.execute(
                "UPDATE public.bot_runtime_state SET status='SEALED',storage_generation=%s,"
                "maintenance_epoch=maintenance_epoch+1,data_admission=false,"
                "control_admission=false WHERE bot_id=%s",
                (job["target_storage_generation"], job["bot_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_host_lease SET host_id=NULL,incarnation=NULL,"
                "owner_epoch=owner_epoch+1,expires_at='1970-01-01 UTC' WHERE bot_id=%s",
                (job["bot_id"],),
            )

    async def _switch_head(self, job: dict) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, job)
            head = await (await conn.execute(
                "SELECT storage_generation FROM bot_storage_heads WHERE bot_id=%s FOR UPDATE",
                (job["bot_id"],),
            )).fetchone()
            if head is None:
                raise ForestError("ACTIVATION_DIRECTORY_CONFLICT", "Bot storage head is missing", 409)
            if head["storage_generation"] == job["target_storage_generation"]:
                return
            if head["storage_generation"] != job["expected_storage_generation"]:
                raise ForestError("ACTIVATION_DIRECTORY_CONFLICT", "Bot storage head changed", 409)
            await conn.execute(
                "UPDATE bot_storage_heads SET storage_generation=%s,revision=revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s",
                (job["target_storage_generation"], job["bot_id"]),
            )

    async def _activate_target(self, job: dict) -> None:
        target = json.loads(self.vault.decrypt(job["target_secret_ciphertext"]))["dsn"]
        async with await psycopg.AsyncConnection.connect(
            target, row_factory=dict_row, connect_timeout=10,
            options=f"-cforest.writer_generation={WRITER_GENERATION}",
        ) as conn, conn.transaction():
            identity = await (await conn.execute(
                "SELECT bot_id FROM public.forest_bot_identity WHERE singleton"
            )).fetchone()
            runtime = await (await conn.execute(
                "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s FOR UPDATE",
                (job["bot_id"],),
            )).fetchone()
            if identity is None or identity["bot_id"] != job["bot_id"] or runtime is None:
                raise ForestError("ACTIVATION_BOT_IDENTITY_MISMATCH", "Target Bot identity differs", 409)
            if (
                runtime["status"] == "ACTIVE"
                and runtime["storage_generation"] == job["target_storage_generation"]
            ):
                return
            if runtime["status"] != "SEALED":
                raise ForestError(
                    "ACTIVATION_TARGET_QUARANTINE_INVALID", "Target Bot is not sealed", 409
                )
            clock = await (await conn.execute(
                "SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s",
                (job["bot_id"],),
            )).fetchone()
            if (
                (0 if clock is None else clock["deletion_epoch"]) != job["deletion_epoch"]
                or await self._critical_digest(conn, job["bot_id"]) != job["safety_digest"]
                or await self._unresolved_effect(conn, job["bot_id"])
            ):
                raise ForestError(
                    "ACTIVATION_TARGET_CHANGED", "Target critical effects changed in quarantine", 409
                )
            await conn.execute(
                "UPDATE public.bot_runtime_state SET status='ACTIVE',storage_generation=%s,"
                "maintenance_epoch=maintenance_epoch+1,data_admission=true,"
                "control_admission=true WHERE bot_id=%s",
                (job["target_storage_generation"], job["bot_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_host_lease SET host_id=NULL,incarnation=NULL,"
                "owner_epoch=owner_epoch+1,expires_at='1970-01-01 UTC' WHERE bot_id=%s",
                (job["bot_id"],),
            )

    async def _execute(self, job: dict) -> dict[str, Any]:
        if job["state"] == "FENCING":
            await self._fence_current(job)
            job = await self._transition(job, "FENCING", "SWITCHING", "fenced_at")
        if job["state"] == "SWITCHING":
            await self._switch_head(job)
            job = await self._transition(job, "SWITCHING", "ACTIVATING", "switched_at")
        if job["state"] == "ACTIVATING":
            await self._activate_target(job)
            async with self.store.pool.connection() as conn, conn.transaction():
                await self._guard(conn, job)
                row = await (await conn.execute(
                    "UPDATE restore_activation_jobs SET state='COMPLETE',owner_id=NULL,"
                    "lease_expires_at=NULL,error_code=NULL,activated_at=clock_timestamp(),"
                    "completed_at=clock_timestamp(),updated_at=clock_timestamp() "
                    "WHERE activation_id=%s AND state='ACTIVATING' RETURNING *",
                    (job["activation_id"],),
                )).fetchone()
                await conn.execute(
                    "INSERT INTO restore_activation_events(activation_id,owner_epoch,step) "
                    "VALUES (%s,%s,'COMPLETE')",
                    (job["activation_id"], job["owner_epoch"]),
                )
                await conn.execute(
                    "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,"
                    "request_id,details) VALUES (%s,%s,'restore.activation.completed',%s,%s,%s)",
                    (
                        str(uuid4()), job["subject_id"], "bot:" + job["bot_id"],
                        job["activation_id"], Jsonb({
                            "activation_id": job["activation_id"],
                            "restore_id": job["restore_id"],
                            "storage_generation": job["target_storage_generation"],
                        }),
                    ),
                )
                return _public(row)
        raise ForestError("ACTIVATION_STATE_CHANGED", "Activation state changed", 409)

    async def _block(self, job: dict, code: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                "UPDATE restore_activation_jobs SET state='BLOCKED',owner_id=NULL,"
                "lease_expires_at=NULL,error_code=%s,updated_at=clock_timestamp() "
                "WHERE activation_id=%s AND state IN ('FENCING','SWITCHING','ACTIVATING') "
                "AND owner_id=%s AND owner_epoch=%s RETURNING *",
                (code, job["activation_id"], self.owner_id, job["owner_epoch"]),
            )).fetchone()
            if row is None:
                return _public(job)
            await conn.execute(
                "INSERT INTO restore_activation_events(activation_id,owner_epoch,step,details) "
                "VALUES (%s,%s,'BLOCKED',%s)",
                (job["activation_id"], job["owner_epoch"], Jsonb({"error_code": code})),
            )
            return _public(row)

    async def run_once(self) -> dict[str, Any] | None:
        job = await self._claim()
        if job is None:
            return None
        work = asyncio.create_task(self._execute(job), name="restore-activation-owned")
        renew = asyncio.create_task(self._renew(job), name="restore-activation-lease")
        try:
            done, _ = await asyncio.wait({work, renew}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                return await work
            error = renew.exception()
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            if error is not None:
                raise error
            raise ForestError("ACTIVATION_LEASE_LOST", "Activation ownership was lost", 409)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, ForestError) else "ACTIVATION_FAILED"
            return await self._block(job, code)
        finally:
            work.cancel()
            renew.cancel()
            await asyncio.gather(work, renew, return_exceptions=True)

    async def run_forever(self) -> None:
        self.closed = False
        while not self.closed:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(1)
