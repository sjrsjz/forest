"""Deployment Bot directory. Public DTOs never contain storage credentials.

Only this directory owns display names/storage plans. It is not an authority for
Bot configuration, policy, state, host leases or business writes. A provision job
is a bounded delegation accepted from a current administrator/session; subsequent
steps recheck the administrator, but browser logout does not cancel accepted work.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forest.bootstrap import normalize_dsn
from forest.errors import ForestError
from forest.security import SecretVault
from forest.storage.postgres import PostgresStore


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _public(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {
        key: row[key].isoformat() if isinstance(row[key], datetime) else row[key]
        for key in keys
    }


_BOT_KEYS = (
    "bot_id", "display_name", "mode", "stage", "revision", "job_id", "error_code",
    "created_at", "updated_at",
)
_JOB_KEYS = (
    "job_id", "bot_id", "stage", "step", "attempt", "owner_epoch", "error_code",
    "created_at", "updated_at",
)


class BotCatalog:
    def __init__(
        self, store: PostgresStore, vault: SecretVault, *, managed_dsn: str | None = None
    ) -> None:
        self.store = store
        self.vault = vault
        self.managed_dsn = managed_dsn

    async def _authorize(
        self, conn: Any, subject_id: str, session_token_hash: str, *, require_admin: bool = True
    ) -> None:
        await self.store._active_account(conn, subject_id, require_admin=require_admin)
        await self.store._active_session(conn, session_token_hash, subject_id)

    @staticmethod
    def _validate(display_name: str | None, idempotency_key: str, expected_revision: int) -> None:
        if display_name is not None and (
            not isinstance(display_name, str) or not 1 <= len(display_name.strip()) <= 120
            or any(ord(char) < 32 for char in display_name)
        ):
            raise ForestError("VALIDATION_FAILED", "Bot display name must contain 1 to 120 characters", 422)
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 160:
            raise ForestError("VALIDATION_FAILED", "A bounded idempotency key is required", 422)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ForestError("VALIDATION_FAILED", "Expected revision must be a nonnegative integer", 422)

    async def _replay(self, conn: Any, subject_id: str, key: str, digest: str) -> dict | None:
        row = await (await conn.execute(
            "SELECT semantic_digest,result FROM bot_catalog_requests "
            "WHERE subject_id=%s AND idempotency_key=%s", (subject_id, key),
        )).fetchone()
        if row:
            if row["semantic_digest"] != digest:
                raise ForestError("IDEMPOTENCY_CONFLICT", "Idempotency key was used for different input")
            return row["result"]
        return None

    async def _record(self, conn: Any, subject_id: str, key: str, digest: str,
                      action: str, row: dict) -> dict:
        result = _public(row, _BOT_KEYS)
        request_id = str(uuid4())
        await conn.execute(
            "INSERT INTO bot_catalog_requests(request_id,subject_id,idempotency_key,"
            "semantic_digest,bot_id,result) VALUES (%s,%s,%s,%s,%s,%s)",
            (request_id, subject_id, key, digest, row["bot_id"], Jsonb(result)),
        )
        await conn.execute(
            "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid4()), subject_id, action, f"bot:{row['bot_id']}", request_id,
             Jsonb({"job_id": row["job_id"], "revision": row["revision"]})),
        )
        return result

    async def create(
        self, *, subject_id: str, session_token_hash: str, display_name: str,
        idempotency_key: str, mode: str = "managed", dsn: str | None = None,
        expected_revision: int = 0,
    ) -> dict[str, Any]:
        self._validate(display_name, idempotency_key, expected_revision)
        if expected_revision != 0:
            raise ForestError("REVISION_CONFLICT", "New Bot creation expects revision zero")
        if mode not in {"managed", "external"} or (mode == "managed" and dsn is not None):
            raise ForestError("VALIDATION_FAILED", "Select managed storage or an external DSN", 422)
        normalized = normalize_dsn(dsn)[0] if mode == "external" else None
        digest = _digest({"action": "create", "name": display_name.strip(), "mode": mode,
                          "dsn": normalized, "expected_revision": 0})
        # Do not hold a database transaction across the managed cluster query.
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            replay = await self._replay(conn, subject_id, idempotency_key, digest)
            if replay:
                return replay
            setup = await (await conn.execute(
                "SELECT setup_id FROM web_setup_state WHERE singleton"
            )).fetchone()
            if not setup:
                raise ForestError("NOT_INITIALIZED", "Deployment setup is required", 503)
        bot_id, job_id = str(uuid4()), str(uuid4())
        plan = await self._prepare_plan(bot_id, setup["setup_id"], mode, normalized)
        async with self.store.pool.connection() as conn, conn.transaction():
            # Account row serializes each subject's idempotency keys.
            await self._authorize(conn, subject_id, session_token_hash)
            replay = await self._replay(conn, subject_id, idempotency_key, digest)
            if replay:
                return replay
            row = await (await conn.execute(
                "INSERT INTO bot_catalog(bot_id,display_name,mode,stage,job_id,created_by) "
                "VALUES (%s,%s,%s,'PENDING',%s,%s) RETURNING *",
                (bot_id, display_name.strip(), mode, job_id, subject_id),
            )).fetchone()
            await conn.execute(
                "INSERT INTO bot_storage_plans(bot_id,deployment_id,plan_digest,cluster_id,"
                "database_name,role_name,secret_ciphertext) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (bot_id, setup["setup_id"], plan["plan_digest"], plan["cluster_id"],
                 plan["database_name"], plan["role_name"], plan["secret_ciphertext"]),
            )
            await conn.execute(
                "INSERT INTO bot_provision_jobs(job_id,bot_id,subject_id,stage) "
                "VALUES (%s,%s,%s,'PENDING')", (job_id, bot_id, subject_id),
            )
            await conn.execute(
                "INSERT INTO bot_provision_events(job_id,owner_epoch,step) VALUES (%s,0,'FROZEN')",
                (job_id,),
            )
            return await self._record(conn, subject_id, idempotency_key, digest, "bot.create", row)

    async def _prepare_plan(self, bot_id: str, deployment_id: str,
                            mode: str, dsn: str | None) -> dict[str, Any]:
        cluster_id = None
        if mode == "managed":
            if not self.managed_dsn:
                raise ForestError("MANAGED_STORAGE_UNAVAILABLE", "Managed PostgreSQL is not configured", 503)
            role_name = "forest_r_" + bot_id.replace("-", "")
            database_name = "forest_b_" + bot_id.replace("-", "")
            password = secrets.token_urlsafe(48)
            try:
                async with await psycopg.AsyncConnection.connect(
                    self.managed_dsn, autocommit=True, row_factory=dict_row, connect_timeout=5
                ) as conn:
                    cluster = await (await conn.execute(
                        "SELECT system_identifier::text AS id FROM pg_control_system()"
                    )).fetchone()
                    cluster_id = cluster["id"]
                    verifier = conn.pgconn.encrypt_password(
                        password.encode(), role_name.encode(), b"scram-sha-256"
                    ).decode()
                options = conninfo_to_dict(self.managed_dsn)
                # Deployment search_path/service/passfile cannot flow into runtime credentials.
                allowed = {"host", "hostaddr", "port", "sslmode", "connect_timeout"}
                dsn = make_conninfo(**{k: v for k, v in options.items() if k in allowed},
                                    dbname=database_name, user=role_name, password=password)
            except (psycopg.Error, ValueError):
                raise ForestError("PROVISIONER_UNAVAILABLE", "Cannot prepare managed PostgreSQL storage", 503) from None
            secret = {"dsn": dsn, "verifier": verifier}
        else:
            assert dsn is not None
            parts = conninfo_to_dict(dsn)
            database_name, role_name = parts["dbname"], parts["user"]
            secret = {"dsn": dsn}
        identity = {"bot_id": bot_id, "deployment_id": deployment_id, "mode": mode,
                    "cluster_id": cluster_id, "database_name": database_name,
                    "role_name": role_name, "secret": secret}
        return {"cluster_id": cluster_id, "database_name": database_name, "role_name": role_name,
                "plan_digest": _digest(identity),
                "secret_ciphertext": self.vault.encrypt(json.dumps(secret))}

    async def list(self, *, subject_id: str, session_token_hash: str) -> list[dict[str, Any]]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, require_admin=False)
            rows = await (await conn.execute("SELECT * FROM bot_catalog ORDER BY created_at,bot_id")).fetchall()
            return [_public(row, _BOT_KEYS) for row in rows]

    async def get(self, *, bot_id: str, subject_id: str, session_token_hash: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, require_admin=False)
            row = await (await conn.execute("SELECT * FROM bot_catalog WHERE bot_id=%s", (bot_id,))).fetchone()
            if row is None:
                raise ForestError("BOT_NOT_FOUND", "Bot does not exist", 404)
            return _public(row, _BOT_KEYS)

    async def read_job(self, *, job_id: str, subject_id: str,
                       session_token_hash: str) -> dict[str, Any]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash, require_admin=False)
            return await self._job(conn, job_id)

    async def _job(self, conn: Any, job_id: str) -> dict[str, Any]:
        row = await (await conn.execute("SELECT * FROM bot_provision_jobs WHERE job_id=%s", (job_id,))).fetchone()
        if row is None:
            raise ForestError("JOB_NOT_FOUND", "Provisioning job does not exist", 404)
        result = _public(row, _JOB_KEYS)
        events = await (await conn.execute(
            "SELECT event_id,owner_epoch,step,details,created_at FROM bot_provision_events "
            "WHERE job_id=%s ORDER BY event_id", (job_id,),
        )).fetchall()
        result["events"] = [_public(event, ("event_id", "owner_epoch", "step", "details", "created_at"))
                            for event in events]
        return result

    async def rename(self, *, bot_id: str, display_name: str, expected_revision: int,
                     idempotency_key: str, subject_id: str, session_token_hash: str) -> dict:
        self._validate(display_name, idempotency_key, expected_revision)
        return await self._change(bot_id=bot_id, display_name=display_name.strip(),
                                  expected_revision=expected_revision, idempotency_key=idempotency_key,
                                  subject_id=subject_id, session_token_hash=session_token_hash)

    async def retry(self, *, bot_id: str, expected_revision: int, idempotency_key: str,
                    subject_id: str, session_token_hash: str) -> dict:
        self._validate(None, idempotency_key, expected_revision)
        return await self._change(bot_id=bot_id, display_name=None, expected_revision=expected_revision,
                                  idempotency_key=idempotency_key, subject_id=subject_id,
                                  session_token_hash=session_token_hash)

    async def _change(self, *, bot_id: str, display_name: str | None, expected_revision: int,
                      idempotency_key: str, subject_id: str, session_token_hash: str) -> dict:
        action = "bot.rename" if display_name is not None else "bot.retry"
        digest = _digest({"action": action, "bot_id": bot_id, "name": display_name,
                          "expected_revision": expected_revision})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize(conn, subject_id, session_token_hash)
            replay = await self._replay(conn, subject_id, idempotency_key, digest)
            if replay:
                return replay
            # Job before catalog is also the provisioner's lock order.
            await conn.execute("SELECT job_id FROM bot_provision_jobs WHERE bot_id=%s FOR UPDATE", (bot_id,))
            row = await (await conn.execute(
                "SELECT * FROM bot_catalog WHERE bot_id=%s FOR UPDATE", (bot_id,),
            )).fetchone()
            if row is None:
                raise ForestError("BOT_NOT_FOUND", "Bot does not exist", 404)
            if row["revision"] != expected_revision:
                raise ForestError("REVISION_CONFLICT", "Bot directory revision changed",
                                  details={"current_revision": row["revision"]})
            await self._authorize(conn, subject_id, session_token_hash)
            if display_name is None:
                if row["stage"] != "FAILED":
                    raise ForestError("JOB_NOT_FAILED", "Only failed provisioning can be retried")
                await conn.execute(
                    "UPDATE bot_provision_jobs SET stage='PENDING',subject_id=%s,error_code=NULL,"
                    "owner_id=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE bot_id=%s",
                    (subject_id, bot_id),
                )
                await conn.execute(
                    "INSERT INTO bot_provision_events(job_id,owner_epoch,step) "
                    "SELECT job_id,owner_epoch,'RETRY_REQUESTED' FROM bot_provision_jobs WHERE bot_id=%s", (bot_id,),
                )
            updated = await (await conn.execute(
                "UPDATE bot_catalog SET display_name=COALESCE(%s,display_name),"
                "stage=CASE WHEN %s THEN 'PENDING' ELSE stage END,"
                "error_code=CASE WHEN %s THEN NULL ELSE error_code END,"
                "revision=revision+1,updated_at=clock_timestamp() WHERE bot_id=%s RETURNING *",
                (display_name, display_name is None, display_name is None, bot_id),
            )).fetchone()
            return await self._record(conn, subject_id, idempotency_key, digest, action, updated)

    async def list_ready(self) -> list[dict[str, Any]]:
        """Trusted Fleet directory projection; this grants no Bot write authority."""
        async with self.store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT * FROM bot_catalog WHERE stage='READY' ORDER BY bot_id"
            )).fetchall()
            return [_public(row, _BOT_KEYS) for row in rows]

    async def resolve_dsn(self, bot_id: str) -> str:
        """Trusted host only. Never expose through a management response."""
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT p.secret_ciphertext FROM bot_catalog b JOIN bot_storage_plans p USING(bot_id) "
                "WHERE bot_id=%s AND b.stage='READY'", (bot_id,),
            )).fetchone()
            if row is None:
                raise ForestError("BOT_NOT_READY", "Bot storage is not ready")
            return json.loads(self.vault.decrypt(row["secret_ciphertext"]))["dsn"]
