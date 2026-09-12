"""Recoverable, non-destructive provisioning of one database per Bot.

An immutable encrypted plan precedes external effects. CREATE ROLE installs a
random frozen SCRAM verifier atomically, so recovery can prove ownership even if
the process dies before COMMENT. CREATE DATABASE atomically records that proven
role as owner. Names alone never prove ownership; an unknown collision fails.

A deployment session advisory lock excludes healthy competing executors, and a
short lease/epoch fences every new DDL permit and local result commit. Losing the
lock/connection cannot revoke a DDL already sent to another connection: that
permit is its explicit in-flight boundary. Recovery reconciles the SAME plan and
ownership proof, including duplicate-object responses, before issuing any next
step. There is no cross-database transaction or claim of instantaneous DDL revoke.
No operation here drops databases, roles or user data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import suppress
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.management.catalog import BotCatalog
from forest.storage.bot import BotStore


def _lock_key(job_id: str) -> int:
    return int.from_bytes(hashlib.sha256(("forest.provision:" + job_id).encode()).digest()[:8],
                          "big", signed=True)


class BotProvisioner:
    def __init__(self, catalog: BotCatalog, *, lease_seconds: float = 30,
                 poll_interval: float = 1) -> None:
        if not 3 <= lease_seconds <= 300 or not 0.05 <= poll_interval <= 60:
            raise ValueError("Provisioner timing limits are invalid")
        self.catalog = catalog
        self.owner_id = str(uuid4())
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self._closed = asyncio.Event()

    async def close(self) -> None:
        self._closed.set()

    async def run_forever(self) -> None:
        while not self._closed.is_set():
            try:
                result = await self.run_once()
            except (psycopg.Error, OSError, TimeoutError):
                # Durable jobs remain resumable after deployment DB interruption.
                result = None
            if result is None:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._closed.wait(), self.poll_interval)

    async def _claim(self, conn: Any) -> dict | None:
        acquired_key = None
        try:
            async with conn.transaction():
                candidates = await (await conn.execute(
                    "SELECT * FROM bot_provision_jobs WHERE stage='PENDING' OR "
                    "(stage='PROVISIONING' AND lease_expires_at<=clock_timestamp()) "
                    "ORDER BY created_at,job_id LIMIT 32 FOR UPDATE SKIP LOCKED"
                )).fetchall()
                for row in candidates:
                    lock = await (await conn.execute("SELECT pg_try_advisory_lock(%s) AS acquired",
                                                    (_lock_key(row["job_id"]),))).fetchone()
                    if not lock["acquired"]:
                        continue
                    acquired_key = _lock_key(row["job_id"])
                    row = await (await conn.execute(
                        "UPDATE bot_provision_jobs SET stage='PROVISIONING',step='RECONCILING',"
                        "attempt=attempt+1,owner_id=%s,owner_epoch=owner_epoch+1,"
                        "lease_expires_at=clock_timestamp()+make_interval(secs => %s),"
                        "error_code=NULL,updated_at=clock_timestamp() WHERE job_id=%s RETURNING *",
                        (self.owner_id, self.lease_seconds, row["job_id"]),
                    )).fetchone()
                    await conn.execute(
                        "UPDATE bot_catalog SET stage='PROVISIONING',error_code=NULL,"
                        "revision=revision+1,updated_at=clock_timestamp() WHERE bot_id=%s", (row["bot_id"],),
                    )
                    await self._event(conn, row, "RECONCILING")
                    return row
                return None
        except BaseException:
            # Transaction rollback must precede unlock: SQL errors leave a
            # transaction aborted, while session advisory locks survive rollback.
            if acquired_key is not None:
                with suppress(psycopg.Error):
                    await conn.execute("SELECT pg_advisory_unlock(%s)", (acquired_key,))
            raise

    async def _event(self, conn: Any, job: dict, step: str, **details: Any) -> None:
        await conn.execute(
            "INSERT INTO bot_provision_events(job_id,owner_epoch,step,details) VALUES (%s,%s,%s,%s)",
            (job["job_id"], job["owner_epoch"], step, Jsonb(details)),
        )

    async def _guard(self, conn: Any, job: dict, *, authorize: bool = True) -> dict:
        if authorize:
            await self.catalog.store._active_account(conn, job["subject_id"], require_admin=True)
        row = await (await conn.execute(
            "SELECT * FROM bot_provision_jobs "
            "WHERE job_id=%s FOR UPDATE", (job["job_id"],),
        )).fetchone()
        # PostgreSQL can evaluate SELECT target expressions before LockRows.
        # Obtain time in a new statement AFTER the lock, never a projected bool.
        now = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
        if (row is None or row["stage"] != "PROVISIONING" or row["owner_id"] != self.owner_id
                or row["owner_epoch"] != job["owner_epoch"] or row["lease_expires_at"] <= now["now"]):
            raise ForestError("PROVISION_LEASE_LOST", "Provisioning ownership changed")
        return row

    async def _permit(self, job: dict, step: str) -> None:
        async with self.catalog.store.pool.connection() as conn, conn.transaction():
            await self._guard(conn, job)
            await conn.execute(
                "UPDATE bot_provision_jobs SET step=%s,updated_at=clock_timestamp() WHERE job_id=%s",
                (step, job["job_id"]),
            )
            await self._event(conn, job, step, boundary="permit_committed")

    async def _heartbeat(self, job: dict) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            try:
                async with self.catalog.store.pool.connection() as conn, conn.transaction():
                    await self._guard(conn, job, authorize=False)
                    await conn.execute(
                        "UPDATE bot_provision_jobs SET lease_expires_at=clock_timestamp()+"
                        "make_interval(secs => %s) WHERE job_id=%s", (self.lease_seconds, job["job_id"]),
                    )
            except (ForestError, psycopg.Error, OSError, TimeoutError):
                return  # Expired lease cannot be renewed; every next permit fails closed.

    async def run_once(self) -> dict | None:
        """Execute at most one durable job; cancellation leaves it reconcilable."""
        async with self.catalog.store.pool.connection() as lock_conn:
            job = await self._claim(lock_conn)
            if job is None:
                return None
            heartbeat = asyncio.create_task(self._heartbeat(job))
            try:
                try:
                    plan = await (await lock_conn.execute(
                        "SELECT p.*,c.mode FROM bot_storage_plans p JOIN bot_catalog c USING(bot_id) "
                        "WHERE bot_id=%s", (job["bot_id"],),
                    )).fetchone()
                    secret = json.loads(self.catalog.vault.decrypt(plan["secret_ciphertext"]))
                    if plan["mode"] == "managed":
                        await self._managed(job, plan, secret)
                    await self._initialize(job, plan, secret)
                    await self._finish(job)
                except ForestError as exc:
                    if exc.code != "PROVISION_LEASE_LOST":
                        await self._finish(job, error_code=exc.code)
                except (psycopg.Error, OSError, TimeoutError):
                    await self._finish(job, error_code="PROVISION_DATABASE_ERROR")
                except Exception:
                    await self._finish(job, error_code="PROVISION_FAILED")
                return await self.catalog._job(lock_conn, job["job_id"])
            except ForestError as exc:
                if exc.code != "PROVISION_LEASE_LOST":
                    raise
                return None
            finally:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
                # Session lock is always explicitly released before pooling.
                with suppress(psycopg.Error):
                    await lock_conn.execute("SELECT pg_advisory_unlock(%s)", (_lock_key(job["job_id"]),))

    async def _finish(self, job: dict, error_code: str | None = None) -> None:
        async with self.catalog.store.pool.connection() as conn, conn.transaction():
            # Revocation can record FAILED, but never authorize READY.
            await self._guard(conn, job, authorize=error_code is None)
            stage = "FAILED" if error_code else "READY"
            await conn.execute(
                "UPDATE bot_provision_jobs SET stage=%s,step=%s,error_code=%s,owner_id=NULL,"
                "lease_expires_at=NULL,updated_at=clock_timestamp() WHERE job_id=%s",
                (stage, stage, error_code, job["job_id"]),
            )
            await conn.execute(
                "UPDATE bot_catalog SET stage=%s,error_code=%s,revision=revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s", (stage, error_code, job["bot_id"]),
            )
            await self._event(conn, job, stage, error_code=error_code)

    @staticmethod
    def _marker(plan: dict) -> str:
        return "forest:bot-storage:" + json.dumps({
            "deployment": plan["deployment_id"], "bot": plan["bot_id"], "plan": plan["plan_digest"],
        }, sort_keys=True, separators=(",", ":"))

    async def _role(self, conn: Any, plan: dict, secret: dict) -> dict | None:
        row = await (await conn.execute(
            "SELECT oid,rolpassword,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,"
            "rolcanlogin,shobj_description(oid,'pg_authid') AS marker FROM pg_authid WHERE rolname=%s",
            (plan["role_name"],),
        )).fetchone()
        if row is None:
            return None
        member = await (await conn.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=%s OR roleid=%s) AS present",
            (row["oid"], row["oid"]),
        )).fetchone()
        if (row["rolpassword"] != secret["verifier"] or not row["rolcanlogin"]
                or any(row[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"))
                or member["present"] or row["marker"] not in (None, self._marker(plan))):
            raise ForestError("STORAGE_OWNERSHIP_CONFLICT", "Existing role is not owned by this frozen storage plan")
        return row

    async def _database(self, conn: Any, plan: dict, role: dict) -> dict | None:
        row = await (await conn.execute(
            "SELECT oid,datdba,datistemplate,shobj_description(oid,'pg_database') AS marker "
            "FROM pg_database WHERE datname=%s", (plan["database_name"],),
        )).fetchone()
        if row and (row["datdba"] != role["oid"] or row["datistemplate"]
                    or row["marker"] not in (None, self._marker(plan))):
            raise ForestError("STORAGE_OWNERSHIP_CONFLICT", "Existing database is not owned by this frozen storage plan")
        return row

    async def _ddl(self, conn: Any, job: dict, step: str, statement: Any) -> None:
        await self._permit(job, step)
        try:
            await conn.execute(statement)
        except (psycopg.errors.DuplicateObject, psycopg.errors.DuplicateDatabase):
            # A previously permitted command may have completed after connection
            # loss. Caller MUST verify the frozen ownership proof immediately.
            pass
        await self._permit(job, step + "_RECONCILE")

    async def _managed(self, job: dict, plan: dict, secret: dict) -> None:
        if not self.catalog.managed_dsn:
            raise ForestError("MANAGED_STORAGE_UNAVAILABLE", "Managed PostgreSQL is not configured", 503)
        async with await psycopg.AsyncConnection.connect(
            self.catalog.managed_dsn, autocommit=True, row_factory=dict_row, connect_timeout=5,
            options="-cstatement_timeout=20000 -clock_timeout=5000",
        ) as conn:
            cluster = await (await conn.execute(
                "SELECT system_identifier::text AS id FROM pg_control_system()"
            )).fetchone()
            if cluster["id"] != plan["cluster_id"]:
                raise ForestError("STORAGE_TARGET_CHANGED", "Managed cluster differs from the frozen target")
            role = await self._role(conn, plan, secret)
            if role is None:
                await self._ddl(conn, job, "CREATE_ROLE", sql.SQL(
                    "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}"
                ).format(sql.Identifier(plan["role_name"]), sql.Literal(secret["verifier"])))
                role = await self._role(conn, plan, secret)
            if role is None:
                raise ForestError("STORAGE_OWNERSHIP_CONFLICT", "Frozen storage role is missing")
            if role["marker"] is None:
                await self._ddl(conn, job, "MARK_ROLE", sql.SQL("COMMENT ON ROLE {} IS {}").format(
                    sql.Identifier(plan["role_name"]), sql.Literal(self._marker(plan))))
            database = await self._database(conn, plan, role)
            if database is None:
                await self._ddl(conn, job, "CREATE_DATABASE", sql.SQL(
                    "CREATE DATABASE {} OWNER {} TEMPLATE template0"
                ).format(sql.Identifier(plan["database_name"]), sql.Identifier(plan["role_name"])))
                database = await self._database(conn, plan, role)
            if database is None:
                raise ForestError("STORAGE_OWNERSHIP_CONFLICT", "Frozen storage database is missing")
            if database["marker"] is None:
                await self._ddl(conn, job, "MARK_DATABASE", sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(plan["database_name"]), sql.Literal(self._marker(plan))))
            # No Bot facts are initialized until PUBLIC access to this empty DB is
            # revoked. The same frozen owner survives an interruption here.
            await self._ddl(conn, job, "RESTRICT_DATABASE", sql.SQL(
                "REVOKE ALL ON DATABASE {} FROM PUBLIC"
            ).format(sql.Identifier(plan["database_name"])))

    async def _initialize(self, job: dict, plan: dict, secret: dict) -> None:
        await self._permit(job, "VALIDATE_RUNTIME_ROLE")
        async with await psycopg.AsyncConnection.connect(
            secret["dsn"], autocommit=True, row_factory=dict_row, connect_timeout=5,
            options="-cstatement_timeout=20000 -clock_timeout=5000",
        ) as conn:
            role = await (await conn.execute(
                "SELECT r.oid,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,"
                "d.datdba,current_database() AS database_name,current_user AS role_name "
                "FROM pg_roles r JOIN pg_database d ON d.datname=current_database() WHERE r.rolname=current_user"
            )).fetchone()
            member = await (await conn.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=%s) AS present", (role["oid"],),
            )).fetchone()
            if (any(role[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"))
                    or member["present"] or role["datdba"] != role["oid"]):
                raise ForestError("UNSAFE_RUNTIME_ROLE", "Bot storage requires a dedicated nonprivileged database owner", 422)
            if (role["database_name"] != plan["database_name"] or role["role_name"] != plan["role_name"]):
                raise ForestError("STORAGE_TARGET_CHANGED", "Runtime connection differs from the frozen target")
        await self._permit(job, "INITIALIZE_BOT")
        bot = BotStore(secret["dsn"], job["bot_id"])
        try:
            await bot.open()
            await bot.migrate()
        finally:
            await bot.close()
        await self._permit(job, "BOT_IDENTITY_CONFIRMED")
