"""Deployment-authority transactions; no cross-Bot atomicity is implied here."""

from __future__ import annotations

import hashlib
from datetime import datetime
from importlib.resources import files
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from forest.errors import ForestError


class PostgresStore:
    """Connection pool and short transactions for the initial management slice.

    Call ``open`` and ``migrate`` explicitly during startup. This store owns only
    deployment resources; each future Bot authority has its own store and guards.
    Configuration fields accepted by the P0 service are all dynamic, therefore
    desired and effective advance together in the commit below.
    """

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 8) -> None:
        self.pool = AsyncConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row, "autocommit": True},
            timeout=10,
        )

    async def open(self) -> None:
        await self.pool.open()
        await self.pool.wait(timeout=15)

    async def close(self) -> None:
        await self.pool.close()

    async def migrate(self) -> None:
        """Versioned, checksummed DDL serialized across independent processes."""
        migration_root = files("forest.storage").joinpath("migrations")
        migrations = sorted(
            (item for item in migration_root.iterdir() if item.name.endswith(".sql")),
            key=lambda item: item.name,
        )
        async with self.pool.connection() as conn, conn.transaction():
            # Database-wide serialization also covers an initially missing table.
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (679330170781,))
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS forest_schema_migrations ("
                "version text PRIMARY KEY, checksum text NOT NULL, "
                "applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
            )
            existing = await (await conn.execute(
                "SELECT version, checksum FROM forest_schema_migrations ORDER BY version"
            )).fetchall()
            known = {item.name for item in migrations}
            if any(row["version"] not in known for row in existing):
                raise ForestError("SCHEMA_TOO_NEW", "Database requires a newer Forest version")
            applied = {row["version"]: row["checksum"] for row in existing}
            for item in migrations:
                source = item.read_text(encoding="utf-8")
                checksum = hashlib.sha256(source.encode("utf-8")).hexdigest()
                if item.name in applied:
                    if applied[item.name] != checksum:
                        raise ForestError("MIGRATION_CHANGED", "Applied migration checksum changed")
                    continue
                await conn.execute(source)
                await conn.execute(
                    "INSERT INTO forest_schema_migrations(version, checksum) VALUES (%s, %s)",
                    (item.name, checksum),
                )

    async def setup(
        self, setup_id: str, plan_digest: str, username: str, password_hash: str
    ) -> dict[str, Any]:
        """Atomically establish exactly one initial administrator in this database.

        The bootstrap authority must already have frozen this target and plan.
        Replaying that same frozen plan returns its account, including after a
        target commit followed by loss of the bootstrap completion acknowledgement.
        """
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (679330170782,))
            current = await (await conn.execute(
                "SELECT s.setup_id, s.plan_digest, a.* FROM web_setup_state s "
                "JOIN web_accounts a ON a.id = s.account_id WHERE s.singleton"
            )).fetchone()
            if current is not None:
                if current["setup_id"] != setup_id:
                    raise ForestError("ALREADY_INITIALIZED", "This database is already initialized")
                if current["plan_digest"] != plan_digest or current["username"] != username:
                    raise ForestError("SETUP_CONFLICT", "Initialization plan is already frozen")
                return self._account(current)
            account_id = str(uuid4())
            row = await (await conn.execute(
                "INSERT INTO web_accounts(id, username, password_hash, role) "
                "VALUES (%s, %s, %s, 'admin') RETURNING *",
                (account_id, username, password_hash),
            )).fetchone()
            await conn.execute(
                "INSERT INTO web_setup_state(singleton, setup_id, plan_digest, account_id) "
                "VALUES (true, %s, %s, %s)", (setup_id, plan_digest, account_id),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id, subject_id, action, resource_key, details) "
                "VALUES (%s, %s, 'deployment.setup', 'deployment', %s)",
                (str(uuid4()), account_id, Jsonb({"setup_id": setup_id, "plan_digest": plan_digest})),
            )
            assert row is not None
            return self._account(row)

    async def setup_status(self, setup_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            return await (await conn.execute(
                "SELECT s.setup_id, s.plan_digest, s.account_id, s.completed_at, a.username "
                "FROM web_setup_state s JOIN web_accounts a ON a.id = s.account_id "
                "WHERE s.setup_id = %s", (setup_id,),
            )).fetchone()

    @staticmethod
    def _account(row: dict[str, Any]) -> dict[str, Any]:
        return {key: row[key] for key in ("id", "username", "password_hash", "disabled", "role")}

    async def find_account(self, username: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT id, username, password_hash, disabled, role FROM web_accounts "
                "WHERE username = %s", (username,),
            )).fetchone()
            return row

    async def create_session(
        self, token_hash: str, csrf_hash: str, account_id: str, expires_at: datetime
    ) -> None:
        self._require_timestamp(expires_at)
        async with self.pool.connection() as conn, conn.transaction():
            await self._active_account(conn, account_id)
            await conn.execute(
                "INSERT INTO web_sessions(token_hash, csrf_hash, account_id, expires_at) "
                "VALUES (%s, %s, %s, %s)", (token_hash, csrf_hash, account_id, expires_at),
            )

    async def get_session(self, token_hash: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            return await (await conn.execute(
                "SELECT s.token_hash, s.csrf_hash, s.account_id, a.username, a.role, s.expires_at "
                "FROM web_sessions s JOIN web_accounts a ON a.id = s.account_id "
                "WHERE s.token_hash = %s AND s.revoked_at IS NULL "
                "AND s.expires_at > clock_timestamp() AND NOT a.disabled", (token_hash,),
            )).fetchone()

    async def revoke_session(self, token_hash: str) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "UPDATE web_sessions SET revoked_at = COALESCE(revoked_at, clock_timestamp()) "
                "WHERE token_hash = %s", (token_hash,),
            )

    @staticmethod
    def _resource(resource_key: str) -> None:
        if resource_key != "deployment":
            raise ForestError("UNSUPPORTED_RESOURCE", "This store manages deployment only", status=400)

    @staticmethod
    def _require_timestamp(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ForestError("INVALID_EXPIRY", "Expiry must include its time zone", status=400)

    @staticmethod
    async def _active_account(
        conn: AsyncConnection, subject_id: str, *, require_admin: bool = False
    ) -> dict[str, Any]:
        row = await (await conn.execute(
            "SELECT id, disabled, role FROM web_accounts WHERE id = %s FOR UPDATE", (subject_id,),
        )).fetchone()
        if row is None or row["disabled"] or (require_admin and row["role"] != "admin"):
            raise ForestError("FORBIDDEN", "Current account cannot perform this operation", status=403)
        return row

    async def read_config(self, resource_key: str) -> dict[str, Any]:
        self._resource(resource_key)
        async with self.pool.connection() as conn:
            # One statement keeps desired/effective on one MVCC snapshot.
            row = await (await conn.execute(
                "SELECT h.resource_key, h.head_revision, d.revision AS desired_revision, "
                "d.values AS desired_values, d.schema_digest AS desired_schema_digest, "
                "d.created_at AS desired_created_at, e.revision AS effective_revision, "
                "e.values AS effective_values, e.schema_digest AS effective_schema_digest, "
                "e.created_at AS effective_created_at "
                "FROM config_heads h "
                "LEFT JOIN config_revisions d ON d.resource_key = h.resource_key "
                "AND d.revision = h.head_revision "
                "LEFT JOIN config_revisions e ON e.resource_key = h.resource_key "
                "AND e.revision = h.effective_revision WHERE h.resource_key = %s", (resource_key,),
            )).fetchone()
            if row is None:
                return {"resource_key": resource_key, "head_revision": 0, "desired": None, "effective": None}
            result: dict[str, Any] = {"resource_key": resource_key, "head_revision": row["head_revision"]}
            for kind in ("desired", "effective"):
                result[kind] = self._revision(
                    resource_key, row[f"{kind}_revision"], row[f"{kind}_values"],
                    row[f"{kind}_schema_digest"], row[f"{kind}_created_at"],
                ) if row[f"{kind}_revision"] is not None else None
            return result

    @staticmethod
    def _revision(
        resource_key: str, revision: int, values: dict[str, Any], schema_digest: str, created_at: datetime,
    ) -> dict[str, Any]:
        return {"revision_ref": f"{resource_key}:{revision}", "revision": revision, "values": values,
                "schema_digest": schema_digest, "created_at": created_at.isoformat()}

    async def save_plan(
        self, plan_id: str, subject_id: str, resource_key: str, base_revision: int,
        values: dict[str, Any], schema_digest: str, plan_digest: str, expires_at: datetime,
    ) -> dict[str, Any]:
        self._resource(resource_key)
        self._require_timestamp(expires_at)
        async with self.pool.connection() as conn, conn.transaction():
            await self._active_account(conn, subject_id, require_admin=True)
            row = await (await conn.execute(
                "INSERT INTO config_plans(plan_id, subject_id, resource_key, base_revision, values, "
                "schema_digest, plan_digest, expires_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "RETURNING *",
                (plan_id, subject_id, resource_key, base_revision, Jsonb(values), schema_digest, plan_digest, expires_at),
            )).fetchone()
            assert row is not None
            return row

    async def get_plan(self, plan_id: str, subject_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            return await (await conn.execute(
                "SELECT p.* FROM config_plans p JOIN web_accounts a ON a.id = p.subject_id "
                "WHERE p.plan_id = %s AND p.subject_id = %s AND NOT a.disabled AND a.role = 'admin'",
                (plan_id, subject_id),
            )).fetchone()

    async def apply_config(
        self, resource_key: str, values: dict[str, Any], expected_revision: int,
        idempotency_key: str, subject_id: str, plan_digest: str, *, plan_id: str, schema_digest: str,
        session_token_hash: str | None = None,
    ) -> dict[str, Any]:
        self._resource(resource_key)
        if not idempotency_key or len(idempotency_key) > 200:
            raise ForestError("INVALID_IDEMPOTENCY_KEY", "A bounded idempotency key is required", status=400)
        async with self.pool.connection() as conn, conn.transaction():
            # Current authority is checked in the same transaction as the effect.
            # Account disable/role changes lock this same row before they commit.
            await self._active_account(conn, subject_id, require_admin=True)
            if session_token_hash is not None:
                await self._active_session(conn, session_token_hash, subject_id)
            previous = await (await conn.execute(
                "SELECT * FROM management_requests WHERE subject_id = %s AND resource_key = %s "
                "AND idempotency_key = %s", (subject_id, resource_key, idempotency_key),
            )).fetchone()
            if previous is not None:
                fixed = {"plan_id": plan_id, "plan_digest": plan_digest,
                         "expected_revision": expected_revision, "values": values}
                if any(previous[key] != value for key, value in fixed.items()):
                    raise ForestError("IDEMPOTENCY_CONFLICT", "Idempotency key was used for different input")
                # A committed request remains recoverable after its preview expires
                # or the server installs a new schema. The immutable plan ID/digest
                # already binds the schema used for that original operation; the
                # current server schema gates only new admission below.
                return previous["result"]
            plan = await (await conn.execute(
                "SELECT *, expires_at > clock_timestamp() AS unexpired FROM config_plans "
                "WHERE plan_id = %s AND subject_id = %s", (plan_id, subject_id),
            )).fetchone()
            if plan is None:
                raise ForestError("PLAN_NOT_FOUND", "Change plan is unavailable", status=404)
            if not plan["unexpired"]:
                raise ForestError("PLAN_EXPIRED", "Preview the change again before applying")
            fixed_plan = {"resource_key": resource_key, "base_revision": expected_revision,
                          "values": values, "schema_digest": schema_digest, "plan_digest": plan_digest}
            if any(plan[key] != value for key, value in fixed_plan.items()):
                raise ForestError("PLAN_MISMATCH", "Apply input no longer matches the frozen change plan")
            await conn.execute(
                "INSERT INTO config_heads(resource_key) VALUES (%s) ON CONFLICT DO NOTHING", (resource_key,),
            )
            head = await (await conn.execute(
                "SELECT head_revision FROM config_heads WHERE resource_key = %s FOR UPDATE", (resource_key,),
            )).fetchone()
            assert head is not None
            if head["head_revision"] != expected_revision:
                raise ForestError("REVISION_CONFLICT", "Configuration changed since preview",
                                  details={"current_revision": head["head_revision"]})
            # clock_timestamp is checked again after potentially waiting on the head lock.
            valid = await (await conn.execute(
                "SELECT expires_at > clock_timestamp() AS unexpired FROM config_plans WHERE plan_id = %s",
                (plan_id,),
            )).fetchone()
            if not valid or not valid["unexpired"]:
                raise ForestError("PLAN_EXPIRED", "Change plan expired before commit")
            if session_token_hash is not None:
                await self._active_session(conn, session_token_hash, subject_id)
            revision = expected_revision + 1
            request_id = str(uuid4())
            timestamp = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
            assert timestamp is not None
            created_at = timestamp["now"]
            revision_value = self._revision(resource_key, revision, values, schema_digest, created_at)
            result = {"resource_key": resource_key, "head_revision": revision,
                      "desired": revision_value, "effective": revision_value, "request_id": request_id}
            await conn.execute(
                "INSERT INTO management_requests(request_id, subject_id, resource_key, idempotency_key, "
                "plan_id, plan_digest, schema_digest, expected_revision, values, result) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (request_id, subject_id, resource_key, idempotency_key, plan_id, plan_digest,
                 schema_digest, expected_revision, Jsonb(values), Jsonb(result)),
            )
            await conn.execute(
                "INSERT INTO config_revisions(resource_key, revision, previous_revision, schema_digest, "
                "values, plan_id, plan_digest, request_id, created_by, created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (resource_key, revision, expected_revision, schema_digest, Jsonb(values), plan_id,
                 plan_digest, request_id, subject_id, created_at),
            )
            await conn.execute(
                "UPDATE config_heads SET head_revision = %s, effective_revision = %s "
                "WHERE resource_key = %s", (revision, revision, resource_key),
            )
            await conn.execute(
                "INSERT INTO audit_records(audit_id, subject_id, action, resource_key, request_id, details) "
                "VALUES (%s, %s, 'config.apply', %s, %s, %s)",
                (str(uuid4()), subject_id, resource_key, request_id,
                 Jsonb({"revision": revision, "previous_revision": expected_revision,
                        "plan_id": plan_id, "plan_digest": plan_digest})),
            )
            await conn.execute(
                "INSERT INTO outbox_events(event_id, topic, resource_key, request_id, payload) "
                "VALUES (%s, 'config.applied', %s, %s, %s)",
                (str(uuid4()), resource_key, request_id, Jsonb({"revision": revision})),
            )
            return result

    @staticmethod
    async def _active_session(conn: AsyncConnection, token_hash: str, subject_id: str) -> None:
        session = await (await conn.execute(
            "SELECT account_id, revoked_at, expires_at > clock_timestamp() AS unexpired "
            "FROM web_sessions WHERE token_hash = %s FOR UPDATE", (token_hash,),
        )).fetchone()
        if (session is None or session["account_id"] != subject_id
                or session["revoked_at"] is not None or not session["unexpired"]):
            raise ForestError("SESSION_INVALID", "Management session has expired or was revoked", status=401)
