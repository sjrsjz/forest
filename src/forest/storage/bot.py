"""Single-Bot PostgreSQL authority and final guarded commits.

Only the trusted host owns this repository and its DSN. Worker messages never
select a Bot, lease, or grant: the host binds those from its admitted call.
Lock order: component lifecycle resource (where needed), component route, Bot
runtime/policy, host lease, call, state heads, immutable steps/facts/outbox.
No transaction performs external I/O. Lifecycle logical leases are not row locks
held over Worker preparation or drain waits.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from importlib.resources import files
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from forest.errors import ForestError


@dataclass(frozen=True)
class HostLease:
    bot_id: str
    host_id: str
    incarnation: str
    owner_epoch: int
    expires_at: datetime


@dataclass(frozen=True)
class CallGrant:
    bot_id: str
    call_id: str
    operation_id: str
    component_id: str
    generation_id: str
    route_epoch: int
    worker_incarnation: str
    host_id: str
    host_incarnation: str
    owner_epoch: int
    storage_generation: int
    maintenance_epoch: int
    policy_revision: int
    cancel_epoch: int
    deadline_at: datetime
    input_digest: str
    parent_call_id: str | None = None
    root_call_id: str | None = None
    call_depth: int = 0
    dependency_components: tuple[str, ...] = ()
    admission_lane: str = "DATA"
    control_service: str | None = None


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()


class BotStore:
    """Fixed Bot identity in a dedicated database; no per-call scope selector."""

    def __init__(self, dsn: str, bot_id: str) -> None:
        if not bot_id or len(bot_id) > 128:
            raise ValueError("bot_id must contain 1 to 128 characters")
        self._bot_id = bot_id
        self.pool = AsyncConnectionPool(
            dsn,
            min_size=1,
            max_size=8,
            open=False,
            timeout=10,
            kwargs={"autocommit": True, "row_factory": dict_row, "options": "-csearch_path=public"},
        )

    @property
    def bot_id(self) -> str:
        return self._bot_id

    async def open(self) -> None:
        await self.pool.open()
        await self.pool.wait(timeout=15)

    async def close(self) -> None:
        await self.pool.close()

    async def migrate(self) -> None:
        migrations = sorted(
            files("forest.storage").joinpath("bot_migrations").iterdir(), key=lambda p: p.name
        )
        async with self.pool.connection() as conn, conn.transaction():
            # Same database-level lock as deployment migration: identity selection
            # cannot race a different Forest authority's initialization.
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (679330170781,))
            tables = await (
                await conn.execute(
                    "SELECT n.nspname AS schema, c.relname AS name FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p') "
                    "AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'"
                )
            ).fetchall()
            if any(
                row["name"] in {"forest_schema_migrations", "web_setup_state"} for row in tables
            ):
                raise ForestError(
                    "DATABASE_SCOPE_MISMATCH", "Deployment database cannot be a Bot authority"
                )
            identities = [row for row in tables if row["name"] == "forest_bot_identity"]
            if identities:
                if len(identities) != 1 or identities[0]["schema"] != "public":
                    raise ForestError(
                        "DATABASE_SCOPE_MISMATCH", "Bot identity must occupy its dedicated database"
                    )
                await self._identity(conn)
            elif tables:
                raise ForestError(
                    "DATABASE_NOT_EMPTY", "A new Bot requires an empty dedicated database"
                )
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS public.forest_bot_schema_migrations "
                "(version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
            )
            applied_rows = await (
                await conn.execute(
                    "SELECT version,checksum FROM public.forest_bot_schema_migrations"
                )
            ).fetchall()
            applied = {row["version"]: row["checksum"] for row in applied_rows}
            known = {path.name for path in migrations if path.name.endswith(".sql")}
            if set(applied) - known:
                raise ForestError("SCHEMA_TOO_NEW", "Bot database requires a newer Forest version")
            for path in migrations:
                if not path.name.endswith(".sql"):
                    continue
                source = path.read_text(encoding="utf-8")
                checksum = hashlib.sha256(source.encode()).hexdigest()
                if path.name in applied:
                    if applied[path.name] != checksum:
                        raise ForestError("MIGRATION_CHANGED", "Bot migration checksum changed")
                    continue
                await conn.execute(source)
                await conn.execute(
                    "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                    (path.name, checksum),
                )
            if not identities:
                await conn.execute(
                    "INSERT INTO public.forest_bot_identity(bot_id) VALUES (%s)", (self.bot_id,)
                )
                await conn.execute(
                    "INSERT INTO public.bot_runtime_state(bot_id) VALUES (%s)", (self.bot_id,)
                )
                await conn.execute(
                    "INSERT INTO public.bot_host_lease(bot_id) VALUES (%s)", (self.bot_id,)
                )

    async def _identity(self, conn: AsyncConnection) -> None:
        row = await (
            await conn.execute("SELECT bot_id FROM public.forest_bot_identity WHERE singleton")
        ).fetchone()
        if row is None or row["bot_id"] != self.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Repository is bound to another Bot authority")

    def _scope(self, value: HostLease | CallGrant) -> None:
        if value.bot_id != self.bot_id:
            raise ForestError(
                "BOT_SCOPE_MISMATCH", "Execution authority belongs to another Bot", status=403
            )

    @staticmethod
    def _ttl(seconds: float) -> None:
        if not 0 < seconds <= 3600:
            raise ValueError("Lease/deadline must be greater than zero and at most one hour")

    async def _runtime(self, conn: AsyncConnection) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s FOR UPDATE",
                (self.bot_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("BOT_SCOPE_MISMATCH", "Repository is bound to another Bot authority")
        return row

    async def runtime(self) -> dict:
        async with self.pool.connection() as conn:
            await self._identity(conn)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_runtime_state WHERE bot_id=%s",
                    (self.bot_id,),
                )
            ).fetchone()
            assert row is not None
            return row

    async def _host(
        self, conn: AsyncConnection, lease: HostLease, runtime: dict | None = None
    ) -> dict:
        self._scope(lease)
        runtime = runtime or await self._runtime(conn)
        if runtime["status"] != "ACTIVE":
            raise ForestError("STORAGE_SEALED", "Bot storage is sealed")
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_host_lease WHERE bot_id=%s FOR UPDATE",
                (self.bot_id,),
            )
        ).fetchone()
        if (
            row is None
            or row["expires_at"] <= await self._now(conn)
            or row["host_id"] != lease.host_id
            or row["incarnation"] != lease.incarnation
            or row["owner_epoch"] != lease.owner_epoch
        ):
            raise ForestError("STALE_HOST", "Host ownership expired or was replaced")
        return row

    @staticmethod
    async def _now(conn: AsyncConnection) -> datetime:
        row = await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone()
        assert row is not None
        return row["now"]

    @staticmethod
    def _host_value(row: dict) -> HostLease:
        return HostLease(**{key: row[key] for key in HostLease.__dataclass_fields__})

    async def acquire_host(
        self, host_id: str, incarnation: str, ttl_seconds: float = 30
    ) -> HostLease:
        self._ttl(ttl_seconds)
        if not host_id or not incarnation:
            raise ValueError("Host identity and incarnation are required")
        async with self.pool.connection() as conn, conn.transaction():
            runtime = await self._runtime(conn)
            if runtime["status"] != "ACTIVE":
                raise ForestError("STORAGE_SEALED", "Bot storage is sealed")
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_host_lease WHERE bot_id=%s FOR UPDATE",
                    (self.bot_id,),
                )
            ).fetchone()
            assert row is not None
            if row["expires_at"] > await self._now(conn):
                if row["host_id"] == host_id and row["incarnation"] == incarnation:
                    return self._host_value(row)
                raise ForestError("HOST_BUSY", "Another host owns this Bot")
            used = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_host_incarnations WHERE bot_id=%s AND incarnation=%s",
                    (self.bot_id, incarnation),
                )
            ).fetchone()
            if used:
                raise ForestError(
                    "INCARNATION_REUSED", "Expired host incarnation cannot be revived"
                )
            epoch = row["owner_epoch"] + 1
            row = await (
                await conn.execute(
                    "UPDATE public.bot_host_lease SET host_id=%s,incarnation=%s,owner_epoch=%s,"
                    "expires_at=clock_timestamp()+%s*interval '1 second' WHERE bot_id=%s RETURNING *",
                    (host_id, incarnation, epoch, ttl_seconds, self.bot_id),
                )
            ).fetchone()
            await conn.execute(
                "INSERT INTO public.bot_host_incarnations(bot_id,incarnation,host_id,owner_epoch) VALUES (%s,%s,%s,%s)",
                (self.bot_id, incarnation, host_id, epoch),
            )
            await self._event(
                conn,
                "host.acquired",
                None,
                {"host_id": host_id, "incarnation": incarnation, "owner_epoch": epoch},
            )
            assert row is not None
            return self._host_value(row)

    async def renew_host(self, lease: HostLease, ttl_seconds: float = 30) -> HostLease:
        self._ttl(ttl_seconds)
        async with self.pool.connection() as conn, conn.transaction():
            await self._host(conn, lease)
            row = await (
                await conn.execute(
                    "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+%s*interval '1 second' "
                    "WHERE bot_id=%s AND expires_at>clock_timestamp() RETURNING *",
                    (ttl_seconds, self.bot_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("STALE_HOST", "Expired host lease cannot be renewed")
            return self._host_value(row)

    async def release_host(self, lease: HostLease) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            await self._host(conn, lease)
            await conn.execute(
                "UPDATE public.bot_host_lease SET expires_at=clock_timestamp() WHERE bot_id=%s",
                (self.bot_id,),
            )
            await self._event(conn, "host.released", None, {"owner_epoch": lease.owner_epoch})

    async def set_maintenance(
        self, lease: HostLease, expected_epoch: int, data_admission: bool
    ) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            # Resume may need to reopen a candidate published while the Bot was
            # paused. Acquire routes before Bot/host locks, in stable key order.
            await (
                await conn.execute(
                    "SELECT component_id FROM public.bot_component_routes WHERE bot_id=%s ORDER BY component_id FOR UPDATE",
                    (self.bot_id,),
                )
            ).fetchall()
            runtime = await self._runtime(conn)
            await self._host(conn, lease, runtime)
            if runtime["maintenance_epoch"] != expected_epoch:
                raise ForestError("MAINTENANCE_CONFLICT", "Maintenance intent changed")
            row = await (
                await conn.execute(
                    "UPDATE public.bot_runtime_state SET maintenance_epoch=maintenance_epoch+1,data_admission=%s "
                    "WHERE bot_id=%s RETURNING *",
                    (data_admission, self.bot_id),
                )
            ).fetchone()
            if data_admission:
                await conn.execute(
                    "UPDATE public.bot_component_routes r SET admission=true FROM public.bot_generations g "
                    "WHERE r.bot_id=%s AND g.bot_id=r.bot_id AND g.generation_id=r.generation_id "
                    "AND g.status='ACTIVE' AND g.host_incarnation=%s AND g.host_owner_epoch=%s",
                    (self.bot_id, lease.incarnation, lease.owner_epoch),
                )
            await self._event(
                conn,
                "bot.maintenance",
                None,
                {"epoch": expected_epoch + 1, "data_admission": data_admission},
            )
            assert row is not None
            return row

    async def revoke_policy(self, lease: HostLease, expected_revision: int) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            runtime = await self._runtime(conn)
            await self._host(conn, lease, runtime)
            if runtime["policy_revision"] != expected_revision:
                raise ForestError("POLICY_CONFLICT", "Policy revision changed")
            row = await (
                await conn.execute(
                    "UPDATE public.bot_runtime_state SET policy_revision=policy_revision+1 WHERE bot_id=%s RETURNING *",
                    (self.bot_id,),
                )
            ).fetchone()
            await self._event(conn, "bot.policy_revoked", None, {"revision": expected_revision + 1})
            assert row is not None
            return row

    async def _route(self, conn: AsyncConnection, component_id: str) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_component_routes WHERE bot_id=%s AND component_id=%s FOR UPDATE",
                (self.bot_id, component_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("ROUTE_NOT_FOUND", "Component route does not exist", status=404)
        return row

    async def _routes(self, conn: AsyncConnection, component_ids) -> dict[str, dict]:
        """Acquire the complete immutable routing fence set before Bot/call locks."""
        return {
            component: await self._route(conn, component)
            for component in sorted(set(component_ids))
        }

    async def _all_routes(self, conn: AsyncConnection) -> None:
        await (
            await conn.execute(
                "SELECT component_id FROM public.bot_component_routes WHERE bot_id=%s "
                "ORDER BY component_id FOR UPDATE",
                (self.bot_id,),
            )
        ).fetchall()

    @staticmethod
    def _dependencies(component_id: str, dependencies) -> tuple[str, ...]:
        if (
            not isinstance(dependencies, (tuple, list))
            or len(dependencies) > 64
            or any(
                not isinstance(item, str) or not item or len(item) > 200 for item in dependencies
            )
        ):
            raise ForestError(
                "INVALID_DEPENDENCIES",
                "Dependency reservations must be bounded component IDs",
                status=400,
            )
        if component_id in dependencies:
            raise ForestError(
                "CALL_CYCLE", "A component cannot reserve itself as a synchronous dependency"
            )
        return tuple(sorted(set(dependencies)))

    @staticmethod
    def _grant_value(row: dict) -> CallGrant:
        value = {key: row[key] for key in CallGrant.__dataclass_fields__}
        value["dependency_components"] = tuple(value["dependency_components"])
        return CallGrant(**value)

    async def get_route(self, component_id: str) -> dict | None:
        async with self.pool.connection() as conn:
            await self._identity(conn)
            return await (
                await conn.execute(
                    "SELECT r.*,g.status AS generation_status,g.worker_incarnation FROM public.bot_component_routes r "
                    "LEFT JOIN public.bot_generations g ON g.bot_id=r.bot_id AND g.generation_id=r.generation_id "
                    "WHERE r.bot_id=%s AND r.component_id=%s",
                    (self.bot_id, component_id),
                )
            ).fetchone()

    async def _resource(self, conn: AsyncConnection, component_id: str) -> dict:
        row = await (
            await conn.execute(
                "SELECT *,lease_until>clock_timestamp() AS live FROM public.bot_component_resources "
                "WHERE bot_id=%s AND component_id=%s FOR UPDATE",
                (self.bot_id, component_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "COMPONENT_NOT_FOUND", "Component must be registered first", status=404
            )
        return row

    async def register_generation(
        self,
        lease: HostLease,
        component_id: str,
        generation_id: str,
        package_digest: str,
        config_digest: str,
        worker_incarnation: str,
        *,
        config: dict | None = None,
        instance_id: str | None = None,
        capability_grants: tuple[str, ...] | list[str] = (),
        state_schema: int = 1,
        state_value_schema: dict | None = None,
        state_migrations: list[dict] | None = None,
    ) -> dict:
        self._scope(lease)
        config = config if config is not None else {}
        instance_id = instance_id or generation_id
        if (
            not isinstance(capability_grants, (list, tuple))
            or len(capability_grants) > 64
            or any(
                not isinstance(item, str) or not item or len(item) > 200
                for item in capability_grants
            )
        ):
            raise ForestError(
                "INVALID_GRANTS", "Generation capability grants must be bounded names", status=400
            )
        grants = sorted(set(capability_grants))
        if type(state_schema) is not int or state_schema < 1:
            raise ForestError("STATE_SCHEMA_INVALID", "State schema must be a positive integer", 422)
        state_value_schema = json.loads(json.dumps(state_value_schema or {}, allow_nan=False))
        state_migrations = json.loads(json.dumps(state_migrations or [], allow_nan=False))
        if not isinstance(state_migrations, list) or len(state_migrations) > 64 or any(
            not isinstance(item, dict) or set(item) != {"from_schema", "to_schema"}
            or type(item["from_schema"]) is not int or item["from_schema"] < 1
            or type(item["to_schema"]) is not int or item["to_schema"] != state_schema
            or item["from_schema"] == state_schema for item in state_migrations
        ):
            raise ForestError("STATE_MIGRATION_INVALID", "Invalid generation migration contract", 422)
        Draft202012Validator.check_schema(state_value_schema)
        async with self.pool.connection() as conn, conn.transaction():
            await self._identity(conn)
            await conn.execute(
                "INSERT INTO public.bot_component_resources(bot_id,component_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.bot_id, component_id),
            )
            await self._resource(conn, component_id)
            await conn.execute(
                "INSERT INTO public.bot_component_routes(bot_id,component_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.bot_id, component_id),
            )
            await self._route(conn, component_id)
            await self._host(conn, lease)
            await conn.execute(
                "INSERT INTO public.bot_component_state_heads(bot_id,component_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.bot_id, component_id),
            )
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_generations WHERE bot_id=%s AND generation_id=%s",
                    (self.bot_id, generation_id),
                )
            ).fetchone()
            fixed = {
                "component_id": component_id,
                "package_digest": package_digest,
                "config_digest": config_digest,
                "config": config,
                "instance_id": instance_id,
                "worker_incarnation": worker_incarnation,
                "host_incarnation": lease.incarnation,
                "host_owner_epoch": lease.owner_epoch,
                "capability_grants": grants,
                "state_schema": state_schema,
                "state_value_schema": state_value_schema,
                "state_migrations": state_migrations,
            }
            if existing:
                if any(existing[key] != value for key, value in fixed.items()):
                    raise ForestError("GENERATION_CONFLICT", "Generation identity cannot be reused")
                return existing
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_generations(bot_id,generation_id,component_id,package_digest,config_digest,"
                    "config,instance_id,worker_incarnation,host_incarnation,host_owner_epoch,capability_grants,state_schema,state_value_schema,state_migrations,status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PREPARING') RETURNING *",
                    (
                        self.bot_id,
                        generation_id,
                        component_id,
                        package_digest,
                        config_digest,
                        Jsonb(config),
                        instance_id,
                        worker_incarnation,
                        lease.incarnation,
                        lease.owner_epoch,
                        Jsonb(grants),
                        state_schema,
                        Jsonb(state_value_schema),
                        Jsonb(state_migrations),
                    ),
                )
            ).fetchone()
            await self._event(
                conn,
                "generation.registered",
                None,
                {"generation_id": generation_id, "component_id": component_id},
            )
            assert row is not None
            return row

    async def _generation(self, conn: AsyncConnection, generation_id: str) -> dict:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_generations WHERE bot_id=%s AND generation_id=%s",
                (self.bot_id, generation_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("GENERATION_NOT_FOUND", "Generation does not exist", status=404)
        return row

    async def mark_ready(self, lease: HostLease, generation_id: str) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            generation = await self._generation(conn, generation_id)
            await self._route(conn, generation["component_id"])
            await self._host(conn, lease)
            generation = await self._generation(conn, generation_id)
            if (
                generation["host_incarnation"] != lease.incarnation
                or generation["host_owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError("STALE_GENERATION", "Candidate belongs to an old host")
            if generation["status"] not in {"PREPARING", "READY"}:
                raise ForestError(
                    "INVALID_GENERATION_PHASE", "Only a preparing candidate can become ready"
                )
            row = await (
                await conn.execute(
                    "UPDATE public.bot_generations SET status='READY' WHERE bot_id=%s AND generation_id=%s RETURNING *",
                    (self.bot_id, generation_id),
                )
            ).fetchone()
            assert row is not None
            return row

    async def snapshot_state(self, lease: HostLease, component_id: str) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            await self._route(conn, component_id)
            await self._host(conn, lease)
            rows = await (
                await conn.execute(
                    "SELECT key,revision,value FROM public.bot_component_state WHERE bot_id=%s AND component_id=%s AND NOT deleted ORDER BY key",
                    (self.bot_id, component_id),
                )
            ).fetchall()
            result = {
                row["key"]: {"revision": row["revision"], "value": row["value"]} for row in rows
            }
            await self._host(conn, lease)
            return result

    async def state_schema(self, lease: HostLease, component_id: str) -> int:
        async with self.pool.connection() as conn, conn.transaction():
            await self._route(conn, component_id)
            await self._host(conn, lease)
            row = await (await conn.execute(
                "SELECT state_schema FROM public.bot_component_state_heads WHERE bot_id=%s AND component_id=%s",
                (self.bot_id, component_id),
            )).fetchone()
            await self._host(conn, lease)
            return row["state_schema"] if row else 1

    async def _state_source(self, conn: AsyncConnection, component_id: str) -> tuple[dict, dict]:
        head = await (await conn.execute(
            "SELECT * FROM public.bot_component_state_heads WHERE bot_id=%s AND component_id=%s FOR UPDATE",
            (self.bot_id, component_id),
        )).fetchone()
        if head is None:
            raise ForestError("STATE_HEAD_MISSING", "Component has no durable state authority", 409)
        rows = await (await conn.execute(
            "SELECT key,revision,value,deleted FROM public.bot_component_state WHERE bot_id=%s AND component_id=%s ORDER BY key",
            (self.bot_id, component_id),
        )).fetchall()
        return head, {row["key"]: {key: value for key, value in row.items() if key != "key"} for row in rows}

    @staticmethod
    def _state_values(rows: dict) -> dict:
        return {key: row["value"] for key, row in rows.items() if not row["deleted"]}

    @staticmethod
    def _validate_state_values(values: dict, schema: dict) -> dict:
        try:
            encoded = json.dumps(values, allow_nan=False, ensure_ascii=False)
            encoded_size = len(encoded.encode())
            result = json.loads(encoded)
        except (ValueError, TypeError, UnicodeError):
            raise ForestError("STATE_MIGRATION_INVALID", "State must contain finite JSON values", 422) from None
        if (
            not isinstance(result, dict) or len(result) > 512 or encoded_size > 120_000
            or any(not isinstance(key, str) or not 1 <= len(key) <= 200 for key in result)
            or not Draft202012Validator(schema).is_valid(result)
        ):
            raise ForestError("STATE_MIGRATION_INVALID", "State violates its bounded durable contract", 422)
        return result

    async def capture_state_migration(self, lease: HostLease, operation_id: str, op_epoch: int) -> dict:
        """Freeze an immutable final source after all admitted writes drained."""
        async with self.pool.connection() as conn, conn.transaction():
            operation, route, runtime = await self._lifecycle(conn, lease, operation_id, op_epoch)
            operation = await self._quiesce_locked(conn, lease, operation, route, runtime)
            previous = await (await conn.execute(
                "SELECT * FROM public.bot_state_migration_sources WHERE bot_id=%s AND operation_id=%s",
                (self.bot_id, operation_id),
            )).fetchone()
            if previous:
                await self._host(conn, lease, runtime)
                if operation["lease_until"] <= await self._now(conn):
                    raise ForestError("STALE_OPERATION", "Lifecycle lease expired during state capture replay")
                return previous
            candidate = await self._generation(conn, operation["candidate_generation_id"])
            head, rows = await self._state_source(conn, operation["component_id"])
            self._validate_state_values(self._state_values(rows), {})
            source = await (await conn.execute(
                "INSERT INTO public.bot_state_migration_sources(bot_id,operation_id,component_id,"
                "source_generation_id,source_route_epoch,source_schema,source_revision,source_digest,source_rows,target_schema) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (self.bot_id, operation_id, operation["component_id"], route["generation_id"],
                 route["route_epoch"], head["state_schema"], head["revision"], _digest(rows), Jsonb(rows), candidate["state_schema"]),
            )).fetchone()
            await self._host(conn, lease, runtime)
            if operation["lease_until"] <= await self._now(conn):
                raise ForestError("STALE_OPERATION", "Lifecycle lease expired during state capture")
            return source

    async def stage_state_migration(
        self, lease: HostLease, operation_id: str, op_epoch: int, *, source_digest: str,
        first_values: dict, repeated_values: dict,
    ) -> dict:
        # Freeze both independent worker results before waiting on authority locks.
        first_values = self._validate_state_values(first_values, {})
        repeated_values = self._validate_state_values(repeated_values, {})
        if _digest(first_values) != _digest(repeated_values):
            raise ForestError("STATE_MIGRATION_NONDETERMINISTIC", "Repeated migration changed its result", 422)
        async with self.pool.connection() as conn, conn.transaction():
            operation, _, runtime = await self._lifecycle(conn, lease, operation_id, op_epoch)
            if operation["phase"] != "QUIESCENT":
                raise ForestError("INVALID_OPERATION_PHASE", "State migration requires final quiescence")
            source = await (await conn.execute(
                "SELECT * FROM public.bot_state_migration_sources WHERE bot_id=%s AND operation_id=%s",
                (self.bot_id, operation_id),
            )).fetchone()
            if source is None or source["source_digest"] != source_digest:
                raise ForestError("STATE_SOURCE_CONFLICT", "Migration source receipt differs")
            candidate = await self._generation(conn, operation["candidate_generation_id"])
            self._validate_state_values(first_values, candidate["state_value_schema"])
            if source["source_generation_id"] is not None and source["source_schema"] != candidate["state_schema"] and {
                "from_schema": source["source_schema"], "to_schema": candidate["state_schema"]
            } not in candidate["state_migrations"]:
                raise ForestError("STATE_MIGRATION_MISSING", "Generation did not declare this source schema", 422)
            if source["source_schema"] == source["target_schema"] and first_values != self._state_values(source["source_rows"]):
                raise ForestError("STATE_MIGRATION_INVALID", "Same-version reload must preserve durable values", 422)
            previous = await (await conn.execute(
                "SELECT * FROM public.bot_state_migration_results WHERE bot_id=%s AND operation_id=%s",
                (self.bot_id, operation_id),
            )).fetchone()
            if previous:
                if previous["result_digest"] != _digest(first_values):
                    raise ForestError("IDEMPOTENCY_CONFLICT", "Migration result is already immutable")
                await self._host(conn, lease, runtime)
                if operation["lease_until"] <= await self._now(conn):
                    raise ForestError("STALE_OPERATION", "Lifecycle lease expired during migration replay")
                return previous
            result = await (await conn.execute(
                "INSERT INTO public.bot_state_migration_results(bot_id,operation_id,source_digest,result_digest,result_values) "
                "VALUES (%s,%s,%s,%s,%s) RETURNING *",
                (self.bot_id, operation_id, source_digest, _digest(first_values), Jsonb(first_values)),
            )).fetchone()
            await self._host(conn, lease, runtime)
            if operation["lease_until"] <= await self._now(conn):
                raise ForestError("STALE_OPERATION", "Lifecycle lease expired during migration staging")
            return result

    async def _publish_state(self, conn: AsyncConnection, operation: dict, route: dict, candidate: dict) -> None:
        source = await (await conn.execute(
            "SELECT * FROM public.bot_state_migration_sources WHERE bot_id=%s AND operation_id=%s",
            (self.bot_id, operation["operation_id"]),
        )).fetchone()
        head, rows = await self._state_source(conn, operation["component_id"])
        if source is None:
            # Legacy trusted callers can publish only an unchanged state contract.
            if head["state_schema"] != candidate["state_schema"]:
                raise ForestError("STATE_MIGRATION_REQUIRED", "Schema transition has no staged state receipt")
            self._validate_state_values(self._state_values(rows), candidate["state_value_schema"])
            return
        if (source["source_revision"] != head["revision"] or source["source_schema"] != head["state_schema"]
            or source["source_digest"] != _digest(rows) or source["source_generation_id"] != route["generation_id"]
            or source["source_route_epoch"] != route["route_epoch"]):
            raise ForestError("STATE_SOURCE_CONFLICT", "Durable state changed after final capture")
        staged = await (await conn.execute(
            "SELECT * FROM public.bot_state_migration_results WHERE bot_id=%s AND operation_id=%s",
            (self.bot_id, operation["operation_id"]),
        )).fetchone()
        if staged is None or staged["source_digest"] != source["source_digest"]:
            raise ForestError("STATE_MIGRATION_REQUIRED", "Candidate has no verified shadow state")
        values = self._validate_state_values(staged["result_values"], candidate["state_value_schema"])
        if source["target_schema"] == head["state_schema"]:
            return
        for key in sorted(set(rows) | set(values)):
            previous = rows.get(key, {"revision": 0})["revision"]
            deleted, value = key not in values, values.get(key)
            await conn.execute(
                "INSERT INTO public.bot_state_revisions(bot_id,component_id,key,revision,previous_revision,value,operation_id,migration_operation_id,deleted) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (self.bot_id, operation["component_id"], key, previous+1, previous, Jsonb(value),
                 operation["operation_id"], operation["operation_id"], deleted),
            )
            await conn.execute(
                "INSERT INTO public.bot_component_state(bot_id,component_id,key,revision,value,deleted) VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (bot_id,component_id,key) DO UPDATE SET revision=EXCLUDED.revision,value=EXCLUDED.value,deleted=EXCLUDED.deleted",
                (self.bot_id, operation["component_id"], key, previous+1, Jsonb(value), deleted),
            )
        await conn.execute(
            "UPDATE public.bot_component_state_heads SET state_schema=%s,revision=revision+1,migration_operation_id=%s "
            "WHERE bot_id=%s AND component_id=%s",
            (source["target_schema"], operation["operation_id"], self.bot_id, operation["component_id"]),
        )
        await self._event(conn, "state.migrated", operation["operation_id"], {
            "component_id": operation["component_id"], "source_schema": source["source_schema"],
            "target_schema": source["target_schema"], "source_revision": source["source_revision"],
            "source_digest": source["source_digest"], "result_digest": staged["result_digest"],
        })

    async def begin_operation(
        self,
        lease: HostLease,
        component_id: str,
        operation_id: str,
        input_digest: str,
        candidate_generation_id: str,
        expected_route_epoch: int,
        ttl_seconds: float = 30,
    ) -> dict:
        self._ttl(ttl_seconds)
        async with self.pool.connection() as conn, conn.transaction():
            resource = await self._resource(conn, component_id)
            route = await self._route(conn, component_id)
            await self._host(conn, lease)
            candidate = await self._generation(conn, candidate_generation_id)
            if candidate["component_id"] != component_id:
                raise ForestError("GENERATION_CONFLICT", "Candidate belongs to another component")
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_lifecycle_operations WHERE bot_id=%s AND operation_id=%s",
                    (self.bot_id, operation_id),
                )
            ).fetchone()
            if existing:
                fixed = {
                    "component_id": component_id,
                    "input_digest": input_digest,
                    "candidate_generation_id": candidate_generation_id,
                    "expected_route_epoch": expected_route_epoch,
                }
                if any(existing[key] != value for key, value in fixed.items()):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Lifecycle operation already has another plan"
                    )
                if existing["phase"] in {"COMMITTED", "FAILED", "ABANDONED"}:
                    return existing
            now = await self._now(conn)
            if resource["operation_id"] and resource["lease_until"] > now:
                if (
                    existing
                    and resource["operation_id"] == operation_id
                    and existing["host_incarnation"] == lease.incarnation
                    and existing["host_owner_epoch"] == lease.owner_epoch
                ):
                    return existing
                holder = await (await conn.execute(
                    "SELECT host_incarnation,host_owner_epoch FROM public.bot_lifecycle_operations WHERE bot_id=%s AND operation_id=%s",
                    (self.bot_id, resource["operation_id"]),
                )).fetchone()
                if holder and holder["host_incarnation"] == lease.incarnation and holder["host_owner_epoch"] == lease.owner_epoch:
                    raise ForestError(
                        "RESOURCE_BUSY", "Another lifecycle operation owns this component"
                    )
                # A new authoritative host may abandon an orphan operation now;
                # its predecessor can never regain publication authority even if
                # the longer component operation lease has not elapsed yet.
            if route["route_epoch"] != expected_route_epoch:
                raise ForestError("ROUTE_CONFLICT", "Component route changed")
            if (
                candidate["host_incarnation"] != lease.incarnation
                or candidate["host_owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError("STALE_GENERATION", "Candidate belongs to an old host")
            if resource["operation_id"] and resource["operation_id"] != operation_id:
                await conn.execute(
                    "UPDATE public.bot_lifecycle_operations SET phase='ABANDONED',error_code='LEASE_EXPIRED' "
                    "WHERE bot_id=%s AND operation_id=%s AND phase IN ('PLAN','FROZEN','QUIESCENT')",
                    (self.bot_id, resource["operation_id"]),
                )
            epoch = resource["op_epoch"] + 1
            if existing:
                row = await (
                    await conn.execute(
                        "UPDATE public.bot_lifecycle_operations SET host_incarnation=%s,host_owner_epoch=%s,"
                        "op_epoch=%s,lease_until=clock_timestamp()+%s*interval '1 second' "
                        "WHERE bot_id=%s AND operation_id=%s RETURNING *",
                        (
                            lease.incarnation,
                            lease.owner_epoch,
                            epoch,
                            ttl_seconds,
                            self.bot_id,
                            operation_id,
                        ),
                    )
                ).fetchone()
            else:
                row = await (
                    await conn.execute(
                        "INSERT INTO public.bot_lifecycle_operations(bot_id,operation_id,component_id,input_digest,"
                        "candidate_generation_id,old_generation_id,expected_route_epoch,phase,host_incarnation,"
                        "host_owner_epoch,op_epoch,lease_until) VALUES (%s,%s,%s,%s,%s,%s,%s,'PLAN',%s,%s,%s,"
                        "clock_timestamp()+%s*interval '1 second') RETURNING *",
                        (
                            self.bot_id,
                            operation_id,
                            component_id,
                            input_digest,
                            candidate_generation_id,
                            route["generation_id"],
                            expected_route_epoch,
                            lease.incarnation,
                            lease.owner_epoch,
                            epoch,
                            ttl_seconds,
                        ),
                    )
                ).fetchone()
            assert row is not None
            await conn.execute(
                "UPDATE public.bot_component_resources SET operation_id=%s,op_epoch=%s,lease_until=%s "
                "WHERE bot_id=%s AND component_id=%s",
                (operation_id, epoch, row["lease_until"], self.bot_id, component_id),
            )
            await self._event(
                conn,
                "lifecycle.planned",
                operation_id,
                {"component_id": component_id, "op_epoch": epoch},
            )
            return row

    async def read_operation(self, operation_id: str) -> dict | None:
        async with self.pool.connection() as conn:
            await self._identity(conn)
            return await (
                await conn.execute(
                    "SELECT * FROM public.bot_lifecycle_operations WHERE bot_id=%s AND operation_id=%s",
                    (self.bot_id, operation_id),
                )
            ).fetchone()

    async def _lifecycle(
        self, conn: AsyncConnection, lease: HostLease, operation_id: str, op_epoch: int,
        *, terminal_cleanup: bool = False,
    ) -> tuple[dict, dict, dict]:
        operation = await (
            await conn.execute(
                "SELECT * FROM public.bot_lifecycle_operations WHERE bot_id=%s AND operation_id=%s",
                (self.bot_id, operation_id),
            )
        ).fetchone()
        if operation is None:
            raise ForestError(
                "OPERATION_NOT_FOUND", "Lifecycle operation does not exist", status=404
            )
        resource = await self._resource(conn, operation["component_id"])
        route = await self._route(conn, operation["component_id"])
        runtime = await self._runtime(conn)
        await self._host(conn, lease, runtime)
        operation = await (
            await conn.execute(
                "SELECT * FROM public.bot_lifecycle_operations WHERE bot_id=%s AND operation_id=%s",
                (self.bot_id, operation_id),
            )
        ).fetchone()
        assert operation is not None
        if (
            operation["op_epoch"] != op_epoch
            or operation["host_incarnation"] != lease.incarnation
            or operation["host_owner_epoch"] != lease.owner_epoch
        ):
            raise ForestError("STALE_OPERATION", "Lifecycle coordinator lost ownership")
        if operation["phase"] not in {"COMMITTED", "FAILED", "ABANDONED"}:
            now = await self._now(conn)
            if (
                resource["operation_id"] != operation_id
                or resource["op_epoch"] != op_epoch
                or (not terminal_cleanup and resource["lease_until"] <= now)
                or (not terminal_cleanup and operation["lease_until"] <= now)
            ):
                raise ForestError("STALE_OPERATION", "Lifecycle lease expired or was replaced")
        return operation, route, runtime

    async def renew_operation(
        self, lease: HostLease, operation_id: str, op_epoch: int, ttl_seconds: float = 30
    ) -> dict:
        self._ttl(ttl_seconds)
        async with self.pool.connection() as conn, conn.transaction():
            operation, _, _ = await self._lifecycle(conn, lease, operation_id, op_epoch)
            if operation["phase"] not in {"PLAN", "FROZEN", "QUIESCENT"}:
                raise ForestError(
                    "INVALID_OPERATION_PHASE", "Terminal operation has no renewable lease"
                )
            row = await (
                await conn.execute(
                    "UPDATE public.bot_lifecycle_operations SET lease_until=clock_timestamp()+%s*interval '1 second' "
                    "WHERE bot_id=%s AND operation_id=%s AND lease_until>clock_timestamp() RETURNING *",
                    (ttl_seconds, self.bot_id, operation_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("STALE_OPERATION", "Expired operation lease cannot be renewed")
            await conn.execute(
                "UPDATE public.bot_component_resources SET lease_until=%s WHERE bot_id=%s AND component_id=%s",
                (row["lease_until"], self.bot_id, operation["component_id"]),
            )
            return row

    async def freeze(self, lease: HostLease, operation_id: str, op_epoch: int) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            operation, route, _ = await self._lifecycle(conn, lease, operation_id, op_epoch)
            if operation["phase"] in {"FROZEN", "QUIESCENT"}:
                return operation
            if operation["phase"] != "PLAN":
                raise ForestError(
                    "INVALID_OPERATION_PHASE", "Only a planned operation can freeze admission"
                )
            if route["route_epoch"] != operation["expected_route_epoch"]:
                raise ForestError("ROUTE_CONFLICT", "Component route changed before freeze")
            await conn.execute(
                "UPDATE public.bot_component_routes SET admission=false,control_admission=false WHERE bot_id=%s AND component_id=%s",
                (self.bot_id, operation["component_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_generations SET status='QUIESCING' WHERE bot_id=%s AND generation_id=%s AND status='ACTIVE'",
                (self.bot_id, route["generation_id"]),
            )
            row = await (
                await conn.execute(
                    "UPDATE public.bot_lifecycle_operations SET phase='FROZEN' WHERE bot_id=%s AND operation_id=%s RETURNING *",
                    (self.bot_id, operation_id),
                )
            ).fetchone()
            await self._event(
                conn, "lifecycle.frozen", operation_id, {"component_id": operation["component_id"]}
            )
            assert row is not None
            return row

    async def _reap(
        self, conn: AsyncConnection, component_id: str, route: dict, runtime: dict, lease: HostLease
    ) -> None:
        await conn.execute(
            "UPDATE public.bot_calls c SET status='FAILED',error_code='CALL_FENCED' WHERE c.bot_id=%s "
            "AND (c.component_id=%s OR %s=ANY(c.dependency_components)) AND c.status='ADMITTED' "
            "AND NOT EXISTS (SELECT 1 FROM public.bot_live_calls l WHERE l.bot_id=c.bot_id AND l.call_id=c.call_id)",
            (self.bot_id, component_id, component_id),
        )

    async def inflight(self, component_id: str) -> int:
        async with self.pool.connection() as conn:
            await self._identity(conn)
            row = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_live_calls WHERE bot_id=%s "
                    "AND (component_id=%s OR %s=ANY(dependency_components))",
                    (self.bot_id, component_id, component_id),
                )
            ).fetchone()
            assert row is not None
            return row["n"]

    async def _quiesce_locked(
        self, conn: AsyncConnection, lease: HostLease, operation: dict, route: dict, runtime: dict
    ) -> dict:
        if operation["phase"] == "QUIESCENT":
            return operation
        if operation["phase"] != "FROZEN":
            raise ForestError("INVALID_OPERATION_PHASE", "Quiescence requires frozen admission")
        if route["route_epoch"] != operation["expected_route_epoch"]:
            raise ForestError("ROUTE_CONFLICT", "Component route changed before quiescence")
        await self._reap(conn, operation["component_id"], route, runtime, lease)
        pending = await (
            await conn.execute(
                "SELECT count(*) AS n FROM public.bot_live_calls WHERE bot_id=%s "
                "AND (component_id=%s OR %s=ANY(dependency_components))",
                (self.bot_id, operation["component_id"], operation["component_id"]),
            )
        ).fetchone()
        if pending and pending["n"]:
            raise ForestError(
                "DRAIN_PENDING", "Admitted calls or reserved dependency callers have not finished"
            )
        await self._host(conn, lease, runtime)
        if operation["lease_until"] <= await self._now(conn):
            raise ForestError("STALE_OPERATION", "Lifecycle lease expired before quiescence")
        await conn.execute(
            "UPDATE public.bot_generations SET status='QUIESCENT' WHERE bot_id=%s AND generation_id=%s "
            "AND status='QUIESCING'",
            (self.bot_id, route["generation_id"]),
        )
        row = await (
            await conn.execute(
                "UPDATE public.bot_lifecycle_operations SET phase='QUIESCENT' WHERE bot_id=%s AND operation_id=%s RETURNING *",
                (self.bot_id, operation["operation_id"]),
            )
        ).fetchone()
        await self._event(
            conn,
            "lifecycle.quiescent",
            operation["operation_id"],
            {"component_id": operation["component_id"]},
        )
        assert row is not None
        return row

    async def quiesce_operation(self, lease: HostLease, operation_id: str, op_epoch: int) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            operation, route, runtime = await self._lifecycle(conn, lease, operation_id, op_epoch)
            return await self._quiesce_locked(conn, lease, operation, route, runtime)

    async def publish(self, lease: HostLease, operation_id: str, op_epoch: int) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            operation, route, runtime = await self._lifecycle(conn, lease, operation_id, op_epoch)
            if operation["phase"] == "COMMITTED":
                if route["generation_id"] != operation["candidate_generation_id"]:
                    raise ForestError(
                        "ROUTE_CONFLICT", "The committed route has subsequently changed"
                    )
                return route
            if operation["phase"] not in {"FROZEN", "QUIESCENT"}:
                raise ForestError(
                    "INVALID_OPERATION_PHASE", "Publication requires frozen admission"
                )
            if route["route_epoch"] != operation["expected_route_epoch"]:
                raise ForestError("ROUTE_CONFLICT", "Component route changed before publication")
            candidate = await self._generation(conn, operation["candidate_generation_id"])
            if (
                candidate["status"] != "READY"
                or candidate["host_incarnation"] != lease.incarnation
                or candidate["host_owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError(
                    "CANDIDATE_NOT_READY", "Candidate has no current ready execution instance"
                )
            operation = await self._quiesce_locked(conn, lease, operation, route, runtime)
            # Recheck time-based authorities after all call locks and drain work.
            await self._publish_state(conn, operation, route, candidate)
            await self._host(conn, lease, runtime)
            if operation["lease_until"] <= await self._now(conn):
                raise ForestError("STALE_OPERATION", "Lifecycle lease expired before publication")
            await conn.execute(
                "UPDATE public.bot_generations SET status='RETIRED' WHERE bot_id=%s AND generation_id=%s AND status<>'DISPOSED'",
                (self.bot_id, route["generation_id"]),
            )
            await conn.execute(
                "UPDATE public.bot_generations SET status='ACTIVE' WHERE bot_id=%s AND generation_id=%s",
                (self.bot_id, candidate["generation_id"]),
            )
            result = await (
                await conn.execute(
                    "UPDATE public.bot_component_routes SET generation_id=%s,route_epoch=route_epoch+1,admission=%s,control_admission=%s "
                    "WHERE bot_id=%s AND component_id=%s RETURNING *",
                    (
                        candidate["generation_id"],
                        runtime["data_admission"],
                        bool(
                            runtime["control_admission"]
                            and operation["component_id"] == "adapter.onebot"
                            and candidate["capability_grants"] == []
                        ),
                        self.bot_id,
                        operation["component_id"],
                    ),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_lifecycle_operations SET phase='COMMITTED' WHERE bot_id=%s AND operation_id=%s",
                (self.bot_id, operation_id),
            )
            await conn.execute(
                "UPDATE public.bot_component_resources SET operation_id=NULL,lease_until=clock_timestamp() "
                "WHERE bot_id=%s AND component_id=%s",
                (self.bot_id, operation["component_id"]),
            )
            await self._event(
                conn,
                "lifecycle.committed",
                operation_id,
                {"generation_id": candidate["generation_id"], "route_epoch": result["route_epoch"]},
            )
            # Publication triggers may acquire configuration business locks.
            # Revalidate elapsed-time authority after those locks as well, so a
            # blocked effective-config update cannot outlive its host/operation.
            await self._host(conn, lease, runtime)
            if operation["lease_until"] <= await self._now(conn):
                raise ForestError("STALE_OPERATION", "Lifecycle lease expired during publication")
            assert result is not None
            return result

    async def abort_operation(
        self, lease: HostLease, operation_id: str, op_epoch: int, error_code: str = "ABORTED"
    ) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            # A current host can terminate its own expired operation while its
            # exact resource token still matches. This grants no publish/renew
            # authority, and cannot unfreeze a successor's operation or route.
            operation, route, runtime = await self._lifecycle(
                conn, lease, operation_id, op_epoch, terminal_cleanup=True
            )
            if operation["phase"] in {"FAILED", "ABANDONED"}:
                return operation
            if operation["phase"] == "COMMITTED":
                raise ForestError(
                    "ALREADY_COMMITTED", "Published routes require a new lifecycle operation"
                )
            if route["route_epoch"] == operation["expected_route_epoch"] and route["generation_id"]:
                old = await self._generation(conn, route["generation_id"])
                live = (
                    old["status"] in {"ACTIVE", "QUIESCING", "QUIESCENT"}
                    and old["host_incarnation"] == lease.incarnation
                    and old["host_owner_epoch"] == lease.owner_epoch
                )
                await conn.execute(
                    "UPDATE public.bot_component_routes SET admission=%s,control_admission=%s WHERE bot_id=%s AND component_id=%s",
                    (
                        bool(live and runtime["data_admission"]),
                        bool(
                            live
                            and runtime["control_admission"]
                            and operation["component_id"] == "adapter.onebot"
                            and old["capability_grants"] == []
                        ),
                        self.bot_id,
                        operation["component_id"],
                    ),
                )
                if live:
                    await conn.execute(
                        "UPDATE public.bot_generations SET status='ACTIVE' WHERE bot_id=%s AND generation_id=%s",
                        (self.bot_id, old["generation_id"]),
                    )
            result = await (
                await conn.execute(
                    "UPDATE public.bot_lifecycle_operations SET phase='FAILED',error_code=%s WHERE bot_id=%s AND operation_id=%s RETURNING *",
                    (error_code, self.bot_id, operation_id),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_component_resources SET operation_id=NULL,lease_until=clock_timestamp() "
                "WHERE bot_id=%s AND component_id=%s",
                (self.bot_id, operation["component_id"]),
            )
            await self._event(conn, "lifecycle.failed", operation_id, {"error_code": error_code})
            await self._host(conn, lease, runtime)
            assert result is not None
            return result

    async def fail_generation(self, lease: HostLease, generation_id: str, reason: str) -> None:
        """Revoke execution before the host terminates a failed Worker process."""
        await self._end_generation(lease, generation_id, reason, "FAILED")

    async def dispose_generation(self, lease: HostLease, generation_id: str, reason: str) -> None:
        """Host calls only after supervised resources have actually been reaped."""
        await self._end_generation(lease, generation_id, reason, "DISPOSED")

    async def _end_generation(
        self, lease: HostLease, generation_id: str, reason: str, status: str
    ) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            generation = await self._generation(conn, generation_id)
            await self._all_routes(conn)
            route = await self._route(conn, generation["component_id"])
            await self._host(conn, lease)
            generation = await self._generation(conn, generation_id)
            if generation["status"] == "DISPOSED" or generation["status"] == status:
                return
            await conn.execute(
                "UPDATE public.bot_generations SET status=%s WHERE bot_id=%s AND generation_id=%s",
                (status, self.bot_id, generation_id),
            )
            if route["generation_id"] == generation_id and generation["status"] not in {
                "FAILED",
                "DISPOSED",
            }:
                await conn.execute(
                    "UPDATE public.bot_component_routes SET admission=false,control_admission=false,route_epoch=route_epoch+1 "
                    "WHERE bot_id=%s AND component_id=%s",
                    (self.bot_id, generation["component_id"]),
                )
            calls = await (
                await conn.execute(
                    "SELECT call_id FROM public.bot_calls WHERE bot_id=%s AND generation_id=%s AND status='ADMITTED' "
                    "ORDER BY call_depth,call_id",
                    (self.bot_id, generation_id),
                )
            ).fetchall()
            for call in calls:
                await self._end_call_tree(conn, call["call_id"], "FAILED", "GENERATION_" + status)
            await self._host(conn, lease)
            await self._event(
                conn,
                "generation." + status.lower(),
                None,
                {"generation_id": generation_id, "reason": reason},
            )

    async def admit(
        self,
        lease: HostLease,
        component_id: str,
        call_id: str,
        operation_id: str,
        input_digest: str,
        timeout_seconds: float = 60,
        *,
        expected_policy_revision: int | None = None,
        expected_generation_id: str | None = None,
        expected_worker_incarnation: str | None = None,
        expected_route_epoch: int | None = None,
        dependency_components: tuple[str, ...] = (),
    ) -> CallGrant:
        return await self._admit(
            lease,
            component_id,
            call_id,
            operation_id,
            input_digest,
            timeout_seconds=timeout_seconds,
            expected_policy_revision=expected_policy_revision,
            expected_generation_id=expected_generation_id,
            expected_worker_incarnation=expected_worker_incarnation,
            expected_route_epoch=expected_route_epoch,
            dependency_components=dependency_components,
        )

    async def admit_control(
        self,
        lease: HostLease,
        component_id: str,
        call_id: str,
        operation_id: str,
        input_digest: str,
        timeout_seconds: float = 10,
        *,
        service_name: str,
        expected_policy_revision: int | None = None,
        expected_generation_id: str | None = None,
        expected_worker_incarnation: str | None = None,
        expected_route_epoch: int | None = None,
        dependency_components: tuple[str, ...] = (),
    ) -> CallGrant:
        if type(timeout_seconds) not in {int, float} or not 0 < timeout_seconds <= 30:
            raise ForestError(
                "INVALID_CALL", "Control codec deadline must be zero to 30 seconds", 422
            )
        if (
            component_id != "adapter.onebot"
            or service_name not in {"onebot.normalize", "onebot.prepare"}
            or dependency_components
        ):
            raise ForestError(
                "CONTROL_SCOPE_DENIED",
                "Control calls are limited to the host's pure OneBot codec",
                403,
            )
        return await self._admit(
            lease,
            component_id,
            call_id,
            operation_id,
            input_digest,
            timeout_seconds=timeout_seconds,
            expected_policy_revision=expected_policy_revision,
            expected_generation_id=expected_generation_id,
            expected_worker_incarnation=expected_worker_incarnation,
            expected_route_epoch=expected_route_epoch,
            admission_lane="CONTROL",
            control_service=service_name,
        )

    async def _admit(
        self,
        lease: HostLease,
        component_id: str,
        call_id: str,
        operation_id: str,
        input_digest: str,
        timeout_seconds: float = 60,
        *,
        expected_policy_revision: int | None = None,
        expected_generation_id: str | None = None,
        expected_worker_incarnation: str | None = None,
        expected_route_epoch: int | None = None,
        dependency_components: tuple[str, ...] = (),
        admission_lane: str = "DATA",
        control_service: str | None = None,
    ) -> CallGrant:
        self._ttl(timeout_seconds)
        dependencies = self._dependencies(component_id, dependency_components)
        async with self.pool.connection() as conn, conn.transaction():
            routes = await self._routes(conn, (component_id, *dependencies))
            route = routes[component_id]
            runtime = await self._runtime(conn)
            await self._host(conn, lease, runtime)
            if (
                expected_policy_revision is not None
                and runtime["policy_revision"] != expected_policy_revision
            ):
                raise ForestError(
                    "POLICY_REVOKED", "Component admission grant was revoked", status=403
                )
            generation = (
                await self._generation(conn, route["generation_id"])
                if route["generation_id"] is not None
                else None
            )
            if (
                (
                    expected_generation_id is not None
                    and route["generation_id"] != expected_generation_id
                )
                or (
                    expected_route_epoch is not None
                    and route["route_epoch"] != expected_route_epoch
                )
                or (
                    expected_worker_incarnation is not None
                    and (
                        generation is None
                        or generation["worker_incarnation"] != expected_worker_incarnation
                    )
                )
            ):
                # Bind admission to the concrete Worker selected by the host,
                # before allocating this stable operation's durable call. A
                # cache/publication race must remain retryable with a fresh route.
                raise ForestError("STALE_ROUTE", "Selected Worker route changed before admission")
            previous = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND (operation_id=%s OR call_id=%s)",
                    (self.bot_id, operation_id, call_id),
                )
            ).fetchall()
            if previous:
                old = previous[0]
                if (
                    len(previous) != 1
                    or old["operation_id"] != operation_id
                    or old["component_id"] != component_id
                    or old["input_digest"] != input_digest
                    or old["parent_call_id"] is not None
                    or old["admission_lane"] != admission_lane
                    or old["control_service"] != control_service
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Call identity already binds different input"
                    )
                if old["status"] == "COMPLETED":
                    raise ForestError(
                        "CALL_COMPLETED",
                        "Operation already completed",
                        details={"result": old["result"]},
                    )
                if old["status"] in {"FAILED", "CANCELLED"}:
                    raise ForestError(
                        "CALL_FAILED",
                        "Operation already terminated",
                        details={"error_code": old["error_code"]},
                    )
                raise ForestError(
                    "OPERATION_PENDING",
                    "Operation is already admitted; reconcile its existing call",
                )
            if (
                not runtime[
                    "control_admission" if admission_lane == "CONTROL" else "data_admission"
                ]
                or not route["control_admission" if admission_lane == "CONTROL" else "admission"]
                or not route["generation_id"]
            ):
                raise ForestError("ADMISSION_CLOSED", "Component is paused or reloading")
            assert generation is not None
            if admission_lane == "CONTROL" and generation["capability_grants"] != []:
                raise ForestError(
                    "CONTROL_SCOPE_DENIED", "Control codec cannot hold business capabilities", 403
                )
            if (
                generation["status"] != "ACTIVE"
                or generation["host_incarnation"] != lease.incarnation
                or generation["host_owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError("STALE_GENERATION", "Route has no current active Worker")
            for dependency in dependencies:
                reserved = routes[dependency]
                if not reserved["admission"] or not reserved["generation_id"]:
                    raise ForestError(
                        "ADMISSION_CLOSED", "A reserved dependency is paused or reloading"
                    )
                target = await self._generation(conn, reserved["generation_id"])
                if (
                    target["status"] != "ACTIVE"
                    or target["host_incarnation"] != lease.incarnation
                    or target["host_owner_epoch"] != lease.owner_epoch
                ):
                    raise ForestError(
                        "ADMISSION_CLOSED", "A reserved dependency has no current active Worker"
                    )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_calls(bot_id,call_id,operation_id,component_id,generation_id,route_epoch,"
                    "worker_incarnation,host_id,host_incarnation,owner_epoch,storage_generation,maintenance_epoch,"
                    "policy_revision,deadline_at,input_digest,root_call_id,dependency_components,admission_lane,control_service) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "clock_timestamp()+%s*interval '1 second',%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.bot_id,
                        call_id,
                        operation_id,
                        component_id,
                        generation["generation_id"],
                        route["route_epoch"],
                        generation["worker_incarnation"],
                        lease.host_id,
                        lease.incarnation,
                        lease.owner_epoch,
                        runtime["storage_generation"],
                        runtime["maintenance_epoch"],
                        runtime["policy_revision"],
                        timeout_seconds,
                        input_digest,
                        call_id,
                        list(dependencies),
                        admission_lane,
                        control_service,
                    ),
                )
            ).fetchone()
            await self._event(
                conn,
                "call.admitted",
                operation_id,
                {
                    "call_id": call_id,
                    "component_id": component_id,
                    "admission_lane": admission_lane,
                },
            )
            assert row is not None
            host = await self._host(conn, lease, runtime)
            await self._guard_time(conn, host, self._grant_value(row))
            return self._grant_value(row)

    async def _chain(self, conn: AsyncConnection, call_id: str) -> list[dict]:
        """Read immutable ancestry before acquiring any runtime or call locks."""
        chain: list[dict] = []
        current = call_id
        seen: set[str] = set()
        while current is not None:
            if current in seen or len(chain) >= 9:
                raise ForestError(
                    "INVALID_CALL_TREE", "Call ancestry is cyclic or exceeds its depth limit"
                )
            seen.add(current)
            row = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s",
                    (self.bot_id, current),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "CALL_NOT_FOUND", "Admitted call ancestry does not exist", status=404
                )
            chain.append(row)
            current = row["parent_call_id"]
        chain.reverse()
        for depth, row in enumerate(chain):
            if row["root_call_id"] != chain[0]["call_id"] or row["call_depth"] != depth:
                raise ForestError(
                    "INVALID_CALL_TREE", "Call ancestry does not match its immutable root"
                )
        return chain

    async def admit_child(
        self,
        parent_grant: CallGrant,
        target_component_id: str,
        call_id: str,
        operation_id: str,
        input_digest: str,
        timeout_seconds: float = 60,
        *,
        expected_policy_revision: int | None = None,
        expected_generation_id: str | None = None,
        expected_worker_incarnation: str | None = None,
        expected_route_epoch: int | None = None,
        dependency_components: tuple[str, ...] = (),
    ) -> CallGrant:
        self._ttl(timeout_seconds)
        if parent_grant.admission_lane != "DATA":
            raise ForestError(
                "CONTROL_SCOPE_DENIED", "Control codec cannot call another component", 403
            )
        dependencies = self._dependencies(target_component_id, dependency_components)
        if target_component_id == parent_grant.component_id:
            raise ForestError(
                "CALL_CYCLE", "Synchronous calls cannot return to an ancestor component"
            )
        if target_component_id not in parent_grant.dependency_components or not set(
            dependencies
        ).issubset(parent_grant.dependency_components):
            raise ForestError(
                "DEPENDENCY_DENIED",
                "Child target and dependencies must belong to the parent's reserved closure",
                status=403,
            )
        async with self.pool.connection() as conn, conn.transaction():
            parent, host = await self._guard(
                conn,
                parent_grant,
                extra_components=(target_component_id, *dependencies),
            )
            chain = await self._chain(conn, parent_grant.call_id)
            if target_component_id in {row["component_id"] for row in chain}:
                raise ForestError(
                    "CALL_CYCLE", "Synchronous calls cannot return to an ancestor component"
                )
            if parent["call_depth"] >= 8:
                raise ForestError(
                    "CALL_DEPTH_EXCEEDED", "Synchronous call nesting is limited to eight levels"
                )
            if (
                expected_policy_revision is not None
                and parent["policy_revision"] != expected_policy_revision
            ):
                raise ForestError(
                    "POLICY_REVOKED", "Target component admission grant was revoked", status=403
                )
            route = await self._route(
                conn, target_component_id
            )  # Already held in the sorted fence set.
            generation = (
                await self._generation(conn, route["generation_id"])
                if route["generation_id"]
                else None
            )
            if (
                (
                    expected_generation_id is not None
                    and route["generation_id"] != expected_generation_id
                )
                or (
                    expected_route_epoch is not None
                    and route["route_epoch"] != expected_route_epoch
                )
                or (
                    expected_worker_incarnation is not None
                    and (
                        generation is None
                        or generation["worker_incarnation"] != expected_worker_incarnation
                    )
                )
            ):
                raise ForestError("STALE_ROUTE", "Selected child Worker changed before admission")
            previous = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND (operation_id=%s OR call_id=%s)",
                    (self.bot_id, operation_id, call_id),
                )
            ).fetchall()
            if previous:
                old = previous[0]
                if (
                    len(previous) != 1
                    or old["operation_id"] != operation_id
                    or old["parent_call_id"] != parent_grant.call_id
                    or old["root_call_id"] != parent_grant.root_call_id
                    or old["component_id"] != target_component_id
                    or old["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT",
                        "Child operation already belongs to another parent or input",
                    )
                if old["status"] == "COMPLETED":
                    raise ForestError(
                        "CALL_COMPLETED",
                        "Child operation already completed",
                        details={"result": old["result"]},
                    )
                if old["status"] in {"FAILED", "CANCELLED"}:
                    raise ForestError(
                        "CALL_FAILED",
                        "Child operation already terminated",
                        details={"error_code": old["error_code"]},
                    )
                raise ForestError("OPERATION_PENDING", "Child operation is already admitted")
            if generation is None or generation["status"] not in {"ACTIVE", "QUIESCING"}:
                raise ForestError(
                    "ADMISSION_CLOSED", "Child target is not an active or draining generation"
                )
            if (
                generation["host_incarnation"] != host["incarnation"]
                or generation["host_owner_epoch"] != host["owner_epoch"]
            ):
                raise ForestError("STALE_GENERATION", "Child target belongs to an old host")
            count = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM public.bot_calls WHERE bot_id=%s AND root_call_id=%s",
                    (self.bot_id, parent_grant.root_call_id),
                )
            ).fetchone()
            if count and count["n"] >= 256:
                raise ForestError(
                    "CALL_TREE_LIMIT",
                    "A call tree may contain at most 256 admitted calls",
                    status=429,
                )
            await self._guard_time(conn, host, parent_grant)
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_calls(bot_id,call_id,operation_id,component_id,generation_id,route_epoch,"
                    "worker_incarnation,host_id,host_incarnation,owner_epoch,storage_generation,maintenance_epoch,"
                    "policy_revision,deadline_at,input_digest,parent_call_id,root_call_id,call_depth,dependency_components) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    "LEAST(%s,clock_timestamp()+%s*interval '1 second'),%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.bot_id,
                        call_id,
                        operation_id,
                        target_component_id,
                        generation["generation_id"],
                        route["route_epoch"],
                        generation["worker_incarnation"],
                        host["host_id"],
                        host["incarnation"],
                        host["owner_epoch"],
                        parent["storage_generation"],
                        parent["maintenance_epoch"],
                        parent["policy_revision"],
                        parent["deadline_at"],
                        timeout_seconds,
                        input_digest,
                        parent["call_id"],
                        parent["root_call_id"],
                        parent["call_depth"] + 1,
                        list(dependencies),
                    ),
                )
            ).fetchone()
            await self._event(
                conn,
                "call.admitted",
                operation_id,
                {
                    "call_id": call_id,
                    "component_id": target_component_id,
                    "parent_call_id": parent["call_id"],
                    "root_call_id": parent["root_call_id"],
                    "call_depth": parent["call_depth"] + 1,
                },
            )
            assert row is not None
            return self._grant_value(row)

    async def authorized_child_call(
        self, parent_grant: CallGrant, operation_id: str, expected_policy_revision: int
    ) -> dict | None:
        async with self.pool.connection() as conn, conn.transaction():
            await self._guard(conn, parent_grant)
            if parent_grant.policy_revision != expected_policy_revision:
                raise ForestError("POLICY_REVOKED", "Child receipt grant was revoked", status=403)
            child = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s",
                    (self.bot_id, operation_id),
                )
            ).fetchone()
            if child is not None and (
                child["parent_call_id"] != parent_grant.call_id
                or child["root_call_id"] != parent_grant.root_call_id
            ):
                raise ForestError("IDEMPOTENCY_CONFLICT", "Child receipt belongs to another parent")
            return child

    async def _guard(
        self, conn: AsyncConnection, grant: CallGrant, *, pending: bool = True, extra_components=()
    ) -> tuple[dict, dict]:
        self._scope(grant)
        chain = await self._chain(conn, grant.call_id)
        routes = await self._routes(
            conn, (*[row["component_id"] for row in chain], *extra_components)
        )
        runtime = await self._runtime(conn)
        host = await self._host(
            conn,
            HostLease(
                grant.bot_id,
                grant.host_id,
                grant.host_incarnation,
                grant.owner_epoch,
                grant.deadline_at,
            ),
            runtime,
        )
        call = None
        for ancestor in chain:
            current = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s FOR UPDATE",
                    (self.bot_id, ancestor["call_id"]),
                )
            ).fetchone()
            assert current is not None
            is_leaf = current["call_id"] == grant.call_id
            if current["status"] == "CANCELLED" or (
                is_leaf and current["cancel_epoch"] != grant.cancel_epoch
            ):
                raise ForestError("CALL_CANCELLED", "Call or ancestor cancellation committed first")
            if is_leaf:
                compared = dict(
                    current, dependency_components=tuple(current["dependency_components"])
                )
                if any(compared[key] != value for key, value in asdict(grant).items()):
                    raise ForestError(
                        "INVALID_GRANT",
                        "Call grant does not match its host-admitted identity",
                        status=403,
                    )
                call = current
            route = routes[current["component_id"]]
            if (
                route["route_epoch"] != current["route_epoch"]
                or route["generation_id"] != current["generation_id"]
            ):
                raise ForestError("STALE_ROUTE", "A caller or target component route was replaced")
            generation = await self._generation(conn, current["generation_id"])
            if generation["status"] not in {"ACTIVE", "QUIESCING"}:
                raise ForestError(
                    "STALE_GENERATION", "A caller or target generation lost execution authority"
                )
            if (
                current["host_id"] != host["host_id"]
                or current["host_incarnation"] != host["incarnation"]
                or current["owner_epoch"] != host["owner_epoch"]
            ):
                raise ForestError("STALE_HOST", "A call ancestor belongs to an old host")
            if runtime["storage_generation"] != current["storage_generation"]:
                raise ForestError("STALE_STORAGE", "Bot storage generation changed")
            if runtime["policy_revision"] != current["policy_revision"]:
                raise ForestError(
                    "POLICY_REVOKED", "Call authorization revision is stale", status=403
                )
            if current["admission_lane"] == "CONTROL" and not runtime["control_admission"]:
                raise ForestError("CONTROL_CLOSED", "Bot control admission was closed", 403)
            if (
                current["admission_lane"] == "DATA"
                and runtime["maintenance_epoch"] != current["maintenance_epoch"]
            ):
                raise ForestError(
                    "STALE_MAINTENANCE", "Maintenance intent invalidated the call tree"
                )
            if current["status"] != "ADMITTED" and (not is_leaf or pending):
                raise ForestError("CALL_TERMINAL", "Call or ancestor has already terminated")
            if current["deadline_at"] <= await self._now(conn):
                raise ForestError("CALL_EXPIRED", "A call or ancestor deadline has elapsed")
        await self._guard_time(conn, host, grant)
        assert call is not None
        return call, host

    async def _guard_time(self, conn: AsyncConnection, host: dict, grant: CallGrant) -> None:
        now = await self._now(conn)
        if host["expires_at"] <= now:
            raise ForestError("STALE_HOST", "Host lease expired before commit")
        if grant.deadline_at <= now:
            raise ForestError("CALL_EXPIRED", "Call deadline elapsed before commit")

    async def read_state(self, grant: CallGrant, key: str) -> dict:
        if grant.admission_lane != "DATA":
            raise ForestError(
                "CONTROL_SCOPE_DENIED", "Control codec cannot read business state", 403
            )
        async with self.pool.connection() as conn, conn.transaction():
            _, host = await self._guard(conn, grant)
            row = await (
                await conn.execute(
                    "SELECT revision,value FROM public.bot_component_state WHERE bot_id=%s AND component_id=%s AND key=%s",
                    (self.bot_id, grant.component_id, key),
                )
            ).fetchone()
            await self._guard_time(conn, host, grant)
            return row or {"revision": 0, "value": None}

    async def commit_state(
        self,
        grant: CallGrant,
        key: str,
        expected_revision: int,
        value: Any,
        operation_id: str,
        input_digest: str,
    ) -> dict:
        if grant.admission_lane != "DATA":
            raise ForestError(
                "CONTROL_SCOPE_DENIED", "Control codec cannot write business state", 403
            )
        if not operation_id or len(operation_id) > 200 or not key or len(key) > 200:
            raise ForestError(
                "INVALID_STATE_STEP",
                "State key and operation ID must be bounded nonempty strings",
                status=400,
            )
        payload_digest = _digest(
            {"key": key, "expected_revision": expected_revision, "value": value}
        )
        value = json.loads(json.dumps(value, allow_nan=False))
        async with self.pool.connection() as conn, conn.transaction():
            _, host = await self._guard(conn, grant)
            previous = await (
                await conn.execute(
                    "SELECT * FROM public.bot_state_steps WHERE bot_id=%s AND call_id=%s AND operation_id=%s",
                    (self.bot_id, grant.call_id, operation_id),
                )
            ).fetchone()
            if previous:
                if (
                    previous["input_digest"] != input_digest
                    or previous["payload_digest"] != payload_digest
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "State step identity binds different input"
                    )
                await self._guard_time(conn, host, grant)
                return previous["receipt"]
            await conn.execute(
                "INSERT INTO public.bot_component_state(bot_id,component_id,key,revision) VALUES (%s,%s,%s,0) ON CONFLICT DO NOTHING",
                (self.bot_id, grant.component_id, key),
            )
            state = await (
                await conn.execute(
                    "SELECT revision FROM public.bot_component_state WHERE bot_id=%s AND component_id=%s AND key=%s FOR UPDATE",
                    (self.bot_id, grant.component_id, key),
                )
            ).fetchone()
            assert state is not None
            if state["revision"] != expected_revision:
                raise ForestError(
                    "REVISION_CONFLICT",
                    "Component state changed",
                    details={"current_revision": state["revision"]},
                )
            generation = await self._generation(conn, grant.generation_id)
            _, rows = await self._state_source(conn, grant.component_id)
            values = self._state_values(rows)
            values[key] = value
            self._validate_state_values(values, generation["state_value_schema"])
            await self._guard_time(conn, host, grant)
            receipt = {
                "operation_id": operation_id,
                "call_operation_id": grant.operation_id,
                "component_id": grant.component_id,
                "revision": expected_revision + 1,
                "value": value,
            }
            await conn.execute(
                "INSERT INTO public.bot_state_steps(bot_id,call_id,operation_id,input_digest,payload_digest,receipt) VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    self.bot_id,
                    grant.call_id,
                    operation_id,
                    input_digest,
                    payload_digest,
                    Jsonb(receipt),
                ),
            )
            await conn.execute(
                "INSERT INTO public.bot_state_revisions(bot_id,component_id,key,revision,previous_revision,value,call_id,operation_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.bot_id,
                    grant.component_id,
                    key,
                    expected_revision + 1,
                    expected_revision,
                    Jsonb(value),
                    grant.call_id,
                    operation_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_component_state SET revision=%s,value=%s,deleted=false WHERE bot_id=%s AND component_id=%s AND key=%s",
                (expected_revision + 1, Jsonb(value), self.bot_id, grant.component_id, key),
            )
            await conn.execute(
                "UPDATE public.bot_component_state_heads SET revision=revision+1 WHERE bot_id=%s AND component_id=%s",
                (self.bot_id, grant.component_id),
            )
            await self._event(
                conn,
                "state.committed",
                operation_id,
                {
                    "call_id": grant.call_id,
                    "call_operation_id": grant.operation_id,
                    "component_id": grant.component_id,
                    "key": key,
                    "revision": expected_revision + 1,
                },
            )
            await self._guard_time(conn, host, grant)
            return receipt

    async def finish_call(self, grant: CallGrant, result: Any) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            call, host = await self._guard(conn, grant, pending=False)
            if call["status"] == "COMPLETED":
                if call["result"] != result:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Call already finished with another result"
                    )
                return call
            if call["status"] != "ADMITTED":
                raise ForestError("CALL_TERMINAL", "Failed call cannot be completed")
            descendants = await self._descendants(conn, grant.call_id)
            if any(
                row["call_id"] != grant.call_id and row["status"] == "ADMITTED"
                for row in descendants
            ):
                raise ForestError(
                    "CHILDREN_PENDING",
                    "A call cannot finish before all of its child calls terminate",
                )
            await self._guard_time(conn, host, grant)
            row = await (
                await conn.execute(
                    "UPDATE public.bot_calls SET status='COMPLETED',result=%s WHERE bot_id=%s AND call_id=%s RETURNING *",
                    (Jsonb(result), self.bot_id, grant.call_id),
                )
            ).fetchone()
            await self._event(
                conn, "call.completed", grant.operation_id, {"call_id": grant.call_id}
            )
            assert row is not None
            return row

    async def read_call(self, operation_id: str) -> dict | None:
        async with self.pool.connection() as conn:
            await self._identity(conn)
            return await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s",
                    (self.bot_id, operation_id),
                )
            ).fetchone()

    async def authorized_call(
        self,
        lease: HostLease,
        operation_id: str,
        expected_policy_revision: int,
        *,
        admission_lane="DATA",
    ) -> dict | None:
        """Receipt query under current host/policy, independent of old call leases."""
        async with self.pool.connection() as conn, conn.transaction():
            runtime = await self._runtime(conn)
            await self._host(conn, lease, runtime)
            if runtime["policy_revision"] != expected_policy_revision:
                raise ForestError(
                    "POLICY_REVOKED", "Component receipt grant was revoked", status=403
                )
            if admission_lane == "CONTROL" and not runtime["control_admission"]:
                raise ForestError("CONTROL_CLOSED", "Bot control admission is closed", 403)
            result = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s",
                    (self.bot_id, operation_id),
                )
            ).fetchone()
            if result is not None and result["parent_call_id"] is not None:
                raise ForestError(
                    "CHILD_AUTHORITY_REQUIRED",
                    "Child receipts require their live parent call authority",
                    status=403,
                )
            if result is not None and result["admission_lane"] != admission_lane:
                raise ForestError(
                    "IDEMPOTENCY_CONFLICT", "Call operation belongs to a different admission lane"
                )
            await self._host(conn, lease, runtime)
            return result

    async def _call_for_host(self, conn: AsyncConnection, lease: HostLease, call_id: str) -> dict:
        call = await (
            await conn.execute(
                "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s",
                (self.bot_id, call_id),
            )
        ).fetchone()
        if call is None:
            raise ForestError("CALL_NOT_FOUND", "Admitted call does not exist", status=404)
        # Cancellation/failure can touch a dynamic descendant subtree. Reserving
        # all existing routes first prevents a child from introducing a new lock
        # after this transaction has acquired runtime or parent-call locks.
        await self._all_routes(conn)
        await self._host(conn, lease)
        call = await (
            await conn.execute(
                "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s FOR UPDATE",
                (self.bot_id, call_id),
            )
        ).fetchone()
        assert call is not None
        # The row lock may have waited beyond expiry; never let an old host
        # cancel/fail a call after its final execution authority has expired.
        await self._host(conn, lease)
        return call

    async def _descendants(self, conn: AsyncConnection, call_id: str) -> list[dict]:
        return await (
            await conn.execute(
                "WITH RECURSIVE subtree AS (SELECT call_id FROM public.bot_calls WHERE bot_id=%s AND call_id=%s "
                "UNION SELECT c.call_id FROM public.bot_calls c JOIN subtree p ON c.parent_call_id=p.call_id WHERE c.bot_id=%s) "
                "SELECT c.* FROM public.bot_calls c JOIN subtree t ON c.call_id=t.call_id "
                "WHERE c.bot_id=%s ORDER BY c.call_depth,c.call_id",
                (self.bot_id, call_id, self.bot_id, self.bot_id),
            )
        ).fetchall()

    async def _end_call_tree(
        self, conn: AsyncConnection, call_id: str, status: str, error_code: str
    ) -> dict:
        descendants = await self._descendants(conn, call_id)
        # Caller owns the complete route set and runtime/host rows already.
        # Lock all affected calls in root-to-leaf canonical order before updates.
        for row in descendants:
            await conn.execute(
                "SELECT call_id FROM public.bot_calls WHERE bot_id=%s AND call_id=%s FOR UPDATE",
                (self.bot_id, row["call_id"]),
            )
        await conn.execute(
            "UPDATE public.bot_calls SET status=%s,error_code=%s,cancel_epoch=cancel_epoch+%s "
            "WHERE bot_id=%s AND call_id=ANY(%s) AND status='ADMITTED'",
            (
                status,
                error_code,
                1 if status == "CANCELLED" else 0,
                self.bot_id,
                [row["call_id"] for row in descendants],
            ),
        )
        result = await (
            await conn.execute(
                "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s",
                (self.bot_id, call_id),
            )
        ).fetchone()
        assert result is not None
        return result

    async def cancel_call(
        self, lease: HostLease, call_id: str, expected_cancel_epoch: int = 0
    ) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            call = await self._call_for_host(conn, lease, call_id)
            if call["status"] != "ADMITTED":
                return call
            if call["cancel_epoch"] != expected_cancel_epoch:
                raise ForestError("CANCEL_CONFLICT", "Call cancellation revision changed")
            row = await self._end_call_tree(conn, call_id, "CANCELLED", "CALL_CANCELLED")
            await self._host(conn, lease)
            await self._event(conn, "call.cancelled", call["operation_id"], {"call_id": call_id})
            assert row is not None
            return row

    async def fail_call(self, lease: HostLease, call_id: str, error_code: str) -> dict:
        async with self.pool.connection() as conn, conn.transaction():
            call = await self._call_for_host(conn, lease, call_id)
            if call["status"] != "ADMITTED":
                return call
            row = await self._end_call_tree(conn, call_id, "FAILED", error_code)
            await self._host(conn, lease)
            await self._event(
                conn,
                "call.failed",
                call["operation_id"],
                {"call_id": call_id, "error_code": error_code},
            )
            assert row is not None
            return row

    async def _event(
        self, conn: AsyncConnection, topic: str, operation_id: str | None, payload: dict
    ) -> None:
        await conn.execute(
            "INSERT INTO public.bot_audit_records(bot_id,audit_id,action,operation_id,details) VALUES (%s,%s,%s,%s,%s)",
            (self.bot_id, str(uuid4()), topic, operation_id, Jsonb(payload)),
        )
        await conn.execute(
            "INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload) VALUES (%s,%s,%s,%s,%s)",
            (self.bot_id, str(uuid4()), topic, operation_id, Jsonb(payload)),
        )
