"""Stable host scope identity and fenced process-incarnation lease."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files

import psycopg

from forest.errors import ForestError

_INCARNATION = re.compile(r"incarnation/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                          r"[0-9a-f]{4}-[0-9a-f]{12}\Z")


@dataclass(frozen=True)
class HostLease:
    host_id: str
    incarnation: str
    owner_epoch: int
    expires_at: datetime


class HostIdentityAuthority:
    def __init__(self, store) -> None:
        self.store = store

    async def initialize(self) -> dict:
        source = files("forest.storage.official").joinpath("host_identity.sql").read_bytes()
        digest = hashlib.sha256(source).hexdigest()
        statement = source.decode("utf-8")
        async with self.store.pool.connection() as conn:
            try:
                async with conn.transaction():
                    await conn.execute("SET LOCAL lock_timeout='5s'")
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        ("forest.host.identity/1",),
                    )
                    relation = await (await conn.execute(
                        "SELECT to_regclass('forest_host.host_identity') AS value"
                    )).fetchone()
                    if relation["value"] is None:
                        await self._require_empty(conn)
                        await conn.execute(statement)
                        await conn.execute(
                            "INSERT INTO forest_host.host_identity(host_id,baseline_sha256) "
                            "VALUES (%s,%s)", (self.store.host_id, digest),
                        )
                        await conn.execute(
                            "INSERT INTO forest_host.supervisor_lease(host_id) VALUES (%s)",
                            (self.store.host_id,),
                        )
                        await conn.execute(
                            "INSERT INTO forest_host.runtime_graph_clock(host_id) VALUES (%s)",
                            (self.store.host_id,),
                        )
                    row = await (await conn.execute(
                        "SELECT * FROM forest_host.host_identity WHERE singleton FOR SHARE"
                    )).fetchone()
                    if (row is None or row["host_id"] != self.store.host_id
                            or row["baseline_sha256"] != digest):
                        raise ForestError(
                            "HOST_SCOPE_MISMATCH", "Host database identity differs", 409,
                        )
                    await self._verify_inventory(conn)
                    return dict(row)
            except ForestError:
                raise
            except psycopg.Error as exc:
                raise ForestError(
                    "HOST_BASELINE_INSTALL_FAILED", "Host baseline could not be installed", 503,
                ) from exc

    @staticmethod
    async def _require_empty(conn) -> None:
        row = await (await conn.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT IN "
            "('pg_catalog','information_schema','public') AND nspname NOT LIKE 'pg_toast%' "
            "AND nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast_temp_%') "
            "OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f')) "
            "OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='public') AS occupied"
        )).fetchone()
        if row["occupied"]:
            raise ForestError(
                "HOST_DATABASE_NOT_EMPTY", "Host identity needs a dedicated database", 409,
            )

    async def _verify_inventory(self, conn) -> None:
        relations = await (await conn.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='forest_host' AND c.relkind IN ('r','p','v','m','S','f')"
        )).fetchall()
        functions = await (await conn.execute(
            "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='forest_host'"
        )).fetchall()
        triggers = await (await conn.execute(
            "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='forest_host' AND NOT t.tgisinternal"
        )).fetchall()
        lease = await (await conn.execute(
            "SELECT host_id FROM forest_host.supervisor_lease"
        )).fetchall()
        clocks = await (await conn.execute(
            "SELECT host_id FROM forest_host.runtime_graph_clock"
        )).fetchall()
        registered = await (await conn.execute(
            "SELECT namespace::text AS namespace FROM forest_host.schema_bundles "
            "WHERE host_id=%s", (self.store.host_id,),
        )).fetchall()
        namespaces = await (await conn.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname NOT IN "
            "('pg_catalog','information_schema','public') "
            "AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp_%' "
            "AND nspname NOT LIKE 'pg_toast_temp_%'"
        )).fetchall()
        if ({row["relname"] for row in relations}
                != {"host_identity", "schema_bundles", "runtime_graph_clock",
                    "runtime_graphs", "runtime_generations", "runtime_graph_members",
                    "runtime_package_selection",
                    "package_service_operations",
                    "runtime_graph_change_requests",
                    "web_authority_receipts", "audit_records", "supervisor_lease"}
                or {row["proname"] for row in functions}
                != {"reject_identity_change", "reject_receipt_change",
                    "guard_runtime_graph_change"}
                or {row["tgname"] for row in triggers}
                != {"host_identity_immutable", "runtime_graph_change_transition",
                    "web_authority_receipt_immutable", "host_audit_record_immutable"}
                or [row["host_id"] for row in lease] != [self.store.host_id]
                or [row["host_id"] for row in clocks] != [self.store.host_id]
                or {row["nspname"] for row in namespaces}
                != {"forest_host", *(row["namespace"] for row in registered)}):
            raise ForestError("HOST_SCHEMA_TAMPERED", "Host schema inventory differs", 409)

    async def claim(self, incarnation: str, *, lease_seconds: int = 30) -> HostLease:
        if (_INCARNATION.fullmatch(incarnation or "") is None
                or type(lease_seconds) is not int or not 3 <= lease_seconds <= 300):
            raise ForestError("HOST_LEASE_INVALID", "Host lease input is invalid", 422)
        async with self.store.transaction() as conn:
            row = await (await conn.execute(
                "SELECT *,expires_at<=clock_timestamp() AS expired "
                "FROM forest_host.supervisor_lease WHERE host_id=%s FOR UPDATE",
                (self.store.host_id,),
            )).fetchone()
            if row is None:
                raise ForestError("HOST_SCHEMA_TAMPERED", "Host lease row is absent", 409)
            same = row["incarnation"] == incarnation
            if not row["expired"] and not same:
                raise ForestError("HOST_SUPERVISOR_BUSY", "Another host incarnation is active", 409)
            epoch = row["owner_epoch"] if same and not row["expired"] else row["owner_epoch"] + 1
            saved = await (await conn.execute(
                "UPDATE forest_host.supervisor_lease SET incarnation=%s,owner_epoch=%s,"
                "expires_at=clock_timestamp()+make_interval(secs => %s) "
                "WHERE host_id=%s RETURNING *",
                (incarnation, epoch, lease_seconds, self.store.host_id),
            )).fetchone()
            return self._lease(saved)

    async def renew(self, lease: HostLease, *, lease_seconds: int = 30) -> HostLease:
        self._validate_lease(lease, lease_seconds)
        async with self.store.transaction() as conn:
            saved = await (await conn.execute(
                "UPDATE forest_host.supervisor_lease SET expires_at=clock_timestamp()+"
                "make_interval(secs => %s) WHERE host_id=%s AND incarnation=%s "
                "AND owner_epoch=%s AND expires_at>clock_timestamp() RETURNING *",
                (lease_seconds, lease.host_id, lease.incarnation, lease.owner_epoch),
            )).fetchone()
            if saved is None:
                raise ForestError("STALE_HOST_LEASE", "Host lease is stale", 409)
            return self._lease(saved)

    async def release(self, lease: HostLease) -> None:
        self._validate_lease(lease)
        async with self.store.transaction() as conn:
            saved = await (await conn.execute(
                "UPDATE forest_host.supervisor_lease SET expires_at=clock_timestamp() "
                "WHERE host_id=%s AND incarnation=%s AND owner_epoch=%s "
                "AND expires_at>clock_timestamp() RETURNING owner_epoch",
                (lease.host_id, lease.incarnation, lease.owner_epoch),
            )).fetchone()
            if saved is None:
                raise ForestError("STALE_HOST_LEASE", "Host lease is stale", 409)

    async def verify_in(self, conn, lease: HostLease) -> None:
        """Fence one resource mutation in the caller's transaction.

        The row lock holds a concurrent takeover until that mutation commits.
        """
        self._validate_lease(lease)
        row = await (await conn.execute(
            "SELECT 1 FROM forest_host.supervisor_lease WHERE host_id=%s "
            "AND incarnation=%s AND owner_epoch=%s AND expires_at>clock_timestamp() "
            "FOR SHARE",
            (lease.host_id, lease.incarnation, lease.owner_epoch),
        )).fetchone()
        if row is None:
            raise ForestError("STALE_HOST_LEASE", "Host lease is stale", 409)

    def _validate_lease(self, lease: HostLease, lease_seconds: int = 30) -> None:
        if (type(lease) is not HostLease or lease.host_id != self.store.host_id
                or type(lease_seconds) is not int or not 3 <= lease_seconds <= 300):
            raise ForestError("HOST_LEASE_INVALID", "Host lease input is invalid", 422)

    def _lease(self, row) -> HostLease:
        return HostLease(
            self.store.host_id, row["incarnation"], row["owner_epoch"], row["expires_at"]
        )
