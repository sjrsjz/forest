"""Clean ``kernel.runtime`` authority for host leases and immutable runtime graphs."""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import (
    ActiveGenerationRoute,
    PreparedGenerationBinding,
    RuntimeGenerationGraph,
    validate_prepared_bindings,
)
from forest.storage.runtime_identity import HostLease

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


class KernelRuntimeRepository:
    """The only writer for the clean runtime Generation and RuntimeGraph facts."""

    def __init__(self, store, namespace: str) -> None:
        if not namespace:
            raise ValueError("kernel runtime namespace is required")
        self.store = store
        self.namespace = namespace

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    @asynccontextmanager
    async def _control_connection(self, caller_conn):
        if caller_conn is not None:
            yield caller_conn
        else:
            async with self.store.pool.connection() as conn, conn.transaction():
                yield conn

    async def status(self) -> dict:
        """Return a bounded view of the current Bot runtime graph for control planes."""

        return await self._status_in(None)

    async def status_in(self, conn) -> dict:
        """Read the bounded runtime projection in the caller's transaction."""

        return await self._status_in(conn)

    async def _status_in(self, caller_conn) -> dict:

        async with self._control_connection(caller_conn) as conn:
            runtime = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT s.status,s.maintenance_epoch,s.policy_revision,"
                        "s.data_admission,s.control_admission,h.graph_id,h.graph_revision,"
                        "g.graph_digest,g.activation_id,g.state AS graph_state,h.published_at FROM {} s "
                        "JOIN {} h ON h.bot_id=s.bot_id LEFT JOIN {} g ON g.bot_id=h.bot_id "
                        "AND g.graph_id=h.graph_id WHERE s.bot_id=%s"
                    ).format(
                        self._table("runtime_state"),
                        self._table("runtime_graph_head"),
                        self._table("runtime_graphs"),
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if runtime is None:
                raise ForestError(
                    "KERNEL_RUNTIME_NOT_INITIALIZED", "Runtime state is unavailable", 503
                )
            members = ()
            if runtime["graph_id"] is not None:
                rows = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT m.component_id,m.generation_id,g.status,g.config_revision,"
                            "m.package_id,m.package_version,m.package_digest,"
                            "m.data_admission,m.control_admission "
                            "FROM {} m JOIN {} g ON g.bot_id=m.bot_id "
                            "AND g.generation_id=m.generation_id WHERE m.bot_id=%s "
                            "AND m.graph_id=%s ORDER BY m.component_id"
                        ).format(
                            self._table("runtime_graph_modules"),
                            self._table("generations"),
                        ),
                        (self.store.bot_id, runtime["graph_id"]),
                    )
                ).fetchall()
                members = tuple(dict(row) for row in rows)
            replacement = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT operation_id,requested_by,expected_graph_revision,"
                        "target_component_id,activation_id,"
                        "state,attempt_count,result_graph_id,result_graph_revision,error_code,"
                        "created_at,updated_at FROM {} WHERE bot_id=%s "
                        "ORDER BY created_at DESC,operation_id DESC LIMIT 1"
                    ).format(self._table("graph_replacement_requests")),
                    (self.store.bot_id,),
                )
            ).fetchone()
        return {
            "protocol": "forest.runtime-status/1",
            "bot_id": self.store.bot_id,
            **dict(runtime),
            "members": members,
            "latest_replacement": dict(replacement) if replacement is not None else None,
        }

    async def last_published_package(self, component_id: str) -> dict | None:
        """Read an immutable package identity from the last published Bot graph."""
        if not isinstance(component_id, str) or _IDENTIFIER.fullmatch(component_id) is None:
            raise ForestError(
                "RUNTIME_COMPONENT_ID_INVALID", "Runtime component identity is invalid", 422
            )
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT g.graph_id,g.graph_revision,m.package_id,m.package_digest,"
                        "m.trust_level FROM {} g LEFT JOIN {} m ON m.bot_id=g.bot_id "
                        "AND m.graph_id=g.graph_id AND m.component_id=%s "
                        "WHERE g.bot_id=%s AND g.published_at IS NOT NULL "
                        "ORDER BY g.graph_revision DESC LIMIT 1"
                    ).format(
                        self._table("runtime_graphs"),
                        self._table("runtime_graph_modules"),
                    ),
                    (component_id, self.store.bot_id),
                )
            ).fetchone()
        if row is None:
            return None
        if row["package_digest"] is None:
            raise ForestError(
                "RUNTIME_BOOTSTRAP_COMPONENT_MISSING",
                "Last published graph lacks the bootstrap component", 409,
            )
        return dict(row)

    async def request_graph_replacement(
        self,
        lease: HostLease,
        *,
        operation_id: str,
        expected_graph_revision: int,
        requested_by: str,
        target_component_id: str | None = None,
        selection_revision: int | None = None,
        selection_digest: str | None = None,
    ) -> dict:
        """Persist one exact graph or component reload intent."""

        return await self._request_graph_replacement(
            None, lease,
            operation_id=operation_id,
            expected_graph_revision=expected_graph_revision,
            requested_by=requested_by,
            target_component_id=target_component_id,
            selection_revision=selection_revision,
            selection_digest=selection_digest,
        )

    async def request_graph_replacement_in(
        self,
        conn,
        lease: HostLease,
        *,
        operation_id: str,
        expected_graph_revision: int,
        requested_by: str,
        target_component_id: str | None = None,
        selection_revision: int | None = None,
        selection_digest: str | None = None,
    ) -> dict:
        """Persist a replacement request in the caller's control transaction."""

        return await self._request_graph_replacement(
            conn, lease,
            operation_id=operation_id,
            expected_graph_revision=expected_graph_revision,
            requested_by=requested_by,
            target_component_id=target_component_id,
            selection_revision=selection_revision,
            selection_digest=selection_digest,
        )

    async def _request_graph_replacement(
        self,
        caller_conn,
        lease: HostLease,
        *,
        operation_id: str,
        expected_graph_revision: int,
        requested_by: str,
        target_component_id: str | None,
        selection_revision: int | None,
        selection_digest: str | None,
    ) -> dict:

        for value, field in ((operation_id, "operation_id"), (requested_by, "requested_by")):
            if not isinstance(value, str) or not 1 <= len(value.encode()) <= 256:
                raise ForestError("RUNTIME_REPLACEMENT_INPUT_INVALID", f"Invalid {field}", 422)
        if type(expected_graph_revision) is not int or expected_graph_revision <= 0:
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid expected graph revision", 422
            )
        if target_component_id is not None and (
            not isinstance(target_component_id, str)
            or _IDENTIFIER.fullmatch(target_component_id) is None
        ):
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid target component", 422
            )
        if (selection_revision is None) != (selection_digest is None) or (
            selection_revision is not None
            and (
                type(selection_revision) is not int
                or selection_revision < 0
                or target_component_id is None
                or not isinstance(selection_digest, str)
                or re.fullmatch(r"sha256:[a-f0-9]{64}", selection_digest) is None
            )
        ):
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid frozen component selection", 422
            )
        input_digest = digest_json(
            {
                "expected_graph_revision": expected_graph_revision,
                "requested_by": requested_by,
                "target_component_id": target_component_id,
                "selection_revision": selection_revision,
                "selection_digest": selection_digest,
            }
        )
        activation_id = str(
            uuid5(NAMESPACE_URL, f"forest:{self.store.bot_id}:reload:{operation_id}")
        )
        async with self._control_connection(caller_conn) as conn:
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.kernel.runtime/reload/{self.store.bot_id}/{operation_id}",),
            )
            replay = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("graph_replacement_requests")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "RUNTIME_REPLACEMENT_CONFLICT",
                        "Graph replacement operation input differs",
                        409,
                    )
                return dict(replay)
            head = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_head")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if head is None or head["graph_revision"] != expected_graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT",
                    "RuntimeGraph revision changed",
                    409,
                    details={"graph_revision": None if head is None else head["graph_revision"]},
                )
            if target_component_id is not None and selection_revision is None:
                target = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT 1 FROM {} WHERE bot_id=%s AND graph_id=%s "
                            "AND component_id=%s"
                        ).format(self._table("runtime_graph_modules")),
                        (self.store.bot_id, head["graph_id"], target_component_id),
                    )
                ).fetchone()
                if target is None:
                    raise ForestError(
                        "RUNTIME_MODULE_NOT_FOUND",
                        "Reload target is not in this RuntimeGraph",
                        404,
                    )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,operation_id,input_digest,requested_by,"
                        "expected_graph_revision,target_component_id,selection_revision,"
                        "selection_digest,activation_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "RETURNING *"
                    ).format(self._table("graph_replacement_requests")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        requested_by,
                        expected_graph_revision,
                        target_component_id,
                        selection_revision,
                        selection_digest,
                        activation_id,
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def cancel_graph_replacement_in(
        self,
        conn,
        lease: HostLease,
        *,
        operation_id: str,
        expected_graph_revision: int,
        cancel_operation_id: str,
        cancelled_by: str,
    ) -> dict:
        """Cancel an unclaimed replacement under the caller's control transaction."""

        for value, field in (
            (operation_id, "operation_id"),
            (cancel_operation_id, "cancel_operation_id"),
            (cancelled_by, "cancelled_by"),
        ):
            if not isinstance(value, str) or not 1 <= len(value.encode()) <= 256:
                raise ForestError("RUNTIME_REPLACEMENT_INPUT_INVALID", f"Invalid {field}", 422)
        if type(expected_graph_revision) is not int or expected_graph_revision <= 0:
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid graph revision", 422
            )
        await self._identity_in(conn)
        await self._host_in(conn, lease)
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.kernel.runtime/cancel/{self.store.bot_id}/{cancel_operation_id}",),
        )
        prior = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND cancel_operation_id=%s"
                ).format(self._table("graph_replacement_requests")),
                (self.store.bot_id, cancel_operation_id),
            )
        ).fetchone()
        if prior is not None:
            if (
                prior["operation_id"] == operation_id
                and prior["expected_graph_revision"] == expected_graph_revision
                and prior["cancelled_by"] == cancelled_by
                and prior["state"] == "CANCELLED"
            ):
                return dict(prior)
            raise ForestError(
                "RUNTIME_REPLACEMENT_CANCEL_CONFLICT",
                "Cancellation operation input differs",
                409,
            )
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s FOR UPDATE"
                ).format(self._table("graph_replacement_requests")),
                (self.store.bot_id, operation_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "RUNTIME_REPLACEMENT_NOT_FOUND", "Graph replacement does not exist", 404
            )
        if row["state"] != "QUEUED":
            raise ForestError(
                "RUNTIME_REPLACEMENT_CANCEL_CONFLICT",
                "Only an unclaimed graph replacement can be cancelled",
                409,
                details={"state": row["state"]},
            )
        head = await (
            await conn.execute(
                sql.SQL(
                    "SELECT graph_revision FROM {} WHERE bot_id=%s FOR SHARE"
                ).format(self._table("runtime_graph_head")),
                (self.store.bot_id,),
            )
        ).fetchone()
        if head is None or head["graph_revision"] != expected_graph_revision:
            raise ForestError(
                "RUNTIME_GRAPH_REVISION_CONFLICT",
                "RuntimeGraph revision changed",
                409,
                details={"graph_revision": None if head is None else head["graph_revision"]},
            )
        if row["expected_graph_revision"] != expected_graph_revision:
            raise ForestError(
                "RUNTIME_REPLACEMENT_CANCEL_CONFLICT",
                "Cancellation targets a different graph revision",
                409,
            )
        saved = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='CANCELLED',cancel_operation_id=%s,"
                    "cancelled_by=%s,updated_at=clock_timestamp() "
                    "WHERE bot_id=%s AND operation_id=%s RETURNING *"
                ).format(self._table("graph_replacement_requests")),
                (cancel_operation_id, cancelled_by, self.store.bot_id, operation_id),
            )
        ).fetchone()
        return dict(saved)

    async def claim_graph_replacement(
        self,
        lease: HostLease,
        *,
        lease_seconds: int = 120,
    ) -> dict | None:
        if type(lease_seconds) is not int or not 10 <= lease_seconds <= 600:
            raise ForestError("RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid replacement lease", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND (state='QUEUED' OR "
                        "(state='RUNNING' AND lease_until<=clock_timestamp())) "
                        "ORDER BY created_at,operation_id FOR UPDATE SKIP LOCKED LIMIT 1"
                    ).format(self._table("graph_replacement_requests")),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if row is None:
                return None
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='RUNNING',owner_host_id=%s,owner_incarnation=%s,"
                        "owner_epoch=%s,lease_until=clock_timestamp()+make_interval(secs => %s),"
                        "attempt_count=attempt_count+1,updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND operation_id=%s RETURNING *"
                    ).format(self._table("graph_replacement_requests")),
                    (
                        lease.host_id,
                        lease.incarnation,
                        lease.owner_epoch,
                        lease_seconds,
                        self.store.bot_id,
                        row["operation_id"],
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def finish_graph_replacement(
        self,
        lease: HostLease,
        *,
        operation_id: str,
        graph_id: str,
        graph_revision: int,
    ) -> dict:
        return await self._settle_graph_replacement(
            lease,
            operation_id=operation_id,
            state="SUCCEEDED",
            graph_id=graph_id,
            graph_revision=graph_revision,
        )

    async def fail_graph_replacement(
        self,
        lease: HostLease,
        *,
        operation_id: str,
        error_code: str,
    ) -> dict:
        if not isinstance(error_code, str) or not 1 <= len(error_code.encode()) <= 128:
            raise ForestError("RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid replacement error", 422)
        return await self._settle_graph_replacement(
            lease,
            operation_id=operation_id,
            state="FAILED",
            error_code=error_code,
        )

    async def _settle_graph_replacement(
        self,
        lease: HostLease,
        *,
        operation_id: str,
        state: str,
        graph_id: str | None = None,
        graph_revision: int | None = None,
        error_code: str | None = None,
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s FOR UPDATE"
                    ).format(self._table("graph_replacement_requests")),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "RUNTIME_REPLACEMENT_NOT_FOUND", "Graph replacement does not exist", 404
                )
            if row["state"] == state:
                expected = (
                    graph_id,
                    graph_revision,
                    error_code,
                )
                actual = (
                    row["result_graph_id"],
                    row["result_graph_revision"],
                    row["error_code"],
                )
                if actual != expected:
                    raise ForestError(
                        "RUNTIME_REPLACEMENT_CONFLICT",
                        "Graph replacement terminal result differs",
                        409,
                    )
                return dict(row)
            if (
                row["state"] != "RUNNING"
                or row["owner_host_id"] != lease.host_id
                or row["owner_incarnation"] != lease.incarnation
                or row["owner_epoch"] != lease.owner_epoch
            ):
                raise ForestError(
                    "STALE_RUNTIME_REPLACEMENT",
                    "Graph replacement ownership changed",
                    409,
                )
            if state == "SUCCEEDED":
                graph = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT graph_id,graph_revision,activation_id,state FROM {} "
                            "WHERE bot_id=%s AND graph_id=%s"
                        ).format(self._table("runtime_graphs")),
                        (self.store.bot_id, graph_id),
                    )
                ).fetchone()
                if (
                    graph is None
                    or graph["graph_revision"] != graph_revision
                    or graph["activation_id"] != row["activation_id"]
                    or graph["state"] not in {"ACTIVE", "RETIRED", "STOPPED"}
                ):
                    raise ForestError(
                        "RUNTIME_REPLACEMENT_RESULT_INVALID",
                        "Replacement graph evidence is invalid",
                        409,
                    )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,result_graph_id=%s,result_graph_revision=%s,"
                        "error_code=%s,lease_until=clock_timestamp(),updated_at=clock_timestamp() "
                        "WHERE bot_id=%s AND operation_id=%s RETURNING *"
                    ).format(self._table("graph_replacement_requests")),
                    (
                        state,
                        graph_id,
                        graph_revision,
                        error_code,
                        self.store.bot_id,
                        operation_id,
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def graph_replacement(self, operation_id: str) -> dict:
        if not isinstance(operation_id, str) or not 1 <= len(operation_id.encode()) <= 256:
            raise ForestError("RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid operation_id", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("graph_replacement_requests")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "RUNTIME_REPLACEMENT_NOT_FOUND", "Graph replacement does not exist", 404
                )
            return dict(row)

    async def graph_for_activation(self, activation_id: str) -> dict | None:
        try:
            UUID(activation_id)
        except (TypeError, ValueError):
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID", "Invalid activation_id", 422
            ) from None
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND activation_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, activation_id),
                )
            ).fetchone()
            return dict(row) if row is not None else None

    async def set_data_admission(
        self,
        lease: HostLease,
        *,
        expected_maintenance_epoch: int,
        data_admission: bool,
    ) -> dict:
        """Atomically pause or resume DATA while leaving CONTROL available."""

        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.set_data_admission_in(
                conn, lease,
                expected_maintenance_epoch=expected_maintenance_epoch,
                data_admission=data_admission,
            )

    async def set_data_admission_in(
        self,
        conn,
        lease: HostLease,
        *,
        expected_maintenance_epoch: int,
        data_admission: bool,
    ) -> dict:
        """Commit a maintenance CAS in the caller's control transaction."""

        if (
            type(expected_maintenance_epoch) is not int
            or expected_maintenance_epoch <= 0
            or type(data_admission) is not bool
        ):
            raise ForestError(
                "RUNTIME_MAINTENANCE_INPUT_INVALID",
                "Maintenance epoch or DATA admission is invalid",
                422,
            )
        await self._identity_in(conn)
        await self._host_in(conn, lease)
        current = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                    self._table("runtime_state")
                ),
                (self.store.bot_id,),
            )
        ).fetchone()
        if current is None:
            raise ForestError(
                "KERNEL_RUNTIME_NOT_INITIALIZED", "Runtime state is unavailable", 503
            )
        if current["maintenance_epoch"] != expected_maintenance_epoch:
            raise ForestError(
                "RUNTIME_MAINTENANCE_CONFLICT",
                "Maintenance intent changed",
                409,
            )
        if current["status"] != "ACTIVE" or not current["control_admission"]:
            raise ForestError(
                "RUNTIME_CONTROL_ADMISSION_CLOSED",
                "Runtime control admission is unavailable",
                409,
            )
        saved = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET maintenance_epoch=maintenance_epoch+1,"
                    "data_admission=%s WHERE bot_id=%s RETURNING *"
                ).format(self._table("runtime_state")),
                (data_admission, self.store.bot_id),
            )
        ).fetchone()
        return dict(saved)

    async def claim_host(
        self,
        host_id: str,
        incarnation: str,
        *,
        ttl: timedelta = timedelta(seconds=30),
    ) -> HostLease:
        seconds = ttl.total_seconds()
        if (
            not isinstance(host_id, str)
            or not host_id
            or len(host_id) > 256
            or not isinstance(incarnation, str)
            or not incarnation
            or len(incarnation) > 256
            or not 5 <= seconds <= 300
        ):
            raise ForestError("HOST_LEASE_INVALID", "Host lease identity or TTL is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT h.*,clock_timestamp() AS current_time FROM {} h "
                        "WHERE bot_id=%s FOR UPDATE"
                    ).format(self._table("host_lease")),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if row is None:
                raise ForestError("KERNEL_RUNTIME_NOT_INITIALIZED", "Host lease row is absent", 503)
            if row["incarnation"] == incarnation:
                if row["host_id"] != host_id or row["expires_at"] <= row["current_time"]:
                    raise ForestError(
                        "STALE_HOST", "A host incarnation cannot be changed or resurrected", 409
                    )
                saved = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET expires_at=clock_timestamp()+make_interval(secs => %s) "
                            "WHERE bot_id=%s RETURNING *"
                        ).format(self._table("host_lease")),
                        (seconds, self.store.bot_id),
                    )
                ).fetchone()
                return self._lease(saved)
            if row["incarnation"] is not None and row["expires_at"] > row["current_time"]:
                raise ForestError("HOST_LEASE_HELD", "Another host incarnation owns the Bot", 409)
            prior = await (
                await conn.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE bot_id=%s AND incarnation=%s").format(
                        self._table("host_incarnations")
                    ),
                    (self.store.bot_id, incarnation),
                )
            ).fetchone()
            if prior is not None:
                raise ForestError("STALE_HOST", "A retired host incarnation cannot be reused", 409)
            owner_epoch = row["owner_epoch"] + 1
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,incarnation,host_id,owner_epoch) VALUES (%s,%s,%s,%s)"
                ).format(self._table("host_incarnations")),
                (self.store.bot_id, incarnation, host_id, owner_epoch),
            )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET host_id=%s,incarnation=%s,owner_epoch=%s,"
                        "expires_at=clock_timestamp()+make_interval(secs => %s) "
                        "WHERE bot_id=%s RETURNING *"
                    ).format(self._table("host_lease")),
                    (host_id, incarnation, owner_epoch, seconds, self.store.bot_id),
                )
            ).fetchone()
            return self._lease(saved)

    async def release_host(self, lease: HostLease) -> None:
        """Relinquish one live host lease after its runtime graph is retired."""

        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET host_id=NULL,incarnation=NULL,"
                        "expires_at=clock_timestamp() WHERE bot_id=%s AND host_id=%s "
                        "AND incarnation=%s AND owner_epoch=%s RETURNING owner_epoch"
                    ).format(self._table("host_lease")),
                    (
                        self.store.bot_id,
                        lease.host_id,
                        lease.incarnation,
                        lease.owner_epoch,
                    ),
                )
            ).fetchone()
            if row is None:
                raise ForestError("STALE_HOST", "Bot host lease is no longer current", 409)

    async def verify_active_generation(
        self,
        *,
        component_id: str,
        generation_id: str,
        admission_lane: str = "DATA",
    ) -> ActiveGenerationRoute:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.verify_active_generation_in(
                conn,
                component_id=component_id,
                generation_id=generation_id,
                admission_lane=admission_lane,
            )

    async def verify_generation_member(
        self,
        *,
        component_id: str,
        generation_id: str,
    ) -> RuntimeGenerationGraph:
        """Verify owned draining work against the current graph membership."""

        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.verify_generation_member_in(
                conn,
                component_id=component_id,
                generation_id=generation_id,
            )

    async def verify_active_generation_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
        admission_lane: str = "DATA",
    ) -> ActiveGenerationRoute:
        """Fence a business write against the current graph and exact route."""

        if (
            not isinstance(component_id, str)
            or _IDENTIFIER.fullmatch(component_id) is None
            or not isinstance(generation_id, str)
            or not generation_id
            or len(generation_id) > 512
            or admission_lane not in {"DATA", "CONTROL"}
        ):
            raise ForestError(
                "RUNTIME_GENERATION_FENCE_INVALID",
                "Generation fence identity or admission lane is invalid",
                422,
            )
        await self._identity_in(conn)
        runtime = await self._runtime_state_in(conn, lock=True)
        head = await self._graph_head_in(conn, lock=True)
        graph = None
        member = None
        generation = None
        route = None
        if head is not None and head["graph_id"] is not None:
            graph = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT graph_id,state FROM {} WHERE bot_id=%s AND graph_id=%s FOR SHARE"
                    ).format(self._table("runtime_graphs")),
                    (self.store.bot_id, head["graph_id"]),
                )
            ).fetchone()
            member = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT component_id,generation_id FROM {} WHERE bot_id=%s "
                        "AND graph_id=%s AND component_id=%s AND generation_id=%s FOR SHARE"
                    ).format(self._table("runtime_graph_modules")),
                    (self.store.bot_id, head["graph_id"], component_id, generation_id),
                )
            ).fetchone()
            generation = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT component_id,generation_id,status FROM {} WHERE bot_id=%s "
                        "AND component_id=%s AND generation_id=%s FOR SHARE"
                    ).format(self._table("generations")),
                    (self.store.bot_id, component_id, generation_id),
                )
            ).fetchone()
            route = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT component_id,generation_id,route_epoch,data_admission,"
                        "control_admission FROM {} WHERE bot_id=%s AND component_id=%s "
                        "AND generation_id=%s FOR SHARE"
                    ).format(self._table("routes")),
                    (self.store.bot_id, component_id, generation_id),
                )
            ).fetchone()
        lane_open = (
            route is not None
            and runtime is not None
            and (
                route["data_admission"] and runtime["data_admission"]
                if admission_lane == "DATA"
                else route["control_admission"] and runtime["control_admission"]
            )
        )
        if (
            runtime is None
            or head is None
            or graph is None
            or member is None
            or generation is None
            or route is None
            or runtime["status"] != "ACTIVE"
            or generation["status"] != "ACTIVE"
            or graph["state"] != "ACTIVE"
            or not lane_open
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "The exact Generation is not the active admitted route",
                409,
                details={
                    "component_id": component_id,
                    "generation_id": generation_id,
                    "admission_lane": admission_lane,
                },
            )
        return ActiveGenerationRoute(
            component_id=route["component_id"],
            generation_id=route["generation_id"],
            route_epoch=route["route_epoch"],
            admission_lane=admission_lane,
            graph_id=head["graph_id"],
            graph_revision=head["graph_revision"],
        )

    async def verify_active_route_in(
        self, conn, *, component_id: str, generation_id: str, admission_lane: str
    ) -> ActiveGenerationRoute:
        return await self.verify_active_generation_in(
            conn, component_id=component_id, generation_id=generation_id,
            admission_lane=admission_lane,
        )

    async def verify_generation_member_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
    ) -> RuntimeGenerationGraph:
        """Fence already-owned work to the current graph while admissions are frozen."""

        return await self._verify_generation_member_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
            lock=True,
        )

    async def verify_generation_graph_fence_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
        graph_id: str,
        graph_revision: int,
    ) -> RuntimeGenerationGraph:
        """Accept an origin Graph fence only while its exact Generation survives."""

        if (
            not isinstance(graph_id, str)
            or not 1 <= len(graph_id) <= 512
            or type(graph_revision) is not int
            or graph_revision <= 0
        ):
            raise ForestError(
                "RUNTIME_GENERATION_FENCE_INVALID",
                "Generation Graph fence identity is invalid",
                422,
            )
        current = await self.verify_generation_member_in(
            conn, component_id=component_id, generation_id=generation_id
        )
        if current.graph_id == graph_id and current.graph_revision == graph_revision:
            return current
        origin = await (
            await conn.execute(
                sql.SQL(
                    "SELECT graph.graph_id FROM {} graph JOIN {} member "
                    "ON member.bot_id=graph.bot_id AND member.graph_id=graph.graph_id "
                    "JOIN {} lifecycle ON lifecycle.bot_id=member.bot_id "
                    "AND lifecycle.graph_id=member.graph_id "
                    "AND lifecycle.component_id=member.component_id "
                    "AND lifecycle.state='ACTIVE' "
                    "WHERE graph.bot_id=%s AND graph.graph_id=%s "
                    "AND graph.graph_revision=%s AND graph.graph_revision<%s "
                    "AND graph.state IN ('RETIRED','FAILED','STOPPED') "
                    "AND member.component_id=%s AND member.generation_id=%s "
                    "FOR SHARE OF graph,member,lifecycle"
                ).format(
                    self._table("runtime_graphs"),
                    self._table("runtime_graph_modules"),
                    self._table("runtime_graph_lifecycle_facts"),
                ),
                (
                    self.store.bot_id,
                    graph_id,
                    graph_revision,
                    current.graph_revision,
                    component_id,
                    generation_id,
                ),
            )
        ).fetchone()
        if origin is None:
            raise ForestError(
                "RUNTIME_GENERATION_GRAPH_FENCE_STALE",
                "Origin Graph does not own the currently retained Generation",
                409,
            )
        return current

    async def _host_owned_generation_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
    ):
        if (
            not isinstance(component_id, str)
            or _IDENTIFIER.fullmatch(component_id) is None
            or not isinstance(generation_id, str)
            or not generation_id
            or len(generation_id) > 512
        ):
            raise ForestError(
                "RUNTIME_GENERATION_FENCE_INVALID",
                "Generation fence identity is invalid",
                422,
            )
        await self._identity_in(conn)
        runtime = await self._runtime_state_in(conn, lock=True)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT graph.graph_id,graph.graph_revision,graph.graph_digest,"
                    "graph.activation_id,graph.state,generation.status "
                    "FROM {} member JOIN {} graph ON graph.bot_id=member.bot_id "
                    "AND graph.graph_id=member.graph_id JOIN {} generation "
                    "ON generation.bot_id=member.bot_id "
                    "AND generation.component_id=member.component_id "
                    "AND generation.generation_id=member.generation_id "
                    "JOIN {} host ON host.bot_id=generation.bot_id "
                    "AND host.incarnation=generation.host_incarnation "
                    "AND host.owner_epoch=generation.host_owner_epoch "
                    "WHERE member.bot_id=%s AND member.component_id=%s "
                    "AND member.generation_id=%s AND host.host_id IS NOT NULL "
                    "AND host.expires_at>clock_timestamp() "
                    "FOR SHARE OF member,graph,generation,host"
                ).format(
                    self._table("runtime_graph_modules"),
                    self._table("runtime_graphs"),
                    self._table("generations"),
                    self._table("host_lease"),
                ),
                (self.store.bot_id, component_id, generation_id),
            )
        ).fetchone()
        graph = None
        if row is not None:
            members = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT component_id,generation_id FROM {} WHERE bot_id=%s "
                        "AND graph_id=%s ORDER BY component_id FOR SHARE"
                    ).format(self._table("runtime_graph_modules")),
                    (self.store.bot_id, row["graph_id"]),
                )
            ).fetchall()
            graph = RuntimeGenerationGraph(
                graph_id=row["graph_id"],
                graph_revision=row["graph_revision"],
                graph_digest=row["graph_digest"],
                activation_id=row["activation_id"],
                generations={
                    item["component_id"]: item["generation_id"] for item in members
                },
            )
        return runtime, row, graph

    async def verify_prepared_generation_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
    ) -> RuntimeGenerationGraph:
        """Fence a package preparation write to one live host-owned candidate graph."""

        runtime, row, graph = await self._host_owned_generation_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
        )
        if (
            runtime is None
            or runtime["status"] != "ACTIVE"
            or row is None
            or row["state"] != "PREPARED"
            or row["status"] != "READY"
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_PREPARED",
                "The Generation is not a live prepared RuntimeGraph member",
                409,
                details={
                    "component_id": component_id,
                    "generation_id": generation_id,
                },
            )
        return graph

    async def verify_generation_cleanup_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
    ) -> RuntimeGenerationGraph:
        """Fence cleanup to a live host-owned Generation with closed admission."""

        runtime, row, graph = await self._host_owned_generation_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
        )
        route = await (
            await conn.execute(
                sql.SQL(
                    "SELECT generation_id,data_admission,control_admission FROM {} "
                    "WHERE bot_id=%s AND component_id=%s FOR UPDATE"
                ).format(self._table("routes")),
                (self.store.bot_id, component_id),
            )
        ).fetchone()
        admitted = (
            route is not None
            and route["generation_id"] == generation_id
            and (route["data_admission"] or route["control_admission"])
        )
        if (
            runtime is None
            or runtime["status"] != "ACTIVE"
            or row is None
            or row["status"] == "PREPARING"
            or admitted
        ):
            raise ForestError(
                "RUNTIME_GENERATION_CLEANUP_FORBIDDEN",
                "Generation cleanup requires current host ownership and closed admission",
                409,
                details={
                    "component_id": component_id,
                    "generation_id": generation_id,
                },
            )
        return graph

    async def verify_generation_member_snapshot_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
    ) -> RuntimeGenerationGraph:
        """Verify membership in a caller-owned repeatable-read, read-only snapshot."""

        return await self._verify_generation_member_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
            lock=False,
        )

    async def generation_capability_grants_digest_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
    ) -> str:
        """Return the immutable grant-set digest sealed into one current Generation."""

        await self.verify_generation_member_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
        )
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT capability_grants_digest FROM {} WHERE bot_id=%s "
                    "AND component_id=%s AND generation_id=%s"
                ).format(self._table("generations")),
                (self.store.bot_id, component_id, generation_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_CURRENT",
                "The Generation is not a current RuntimeGraph member",
                409,
            )
        return row["capability_grants_digest"]

    async def _verify_generation_member_in(
        self,
        conn,
        *,
        component_id: str,
        generation_id: str,
        lock: bool,
    ) -> RuntimeGenerationGraph:

        if (
            not isinstance(component_id, str)
            or _IDENTIFIER.fullmatch(component_id) is None
            or not isinstance(generation_id, str)
            or not generation_id
            or len(generation_id) > 512
        ):
            raise ForestError(
                "RUNTIME_GENERATION_FENCE_INVALID",
                "Generation member fence identity is invalid",
                422,
            )
        await self._identity_in(conn, lock=lock)
        runtime = await self._runtime_state_in(conn, lock=lock)
        head = await self._graph_head_in(conn, lock=lock)
        graph = None
        member = None
        generation = None
        lock_clause = sql.SQL(" FOR SHARE" if lock else "")
        if head is not None and head["graph_id"] is not None:
            graph = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT graph_id,graph_digest,activation_id,state FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s"
                    ).format(self._table("runtime_graphs"))
                    + lock_clause,
                    (self.store.bot_id, head["graph_id"]),
                )
            ).fetchone()
            member = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT component_id,generation_id FROM {} WHERE bot_id=%s "
                        "AND graph_id=%s AND component_id=%s AND generation_id=%s"
                    ).format(self._table("runtime_graph_modules"))
                    + lock_clause,
                    (self.store.bot_id, head["graph_id"], component_id, generation_id),
                )
            ).fetchone()
            generation = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT component_id,generation_id,status FROM {} WHERE bot_id=%s "
                        "AND component_id=%s AND generation_id=%s"
                    ).format(self._table("generations"))
                    + lock_clause,
                    (self.store.bot_id, component_id, generation_id),
                )
            ).fetchone()
        if (
            runtime is None
            or head is None
            or graph is None
            or member is None
            or generation is None
            or runtime["status"] != "ACTIVE"
            or generation["status"] != "ACTIVE"
            or graph["state"] != "ACTIVE"
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_CURRENT",
                "The Generation is not a current RuntimeGraph member",
                409,
                details={
                    "component_id": component_id,
                    "generation_id": generation_id,
                },
            )
        members = await self._members_for_id_in(conn, head["graph_id"], lock=False)
        return RuntimeGenerationGraph(
            graph_id=head["graph_id"],
            graph_revision=head["graph_revision"],
            graph_digest=graph["graph_digest"],
            activation_id=graph["activation_id"],
            generations={item["component_id"]: item["generation_id"] for item in members},
        )

    async def prepare_graph(
        self,
        lease: HostLease,
        bindings: tuple[PreparedGenerationBinding, ...] | list[PreparedGenerationBinding],
        activation_id: str,
        *,
        retained_generations: dict[str, str] | None = None,
        source_graph: RuntimeGenerationGraph | None = None,
    ) -> RuntimeGenerationGraph:
        if retained_generations is not None and not isinstance(
            retained_generations, dict
        ):
            raise ForestError(
                "RUNTIME_RETAINED_GENERATION_INVALID",
                "Retained member identities must be a component-to-Generation mapping",
                422,
            )
        retained = dict(retained_generations or {})
        if (
            (bool(retained) != (source_graph is not None))
            or (source_graph is not None and not isinstance(source_graph, RuntimeGenerationGraph))
            or any(
                not isinstance(component_id, str)
                or _IDENTIFIER.fullmatch(component_id) is None
                or not isinstance(generation_id, str)
                or not generation_id
                or len(generation_id) > 512
                for component_id, generation_id in retained.items()
            )
        ):
            raise ForestError(
                "RUNTIME_RETAINED_GENERATION_INVALID",
                "Retained members require exact source Graph and Generation identities",
                422,
            )
        ordered = self._validate_bindings(
            bindings, activation_id, available_components=set(retained)
        )
        if set(retained) & {item.component_id for item in ordered}:
            raise ForestError(
                "RUNTIME_RETAINED_GENERATION_INVALID",
                "A component cannot be retained and prepared again in one graph",
                422,
            )
        graph_digest = digest_json(
            {
                "protocol": "forest.runtime-graph/1",
                "members": [item.as_fact() for item in ordered],
                **(
                    {
                        "retained_from": source_graph.graph_id,
                        "retained": sorted(retained.items()),
                    }
                    if retained
                    else {}
                ),
            }
        )
        generations = {
            item.component_id: (f"generation:{activation_id}:{digest_json(item.as_fact())[-24:]}")
            for item in ordered
        }
        generations.update(retained)
        graph_id = f"runtime-graph:{activation_id}"
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            clock = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_clock")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if clock is None:
                raise ForestError(
                    "KERNEL_RUNTIME_NOT_INITIALIZED", "Runtime graph clock is absent", 503
                )
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND activation_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, activation_id),
                )
            ).fetchone()
            if existing is not None:
                await self._guard_graph_replay_in(
                    conn, lease, existing, graph_id, graph_digest, generations
                )
                return RuntimeGenerationGraph(
                    graph_id,
                    existing["graph_revision"],
                    graph_digest,
                    activation_id,
                    generations,
                )
            if retained:
                head = await self._graph_head_in(conn, lock=True)
                await self._graph_in(conn, source_graph, lock=True)
                if head is None or head["graph_id"] != source_graph.graph_id:
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_STALE",
                        "Retained Generations must belong to the current Graph",
                        409,
                    )
                source_members = {
                    item["component_id"]: item
                    for item in await self._members_in(conn, source_graph, lock=True)
                }
                if any(
                    component_id not in source_members
                    or source_members[component_id]["generation_id"] != generation_id
                    or source_members[component_id]["status"] != "ACTIVE"
                    or source_members[component_id]["host_incarnation"] != lease.incarnation
                    or source_members[component_id]["host_owner_epoch"] != lease.owner_epoch
                    for component_id, generation_id in retained.items()
                ):
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_STALE",
                        "Retained Generation is not active under this Host lease",
                        409,
                    )
                dependencies = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT component_id,required_component_id FROM {} "
                            "WHERE bot_id=%s AND graph_id=%s UNION "
                            "SELECT component_id,required_component_id FROM {} "
                            "WHERE bot_id=%s AND graph_id=%s"
                        ).format(
                            self._table("runtime_graph_dependencies"),
                            self._table("runtime_graph_retention_dependencies"),
                        ),
                        (
                            self.store.bot_id,
                            source_graph.graph_id,
                            self.store.bot_id,
                            source_graph.graph_id,
                        ),
                    )
                ).fetchall()
                retained_dependencies = [
                    item for item in dependencies if item["component_id"] in retained
                ]
                if any(
                    item["required_component_id"] not in retained
                    for item in retained_dependencies
                ):
                    raise ForestError(
                        "RUNTIME_RETAINED_DEPENDENCY_CHANGED",
                        "A retained Generation cannot keep a route to a replaced member",
                        409,
                    )
            revision = clock["next_revision"]
            await conn.execute(
                sql.SQL("UPDATE {} SET next_revision=next_revision+1 WHERE bot_id=%s").format(
                    self._table("runtime_graph_clock")
                ),
                (self.store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,graph_revision,graph_digest,"
                    "activation_id,source_graph_id,state) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'PREPARED')"
                ).format(self._table("runtime_graphs")),
                (
                    self.store.bot_id,
                    graph_id,
                    revision,
                    graph_digest,
                    activation_id,
                    source_graph.graph_id if source_graph is not None else None,
                ),
            )
            for item in ordered:
                await self._storage_in(conn, item)
                generation_id = generations[item.component_id]
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,generation_id,component_id,package_digest,"
                        "config_revision,config_payload_id,config_payload_digest,instance_id,"
                        "worker_incarnation,host_incarnation,host_owner_epoch,state_schema,"
                        "state_contract_digest,capability_grants_digest,status) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'READY')"
                    ).format(self._table("generations")),
                    (
                        self.store.bot_id,
                        generation_id,
                        item.component_id,
                        item.package_digest,
                        item.config_revision,
                        item.config_payload_id,
                        item.config_payload_digest,
                        item.instance_id,
                        item.worker_incarnation,
                        lease.incarnation,
                        lease.owner_epoch,
                        item.state_schema,
                        item.state_contract_digest,
                        item.capability_grants_digest,
                    ),
                )
                storage = item.storage
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,graph_id,component_id,generation_id,package_id,"
                        "package_version,package_digest,driver,trust_level,descriptor_digest,"
                        "route_contract_digest,management_contract_digest,management_ui_digest,"
                        "data_admission,control_admission,storage_schema_id,storage_schema_version,"
                        "storage_namespace,storage_baseline_sha256,storage_package_digest,"
                        "storage_repository_contract,storage_execution_profile) VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("runtime_graph_modules")),
                    (
                        self.store.bot_id,
                        graph_id,
                        item.component_id,
                        generation_id,
                        item.package_id,
                        item.package_version,
                        item.package_digest,
                        item.driver,
                        item.trust_level,
                        item.descriptor_digest,
                        item.route_contract_digest,
                        item.management_contract_digest,
                        item.management_ui_digest,
                        item.data_admission,
                        item.control_admission,
                        storage.schema_id if storage else None,
                        storage.schema_version if storage else None,
                        storage.namespace if storage else None,
                        storage.baseline_sha256 if storage else None,
                        storage.package_digest if storage else None,
                        storage.repository_contract if storage else None,
                        storage.execution_profile if storage else None,
                    ),
                )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,graph_id,component_id,sequence,state) "
                        "VALUES (%s,%s,%s,1,'PREPARED')"
                    ).format(self._table("runtime_graph_lifecycle_facts")),
                    (self.store.bot_id, graph_id, item.component_id),
                )
            for component_id, generation_id in sorted(retained.items()):
                copied = await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,graph_id,component_id,generation_id,"
                        "package_id,package_version,package_digest,driver,trust_level,"
                        "descriptor_digest,route_contract_digest,management_contract_digest,"
                        "management_ui_digest,data_admission,control_admission,"
                        "storage_schema_id,storage_schema_version,storage_namespace,"
                        "storage_baseline_sha256,storage_package_digest,"
                        "storage_repository_contract,storage_execution_profile) "
                        "SELECT bot_id,%s,component_id,generation_id,package_id,"
                        "package_version,package_digest,driver,trust_level,"
                        "descriptor_digest,route_contract_digest,management_contract_digest,"
                        "management_ui_digest,data_admission,control_admission,"
                        "storage_schema_id,storage_schema_version,storage_namespace,"
                        "storage_baseline_sha256,storage_package_digest,"
                        "storage_repository_contract,storage_execution_profile "
                        "FROM {} WHERE bot_id=%s AND graph_id=%s AND component_id=%s "
                        "AND generation_id=%s"
                    ).format(
                        self._table("runtime_graph_modules"),
                        self._table("runtime_graph_modules"),
                    ),
                    (
                        graph_id,
                        self.store.bot_id,
                        source_graph.graph_id,
                        component_id,
                        generation_id,
                    ),
                )
                if copied.rowcount != 1:
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_STALE",
                        "Retained Graph member changed during preparation",
                        409,
                    )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,graph_id,component_id,sequence,state) "
                        "VALUES (%s,%s,%s,1,'PREPARED')"
                    ).format(self._table("runtime_graph_lifecycle_facts")),
                    (self.store.bot_id, graph_id, component_id),
                )
            for item in ordered:
                for required in item.requires:
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,graph_id,component_id,required_component_id) "
                            "VALUES (%s,%s,%s,%s)"
                        ).format(self._table("runtime_graph_dependencies")),
                        (self.store.bot_id, graph_id, item.component_id, required),
                    )
                for required in item.retention_requires:
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,graph_id,component_id,required_component_id) "
                            "VALUES (%s,%s,%s,%s)"
                        ).format(self._table("runtime_graph_retention_dependencies")),
                        (self.store.bot_id, graph_id, item.component_id, required),
                    )
            for dependency in retained_dependencies if retained else ():
                for table in (
                    "runtime_graph_dependencies",
                    "runtime_graph_retention_dependencies",
                ):
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,graph_id,component_id,required_component_id) "
                            "SELECT bot_id,%s,component_id,required_component_id FROM {} "
                            "WHERE bot_id=%s AND graph_id=%s AND component_id=%s "
                            "AND required_component_id=%s"
                        ).format(self._table(table), self._table(table)),
                        (
                            graph_id,
                            self.store.bot_id,
                            source_graph.graph_id,
                            dependency["component_id"],
                            dependency["required_component_id"],
                        ),
                    )
        return RuntimeGenerationGraph(graph_id, revision, graph_digest, activation_id, generations)

    async def publish_graph(
        self,
        lease: HostLease,
        graph: RuntimeGenerationGraph,
        *,
        configuration_activators: dict[str, object] | None = None,
        publication_participants: dict[str, object] | None = None,
    ) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            head = await self._graph_head_in(conn, lock=True, update=True)
            saved = await self._graph_in(conn, graph, lock=True)
            if head is not None and head["graph_id"] == graph.graph_id:
                if saved["state"] != "ACTIVE":
                    raise ForestError(
                        "RUNTIME_GRAPH_IDENTITY_CONFLICT",
                        "The published head is not an active graph",
                        409,
                    )
                return head["graph_revision"]
            if head is not None and head["graph_revision"] >= graph.graph_revision:
                raise ForestError(
                    "RUNTIME_GRAPH_STALE", "A newer runtime publication already exists", 409
                )
            if (
                saved["source_graph_id"] is not None
                and (head is None or head["graph_id"] != saved["source_graph_id"])
            ):
                raise ForestError(
                    "RUNTIME_RETAINED_GENERATION_STALE",
                    "The source Graph changed before retained Generations could publish",
                    409,
                )
            members = await self._members_in(conn, graph, lock=True)
            old_members = (
                await self._members_for_id_in(conn, head["graph_id"], lock=True)
                if head is not None and head["graph_id"] is not None
                else ()
            )
            old_by_component = {item["component_id"]: item for item in old_members}
            retained = {
                item["component_id"]
                for item in members
                if item["component_id"] in old_by_component
                and old_by_component[item["component_id"]]["generation_id"]
                == item["generation_id"]
            }
            if any(
                item["host_incarnation"] != lease.incarnation
                or item["host_owner_epoch"] != lease.owner_epoch
                or item["status"]
                != ("ACTIVE" if item["component_id"] in retained else "READY")
                for item in members
            ):
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_READY",
                    "New Generations must be READY and retained Generations ACTIVE under this Host",
                    409,
                )
            if configuration_activators is not None:
                components = {
                    item["component_id"] for item in members
                } - retained
                if set(configuration_activators) != components:
                    raise ForestError(
                        "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID",
                        "Every new Generation must provide exactly one configuration activator",
                        503,
                    )
                for item in members:
                    if item["component_id"] in retained:
                        continue
                    activator = configuration_activators[item["component_id"]]
                    activate = getattr(activator, "activate_in", None)
                    if activate is None:
                        raise ForestError(
                            "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID",
                            "Configuration participant does not expose activate_in",
                            503,
                        )
                    await activate(
                        conn,
                        operation_id=(f"runtime-publish:{graph.graph_id}:{item['component_id']}"),
                        revision=item["config_revision"],
                        generation_id=item["generation_id"],
                        expected_payload_id=item["config_payload_id"],
                        expected_payload_digest=item["config_payload_digest"],
                    )
            if publication_participants is not None:
                components = {
                    item["component_id"] for item in members
                } - retained
                if set(publication_participants) - components:
                    raise ForestError(
                        "RUNTIME_PUBLICATION_PARTICIPANTS_INVALID",
                        "Runtime publication participant is not a graph member",
                        503,
                    )
                members_by_component = {item["component_id"]: item for item in members}
                for component_id in sorted(publication_participants):
                    participant = publication_participants[component_id]
                    publish = getattr(participant, "publish_generation_in", None)
                    if publish is None:
                        raise ForestError(
                            "RUNTIME_PUBLICATION_PARTICIPANTS_INVALID",
                            "Runtime publication participant has no transaction port",
                            503,
                        )
                    item = members_by_component[component_id]
                    await publish(
                        conn,
                        generation_id=item["generation_id"],
                        graph_id=graph.graph_id,
                    )
            old_components: set[str] = set()
            if head is not None and head["graph_id"] is not None:
                old_components = {item["component_id"] for item in old_members}
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='RETIRED' WHERE bot_id=%s AND graph_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, head["graph_id"]),
                )
                for item in old_members:
                    if item["status"] == "ACTIVE" and item["component_id"] not in retained:
                        await conn.execute(
                            sql.SQL(
                                "UPDATE {} SET status='QUIESCING' "
                                "WHERE bot_id=%s AND generation_id=%s"
                            ).format(self._table("generations")),
                            (self.store.bot_id, item["generation_id"]),
                        )
                        await self._append_lifecycle_in(
                            conn, head["graph_id"], item["component_id"], "QUIESCING"
                        )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='ACTIVE',published_at=COALESCE("
                        "published_at,clock_timestamp()) WHERE bot_id=%s AND graph_id=%s").format(
                    self._table("runtime_graphs")
                ),
                (self.store.bot_id, graph.graph_id),
            )
            new_components = set()
            for item in members:
                new_components.add(item["component_id"])
                if item["component_id"] not in retained:
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET status='ACTIVE' WHERE bot_id=%s AND generation_id=%s"
                        ).format(self._table("generations")),
                        (self.store.bot_id, item["generation_id"]),
                    )
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,component_id,generation_id,route_epoch,"
                            "data_admission,control_admission) VALUES (%s,%s,%s,1,%s,%s) "
                            "ON CONFLICT (bot_id,component_id) DO UPDATE SET "
                            "generation_id=EXCLUDED.generation_id,route_epoch=routes.route_epoch+1,"
                            "data_admission=EXCLUDED.data_admission,"
                            "control_admission=EXCLUDED.control_admission"
                        ).format(self._table("routes")),
                        (
                            self.store.bot_id,
                            item["component_id"],
                            item["generation_id"],
                            item["data_admission"],
                            item["control_admission"],
                        ),
                    )
                await self._append_lifecycle_in(
                    conn, graph.graph_id, item["component_id"], "ACTIVE"
                )
            for component_id in old_components - new_components:
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET generation_id=NULL,route_epoch=route_epoch+1,"
                        "data_admission=false,control_admission=false "
                        "WHERE bot_id=%s AND component_id=%s"
                    ).format(self._table("routes")),
                    (self.store.bot_id, component_id),
                )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,graph_id,graph_revision) VALUES (%s,%s,%s) "
                    "ON CONFLICT (bot_id) DO UPDATE SET graph_id=EXCLUDED.graph_id,"
                    "graph_revision=EXCLUDED.graph_revision,published_at=clock_timestamp()"
                ).format(self._table("runtime_graph_head")),
                (self.store.bot_id, graph.graph_id, graph.graph_revision),
            )
            return graph.graph_revision

    async def freeze_graph_admission(
        self,
        lease: HostLease,
        graph: RuntimeGenerationGraph,
        *,
        components: tuple[str, ...] | None = None,
    ) -> None:
        """Stop new route admission for a dependency-closed set of current members."""

        selected = self._admission_selection(components)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            head = await self._graph_head_in(conn, lock=True, update=True)
            await self._graph_in(conn, graph, lock=True)
            if head is None or head["graph_id"] != graph.graph_id:
                raise ForestError(
                    "RUNTIME_GRAPH_NOT_CURRENT",
                    "Only the current RuntimeGraph can freeze admission",
                    409,
                )
            members = await self._members_in(conn, graph, lock=True)
            self._validate_admission_selection(selected, members)
            if selected is not None:
                dependencies = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT component_id,required_component_id FROM {} "
                            "WHERE bot_id=%s AND graph_id=%s UNION "
                            "SELECT component_id,required_component_id FROM {} "
                            "WHERE bot_id=%s AND graph_id=%s"
                        ).format(
                            self._table("runtime_graph_dependencies"),
                            self._table("runtime_graph_retention_dependencies"),
                        ),
                        (self.store.bot_id, graph.graph_id, self.store.bot_id, graph.graph_id),
                    )
                ).fetchall()
                if any(
                    item["required_component_id"] in selected
                    and item["component_id"] not in selected
                    for item in dependencies
                ):
                    raise ForestError(
                        "RUNTIME_GRAPH_MEMBER_SELECTION_UNSAFE",
                        "Selected members omit a Generation that depends on them",
                        409,
                    )
            for item in members:
                if selected is not None and item["component_id"] not in selected:
                    continue
                cursor = await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET data_admission=false,control_admission=false,"
                        "route_epoch=route_epoch+1 WHERE bot_id=%s AND component_id=%s "
                        "AND generation_id=%s"
                    ).format(self._table("routes")),
                    (self.store.bot_id, item["component_id"], item["generation_id"]),
                )
                if cursor.rowcount != 1:
                    raise ForestError(
                        "RUNTIME_GRAPH_ROUTE_CONFLICT",
                        "RuntimeGraph route changed while admission was frozen",
                        409,
                    )

    async def resume_graph_admission(
        self,
        lease: HostLease,
        graph: RuntimeGenerationGraph,
        *,
        components: tuple[str, ...] | None = None,
    ) -> None:
        """Reopen selected members if the source Graph remains current."""

        selected = self._admission_selection(components)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            head = await self._graph_head_in(conn, lock=True, update=True)
            saved = await self._graph_in(conn, graph, lock=True)
            if head is None or head["graph_id"] != graph.graph_id or saved["state"] != "ACTIVE":
                raise ForestError(
                    "RUNTIME_GRAPH_NOT_CURRENT",
                    "Only the current active RuntimeGraph can resume admission",
                    409,
                )
            members = await self._members_in(conn, graph, lock=True)
            self._validate_admission_selection(selected, members)
            if any(
                item["status"] != "ACTIVE"
                for item in members
                if selected is None or item["component_id"] in selected
            ):
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_READY",
                    "A graph with retired Generations cannot resume admission",
                    409,
                )
            for item in members:
                if selected is not None and item["component_id"] not in selected:
                    continue
                cursor = await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET data_admission=%s,control_admission=%s,"
                        "route_epoch=route_epoch+1 WHERE bot_id=%s AND component_id=%s "
                        "AND generation_id=%s"
                    ).format(self._table("routes")),
                    (
                        item["data_admission"],
                        item["control_admission"],
                        self.store.bot_id,
                        item["component_id"],
                        item["generation_id"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ForestError(
                        "RUNTIME_GRAPH_ROUTE_CONFLICT",
                        "RuntimeGraph route changed while admission was frozen",
                        409,
                    )

    @staticmethod
    def _admission_selection(components: tuple[str, ...] | None) -> set[str] | None:
        if components is None:
            return None
        if (
            not isinstance(components, tuple)
            or not components
            or any(
                not isinstance(item, str) or _IDENTIFIER.fullmatch(item) is None
                for item in components
            )
            or len(components) != len(set(components))
        ):
            raise ForestError(
                "RUNTIME_GRAPH_MEMBER_SELECTION_INVALID",
                "Admission selection must name distinct components",
                422,
            )
        return set(components)

    @staticmethod
    def _validate_admission_selection(selected: set[str] | None, members) -> None:
        if selected is not None and selected - {item["component_id"] for item in members}:
            raise ForestError(
                "RUNTIME_GRAPH_MEMBER_SELECTION_INVALID",
                "Admission selection contains a component outside this RuntimeGraph",
                422,
            )

    async def retire_graph(self, lease: HostLease, graph: RuntimeGenerationGraph) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            head = await self._graph_head_in(conn, lock=True, update=True)
            await self._graph_in(conn, graph, lock=True)
            if head is None or head["graph_id"] != graph.graph_id:
                return head["graph_revision"] if head is not None else 0
            clock = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("runtime_graph_clock")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            revision = clock["next_revision"]
            await conn.execute(
                sql.SQL("UPDATE {} SET next_revision=next_revision+1 WHERE bot_id=%s").format(
                    self._table("runtime_graph_clock")
                ),
                (self.store.bot_id,),
            )
            members = await self._members_in(conn, graph, lock=True)
            for item in members:
                if item["status"] == "ACTIVE":
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET status='QUIESCING' WHERE bot_id=%s AND generation_id=%s"
                        ).format(self._table("generations")),
                        (self.store.bot_id, item["generation_id"]),
                    )
                    await self._append_lifecycle_in(
                        conn, graph.graph_id, item["component_id"], "QUIESCING"
                    )
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET generation_id=NULL,route_epoch=route_epoch+1,"
                        "data_admission=false,control_admission=false "
                        "WHERE bot_id=%s AND component_id=%s AND generation_id=%s"
                    ).format(self._table("routes")),
                    (self.store.bot_id, item["component_id"], item["generation_id"]),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET state='RETIRED' WHERE bot_id=%s AND graph_id=%s").format(
                    self._table("runtime_graphs")
                ),
                (self.store.bot_id, graph.graph_id),
            )
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET graph_id=NULL,graph_revision=%s,published_at=clock_timestamp() "
                    "WHERE bot_id=%s"
                ).format(self._table("runtime_graph_head")),
                (revision, self.store.bot_id),
            )
            return revision

    async def record_states(
        self,
        lease: HostLease,
        graph: RuntimeGenerationGraph,
        states: dict[str, tuple[str, str | None]],
    ) -> None:
        allowed = {"PREPARED", "ACTIVE", "QUIESCING", "QUIESCENT", "STOPPED", "FAILED"}
        if set(states) - set(graph.generations) or any(
            state not in allowed or (state != "FAILED" and error_code is not None)
            for state, error_code in states.values()
        ):
            raise ForestError(
                "RUNTIME_LIFECYCLE_INVALID", "Runtime lifecycle update is invalid", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._identity_in(conn)
            await self._host_in(conn, lease)
            head = await self._graph_head_in(conn, lock=True)
            await self._graph_in(conn, graph, lock=True)
            members = {
                item["component_id"]: item
                for item in await self._members_in(conn, graph, lock=True)
            }
            current_members = (
                {
                    item["component_id"]: item["generation_id"]
                    for item in await self._members_for_id_in(
                        conn, head["graph_id"], lock=False
                    )
                }
                if head is not None and head["graph_id"] != graph.graph_id
                and head["graph_id"] is not None
                else {}
            )
            for component_id, (state, error_code) in sorted(states.items()):
                member = members[component_id]
                if (
                    current_members.get(component_id) == member["generation_id"]
                    and state != "ACTIVE"
                ):
                    raise ForestError(
                        "RUNTIME_RETAINED_GENERATION_ACTIVE",
                        "A historical Graph cannot retire a Generation retained by the head",
                        409,
                    )
                latest = await self._latest_lifecycle_in(conn, graph.graph_id, component_id)
                path = self._lifecycle_path(latest["state"], state)
                for step in path:
                    await self._append_lifecycle_in(
                        conn,
                        graph.graph_id,
                        component_id,
                        step,
                        error_code=error_code if step == "FAILED" else None,
                    )
                generation_status = {
                    "PREPARED": "READY",
                    "ACTIVE": "ACTIVE",
                    "QUIESCING": "QUIESCING",
                    "QUIESCENT": "QUIESCENT",
                    "STOPPED": "DISPOSED",
                    "FAILED": "FAILED",
                }[state]
                await conn.execute(
                    sql.SQL("UPDATE {} SET status=%s WHERE bot_id=%s AND generation_id=%s").format(
                        self._table("generations")
                    ),
                    (generation_status, self.store.bot_id, member["generation_id"]),
                )
            final_states = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT DISTINCT ON (component_id) component_id,state FROM {} "
                        "WHERE bot_id=%s AND graph_id=%s ORDER BY component_id,sequence DESC"
                    ).format(self._table("runtime_graph_lifecycle_facts")),
                    (self.store.bot_id, graph.graph_id),
                )
            ).fetchall()
            if final_states and all(item["state"] == "STOPPED" for item in final_states):
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='STOPPED' WHERE bot_id=%s AND graph_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, graph.graph_id),
                )
            elif any(item["state"] == "FAILED" for item in final_states):
                await conn.execute(
                    sql.SQL("UPDATE {} SET state='FAILED' WHERE bot_id=%s AND graph_id=%s").format(
                        self._table("runtime_graphs")
                    ),
                    (self.store.bot_id, graph.graph_id),
                )

    async def _identity_in(self, conn, *, lock: bool = True) -> None:
        lock_clause = " FOR SHARE" if lock else ""
        row = await (
            await conn.execute(
                "SELECT bot_id FROM forest_kernel.bot_identity WHERE singleton" + lock_clause
            )
        ).fetchone()
        if row is None or row["bot_id"] != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Runtime authority belongs to another Bot", 403)

    async def _runtime_state_in(self, conn, *, lock: bool):
        suffix = sql.SQL(" FOR SHARE") if lock else sql.SQL("")
        return await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(self._table("runtime_state"))
                + suffix,
                (self.store.bot_id,),
            )
        ).fetchone()

    async def _graph_head_in(self, conn, *, lock: bool, update: bool = False):
        if update and not lock:
            raise ValueError("an update graph-head lock must be enabled")
        suffix = sql.SQL(" FOR UPDATE" if update else " FOR SHARE" if lock else "")
        return await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(
                    self._table("runtime_graph_head")
                )
                + suffix,
                (self.store.bot_id,),
            )
        ).fetchone()

    async def _storage_in(self, conn, binding: PreparedGenerationBinding) -> None:
        storage = binding.storage
        if storage is None:
            return
        row = await (
            await conn.execute(
                "SELECT * FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND schema_id=%s FOR SHARE",
                (self.store.bot_id, storage.schema_id),
            )
        ).fetchone()
        expected = {
            "component_id": binding.component_id,
            "schema_version": storage.schema_version,
            "namespace": storage.namespace,
            "baseline_sha256": storage.baseline_sha256,
            "package_digest": storage.package_digest,
            "repository_contract": storage.repository_contract,
            "execution_profile": storage.execution_profile,
            "state": "READY",
        }
        if row is None or any(str(row[key]) != value for key, value in expected.items()):
            raise ForestError(
                "RUNTIME_STORAGE_BINDING_INVALID",
                f"Generation storage is not the installed READY bundle for {binding.component_id}",
                409,
            )

    async def _host_in(self, conn, lease: HostLease) -> None:
        if lease.bot_id != self.store.bot_id:
            raise ForestError("BOT_SCOPE_MISMATCH", "Host lease belongs to another Bot", 403)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT *,clock_timestamp() AS current_time FROM {} WHERE bot_id=%s FOR SHARE"
                ).format(self._table("host_lease")),
                (self.store.bot_id,),
            )
        ).fetchone()
        if (
            row is None
            or row["host_id"] != lease.host_id
            or row["incarnation"] != lease.incarnation
            or row["owner_epoch"] != lease.owner_epoch
            or row["expires_at"] <= row["current_time"]
        ):
            raise ForestError("STALE_HOST", "Host lease is stale", 409)

    async def _guard_graph_replay_in(
        self,
        conn,
        lease: HostLease,
        existing,
        graph_id: str,
        graph_digest: str,
        generations: dict[str, str],
    ) -> None:
        members = await self._members_for_id_in(conn, graph_id, lock=False)
        actual = {item["component_id"]: item["generation_id"] for item in members}
        if (
            existing["graph_id"] != graph_id
            or existing["graph_digest"] != graph_digest
            or actual != generations
            or any(
                item["host_incarnation"] != lease.incarnation
                or item["host_owner_epoch"] != lease.owner_epoch
                for item in members
            )
        ):
            raise ForestError(
                "RUNTIME_ACTIVATION_CONFLICT",
                "Activation identity names different prepared facts",
                409,
            )

    async def _graph_in(self, conn, graph: RuntimeGenerationGraph, *, lock: bool):
        suffix = sql.SQL(" FOR UPDATE") if lock else sql.SQL("")
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND graph_id=%s").format(
                    self._table("runtime_graphs")
                )
                + suffix,
                (self.store.bot_id, graph.graph_id),
            )
        ).fetchone()
        if row is None or any(
            row[key] != value
            for key, value in {
                "graph_revision": graph.graph_revision,
                "graph_digest": graph.graph_digest,
                "activation_id": graph.activation_id,
            }.items()
        ):
            raise ForestError(
                "RUNTIME_GRAPH_IDENTITY_CONFLICT", "Runtime graph identity is not exact", 409
            )
        return row

    async def _members_in(self, conn, graph: RuntimeGenerationGraph, *, lock: bool):
        members = await self._members_for_id_in(conn, graph.graph_id, lock=lock)
        actual = {item["component_id"]: item["generation_id"] for item in members}
        if actual != graph.generations:
            raise ForestError(
                "RUNTIME_GRAPH_IDENTITY_CONFLICT", "Runtime graph membership is not exact", 409
            )
        return members

    async def _members_for_id_in(self, conn, graph_id: str, *, lock: bool):
        suffix = sql.SQL(" FOR UPDATE OF g") if lock else sql.SQL("")
        return await (
            await conn.execute(
                sql.SQL(
                    "SELECT m.*,g.status,g.host_incarnation,g.host_owner_epoch,"
                    "g.config_revision,g.config_payload_id,g.config_payload_digest "
                    "FROM {} m JOIN {} g ON (g.bot_id,g.generation_id)="
                    "(m.bot_id,m.generation_id) WHERE m.bot_id=%s AND m.graph_id=%s "
                    "ORDER BY m.component_id"
                ).format(self._table("runtime_graph_modules"), self._table("generations"))
                + suffix,
                (self.store.bot_id, graph_id),
            )
        ).fetchall()

    async def _append_lifecycle_in(
        self,
        conn,
        graph_id: str,
        component_id: str,
        state: str,
        *,
        error_code: str | None = None,
    ) -> None:
        latest = await self._latest_lifecycle_in(conn, graph_id, component_id)
        if latest["state"] == state:
            return
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,graph_id,component_id,sequence,state,error_code) "
                "VALUES (%s,%s,%s,%s,%s,%s)"
            ).format(self._table("runtime_graph_lifecycle_facts")),
            (
                self.store.bot_id,
                graph_id,
                component_id,
                latest["sequence"] + 1,
                state,
                error_code,
            ),
        )

    async def _latest_lifecycle_in(self, conn, graph_id: str, component_id: str):
        latest = await (
            await conn.execute(
                sql.SQL(
                    "SELECT sequence,state,error_code FROM {} WHERE bot_id=%s AND graph_id=%s "
                    "AND component_id=%s ORDER BY sequence DESC LIMIT 1 FOR UPDATE"
                ).format(self._table("runtime_graph_lifecycle_facts")),
                (self.store.bot_id, graph_id, component_id),
            )
        ).fetchone()
        if latest is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_PREPARED", "Generation lifecycle is absent", 409
            )
        return latest

    @staticmethod
    def _lifecycle_path(current: str, target: str) -> tuple[str, ...]:
        if current == target:
            return ()
        if target == "FAILED" and current not in {"STOPPED", "FAILED"}:
            return ("FAILED",)
        paths = {
            ("PREPARED", "ACTIVE"): ("ACTIVE",),
            ("PREPARED", "STOPPED"): ("STOPPED",),
            ("ACTIVE", "QUIESCING"): ("QUIESCING",),
            ("ACTIVE", "QUIESCENT"): ("QUIESCING", "QUIESCENT"),
            ("ACTIVE", "STOPPED"): ("QUIESCING", "QUIESCENT", "STOPPED"),
            ("QUIESCING", "QUIESCENT"): ("QUIESCENT",),
            ("QUIESCING", "STOPPED"): ("QUIESCENT", "STOPPED"),
            ("QUIESCENT", "STOPPED"): ("STOPPED",),
            ("FAILED", "STOPPED"): ("STOPPED",),
        }
        path = paths.get((current, target))
        if path is None:
            raise ForestError(
                "RUNTIME_LIFECYCLE_INVALID",
                f"Runtime lifecycle cannot move from {current} to {target}",
                409,
            )
        return path

    @staticmethod
    def _lease(row) -> HostLease:
        return HostLease(
            row["bot_id"],
            row["host_id"],
            row["incarnation"],
            row["owner_epoch"],
            row["expires_at"],
        )

    @staticmethod
    def _validate_bindings(
        bindings: tuple[PreparedGenerationBinding, ...] | list[PreparedGenerationBinding],
        activation_id: str,
        *,
        available_components: set[str] | None = None,
    ) -> list[PreparedGenerationBinding]:
        return validate_prepared_bindings(
            bindings, activation_id, available_components=available_components
        )
