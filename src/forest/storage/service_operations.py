"""Durable exactly-once result ledger for effectful package services."""

from __future__ import annotations

import json
from typing import Any

from psycopg import sql

from forest.errors import ForestError
from forest.package_capabilities import (
    PackageCallScope,
    PackageServiceAdmission,
    PackageServiceError,
    PackageServiceRoute,
)
from forest.packages import digest_json


def _bounded(value, field: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("SERVICE_OPERATION_INPUT_INVALID", f"Invalid {field}", 422)
    return value


class KernelServiceOperationLedger:
    """Admit one dispatch per stable operation and replay its terminal outcome."""

    def __init__(self, runtime, payloads) -> None:
        self.runtime = runtime
        self.payloads = payloads

    async def _verify_begin_route(self, conn, scope, route) -> None:
        if route.owned_work:
            accepted = await self.runtime.verify_generation_member_in(
                conn,
                component_id=scope.component_id,
                generation_id=scope.generation_id,
            )
        else:
            accepted = await self.runtime.verify_active_generation_in(
                conn,
                component_id=scope.component_id,
                generation_id=scope.generation_id,
                admission_lane=route.admission_lane,
            )
        if (
            accepted.graph_id != route.graph_id
            or accepted.graph_revision != route.graph_revision
        ):
            raise ForestError(
                "SERVICE_OPERATION_ROUTE_STALE",
                "Service operation route differs from the current RuntimeGraph",
                409,
            )

    async def _verify_completion_route(self, conn, scope, route) -> None:
        accepted = await self.runtime.verify_generation_member_in(
            conn,
            component_id=scope.component_id,
            generation_id=scope.generation_id,
        )
        if (
            accepted.graph_id != route.graph_id
            or accepted.graph_revision != route.graph_revision
        ):
            raise ForestError(
                "SERVICE_OPERATION_ROUTE_STALE",
                "Service operation no longer belongs to the current RuntimeGraph",
                409,
            )

    def _validate(self, scope: PackageCallScope, route: PackageServiceRoute) -> None:
        for value, field, limit in (
            (scope.component_id, "component_id", 128),
            (scope.generation_id, "generation_id", 512),
            (scope.worker_incarnation, "worker_incarnation", 512),
            (scope.call_id, "call_id", 128),
            (scope.call_operation_id, "operation_id", 256),
            (scope.causal_root_kind, "causal_root_kind", 128),
            (scope.causal_root_id, "causal_root_id", 256),
            (route.service_id, "service_id", 128),
            (route.graph_id, "graph_id", 512),
        ):
            _bounded(value, field, limit)
        if scope.bot_id != self.runtime.store.bot_id:
            raise ForestError(
                "SERVICE_OPERATION_SCOPE_MISMATCH",
                "Service operation belongs to another Bot",
                403,
            )
        if route.parent_call_id is not None:
            _bounded(route.parent_call_id, "parent_call_id", 128)
        if (
            type(route.graph_revision) is not int
            or route.graph_revision <= 0
            or route.admission_lane not in {"DATA", "CONTROL"}
            or type(route.owned_work) is not bool
            or isinstance(route.timeout_seconds, bool)
            or not isinstance(route.timeout_seconds, (int, float))
            or not 0 < route.timeout_seconds <= 120
        ):
            raise ForestError(
                "SERVICE_OPERATION_INPUT_INVALID",
                "Invalid service route or deadline",
                422,
            )

    async def _outcome(self, conn, row) -> PackageServiceAdmission:
        if row["status"] == "UNKNOWN":
            return PackageServiceAdmission(
                execute=False,
                error=PackageServiceError(
                    "SERVICE_OPERATION_UNKNOWN",
                    "The original service attempt ended without a provable result",
                    409,
                ),
            )
        reference = await self.payloads.verify_in(
            conn,
            payload_id=row["outcome_payload_id"],
            digest=row["outcome_digest"],
            owner_component_id=row["component_id"],
            owner_kind="service.operation",
            owner_id=row["outcome_owner_id"],
        )
        body = await self.payloads.read_bytes_in(
            conn,
            reference.payload_id,
            expected_deletion_epoch=reference.accepted_deletion_epoch,
        )
        try:
            envelope = json.loads(body)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ForestError(
                "SERVICE_OPERATION_CORRUPT",
                "Stored service outcome is not valid JSON",
                500,
            ) from exc
        if not isinstance(envelope, dict):
            raise ForestError(
                "SERVICE_OPERATION_CORRUPT",
                "Stored service outcome is not an object",
                500,
            )
        if row["status"] == "SUCCEEDED" and envelope.get("kind") == "result":
            return PackageServiceAdmission(execute=False, result=envelope.get("value"))
        error = envelope.get("error")
        if (
            row["status"] != "FAILED"
            or envelope.get("kind") != "error"
            or not isinstance(error, dict)
            or not isinstance(error.get("code"), str)
            or not error["code"]
            or not isinstance(error.get("message"), str)
            or not error["message"]
            or type(error.get("status")) is not int
            or not 400 <= error["status"] <= 599
            or (
                error.get("details") is not None
                and not isinstance(error["details"], dict)
            )
        ):
            raise ForestError(
                "SERVICE_OPERATION_CORRUPT",
                "Stored service outcome differs from its terminal state",
                500,
            )
        return PackageServiceAdmission(
            execute=False,
            error=PackageServiceError(
                code=error["code"],
                message=error["message"],
                status=error["status"],
                details=error.get("details"),
            ),
        )

    async def _record_nonexecuting_attempt(
        self,
        conn,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        *,
        deadline_at,
        status: str,
        error_code: str | None,
    ) -> None:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,call_id,component_id,operation_id,generation_id,"
                "worker_incarnation,service_name,graph_id,graph_revision,admission_lane,"
                "owned_work,parent_call_id,causal_root_kind,causal_root_id,deadline_at,"
                "status,error_code,completed_at) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp())"
            ).format(self.runtime._table("service_call_attempts")),
            (
                self.runtime.store.bot_id,
                scope.call_id,
                scope.component_id,
                scope.call_operation_id,
                scope.generation_id,
                scope.worker_incarnation,
                route.service_id,
                route.graph_id,
                route.graph_revision,
                route.admission_lane,
                route.owned_work,
                route.parent_call_id,
                scope.causal_root_kind,
                scope.causal_root_id,
                deadline_at,
                status,
                error_code,
            ),
        )

    async def begin(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        input_value: dict,
    ) -> PackageServiceAdmission:
        self._validate(scope, route)
        input_digest = digest_json(
            {"service_id": route.service_id, "input": input_value}
        )
        async with self.runtime.store.pool.connection() as conn, conn.transaction():
            await self._verify_begin_route(conn, scope, route)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (
                    "forest.kernel.runtime/service-operation/"
                    f"{self.runtime.store.bot_id}/{scope.component_id}/"
                    f"{scope.call_operation_id}",
                ),
            )
            deadline_at = (
                await (
                    await conn.execute(
                        "SELECT clock_timestamp()+make_interval(secs => %s) AS deadline_at",
                        (float(route.timeout_seconds),),
                    )
                ).fetchone()
            )["deadline_at"]
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT *,deadline_at<=clock_timestamp() AS expired FROM {} "
                        "WHERE bot_id=%s AND component_id=%s AND operation_id=%s FOR UPDATE"
                    ).format(self.runtime._table("service_operations")),
                    (
                        self.runtime.store.bot_id,
                        scope.component_id,
                        scope.call_operation_id,
                    ),
                )
            ).fetchone()
            if row is not None:
                if (
                    row["service_name"] != route.service_id
                    or row["input_digest"] != input_digest
                    or row["causal_root_kind"] != scope.causal_root_kind
                    or row["causal_root_id"] != scope.causal_root_id
                ):
                    raise ForestError(
                        "SERVICE_OPERATION_CONFLICT",
                        "Stable service operation input differs",
                        409,
                    )
                if row["status"] == "ACTIVE":
                    if not row["expired"]:
                        await self._record_nonexecuting_attempt(
                            conn,
                            scope,
                            route,
                            deadline_at=deadline_at,
                            status="REJECTED",
                            error_code="SERVICE_OPERATION_IN_PROGRESS",
                        )
                        return PackageServiceAdmission(
                            execute=False,
                            error=PackageServiceError(
                                "SERVICE_OPERATION_IN_PROGRESS",
                                "The stable service operation is still executing",
                                409,
                            ),
                        )
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET status='UNKNOWN',error_code='DEADLINE_EXPIRED',"
                            "completed_at=clock_timestamp() WHERE bot_id=%s AND component_id=%s "
                            "AND operation_id=%s AND status='ACTIVE'"
                        ).format(self.runtime._table("service_operations")),
                        (
                            self.runtime.store.bot_id,
                            scope.component_id,
                            scope.call_operation_id,
                        ),
                    )
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET status='UNKNOWN',error_code='DEADLINE_EXPIRED',"
                            "completed_at=clock_timestamp() WHERE bot_id=%s AND call_id=%s "
                            "AND status='ACTIVE'"
                        ).format(self.runtime._table("service_call_attempts")),
                        (self.runtime.store.bot_id, row["active_call_id"]),
                    )
                    row = {**dict(row), "status": "UNKNOWN"}
                admission = await self._outcome(conn, row)
                await self._record_nonexecuting_attempt(
                    conn,
                    scope,
                    route,
                    deadline_at=deadline_at,
                    status="REPLAYED",
                    error_code=(admission.error.code if admission.error is not None else None),
                )
                return admission
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,component_id,operation_id,service_name,input_digest,"
                    "causal_root_kind,causal_root_id,status,active_call_id,"
                    "active_generation_id,deadline_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s,%s)"
                ).format(self.runtime._table("service_operations")),
                (
                    self.runtime.store.bot_id,
                    scope.component_id,
                    scope.call_operation_id,
                    route.service_id,
                    input_digest,
                    scope.causal_root_kind,
                    scope.causal_root_id,
                    scope.call_id,
                    scope.generation_id,
                    deadline_at,
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,call_id,component_id,operation_id,generation_id,"
                    "worker_incarnation,service_name,graph_id,graph_revision,admission_lane,"
                    "owned_work,parent_call_id,causal_root_kind,causal_root_id,deadline_at,"
                    "status) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE')"
                ).format(self.runtime._table("service_call_attempts")),
                (
                    self.runtime.store.bot_id,
                    scope.call_id,
                    scope.component_id,
                    scope.call_operation_id,
                    scope.generation_id,
                    scope.worker_incarnation,
                    route.service_id,
                    route.graph_id,
                    route.graph_revision,
                    route.admission_lane,
                    route.owned_work,
                    route.parent_call_id,
                    scope.causal_root_kind,
                    scope.causal_root_id,
                    deadline_at,
                ),
            )
            return PackageServiceAdmission(execute=True)

    async def _finish(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        *,
        status: str,
        envelope: dict,
        error_code: str | None,
    ) -> None:
        owner_id = f"{scope.component_id}:{scope.call_operation_id}"
        async with self.runtime.store.pool.connection() as conn, conn.transaction():
            await self._verify_completion_route(conn, scope, route)
            operation = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND component_id=%s "
                        "AND operation_id=%s FOR UPDATE"
                    ).format(self.runtime._table("service_operations")),
                    (
                        self.runtime.store.bot_id,
                        scope.component_id,
                        scope.call_operation_id,
                    ),
                )
            ).fetchone()
            if (
                operation is None
                or operation["status"] != "ACTIVE"
                or operation["active_call_id"] != scope.call_id
            ):
                raise ForestError(
                    "SERVICE_OPERATION_STALE_ATTEMPT",
                    "Service result does not belong to the active attempt",
                    409,
                )
            deletion_epoch = await self.payloads.epoch_in(conn)
            saved = await self.payloads.put_json_in(
                conn,
                owner_component_id=scope.component_id,
                owner_kind="service.operation",
                owner_id=owner_id,
                value=envelope,
                expected_deletion_epoch=deletion_epoch,
            )
            changed = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET status=%s,outcome_payload_id=%s,outcome_digest=%s,"
                        "outcome_owner_id=%s,error_code=%s,completed_at=clock_timestamp() "
                        "WHERE bot_id=%s AND component_id=%s AND operation_id=%s "
                        "AND status='ACTIVE' AND active_call_id=%s RETURNING operation_id"
                    ).format(self.runtime._table("service_operations")),
                    (
                        status,
                        saved.payload_id,
                        saved.content_digest,
                        owner_id,
                        error_code,
                        self.runtime.store.bot_id,
                        scope.component_id,
                        scope.call_operation_id,
                        scope.call_id,
                    ),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET status=%s,error_code=%s,completed_at=clock_timestamp() "
                        "WHERE bot_id=%s AND call_id=%s AND status='ACTIVE' RETURNING call_id"
                    ).format(self.runtime._table("service_call_attempts")),
                    (
                        status,
                        error_code,
                        self.runtime.store.bot_id,
                        scope.call_id,
                    ),
                )
            ).fetchone()
            if changed is None or attempt is None:
                raise ForestError(
                    "SERVICE_OPERATION_STALE_ATTEMPT",
                    "Service operation changed before result commit",
                    409,
                )

    async def succeed(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        result: Any,
    ) -> None:
        await self._finish(
            scope,
            route,
            status="SUCCEEDED",
            envelope={"kind": "result", "value": result},
            error_code=None,
        )

    async def fail(
        self,
        scope: PackageCallScope,
        route: PackageServiceRoute,
        error: PackageServiceError,
    ) -> None:
        await self._finish(
            scope,
            route,
            status="FAILED",
            envelope={
                "kind": "error",
                "error": {
                    "code": error.code,
                    "message": error.message,
                    "status": error.status,
                    "details": error.details,
                },
            },
            error_code=error.code,
        )
