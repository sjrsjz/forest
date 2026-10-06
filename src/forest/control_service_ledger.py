"""Durable control-scope outcomes for package-owned work."""

from __future__ import annotations

import json
import re

import psycopg
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.package_capabilities import PackageServiceAdmission, PackageServiceError
from forest.packages import digest_json

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")
_SCOPE = {
    "deployment": ("forest_deployment", "deployment_id"),
    "host": ("forest_host", "host_id"),
}


def _bounded(value: object, limit: int) -> bool:
    if not isinstance(value, str):
        return False
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        return False
    return 1 <= size <= limit and all(ord(char) >= 32 and ord(char) != 127 for char in value)


class ControlPackageServiceOperationLedger:
    """Fence one durable dispatch per stable package operation in a control scope."""

    scope: str

    def __init__(self, store, generations) -> None:
        self.store = store
        self.generations = generations
        if self.scope not in _SCOPE:
            raise ValueError("Control service operation scope is invalid")
        self.schema, self.id_column = _SCOPE[self.scope]
        self.scope_id = getattr(store, self.id_column)

    async def _execute(self, conn, query: str, params=None):
        # Both identifiers are fixed by _SCOPE, never supplied by a package.
        query = query.replace("__scope_schema__", self.schema)
        query = query.replace("__scope_id_column__", self.id_column)
        return await conn.execute(query, params)

    def _validate(self, scope, route) -> None:
        if (
            scope.scope_kind != self.scope
            or scope.scope_id != self.scope_id
            or scope.bot_id is not None
            or not all((
                _bounded(scope.component_id, 128),
                _bounded(scope.generation_id, 512),
                _bounded(scope.worker_incarnation, 512),
                _bounded(scope.call_id, 128),
                _bounded(scope.call_operation_id, 256),
                _bounded(scope.causal_root_kind, 128),
                _bounded(scope.causal_root_id, 256),
                _bounded(route.service_id, 128),
                _bounded(route.graph_id, 512),
            ))
            or not route.service_id.startswith("owned-work.")
            or type(route.graph_revision) is not int
            or route.graph_revision < 1
            or route.admission_lane != "DATA"
            or route.owned_work is not True
            or route.parent_call_id is not None
            or isinstance(route.timeout_seconds, bool)
            or not isinstance(route.timeout_seconds, (int, float))
            or not 0 < route.timeout_seconds <= 86_400
        ):
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OPERATION_INVALID",
                "Control-scope owned-work operation identity is invalid", 422,
            )

    @staticmethod
    def _outcome(row) -> PackageServiceAdmission:
        status = row["status"]
        if status == "UNKNOWN":
            return PackageServiceAdmission(
                execute=False,
                error=PackageServiceError(
                    "SERVICE_OPERATION_UNKNOWN",
                    "Original owned-work dispatch has no provable outcome", 409,
                ),
            )
        envelope = row["outcome"]
        if status == "SUCCEEDED" and isinstance(envelope, dict) and (
            envelope.get("kind") == "result" and set(envelope) == {"kind", "value"}
        ):
            return PackageServiceAdmission(execute=False, result=envelope["value"])
        if status == "FAILED" and isinstance(envelope, dict) and (
            envelope.get("kind") == "error" and isinstance(envelope.get("error"), dict)
        ):
            error = envelope["error"]
            if (
                isinstance(error.get("code"), str)
                and _ERROR.fullmatch(error["code"])
                and isinstance(error.get("message"), str)
                and type(error.get("status")) is int
                and 400 <= error["status"] <= 599
                and (error.get("details") is None or isinstance(error["details"], dict))
            ):
                return PackageServiceAdmission(
                    execute=False,
                    error=PackageServiceError(
                        error["code"], error["message"], error["status"], error.get("details"),
                    ),
                )
        raise ForestError(
            "SERVICE_OPERATION_CORRUPT",
            "Stored control-scope service outcome is invalid", 500,
        )

    async def begin(self, scope, route, input_value) -> PackageServiceAdmission:
        self._validate(scope, route)
        if not isinstance(input_value, dict):
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OPERATION_INVALID", "Owned-work input is invalid", 422,
            )
        input_digest = digest_json({"service_id": route.service_id, "input": input_value})
        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                await self.generations.verify_active_generation_in(
                    conn, component_id=scope.component_id,
                    generation_id=scope.generation_id, admission_lane="DATA",
                )
                created = await (
                    await self._execute(conn,
                        "INSERT INTO __scope_schema__.package_service_operations("
                        "__scope_id_column__,component_id,operation_id,service_id,input_digest,"
                        "causal_root_kind,causal_root_id,generation_id,call_id,deadline_at,status) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                        "clock_timestamp()+make_interval(secs => %s),'ACTIVE') "
                        "ON CONFLICT (__scope_id_column__,component_id,operation_id) DO NOTHING "
                        "RETURNING call_id",
                        (
                            self.scope_id, scope.component_id,
                            scope.call_operation_id, route.service_id, input_digest,
                            scope.causal_root_kind, scope.causal_root_id,
                            scope.generation_id, scope.call_id, float(route.timeout_seconds),
                        ),
                    )
                ).fetchone()
                if created is not None:
                    return PackageServiceAdmission(execute=True)
                row = await (
                    await self._execute(conn,
                        "SELECT *,deadline_at<=clock_timestamp() AS expired FROM "
                        "__scope_schema__.package_service_operations "
                        "WHERE __scope_id_column__=%s AND component_id=%s AND operation_id=%s FOR UPDATE",
                        (self.scope_id, scope.component_id, scope.call_operation_id),
                    )
                ).fetchone()
                if row is None or (
                    row["service_id"] != route.service_id
                    or row["input_digest"] != input_digest
                    or row["causal_root_kind"] != scope.causal_root_kind
                    or row["causal_root_id"] != scope.causal_root_id
                ):
                    raise ForestError(
                        "SERVICE_OPERATION_CONFLICT",
                        "Stable control-scope operation input differs", 409,
                    )
                if row["status"] == "ACTIVE":
                    if not row["expired"]:
                        return PackageServiceAdmission(
                            execute=False,
                            error=PackageServiceError(
                                "SERVICE_OPERATION_IN_PROGRESS",
                                "Control-scope operation is still executing", 409,
                            ),
                        )
                    await self._execute(conn,
                        "UPDATE __scope_schema__.package_service_operations "
                        "SET status='UNKNOWN',error_code='DEADLINE_EXPIRED',"
                        "completed_at=clock_timestamp() "
                        "WHERE __scope_id_column__=%s AND component_id=%s AND operation_id=%s",
                        (self.scope_id, scope.component_id, scope.call_operation_id),
                    )
                    return PackageServiceAdmission(
                        execute=False,
                        error=PackageServiceError(
                            "SERVICE_OPERATION_UNKNOWN",
                            "Original owned-work dispatch has no provable outcome", 409,
                        ),
                    )
                return self._outcome(row)
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OPERATION_FAILED",
                "Control-scope service admission transaction failed", 503,
            ) from exc

    async def _finish(self, scope, route, *, status: str, envelope: dict, error_code=None) -> None:
        self._validate(scope, route)
        try:
            encoded = json.dumps(envelope, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OUTCOME_INVALID", "Owned-work outcome is not JSON", 502,
            ) from exc
        if len(encoded.encode("utf-8")) > 65536:
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OUTCOME_TOO_LARGE", "Owned-work outcome exceeds 64 KiB", 502,
            )
        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                await self._execute(conn,
                    "SELECT generation_id FROM __scope_schema__.runtime_generations "
                    "WHERE __scope_id_column__=%s AND component_id=%s AND generation_id=%s "
                    "FOR SHARE",
                    (self.scope_id, scope.component_id, scope.generation_id),
                )
                await self.generations.verify_generation_member_in(
                    conn, component_id=scope.component_id, generation_id=scope.generation_id,
                )
                row = await (
                    await self._execute(conn,
                        "SELECT * FROM __scope_schema__.package_service_operations "
                        "WHERE __scope_id_column__=%s AND component_id=%s AND operation_id=%s FOR UPDATE",
                        (self.scope_id, scope.component_id, scope.call_operation_id),
                    )
                ).fetchone()
                if (
                    row is None or row["call_id"] != scope.call_id
                    or row["generation_id"] != scope.generation_id
                ):
                    raise ForestError(
                        "SERVICE_OPERATION_STALE_ATTEMPT",
                        "Owned-work result has no matching active attempt", 409,
                    )
                if row["status"] == status and row["outcome"] == envelope:
                    return
                if row["status"] != "ACTIVE":
                    raise ForestError(
                        "SERVICE_OPERATION_STALE_ATTEMPT",
                        "Owned-work result arrived after its attempt ended", 409,
                    )
                await self._execute(conn,
                    "UPDATE __scope_schema__.package_service_operations "
                    "SET status=%s,outcome=%s,error_code=%s,completed_at=clock_timestamp() "
                    "WHERE __scope_id_column__=%s AND component_id=%s AND operation_id=%s",
                    (
                        status, Jsonb(envelope), error_code, self.scope_id,
                        scope.component_id, scope.call_operation_id,
                    ),
                )
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OPERATION_FAILED",
                "Control-scope service settlement transaction failed", 503,
            ) from exc

    async def succeed(self, scope, route, result) -> None:
        await self._finish(
            scope, route, status="SUCCEEDED", envelope={"kind": "result", "value": result},
        )

    async def fail(self, scope, route, error: PackageServiceError) -> None:
        if (
            not isinstance(error, PackageServiceError)
            or not isinstance(error.code, str)
            or _ERROR.fullmatch(error.code) is None
            or not _bounded(error.message, 1024)
            or type(error.status) is not int
            or not 400 <= error.status <= 599
        ):
            raise ForestError(
                f"{self.scope.upper()}_SERVICE_OUTCOME_INVALID", "Owned-work failure is invalid", 502,
            )
        await self._finish(
            scope, route, status="FAILED",
            envelope={"kind": "error", "error": {
                "code": error.code, "message": error.message,
                "status": error.status, "details": error.details,
            }},
            error_code=error.code,
        )
