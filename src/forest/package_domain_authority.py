"""Generic PostgreSQL authority for package-declared domain Query/Command ports."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope

_FUNCTION = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class PostgresPackageDomainAuthority:
    """Execute a frozen package function without knowing its tables or DTO semantics."""

    def __init__(
        self,
        store,
        runtime,
        *,
        component_id: str,
        runtime_package_digest: str,
        storage_spec,
        storage_package_digest: str,
        storage_execution_profile: str,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.component_id = component_id
        self.runtime_package_digest = runtime_package_digest
        self.storage_spec = storage_spec
        self.storage_package_digest = storage_package_digest
        self.storage_execution_profile = storage_execution_profile

    async def invoke_query(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        return await self._invoke(
            "query",
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=timeout_seconds,
        )

    async def invoke_command(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        return await self._invoke(
            "command",
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=timeout_seconds,
        )

    async def invoke_participant_in(
        self,
        conn,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        try:
            async with asyncio.timeout(timeout_seconds):
                return await self._invoke_in(
                    conn,
                    "participant",
                    scope,
                    declaration,
                    payload,
                    parent_call_id=parent_call_id,
                    timeout_seconds=timeout_seconds,
                )
        except ForestError:
            raise
        except TimeoutError as exc:
            raise ForestError(
                "DOMAIN_AUTHORITY_TIMEOUT", "Domain authority exceeded its deadline", 504
            ) from exc
        except psycopg.Error as exc:
            raise ForestError(
                "DOMAIN_AUTHORITY_FAILED", "Domain authority transaction failed", 503
            ) from exc

    async def invoke_owned_work(
        self,
        phase: str,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        timeout_seconds: float,
    ) -> Any:
        if phase not in {"claim", "settle"}:
            raise ForestError(
                "OWNED_WORK_PHASE_INVALID", "Owned work phase is invalid", 422
            )
        function = declaration.get(f"{phase}_function")
        port = {
            "name": f"{declaration.get('name', '')}.{phase}",
            "function": function,
        }
        return await self._invoke(
            f"owned_{phase}",
            scope,
            port,
            payload,
            parent_call_id=None,
            timeout_seconds=timeout_seconds,
        )

    async def _invoke(
        self,
        kind: str,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self.store.pool.connection() as conn, conn.transaction():
                    if kind == "query":
                        await conn.execute(
                            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                        )
                        await self.runtime.verify_generation_member_snapshot_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    elif kind == "owned_claim":
                        await self.runtime.verify_active_generation_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                            admission_lane="DATA",
                        )
                    else:
                        await self.runtime.verify_generation_member_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    return await self._invoke_in(
                        conn,
                        kind,
                        scope,
                        declaration,
                        payload,
                        parent_call_id=parent_call_id,
                        timeout_seconds=timeout_seconds,
                        generation_verified=True,
                    )
        except ForestError:
            raise
        except TimeoutError as exc:
            raise ForestError(
                "DOMAIN_AUTHORITY_TIMEOUT", "Domain authority exceeded its deadline", 504
            ) from exc
        except psycopg.Error as exc:
            raise ForestError(
                "DOMAIN_AUTHORITY_FAILED", "Domain authority transaction failed", 503
            ) from exc

    async def _invoke_in(
        self,
        conn,
        kind: str,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
        generation_verified: bool = False,
    ) -> Any:
        function = declaration.get("function")
        name = declaration.get("name")
        if (
            scope.component_id != self.component_id
            or not isinstance(name, str)
            or not isinstance(function, str)
            or _FUNCTION.fullmatch(function) is None
            or kind
            not in {
                "query",
                "command",
                "participant",
                "owned_claim",
                "owned_settle",
            }
        ):
            raise ForestError(
                "DOMAIN_AUTHORITY_BINDING_INVALID",
                "Domain authority call differs from its frozen package binding",
                409,
            )
        if not generation_verified:
            await self.runtime.verify_generation_member_in(
                conn,
                component_id=self.component_id,
                generation_id=scope.generation_id,
            )
        await self._verify_storage(conn)
        await self._verify_function(conn, function, query=kind == "query")
        context = {
            "protocol": "forest.domain-context/1",
            "port_kind": kind,
            "port_name": name,
            "bot_id": scope.bot_id,
            "component_id": scope.component_id,
            "generation_id": scope.generation_id,
            "instance_id": scope.instance_id,
            "worker_incarnation": scope.worker_incarnation,
            "call_id": scope.call_id,
            "parent_call_id": parent_call_id,
            "operation_id": scope.call_operation_id,
            "causal_root_kind": scope.causal_root_kind,
            "causal_root_id": scope.causal_root_id,
            "timeout_seconds": timeout_seconds,
            "runtime_package_digest": self.runtime_package_digest,
            "storage_package_digest": self.storage_package_digest,
        }
        row = await (
            await conn.execute(
                sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                    sql.Identifier(self.storage_spec.namespace),
                    sql.Identifier(function),
                ),
                (Jsonb(context), Jsonb(payload)),
            )
        ).fetchone()
        return self._result(row["result"] if row is not None else None)

    async def _verify_storage(self, conn) -> None:
        row = await (
            await conn.execute(
                "SELECT component_id,schema_version,namespace,baseline_sha256,package_digest,"
                "repository_contract,execution_profile,state FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND schema_id=%s",
                (self.store.bot_id, self.storage_spec.schema_id),
            )
        ).fetchone()
        expected = {
            "component_id": self.component_id,
            "schema_version": self.storage_spec.schema_version,
            "namespace": self.storage_spec.namespace,
            "baseline_sha256": self.storage_spec.baseline_sha256,
            "package_digest": self.storage_package_digest,
            "repository_contract": self.storage_spec.repository_contract,
            "execution_profile": self.storage_execution_profile,
            "state": "READY",
        }
        if row is None or any(str(row[key]) != value for key, value in expected.items()):
            raise ForestError(
                "DOMAIN_STORAGE_FENCE_MISMATCH",
                "Domain authority storage identity differs from the frozen package",
                409,
            )

    async def _verify_function(self, conn, function: str, *, query: bool) -> None:
        rows = await (
            await conn.execute(
                "SELECT p.provolatile,p.prosecdef,p.proleakproof,p.proconfig,"
                "p.prorettype='jsonb'::regtype::oid AS returns_jsonb "
                "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname=%s AND p.proname=%s AND p.pronargs=2 "
                "AND p.proargtypes[0]='jsonb'::regtype::oid "
                "AND p.proargtypes[1]='jsonb'::regtype::oid",
                (self.storage_spec.namespace, function),
            )
        ).fetchall()
        if (
            len(rows) != 1
            or rows[0]["prosecdef"]
            or rows[0]["proleakproof"]
            or rows[0]["proconfig"] != ["search_path=pg_catalog"]
            or not rows[0]["returns_jsonb"]
            or (query and rows[0]["provolatile"] not in {"s", "i"})
            or (not query and rows[0]["provolatile"] != "v")
        ):
            raise ForestError(
                "DOMAIN_FUNCTION_INVALID",
                "Declared domain function does not implement the required ABI",
                409,
            )

    @staticmethod
    def _result(envelope: object) -> Any:
        if not isinstance(envelope, dict) or envelope.get("protocol") != "forest.domain-result/1":
            raise ForestError(
                "DOMAIN_RESULT_INVALID", "Domain function returned an invalid envelope", 502
            )
        if envelope.get("ok") is True and set(envelope) == {"protocol", "ok", "value"}:
            return envelope["value"]
        error = envelope.get("error")
        if (
            envelope.get("ok") is not False
            or set(envelope) != {"protocol", "ok", "error"}
            or not isinstance(error, dict)
            or set(error) not in (
                {"code", "message", "status"},
                {"code", "message", "status", "details"},
            )
            or not isinstance(error.get("code"), str)
            or _ERROR_CODE.fullmatch(error["code"]) is None
            or not isinstance(error.get("message"), str)
            or not 1 <= len(error["message"].encode("utf-8")) <= 1024
            or type(error.get("status")) is not int
            or not 400 <= error["status"] <= 599
            or ("details" in error and not isinstance(error["details"], dict))
        ):
            raise ForestError(
                "DOMAIN_RESULT_INVALID", "Domain function returned an invalid envelope", 502
            )
        raise ForestError(
            error["code"],
            error["message"],
            error["status"],
            details=error.get("details"),
        )
