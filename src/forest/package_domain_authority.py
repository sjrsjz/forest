"""Generic PostgreSQL authority for package-declared domain Query/Command ports."""

from __future__ import annotations

import asyncio
import inspect
import re
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.packages import digest_json
from forest.storage.management_asset_authority import ManagementAssetAuthority

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
        payloads=None,
    ) -> None:
        self.store = store
        self.runtime = runtime
        self.component_id = component_id
        self.runtime_package_digest = runtime_package_digest
        self.storage_spec = storage_spec
        self.storage_package_digest = storage_package_digest
        self.storage_execution_profile = storage_execution_profile
        self.payloads = payloads
        self.management_assets = ManagementAssetAuthority(payloads) if payloads is not None else None

    @staticmethod
    def _pointer(value: object, pointer: str, bindings: tuple[str, ...] = ()) -> object:
        current = value
        wildcard = iter(bindings)
        for encoded in pointer[1:].split("/"):
            token = encoded.replace("~1", "/").replace("~0", "~")
            if token == "*":
                token = next(wildcard, None)
            if isinstance(current, dict) and token in current:
                current = current[token]
            elif isinstance(current, list) and isinstance(token, str) and token.isdecimal():
                index = int(token)
                if index >= len(current):
                    raise ForestError(
                        "DOMAIN_PAYLOAD_REFERENCE_INVALID",
                        "Managed payload reference path is absent from the command input",
                        422,
                    )
                current = current[index]
            else:
                raise ForestError(
                    "DOMAIN_PAYLOAD_REFERENCE_INVALID",
                    "Managed payload reference path is absent from the command input",
                    422,
                )
        return current

    @classmethod
    def _pointer_bindings(cls, value: object, pointer: str) -> tuple[tuple[str, ...], ...]:
        tokens = pointer[1:].split("/")
        results: list[tuple[str, ...]] = []

        def walk(current: object, offset: int, bindings: tuple[str, ...]) -> None:
            if offset == len(tokens):
                results.append(bindings)
                return
            token = tokens[offset].replace("~1", "/").replace("~0", "~")
            if token == "*":
                if isinstance(current, list):
                    for index, item in enumerate(current):
                        walk(item, offset + 1, (*bindings, str(index)))
                    return
                if isinstance(current, dict):
                    for key in sorted(current):
                        walk(current[key], offset + 1, (*bindings, key))
                    return
                raise ForestError(
                    "DOMAIN_PAYLOAD_REFERENCE_INVALID",
                    "Managed payload wildcard does not select a collection",
                    422,
                )
            if isinstance(current, dict) and token in current:
                walk(current[token], offset + 1, bindings)
                return
            if isinstance(current, list) and token.isdecimal() and int(token) < len(current):
                walk(current[int(token)], offset + 1, bindings)
                return
            raise ForestError(
                "DOMAIN_PAYLOAD_REFERENCE_INVALID",
                "Managed payload reference path is absent from the command input",
                422,
            )

        walk(value, 0, ())
        return tuple(results)

    async def _verify_payload_refs(
        self, conn, declaration: dict, payload: dict, *, scope,
        management_authorization=None,
    ) -> None:
        references = declaration.get("payload_refs", ())
        if not references:
            return
        if self.payloads is None:
            raise ForestError(
                "DOMAIN_PAYLOAD_AUTHORITY_UNAVAILABLE",
                "Managed payload references require the kernel payload authority",
                503,
            )
        for declaration_ref in references:
            bindings_set = self._pointer_bindings(payload, declaration_ref["path"])
            for bindings in bindings_set:
                reference = self._pointer(payload, declaration_ref["path"], bindings)
                owner_id = self._pointer(payload, declaration_ref["owner_id_path"], bindings)
                owner_component = declaration_ref.get("owner_component")
                if owner_component is None:
                    owner_component = self._pointer(
                        payload, declaration_ref["owner_component_path"], bindings
                    )
                owner_kind = declaration_ref.get("owner_kind")
                if owner_kind is None:
                    owner_kind = self._pointer(
                        payload, declaration_ref["owner_kind_path"], bindings
                    )
                if (
                    not isinstance(reference, dict)
                    or set(reference)
                    != {
                        "payload_id",
                        "digest",
                        "owner_component_id",
                        "owner_kind",
                        "owner_id",
                    }
                    or reference.get("owner_component_id") != owner_component
                    or reference.get("owner_kind") != owner_kind
                    or reference.get("owner_id") != owner_id
                ):
                    raise ForestError(
                        "DOMAIN_PAYLOAD_REFERENCE_INVALID",
                        "Managed payload reference differs from its frozen port declaration",
                        409,
                    )
                verified = await self.payloads.verify_in(
                    conn,
                    payload_id=reference.get("payload_id"),
                    digest=reference.get("digest"),
                    owner_component_id=reference["owner_component_id"],
                    owner_kind=reference["owner_kind"],
                    owner_id=reference["owner_id"],
                )
                media_type_path = declaration_ref.get("media_type_path")
                byte_length_path = declaration_ref.get("byte_length_path")
                if (
                    media_type_path is not None
                    and self._pointer(payload, media_type_path, bindings) != verified.media_type
                ) or (
                    byte_length_path is not None
                    and self._pointer(payload, byte_length_path, bindings) != verified.byte_length
                ):
                    raise ForestError(
                        "DOMAIN_PAYLOAD_METADATA_CONFLICT",
                        "Managed payload metadata differs from its authoritative bytes",
                        409,
                    )
                asset_id = declaration_ref.get("management_asset_id")
                if asset_id is not None:
                    actor = getattr(management_authorization, "web_subject_ref", None)
                    target_generation = getattr(
                        management_authorization, "target_generation_id", None
                    )
                    if actor is None or target_generation != scope.generation_id:
                        raise ForestError(
                            "MANAGEMENT_ASSET_AUTHORITY_REQUIRED",
                            "Asset-backed commands require current Web authority", 403,
                        )
                    asset = await self.management_assets.verify_asset_in(
                        conn, payload_id=verified.payload_id,
                        digest=verified.content_digest, component_id=self.component_id,
                        asset_id=asset_id, actor_subject_ref=actor,
                        generation_id=scope.generation_id,
                        target_ref=self._pointer(
                            payload, declaration_ref["target_ref_path"], bindings
                        ),
                        part_id=self._pointer(
                            payload, declaration_ref["part_id_path"], bindings
                        ),
                        min_remaining_seconds=declaration_ref.get("min_remaining_s", 0),
                    )
                    if (
                        asset["payload"] != reference
                        or asset["media_type"] != verified.media_type
                        or asset["byte_length"] != verified.byte_length
                        or asset["width"] != self._pointer(
                            payload, declaration_ref["width_path"], bindings
                        )
                        or asset["height"] != self._pointer(
                            payload, declaration_ref["height_path"], bindings
                        )
                    ):
                        raise ForestError(
                            "MANAGEMENT_ASSET_METADATA_CONFLICT",
                            "Asset metadata differs from its authoritative bytes", 409,
                        )

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
        result_validator=None,
    ) -> Any:
        return await self._invoke(
            "command",
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=timeout_seconds,
            result_validator=result_validator,
        )

    async def invoke_management_action(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
        before_write=None,
        management_authorization=None,
        result_validator=None,
    ) -> Any:
        """Run one package Command behind the current CONTROL route and authority fence."""

        return await self._invoke(
            "management_action",
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=timeout_seconds,
            before_write=before_write,
            management_authorization=management_authorization,
            result_validator=result_validator,
        )

    async def invoke_preparation_command(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
        result_validator=None,
    ) -> Any:
        return await self._invoke(
            "preparation_command",
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=timeout_seconds,
            result_validator=result_validator,
        )

    async def invoke_cleanup_command(
        self,
        scope: PackageCallScope,
        declaration: dict,
        payload: dict,
        *,
        parent_call_id: str,
        timeout_seconds: float,
        result_validator=None,
    ) -> Any:
        return await self._invoke(
            "cleanup_command",
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=timeout_seconds,
            result_validator=result_validator,
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

    async def invoke_artifact_command_in(
        self, conn, scope: PackageCallScope, declaration: dict, payload: dict,
        *, parent_call_id: str, timeout_seconds: float, result_validator,
    ) -> Any:
        """Execute a declared artifact hook inside the Web authority transaction."""
        try:
            async with asyncio.timeout(timeout_seconds):
                await self.runtime.verify_active_generation_in(
                    conn, component_id=self.component_id,
                    generation_id=scope.generation_id, admission_lane="CONTROL",
                )
                result = await self._invoke_in(
                    conn, "management_action", scope, declaration, payload,
                    parent_call_id=parent_call_id, timeout_seconds=timeout_seconds,
                    generation_verified=True,
                )
                if not result_validator(result):
                    raise ForestError(
                        "MODULE_MANAGEMENT_OUTPUT_INVALID",
                        "Artifact command result violates its package schema", 502,
                    )
                return result
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
        if phase not in {"claim", "renew", "settle"}:
            raise ForestError("OWNED_WORK_PHASE_INVALID", "Owned work phase is invalid", 422)
        function = declaration.get(f"{phase}_function")
        if function is None:
            raise ForestError("OWNED_WORK_PHASE_INVALID", "Owned work phase is undeclared", 422)
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
        before_write=None,
        management_authorization=None,
        result_validator=None,
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
                        await self.runtime.verify_generation_member_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    elif kind == "management_action":
                        await self.runtime.verify_active_generation_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                            admission_lane="CONTROL",
                        )
                    elif kind == "preparation_command":
                        await self.runtime.verify_prepared_generation_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    elif kind == "cleanup_command":
                        await self.runtime.verify_generation_cleanup_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    else:
                        await self.runtime.verify_generation_member_in(
                            conn,
                            component_id=self.component_id,
                            generation_id=scope.generation_id,
                        )
                    if before_write is not None:
                        guarded = before_write(conn)
                        if inspect.isawaitable(guarded):
                            await guarded
                    port_kind = (
                        "command"
                        if kind in {"preparation_command", "cleanup_command"}
                        else kind
                    )
                    result = await self._invoke_in(
                        conn,
                        port_kind,
                        scope,
                        declaration,
                        payload,
                        parent_call_id=parent_call_id,
                        timeout_seconds=timeout_seconds,
                        generation_verified=True,
                        management_authorization=management_authorization,
                    )
                    if result_validator is not None and not result_validator(result):
                        code, message = {
                            "management_action": (
                                "MODULE_MANAGEMENT_OUTPUT_INVALID",
                                "Management action result violates its immutable package schema",
                            ),
                            "preparation_command": (
                                "CONTRIBUTION_DIRECTORY_SYNCHRONIZATION_INVALID",
                                "Contribution directory synchronization returned an invalid result",
                            ),
                            "cleanup_command": (
                                "CONTRIBUTION_DIRECTORY_CLEANUP_INVALID",
                                "Contribution directory cleanup returned an invalid result",
                            ),
                        }.get(kind, (
                            "DOMAIN_OUTPUT_INVALID",
                            "Domain authority result violates its immutable schema",
                        ))
                        raise ForestError(
                            code, message, 502,
                        )
                    return result
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
        management_authorization=None,
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
                "management_action",
                "participant",
                "owned_claim",
                "owned_renew",
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
        await self._verify_storage(conn, scope)
        await self._verify_function(conn, function, query=kind == "query")
        await self._verify_payload_refs(
            conn, declaration, payload, scope=scope,
            management_authorization=management_authorization,
        )
        context = {
            "protocol": "forest.domain-context/1",
            "port_kind": kind,
            "port_name": name,
            "scope_kind": scope.scope_kind,
            "scope_id": scope.scope_id,
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
            "recovered_generation": scope.recovered_generation,
            "timeout_seconds": timeout_seconds,
            "input_digest": digest_json(payload),
            "participant_input_digest": (
                digest_json(payload["input"])
                if kind == "participant"
                and payload.get("protocol") == "forest.uow-step/1"
                and isinstance(payload.get("input"), dict)
                else None
            ),
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

    async def _verify_storage(self, conn, scope: PackageCallScope) -> None:
        if self.storage_spec.scope != scope.scope_kind:
            raise ForestError(
                "DOMAIN_STORAGE_FENCE_MISMATCH",
                "Domain storage scope differs from the caller", 409,
            )
        if scope.scope_kind == "bot" and (
            scope.scope_id == getattr(self.store, "bot_id", None)
            and scope.bot_id == getattr(self.store, "bot_id", None)
        ):
            ledger = sql.Identifier("forest_kernel", "schema_bundles")
            owner_column = "bot_id"
        elif scope.scope_kind == "deployment" and (
            scope.scope_id == getattr(self.store, "deployment_id", None)
            and scope.bot_id is None
        ):
            ledger = sql.Identifier("forest_deployment", "schema_bundles")
            owner_column = "deployment_id"
        elif scope.scope_kind == "host" and (
            scope.scope_id == getattr(self.store, "host_id", None)
            and scope.bot_id is None
        ):
            ledger = sql.Identifier("forest_host", "schema_bundles")
            owner_column = "host_id"
        else:
            raise ForestError(
                "DOMAIN_STORAGE_FENCE_MISMATCH",
                "Domain storage authority does not own the caller scope", 409,
            )
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT component_id,schema_version,namespace,baseline_sha256,package_digest,"
                    "repository_contract,execution_profile,state FROM {} "
                    "WHERE {}=%s AND schema_id=%s"
                ).format(ledger, sql.Identifier(owner_column)),
                (scope.scope_id, self.storage_spec.schema_id),
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
            or set(error)
            not in (
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
