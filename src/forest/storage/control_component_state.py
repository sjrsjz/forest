"""Kernel-owned JSON Payload and revisioned state for control-scope packages."""

from __future__ import annotations

import hashlib
import json

import psycopg
from psycopg import sql

from forest.control_generated_storage import generated_control_storage
from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.packages import digest_json


def _bounded(value: object, field: str, limit: int) -> str:
    try:
        size = len(value.encode("utf-8")) if isinstance(value, str) else 0
    except UnicodeError:
        size = 0
    if (
        not isinstance(value, str)
        or not 1 <= size <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("COMPONENT_STATE_INPUT_INVALID", f"Invalid {field}", 422)
    return value


class ControlPackageStateAuthority:
    """One control scope's immutable values, operation steps and CAS heads."""

    def __init__(
        self, runtime, *, component_id: str, namespace: str,
        state_schema: int, value_schema: dict,
    ) -> None:
        self.runtime = runtime
        self.scope_kind = runtime.scope_kind
        if (
            self.scope_kind not in {"deployment", "host"}
            or namespace != generated_control_storage(
                component_id, self.scope_kind
            )[0].namespace
            or type(state_schema) is not int or state_schema <= 0
            or not isinstance(value_schema, dict)
        ):
            raise ValueError("Control state needs a declared scope and value contract")
        self.scope_id = runtime.scope_id
        self.component_id = component_id
        self.schema = namespace
        self.scope_column = self.scope_kind + "_id"
        self.state_schema = state_schema
        self.contract_digest = digest_json({
            "state_schema": state_schema, "value_schema": value_schema,
        })

    def _scope(self, scope: PackageCallScope) -> None:
        if (
            not isinstance(scope, PackageCallScope)
            or scope.scope_kind != self.scope_kind
            or scope.scope_id != self.scope_id
            or scope.bot_id is not None
            or scope.component_id != self.component_id
        ):
            raise ForestError(
                "PACKAGE_CAPABILITY_SCOPE_MISMATCH",
                "State request belongs to another control scope",
                403,
            )

    def _table(self, name: str):
        return sql.Identifier(self.schema, name)

    async def _verify_contract_in(self, conn, component_id: str) -> bool:
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state_schema,contract_digest FROM {} "
                    "WHERE {}=%s AND component_id=%s FOR SHARE"
                ).format(
                    self._table("component_state_contracts"),
                    sql.Identifier(self.scope_column),
                ),
                (self.scope_id, component_id),
            )
        ).fetchone()
        if row is None:
            return False
        if (row["state_schema"] != self.state_schema
                or row["contract_digest"] != self.contract_digest):
            raise ForestError(
                "COMPONENT_STATE_CONTRACT_MISMATCH",
                "Persisted state requires an explicit contract migration", 409,
            )
        return True

    async def verify_contract(self, component_id: str) -> None:
        try:
            async with self.runtime.store.pool.connection() as conn, conn.transaction():
                await self._verify_contract_in(conn, component_id)
        except psycopg.Error as exc:
            raise ForestError(
                "COMPONENT_STATE_STORE_FAILED", "Control state contract query failed", 503,
            ) from exc

    @staticmethod
    def _value(value: object) -> tuple[bytes, str]:
        try:
            encoded = json.dumps(
                value, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ForestError(
                "COMPONENT_STATE_INPUT_INVALID", "State value is not JSON", 422,
            ) from exc
        if len(encoded) > 65536:
            raise ForestError(
                "COMPONENT_STATE_INPUT_INVALID", "State value exceeds 64 KiB", 422,
            )
        return encoded, "sha256:" + hashlib.sha256(encoded).hexdigest()

    async def read_state(self, scope: PackageCallScope, *, state_key: str) -> dict:
        self._scope(scope)
        _bounded(state_key, "state_key", 128)
        try:
            async with self.runtime.store.pool.connection() as conn, conn.transaction():
                await self.runtime.generation_authority.verify_generation_member_in(
                    conn, component_id=scope.component_id,
                    generation_id=scope.generation_id,
                )
                await self._verify_contract_in(conn, scope.component_id)
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT h.revision,p.content_bytes,p.content_digest "
                            "FROM {} h LEFT JOIN {} p ON p.{}=h.{} "
                            "AND p.component_id=h.component_id AND p.payload_id=h.payload_id "
                            "AND p.content_digest=h.value_digest "
                            "WHERE h.{}=%s AND h.component_id=%s AND h.state_key=%s"
                        ).format(
                            self._table("component_state_heads"),
                            self._table("component_state_payloads"),
                            sql.Identifier(self.scope_column),
                            sql.Identifier(self.scope_column),
                            sql.Identifier(self.scope_column),
                        ),
                        (self.scope_id, scope.component_id, state_key),
                    )
                ).fetchone()
                if row is None:
                    return {"value": None, "revision": 0}
                if row["content_bytes"] is None:
                    raise ForestError(
                        "COMPONENT_STATE_CORRUPT", "State Payload is absent", 500,
                    )
                content = bytes(row["content_bytes"])
                if "sha256:" + hashlib.sha256(content).hexdigest() != row["content_digest"]:
                    raise ForestError(
                        "COMPONENT_STATE_CORRUPT", "State Payload digest differs", 500,
                    )
                try:
                    value = json.loads(content)
                except (TypeError, ValueError, UnicodeError) as exc:
                    raise ForestError(
                        "COMPONENT_STATE_CORRUPT", "State Payload is not JSON", 500,
                    ) from exc
                return {"value": value, "revision": row["revision"]}
        except psycopg.Error as exc:
            raise ForestError(
                "COMPONENT_STATE_STORE_FAILED", "Control state query failed", 503,
            ) from exc

    async def commit_state(
        self,
        scope: PackageCallScope,
        *,
        state_key: str,
        value,
        expected_revision: int,
        operation_id: str,
    ) -> dict:
        self._scope(scope)
        _bounded(scope.call_id, "call_id", 128)
        _bounded(state_key, "state_key", 128)
        _bounded(operation_id, "operation_id", 256)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ForestError(
                "COMPONENT_STATE_INPUT_INVALID", "Invalid expected_revision", 422,
            )
        content, value_digest = self._value(value)
        input_digest = "sha256:" + hashlib.sha256(
            json.dumps(
                {"state_key": state_key, "value": value,
                 "expected_revision": expected_revision},
                sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        payload_id = "payload:state:" + hashlib.sha256(
            (self.scope_kind + "\0" + self.scope_id + "\0" +
             scope.component_id + "\0" + operation_id).encode("utf-8")
        ).hexdigest()
        try:
            async with self.runtime.store.pool.connection() as conn, conn.transaction():
                await self.runtime.generation_authority.verify_generation_member_in(
                    conn, component_id=scope.component_id,
                    generation_id=scope.generation_id,
                )
                for lock_kind, value_id in (
                    ("operation", operation_id), ("state", state_key),
                ):
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (f"forest.{self.scope_kind}.component-state/"
                         f"{self.scope_id}/{scope.component_id}/{lock_kind}/{value_id}",),
                    )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}({},component_id,state_schema,contract_digest) "
                        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                    ).format(
                        self._table("component_state_contracts"),
                        sql.Identifier(self.scope_column),
                    ),
                    (self.scope_id, scope.component_id,
                     self.state_schema, self.contract_digest),
                )
                await self._verify_contract_in(conn, scope.component_id)
                step = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT input_digest,revision,value_digest "
                            "FROM {} WHERE {}=%s AND component_id=%s AND operation_id=%s"
                        ).format(
                            self._table("component_state_steps"),
                            sql.Identifier(self.scope_column),
                        ),
                        (self.scope_id, scope.component_id, operation_id),
                    )
                ).fetchone()
                if step is not None:
                    if step["input_digest"] != input_digest:
                        raise ForestError(
                            "COMPONENT_STATE_OPERATION_CONFLICT",
                            "State operation input differs", 409,
                        )
                    return {
                        "value": value, "revision": step["revision"],
                        "value_digest": step["value_digest"],
                    }
                head = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT revision FROM {} WHERE {}=%s AND component_id=%s "
                            "AND state_key=%s FOR UPDATE"
                        ).format(
                            self._table("component_state_heads"),
                            sql.Identifier(self.scope_column),
                        ),
                        (self.scope_id, scope.component_id, state_key),
                    )
                ).fetchone()
                revision = 0 if head is None else head["revision"]
                if revision != expected_revision:
                    raise ForestError(
                        "COMPONENT_STATE_REVISION_CONFLICT",
                        "Component state revision changed", 409,
                        details={"current_revision": revision},
                    )
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}({},component_id,payload_id,operation_id,"
                        "content_digest,content_bytes) VALUES (%s,%s,%s,%s,%s,%s)"
                    ).format(
                        self._table("component_state_payloads"),
                        sql.Identifier(self.scope_column),
                    ),
                    (self.scope_id, scope.component_id, payload_id, operation_id,
                     value_digest, content),
                )
                next_revision = revision + 1
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}({},component_id,operation_id,generation_id,"
                        "call_id,state_key,expected_revision,revision,input_digest,"
                        "payload_id,value_digest) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ).format(
                        self._table("component_state_steps"),
                        sql.Identifier(self.scope_column),
                    ),
                    (self.scope_id, scope.component_id, operation_id,
                     scope.generation_id, scope.call_id, state_key, revision,
                     next_revision, input_digest, payload_id, value_digest),
                )
                if head is None:
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}({},component_id,state_key,revision,"
                            "payload_id,value_digest) VALUES (%s,%s,%s,%s,%s,%s)"
                        ).format(
                            self._table("component_state_heads"),
                            sql.Identifier(self.scope_column),
                        ),
                        (self.scope_id, scope.component_id, state_key,
                         next_revision, payload_id, value_digest),
                    )
                else:
                    updated = await (
                        await conn.execute(
                            sql.SQL(
                                "UPDATE {} SET revision=%s,payload_id=%s,value_digest=%s "
                                "WHERE {}=%s AND component_id=%s AND state_key=%s "
                                "AND revision=%s RETURNING revision"
                            ).format(
                                self._table("component_state_heads"),
                                sql.Identifier(self.scope_column),
                            ),
                            (next_revision, payload_id, value_digest, self.scope_id,
                             scope.component_id, state_key, revision),
                        )
                    ).fetchone()
                    if updated is None:
                        raise ForestError(
                            "COMPONENT_STATE_REVISION_CONFLICT",
                            "Component state revision changed", 409,
                        )
                return {
                    "value": value, "revision": next_revision,
                    "value_digest": value_digest,
                }
        except psycopg.Error as exc:
            raise ForestError(
                "COMPONENT_STATE_STORE_FAILED", "Control state commit failed", 503,
            ) from exc
