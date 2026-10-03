"""Deployment-scoped module configuration; facts live in each package's SQL schema."""

from __future__ import annotations

import hashlib

from psycopg import sql
from psycopg.types.json import Jsonb

from forest.configuration_values import _MISSING, EffectiveModuleConfiguration, _pointer_value
from forest.errors import ForestError
from forest.packages import digest_json


class DeploymentModuleConfigurationRepository:
    """Revisioned configuration joined atomically to a deployment RuntimeGraph."""

    def __init__(self, store, *, component_id: str, namespace: str,
                 immutable_paths: tuple[str, ...] = ()) -> None:
        if not component_id or not namespace:
            raise ValueError("Module configuration identity is required")
        self.store = store
        self.component_id = component_id
        self.namespace = namespace
        self.immutable_paths = immutable_paths

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    @staticmethod
    def _payload(value: object) -> tuple[str, str]:
        try:
            digest = digest_json(value)
        except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
            raise ForestError("MODULE_CONFIGURATION_INVALID", "Configuration must be finite JSON", 422) from exc
        return "config:" + digest[7:], digest

    @staticmethod
    def _configuration(row, *, include_value: bool) -> EffectiveModuleConfiguration:
        value = row["config_value"]
        if digest_json(value) != row["config_payload_digest"] or (
            "config:" + row["config_payload_digest"][7:] != row["config_payload_id"]
        ):
            raise ForestError("MODULE_CONFIGURATION_CORRUPT", "Configuration value differs from its digest", 500)
        return EffectiveModuleConfiguration(
            row["revision"], row["config_payload_id"], row["config_payload_digest"],
            row["validation_digest"], value if include_value else None,
        )

    async def _head_in(self, conn, *, lock: bool = False):
        row = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE deployment_id=%s ").format(
                self._table("module_configuration_heads")
            ) + sql.SQL("FOR UPDATE" if lock else "FOR SHARE"),
            (self.store.deployment_id,),
        )).fetchone()
        if row is None:
            raise ForestError("MODULE_CONFIGURATION_UNINITIALIZED", "Module configuration head is absent", 409)
        return row

    async def _revision_in(self, conn, revision: int, *, include_value: bool):
        row = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE deployment_id=%s AND revision=%s").format(
                self._table("module_configuration_revisions")
            ), (self.store.deployment_id, revision),
        )).fetchone()
        if row is None:
            raise ForestError("MODULE_CONFIGURATION_NOT_FOUND", "Configuration revision is absent", 404)
        return self._configuration(row, include_value=include_value)

    async def _current_in(self, conn, field: str, *, include_value: bool):
        head = await self._head_in(conn)
        if head[field] == 0:
            raise ForestError("MODULE_CONFIGURATION_UNINITIALIZED", "Module configuration is absent", 409)
        return await self._revision_in(conn, head[field], include_value=include_value)

    async def desired(self, *, include_value: bool = False) -> EffectiveModuleConfiguration:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._current_in(conn, "desired_revision", include_value=include_value)

    async def effective(self, *, include_value: bool = False) -> EffectiveModuleConfiguration:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._current_in(conn, "effective_revision", include_value=include_value)

    async def desired_checked(self) -> EffectiveModuleConfiguration:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            return await self.desired_checked_in(conn)

    async def desired_checked_in(self, conn) -> EffectiveModuleConfiguration:
        desired = await self._current_in(conn, "desired_revision", include_value=True)
        await self._assert_immutable_in(conn, desired.value)
        return desired

    async def _assert_immutable_in(self, conn, value: object) -> None:
        if not self.immutable_paths:
            return
        original = (await self._current_in(conn, "effective_revision", include_value=True)).value
        for path in self.immutable_paths:
            before, after = _pointer_value(original, path), _pointer_value(value, path)
            if before is _MISSING or after is _MISSING or digest_json(before) != digest_json(after):
                raise ForestError("MODULE_CONFIGURATION_IMMUTABLE", "Deployment-lifetime configuration cannot change", 409,
                                  details={"path": path})

    async def assert_immutable(self, value: object) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._assert_immutable_in(conn, value)

    async def ensure_initialized(self, *, operation_id: str, value: object,
                                 validation_digest: str) -> EffectiveModuleConfiguration:
        try:
            return await self.desired()
        except ForestError as exc:
            if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
                raise
        return await self.initialize(
            operation_id=operation_id, value=value, validation_digest=validation_digest
        )

    async def initialize(self, *, operation_id: str, value: object,
                         validation_digest: str) -> EffectiveModuleConfiguration:
        payload_id, payload_digest = self._payload(value)
        input_digest = digest_json([
            "forest.deployment.module-configuration.initialize/1", self.component_id,
            value, validation_digest,
        ])
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("INSERT INTO {}(deployment_id) VALUES (%s) ON CONFLICT DO NOTHING").format(
                    self._table("module_configuration_heads")
                ), (self.store.deployment_id,),
            )
            head = await self._head_in(conn, lock=True)
            existing = await (await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE deployment_id=%s AND operation_id=%s").format(
                    self._table("module_configuration_revisions")
                ), (self.store.deployment_id, operation_id),
            )).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest or existing["revision"] != 1:
                    raise ForestError("MODULE_CONFIGURATION_OPERATION_CONFLICT", "Initialization identity differs", 409)
                return self._configuration(existing, include_value=False)
            if head["desired_revision"] != 0:
                raise ForestError("MODULE_CONFIGURATION_ALREADY_INITIALIZED", "Module configuration exists", 409)
            row = await (await conn.execute(
                sql.SQL("INSERT INTO {}(deployment_id,revision,previous_revision,operation_id,"
                        "input_digest,config_value,config_payload_id,config_payload_digest,validation_digest) "
                        "VALUES (%s,1,0,%s,%s,%s,%s,%s,%s) RETURNING *").format(
                    self._table("module_configuration_revisions")
                ), (self.store.deployment_id, operation_id, input_digest, Jsonb(value),
                    payload_id, payload_digest, validation_digest),
            )).fetchone()
            await conn.execute(sql.SQL("UPDATE {} SET desired_revision=1,effective_revision=1 "
                                       "WHERE deployment_id=%s").format(
                self._table("module_configuration_heads")
            ), (self.store.deployment_id,))
            return self._configuration(row, include_value=False)

    async def _authorize_in(self, conn, *, authorization, authorization_guard,
                            operation_id: str, action_key: str, input_digest: str,
                            plan_digest: str | None = None) -> None:
        if authorization is None:
            if authorization_guard is not None:
                raise ForestError("WEB_AUTHORITY_TICKET_REQUIRED", "Management ticket is required", 403)
            return
        if not callable(authorization_guard):
            raise ForestError("WEB_AUTHORITY_TRANSACTION_REQUIRED", "Authorization transaction is absent", 503)
        await authorization_guard(
            conn, ticket=authorization, component_id=self.component_id,
            operation_id=operation_id, action_key=action_key,
            input_digest=input_digest, plan_digest=plan_digest,
        )

    async def plan(self, *, operation_id: str, expected_desired_revision: int,
                   value: object, validation_digest: str, authorization=None,
                   authorization_guard=None, authorization_input_digest: str | None = None) -> dict:
        payload_id, payload_digest = self._payload(value)
        target_generation_id = getattr(authorization, "target_generation_id", None)
        target_graph_revision = getattr(authorization, "target_graph_revision", None)
        input_digest = digest_json([
            "forest.deployment.module-configuration.plan/1", self.component_id,
            expected_desired_revision, value, validation_digest,
            target_generation_id, target_graph_revision,
        ])
        plan_id = "config-plan:" + hashlib.sha256(
            f"{self.store.deployment_id}\0{self.component_id}\0{operation_id}".encode()
        ).hexdigest()
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize_in(
                conn, authorization=authorization, authorization_guard=authorization_guard,
                operation_id=operation_id, action_key="configuration.plan",
                input_digest=authorization_input_digest or digest_json({
                    "expected_desired_revision": expected_desired_revision, "value": value,
                }),
            )
            head = await self._head_in(conn, lock=True)
            existing = await (await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE deployment_id=%s AND operation_id=%s").format(
                    self._table("module_configuration_plans")
                ), (self.store.deployment_id, operation_id),
            )).fetchone()
            if existing is not None:
                if (existing["input_digest"] != input_digest
                        or existing["expected_desired_revision"] != expected_desired_revision):
                    raise ForestError("MODULE_CONFIGURATION_OPERATION_CONFLICT", "Plan identity differs", 409)
                return dict(existing)
            if head["desired_revision"] != expected_desired_revision:
                raise ForestError("MODULE_CONFIGURATION_CONFLICT", "Desired configuration changed", 409)
            await self._assert_immutable_in(conn, value)
            row = await (await conn.execute(
                sql.SQL("INSERT INTO {}(deployment_id,plan_id,operation_id,input_digest,"
                        "expected_desired_revision,target_generation_id,target_graph_revision,"
                        "config_value,config_payload_id,"
                        "config_payload_digest,validation_digest) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *").format(
                    self._table("module_configuration_plans")
                ), (self.store.deployment_id, plan_id, operation_id, input_digest,
                    expected_desired_revision, target_generation_id,
                    target_graph_revision, Jsonb(value), payload_id,
                    payload_digest, validation_digest),
            )).fetchone()
            return dict(row)

    async def apply(self, *, operation_id: str, plan_id: str,
                    expected_desired_revision: int, authorization=None,
                    authorization_guard=None, expected_validation_digest=None,
                    target_generation_id=None,
                    target_graph_revision=None) -> EffectiveModuleConfiguration:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.apply_in(
                conn, operation_id=operation_id, plan_id=plan_id,
                expected_desired_revision=expected_desired_revision,
                authorization=authorization, authorization_guard=authorization_guard,
                expected_validation_digest=expected_validation_digest,
                target_generation_id=target_generation_id,
                target_graph_revision=target_graph_revision,
            )

    async def apply_in(self, conn, *, operation_id: str, plan_id: str,
                       expected_desired_revision: int, authorization=None,
                       authorization_guard=None, pre_commit_validator=None,
                       expected_validation_digest=None, target_generation_id=None,
                       target_graph_revision=None) -> EffectiveModuleConfiguration:
        input_digest = digest_json([
            "forest.deployment.module-configuration.apply/1", self.component_id,
            plan_id, expected_desired_revision,
        ])
        await self._authorize_in(
            conn, authorization=authorization, authorization_guard=authorization_guard,
            operation_id=operation_id, action_key="configuration.apply",
            input_digest=digest_json({"plan_id": plan_id,
                                      "expected_desired_revision": expected_desired_revision}),
            plan_digest=digest_json({"plan_id": plan_id}),
        )
        head = await self._head_in(conn, lock=True)
        existing = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE deployment_id=%s AND operation_id=%s").format(
                self._table("module_configuration_revisions")
            ), (self.store.deployment_id, operation_id),
        )).fetchone()
        if existing is not None:
            if (existing["input_digest"] != input_digest
                    or existing["previous_revision"] != expected_desired_revision
                    or existing["plan_id"] != plan_id):
                raise ForestError("MODULE_CONFIGURATION_OPERATION_CONFLICT", "Apply identity differs", 409)
            return self._configuration(existing, include_value=False)
        plan = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE deployment_id=%s AND plan_id=%s FOR UPDATE").format(
                self._table("module_configuration_plans")
            ), (self.store.deployment_id, plan_id),
        )).fetchone()
        if (plan is None or plan["state"] != "PLANNED"
                or plan["expected_desired_revision"] != expected_desired_revision
                or head["desired_revision"] != expected_desired_revision
                or plan["target_generation_id"] != target_generation_id
                or plan["target_graph_revision"] != target_graph_revision
                or (expected_validation_digest is not None and
                    plan["validation_digest"] != expected_validation_digest)):
            raise ForestError("MODULE_CONFIGURATION_CONFLICT", "Plan is stale or consumed", 409)
        if self._payload(plan["config_value"]) != (
            plan["config_payload_id"], plan["config_payload_digest"]
        ):
            raise ForestError("MODULE_CONFIGURATION_CORRUPT", "Plan value differs from its digest", 500)
        if pre_commit_validator is not None:
            await pre_commit_validator(
                plan["config_value"], expected_desired_revision + 1,
                plan["config_payload_digest"],
            )
        await self._assert_immutable_in(conn, plan["config_value"])
        revision = expected_desired_revision + 1
        row = await (await conn.execute(
            sql.SQL("INSERT INTO {}(deployment_id,revision,previous_revision,operation_id,"
                    "input_digest,plan_id,config_value,config_payload_id,"
                    "config_payload_digest,validation_digest) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *").format(
                self._table("module_configuration_revisions")
            ), (self.store.deployment_id, revision, expected_desired_revision,
                operation_id, input_digest, plan_id, Jsonb(plan["config_value"]),
                plan["config_payload_id"], plan["config_payload_digest"],
                plan["validation_digest"]),
        )).fetchone()
        await conn.execute(sql.SQL("UPDATE {} SET state='APPLIED',applied_revision=%s,"
                                   "applied_at=clock_timestamp() WHERE deployment_id=%s AND plan_id=%s").format(
            self._table("module_configuration_plans")
        ), (revision, self.store.deployment_id, plan_id))
        await conn.execute(sql.SQL("UPDATE {} SET desired_revision=%s,updated_at=clock_timestamp() "
                                   "WHERE deployment_id=%s").format(
            self._table("module_configuration_heads")
        ), (revision, self.store.deployment_id))
        return self._configuration(row, include_value=False)

    async def activate_in(self, conn, *, operation_id: str, revision: int,
                          generation_id: str, expected_payload_id: str,
                          expected_payload_digest: str) -> EffectiveModuleConfiguration:
        head = await self._head_in(conn, lock=True)
        input_digest = digest_json([
            "forest.deployment.module-configuration.activate/1", self.component_id,
            revision, generation_id, expected_payload_id, expected_payload_digest,
        ])
        existing = await (await conn.execute(
            sql.SQL("SELECT * FROM {} WHERE deployment_id=%s AND operation_id=%s").format(
                self._table("module_configuration_activations")
            ), (self.store.deployment_id, operation_id),
        )).fetchone()
        if existing is not None:
            if (existing["input_digest"], existing["revision"], existing["generation_id"]) != (
                input_digest, revision, generation_id
            ):
                raise ForestError("MODULE_CONFIGURATION_OPERATION_CONFLICT", "Activation identity differs", 409)
            return await self._revision_in(conn, revision, include_value=False)
        if head["effective_revision"] < 1 or (
            head["desired_revision"] != revision
            and head["effective_revision"] != revision
        ):
            raise ForestError("MODULE_CONFIGURATION_CONFLICT", "Desired configuration changed", 409)
        configuration = await self._revision_in(conn, revision, include_value=False)
        if (configuration.payload_id != expected_payload_id
                or configuration.payload_digest != expected_payload_digest):
            raise ForestError("MODULE_CONFIGURATION_IDENTITY_CONFLICT", "Generation configuration differs", 409)
        await conn.execute(sql.SQL("INSERT INTO {}(deployment_id,operation_id,input_digest,revision,"
                                   "expected_effective_revision,generation_id) "
                                   "VALUES (%s,%s,%s,%s,%s,%s)").format(
            self._table("module_configuration_activations")
        ), (self.store.deployment_id, operation_id, input_digest, revision,
            head["effective_revision"], generation_id))
        if head["effective_revision"] != revision:
            await conn.execute(sql.SQL("UPDATE {} SET effective_revision=%s,"
                                       "updated_at=clock_timestamp() WHERE deployment_id=%s").format(
                self._table("module_configuration_heads")
            ), (revision, self.store.deployment_id))
        return configuration
