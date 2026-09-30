"""Module-owned desired/effective configuration backed by ``kernel.payload``."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource


@dataclass(frozen=True)
class EffectiveModuleConfiguration:
    revision: int
    payload_id: str
    payload_digest: str
    validation_digest: str
    value: object | None = None


_MISSING = object()


def _pointer_value(value: object, path: str) -> object:
    for token in path[1:].split("/"):
        key = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, Mapping):
            value = value.get(key, _MISSING)
        elif isinstance(value, list) and key.isascii() and key.isdecimal():
            index = int(key)
            value = value[index] if index < len(value) else _MISSING
        else:
            return _MISSING
        if value is _MISSING:
            return _MISSING
    return value


class ModuleConfigurationRepository:
    """Shared protocol code operating only on one module's private schema."""

    def __init__(
        self,
        store,
        *,
        component_id: str,
        namespace: str,
        payloads: KernelPayloadRepository,
        immutable_paths: tuple[str, ...] = (),
    ) -> None:
        if not component_id or not namespace:
            raise ValueError("module configuration identity is required")
        self.store = store
        self.component_id = component_id
        self.namespace = namespace
        self.payloads = payloads
        self.immutable_paths = immutable_paths

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    async def ensure_initialized(
        self,
        *,
        operation_id: str,
        value: object,
        validation_digest: str,
    ) -> EffectiveModuleConfiguration:
        """Create revision one only for an uninitialized module authority."""

        try:
            return await self.desired()
        except ForestError as exc:
            if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
                raise
        return await self.initialize(
            operation_id=operation_id,
            value=value,
            validation_digest=validation_digest,
        )

    async def initialize(
        self,
        *,
        operation_id: str,
        value: object,
        validation_digest: str,
    ) -> EffectiveModuleConfiguration:
        input_digest = digest_json(
            ["forest.module-configuration.initialize/1", self.component_id, value, validation_digest]
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            head = await self._head_in(conn, lock=True)
            existing = await self._revision_by_operation_in(conn, operation_id)
            if existing is not None:
                self._guard_revision(existing, input_digest, previous_revision=0, plan_id=None)
                return self._configuration(existing)
            if head["desired_revision"] != 0 or head["effective_revision"] != 0:
                raise ForestError(
                    "MODULE_CONFIGURATION_ALREADY_INITIALIZED",
                    "Module configuration already has an effective revision",
                    409,
                )
            payload = await self._put_value_in(
                conn,
                operation_id=operation_id,
                owner_id="revision:1",
                source_revision=1,
                value=value,
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,revision,previous_revision,operation_id,"
                        "input_digest,plan_id,config_payload_id,config_payload_digest,"
                        "validation_digest) VALUES (%s,1,0,%s,%s,NULL,%s,%s,%s) RETURNING *"
                    ).format(self._table("module_configuration_revisions")),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        payload.payload_id,
                        payload.content_digest,
                        validation_digest,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET desired_revision=1,effective_revision=1,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s"
                ).format(self._table("module_configuration_heads")),
                (self.store.bot_id,),
            )
            return self._configuration(row)

    async def plan(
        self,
        *,
        operation_id: str,
        expected_desired_revision: int,
        value: object,
        validation_digest: str,
        authorization=None,
        authorization_guard=None,
        authorization_input_digest: str | None = None,
    ) -> dict:
        input_digest = digest_json(
            [
                "forest.module-configuration.plan/1",
                self.component_id,
                expected_desired_revision,
                value,
                validation_digest,
            ]
        )
        plan_id = "config-plan:" + hashlib.sha256(
            f"{self.store.bot_id}\0{self.component_id}\0{operation_id}".encode()
        ).hexdigest()
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._authorize_management_in(
                conn,
                authorization=authorization,
                authorization_guard=authorization_guard,
                operation_id=operation_id,
                action_key="configuration.plan",
                input_digest=authorization_input_digest
                or digest_json(
                    {"expected_desired_revision": expected_desired_revision, "value": value}
                ),
                plan_digest=None,
            )
            head = await self._head_in(conn, lock=True)
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                        self._table("module_configuration_plans")
                    ),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                self._guard_plan(
                    existing,
                    input_digest,
                    expected_desired_revision,
                    validation_digest,
                )
                return dict(existing)
            if (
                head["desired_revision"] != expected_desired_revision
                or head["effective_revision"] != expected_desired_revision
            ):
                raise ForestError(
                    "MODULE_CONFIGURATION_CONFLICT",
                    "Configuration head changed or still awaits Generation activation",
                    409,
                )
            await self._assert_immutable_in(conn, value)
            payload = await self._put_value_in(
                conn,
                operation_id=operation_id,
                owner_id=f"plan:{plan_id}",
                source_revision=expected_desired_revision + 1,
                value=value,
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,plan_id,operation_id,input_digest,"
                        "expected_desired_revision,config_payload_id,config_payload_digest,"
                        "validation_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("module_configuration_plans")),
                    (
                        self.store.bot_id,
                        plan_id,
                        operation_id,
                        input_digest,
                        expected_desired_revision,
                        payload.payload_id,
                        payload.content_digest,
                        validation_digest,
                    ),
                )
            ).fetchone()
            return dict(row)

    async def apply(
        self,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
        authorization=None,
        authorization_guard=None,
    ) -> EffectiveModuleConfiguration:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.apply_in(
                conn,
                operation_id=operation_id,
                plan_id=plan_id,
                expected_desired_revision=expected_desired_revision,
                authorization=authorization,
                authorization_guard=authorization_guard,
            )

    async def apply_in(
        self,
        conn,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
        authorization=None,
        authorization_guard=None,
    ) -> EffectiveModuleConfiguration:
        """Commit desired in the caller's transaction for composed UnitOfWork plans."""

        input_digest = digest_json(
            [
                "forest.module-configuration.apply/1",
                self.component_id,
                plan_id,
                expected_desired_revision,
            ]
        )
        await self._authorize_management_in(
            conn,
            authorization=authorization,
            authorization_guard=authorization_guard,
            operation_id=operation_id,
            action_key="configuration.apply",
            input_digest=digest_json(
                {
                    "plan_id": plan_id,
                    "expected_desired_revision": expected_desired_revision,
                }
            ),
            plan_digest=digest_json({"plan_id": plan_id}),
        )
        head = await self._head_in(conn, lock=True)
        existing = await self._revision_by_operation_in(conn, operation_id)
        if existing is not None:
            self._guard_revision(
                existing,
                input_digest,
                previous_revision=expected_desired_revision,
                plan_id=plan_id,
            )
            return self._configuration(existing)
        plan = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND plan_id=%s FOR UPDATE").format(
                    self._table("module_configuration_plans")
                ),
                (self.store.bot_id, plan_id),
            )
        ).fetchone()
        if (
            plan is None
            or plan["state"] != "PLANNED"
            or plan["expected_desired_revision"] != expected_desired_revision
            or head["desired_revision"] != expected_desired_revision
            or head["effective_revision"] != expected_desired_revision
        ):
            raise ForestError(
                "MODULE_CONFIGURATION_CONFLICT",
                "Configuration plan is stale or already consumed",
                409,
            )
        await self._verify_payload_in(conn, plan, f"plan:{plan_id}")
        if self.immutable_paths:
            body = await self.payloads.read_bytes_in(
                conn,
                plan["config_payload_id"],
            )
            await self._assert_immutable_in(conn, self._decode_value(body))
        revision = expected_desired_revision + 1
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,revision,previous_revision,operation_id,"
                    "input_digest,plan_id,config_payload_id,config_payload_digest,"
                    "validation_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                ).format(self._table("module_configuration_revisions")),
                (
                    self.store.bot_id,
                    revision,
                    expected_desired_revision,
                    operation_id,
                    input_digest,
                    plan_id,
                    plan["config_payload_id"],
                    plan["config_payload_digest"],
                    plan["validation_digest"],
                ),
            )
        ).fetchone()
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET state='APPLIED',applied_revision=%s,"
                "applied_at=clock_timestamp() WHERE bot_id=%s AND plan_id=%s"
            ).format(self._table("module_configuration_plans")),
            (revision, self.store.bot_id, plan_id),
        )
        await conn.execute(
            sql.SQL(
                "UPDATE {} SET desired_revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s"
            ).format(self._table("module_configuration_heads")),
            (revision, self.store.bot_id),
        )
        return self._configuration(row)

    async def applied_by_operation_in(
        self,
        conn,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
    ) -> EffectiveModuleConfiguration | None:
        """Read an exact committed apply for authorized, side-effect-free replay."""

        existing = await self._revision_by_operation_in(conn, operation_id)
        if existing is None:
            return None
        input_digest = digest_json(
            [
                "forest.module-configuration.apply/1",
                self.component_id,
                plan_id,
                expected_desired_revision,
            ]
        )
        self._guard_revision(
            existing,
            input_digest,
            previous_revision=expected_desired_revision,
            plan_id=plan_id,
        )
        return self._configuration(existing)

    async def activate_in(
        self,
        conn,
        *,
        operation_id: str,
        revision: int,
        generation_id: str,
        expected_payload_id: str,
        expected_payload_digest: str,
    ) -> EffectiveModuleConfiguration:
        """Atomically join configuration publication to RuntimeGraph publication."""

        head = await self._head_in(conn, lock=True)
        observed_effective_revision = head["effective_revision"]
        input_digest = digest_json(
            [
                "forest.module-configuration.activate/1",
                self.component_id,
                revision,
                generation_id,
                expected_payload_id,
                expected_payload_digest,
            ]
        )
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                    self._table("module_configuration_activations")
                ),
                (self.store.bot_id, operation_id),
            )
        ).fetchone()
        if existing is not None:
            expected = (input_digest, revision, generation_id)
            actual = tuple(
                existing[key] for key in ("input_digest", "revision", "generation_id")
            )
            if actual != expected:
                raise ForestError(
                    "MODULE_CONFIGURATION_OPERATION_CONFLICT",
                    "Activation operation identity names different facts",
                    409,
                )
            return await self._revision_in(conn, revision, verify=True)
        if head["desired_revision"] != revision or observed_effective_revision not in {
            revision,
            revision - 1,
        }:
            raise ForestError(
                "MODULE_CONFIGURATION_CONFLICT",
                "Configuration cannot become effective from the current head",
                409,
            )
        configuration = await self._revision_in(conn, revision, verify=True)
        if (
            configuration.payload_id != expected_payload_id
            or configuration.payload_digest != expected_payload_digest
        ):
            raise ForestError(
                "MODULE_CONFIGURATION_IDENTITY_CONFLICT",
                "Generation adopted a different configuration payload",
                409,
            )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,operation_id,input_digest,revision,"
                "expected_effective_revision,generation_id) VALUES (%s,%s,%s,%s,%s,%s)"
            ).format(self._table("module_configuration_activations")),
            (
                self.store.bot_id,
                operation_id,
                input_digest,
                revision,
                observed_effective_revision,
                generation_id,
            ),
        )
        if observed_effective_revision != revision:
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET effective_revision=%s,updated_at=clock_timestamp() "
                    "WHERE bot_id=%s"
                ).format(self._table("module_configuration_heads")),
                (revision, self.store.bot_id),
            )
        return configuration

    async def effective(self, *, include_value: bool = False) -> EffectiveModuleConfiguration:
        return await self._head_configuration("effective_revision", include_value=include_value)

    async def assert_immutable(self, value: object) -> None:
        """Check package-declared Bot-lifetime fields against the effective configuration."""

        if not self.immutable_paths:
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._assert_immutable_in(conn, value)

    async def _assert_immutable_in(self, conn, value: object) -> None:
        if not self.immutable_paths:
            return
        baseline = (await self.effective_in(conn, include_value=True)).value
        for path in self.immutable_paths:
            original = _pointer_value(baseline, path)
            candidate = _pointer_value(value, path)
            if (
                original is _MISSING
                or candidate is _MISSING
                or digest_json(original) != digest_json(candidate)
            ):
                raise ForestError(
                    "MODULE_CONFIGURATION_IMMUTABLE",
                    "Bot-lifetime module configuration cannot change",
                    409,
                    details={"path": path},
                )

    async def effective_in(
        self, conn, *, include_value: bool = False
    ) -> EffectiveModuleConfiguration:
        return await self._head_configuration_in(
            conn, "effective_revision", include_value=include_value
        )

    async def desired(self, *, include_value: bool = False) -> EffectiveModuleConfiguration:
        """Return the validated candidate a newly prepared Generation must adopt."""

        return await self._head_configuration("desired_revision", include_value=include_value)

    async def desired_checked(self) -> EffectiveModuleConfiguration:
        """Read desired and verify Bot-lifetime fields in one snapshot without writes."""

        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            desired = await self._head_configuration_in(
                conn, "desired_revision", include_value=True
            )
            await self._assert_immutable_in(conn, desired.value)
            return desired

    async def owner_id(self, revision: int) -> str:
        if type(revision) is not int or revision < 1:
            raise ForestError(
                "MODULE_CONFIGURATION_NOT_FOUND", "Configuration revision does not exist", 404
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._owner_id_in(conn, revision)

    async def _head_configuration(
        self, field: str, *, include_value: bool
    ) -> EffectiveModuleConfiguration:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._head_configuration_in(
                conn, field, include_value=include_value
            )

    async def _head_configuration_in(
        self, conn, field: str, *, include_value: bool
    ) -> EffectiveModuleConfiguration:
        head = await self._head_in(conn)
        revision = head[field]
        if revision == 0:
            raise ForestError(
                "MODULE_CONFIGURATION_UNINITIALIZED",
                "Module has no configuration revision",
                409,
            )
        saved = await self._revision_in(conn, revision, verify=True)
        if not include_value:
            return saved
        await self.payloads.verify_in(
            conn,
            payload_id=saved.payload_id,
            digest=saved.payload_digest,
            owner_component_id=self.component_id,
            owner_kind="module.configuration",
            owner_id=await self._owner_id_in(conn, saved.revision),
        )
        body = await self.payloads.read_bytes_in(
            conn,
            saved.payload_id,
        )
        value = self._decode_value(body)
        return EffectiveModuleConfiguration(
            saved.revision,
            saved.payload_id,
            saved.payload_digest,
            saved.validation_digest,
            value,
        )

    @staticmethod
    def _decode_value(body: bytes) -> object:
        try:
            return json.loads(body)
        except (UnicodeError, json.JSONDecodeError) as exc:  # database invariant
            raise ForestError(
                "MODULE_CONFIGURATION_CORRUPT", "Configuration payload is not JSON", 500
            ) from exc

    async def _authorize_management_in(
        self,
        conn,
        *,
        authorization,
        authorization_guard,
        operation_id: str,
        action_key: str,
        input_digest: str,
        plan_digest: str | None,
    ) -> None:
        if authorization is None:
            if authorization_guard is not None:
                raise ForestError(
                    "WEB_AUTHORITY_TICKET_REQUIRED",
                    "Management authorization ticket is required",
                    403,
                )
            return
        if not callable(authorization_guard):
            raise ForestError(
                "WEB_AUTHORITY_TRANSACTION_REQUIRED",
                "Management authorization transaction guard is unavailable",
                503,
            )
        await authorization_guard(
            conn,
            ticket=authorization,
            component_id=self.component_id,
            operation_id=operation_id,
            action_key=action_key,
            input_digest=input_digest,
            plan_digest=plan_digest,
        )

    async def _head_in(self, conn, *, lock: bool = False):
        suffix = sql.SQL(" FOR UPDATE") if lock else sql.SQL(" FOR SHARE")
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(
                    self._table("module_configuration_heads")
                )
                + suffix,
                (self.store.bot_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "MODULE_CONFIGURATION_UNAVAILABLE", "Module configuration head is absent", 503
            )
        return row

    async def _revision_by_operation_in(self, conn, operation_id: str):
        return await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                    self._table("module_configuration_revisions")
                ),
                (self.store.bot_id, operation_id),
            )
        ).fetchone()

    async def _revision_in(self, conn, revision: int, *, verify: bool):
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND revision=%s").format(
                    self._table("module_configuration_revisions")
                ),
                (self.store.bot_id, revision),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "MODULE_CONFIGURATION_NOT_FOUND", "Configuration revision does not exist", 404
            )
        if verify:
            await self._verify_payload_in(
                conn, row, await self._owner_id_in(conn, revision)
            )
        return self._configuration(row)

    async def _owner_id_in(self, conn, revision: int) -> str:
        row = await (
            await conn.execute(
                sql.SQL("SELECT plan_id FROM {} WHERE bot_id=%s AND revision=%s").format(
                    self._table("module_configuration_revisions")
                ),
                (self.store.bot_id, revision),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "MODULE_CONFIGURATION_NOT_FOUND", "Configuration revision does not exist", 404
            )
        return "revision:1" if row["plan_id"] is None else f"plan:{row['plan_id']}"

    async def _put_value_in(
        self,
        conn,
        *,
        operation_id: str,
        owner_id: str,
        source_revision: int,
        value: object,
    ):
        epoch = await self.payloads.epoch_in(conn)
        return await self.payloads.put_json_in(
            conn,
            value=value,
            owner_component_id=self.component_id,
            owner_kind="module.configuration",
            owner_id=owner_id,
            sources=(
                PayloadSource(
                    self.component_id,
                    "MODULE_CONFIGURATION",
                    operation_id,
                    source_revision,
                ),
            ),
            expected_deletion_epoch=epoch,
        )

    async def _verify_payload_in(self, conn, row, owner_id: str):
        return await self.payloads.verify_in(
            conn,
            payload_id=row["config_payload_id"],
            digest=row["config_payload_digest"],
            owner_component_id=self.component_id,
            owner_kind="module.configuration",
            owner_id=owner_id,
        )

    @staticmethod
    def _configuration(row) -> EffectiveModuleConfiguration:
        return EffectiveModuleConfiguration(
            row["revision"],
            row["config_payload_id"],
            row["config_payload_digest"],
            row["validation_digest"],
        )

    @staticmethod
    def _guard_revision(row, input_digest, *, previous_revision, plan_id) -> None:
        if (
            row["input_digest"] != input_digest
            or row["previous_revision"] != previous_revision
            or row["plan_id"] != plan_id
        ):
            raise ForestError(
                "MODULE_CONFIGURATION_OPERATION_CONFLICT",
                "Configuration operation identity names different facts",
                409,
            )

    @staticmethod
    def _guard_plan(row, input_digest, expected_revision, validation_digest) -> None:
        if (
            row["input_digest"] != input_digest
            or row["expected_desired_revision"] != expected_revision
            or row["validation_digest"] != validation_digest
        ):
            raise ForestError(
                "MODULE_CONFIGURATION_OPERATION_CONFLICT",
                "Configuration plan operation names different facts",
                409,
            )
