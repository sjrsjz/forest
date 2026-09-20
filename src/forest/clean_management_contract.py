"""Generation-bound management contracts for clean module configuration."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from forest.errors import ForestError
from forest.packages import digest_json


@dataclass(frozen=True)
class CleanManagementActionResult:
    value: dict
    replace_runtime_graph: bool = False


class CleanModuleConfigurationContract:
    """Expose one module-owned configuration authority to management gateways."""

    protocol = "forest.module-configuration-management/1"

    def __init__(
        self,
        configuration,
        *,
        schema: dict,
        validator: Callable[[object], object] | None = None,
        reload_policy: str = "generation",
    ) -> None:
        if reload_policy not in {"dynamic", "generation", "restart"}:
            raise ValueError("Unsupported configuration reload policy")
        self.configuration = configuration
        self.schema = schema
        self.schema_digest = digest_json(schema)
        self.validator = validator
        self.reload_policy = reload_policy

    def describe(self) -> dict:
        return {
            "protocol": self.protocol,
            "configuration": {
                "schema": self.schema,
                "schema_digest": self.schema_digest,
                "reload_policy": self.reload_policy,
                "operations": ["configuration.read", "configuration.plan", "configuration.apply"],
            },
        }

    async def read(self) -> dict:
        desired = await self.configuration.desired(include_value=True)
        effective = await self.configuration.effective(include_value=True)
        return {
            "protocol": self.protocol,
            "component_id": self.configuration.component_id,
            "schema": self.schema,
            "schema_digest": self.schema_digest,
            "reload_policy": self.reload_policy,
            "desired": self._configuration(desired),
            "effective": self._configuration(effective),
        }

    async def plan(
        self,
        *,
        operation_id: str,
        expected_desired_revision: int,
        value: object,
    ) -> dict:
        if self.validator is not None:
            try:
                validated = self.validator(value)
                if inspect.isawaitable(validated):
                    await validated
            except ForestError:
                raise
            except (TypeError, ValueError) as exc:
                raise ForestError(
                    "MODULE_CONFIGURATION_INVALID",
                    "Module configuration is invalid",
                    422,
                    details={"reason": str(exc)},
                ) from None
        plan = await self.configuration.plan(
            operation_id=operation_id,
            expected_desired_revision=expected_desired_revision,
            value=value,
            validation_digest=self.schema_digest,
        )
        return {
            "plan_id": plan["plan_id"],
            "expected_desired_revision": plan["expected_desired_revision"],
            "input_digest": plan["input_digest"],
            "reload_policy": self.reload_policy,
            "state": plan["state"],
        }

    async def apply(
        self,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
    ) -> dict:
        saved = await self.configuration.apply(
            operation_id=operation_id,
            plan_id=plan_id,
            expected_desired_revision=expected_desired_revision,
        )
        return self._configuration(saved)

    @staticmethod
    def _configuration(value) -> dict:
        return {
            "revision": value.revision,
            "payload_id": value.payload_id,
            "payload_digest": value.payload_digest,
            "validation_digest": value.validation_digest,
            "value": value.value,
        }


class CleanMemoryManagementContract(CleanModuleConfigurationContract):
    """Memory-owned bounded source and relation queries plus configuration."""

    _source_fields = {
        "limit",
        "trunk_id",
        "author_subject_ref",
        "modality",
        "unit_kind",
        "occurred_from",
        "occurred_to",
        "snapshot_accepted_at",
        "after_occurred_at",
        "after_source_unit_id",
    }
    _relation_fields = {
        "endpoint_kind",
        "endpoint_id",
        "active",
        "eligible_only",
        "limit",
        "after_relation_id",
        "after_contribution_id",
        "snapshot_created_at",
    }

    def __init__(self, configuration, *, runtime, schema: dict, validator) -> None:
        super().__init__(configuration, schema=schema, validator=validator)
        self.runtime = runtime

    def describe(self) -> dict:
        value = super().describe()
        value["queries"] = [
            {"query_id": "memory.trunks.list", "max_items": 4096},
            {"query_id": "memory.sources.query", "default_limit": 100, "max_limit": 500},
            {"query_id": "memory.relations.query", "default_limit": 100, "max_limit": 500},
        ]
        return value

    async def query(self, query_id: str, payload: dict) -> object:
        if not isinstance(payload, dict):
            raise ForestError("MODULE_MANAGEMENT_INPUT_INVALID", "Query input must be an object", 422)
        if query_id == "memory.trunks.list":
            if payload:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Trunk query takes no fields", 422
                )
            return await self.runtime.list_trunks()
        if query_id == "memory.sources.query":
            values = self._fields(payload, self._source_fields)
            for field in (
                "occurred_from",
                "occurred_to",
                "snapshot_accepted_at",
                "after_occurred_at",
            ):
                if values.get(field) is not None:
                    values[field] = self._datetime(values[field], field)
            return await self.runtime.query_sources(**values)
        if query_id == "memory.relations.query":
            values = self._fields(payload, self._relation_fields)
            if values.get("snapshot_created_at") is not None:
                values["snapshot_created_at"] = self._datetime(
                    values["snapshot_created_at"], "snapshot_created_at"
                )
            return await self.runtime.query_relations(**values)
        raise ForestError(
            "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Memory query does not exist", 404
        )

    @staticmethod
    def _fields(payload: dict, allowed: set[str]) -> dict:
        if set(payload) - allowed:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Query contains unknown fields", 422
            )
        return {key: value for key, value in payload.items() if value is not None and value != ""}

    @staticmethod
    def _datetime(value: object, field: str) -> datetime:
        if not isinstance(value, str) or len(value) > 64:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", f"{field} is not an ISO timestamp", 422
            )
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", f"{field} is not an ISO timestamp", 422
            ) from None
        if parsed.tzinfo is None:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", f"{field} requires a timezone", 422
            )
        return parsed


class CleanAuditManagementContract(CleanModuleConfigurationContract):
    """Audit-owned metadata query projection plus immutable kernel configuration."""

    _fields_allowed = {
        "action",
        "producer_component_id",
        "origin_kind",
        "resource_kind",
        "resource_id",
        "operation_kind",
        "operation_id",
        "before",
        "limit",
    }

    def __init__(self, configuration, *, audit, schema: dict) -> None:
        super().__init__(configuration, schema=schema)
        self.audit = audit

    def describe(self) -> dict:
        value = super().describe()
        value["queries"] = [
            {"query_id": "audit.records.query", "default_limit": 50, "max_limit": 100}
        ]
        return value

    async def query(self, query_id: str, payload: dict) -> object:
        if query_id != "audit.records.query":
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Audit query does not exist", 404
            )
        if not isinstance(payload, dict) or set(payload) - self._fields_allowed:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Audit query contains unknown fields",
                422,
            )
        values = {
            key: value
            for key, value in payload.items()
            if value is not None and value != ""
        }
        for kind, identity in (
            ("resource_kind", "resource_id"),
            ("operation_kind", "operation_id"),
        ):
            if (kind in values) != (identity in values):
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID",
                    f"{kind} and {identity} must be supplied together",
                    422,
                )
        return await self.audit.scan(**values)
