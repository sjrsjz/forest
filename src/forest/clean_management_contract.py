"""Generation-bound management contracts for clean module configuration."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
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

    def query_required_role(self, query_id: str) -> str:
        return "AUDITOR"

    async def query_authorized(
        self, query_id: str, payload: dict, *, authorization=None
    ) -> object:
        return await self.query(query_id, payload)

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
        authorization=None,
        authorization_guard=None,
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
            authorization=authorization,
            authorization_guard=authorization_guard,
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
        authorization=None,
        authorization_guard=None,
    ) -> dict:
        saved = await self.configuration.apply(
            operation_id=operation_id,
            plan_id=plan_id,
            expected_desired_revision=expected_desired_revision,
            authorization=authorization,
            authorization_guard=authorization_guard,
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
