"""Delivery-owned UNKNOWN inspection and evidence reconciliation contract."""

from __future__ import annotations

import hashlib
from datetime import datetime

from forest.clean_management_contract import (
    CleanManagementActionResult,
    CleanModuleConfigurationContract,
)
from forest.errors import ForestError
from forest.packages import digest_json


class CleanDeliveryManagementContract(CleanModuleConfigurationContract):
    protocol = "forest.delivery-management/1"

    def __init__(self, configuration, *, runtime, schema: dict) -> None:
        super().__init__(configuration, schema=schema, validator=self._validate_configuration)
        self.runtime = runtime

    @staticmethod
    def _validate_configuration(value: object) -> None:
        if value != {"protocol": "forest.delivery/1"}:
            raise ForestError(
                "MODULE_CONFIGURATION_INVALID", "Delivery configuration is invalid", 422
            )

    def describe(self) -> dict:
        value = super().describe()
        value["queries"] = [
            {"query_id": "delivery.unknown.query", "default_limit": 50, "max_limit": 200},
            {"query_id": "delivery.unknown.read"},
        ]
        value["actions"] = [
            {"action_id": "delivery.unknown.resolve", "priority": "P0"}
        ]
        return value

    async def query(self, query_id: str, payload: dict) -> object:
        if not isinstance(payload, dict):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Delivery query input must be an object", 422
            )
        if query_id == "delivery.unknown.query":
            allowed = {
                "lane_kind",
                "channel_ref",
                "snapshot_updated_at",
                "before_updated_at",
                "before_intent_id",
                "limit",
            }
            if set(payload) - allowed:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID",
                    "Delivery UNKNOWN query contains unknown fields",
                    422,
                )
            values = {key: item for key, item in payload.items() if item not in (None, "")}
            for field in ("snapshot_updated_at", "before_updated_at"):
                if field in values:
                    values[field] = self._datetime(values[field], field)
            return await self.runtime.query_unknown(**values)
        if query_id == "delivery.unknown.read":
            if set(payload) != {"intent_id"}:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Delivery identity is invalid", 422
                )
            return await self.runtime.inspect_unknown(payload["intent_id"])
        raise ForestError(
            "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Delivery query does not exist", 404
        )

    async def action(
        self,
        action_id: str,
        *,
        operation_id: str,
        payload: dict,
        authorization=None,
        authorization_guard=None,
    ) -> CleanManagementActionResult:
        if action_id != "delivery.unknown.resolve":
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Delivery action does not exist", 404
            )
        if (
            not isinstance(payload, dict)
            or set(payload) != {"intent_id", "attempt_id", "resolution", "evidence"}
            or not isinstance(payload["evidence"], dict)
        ):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Delivery reconciliation input is invalid", 422
            )
        intent_id = self._text(payload["intent_id"], "intent_id", 256)
        attempt_id = self._text(payload["attempt_id"], "attempt_id", 256)
        resolution = payload["resolution"]
        mapping = {
            "SENT": ("SENT", None),
            "NOT_APPLIED_RETRY": (
                "RETRYABLE_NOT_APPLIED",
                "DELIVERY_RECONCILED_NOT_APPLIED",
            ),
            "NOT_APPLIED_FINAL": (
                "REJECTED_NOT_APPLIED",
                "DELIVERY_RECONCILED_REJECTED",
            ),
        }
        if resolution not in mapping:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Delivery resolution is invalid", 422
            )
        supplied = payload["evidence"]
        if set(supplied) - {"source_kind", "source_ref", "note"} or not {
            "source_kind",
            "source_ref",
        } <= set(supplied):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Reconciliation evidence is invalid", 422
            )
        source_kind = supplied["source_kind"]
        if source_kind not in {
            "PROVIDER_QUERY",
            "PLATFORM_OBSERVATION",
            "OPERATOR_OBSERVATION",
        }:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Evidence source kind is invalid", 422
            )
        evidence = {
            "protocol": "forest.delivery-reconciliation-evidence/1",
            "source_kind": source_kind,
            "source_ref": self._text(supplied["source_ref"], "source_ref", 1024),
            "recorded_by": self._text(
                getattr(authorization, "web_subject_ref", "management.web"),
                "recorded_by",
                256,
            ),
        }
        if "note" in supplied:
            evidence["note"] = self._text(supplied["note"], "note", 4096)
        observed_status, error_code = mapping[resolution]
        saved = await self.runtime.resolve_unknown(
            observation_operation_id=operation_id,
            effect_observation_operation_id=(
                "delivery-effect-reconcile:"
                + hashlib.sha256(operation_id.encode("utf-8")).hexdigest()
            ),
            intent_id=intent_id,
            attempt_id=attempt_id,
            observed_status=observed_status,
            error_code=error_code,
            evidence_details=evidence,
            authorization=authorization,
            authorization_guard=authorization_guard,
            authorization_input_digest=digest_json(payload),
        )
        if observed_status == "RETRYABLE_NOT_APPLIED" and saved["accepted"]:
            self.runtime.notify()
        return CleanManagementActionResult(
            {
                key: saved[key]
                for key in (
                    "observation_operation_id",
                    "attempt_id",
                    "observed_status",
                    "accepted",
                    "previous_state",
                    "resulting_state",
                    "conflict_code",
                    "observed_at",
                )
            }
        )

    @staticmethod
    def _text(value: object, field: str, limit: int) -> str:
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value.encode("utf-8")) > limit
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", f"{field} is invalid", 422
            )
        return value

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
