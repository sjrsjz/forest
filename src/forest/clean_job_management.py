"""Job-owned Web management query and action contract."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from forest.clean_management_contract import (
    CleanManagementActionResult,
    CleanModuleConfigurationContract,
)
from forest.errors import ForestError


class CleanJobManagementContract(CleanModuleConfigurationContract):
    protocol = "forest.job-management/1"

    def __init__(self, configuration, *, runtime, schema: dict) -> None:
        super().__init__(configuration, schema=schema, validator=self._validate_configuration)
        self.runtime = runtime

    @staticmethod
    def _validate_configuration(value: object) -> None:
        if value != {"protocol": "forest.job.scheduler/1"}:
            raise ForestError(
                "MODULE_CONFIGURATION_INVALID", "Job configuration is invalid", 422
            )

    def describe(self) -> dict:
        value = super().describe()
        value["queries"] = [
            {"query_id": "jobs.types"},
            {"query_id": "jobs.query", "default_limit": 50, "max_limit": 100},
            {"query_id": "jobs.read"},
        ]
        value["actions"] = [
            {"action_id": "jobs.run", "priority": "P1"},
            {"action_id": "jobs.cancel", "priority": "P0"},
        ]
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

    async def query(self, query_id: str, payload: dict) -> object:
        if not isinstance(payload, dict):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Job query input must be an object", 422
            )
        if query_id == "jobs.types":
            if payload:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Job type query takes no fields", 422
                )
            return {"items": self.runtime.job_types()}
        if query_id == "jobs.query":
            allowed = {
                "states",
                "job_type",
                "requested_by",
                "snapshot_created_at",
                "before_created_at",
                "before_job_id",
                "limit",
            }
            if set(payload) - allowed:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID",
                    "Job query contains unknown fields",
                    422,
                )
            values = {key: value for key, value in payload.items() if value not in (None, "")}
            states = values.get("states", [])
            if not isinstance(states, list) or any(not isinstance(item, str) for item in states):
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Job states are invalid", 422
                )
            values["states"] = tuple(states)
            for field in ("snapshot_created_at", "before_created_at"):
                if field in values:
                    values[field] = self._datetime(values[field], field)
            return await self.runtime.query_jobs(**values)
        if query_id == "jobs.read":
            if set(payload) != {"job_id"} or not isinstance(payload["job_id"], str):
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Job identity is invalid", 422
                )
            row = await self.runtime.inspect(payload["job_id"])
            row["input"] = await self._payload(
                row["job_id"], "input", row.get("input_payload_id"), row.get("input_payload_digest")
            )
            row["result"] = await self._payload(
                row["job_id"],
                "result",
                row.get("result_payload_id"),
                row.get("result_payload_digest"),
            )
            return row
        raise ForestError(
            "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Job query does not exist", 404
        )

    async def _payload(self, job_id: str, kind: str, payload_id, digest):
        if payload_id is None:
            return None
        body = await self.runtime.payloads.read_bytes(payload_id)
        if "sha256:" + hashlib.sha256(body).hexdigest() != digest:
            raise ForestError("JOB_PAYLOAD_INVALID", f"Job {kind} payload differs", 409)
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ForestError("JOB_PAYLOAD_INVALID", f"Job {kind} payload is invalid", 409) from exc
        return value

    async def action(
        self,
        action_id: str,
        *,
        operation_id: str,
        payload: dict,
    ) -> CleanManagementActionResult:
        if action_id == "jobs.run":
            if (
                not isinstance(payload, dict)
                or set(payload) != {"job_type", "input"}
                or not isinstance(payload["job_type"], str)
                or not isinstance(payload["input"], dict)
            ):
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Job run input is invalid", 422
                )
            saved = await self.runtime.enqueue(
                job_type=payload["job_type"],
                operation_id=operation_id,
                requested_by="management.web",
                input_value=payload["input"],
            )
            self.runtime.notify()
            return CleanManagementActionResult(saved)
        if action_id == "jobs.cancel":
            if not isinstance(payload, dict) or set(payload) - {
                "job_id",
                "expected_revision",
                "reason_code",
            } or not {"job_id", "expected_revision"} <= set(payload):
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Job cancellation input is invalid", 422
                )
            saved = await self.runtime.cancel(
                operation_id=operation_id,
                job_id=payload["job_id"],
                expected_revision=payload["expected_revision"],
                requested_by="management.web",
                reason_code=payload.get("reason_code", "ADMIN_REQUESTED"),
            )
            self.runtime.notify()
            return CleanManagementActionResult(
                {
                    key: saved[key]
                    for key in (
                        "operation_id",
                        "job_id",
                        "resulting_state",
                        "resulting_revision",
                        "created_at",
                    )
                }
            )
        raise ForestError(
            "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Job action does not exist", 404
        )
