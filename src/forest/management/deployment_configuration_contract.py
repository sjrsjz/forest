"""Deployment configuration schema and module-owned management contract."""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.management.module_api import ModuleManagementContract, ModuleOperation

DEFAULT_VALUES = {"display_name": "Forest", "session_ttl_minutes": 480}
CONFIG_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["display_name", "session_ttl_minutes"],
    "properties": {
        "display_name": {"type": "string", "minLength": 1, "maxLength": 80},
        "session_ttl_minutes": {"type": "integer", "minimum": 5, "maximum": 1440},
    },
}


def canonical_digest(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(data.encode()).hexdigest()


SCHEMA_DIGEST = canonical_digest(CONFIG_SCHEMA)


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PlanBody(Body):
    values: dict[str, Any]
    expected_revision: int = Field(ge=0, strict=True)


class ApplyBody(Body):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class DeploymentConfigurationManagementContract(ModuleManagementContract):
    def __init__(self, store):
        self.store = store
        super().__init__(
            (
                ModuleOperation(
                    "configuration", "query", "deployment.configuration.read", Body,
                    self.configuration,
                ),
                ModuleOperation(
                    "configuration.plan", "action", "deployment.configuration.plan", PlanBody,
                    self.plan, principal="admin",
                ),
                ModuleOperation(
                    "configuration.apply", "action", "deployment.configuration.apply",
                    ApplyBody, self.apply, principal="admin",
                ),
            )
        )

    async def configuration(self, request, body):
        state = await self.store.read_config("deployment")
        return {
            **state,
            "schema": CONFIG_SCHEMA,
            "schema_digest": SCHEMA_DIGEST,
            "defaults": DEFAULT_VALUES,
        }

    async def plan(self, request, body):
        issues = list(Draft202012Validator(CONFIG_SCHEMA).iter_errors(body.values))
        if issues:
            raise ForestError(
                "VALIDATION_FAILED",
                "配置字段不符合要求。",
                422,
                [{"path": list(issue.path), "rule": issue.validator} for issue in issues],
            )
        current = await self.store.read_config("deployment")
        if current["head_revision"] != body.expected_revision:
            raise ForestError(
                "REVISION_CONFLICT",
                "配置已被修改，请先重新读取。",
                409,
                {"head_revision": current["head_revision"]},
            )
        plan_id = "plan_" + secrets.token_hex(16)
        digest = canonical_digest(
            {
                "resource_key": "deployment",
                "values": body.values,
                "expected_revision": body.expected_revision,
                "schema_digest": SCHEMA_DIGEST,
                "subject_id": request.principal["subject_id"],
            }
        )
        await self.store.save_plan(
            plan_id,
            request.principal["subject_id"],
            "deployment",
            body.expected_revision,
            body.values,
            SCHEMA_DIGEST,
            digest,
            datetime.now(UTC) + timedelta(minutes=10),
        )
        before = current["effective"]["values"] if current["effective"] else DEFAULT_VALUES
        diff = [
            {"field": key, "before": before[key], "after": value}
            for key, value in body.values.items()
            if before[key] != value
        ]
        return {
            "plan_id": plan_id,
            "plan_digest": digest,
            "expected_revision": body.expected_revision,
            "diff": diff,
            "effect": "显示名称立即用于后台；会话时长用于后续登录。",
        }

    async def apply(self, request, body):
        saved = await self.store.get_plan(
            body.plan_id, request.principal["subject_id"]
        )
        if saved is None:
            raise ForestError("PLAN_NOT_FOUND", "变更计划不存在或不可访问。", 404)
        return await self.store.apply_config(
            "deployment",
            saved["values"],
            body.expected_revision,
            request.operation_id,
            request.principal["subject_id"],
            body.plan_digest,
            plan_id=body.plan_id,
            schema_digest=SCHEMA_DIGEST,
            session_token_hash=request.principal["session_token_hash"],
        )
