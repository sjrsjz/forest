"""Deployment-owned management contract for redacted audit facts."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

from forest.management.module_api import ModuleManagementContract, ModuleOperation


class DeploymentAuditQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    before: str | None = Field(default=None, min_length=1, max_length=512)
    limit: int = Field(default=50, ge=1, le=200)
    action: str | None = Field(default=None, min_length=1, max_length=256)
    resource_prefix: str | None = Field(default=None, min_length=1, max_length=256)
    actor_id: str | None = Field(default=None, min_length=1, max_length=256)

    @field_validator("action", "resource_prefix", "actor_id")
    @classmethod
    def printable_selector(cls, value):
        if value is not None and any(ord(char) < 32 for char in value):
            raise ValueError("control characters are forbidden")
        return value


class DeploymentAuditManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation(
                    "records",
                    "query",
                    "deployment.audit.read",
                    DeploymentAuditQuery,
                    self.records,
                    principal="admin",
                ),
            )
        )

    async def records(self, request, body):
        return await request.module.repository.read(
            subject_id=request.principal["subject_id"],
            session_token_hash=request.principal["session_token_hash"],
            **request.payload,
        )
