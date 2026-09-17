"""Module-owned management contract for redacted Bot audit metadata."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import ModuleManagementContract, ModuleOperation


class AuditQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"
    )
    before: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"
    )
    limit: int = Field(default=50, ge=1, le=100)


class AuditManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation(
                    "records", "query", "audit.read", AuditQuery, self.records
                ),
            )
        )

    async def records(self, request, body):
        return await request.module.store.read(
            request.ticket, request.operation_id, request.payload
        )
