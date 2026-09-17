"""Host-owned physical storage inventory and reclamation contract."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import ModuleManagementContract, ModuleOperation
from forest.management.reauthentication import reauthenticate_admin


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ListQuery(Body):
    limit: int = Field(default=100, ge=1, le=200)


class CleanupIdentity(Body):
    cleanup_id: str = Field(min_length=1, max_length=128)


class CleanupPlan(Body):
    resource_kind: Literal["BACKUP_ARTIFACT", "RESTORE_STAGE", "BOT_STORAGE_GENERATION"]
    resource_id: str = Field(min_length=1, max_length=256)


class CleanupApply(Body):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class CleanupRetry(CleanupIdentity):
    current_password: str = Field(min_length=12, max_length=1024)


def _without_password(body):
    return body.model_dump(exclude={"current_password"})


class HostStorageManagementContract(ModuleManagementContract):
    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(
            (
                ModuleOperation(
                    "generations", "query", "host.storage.read", Body,
                    self.generations, principal="admin",
                ),
                ModuleOperation(
                    "cleanup.plan", "action", "host.storage.cleanup.plan", CleanupPlan,
                    self.cleanup_plan, principal="admin",
                ),
                ModuleOperation(
                    "cleanups", "query", "host.storage.read", ListQuery,
                    self.cleanups, principal="admin",
                ),
                ModuleOperation(
                    "cleanup", "query", "host.storage.read", CleanupIdentity,
                    self.cleanup, principal="admin",
                ),
                ModuleOperation(
                    "cleanup.apply", "action", "host.storage.cleanup.apply", CleanupApply,
                    self.cleanup_apply, authorization_payload=_without_password,
                    authorization_preparer=self._prepare_password,
                    principal="admin", success_status=202,
                ),
                ModuleOperation(
                    "cleanup.retry", "action", "host.storage.cleanup.retry", CleanupRetry,
                    self.cleanup_retry, authorization_payload=_without_password,
                    authorization_preparer=self._prepare_password,
                    principal="admin", success_status=202,
                ),
            )
        )

    async def _prepare_password(self, request, body):
        expected = await reauthenticate_admin(
            self.runtime.store,
            self.runtime.password_slots,
            request.principal["subject_id"],
            body.current_password,
        )
        return _without_password(body), expected

    @staticmethod
    def _principal(request):
        return {
            "subject_id": request.principal["subject_id"],
            "session_token_hash": request.principal["session_token_hash"],
        }

    async def generations(self, request, body):
        rows = await self.runtime.cleanup.list_generations(**self._principal(request))
        return {"storage_generations": rows}

    async def cleanup_plan(self, request, body):
        return await self.runtime.cleanup.plan(
            kind=body.resource_kind, resource_id=body.resource_id,
            **self._principal(request),
        )

    async def cleanups(self, request, body):
        rows = await self.runtime.cleanup.list(
            **self._principal(request), limit=body.limit
        )
        return {"cleanups": rows}

    async def cleanup(self, request, body):
        return await self.runtime.cleanup.read(
            cleanup_id=body.cleanup_id, **self._principal(request)
        )

    async def cleanup_apply(self, request, body):
        return await self.runtime.cleanup.apply(
            plan_id=body.plan_id, plan_digest=body.plan_digest,
            expected_password_hash=request.prepared, idempotency_key=request.operation_id,
            **self._principal(request),
        )

    async def cleanup_retry(self, request, body):
        return await self.runtime.cleanup.retry(
            cleanup_id=body.cleanup_id, expected_password_hash=request.prepared,
            idempotency_key=request.operation_id, **self._principal(request),
        )
