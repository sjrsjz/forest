"""Deployment-owned sealed restore candidates and storage-head activation."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import ModuleManagementContract, ModuleOperation
from forest.management.reauthentication import reauthenticate_admin


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ListQuery(Body):
    limit: int = Field(default=100, ge=1, le=200)


class RestoreIdentity(Body):
    restore_id: str = Field(min_length=1, max_length=128)


class ActivationIdentity(Body):
    activation_id: str = Field(min_length=1, max_length=128)


class StageCreate(Body):
    backup_id: str = Field(min_length=1, max_length=128)
    verification_id: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class ActivationPlan(RestoreIdentity):
    target_ordinal: int = Field(ge=0, le=1023, strict=True)


class ActivationApply(Body):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class ActivationRetry(ActivationIdentity):
    current_password: str = Field(min_length=12, max_length=1024)


def _without_password(body):
    return body.model_dump(exclude={"current_password"})


class DeploymentRestoreManagementContract(ModuleManagementContract):
    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(
            (
                ModuleOperation(
                    "stages", "query", "deployment.restore.read", ListQuery,
                    self.stages, principal="admin",
                ),
                ModuleOperation(
                    "stage", "query", "deployment.restore.read", RestoreIdentity,
                    self.stage, principal="admin",
                ),
                ModuleOperation(
                    "stage.create", "action", "deployment.restore.create", StageCreate,
                    self.stage_create, authorization_payload=_without_password,
                    authorization_preparer=self._prepare_password,
                    principal="admin", success_status=202,
                ),
                ModuleOperation(
                    "activation.plan", "action", "deployment.restore.activation.plan",
                    ActivationPlan, self.activation_plan, principal="admin",
                ),
                ModuleOperation(
                    "activations", "query", "deployment.restore.activation.read", ListQuery,
                    self.activations, principal="admin",
                ),
                ModuleOperation(
                    "activation", "query", "deployment.restore.activation.read",
                    ActivationIdentity, self.activation, principal="admin",
                ),
                ModuleOperation(
                    "activation.apply", "action", "deployment.restore.activation.apply",
                    ActivationApply, self.activation_apply,
                    authorization_payload=_without_password,
                    authorization_preparer=self._prepare_password,
                    principal="admin", success_status=202,
                ),
                ModuleOperation(
                    "activation.retry", "action", "deployment.restore.activation.retry",
                    ActivationRetry, self.activation_retry,
                    authorization_payload=_without_password,
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

    async def stages(self, request, body):
        principal = self._principal(request)
        stages = await self.runtime.restores.list(**principal, limit=body.limit)
        backups = await self.runtime.backups.list(**principal, limit=body.limit)
        verifications = await self.runtime.verifications.list(**principal, limit=body.limit)
        receipts = {}
        for row in verifications:
            if row["receipt_valid"]:
                receipts.setdefault(row["backup_id"], row)
        eligible = [
            {
                "backup_id": row["backup_id"],
                "created_at": row["created_at"],
                "verification_id": receipts[row["backup_id"]]["verification_id"],
                "verification_expires_at": receipts[row["backup_id"]]["expires_at"],
            }
            for row in backups
            if row["state"] == "READY" and row["backup_id"] in receipts
        ]
        return {"restore_stages": stages, "eligible_backups": eligible}

    async def stage(self, request, body):
        return await self.runtime.restores.read(
            restore_id=body.restore_id, **self._principal(request)
        )

    async def stage_create(self, request, body):
        return await self.runtime.restores.create(
            backup_id=body.backup_id, verification_id=body.verification_id,
            expected_password_hash=request.prepared, idempotency_key=request.operation_id,
            **self._principal(request),
        )

    async def activation_plan(self, request, body):
        return await self.runtime.activations.plan(
            restore_id=body.restore_id, target_ordinal=body.target_ordinal,
            **self._principal(request),
        )

    async def activations(self, request, body):
        rows = await self.runtime.activations.list(**self._principal(request))
        return {"restore_activations": rows[: body.limit]}

    async def activation(self, request, body):
        return await self.runtime.activations.read(
            activation_id=body.activation_id, **self._principal(request)
        )

    async def activation_apply(self, request, body):
        return await self.runtime.activations.apply(
            plan_id=body.plan_id, plan_digest=body.plan_digest,
            expected_password_hash=request.prepared, idempotency_key=request.operation_id,
            **self._principal(request),
        )

    async def activation_retry(self, request, body):
        return await self.runtime.activations.retry(
            activation_id=body.activation_id, expected_password_hash=request.prepared,
            idempotency_key=request.operation_id, **self._principal(request),
        )
