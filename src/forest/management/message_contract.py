"""Module-owned management contract for ordinary-message routing."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation
from forest.storage.message_handler_policies import normalize_message_handler_policy


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BindingInput(Body):
    binding_id: str = Field(min_length=1, max_length=128)


class PolicyValues(Body):
    mode: Literal["DISABLED", "EVERYONE", "ADMINS", "USER_SET"]
    user_ids: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("user_ids")
    @classmethod
    def users(cls, value):
        normalized = normalize_message_handler_policy(
            {"mode": "USER_SET" if value else "DISABLED", "user_ids": value}
        )
        return normalized["user_ids"]

    @model_validator(mode="after")
    def selection(self):
        normalized = normalize_message_handler_policy(self.model_dump())
        self.user_ids = normalized["user_ids"]
        return self


class PolicyPlan(Body):
    binding_id: str = Field(min_length=1, max_length=128)
    component_id: str = Field(min_length=1, max_length=128)
    handler_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)
    values: PolicyValues
    current_password: str = Field(min_length=12, max_length=1024)


class PolicyApply(Body):
    binding_id: str = Field(min_length=1, max_length=128)
    component_id: str = Field(min_length=1, max_length=128)
    handler_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class LimitValues(Body):
    max_event_bytes: int = Field(ge=1, le=1048576, strict=True)
    max_pending_bot: int = Field(ge=1, le=100000, strict=True)
    max_pending_binding: int = Field(ge=1, le=10000, strict=True)
    max_pending_principal: int = Field(ge=1, le=1000, strict=True)
    max_handlers_per_chain: int = Field(ge=1, le=256, strict=True)
    principal_burst: int = Field(ge=1, le=1000, strict=True)
    principal_refill_seconds: int = Field(ge=1, le=86400, strict=True)
    deadline_seconds: int = Field(ge=1, le=3600, strict=True)

    @model_validator(mode="after")
    def ordered(self):
        if not self.max_pending_principal <= self.max_pending_binding <= self.max_pending_bot:
            raise ValueError("Principal, Binding and Bot pending limits must be ordered")
        return self


class LimitPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: LimitValues
    current_password: str = Field(min_length=12, max_length=1024)


class LimitApply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


class DiagnosticsInput(Body):
    binding_id: str | None = Field(
        default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"
    )
    limit: int = Field(default=50, ge=1, le=100)


class RecoveryInput(Body):
    limit: int = Field(default=128, ge=1, le=256, strict=True)


def _without_password(body: BaseModel) -> dict:
    return body.model_dump(exclude={"current_password"}, exclude_none=True)


class MessageRoutingManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation(
                    "bindings", "query", "binding.read", EmptyInput, self.bindings
                ),
                ModuleOperation(
                    "policies",
                    "query",
                    "message.handler.policy.read",
                    BindingInput,
                    self.policies,
                ),
                ModuleOperation(
                    "policy.plan",
                    "action",
                    "message.handler.policy.plan",
                    PolicyPlan,
                    self.plan_policy,
                    authorization_payload=_without_password,
                    authorization_preparer=self.reauthenticate,
                ),
                ModuleOperation(
                    "policy.apply",
                    "action",
                    "message.handler.policy.apply",
                    PolicyApply,
                    self.apply_policy,
                    authorization_payload=_without_password,
                    authorization_preparer=self.reauthenticate,
                ),
                ModuleOperation(
                    "limits",
                    "query",
                    "message.handler.limit.read",
                    EmptyInput,
                    self.limits,
                ),
                ModuleOperation(
                    "limit.plan",
                    "action",
                    "message.handler.limit.plan",
                    LimitPlan,
                    self.plan_limit,
                    authorization_payload=_without_password,
                    authorization_preparer=self.reauthenticate,
                ),
                ModuleOperation(
                    "limit.apply",
                    "action",
                    "message.handler.limit.apply",
                    LimitApply,
                    self.apply_limit,
                    authorization_payload=_without_password,
                    authorization_preparer=self.reauthenticate,
                ),
                ModuleOperation(
                    "diagnostics",
                    "query",
                    "message.handler.execution.read",
                    DiagnosticsInput,
                    self.diagnostics,
                ),
                ModuleOperation(
                    "recovery",
                    "action",
                    "message.handler.execution.recover",
                    RecoveryInput,
                    self.recover,
                ),
            )
        )

    async def reauthenticate(self, request, body):
        await request.web.reauthenticate_admin(
            request.principal["subject_id"], body.current_password
        )
        return request.payload, None

    async def bindings(self, request, body):
        rows = await request.module.bindings.list(request.ticket, request.operation_id)
        return {
            "bindings": [
                {**row, "registration_stage": "REGISTERED"} for row in rows
            ]
        }

    async def policies(self, request, body):
        return await request.module.policies.read(
            request.ticket, request.operation_id, body.binding_id
        )

    async def plan_policy(self, request, body):
        return await request.module.policies.plan(
            request.ticket, request.operation_id, request.payload
        )

    async def apply_policy(self, request, body):
        result = await request.module.policies.apply(
            request.ticket, request.operation_id, request.payload
        )
        if result is not None and not request.bot.control.is_unavailable(result):
            request.bot.runtime.notify()
        return result

    async def limits(self, request, body):
        return await request.module.limits.read(request.ticket, request.operation_id)

    async def plan_limit(self, request, body):
        return await request.module.limits.plan(
            request.ticket, request.operation_id, request.payload
        )

    async def apply_limit(self, request, body):
        result = await request.module.limits.apply(
            request.ticket, request.operation_id, request.payload
        )
        if result is not None and not request.bot.control.is_unavailable(result):
            request.bot.runtime.notify()
        return result

    async def diagnostics(self, request, body):
        return await request.module.diagnostics.read(
            request.ticket,
            request.operation_id,
            body.model_dump(exclude_none=True),
        )

    async def recover(self, request, body):
        result = await request.module.diagnostics.recover(
            request.ticket, request.operation_id, body.model_dump()
        )
        if result is not None and not request.bot.control.is_unavailable(result):
            request.bot.runtime.notify()
        return result
