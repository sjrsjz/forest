"""Authenticated multimodal memory configuration and observed runtime status."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.management.module_api import (
    EmptyInput,
    ModuleManagementContract,
    ModuleOperation,
)
from forest.memory_settings import MemorySettings
from forest.packages import digest_json


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MemoryPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: MemorySettings
    api_key: str | None = Field(default=None, min_length=1, max_length=4096)
    clear_api_key: bool = Field(default=False, strict=True)

    @field_validator("api_key")
    @classmethod
    def key(cls, value):
        if value is not None and any(ord(c) < 33 or ord(c) > 126 for c in value):
            raise ValueError("Use an ASCII API key without whitespace")
        return value

    @model_validator(mode="after")
    def replacement(self):
        if self.clear_api_key and self.api_key is not None:
            raise ValueError("Choose either a replacement key or clearing the key")
        return self


class Apply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class ForgetPlan(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    source_unit_ids: list[str] = Field(min_length=1, max_length=32)

    @field_validator("source_unit_ids")
    @classmethod
    def sources(cls, value):
        if len(set(value)) != len(value) or any(
            not isinstance(item, str)
            or not 1 <= len(item) <= 256
            or any(ord(char) < 32 for char in item)
            for item in value
        ):
            raise ValueError("Select distinct bounded source identifiers")
        return value


class ForgetApply(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class AssociationPlan(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    expected_revision: int = Field(ge=0, strict=True)
    active: bool = Field(strict=True)
    source_unit_ids: list[str] | None = Field(default=None, min_length=2, max_length=2)
    association_id: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def target(self):
        if self.active:
            if (
                self.association_id is not None
                or self.source_unit_ids is None
                or len(set(self.source_unit_ids)) != 2
                or any(
                    not 1 <= len(item) <= 256 or any(ord(char) < 32 for char in item)
                    for item in self.source_unit_ids
                )
            ):
                raise ValueError("Select exactly two distinct bounded sources")
        elif self.association_id is None or self.source_unit_ids is not None:
            raise ValueError("Retraction requires one association identity")
        return self


class AssociationApply(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class SourceQuery(Body):
    limit: int = Field(default=50, ge=1, le=100)
    after: str | None = Field(default=None, min_length=1, max_length=512)
    group_source_id: str | None = Field(default=None, min_length=1, max_length=256)


class AssociationQuery(Body):
    limit: int = Field(default=50, ge=1, le=100)
    after: str | None = Field(default=None, min_length=1, max_length=512)


class MemoryManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__((
            ModuleOperation("configuration", "query", "memory.config.read", EmptyInput, self.configuration),
            ModuleOperation("status", "query", "memory.read", EmptyInput, self.status),
            ModuleOperation("sources", "query", "memory.forget.read", SourceQuery, self.sources),
            ModuleOperation(
                "associations", "query", "memory.association.read", AssociationQuery,
                self.associations,
            ),
            ModuleOperation(
                "configuration.plan", "action", "memory.config.plan", MemoryPlan, self.plan,
                self.plan_payload,
            ),
            ModuleOperation("configuration.apply", "action", "memory.config.apply", Apply, self.apply),
            ModuleOperation("forget.plan", "action", "memory.forget.plan", ForgetPlan, self.forget_plan),
            ModuleOperation("forget.apply", "action", "memory.forget.apply", ForgetApply, self.forget_apply),
            ModuleOperation(
                "association.plan", "action", "memory.association.plan", AssociationPlan,
                self.association_plan,
            ),
            ModuleOperation(
                "association.apply", "action", "memory.association.apply", AssociationApply,
                self.association_apply,
            ),
        ))

    @staticmethod
    def plan_payload(body):
        secret = "" if body.clear_api_key else body.api_key
        changed = body.clear_api_key or secret is not None
        return {
            "expected_revision": body.expected_revision,
            "values": body.values.model_dump(),
            "credentials_changed": changed,
            "secret_digest": digest_json({"api_key": secret}),
        }

    async def configuration(self, request, body):
        return await request.module.config.read(request.ticket, request.operation_id)

    async def status(self, request, body):
        return await request.module.inspect(request.ticket, request.operation_id, request.payload)

    async def sources(self, request, body):
        return await request.module.administration.read(
            request.ticket, request.operation_id, request.payload
        )

    async def associations(self, request, body):
        return await request.module.associations.read(
            request.ticket, request.operation_id, request.payload
        )

    async def plan(self, request, body):
        secret = "" if body.clear_api_key else body.api_key
        changed = body.clear_api_key or secret is not None
        return await request.module.config.plan(
            request.ticket,
            request.operation_id,
            request.payload,
            secret_ciphertext=request.web.catalog.vault.encrypt(secret) if changed else None,
        )

    async def apply(self, request, body):
        result = await request.module.config.apply(
            request.ticket, request.operation_id, request.payload
        )
        if result is not None and not request.bot.control.is_unavailable(result):
            request.module.notify()
        return result

    async def forget_plan(self, request, body):
        return await request.module.administration.plan(
            request.ticket, request.operation_id, request.payload
        )

    async def forget_apply(self, request, body):
        result = await request.module.administration.apply(
            request.ticket,
            request.operation_id,
            request.payload,
            lease=request.bot.kernel._authority(),
        )
        request.module.notify()
        return result

    async def association_plan(self, request, body):
        return await request.module.associations.plan(
            request.ticket, request.operation_id, request.payload
        )

    async def association_apply(self, request, body):
        result = await request.module.associations.apply(
            request.ticket,
            request.operation_id,
            request.payload,
            lease=request.bot.kernel._authority(),
        )
        request.module.notify()
        return result
