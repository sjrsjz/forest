"""Authenticated model configuration, approval and Run management."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.agent_settings import AgentSettings
from forest.management.module_api import (
    EmptyInput,
    ModuleManagementContract,
    ModuleOperation,
)
from forest.packages import digest_json


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AgentPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: AgentSettings
    api_key: str | None = Field(default=None, min_length=1, max_length=4096)
    clear_api_key: bool = False

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


class Approval(Body):
    approval_id: str = Field(min_length=1, max_length=128)
    decision: Literal["APPROVE", "REJECT"]
    arguments_digest: str = Field(min_length=1, max_length=128)


class Cancel(Body):
    run_id: str = Field(min_length=1, max_length=128)
    expected_checkpoint_seq: int = Field(ge=0, strict=True)
    reason: str = Field(min_length=1, max_length=1024)


class Resolve(Body):
    run_id: str = Field(min_length=1, max_length=128)
    tool_call_id: str = Field(min_length=1, max_length=128)
    expected_checkpoint_seq: int = Field(ge=0, strict=True)
    arguments_digest: str = Field(min_length=1, max_length=128)
    status: Literal["SUCCEEDED", "FAILED"]
    result: dict


class AgentManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__((
            ModuleOperation("configuration", "query", "agent.config.read", EmptyInput, self.configuration),
            ModuleOperation("runs", "query", "agent.read", EmptyInput, self.runs),
            ModuleOperation(
                "configuration.plan", "action", "agent.config.plan", AgentPlan, self.plan,
                self.plan_payload,
            ),
            ModuleOperation("configuration.apply", "action", "agent.config.apply", Apply, self.apply),
            ModuleOperation("approval.decide", "action", "agent.approval", Approval, self.approval),
            ModuleOperation("run.cancel", "action", "agent.cancel", Cancel, self.cancel),
            ModuleOperation("tool.resolve", "action", "agent.resolve", Resolve, self.resolve),
        ))

    async def configuration(self, request, body):
        return await request.module.config.read(request.ticket, request.operation_id)

    async def runs(self, request, body):
        return await request.module.runner.state.inspect(
            request.ticket, request.operation_id, {}
        )

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

    async def plan(self, request, body):
        if body.values.enabled:
            for tool in body.values.allowed_tools:
                request.module._freeze_tool(tool.model_dump())
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
            request.bot.runtime.module("conversation.scheduler").notify()
        return result

    async def approval(self, request, body):
        return await self._advance(
            request, request.module.runner.state.decide_approval, body.model_dump()
        )

    async def cancel(self, request, body):
        return await self._advance(
            request, request.module.runner.state.cancel, body.model_dump()
        )

    async def resolve(self, request, body):
        return await self._advance(
            request, request.module.runner.state.resolve, body.model_dump()
        )

    async def _advance(self, request, method, payload):
        result = await method(request.ticket, request.operation_id, payload)
        if result is not None and not request.bot.control.is_unavailable(result):
            request.bot.runtime.module("conversation.scheduler").notify()
        return result
