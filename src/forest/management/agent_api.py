"""Authenticated model configuration, approval and Run management."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.agent_settings import AgentSettings
from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token


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


def install_agent_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {"subject_id": session["account_id"],
               "session_token_hash": digest_token(request.cookies["forest_session"])}
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        return who, bot

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        op = "web:" + digest_json({"subject_id": who["subject_id"], "bot_id": bot.store.bot_id,
                                   "action": action, "key": key})
        ticket = await runtime.authority.issue(
            **who, bot_id=bot.store.bot_id, action=action, operation_id=op, payload=payload,
            plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, op

    @app.get("/management/v1/bots/{bot_id}/agent/configuration")
    async def configuration(bot_id: str, request: Request):
        who, bot = await target(request, bot_id)
        ticket, op = await signed(request, bot, who, "agent.config.read", {}, write=False)
        return await bot.platform.agent.config.read(ticket, op)

    @app.post("/management/v1/bots/{bot_id}/agent/change-plans")
    async def plan(bot_id: str, body: AgentPlan, request: Request):
        who, bot = await target(request, bot_id, write=True)
        if body.values.enabled:
            for tool in body.values.allowed_tools:
                bot.platform.agent._freeze_tool(tool.model_dump())
        secret = "" if body.clear_api_key else body.api_key
        changed = body.clear_api_key or secret is not None
        payload = {"expected_revision": body.expected_revision, "values": body.values.model_dump(),
                   "credentials_changed": changed, "secret_digest": digest_json({"api_key": secret})}
        ticket, op = await signed(request, bot, who, "agent.config.plan", payload)
        return await bot.platform.agent.config.plan(
            ticket, op, payload,
            secret_ciphertext=runtime.catalog.vault.encrypt(secret) if changed else None,
        )

    @app.post("/management/v1/bots/{bot_id}/agent/management-requests")
    async def apply(bot_id: str, body: Apply, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, op = await signed(request, bot, who, "agent.config.apply", payload)
        result = await bot.platform.agent.config.apply(ticket, op, payload)
        bot.platform.scheduler.notify()
        return result

    @app.get("/management/v1/bots/{bot_id}/agent/runs")
    async def runs(bot_id: str, request: Request):
        who, bot = await target(request, bot_id)
        payload = {}
        ticket, op = await signed(request, bot, who, "agent.read", payload, write=False)
        return await bot.platform.agent.runner.state.inspect(ticket, op, payload)

    @app.post("/management/v1/bots/{bot_id}/agent/approvals")
    async def approval(bot_id: str, body: Approval, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, op = await signed(request, bot, who, "agent.approval", payload)
        result = await bot.platform.agent.runner.state.decide_approval(ticket, op, payload)
        bot.platform.scheduler.notify()
        return result

    @app.post("/management/v1/bots/{bot_id}/agent/cancellations")
    async def cancel(bot_id: str, body: Cancel, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, op = await signed(request, bot, who, "agent.cancel", payload)
        result = await bot.platform.agent.runner.state.cancel(ticket, op, payload)
        bot.platform.scheduler.notify()
        return result

    @app.post("/management/v1/bots/{bot_id}/agent/resolutions")
    async def resolve(bot_id: str, body: Resolve, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, op = await signed(request, bot, who, "agent.resolve", payload)
        result = await bot.platform.agent.runner.state.resolve(ticket, op, payload)
        bot.platform.scheduler.notify()
        return result
