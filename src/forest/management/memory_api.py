"""Authenticated multimodal memory configuration and observed runtime status."""

from __future__ import annotations

from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.errors import ForestError
from forest.memory_settings import MemorySettings
from forest.packages import digest_json
from forest.security import digest_token


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


def install_memory_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        return who, bot

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422
            )
        op = "web:" + digest_json(
            {
                "subject_id": who["subject_id"],
                "bot_id": bot.store.bot_id,
                "action": action,
                "key": key,
            }
        )
        ticket = await runtime.authority.issue(
            **who,
            bot_id=bot.store.bot_id,
            action=action,
            operation_id=op,
            payload=payload,
            plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, op

    @app.get("/management/v1/bots/{bot_id}/memory/configuration")
    async def configuration(bot_id: str, request: Request):
        who, bot = await target(request, bot_id)
        ticket, op = await signed(request, bot, who, "memory.config.read", {}, write=False)
        return await bot.platform.memory.config.read(ticket, op)

    @app.post("/management/v1/bots/{bot_id}/memory/change-plans")
    async def plan(bot_id: str, body: MemoryPlan, request: Request):
        who, bot = await target(request, bot_id, write=True)
        secret = "" if body.clear_api_key else body.api_key
        changed = body.clear_api_key or secret is not None
        payload = {
            "expected_revision": body.expected_revision,
            "values": body.values.model_dump(),
            "credentials_changed": changed,
            "secret_digest": digest_json({"api_key": secret}),
        }
        ticket, op = await signed(request, bot, who, "memory.config.plan", payload)
        return await bot.platform.memory.config.plan(
            ticket,
            op,
            payload,
            secret_ciphertext=runtime.catalog.vault.encrypt(secret) if changed else None,
        )

    @app.post("/management/v1/bots/{bot_id}/memory/management-requests")
    async def apply(bot_id: str, body: Apply, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, op = await signed(request, bot, who, "memory.config.apply", payload)
        result = await bot.platform.memory.config.apply(ticket, op, payload)
        bot.platform.memory.notify()
        return result

    @app.get("/management/v1/bots/{bot_id}/memory/status")
    async def status(bot_id: str, request: Request):
        who, bot = await target(request, bot_id)
        ticket, operation = await signed(request, bot, who, "memory.read", {}, write=False)
        return await bot.platform.memory.inspect(ticket, operation, {})
