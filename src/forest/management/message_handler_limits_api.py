"""Authenticated Web management for Bot-wide ordinary-message Handler limits."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token, verify_password


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HandlerLimitValues(Body):
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
        if not (
            self.max_pending_principal
            <= self.max_pending_binding
            <= self.max_pending_bot
        ):
            raise ValueError("Principal, Binding and Bot pending limits must be ordered")
        return self


class HandlerLimitPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: HandlerLimitValues
    current_password: str = Field(min_length=12, max_length=1024)


class HandlerLimitApply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


def install_message_handler_limit_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        return session, who, await runtime.fleet.attach(bot_id)

    async def reauthenticate(session, password):
        store = runtime.require_store()
        async with store.pool.connection() as conn:
            account = await (
                await conn.execute(
                    "SELECT password_hash,disabled,role FROM web_accounts WHERE id=%s",
                    (session["account_id"],),
                )
            ).fetchone()
        encoded = account["password_hash"] if account else "malformed"
        async with runtime.password_slots:
            valid = await asyncio.to_thread(verify_password, password, encoded)
        if not valid or account is None or account["disabled"] or account["role"] != "admin":
            raise ForestError("REAUTHENTICATION_FAILED", "Current password is incorrect", 403)

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        operation = "web:" + digest_json(
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
            operation_id=operation,
            payload=payload,
            plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, operation

    @app.get("/management/v1/bots/{bot_id}/message-handler-limits")
    async def read(bot_id: str, request: Request):
        _, who, bot = await target(request, bot_id)
        ticket, operation = await signed(
            request, bot, who, "message.handler.limit.read", {}, write=False
        )
        limits = bot.platform.module("message.router").limits
        return await limits.read(ticket, operation)

    @app.post("/management/v1/bots/{bot_id}/message-handler-limit-change-plans")
    async def plan(bot_id: str, body: HandlerLimitPlan, request: Request):
        session, who, bot = await target(request, bot_id, write=True)
        await reauthenticate(session, body.current_password)
        payload = {
            "expected_revision": body.expected_revision,
            "values": body.values.model_dump(),
        }
        ticket, operation = await signed(request, bot, who, "message.handler.limit.plan", payload)
        limits = bot.platform.module("message.router").limits
        return await limits.plan(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/message-handler-limit-management-requests")
    async def apply(bot_id: str, body: HandlerLimitApply, request: Request):
        session, who, bot = await target(request, bot_id, write=True)
        await reauthenticate(session, body.current_password)
        payload = {
            "expected_revision": body.expected_revision,
            "plan_id": body.plan_id,
            "plan_digest": body.plan_digest,
        }
        ticket, operation = await signed(request, bot, who, "message.handler.limit.apply", payload)
        limits = bot.platform.module("message.router").limits
        result = await limits.apply(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            bot.platform.notify()
        return result
