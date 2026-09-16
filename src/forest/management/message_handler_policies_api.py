"""Authenticated Web management for per-Binding ordinary-message handler policies."""

from __future__ import annotations

import asyncio
from typing import Literal
from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token, verify_password
from forest.storage.message_handler_policies import normalize_message_handler_policy


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PolicyValues(Body):
    mode: Literal["DISABLED", "EVERYONE", "ADMINS", "USER_SET"]
    user_ids: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("user_ids")
    @classmethod
    def users(cls, value):
        normalized = normalize_message_handler_policy({"mode": "USER_SET" if value else "DISABLED", "user_ids": value})
        return normalized["user_ids"]

    @model_validator(mode="after")
    def selection(self):
        normalized = normalize_message_handler_policy(self.model_dump())
        self.user_ids = normalized["user_ids"]
        return self


class PolicyPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: PolicyValues
    current_password: str = Field(min_length=12, max_length=1024)


class PolicyApply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)
    current_password: str = Field(min_length=12, max_length=1024)


def install_message_handler_policy_routes(app, runtime):
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
            account = await (await conn.execute(
                "SELECT password_hash,disabled,role FROM web_accounts WHERE id=%s",
                (session["account_id"],),
            )).fetchone()
        encoded = account["password_hash"] if account else "malformed"
        async with runtime.password_slots:
            valid = await asyncio.to_thread(verify_password, password, encoded)
        if not valid or account is None or account["disabled"] or account["role"] != "admin":
            raise ForestError("REAUTHENTICATION_FAILED", "Current password is incorrect", 403)

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        operation = "web:" + digest_json({
            "subject_id": who["subject_id"], "bot_id": bot.store.bot_id,
            "action": action, "key": key,
        })
        ticket = await runtime.authority.issue(
            **who, bot_id=bot.store.bot_id, action=action, operation_id=operation,
            payload=payload, plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, operation

    @app.get("/management/v1/bots/{bot_id}/bindings/{binding_id}/message-handler-policies")
    async def policies(bot_id: str, binding_id: str, request: Request):
        _, who, bot = await target(request, bot_id)
        payload = {"binding_id": binding_id}
        ticket, operation = await signed(
            request, bot, who, "message.handler.policy.read", payload, write=False
        )
        policies = bot.platform.module("message.router").policies
        return await policies.read(ticket, operation, binding_id)

    @app.post(
        "/management/v1/bots/{bot_id}/bindings/{binding_id}/components/{component_id}/"
        "message-handlers/{handler_id}/policy-change-plans"
    )
    async def plan(
        bot_id: str, binding_id: str, component_id: str, handler_id: str,
        body: PolicyPlan, request: Request,
    ):
        session, who, bot = await target(request, bot_id, write=True)
        await reauthenticate(session, body.current_password)
        payload = {
            "binding_id": binding_id, "component_id": component_id, "handler_id": handler_id,
            "expected_revision": body.expected_revision,
            "values": body.values.model_dump(),
        }
        ticket, operation = await signed(request, bot, who, "message.handler.policy.plan", payload)
        policies = bot.platform.module("message.router").policies
        return await policies.plan(ticket, operation, payload)

    @app.post(
        "/management/v1/bots/{bot_id}/bindings/{binding_id}/components/{component_id}/"
        "message-handlers/{handler_id}/policy-management-requests"
    )
    async def apply(
        bot_id: str, binding_id: str, component_id: str, handler_id: str,
        body: PolicyApply, request: Request,
    ):
        session, who, bot = await target(request, bot_id, write=True)
        await reauthenticate(session, body.current_password)
        payload = {
            "binding_id": binding_id, "component_id": component_id, "handler_id": handler_id,
            "expected_revision": body.expected_revision,
            "plan_id": body.plan_id, "plan_digest": body.plan_digest,
        }
        ticket, operation = await signed(request, bot, who, "message.handler.policy.apply", payload)
        policies = bot.platform.module("message.router").policies
        result = await policies.apply(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            bot.platform.notify()
        return result
