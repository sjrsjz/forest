"""Authenticated observation, replay and retirement for component events."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from fastapi import Query, Request
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token

DeliveryStatus = Literal["PENDING", "INFLIGHT", "RETRY_WAIT", "ACKED", "DEAD", "RETIRED"]


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Replay(Body):
    delivery_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class RetirementPlan(Body):
    subscriber_id: str = Field(min_length=1, max_length=128)
    contract_major: int = Field(ge=1, le=2**31 - 1, strict=True)
    expected_revision: int = Field(ge=0, strict=True)


class RetirementApply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


def install_event_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        from forest.storage.events import EventStore

        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        return who, bot, EventStore(bot.store, control=bot.control)

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128 or any(ord(c) < 33 or ord(c) == 127 for c in key):
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422
            )
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

    @app.get("/management/v1/bots/{bot_id}/events")
    async def read(
        bot_id: str,
        request: Request,
        subscriber_id: str | None = Query(default=None, min_length=1, max_length=128),
        topic: str | None = Query(default=None, min_length=1, max_length=128),
        status: DeliveryStatus | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=200),
    ):
        who, bot, store = await target(request, bot_id)
        payload = {"limit": limit}
        for key, value in (("subscriber_id", subscriber_id), ("topic", topic), ("status", status)):
            if value is not None:
                payload[key] = value
        ticket, operation = await signed(request, bot, who, "event.read", payload, write=False)
        return await store.read(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/events/replays")
    async def replay(bot_id: str, body: Replay, request: Request):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "event.replay", payload)
        result = await store.replay(ticket, operation, payload)
        bot.kernel.notify_events()
        return result

    @app.post("/management/v1/bots/{bot_id}/events/retirement-plans")
    async def retirement_plan(bot_id: str, body: RetirementPlan, request: Request):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "event.retire.plan", payload)
        return await store.plan_retirement(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/events/retirement-requests")
    async def retirement_apply(bot_id: str, body: RetirementApply, request: Request):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "event.retire.apply", payload)
        result = await store.apply_retirement(ticket, operation, payload)
        bot.kernel.notify_events()
        return result
