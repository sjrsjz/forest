"""Authenticated management for plugin-owned durable component jobs."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from fastapi import Query, Request
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token

JobStatus = Literal["PENDING", "INFLIGHT", "RETRY_WAIT", "ACKED", "DEAD", "RETIRED"]


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Replay(Body):
    job_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class RetirementPlan(Body):
    component_id: str = Field(min_length=1, max_length=128)
    handler_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class RetirementApply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


def install_component_job_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        from forest.storage.component_jobs import ComponentJobStore

        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        return who, bot, ComponentJobStore(bot.store, control=bot.control)

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128 or any(ord(char) < 33 or ord(char) == 127 for char in key):
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422
            )
        operation = "web:" + digest_json({
            "subject_id": who["subject_id"],
            "bot_id": bot.store.bot_id,
            "action": action,
            "key": key,
        })
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

    @app.get("/management/v1/bots/{bot_id}/component-jobs")
    async def read(
        bot_id: str,
        request: Request,
        component_id: str | None = Query(default=None, min_length=1, max_length=128),
        handler_id: str | None = Query(default=None, min_length=1, max_length=128),
        status: JobStatus | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=200),
    ):
        who, bot, store = await target(request, bot_id)
        payload = {"limit": limit}
        for key, value in (
            ("component_id", component_id), ("handler_id", handler_id), ("status", status)
        ):
            if value is not None:
                payload[key] = value
        ticket, operation = await signed(
            request, bot, who, "component_job.read", payload, write=False
        )
        return await store.read(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/component-jobs/replays")
    async def replay(bot_id: str, body: Replay, request: Request):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "component_job.replay", payload)
        result = await store.replay(ticket, operation, payload)
        bot.kernel.notify_component_jobs()
        return result

    @app.post("/management/v1/bots/{bot_id}/component-jobs/retirement-plans")
    async def retirement_plan(bot_id: str, body: RetirementPlan, request: Request):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(
            request, bot, who, "component_job.retire.plan", payload
        )
        return await store.plan_retirement(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/component-jobs/retirement-requests")
    async def retirement_apply(bot_id: str, body: RetirementApply, request: Request):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(
            request, bot, who, "component_job.retire.apply", payload
        )
        result = await store.apply_retirement(ticket, operation, payload)
        bot.kernel.notify_component_jobs()
        return result
