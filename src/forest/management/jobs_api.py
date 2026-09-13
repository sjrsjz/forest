"""Authenticated job plans, bounded occurrence reads and explicit cancellation."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from fastapi import Query, Request
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schedules import JobSettings
from forest.security import digest_token


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Revision(Body):
    job_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)


class Plan(Revision):
    values: JobSettings


class Apply(Revision):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class Cancel(Revision):
    scope: Literal["future", "pending", "all"]
    reason: str = Field(min_length=1, max_length=256)


class Replay(Body):
    occurrence_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=1, strict=True)


def install_job_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        from forest.storage.jobs import JobStore

        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {"subject_id": session["account_id"],
               "session_token_hash": digest_token(request.cookies["forest_session"])}
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        return who, bot, JobStore(bot.platform.messages)

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128 or any(ord(c) < 33 or ord(c) == 127 for c in key):
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        operation = "web:" + digest_json({"subject_id": who["subject_id"], "bot_id": bot.store.bot_id,
                                          "action": action, "key": key})
        ticket = await runtime.authority.issue(
            **who, bot_id=bot.store.bot_id, action=action, operation_id=operation, payload=payload,
            plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, operation

    def notify(bot):
        bot.platform.scheduler.notify()
        bot.platform.jobs.notify()

    @app.get("/management/v1/bots/{bot_id}/jobs")
    async def read(bot_id: str, request: Request, job_id: str | None = Query(default=None, min_length=1, max_length=128)):
        who, bot, store = await target(request, bot_id)
        payload = {"job_id": job_id} if job_id is not None else {}
        ticket, operation = await signed(request, bot, who, "job.read", payload, write=False)
        return await store.read(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/jobs/change-plans")
    async def plan(bot_id: str, request: Request, body: Plan):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "job.plan", payload)
        return await store.plan(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/jobs/management-requests")
    async def apply(bot_id: str, request: Request, body: Apply):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "job.apply", payload)
        result = await store.apply(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            notify(bot)
        return result

    @app.post("/management/v1/bots/{bot_id}/jobs/run-requests")
    async def run(bot_id: str, request: Request, body: Revision):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "job.run", payload)
        result = await store.run_now(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            notify(bot)
        return result

    @app.post("/management/v1/bots/{bot_id}/jobs/cancellations")
    async def cancel(bot_id: str, request: Request, body: Cancel):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "job.cancel", payload)
        result = await store.cancel(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            notify(bot)
        return result

    @app.post("/management/v1/bots/{bot_id}/jobs/replays")
    async def replay(bot_id: str, request: Request, body: Replay):
        who, bot, store = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "job.replay", payload)
        result = await store.replay(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            notify(bot)
        return result
