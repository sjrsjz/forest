"""Authenticated Web diagnostics and bounded wakeup for message handlers."""

from __future__ import annotations

from uuid import uuid4

from fastapi import Query, Request
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token
from forest.storage.message_handler_diagnostics import MessageHandlerDiagnosticsStore

_IDENTITY = r"^[A-Za-z0-9_.:-]{1,128}$"


class HandlerRecoveryBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: int = Field(default=128, ge=1, le=256, strict=True)


def install_message_handler_diagnostic_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        return who, await runtime.fleet.attach(bot_id)

    async def signed(request, bot, who, action, payload, *, write=False):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128:
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
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, operation

    def store(bot):
        return MessageHandlerDiagnosticsStore(bot.store, control=bot.control)

    @app.get("/management/v1/bots/{bot_id}/message-handlers/diagnostics")
    async def diagnostics(
        bot_id: str,
        request: Request,
        binding_id: str | None = Query(default=None, pattern=_IDENTITY),
        limit: int = Query(default=50, ge=1, le=100),
    ):
        if not set(request.query_params) <= {"binding_id", "limit"}:
            raise ForestError(
                "HANDLER_DIAGNOSTICS_QUERY_INVALID", "Unknown diagnostic query field", 422
            )
        who, bot = await target(request, bot_id)
        payload = {"limit": limit}
        if binding_id is not None:
            payload["binding_id"] = binding_id
        ticket, operation = await signed(
            request, bot, who, "message.handler.execution.read", payload
        )
        return await store(bot).read(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/message-handlers/recovery")
    async def recover(bot_id: str, body: HandlerRecoveryBody, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(
            request,
            bot,
            who,
            "message.handler.execution.recover",
            payload,
            write=True,
        )
        result = await store(bot).recover(ticket, operation, payload)
        if result is not None and not bot.control.is_unavailable(result):
            bot.platform.notify()
        return result
