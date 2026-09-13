"""Authenticated Bot audit metadata queries."""

from __future__ import annotations

from uuid import uuid4

from fastapi import Query, Request

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token

_IDENTITY = r"^[A-Za-z0-9_.:-]{1,128}$"


def install_bot_audit_routes(app, runtime):
    @app.get("/management/v1/bots/{bot_id}/audit")
    async def read(
        bot_id: str,
        request: Request,
        action: str | None = Query(default=None, pattern=_IDENTITY),
        before: str | None = Query(default=None, pattern=_IDENTITY),
        limit: int = Query(default=50, ge=1, le=100),
    ):
        from forest.storage.bot_audit import BotAuditStore

        if not set(request.query_params) <= {"action", "before", "limit"}:
            raise ForestError("AUDIT_QUERY_INVALID", "Unknown audit query field", 422)
        session = await runtime.principal(request)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError(
                "BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503
            )
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        payload = {"limit": limit}
        if action is not None:
            payload["action"] = action
        if before is not None:
            payload["before"] = before
        operation = "web:" + digest_json(
            {
                "subject_id": who["subject_id"],
                "bot_id": bot.store.bot_id,
                "action": "audit.read",
                "key": str(uuid4()),
            }
        )
        ticket = await runtime.authority.issue(
            **who,
            bot_id=bot.store.bot_id,
            action="audit.read",
            operation_id=operation,
            payload=payload,
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return await BotAuditStore(bot.store, control=bot.control).read(
            ticket, operation, payload
        )
