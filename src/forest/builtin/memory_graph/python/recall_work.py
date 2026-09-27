"""Advance one Memory-owned semantic stage per durable claim."""

import asyncio
from datetime import datetime, timezone

from forest.errors import ForestError


async def advance(plugin, task, ctx):
    payload = task["payload"]
    deadline = datetime.fromisoformat(payload["deadline_at"])
    remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        raise ForestError("MEMORY_RECALL_DEADLINE", "Recall deadline has expired", 408)
    # Stage commands are idempotent. Lost orchestration receipts recover from
    # their committed facts; this worker never dispatches an Embedding Effect.
    async with asyncio.timeout(min(remaining, 150)):
        request = {"query_id": payload["query_id"]}
        progress = await plugin.query_progress(request, ctx)
        phase = progress["phase"]
        action = {
            "NEED_PREPARATION": plugin.prepare_query,
            "NEED_MATCHING": plugin.match_query,
            "NEED_FREEZING": plugin.freeze_query,
            "NEED_MATERIALIZATION": plugin.materialize_query,
        }.get(phase)
        if action is not None:
            await action(request, ctx)
        return {"query_id": payload["query_id"]}
