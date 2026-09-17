"""Cross-module InputBatch and Memory eligibility transaction."""

from __future__ import annotations

import inspect

from forest.errors import ForestError


async def _call(port, *args, **values):
    result = port(*args, **values)
    return await result if inspect.isawaitable(result) else result


class ConversationMemoryInputUnitOfWork:
    """Commit a Conversation cut and its Memory eligibility without double writes."""

    def __init__(self, store, *, scheduler, memory, deletion_epoch_guard) -> None:
        self.store = store
        self.scheduler = scheduler
        self.memory = memory
        self.deletion_epoch_guard = deletion_epoch_guard

    async def select_input(
        self,
        actor,
        operation_id: str,
        *,
        boundary: str,
        session_ref: str | None = None,
        run_ref: str | None = None,
    ) -> dict:
        if self.deletion_epoch_guard is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING", "Payload deletion epoch port is unavailable", 503
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            result = await self.scheduler.select_input_in(
                conn,
                actor,
                operation_id,
                boundary=boundary,
                session_ref=session_ref,
                run_ref=run_ref,
            )
            event_refs = tuple(result.get("event_ids", ()))
            if result.get("input_batch_id") is not None and event_refs:
                deletion_epoch = await _call(self.deletion_epoch_guard, conn)
                qualifications = await self.memory.qualify_input_batch_in(
                    conn,
                    operation_id=operation_id,
                    input_batch_ref=result["input_batch_id"],
                    conversation_ref=actor.conversation_id,
                    event_refs=event_refs,
                    context_revision=result["context_revision"],
                    expected_deletion_epoch=deletion_epoch,
                )
                result = {**result, "memory_qualified_count": len(qualifications)}
            else:
                result = {**result, "memory_qualified_count": 0}
            return result
