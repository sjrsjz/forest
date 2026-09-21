"""Cross-module InputBatch and Memory eligibility transaction."""

from __future__ import annotations

import hashlib
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
                    input_batch_verifier=self.scheduler.verify_input_batch_occurrence_in,
                )
                pending = sum(
                    item.get("state") == "PENDING_CAPTURE" for item in qualifications
                )
                result = {
                    **result,
                    "memory_qualified_count": len(qualifications) - pending,
                    "memory_pending_count": pending,
                }
            else:
                result = {
                    **result,
                    "memory_qualified_count": 0,
                    "memory_pending_count": 0,
                }
            archive = result.get("memory_archive")
            if archive and archive.get("event_refs"):
                deletion_epoch = await _call(self.deletion_epoch_guard, conn)
                archived = await self.memory.qualify_archive_in(
                    conn,
                    operation_id="archive:"
                    + hashlib.sha256(operation_id.encode("utf-8")).hexdigest(),
                    compaction_ref=archive["compaction_ref"],
                    conversation_ref=actor.conversation_id,
                    event_refs=archive["event_refs"],
                    expected_deletion_epoch=deletion_epoch,
                    archive_verifier=self.scheduler.verify_context_archive_occurrence_in,
                )
                result = {
                    **result,
                    "memory_archived_count": sum(
                        item.get("state") != "PENDING_CAPTURE" for item in archived
                    ),
                    "memory_archive_pending_count": sum(
                        item.get("state") == "PENDING_CAPTURE" for item in archived
                    ),
                }
            else:
                result = {
                    **result,
                    "memory_archived_count": 0,
                    "memory_archive_pending_count": 0,
                }
            return result
