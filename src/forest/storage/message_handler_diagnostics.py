"""Redacted message-handler execution diagnostics and bounded wake requests."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.control import BotControl

_READ_KEYS = frozenset({"binding_id", "limit"})
_RECOVERY_KEYS = frozenset({"limit"})


def _identity(value, field):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or any(not (char.isascii() and (char.isalnum() or char in "_.:-")) for char in value)
    ):
        raise ForestError("HANDLER_DIAGNOSTICS_QUERY_INVALID", f"Invalid {field}", 422)
    return value


def _read_payload(value):
    if not isinstance(value, dict) or not set(value) <= _READ_KEYS:
        raise ForestError(
            "HANDLER_DIAGNOSTICS_QUERY_INVALID", "Handler diagnostic query fields differ", 422
        )
    result = {}
    if "binding_id" in value:
        result["binding_id"] = _identity(value["binding_id"], "binding")
    limit = value.get("limit", 50)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ForestError(
            "HANDLER_DIAGNOSTICS_QUERY_INVALID", "Diagnostic limit is outside 1..100", 422
        )
    result["limit"] = limit
    return result


def _recovery_payload(value):
    if not isinstance(value, dict) or set(value) != _RECOVERY_KEYS:
        raise ForestError(
            "HANDLER_RECOVERY_INPUT_INVALID", "Handler recovery fields differ", 422
        )
    limit = value["limit"]
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ForestError(
            "HANDLER_RECOVERY_INPUT_INVALID", "Recovery limit is outside 1..256", 422
        )
    return {"limit": limit}


def _iso(value):
    return value.isoformat() if value is not None else None


class MessageHandlerDiagnosticsStore:
    """Read metadata only; recovery requests wake the normal Actor-owned path."""

    def __init__(self, store, *, control=None):
        self.store = store
        self.control = control or BotControl(store)

    async def read(self, ticket, operation_id, payload=None) -> dict:
        payload = _read_payload({} if payload is None else payload)
        binding_id = payload.get("binding_id")
        limit = payload["limit"]
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "message.handler.execution.read", operation_id, payload
            )
            if binding_id is not None:
                binding = await (
                    await conn.execute(
                        "SELECT 1 FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s",
                        (self.store.bot_id, binding_id),
                    )
                ).fetchone()
                if binding is None:
                    raise ForestError("BINDING_NOT_FOUND", "Binding does not exist", 404)

            summary = await (
                await conn.execute(
                    "SELECT count(*) FILTER(WHERE h.status='PENDING') AS pending,"
                    "count(*) FILTER(WHERE h.status='PENDING' AND h.claim_actor_id IS NULL) "
                    "AS unclaimed,count(*) FILTER(WHERE h.status='PENDING' "
                    "AND h.claim_actor_id IS NOT NULL) AS claimed,"
                    "count(*) FILTER(WHERE h.status='PASSED') AS passed,"
                    "count(*) FILTER(WHERE h.status='CONSUMED') AS consumed,"
                    "count(*) FILTER(WHERE h.status='REPLIED') AS replied,"
                    "count(*) FILTER(WHERE h.status='FAILED') AS failed,"
                    "count(*) FILTER(WHERE h.status='CANCELLED') AS cancelled,"
                    "min(h.created_at) FILTER(WHERE h.status='PENDING') AS oldest_pending_at "
                    "FROM public.bot_message_handler_chains h WHERE h.bot_id=%s "
                    "AND (%s::text IS NULL OR h.binding_id=%s)",
                    (self.store.bot_id, binding_id, binding_id),
                )
            ).fetchone()
            call_summary = await (
                await conn.execute(
                    "SELECT count(*) FILTER(WHERE c.status='ADMITTED') AS admitted,"
                    "count(*) FILTER(WHERE c.status='COMPLETED') AS completed,"
                    "count(*) FILTER(WHERE c.status IN ('FAILED','CANCELLED')) AS terminated,"
                    "count(*) FILTER(WHERE c.status IS NULL) AS absent "
                    "FROM public.bot_message_handler_chains h "
                    "JOIN public.bot_message_handler_attempts a ON "
                    "(a.bot_id,a.chain_id,a.attempt_no)=(h.bot_id,h.chain_id,h.attempt_count) "
                    "LEFT JOIN public.bot_calls c ON c.bot_id=a.bot_id "
                    "AND c.operation_id=a.call_operation_id "
                    "WHERE h.bot_id=%s AND h.status='PENDING' "
                    "AND h.claim_actor_id IS NOT NULL "
                    "AND (%s::text IS NULL OR h.binding_id=%s)",
                    (self.store.bot_id, binding_id, binding_id),
                )
            ).fetchone()
            chains = await (
                await conn.execute(
                    "SELECT h.chain_id,h.event_id,h.conversation_id,h.binding_id,h.ingress_seq,"
                    "h.status,h.current_index,h.route_count,h.attempt_count,h.next_attempt_at,"
                    "h.deadline_at,h.last_error_code,h.terminal_reason,h.fallback_decision,"
                    "h.admission_limit_revision,h.event_bytes,"
                    "h.created_at,h.completed_at,(h.claim_actor_id IS NOT NULL) AS claimed,"
                    "ci.component_id,ci.handler_id,ci.generation_id,ci.priority,c.status AS call_status "
                    "FROM public.bot_message_handler_chains h "
                    "LEFT JOIN public.bot_message_handler_chain_items ci ON "
                    "(ci.bot_id,ci.chain_id,ci.route_index)=(h.bot_id,h.chain_id,h.current_index) "
                    "LEFT JOIN public.bot_message_handler_attempts a ON "
                    "(a.bot_id,a.chain_id,a.attempt_no)=(h.bot_id,h.chain_id,h.attempt_count) "
                    "LEFT JOIN public.bot_calls c ON c.bot_id=a.bot_id "
                    "AND c.operation_id=a.call_operation_id "
                    "WHERE h.bot_id=%s AND (%s::text IS NULL OR h.binding_id=%s) "
                    "ORDER BY h.created_at DESC,h.chain_id DESC LIMIT %s",
                    (self.store.bot_id, binding_id, binding_id, limit),
                )
            ).fetchall()
            chain_ids = [row["chain_id"] for row in chains]
            attempts = []
            if chain_ids:
                attempts = await (
                    await conn.execute(
                        "SELECT a.chain_id,a.attempt_no,a.route_index,ci.component_id,ci.handler_id,"
                        "ci.generation_id,c.status AS call_status,c.error_code AS call_error_code,"
                        "r.disposition,f.error_code AS failure_error_code,f.retryable,f.retry_at,"
                        "a.created_at FROM public.bot_message_handler_attempts a "
                        "JOIN public.bot_message_handler_chain_items ci ON "
                        "(ci.bot_id,ci.chain_id,ci.route_index)=(a.bot_id,a.chain_id,a.route_index) "
                        "LEFT JOIN public.bot_calls c ON c.bot_id=a.bot_id "
                        "AND c.operation_id=a.call_operation_id "
                        "LEFT JOIN public.bot_message_handler_results r ON "
                        "(r.bot_id,r.chain_id,r.attempt_no)=(a.bot_id,a.chain_id,a.attempt_no) "
                        "LEFT JOIN public.bot_message_handler_failures f ON "
                        "(f.bot_id,f.chain_id,f.attempt_no)=(a.bot_id,a.chain_id,a.attempt_no) "
                        "WHERE a.bot_id=%s AND a.chain_id=ANY(%s) "
                        "ORDER BY a.created_at DESC,a.chain_id,a.attempt_no DESC",
                        (self.store.bot_id, chain_ids),
                    )
                ).fetchall()
            now = await self.store._now(conn)
            await self.control._time(conn, claims)

        oldest = summary["oldest_pending_at"]
        return {
            "filter": {"binding_id": binding_id, "limit": limit},
            "summary": {
                **{key: summary[key] for key in (
                    "pending", "unclaimed", "claimed", "passed", "consumed", "replied",
                    "failed", "cancelled",
                )},
                "claimed_call_admitted": call_summary["admitted"],
                "claimed_call_completed": call_summary["completed"],
                "claimed_call_terminated": call_summary["terminated"],
                "claimed_call_absent": call_summary["absent"],
                "oldest_pending_age_seconds": (
                    max(0.0, (now - oldest).total_seconds()) if oldest is not None else None
                ),
            },
            "chains": [
                {
                    **row,
                    "next_attempt_at": _iso(row["next_attempt_at"]),
                    "deadline_at": _iso(row["deadline_at"]),
                    "created_at": _iso(row["created_at"]),
                    "completed_at": _iso(row["completed_at"]),
                }
                for row in chains
            ],
            "attempts": [
                {
                    **row,
                    "retry_at": _iso(row["retry_at"]),
                    "created_at": _iso(row["created_at"]),
                }
                for row in attempts
            ],
        }

    async def recover(self, ticket, operation_id, payload) -> dict:
        """Record one idempotent bounded wake request without bypassing the Actor."""
        payload = _recovery_payload(payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "message.handler.execution.recover", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            rows = await (
                await conn.execute(
                    "SELECT h.claim_actor_id,c.status AS call_status,h.next_attempt_at<=clock_timestamp() "
                    "AS due FROM public.bot_message_handler_chains h "
                    "LEFT JOIN public.bot_message_handler_attempts a ON "
                    "(a.bot_id,a.chain_id,a.attempt_no)=(h.bot_id,h.chain_id,h.attempt_count) "
                    "LEFT JOIN public.bot_calls c ON c.bot_id=a.bot_id "
                    "AND c.operation_id=a.call_operation_id "
                    "WHERE h.bot_id=%s AND h.status='PENDING' "
                    "ORDER BY h.ingress_seq,h.chain_id LIMIT %s",
                    (self.store.bot_id, payload["limit"]),
                )
            ).fetchall()
            result = {
                "operation_id": operation_id,
                "inspected": len(rows),
                "due_unclaimed": sum(
                    row["claim_actor_id"] is None and row["due"] for row in rows
                ),
                "completed_to_finalize": sum(
                    row["claim_actor_id"] is not None and row["call_status"] == "COMPLETED"
                    for row in rows
                ),
                "admitted_waiting": sum(row["call_status"] == "ADMITTED" for row in rows),
                "abandoned_to_reconcile": sum(
                    row["claim_actor_id"] is not None
                    and row["call_status"] in {None, "FAILED", "CANCELLED"}
                    for row in rows
                ),
                "scheduler_wakeup": True,
                "limit": payload["limit"],
            }
            await self.control._time(conn, claims)
            await self.control._record(conn, claims, result)
            return result
