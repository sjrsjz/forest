"""Purpose-bound business-command diagnostics and convergent recovery."""

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
        raise ForestError("COMMAND_DIAGNOSTICS_QUERY_INVALID", f"Invalid {field}", 422)
    return value


def _read_payload(value):
    if not isinstance(value, dict) or not set(value) <= _READ_KEYS:
        raise ForestError(
            "COMMAND_DIAGNOSTICS_QUERY_INVALID", "Command diagnostic query fields differ", 422
        )
    result = {}
    if "binding_id" in value:
        result["binding_id"] = _identity(value["binding_id"], "binding")
    limit = value.get("limit", 50)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ForestError(
            "COMMAND_DIAGNOSTICS_QUERY_INVALID", "Diagnostic limit is outside 1..100", 422
        )
    result["limit"] = limit
    return result


def _recovery_payload(value):
    if not isinstance(value, dict) or set(value) != _RECOVERY_KEYS:
        raise ForestError(
            "COMMAND_RECOVERY_INPUT_INVALID", "Command recovery fields differ", 422
        )
    limit = value.get("limit")
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ForestError(
            "COMMAND_RECOVERY_INPUT_INVALID", "Recovery limit is outside 1..256", 422
        )
    return {"limit": limit}


def _iso(value):
    return value.isoformat() if value is not None else None


class BusinessCommandDiagnosticsStore:
    """Read safe queue facts and request a bounded pass over retryable work."""

    def __init__(self, store, *, messages, lease, control=None):
        self.store = store
        self.messages = messages
        self.lease = lease
        self.control = control or BotControl(store)

    async def read(self, ticket, operation_id, payload=None) -> dict:
        payload = _read_payload({} if payload is None else payload)
        binding_id = payload.get("binding_id")
        limit = payload["limit"]
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.execution.read", operation_id, payload
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
                    "SELECT "
                    "count(*) FILTER(WHERE status='PENDING') AS pending_requests,"
                    "count(*) FILTER(WHERE status='CLAIMED') AS claimed_requests,"
                    "count(*) FILTER(WHERE status='FAILED') AS failed_requests,"
                    "count(*) FILTER(WHERE status='DENIED') AS denied_requests,"
                    "min(created_at) FILTER(WHERE status IN ('PENDING','CLAIMED')) AS oldest_request_at "
                    "FROM public.bot_business_command_requests WHERE bot_id=%s "
                    "AND (%s::text IS NULL OR binding_id=%s)",
                    (self.store.bot_id, binding_id, binding_id),
                )
            ).fetchone()
            classification = await (
                await conn.execute(
                    "SELECT count(*) FILTER(WHERE i.business_classification_pending) AS pending,"
                    "count(*) FILTER(WHERE i.business_classification_attempt_count>0) AS retried,"
                    "min(i.received_at) FILTER(WHERE i.business_classification_pending) AS oldest_at "
                    "FROM public.bot_message_inbox i JOIN public.bot_conversations c "
                    "ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id "
                    "WHERE i.bot_id=%s AND (%s::text IS NULL OR c.binding_id=%s)",
                    (self.store.bot_id, binding_id, binding_id),
                )
            ).fetchone()
            notice_summary = await (
                await conn.execute(
                    "SELECT count(*) FILTER(WHERE n.status='PENDING') AS pending,"
                    "count(*) FILTER(WHERE n.status='CLAIMED') AS claimed,"
                    "min(n.created_at) FILTER(WHERE n.status IN ('PENDING','CLAIMED')) AS oldest_at "
                    "FROM public.bot_conversation_notices n JOIN public.bot_conversations c "
                    "ON c.bot_id=n.bot_id AND c.conversation_id=n.conversation_id "
                    "WHERE n.bot_id=%s AND (%s::text IS NULL OR c.binding_id=%s)",
                    (self.store.bot_id, binding_id, binding_id),
                )
            ).fetchone()
            requests = await (
                await conn.execute(
                    "SELECT request_id,event_id,conversation_id,binding_id,ingress_seq,command_name,"
                    "status,terminal_reason,execution_attempt_count,next_attempt_at,"
                    "execution_deadline_at,last_error_code,created_at,completed_at "
                    "FROM public.bot_business_command_requests WHERE bot_id=%s "
                    "AND (%s::text IS NULL OR binding_id=%s) "
                    "ORDER BY created_at DESC,request_id DESC LIMIT %s",
                    (self.store.bot_id, binding_id, binding_id, limit),
                )
            ).fetchall()
            request_ids = [row["request_id"] for row in requests]
            attempts = []
            if request_ids:
                attempts = await (
                    await conn.execute(
                        "SELECT request_id,attempt_no,error_code,retryable,retry_at,created_at "
                        "FROM public.bot_business_command_attempts WHERE bot_id=%s "
                        "AND request_id=ANY(%s) ORDER BY created_at DESC,request_id,attempt_no DESC",
                        (self.store.bot_id, request_ids),
                    )
                ).fetchall()
            classifications = await (
                await conn.execute(
                    "SELECT i.event_id,i.conversation_id,c.binding_id,i.ingress_seq,"
                    "i.trigger_decision,i.business_classification_pending,"
                    "i.business_classification_attempt_count,i.business_classification_next_attempt_at,"
                    "i.classification_deadline,i.business_classification_error_code,"
                    "i.received_at AS created_at "
                    "FROM public.bot_message_inbox i JOIN public.bot_conversations c "
                    "ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id "
                    "WHERE i.bot_id=%s AND (i.business_classification_pending "
                    "OR i.business_classification_attempt_count>0) "
                    "AND (%s::text IS NULL OR c.binding_id=%s) "
                    "ORDER BY i.received_at DESC,i.event_id DESC LIMIT %s",
                    (self.store.bot_id, binding_id, binding_id, limit),
                )
            ).fetchall()
            event_ids = [row["event_id"] for row in classifications]
            classification_attempts = []
            if event_ids:
                classification_attempts = await (
                    await conn.execute(
                        "SELECT event_id,attempt_no,error_code,retry_at,created_at "
                        "FROM public.bot_business_classification_attempts WHERE bot_id=%s "
                        "AND event_id=ANY(%s) ORDER BY created_at DESC,event_id,attempt_no DESC",
                        (self.store.bot_id, event_ids),
                    )
                ).fetchall()
            notices = await (
                await conn.execute(
                    "SELECT n.notice_id,n.event_id,n.request_id,n.conversation_id,c.binding_id,"
                    "n.ingress_seq,n.kind,n.error_code,n.status,n.created_at,n.completed_at,"
                    "r.response_seq FROM public.bot_conversation_notices n "
                    "JOIN public.bot_conversations c ON c.bot_id=n.bot_id "
                    "AND c.conversation_id=n.conversation_id "
                    "LEFT JOIN public.bot_conversation_notice_results r ON r.bot_id=n.bot_id "
                    "AND r.notice_id=n.notice_id WHERE n.bot_id=%s "
                    "AND (%s::text IS NULL OR c.binding_id=%s) "
                    "ORDER BY n.created_at DESC,n.notice_id DESC LIMIT %s",
                    (self.store.bot_id, binding_id, binding_id, limit),
                )
            ).fetchall()
            now = await self.store._now(conn)
            await self.control._time(conn, claims)

        def age(value):
            return max(0.0, (now - value).total_seconds()) if value is not None else None

        return {
            "filter": {"binding_id": binding_id, "limit": limit},
            "summary": {
                "pending_requests": summary["pending_requests"],
                "claimed_requests": summary["claimed_requests"],
                "failed_requests": summary["failed_requests"],
                "denied_requests": summary["denied_requests"],
                "pending_classifications": classification["pending"],
                "retried_classifications": classification["retried"],
                "pending_notices": notice_summary["pending"],
                "claimed_notices": notice_summary["claimed"],
                "oldest_request_age_seconds": age(summary["oldest_request_at"]),
                "oldest_classification_age_seconds": age(classification["oldest_at"]),
                "oldest_notice_age_seconds": age(notice_summary["oldest_at"]),
            },
            "requests": [
                {
                    **row,
                    "next_attempt_at": _iso(row["next_attempt_at"]),
                    "execution_deadline_at": _iso(row["execution_deadline_at"]),
                    "created_at": _iso(row["created_at"]),
                    "completed_at": _iso(row["completed_at"]),
                }
                for row in requests
            ],
            "execution_attempts": [
                {**row, "retry_at": _iso(row["retry_at"]), "created_at": _iso(row["created_at"])}
                for row in attempts
            ],
            "classifications": [
                {
                    **row,
                    "business_classification_next_attempt_at": _iso(
                        row["business_classification_next_attempt_at"]
                    ),
                    "classification_deadline": _iso(row["classification_deadline"]),
                    "created_at": _iso(row["created_at"]),
                }
                for row in classifications
            ],
            "classification_attempts": [
                {**row, "retry_at": _iso(row["retry_at"]), "created_at": _iso(row["created_at"])}
                for row in classification_attempts
            ],
            "notices": [
                {**row, "created_at": _iso(row["created_at"]), "completed_at": _iso(row["completed_at"])}
                for row in notices
            ],
        }

    async def recover(self, ticket, operation_id, payload) -> dict:
        payload = _recovery_payload(payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.execution.recover", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            await self.control._time(conn, claims)

        retried = await self.messages.retry_business_classifications(
            self.lease, limit=payload["limit"]
        )
        expired = await self.messages.expire_classifications(
            self.lease, limit=payload["limit"]
        )
        result = {
            "operation_id": operation_id,
            "classification_completed": retried,
            "classification_expired": expired,
            "limit": payload["limit"],
        }
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "command.execution.recover", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            await self.control._record(conn, claims, result)
        return result
