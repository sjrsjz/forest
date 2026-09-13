"""Purpose-bound reads of immutable Bot audit metadata."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.control import BotControl

_KEYS = frozenset({"action", "before", "limit"})


def _identity(value, field):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or any(not (char.isascii() and (char.isalnum() or char in "_.:-")) for char in value)
    ):
        raise ForestError("AUDIT_QUERY_INVALID", f"Invalid {field}", 422)
    return value


def _payload(value):
    if not isinstance(value, dict) or not set(value) <= _KEYS:
        raise ForestError("AUDIT_QUERY_INVALID", "Audit query fields differ", 422)
    result = {}
    if "action" in value:
        result["action"] = _identity(value["action"], "action")
    if "before" in value:
        result["before"] = _identity(value["before"], "cursor")
    limit = value.get("limit", 50)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ForestError("AUDIT_QUERY_INVALID", "Audit limit is outside 1..100", 422)
    result["limit"] = limit
    return result


class BotAuditStore:
    """Expose ordering and origin metadata without hydrating retained result bodies."""

    def __init__(self, store, *, control=None):
        self.store = store
        self.control = control or BotControl(store)

    async def read(self, ticket, operation_id, payload=None) -> dict:
        payload = _payload({} if payload is None else payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "audit.read", operation_id, payload
            )
            before = payload.get("before")
            cursor = None
            if before is not None:
                cursor = await (
                    await conn.execute(
                        "SELECT created_at,audit_id FROM public.bot_audit_records "
                        "WHERE bot_id=%s AND audit_id=%s",
                        (self.store.bot_id, before),
                    )
                ).fetchone()
                if cursor is None:
                    raise ForestError(
                        "AUDIT_CURSOR_NOT_FOUND", "Audit cursor is unavailable", 404
                    )
            rows = await (
                await conn.execute(
                    "SELECT a.audit_id,a.action,a.operation_id,a.created_at,"
                    "(a.control_result_payload_id IS NOT NULL) AS has_receipt,"
                    "CASE WHEN left(o.issuer_id,6)='shell:' THEN 'shell' "
                    "WHEN o.operation_id IS NOT NULL THEN 'web' ELSE 'runtime' END AS origin_kind "
                    "FROM public.bot_audit_records a LEFT JOIN public.bot_web_operations o "
                    "ON o.bot_id=a.bot_id AND o.operation_id=a.operation_id "
                    "WHERE a.bot_id=%s AND (%s::text IS NULL OR a.action=%s) "
                    "AND (%s::timestamptz IS NULL OR (a.created_at,a.audit_id)<(%s,%s)) "
                    "ORDER BY a.created_at DESC,a.audit_id DESC LIMIT %s",
                    (
                        self.store.bot_id,
                        payload.get("action"),
                        payload.get("action"),
                        cursor["created_at"] if cursor else None,
                        cursor["created_at"] if cursor else None,
                        cursor["audit_id"] if cursor else "",
                        payload["limit"] + 1,
                    ),
                )
            ).fetchall()
            await self.control._time(conn, claims)
        visible = rows[: payload["limit"]]
        return {
            "records": [
                {
                    "audit_id": row["audit_id"],
                    "action": row["action"],
                    "operation_id": row["operation_id"],
                    "origin_kind": row["origin_kind"],
                    "has_receipt": row["has_receipt"],
                    "created_at": row["created_at"].isoformat(),
                }
                for row in visible
            ],
            "next_before": visible[-1]["audit_id"] if len(rows) > len(visible) else None,
        }
