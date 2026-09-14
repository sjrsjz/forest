"""Bounded, redacted deployment audit queries."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any

from forest.errors import ForestError

_DETAIL_FIELDS = {
    "deployment.setup": frozenset({"setup_id", "plan_digest"}),
    "config.apply": frozenset({"revision", "previous_revision", "plan_id", "plan_digest"}),
    "package.import": frozenset({"package_id", "version"}),
    "bot.create": frozenset({"job_id", "revision"}),
    "bot.rename": frozenset({"job_id", "revision"}),
    "bot.retry": frozenset({"job_id", "revision"}),
    "binding.reserve": frozenset({"bot_id", "binding_id"}),
    "backup.create": frozenset({"backup_id", "include_packages"}),
    "restore.stage.create": frozenset({"restore_id", "backup_id", "database_total"}),
    "restore.stage.completed": frozenset({
        "restore_id", "backup_id", "database_total", "manifest_digest",
    }),
    "account.create": frozenset({"username", "role", "revision"}),
    "account.apply": frozenset({
        "previous_revision", "revision", "role", "disabled", "password_changed",
        "revoked_sessions", "plan_id", "plan_digest",
    }),
}
_SCALAR_TYPES = (str, int, float, bool, type(None))


def _cursor(created_at: datetime, audit_id: str) -> str:
    value = json.dumps(
        {"created_at": created_at.isoformat(), "audit_id": audit_id},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_cursor(value: str | None) -> tuple[datetime, str] | None:
    if value is None:
        return None
    if not 1 <= len(value) <= 512:
        raise ForestError("AUDIT_CURSOR_INVALID", "Audit cursor is invalid", 422)
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        data = json.loads(raw.decode("ascii"))
        if not isinstance(data, dict) or set(data) != {"created_at", "audit_id"}:
            raise ValueError
        created_at = datetime.fromisoformat(data["created_at"])
        audit_id = data["audit_id"]
        if created_at.tzinfo is None or not isinstance(audit_id, str) or not 1 <= len(audit_id) <= 128:
            raise ValueError
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
        raise ForestError("AUDIT_CURSOR_INVALID", "Audit cursor is invalid", 422) from None
    return created_at, audit_id


def _details(action: str, value: Any) -> tuple[dict[str, Any], bool]:
    if not isinstance(value, dict):
        return {}, True
    allowed = _DETAIL_FIELDS.get(action, frozenset())
    result = {
        key: item
        for key, item in value.items()
        if key in allowed and isinstance(item, _SCALAR_TYPES)
    }
    return result, set(value) != set(result)


class DeploymentAudit:
    """Read deployment audit facts without hydrating arbitrary stored JSON."""

    def __init__(self, store):
        self.store = store

    async def read(
        self,
        *,
        subject_id: str,
        session_token_hash: str,
        before: str | None = None,
        limit: int = 50,
        action: str | None = None,
        resource_prefix: str | None = None,
        actor_id: str | None = None,
    ) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ForestError("AUDIT_QUERY_INVALID", "Audit page limit is 1 to 200", 422)
        for value in (action, resource_prefix, actor_id):
            if value is not None and (
                not isinstance(value, str)
                or not 1 <= len(value) <= 256
                or any(ord(char) < 32 for char in value)
            ):
                raise ForestError("AUDIT_QUERY_INVALID", "Audit filter is invalid", 422)
        position = _decode_cursor(before)
        conditions, parameters = [], []
        if position is not None:
            conditions.append("(a.created_at,a.audit_id)<(%s,%s)")
            parameters.extend(position)
        if action is not None:
            conditions.append("a.action=%s")
            parameters.append(action)
        if resource_prefix is not None:
            conditions.append("starts_with(a.resource_key,%s)")
            parameters.append(resource_prefix)
        if actor_id is not None:
            conditions.append("a.subject_id=%s")
            parameters.append(actor_id)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        query = (
            "SELECT a.audit_id,a.subject_id,w.username AS subject_username,a.action,a.resource_key,"
            "a.request_id,a.details,a.created_at FROM audit_records a "
            "JOIN web_accounts w ON w.id=a.subject_id" + where +
            " ORDER BY a.created_at DESC,a.audit_id DESC LIMIT %s"
        )
        parameters.append(limit + 1)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._active_account(conn, subject_id, require_admin=True)
            await self.store._active_session(conn, session_token_hash, subject_id)
            rows = await (await conn.execute(query, parameters)).fetchall()
            await self.store._active_account(conn, subject_id, require_admin=True)
            await self.store._active_session(conn, session_token_hash, subject_id)
        page, has_more = rows[:limit], len(rows) > limit
        records = []
        for row in page:
            details, redacted = _details(row["action"], row["details"])
            records.append({
                "audit_id": row["audit_id"],
                "actor": {"account_id": row["subject_id"], "username": row["subject_username"]},
                "action": row["action"],
                "resource_key": row["resource_key"],
                "request_id": row["request_id"],
                "details": details,
                "details_redacted": redacted,
                "created_at": row["created_at"].isoformat(),
            })
        return {
            "records": records,
            "next_before": _cursor(page[-1]["created_at"], page[-1]["audit_id"])
            if has_more else None,
        }
