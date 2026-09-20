"""Immutable deployment-scoped management audit authority."""

from __future__ import annotations

import hashlib
import re

from forest.errors import ForestError
from forest.web_authority_ticket import WebAuthorityTicket

_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]*\Z")


def _identity(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or _IDENTITY.fullmatch(value) is None
    ):
        raise ForestError("DEPLOYMENT_AUDIT_QUERY_INVALID", f"Invalid {field}", 422)
    return value


def _pair(kind, identity, label):
    if (kind is None) != (identity is None):
        raise ForestError(
            "DEPLOYMENT_AUDIT_QUERY_INVALID",
            f"{label} kind and identity must appear together",
            422,
        )
    if kind is not None:
        _identity(kind, f"{label}_kind", 64)
        if not isinstance(identity, str) or not 1 <= len(identity.encode("utf-8")) <= 256:
            raise ForestError(
                "DEPLOYMENT_AUDIT_QUERY_INVALID", f"Invalid {label}_id", 422
            )


class DeploymentAuditRepository:
    """Append Web management envelopes and expose only redacted metadata."""

    _IMMUTABLE_FIELDS = (
        "action",
        "severity",
        "producer_component_id",
        "producer_generation_id",
        "origin_kind",
        "actor_kind",
        "actor_id",
        "resource_kind",
        "resource_id",
        "operation_kind",
        "operation_id",
        "receipt_ticket_id",
    )

    def __init__(self, store) -> None:
        self.store = store

    async def append_web_action_in(
        self, conn, *, ticket: WebAuthorityTicket, receipt: dict
    ) -> dict:
        if not isinstance(ticket, WebAuthorityTicket):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid Web authority ticket", 403)
        audit_id = "audit:" + hashlib.sha256(
            (self.store.deployment_id + "\0" + ticket.target_operation_ref).encode()
        ).hexdigest()
        values = {
            "action": ticket.action_key,
            "severity": "SECURITY",
            "producer_component_id": ticket.target_component_id,
            "producer_generation_id": ticket.target_generation_id,
            "origin_kind": "web.management",
            "actor_kind": "web.subject",
            "actor_id": ticket.web_subject_ref,
            "resource_kind": "deployment.component",
            "resource_id": ticket.target_component_id,
            "operation_kind": "web.action",
            "operation_id": ticket.target_operation_ref,
            "receipt_ticket_id": receipt["ticket_id"],
        }
        await conn.execute(
            "INSERT INTO forest_deployment.audit_records("
            "deployment_id,audit_id,action,severity,producer_component_id,"
            "producer_generation_id,origin_kind,actor_kind,actor_id,resource_kind,"
            "resource_id,operation_kind,operation_id,receipt_ticket_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (deployment_id,audit_id) DO NOTHING",
            (
                self.store.deployment_id,
                audit_id,
                *(values[field] for field in self._IMMUTABLE_FIELDS),
            ),
        )
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.audit_records "
                "WHERE deployment_id=%s AND audit_id=%s",
                (self.store.deployment_id, audit_id),
            )
        ).fetchone()
        if row is None or any(row[field] != values[field] for field in self._IMMUTABLE_FIELDS):
            raise ForestError(
                "DEPLOYMENT_AUDIT_ID_CONFLICT",
                "Deployment audit identity belongs to another operation",
                409,
            )
        return self._project(row)

    async def scan(
        self,
        *,
        action: str | None = None,
        producer_component_id: str | None = None,
        origin_kind: str | None = None,
        actor_kind: str | None = None,
        actor_id: str | None = None,
        resource_kind: str | None = None,
        resource_id: str | None = None,
        operation_kind: str | None = None,
        operation_id: str | None = None,
        before: str | None = None,
        limit: int = 50,
    ) -> dict:
        for field, value, size in (
            ("action", action, 200),
            ("producer_component_id", producer_component_id, 128),
            ("origin_kind", origin_kind, 64),
        ):
            _identity(value, field, size, optional=True)
        for kind, identity, label in (
            (actor_kind, actor_id, "actor"),
            (resource_kind, resource_id, "resource"),
            (operation_kind, operation_id, "operation"),
        ):
            _pair(kind, identity, label)
        _identity(before, "before", 128, optional=True)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError(
                "DEPLOYMENT_AUDIT_QUERY_INVALID", "Audit limit is outside 1..100", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            cursor = None
            if before is not None:
                cursor = await (
                    await conn.execute(
                        "SELECT created_at,audit_id FROM forest_deployment.audit_records "
                        "WHERE deployment_id=%s AND audit_id=%s",
                        (self.store.deployment_id, before),
                    )
                ).fetchone()
                if cursor is None:
                    raise ForestError(
                        "DEPLOYMENT_AUDIT_CURSOR_NOT_FOUND", "Audit cursor is unavailable", 404
                    )
            clauses = ["deployment_id=%s"]
            params: list[object] = [self.store.deployment_id]
            for field, value in (
                ("action", action),
                ("producer_component_id", producer_component_id),
                ("origin_kind", origin_kind),
                ("actor_kind", actor_kind),
                ("actor_id", actor_id),
                ("resource_kind", resource_kind),
                ("resource_id", resource_id),
                ("operation_kind", operation_kind),
                ("operation_id", operation_id),
            ):
                if value is not None:
                    clauses.append(field + "=%s")
                    params.append(value)
            if cursor is not None:
                clauses.append("(created_at,audit_id)<(%s,%s)")
                params.extend((cursor["created_at"], cursor["audit_id"]))
            params.append(limit + 1)
            rows = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.audit_records WHERE "
                    + " AND ".join(clauses)
                    + " ORDER BY created_at DESC,audit_id DESC LIMIT %s",
                    params,
                )
            ).fetchall()
        visible = rows[:limit]
        return {
            "records": [self._project(row) for row in visible],
            "next_before": visible[-1]["audit_id"] if len(rows) > limit else None,
        }

    @staticmethod
    def _project(row) -> dict:
        return {
            "audit_id": row["audit_id"],
            "action": row["action"],
            "severity": row["severity"],
            "producer_component_id": row["producer_component_id"],
            "producer_generation_id": row["producer_generation_id"],
            "origin_kind": row["origin_kind"],
            "actor_kind": row["actor_kind"],
            "actor_id": row["actor_id"],
            "resource_kind": row["resource_kind"],
            "resource_id": row["resource_id"],
            "operation_kind": row["operation_kind"],
            "operation_id": row["operation_id"],
            "call_id": None,
            "details_payload_id": None,
            "details_payload_digest": None,
            "created_at": row["created_at"].isoformat(),
        }
