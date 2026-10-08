"""Immutable management audit authority for one control scope."""

from __future__ import annotations

import hashlib

from forest.errors import ForestError
from forest.web_authority_ticket import WebAuthorityTicket

_SCOPES = {
    "deployment": ("forest_deployment", "deployment_id"),
    "host": ("forest_host", "host_id"),
}


class ControlAuditRepository:
    """Append immutable Web envelopes to the owning control scope."""

    scope_kind: str

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
        if self.scope_kind not in _SCOPES:
            raise ValueError("Control audit scope is invalid")
        self.store = store
        self.schema, self.id_column = _SCOPES[self.scope_kind]
        self.scope_id = getattr(store, self.id_column)

    async def append_web_action_in(
        self, conn, *, ticket: WebAuthorityTicket, receipt: dict
    ) -> dict:
        return await self._append_in(
            conn, ticket=ticket, operation_kind="web.action",
            receipt_ticket_id=receipt["ticket_id"],
        )

    async def append_web_query(self, *, ticket: WebAuthorityTicket) -> dict:
        """Record an authorized read attempt without manufacturing a write receipt."""
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._append_in(
                conn, ticket=ticket, operation_kind="web.query", receipt_ticket_id=None,
            )

    async def _append_in(
        self, conn, *, ticket: WebAuthorityTicket,
        operation_kind: str, receipt_ticket_id: str | None,
    ) -> dict:
        if not isinstance(ticket, WebAuthorityTicket):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid Web authority ticket", 403)
        expected_scope_id = "deployment" if self.scope_kind == "deployment" else self.scope_id
        if (
            ticket.target_scope_kind != self.scope_kind
            or ticket.target_scope_id != expected_scope_id
        ):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                "Web audit ticket belongs to another control scope", 403,
            )
        audit_id = "audit:" + hashlib.sha256(
            (self.scope_id + "\0" + ticket.target_operation_ref).encode()
        ).hexdigest()
        values = {
            "action": ticket.action_key,
            "severity": "SECURITY",
            "producer_component_id": ticket.target_component_id,
            "producer_generation_id": ticket.target_generation_id,
            "origin_kind": "web.management",
            "actor_kind": "web.subject",
            "actor_id": ticket.web_subject_ref,
            "resource_kind": self.scope_kind + ".component",
            "resource_id": ticket.target_component_id,
            "operation_kind": operation_kind,
            "operation_id": ticket.target_operation_ref,
            "receipt_ticket_id": receipt_ticket_id,
        }
        await conn.execute(
            f"INSERT INTO {self.schema}.audit_records("
            f"{self.id_column},audit_id,action,severity,producer_component_id,"
            "producer_generation_id,origin_kind,actor_kind,actor_id,resource_kind,"
            "resource_id,operation_kind,operation_id,receipt_ticket_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            f"ON CONFLICT ({self.id_column},audit_id) DO NOTHING",
            (
                self.scope_id,
                audit_id,
                *(values[field] for field in self._IMMUTABLE_FIELDS),
            ),
        )
        row = await (
            await conn.execute(
                f"SELECT * FROM {self.schema}.audit_records "
                f"WHERE {self.id_column}=%s AND audit_id=%s",
                (self.scope_id, audit_id),
            )
        ).fetchone()
        if row is None or any(row[field] != values[field] for field in self._IMMUTABLE_FIELDS):
            raise ForestError(
                self.scope_kind.upper() + "_AUDIT_ID_CONFLICT",
                "Control audit identity belongs to another operation",
                409,
            )
        return self._project(row)

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
