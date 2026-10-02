"""Immutable deployment-scoped management audit authority."""

from __future__ import annotations

import hashlib

from forest.errors import ForestError
from forest.web_authority_ticket import WebAuthorityTicket


class DeploymentAuditRepository:
    """Append immutable Web management envelopes; query projection belongs to the package."""

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
