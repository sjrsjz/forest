"""Immutable Web authority receipts for deployment-scoped mutations."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json
from forest.web_authority_ticket import WebAuthorityTicket


class DeploymentWebAuthorityRepository:
    """Consume a signed Web ticket in the target deployment transaction."""

    def __init__(self, store) -> None:
        self.store = store

    async def accept_in(self, conn, *, ticket: WebAuthorityTicket) -> dict:
        if not isinstance(ticket, WebAuthorityTicket):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid Web authority ticket", 403)
        if (
            ticket.target_scope_kind != "deployment"
            or ticket.target_scope_id != "deployment"
            or ticket.target_graph_revision != 1
        ):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                "Web authority ticket does not name this deployment control plane",
                403,
            )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.web-authority/{ticket.target_operation_ref}",),
        )
        expected = {
            "issuer_id": ticket.issuer_id,
            "web_subject_ref": ticket.web_subject_ref,
            "web_role": ticket.web_role,
            "session_ref": ticket.session_ref,
            "session_revision": ticket.session_revision,
            "target_scope_kind": ticket.target_scope_kind,
            "target_scope_id": ticket.target_scope_id,
            "target_component_id": ticket.target_component_id,
            "target_generation_id": ticket.target_generation_id,
            "target_graph_revision": ticket.target_graph_revision,
            "target_operation_ref": ticket.target_operation_ref,
            "action_key": ticket.action_key,
            "input_digest": ticket.input_digest,
            "plan_digest": ticket.plan_digest,
        }
        existing = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.web_authority_receipts "
                "WHERE deployment_id=%s AND (ticket_id=%s OR target_operation_ref=%s)",
                (self.store.deployment_id, ticket.ticket_id, ticket.target_operation_ref),
            )
        ).fetchone()
        if existing is not None:
            if any(existing[key] != value for key, value in expected.items()):
                raise ForestError(
                    "WEB_AUTHORITY_RECEIPT_CONFLICT",
                    "Web authority operation identity differs",
                    409,
                )
            return dict(existing)
        now = (
            await (await conn.execute("SELECT clock_timestamp() AS value")).fetchone()
        )["value"]
        if ticket.expires_at <= now:
            raise ForestError("WEB_AUTHORITY_TICKET_EXPIRED", "Web authority ticket has expired", 403)
        ticket_digest = digest_json({"payload": ticket.payload(), "signature": ticket.signature})
        row = await (
            await conn.execute(
                "INSERT INTO forest_deployment.web_authority_receipts("
                "deployment_id,ticket_id,ticket_digest,issuer_id,key_id,web_subject_ref,"
                "web_role,session_ref,session_revision,target_scope_kind,target_scope_id,"
                "target_component_id,target_generation_id,target_graph_revision,"
                "target_operation_ref,action_key,input_digest,plan_digest,issued_at,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    self.store.deployment_id,
                    ticket.ticket_id,
                    ticket_digest,
                    ticket.issuer_id,
                    ticket.key_id,
                    ticket.web_subject_ref,
                    ticket.web_role,
                    ticket.session_ref,
                    ticket.session_revision,
                    ticket.target_scope_kind,
                    ticket.target_scope_id,
                    ticket.target_component_id,
                    ticket.target_generation_id,
                    ticket.target_graph_revision,
                    ticket.target_operation_ref,
                    ticket.action_key,
                    ticket.input_digest,
                    ticket.plan_digest,
                    ticket.issued_at,
                    ticket.expires_at,
                ),
            )
        ).fetchone()
        return dict(row)

    async def accept(self, *, ticket: WebAuthorityTicket) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.accept_in(conn, ticket=ticket)
