"""Immutable Web authority receipts for control-scope mutations."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json
from forest.web_authority_ticket import WebAuthorityTicket, WebAuthorityTicketAuthority

_SCOPE = {
    "deployment": ("forest_deployment", "deployment_id"),
    "host": ("forest_host", "host_id"),
}


class ControlWebAuthorityRepository:
    """Consume a signed Web ticket in its target control-scope transaction."""

    scope: str

    def __init__(
        self, store, *, ticket_authority: WebAuthorityTicketAuthority | None = None,
    ) -> None:
        if self.scope not in _SCOPE or (self.scope == "host" and ticket_authority is None):
            raise ValueError("Control Web receipt scope or verifier is invalid")
        self.store = store
        self.schema, self.id_column = _SCOPE[self.scope]
        self.scope_id = getattr(store, self.id_column)
        self.web_scope_id = "deployment" if self.scope == "deployment" else self.scope_id
        self.ticket_authority = ticket_authority

    async def _execute(self, conn, query: str, params=None):
        # Both SQL identifiers are fixed by _SCOPE.
        query = query.replace("__scope_schema__", self.schema)
        query = query.replace("__scope_id_column__", self.id_column)
        return await conn.execute(query, params)

    async def accept_in(self, conn, *, ticket: WebAuthorityTicket) -> dict:
        if not isinstance(ticket, WebAuthorityTicket):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid Web authority ticket", 403)
        if (
            ticket.target_scope_kind != self.scope
            or ticket.target_scope_id != self.web_scope_id
            or ticket.target_graph_revision < 1
        ):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                "Web authority ticket does not name this control scope",
                403,
            )
        if self.ticket_authority is not None:
            await self.ticket_authority.verify(
                ticket,
                target_scope_kind=self.scope, target_scope_id=self.web_scope_id,
                target_component_id=ticket.target_component_id,
                target_generation_id=ticket.target_generation_id,
                target_graph_revision=ticket.target_graph_revision,
                target_operation_ref=ticket.target_operation_ref,
                action_key=ticket.action_key, input_digest=ticket.input_digest,
                plan_digest=ticket.plan_digest, required_role="ADMIN",
            )
        await self._execute(conn,
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.{self.scope}.web-authority/{ticket.target_operation_ref}",),
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
            await self._execute(conn,
                "SELECT * FROM __scope_schema__.web_authority_receipts "
                "WHERE __scope_id_column__=%s AND (ticket_id=%s OR target_operation_ref=%s)",
                (self.scope_id, ticket.ticket_id, ticket.target_operation_ref),
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
        if ticket.target_component_id == "$runtime" or not (
            self.scope == "deployment"
            and ticket.target_graph_revision == 1
            and ticket.target_generation_id
            == f"deployment/{ticket.target_component_id}/1"
        ):
            clock = await (
                await self._execute(conn,
                    "SELECT active_graph_id,active_revision FROM "
                    "__scope_schema__.runtime_graph_clock WHERE __scope_id_column__=%s FOR SHARE",
                    (self.scope_id,),
                )
            ).fetchone()
            if clock is None or clock["active_revision"] != ticket.target_graph_revision:
                raise ForestError(
                    "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                    "Control RuntimeGraph changed before Web action", 403,
                )
            if ticket.target_component_id == "$runtime":
                matching = clock["active_graph_id"] == ticket.target_generation_id
            else:
                member = await (
                    await self._execute(conn,
                        "SELECT m.generation_id,m.control_admission,g.state FROM "
                        "__scope_schema__.runtime_graph_members m JOIN "
                        "__scope_schema__.runtime_generations g ON "
                        "g.__scope_id_column__=m.__scope_id_column__ AND g.generation_id=m.generation_id "
                        "WHERE m.__scope_id_column__=%s AND m.graph_id=%s AND m.component_id=%s",
                        (
                            self.scope_id, clock["active_graph_id"],
                            ticket.target_component_id,
                        ),
                    )
                ).fetchone()
                matching = (
                    member is not None
                    and member["generation_id"] == ticket.target_generation_id
                    and member["control_admission"]
                    and member["state"] == "ACTIVE"
                )
            if not matching:
                raise ForestError(
                    "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                    "Control Generation changed before Web action", 403,
                )
        now = (
            await (await self._execute(conn, "SELECT clock_timestamp() AS value")).fetchone()
        )["value"]
        if ticket.expires_at <= now:
            raise ForestError("WEB_AUTHORITY_TICKET_EXPIRED", "Web authority ticket has expired", 403)
        ticket_digest = digest_json({"payload": ticket.payload(), "signature": ticket.signature})
        row = await (
            await self._execute(conn,
                "INSERT INTO __scope_schema__.web_authority_receipts("
                "__scope_id_column__,ticket_id,ticket_digest,issuer_id,key_id,web_subject_ref,"
                "web_role,session_ref,session_revision,target_scope_kind,target_scope_id,"
                "target_component_id,target_generation_id,target_graph_revision,"
                "target_operation_ref,action_key,input_digest,plan_digest,issued_at,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    self.scope_id,
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
