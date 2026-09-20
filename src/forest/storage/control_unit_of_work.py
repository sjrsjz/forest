"""UnitOfWork coordinators for typed control results."""

from __future__ import annotations

import hashlib

from forest.errors import ForestError
from forest.storage.kernel_audit import AuditEnvelope
from forest.storage.shell_command_authority import ShellClaim, ShellPayloadRef


class ShellControlResultUnitOfWork:
    """Commit the Shell result and its CONTROL SendIntent atomically."""

    def __init__(self, store, *, shell, delivery, audit) -> None:
        self.store = store
        self.shell = shell
        self.delivery = delivery
        self.audit = audit

    async def commit(
        self,
        claim: ShellClaim,
        *,
        result_operation_id: str,
        resulting_state: str,
        result: ShellPayloadRef,
        intent_operation_id: str,
        intent_id: str,
        binding_ref: str,
        channel_kind: str,
        channel_ref: str,
        delivery_generation_id: str,
        parts,
        producer_generation_id: str,
        authorization_ticket_id: str | None = None,
        authorization_operation_id: str | None = None,
        error_code: str | None = None,
        terminal_without_authorization: bool = False,
    ) -> dict:
        if type(claim) is not ShellClaim or claim.binding_ref != binding_ref:
            raise ForestError(
                "SHELL_RESULT_IDENTITY_CONFLICT",
                "CONTROL delivery differs from the Shell claim",
                409,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            if terminal_without_authorization:
                if error_code is None:
                    raise ForestError(
                        "SHELL_RESULT_IDENTITY_CONFLICT",
                        "Unprivileged terminal result requires an error code",
                        409,
                    )
                shell_result = await self.shell.fail_claim_in(
                    conn,
                    claim,
                    operation_id=result_operation_id,
                    resulting_state=resulting_state,
                    result=result,
                    error_code=error_code,
                )
            else:
                if authorization_ticket_id is None or authorization_operation_id is None:
                    raise ForestError(
                        "SHELL_RESULT_IDENTITY_CONFLICT",
                        "Successful control result requires authorization",
                        409,
                    )
                shell_result = await self.shell.complete_in(
                    conn,
                    claim,
                    operation_id=result_operation_id,
                    resulting_state=resulting_state,
                    result=result,
                    error_code=error_code,
                    authorization_ticket_id=authorization_ticket_id,
                    authorization_operation_id=authorization_operation_id,
                )
            delivery_result = await self.delivery.create_intent_in(
                conn,
                operation_id=intent_operation_id,
                intent_id=intent_id,
                binding_ref=binding_ref,
                lane_kind="CONTROL",
                ordering_key=claim.conversation_ref,
                conversation_ref=None,
                channel_kind=channel_kind,
                channel_ref=channel_ref,
                cause_component_id="shell.command",
                cause_kind="shell.request",
                cause_id=claim.request_id,
                delivery_generation_id=delivery_generation_id,
                reply_ref=None,
                parts=parts,
            )
            audit_id = "shell:result:" + hashlib.sha256(
                (claim.bot_id + "\0" + claim.request_id).encode("utf-8")
            ).hexdigest()
            audit_result = await self.audit.append_in(
                conn,
                AuditEnvelope(
                    audit_id=audit_id,
                    action="shell.command." + resulting_state.lower(),
                    producer_component_id="shell.command",
                    producer_generation_id=producer_generation_id,
                    origin_kind=channel_kind.lower() + ".shell",
                    severity=("INFO" if resulting_state == "SUCCEEDED" else "ERROR"),
                    actor_kind="principal",
                    actor_id=claim.principal_ref,
                    resource_kind="bot",
                    resource_id=claim.bot_id,
                    operation_kind="shell.request",
                    operation_id=claim.request_id,
                ),
            )
            return {
                "shell": shell_result,
                "delivery": delivery_result,
                "audit": audit_result,
            }
