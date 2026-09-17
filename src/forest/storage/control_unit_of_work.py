"""UnitOfWork coordinators for typed control results."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.shell_command_authority import ShellClaim, ShellPayloadRef


class ShellControlResultUnitOfWork:
    """Commit the Shell result and its CONTROL SendIntent atomically."""

    def __init__(self, store, *, shell, delivery) -> None:
        self.store = store
        self.shell = shell
        self.delivery = delivery

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
        authorization_ticket_id: str,
        authorization_operation_id: str,
        error_code: str | None = None,
    ) -> dict:
        if type(claim) is not ShellClaim or claim.binding_ref != binding_ref:
            raise ForestError(
                "SHELL_RESULT_IDENTITY_CONFLICT",
                "CONTROL delivery differs from the Shell claim",
                409,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
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
            return {"shell": shell_result, "delivery": delivery_result}
