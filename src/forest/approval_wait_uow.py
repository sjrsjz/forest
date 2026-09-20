"""Atomic Agent approval wait registration across module-owned authorities."""

from __future__ import annotations

from forest.errors import ForestError


class ApprovalWaitUnitOfWork:
    """Prevent a decision racing ahead of Conversation deferral from losing its wake."""

    def __init__(self, store, *, authorization, agent, conversation) -> None:
        self.store = store
        self.authorization = authorization
        self.agent = agent
        self.conversation = conversation

    async def defer(
        self,
        actor,
        *,
        approval_ref: str,
        delay_seconds: float,
    ) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            approval = await self.authorization.approval_in(
                conn,
                approval_ref,
                update=True,
            )
            if (
                approval["request_kind"] != "agent.tool"
                or approval["requester_kind"] != "component"
                or approval["requester_id"] != "agent.runtime"
            ):
                raise ForestError(
                    "AGENT_APPROVAL_SCOPE_INVALID",
                    "Approval does not belong to an Agent tool call",
                    409,
                )
            context = await self.agent.approval_context_in(conn, approval_ref)
            if context["conversation_ref"] != actor.conversation_id:
                raise ForestError(
                    "AGENT_APPROVAL_CONVERSATION_CONFLICT",
                    "Approval belongs to another Conversation",
                    409,
                )
            if approval["state"] != "PENDING":
                return {
                    "deferred": False,
                    "approval_state": approval["state"],
                    "conversation_id": actor.conversation_id,
                }
            lane = await self.conversation.defer_actor_in(
                conn,
                actor,
                delay_seconds=delay_seconds,
                reason_code="AGENT_WAITING_APPROVAL",
                reason_ref=approval_ref,
            )
            return {
                "deferred": True,
                "approval_state": approval["state"],
                "conversation_id": actor.conversation_id,
                "resume_not_before": lane["resume_not_before"],
            }
