"""Atomic final reply commit across Agent, Conversation, and Delivery authorities."""

from __future__ import annotations

import inspect
from dataclasses import dataclass

from forest.errors import ForestError


async def _call(port, conn, **values):
    result = port(conn, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class RuntimeParticipant:
    component_id: str
    generation_id: str


@dataclass(frozen=True)
class FinalReplyPlan:
    session_ref: str
    run_ref: str
    completion_checkpoint_id: str
    completion_state: object
    context_operation_id: str
    facts: tuple
    answered_trigger_ids: tuple[str, ...]
    reply_ref: str
    delivery_operation_id: str
    intent_id: str
    binding_ref: str
    ordering_key: str
    channel_kind: str
    channel_ref: str
    delivery_generation_id: str
    parts: tuple


class FinalReplyUnitOfWork:
    """Commit one visible answer without sharing table ownership.

    Each participant validates and writes only its own namespace.  The
    coordinator supplies the common PostgreSQL transaction and verifies every
    exact runtime Generation before the first participant mutates state.
    """

    def __init__(
        self,
        *,
        store,
        agent_participant,
        conversation_participant,
        delivery_participant,
        generation_member_verifier,
        participants: tuple[RuntimeParticipant, ...],
    ) -> None:
        expected = {"agent.runtime", "conversation.scheduler", "delivery"}
        actual = {item.component_id for item in participants}
        if len(participants) != 3 or actual != expected:
            raise ValueError("final reply requires exact Agent, Conversation, and Delivery participants")
        if generation_member_verifier is None:
            raise ValueError("final reply Generation verifier is required")
        self.store = store
        self.agent_participant = agent_participant
        self.conversation_participant = conversation_participant
        self.delivery_participant = delivery_participant
        self.generation_member_verifier = generation_member_verifier
        self.participants = tuple(sorted(participants, key=lambda item: item.component_id))
        self._generation_ids = {
            item.component_id: item.generation_id for item in self.participants
        }

    async def commit(self, *, actor, agent_owner, plan: FinalReplyPlan) -> dict:
        if type(plan) is not FinalReplyPlan:
            raise ForestError(
                "FINAL_REPLY_INPUT_INVALID", "Final reply plan is invalid", 422
            )
        if not plan.answered_trigger_ids:
            raise ForestError(
                "FINAL_REPLY_INPUT_INVALID", "Final reply answers no trigger", 422
            )
        if (
            len(set(plan.answered_trigger_ids)) != len(plan.answered_trigger_ids)
            or plan.run_ref != agent_owner.run_id
            or plan.ordering_key != actor.conversation_id
            or plan.delivery_generation_id != self._generation_ids["delivery"]
        ):
            raise ForestError(
                "FINAL_REPLY_INPUT_INVALID", "Final reply fences are inconsistent", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            for participant in self.participants:
                await _call(
                    self.generation_member_verifier,
                    conn,
                    component_id=participant.component_id,
                    generation_id=participant.generation_id,
                )

            agent_result = await self.agent_participant.transition_in(
                conn,
                agent_owner,
                checkpoint_id=plan.completion_checkpoint_id,
                expected_phase="FINAL_READY",
                target_phase="COMPLETED",
                state=plan.completion_state,
            )
            context_result = await self.conversation_participant.append_context_in(
                conn,
                actor,
                operation_id=plan.context_operation_id,
                facts=plan.facts,
                session_ref=plan.session_ref,
                run_ref=plan.run_ref,
                answer_trigger_ids=plan.answered_trigger_ids,
                reply_ref=plan.reply_ref,
            )
            delivery_result = await self.delivery_participant.create_intent_in(
                conn,
                operation_id=plan.delivery_operation_id,
                intent_id=plan.intent_id,
                binding_ref=plan.binding_ref,
                lane_kind="CHAT",
                ordering_key=plan.ordering_key,
                conversation_ref=actor.conversation_id,
                channel_kind=plan.channel_kind,
                channel_ref=plan.channel_ref,
                cause_component_id="conversation.scheduler",
                cause_kind="context.reply",
                cause_id=plan.reply_ref,
                delivery_generation_id=plan.delivery_generation_id,
                reply_ref=plan.reply_ref,
                parts=plan.parts,
            )
            return {
                "agent": agent_result,
                "context": context_result,
                "delivery": delivery_result,
            }
