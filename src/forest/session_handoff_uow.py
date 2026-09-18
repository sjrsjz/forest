"""Atomic handoff between a completed Agent Run and Conversation scheduling."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_runtime_authority import AgentInputBatch, AgentTriggerInput


async def _call(port, conn, **values):
    result = port(conn, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class SessionHandoffPlan:
    selection_operation_id: str
    session_id: str
    completed_run_id: str
    successor_run_id: str
    successor_start_operation_id: str
    session_close_operation_id: str
    successor_deadline: datetime


def agent_input_from_selection(selection: dict) -> AgentInputBatch:
    triggers = tuple(
        AgentTriggerInput(
            item["trigger_id"],
            item["principal_ref"],
            item["ingress_seq"],
        )
        for item in selection["triggers"]
    )
    source_manifest = {
        "conversation_id": selection["conversation_id"],
        "input_batch_ref": selection["input_batch_id"],
        "event_ids": selection["event_ids"],
        "triggers": selection["triggers"],
        "selected_input_end_seq": selection["selected_input_end_seq"],
        "context_revision": selection["context_revision"],
        "context_view_revision": selection["context_view_revision"],
    }
    return AgentInputBatch(
        input_batch_ref=selection["input_batch_id"],
        input_batch_digest=digest_json(source_manifest),
        selected_input_end_seq=selection["selected_input_end_seq"],
        context_revision=selection["context_revision"],
        context_view_revision=selection["context_view_revision"],
        source_manifest_digest=digest_json(source_manifest),
        triggers=triggers,
    )


class SessionHandoffUnitOfWork:
    """Select a successor input cut and update the Agent Session atomically."""

    def __init__(
        self,
        *,
        store,
        agent_participant,
        conversation_participant,
        generation_member_verifier,
        agent_generation_id: str,
        conversation_generation_id: str,
    ) -> None:
        if not agent_generation_id or not conversation_generation_id:
            raise ValueError("Session handoff needs exact participant Generations")
        self.store = store
        self.agent_participant = agent_participant
        self.conversation_participant = conversation_participant
        self.generation_member_verifier = generation_member_verifier
        self.participants = (
            ("agent.runtime", agent_generation_id),
            ("conversation.scheduler", conversation_generation_id),
        )

    async def decide(self, *, actor, plan: SessionHandoffPlan) -> dict:
        if type(plan) is not SessionHandoffPlan:
            raise ForestError(
                "SESSION_HANDOFF_INPUT_INVALID", "Session handoff plan is invalid", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            for component_id, generation_id in self.participants:
                await _call(
                    self.generation_member_verifier,
                    conn,
                    component_id=component_id,
                    generation_id=generation_id,
                )
            selection = await self.conversation_participant.select_input_in(
                conn,
                actor,
                plan.selection_operation_id,
                boundary="SESSION_END",
                session_ref=plan.session_id,
                run_ref=plan.completed_run_id,
                next_run_ref=plan.successor_run_id,
            )
            if selection.get("kind") == "TRIGGER_INPUT":
                successor = await self.agent_participant.start_successor_run_in(
                    conn,
                    operation_id=plan.successor_start_operation_id,
                    session_id=plan.session_id,
                    completed_run_id=plan.completed_run_id,
                    run_id=plan.successor_run_id,
                    input_batch=agent_input_from_selection(selection),
                    absolute_deadline=plan.successor_deadline,
                )
                return {"action": "SUCCESSOR", "selection": selection, "run": successor}
            if selection["status"] == "WAIT_INPUT":
                return {"action": "WAIT_INPUT", "selection": selection}
            if selection["status"] == "IDLE" or selection.get("kind") == "SILENT_DRAIN":
                closed = await self.agent_participant.close_session_in(
                    conn,
                    operation_id=plan.session_close_operation_id,
                    session_id=plan.session_id,
                    completed_run_id=plan.completed_run_id,
                )
                return {"action": "CLOSED", "selection": selection, "session": closed}
            raise ForestError(
                "SESSION_HANDOFF_RESULT_INVALID",
                "Conversation returned an invalid Session-end decision",
                500,
            )
