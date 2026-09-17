"""Transactional completion and Session decision ports for the Conversation worker."""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from typing import Protocol

from forest.conversation_worker import AgentExecutionCompletion, AgentExecutionRequest
from forest.errors import ForestError
from forest.final_reply_uow import FinalReplyPlan, FinalReplyUnitOfWork
from forest.session_handoff_uow import (
    SessionHandoffPlan,
    SessionHandoffUnitOfWork,
)


def _operation(kind: str, *parts: object) -> str:
    wire = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"conversation-transaction:{kind}:{hashlib.sha256(wire).hexdigest()}"


@dataclass(frozen=True)
class PreparedFinalReplyCommit:
    agent_owner: object
    plan: FinalReplyPlan


class FinalReplyPlanPort(Protocol):
    async def prepare(
        self,
        request: AgentExecutionRequest,
        completion: AgentExecutionCompletion,
    ) -> PreparedFinalReplyCommit: ...


async def _call(port, *args, **kwargs):
    result = port(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


class TransactionalConversationCompletionPort:
    """Require Agent, Context, and Delivery to commit as one Bot transaction."""

    def __init__(self, *, unit_of_work: FinalReplyUnitOfWork, plans: FinalReplyPlanPort) -> None:
        self.unit_of_work = unit_of_work
        self.plans = plans

    async def commit(self, *, actor, request, completion) -> dict:
        prepared = await _call(self.plans.prepare, request, completion)
        if type(prepared) is not PreparedFinalReplyCommit or type(prepared.plan) is not FinalReplyPlan:
            raise ForestError(
                "FINAL_REPLY_PLAN_INVALID", "Final reply planner returned an invalid plan", 500
            )
        plan = prepared.plan
        if (
            plan.session_ref != request.session_ref
            or plan.run_ref != request.run_ref
            or plan.reply_ref != completion.reply_ref
            or plan.facts != completion.facts
            or plan.answered_trigger_ids != completion.answered_trigger_ids
        ):
            raise ForestError(
                "FINAL_REPLY_PLAN_CONFLICT",
                "Final reply plan differs from the Agent completion",
                409,
            )
        return await self.unit_of_work.commit(
            actor=actor,
            agent_owner=prepared.agent_owner,
            plan=plan,
        )


class TransactionalSessionDecisionPort:
    """Use the Agent/Conversation UoW for successor creation or silent close."""

    def __init__(self, *, unit_of_work: SessionHandoffUnitOfWork, agent_repository) -> None:
        self.unit_of_work = unit_of_work
        self.agent_repository = agent_repository

    async def decide(
        self,
        *,
        actor,
        session_ref,
        completed_run_ref,
        successor_run_ref,
        decision_index,
    ) -> dict:
        if type(decision_index) is not int or decision_index < 1:
            raise ForestError(
                "SESSION_HANDOFF_INPUT_INVALID", "Session decision index is invalid", 422
            )
        run = await self.agent_repository.load(completed_run_ref)
        if run["session_id"] != session_ref or run["state"] != "COMPLETED":
            raise ForestError(
                "SESSION_HANDOFF_INPUT_INVALID",
                "Session decision does not refer to a completed Agent Run",
                409,
            )
        identity = (
            actor.conversation_id,
            session_ref,
            completed_run_ref,
            successor_run_ref,
            decision_index,
        )
        return await self.unit_of_work.decide(
            actor=actor,
            plan=SessionHandoffPlan(
                selection_operation_id=_operation("select", *identity),
                session_id=session_ref,
                completed_run_id=completed_run_ref,
                successor_run_id=successor_run_ref,
                successor_start_operation_id=_operation("successor", *identity),
                session_close_operation_id=_operation("close", *identity),
                successor_deadline=run["absolute_deadline"],
            ),
        )
