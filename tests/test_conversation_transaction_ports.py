from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from forest.conversation_transaction_ports import (
    PreparedFinalReplyCommit,
    TransactionalConversationCompletionPort,
    TransactionalSessionDecisionPort,
)
from forest.conversation_worker import AgentExecutionCompletion, AgentExecutionRequest
from forest.errors import ForestError
from forest.final_reply_uow import FinalReplyPlan


def _request():
    async def context_snapshot():
        return {"conversation_id": "group/one"}

    async def next_input():
        return None

    return AgentExecutionRequest(
        conversation_id="group/one",
        session_ref="session/one",
        run_ref="run/one",
        initial_batch=None,
        recovery=True,
        context_snapshot=context_snapshot,
        next_model_input=next_input,
    )


def _completion():
    return AgentExecutionCompletion(
        facts=("reply-fact",),
        reply_ref="reply/one",
        answered_trigger_ids=("trigger/one",),
    )


def _plan():
    return FinalReplyPlan(
        session_ref="session/one",
        run_ref="run/one",
        completion_checkpoint_id="checkpoint/complete",
        completion_state="state/complete",
        context_operation_id="context/reply/one",
        facts=("reply-fact",),
        answered_trigger_ids=("trigger/one",),
        reply_ref="reply/one",
        delivery_operation_id="delivery/reply/one",
        intent_id="intent/reply/one",
        binding_ref="binding/main",
        ordering_key="group/one",
        channel_kind="GROUP",
        channel_ref="933179609",
        delivery_generation_id="generation/delivery-one",
        parts=("part/one",),
    )


@pytest.mark.asyncio
async def test_completion_port_passes_only_an_exact_agent_context_delivery_plan():
    calls = []

    class Plans:
        released = []

        async def prepare(self, actor, request, completion):
            assert actor.conversation_id == "group/one"
            assert request.conversation_id == "group/one"
            assert request.session_ref == "session/one"
            assert request.run_ref == "run/one"
            assert completion == _completion()
            return PreparedFinalReplyCommit("owner/one", _plan())

        async def release(self, prepared):
            self.released.append(prepared)

    class UnitOfWork:
        async def commit(self, **values):
            calls.append(values)
            return {"committed": True}

    actor = SimpleNamespace(conversation_id="group/one")
    plans = Plans()
    port = TransactionalConversationCompletionPort(unit_of_work=UnitOfWork(), plans=plans)
    assert await port.commit(
        actor=actor,
        request=_request(),
        completion=_completion(),
    ) == {"committed": True}
    assert calls == [{"actor": actor, "agent_owner": "owner/one", "plan": _plan()}]
    assert plans.released == [PreparedFinalReplyCommit("owner/one", _plan())]

    class WrongPlans:
        released = []

        async def prepare(self, _actor, _request_value, _completion_value):
            plan = _plan()
            return PreparedFinalReplyCommit(
                "owner/one",
                FinalReplyPlan(**{**plan.__dict__, "reply_ref": "reply/other"}),
            )

        async def release(self, prepared):
            self.released.append(prepared)

    port = TransactionalConversationCompletionPort(
        unit_of_work=UnitOfWork(), plans=WrongPlans()
    )
    with pytest.raises(ForestError) as caught:
        await port.commit(actor=actor, request=_request(), completion=_completion())
    assert caught.value.code == "FINAL_REPLY_PLAN_CONFLICT"


@pytest.mark.asyncio
async def test_session_port_derives_stable_uow_plan_from_completed_run():
    deadline = datetime.now(timezone.utc) + timedelta(hours=1)
    plans = []

    class AgentRepository:
        async def load(self, run_id):
            assert run_id == "run/one"
            return {
                "session_id": "session/one",
                "state": "COMPLETED",
                "absolute_deadline": deadline,
            }

    class UnitOfWork:
        async def decide(self, *, actor, plan):
            plans.append((actor, plan))
            return {"action": "CLOSED", "selection": {"status": "IDLE"}}

    actor = SimpleNamespace(conversation_id="group/one")
    port = TransactionalSessionDecisionPort(
        unit_of_work=UnitOfWork(),
        agent_repository=AgentRepository(),
    )
    first = await port.decide(
        actor=actor,
        session_ref="session/one",
        completed_run_ref="run/one",
        successor_run_ref="run/two",
        decision_index=1,
    )
    second = await port.decide(
        actor=actor,
        session_ref="session/one",
        completed_run_ref="run/one",
        successor_run_ref="run/two",
        decision_index=1,
    )
    assert first == second
    assert plans[0][1] == plans[1][1]
    assert plans[0][1].successor_deadline == deadline
