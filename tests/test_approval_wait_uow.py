from __future__ import annotations

import pytest

from forest.approval_wait_uow import ApprovalWaitUnitOfWork
from forest.storage.conversation_scheduler import ConversationActor


class _Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_exc):
        return False


class _Connection:
    def transaction(self):
        return _Context(self)


class _Pool:
    def __init__(self, connection):
        self._connection = connection

    def connection(self):
        return _Context(self._connection)


class _Store:
    def __init__(self, connection):
        self.pool = _Pool(connection)


class _Authorization:
    def __init__(self, events, state):
        self.events = events
        self.state = state

    async def approval_in(self, connection, approval_ref, *, update):
        self.events.append(("approval", connection, approval_ref, update))
        return {
            "state": self.state,
            "request_kind": "agent.tool",
            "requester_kind": "component",
            "requester_id": "agent.runtime",
        }


class _Agent:
    def __init__(self, events):
        self.events = events

    async def approval_context_in(self, connection, approval_ref):
        self.events.append(("agent", connection, approval_ref))
        return {"conversation_ref": "group/one"}


class _Conversation:
    def __init__(self, events):
        self.events = events

    async def defer_actor_in(self, connection, actor, **values):
        self.events.append(("conversation", connection, actor, values))
        return {"resume_not_before": "deadline"}


def _uow(state):
    events = []
    connection = _Connection()
    return (
        ApprovalWaitUnitOfWork(
            _Store(connection),
            authorization=_Authorization(events, state),
            agent=_Agent(events),
            conversation=_Conversation(events),
        ),
        connection,
        events,
    )


@pytest.mark.asyncio
async def test_pending_approval_is_locked_before_exact_conversation_deferral():
    uow, connection, events = _uow("PENDING")
    actor = ConversationActor("group/one", "worker/one", 1, None)

    result = await uow.defer(
        actor,
        approval_ref="approval/tool/one",
        delay_seconds=1.0,
    )

    assert result == {
        "deferred": True,
        "approval_state": "PENDING",
        "conversation_id": "group/one",
        "resume_not_before": "deadline",
    }
    assert [event[0] for event in events] == ["approval", "agent", "conversation"]
    assert all(event[1] is connection for event in events)
    assert events[0][3] is True
    assert events[2][3] == {
        "delay_seconds": 1.0,
        "reason_code": "AGENT_WAITING_APPROVAL",
        "reason_ref": "approval/tool/one",
    }


@pytest.mark.asyncio
async def test_already_decided_approval_does_not_install_a_late_blocker():
    uow, connection, events = _uow("APPROVED")
    actor = ConversationActor("group/one", "worker/one", 1, None)

    result = await uow.defer(
        actor,
        approval_ref="approval/tool/one",
        delay_seconds=1.0,
    )

    assert result == {
        "deferred": False,
        "approval_state": "APPROVED",
        "conversation_id": "group/one",
    }
    assert [event[0] for event in events] == ["approval", "agent"]
    assert all(event[1] is connection for event in events)
