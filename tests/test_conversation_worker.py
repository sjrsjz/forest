from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from forest.conversation_worker import (
    AgentExecutionCompletion,
    ConversationWorker,
)
from forest.errors import ForestError
from forest.storage.conversation_scheduler import (
    ConversationActor,
    SchedulerContextFact,
    SchedulerPayloadRef,
)


def _batch(conversation: str, *trigger_ids: str, kind: str = "TRIGGER_INPUT") -> dict:
    return {
        "status": "BATCH",
        "kind": kind,
        "conversation_id": conversation,
        "trigger_ids": list(trigger_ids),
    }


def _reply(run_ref: str, *trigger_ids: str) -> AgentExecutionCompletion:
    reply_ref = f"reply/{run_ref}"
    payload = SchedulerPayloadRef(
        payload_id=f"payload/{run_ref}",
        digest="sha256:" + "a" * 64,
        owner_component_id="agent.runtime",
        owner_kind="final.reply",
        owner_id=reply_ref,
    )
    return AgentExecutionCompletion(
        facts=(
            SchedulerContextFact(
                unit_id=reply_ref,
                kind="REPLY",
                source_component_id="agent.runtime",
                source_kind="final.reply",
                source_id=reply_ref,
                payload=payload,
            ),
        ),
        reply_ref=reply_ref,
        answered_trigger_ids=tuple(trigger_ids),
    )


@dataclass
class Lane:
    initial: dict
    safe: list[dict] = field(default_factory=list)
    ending: list[dict] = field(default_factory=lambda: [{"status": "IDLE"}])
    active: tuple[str, str] | None = None
    claimed: tuple[str, ...] = ()


class FakeScheduler:
    def __init__(self, lanes: dict[str, Lane]):
        self.lanes = lanes
        self.actors: set[str] = set()
        self.appends: list[dict] = []
        self.selections: list[tuple[str, str, str | None, str | None]] = []

    async def claimable_conversations(self, *, limit):
        return tuple(self.lanes)[:limit]

    async def acquire_actor(self, conversation_id, owner_id, *, lease_seconds):
        if conversation_id in self.actors:
            raise ForestError("CONVERSATION_ACTOR_BUSY", "busy", 409)
        self.actors.add(conversation_id)
        return ConversationActor(
            conversation_id,
            owner_id,
            1,
            datetime.now(timezone.utc),
        )

    async def renew_actor(self, actor, *, lease_seconds):
        assert actor.conversation_id in self.actors
        return actor

    async def execution_state(self, actor):
        lane = self.lanes[actor.conversation_id]
        if lane.active is None:
            return {"active": False, "session_ref": None, "run_ref": None}
        return {
            "active": True,
            "session_ref": lane.active[0],
            "run_ref": lane.active[1],
            "claimed_triggers": [
                {"trigger_id": item} for item in lane.claimed
            ],
        }

    async def release_actor(self, actor):
        self.actors.remove(actor.conversation_id)

    async def select_input(
        self,
        actor,
        _operation_id,
        *,
        boundary,
        session_ref=None,
        run_ref=None,
        next_run_ref=None,
    ):
        self.selections.append((actor.conversation_id, boundary, run_ref, next_run_ref))
        lane = self.lanes[actor.conversation_id]
        if boundary == "IDLE":
            return lane.initial
        if boundary == "MODEL_SAFE_POINT":
            return lane.safe.pop(0) if lane.safe else {"status": "CONTINUE"}
        result = lane.ending.pop(0)
        if result.get("kind") == "TRIGGER_INPUT":
            result = dict(result)
            result["selected_run_ref"] = next_run_ref
        return result

    async def append_context(self, actor, **values):
        self.appends.append({"conversation_id": actor.conversation_id, **values})
        return values

    async def requeue_run(self, *_args, **_values):
        return ()


class SafePointAgent:
    def __init__(self):
        self.requests = []

    async def execute(self, request):
        self.requests.append(request)
        trigger_ids = [] if request.initial_batch is None else list(
            request.initial_batch["trigger_ids"]
        )
        selected = await request.next_model_input()
        if selected is not None:
            trigger_ids.extend(selected["trigger_ids"])
        return _reply(request.run_ref, *trigger_ids)


@pytest.mark.asyncio
async def test_next_model_call_absorbs_through_last_trigger_in_same_run():
    scheduler = FakeScheduler(
        {
            "group/a": Lane(
                initial=_batch("group/a", "trigger/1"),
                safe=[_batch("group/a", "trigger/2")],
                ending=[{"status": "BATCH", "kind": "SILENT_DRAIN", "trigger_ids": []}],
            )
        }
    )
    agent = SafePointAgent()
    worker = ConversationWorker(scheduler, agent, owner_id="generation/one")
    await worker.run_once("group/a")

    assert len(agent.requests) == 1
    assert not agent.requests[0].recovery
    assert scheduler.appends[0]["answer_trigger_ids"] == ("trigger/1", "trigger/2")
    assert [item[1] for item in scheduler.selections] == [
        "IDLE",
        "MODEL_SAFE_POINT",
        "SESSION_END",
    ]


@pytest.mark.asyncio
async def test_trigger_after_final_candidate_uses_successor_run_in_same_session():
    scheduler = FakeScheduler(
        {
            "group/a": Lane(
                initial=_batch("group/a", "trigger/1"),
                ending=[
                    _batch("group/a", "trigger/2"),
                    {"status": "IDLE"},
                ],
            )
        }
    )
    agent = SafePointAgent()
    worker = ConversationWorker(scheduler, agent, owner_id="generation/one")
    await worker.run_once("group/a")

    assert len(agent.requests) == 2
    assert agent.requests[0].session_ref == agent.requests[1].session_ref
    assert agent.requests[0].run_ref != agent.requests[1].run_ref
    end_calls = [item for item in scheduler.selections if item[1] == "SESSION_END"]
    assert end_calls[0][3] == agent.requests[1].run_ref
    assert scheduler.appends[1]["answer_trigger_ids"] == ("trigger/2",)


class ParallelAgent:
    def __init__(self, expected: int):
        self.expected = expected
        self.entered: set[str] = set()
        self.all_entered = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, request):
        self.entered.add(request.conversation_id)
        if len(self.entered) == self.expected:
            self.all_entered.set()
        await self.release.wait()
        return _reply(request.run_ref, *request.initial_batch["trigger_ids"])


@pytest.mark.asyncio
async def test_same_group_is_serial_and_different_groups_run_in_parallel():
    scheduler = FakeScheduler(
        {
            "group/a": Lane(initial=_batch("group/a", "trigger/a")),
            "group/b": Lane(initial=_batch("group/b", "trigger/b")),
        }
    )
    agent = ParallelAgent(expected=2)
    worker = ConversationWorker(scheduler, agent, owner_id="generation/one")
    first_a = asyncio.create_task(worker.run_once("group/a"))
    second_a = asyncio.create_task(worker.run_once("group/a"))
    group_b = asyncio.create_task(worker.run_once("group/b"))
    await asyncio.wait_for(agent.all_entered.wait(), timeout=1)
    with pytest.raises(ForestError) as caught:
        await second_a
    assert caught.value.code == "CONVERSATION_ACTOR_BUSY"
    assert agent.entered == {"group/a", "group/b"}
    agent.release.set()
    await asyncio.gather(first_a, group_b)


@pytest.mark.asyncio
async def test_active_lane_resumes_same_session_and_run_without_idle_selection():
    scheduler = FakeScheduler(
        {
            "group/a": Lane(
                initial=_batch("group/a", "must/not/be/selected"),
                active=("session/existing", "run/existing"),
                claimed=("trigger/existing",),
            )
        }
    )

    class RecoveryAgent:
        def __init__(self):
            self.request = None

        async def execute(self, request):
            self.request = request
            return _reply(request.run_ref, "trigger/existing")

    agent = RecoveryAgent()
    worker = ConversationWorker(scheduler, agent, owner_id="generation/two")
    await worker.run_once("group/a")

    assert agent.request.recovery
    assert agent.request.initial_batch is None
    assert agent.request.session_ref == "session/existing"
    assert agent.request.run_ref == "run/existing"
    assert all(boundary != "IDLE" for _, boundary, _, _ in scheduler.selections)
    assert scheduler.appends[0]["answer_trigger_ids"] == ("trigger/existing",)
