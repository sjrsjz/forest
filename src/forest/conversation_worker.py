"""Per-conversation worker for the single Agent execution chain.

The worker owns scheduling only.  The injected Agent port owns one QiYu-style
Session/Run state machine and may ask for more input immediately before its
next model call.  That callback never creates another Agent.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from forest.errors import ForestError
from forest.storage.conversation_scheduler import ConversationActor, SchedulerContextFact


class _WorkerQuiescing(Exception):
    pass


class AgentReloadRequested(Exception):
    def __init__(self, operation_id: str) -> None:
        super().__init__(operation_id)
        self.operation_id = operation_id


def _ref(kind: str) -> str:
    return f"{kind}:{secrets.token_hex(16)}"


def _operation(kind: str, *parts: object) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"conversation-worker:{kind}:{hashlib.sha256(encoded).hexdigest()}"


@dataclass(frozen=True)
class AgentExecutionRequest:
    conversation_id: str
    session_ref: str
    run_ref: str
    initial_batch: dict | None
    recovery: bool
    context_snapshot: Callable[[], Awaitable[dict]]
    next_model_input: Callable[[], Awaitable[dict | None]]
    before_external_step: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class AgentExecutionCompletion:
    facts: tuple[SchedulerContextFact, ...]
    reply_ref: str
    answered_trigger_ids: tuple[str, ...]
    already_committed: bool = False


class AgentExecutionPort(Protocol):
    async def execute(self, request: AgentExecutionRequest) -> AgentExecutionCompletion: ...


class ConversationCompletionPort(Protocol):
    async def commit(
        self,
        *,
        actor: ConversationActor,
        request: AgentExecutionRequest,
        completion: AgentExecutionCompletion,
    ) -> dict: ...


class SessionDecisionPort(Protocol):
    async def decide(
        self,
        *,
        actor: ConversationActor,
        session_ref: str,
        completed_run_ref: str,
        successor_run_ref: str,
        decision_index: int,
    ) -> dict: ...


class ApprovalWaitPort(Protocol):
    async def defer(
        self,
        actor: ConversationActor,
        *,
        approval_ref: str,
        delay_seconds: float,
    ) -> dict: ...


class ConversationWorker:
    """Poll durable lanes, serialize each group, and run groups in parallel."""

    def __init__(
        self,
        scheduler,
        agent: AgentExecutionPort,
        completion: ConversationCompletionPort,
        session_decision: SessionDecisionPort,
        *,
        approval_wait: ApprovalWaitPort,
        owner_id: str,
        poll_interval: float = 0.1,
        approval_poll_interval: float = 1.0,
        actor_lease_seconds: int = 30,
        max_parallel_conversations: int = 16,
        candidate_limit: int = 64,
        graph_admission: asyncio.Event | None = None,
    ) -> None:
        if not owner_id or len(owner_id.encode("utf-8")) > 256:
            raise ValueError("owner_id must be a bounded non-empty string")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if not 0.1 <= approval_poll_interval <= 300:
            raise ValueError("approval_poll_interval must be between 0.1 and 300 seconds")
        if not 1 <= actor_lease_seconds <= 300:
            raise ValueError("actor_lease_seconds is outside the repository lease range")
        if not 1 <= max_parallel_conversations <= 1024:
            raise ValueError("max_parallel_conversations is invalid")
        if not 1 <= candidate_limit <= 1024:
            raise ValueError("candidate_limit is invalid")
        self.scheduler = scheduler
        self.agent = agent
        self.completion = completion
        self.session_decision = session_decision
        self.approval_wait = approval_wait
        self.owner_id = owner_id
        self.poll_interval = poll_interval
        self.approval_poll_interval = approval_poll_interval
        self.actor_lease_seconds = actor_lease_seconds
        self.candidate_limit = candidate_limit
        self.graph_admission = graph_admission
        self._parallel = asyncio.Semaphore(max_parallel_conversations)
        self._admission = asyncio.Event()
        self._poll_wake = asyncio.Event()
        self._input_wake = asyncio.Event()
        self._replacement_operation_id: str | None = None
        self._closing = False
        self._poll_task: asyncio.Task | None = None
        self._conversation_tasks: dict[str, asyncio.Task] = {}
        self.failures: dict[str, BaseException] = {}

    def start(self) -> None:
        if self._poll_task is not None:
            raise RuntimeError("conversation worker already started")
        self._poll_task = asyncio.create_task(self._poll(), name=f"{self.owner_id}:poll")

    def activate(self) -> None:
        self._admission.set()
        self._poll_wake.set()

    def wake(self) -> None:
        self._poll_wake.set()
        self._input_wake.set()

    def begin_replacement(self, operation_id: str) -> None:
        if not operation_id or len(operation_id.encode("utf-8")) > 256:
            raise ValueError("replacement operation_id must be bounded and non-empty")
        if (
            self._replacement_operation_id is not None
            and self._replacement_operation_id != operation_id
        ):
            raise RuntimeError("conversation worker already belongs to another replacement")
        self._replacement_operation_id = operation_id

    async def quiesce(self) -> None:
        self._admission.clear()
        self._poll_wake.set()
        self._input_wake.set()
        active = tuple(self._conversation_tasks.values())
        if active:
            await asyncio.gather(*active, return_exceptions=True)

    async def close(self) -> None:
        self._closing = True
        self._admission.set()
        self._poll_wake.set()
        self._input_wake.set()
        tasks = tuple(self._conversation_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._poll_task is not None:
            self._poll_task.cancel()
            await asyncio.gather(self._poll_task, return_exceptions=True)

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        tasks = tuple(self._conversation_tasks.values())
        if self._poll_task is not None:
            return (self._poll_task, *tasks)
        return tasks

    async def _wait_for_wake(self) -> None:
        try:
            await asyncio.wait_for(self._poll_wake.wait(), timeout=self.poll_interval)
        except TimeoutError:
            pass
        self._poll_wake.clear()

    async def _poll(self) -> None:
        while not self._closing:
            await self._admission.wait()
            if self.graph_admission is not None:
                await self.graph_admission.wait()
            if self._closing:
                return
            try:
                candidates = await self.scheduler.claimable_conversations(
                    limit=self.candidate_limit
                )
            except ForestError as exc:
                if exc.code != "RUNTIME_GENERATION_NOT_ACTIVE":
                    raise
                # Runtime-wide DATA maintenance closes the persistent route
                # without retiring this process-local Generation. Keep the
                # poller alive so it can resume from durable work when DATA
                # admission reopens; graph retirement clears graph_admission
                # and fences the same retry path.
                await self._wait_for_wake()
                continue
            for conversation_id in candidates:
                if not self._admission.is_set() or self._closing:
                    break
                if conversation_id in self._conversation_tasks:
                    continue
                task = asyncio.create_task(
                    self._run_candidate(conversation_id),
                    name=f"{self.owner_id}:{conversation_id}",
                )
                self._conversation_tasks[conversation_id] = task
                task.add_done_callback(
                    lambda _task, key=conversation_id: self._conversation_tasks.pop(key, None)
                )
            await self._wait_for_wake()

    async def _run_candidate(self, conversation_id: str) -> None:
        async with self._parallel:
            try:
                await self.run_once(conversation_id)
                self.failures.pop(conversation_id, None)
            except asyncio.CancelledError:
                raise
            except _WorkerQuiescing:
                self.failures.pop(conversation_id, None)
            except ForestError as exc:
                if exc.code != "CONVERSATION_ACTOR_BUSY":
                    self.failures[conversation_id] = exc
            except BaseException as exc:  # kept observable; the poller must remain alive
                self.failures[conversation_id] = exc

    async def _heartbeat(self, actor: ConversationActor, stopped: asyncio.Event) -> None:
        interval = max(0.25, self.actor_lease_seconds / 3)
        while not stopped.is_set():
            try:
                await asyncio.wait_for(stopped.wait(), timeout=interval)
            except TimeoutError:
                await self.scheduler.renew_actor(
                    actor,
                    lease_seconds=self.actor_lease_seconds,
                )

    async def _wait_input(self) -> None:
        try:
            await asyncio.wait_for(self._input_wake.wait(), timeout=self.poll_interval)
        except TimeoutError:
            pass
        self._input_wake.clear()
        if not self._admission.is_set() or self._closing:
            if self._replacement_operation_id is not None and not self._closing:
                raise AgentReloadRequested(self._replacement_operation_id)
            raise _WorkerQuiescing

    async def run_once(self, conversation_id: str) -> None:
        """Process one lane until it is idle or waiting for external classification."""

        actor = await self.scheduler.acquire_actor(
            conversation_id,
            self.owner_id,
            lease_seconds=self.actor_lease_seconds,
        )
        stopped = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat(actor, stopped),
            name=f"{self.owner_id}:{conversation_id}:lease",
        )
        try:
            execution = await self.scheduler.execution_state(actor)
            recovery = bool(execution["active"])
            if recovery:
                session_ref = execution["session_ref"]
                run_ref = execution["run_ref"]
                initial = execution.get("active_input")
            else:
                session_ref = _ref("session")
                run_ref = _ref("run")
                initial = await self.scheduler.select_input(
                    actor,
                    _operation("idle", conversation_id, actor.epoch, session_ref, run_ref),
                    boundary="IDLE",
                    session_ref=session_ref,
                    run_ref=run_ref,
                )
                if initial.get("kind") != "TRIGGER_INPUT":
                    return
            batch = initial
            while True:
                safe_point_index = 0

                async def next_model_input() -> dict | None:
                    nonlocal safe_point_index
                    while True:
                        safe_point_index += 1
                        selection = await self.scheduler.select_input(
                            actor,
                            _operation(
                                "safe",
                                conversation_id,
                                actor.epoch,
                                session_ref,
                                run_ref,
                                safe_point_index,
                            ),
                            boundary="MODEL_SAFE_POINT",
                            session_ref=session_ref,
                            run_ref=run_ref,
                        )
                        if selection["status"] == "WAIT_INPUT":
                            await self._wait_input()
                            continue
                        if selection.get("kind") == "TRIGGER_INPUT":
                            return selection
                        return None

                async def before_external_step() -> None:
                    if not self._admission.is_set() or self._closing:
                        if self._replacement_operation_id is not None and not self._closing:
                            raise AgentReloadRequested(self._replacement_operation_id)
                        raise _WorkerQuiescing

                request = AgentExecutionRequest(
                    conversation_id=conversation_id,
                    session_ref=session_ref,
                    run_ref=run_ref,
                    initial_batch=batch,
                    recovery=recovery,
                    context_snapshot=lambda: self.scheduler.context_snapshot(actor),
                    next_model_input=next_model_input,
                    before_external_step=before_external_step,
                )
                try:
                    completion = await self.agent.execute(request)
                except ForestError as exc:
                    if exc.code == "AGENT_RUN_CANCELLED":
                        reason_code = (
                            exc.details.get("reason_code")
                            if isinstance(exc.details, dict)
                            and isinstance(exc.details.get("reason_code"), str)
                            else "AGENT_RUN_CANCELLED"
                        )
                        await self.scheduler.cancel_run(
                            actor,
                            operation_id=_operation(
                                "cancelled",
                                conversation_id,
                                actor.epoch,
                                session_ref,
                                run_ref,
                            ),
                            session_ref=session_ref,
                            run_ref=run_ref,
                            reason_code=reason_code,
                        )
                        return
                    if exc.code in {
                        "AGENT_PROTOCOL_FAILED",
                        "AGENT_MODEL_TRANSPORT_FAILED",
                    }:
                        await self.scheduler.fail_run(
                            actor,
                            operation_id=_operation(
                                "failed",
                                conversation_id,
                                actor.epoch,
                                session_ref,
                                run_ref,
                                exc.code,
                            ),
                            session_ref=session_ref,
                            run_ref=run_ref,
                            reason_code=exc.code,
                        )
                        return
                    if exc.code == "AGENT_RELOAD_WAIT":
                        operation_id = (
                            exc.details.get("operation_id")
                            if isinstance(exc.details, dict)
                            else None
                        )
                        if not isinstance(operation_id, str):
                            raise ForestError(
                                "AGENT_RELOAD_OPERATION_MISSING",
                                "Agent reload wait omitted its lifecycle operation",
                                500,
                            ) from exc
                        await self.scheduler.defer_actor(
                            actor,
                            delay_seconds=300,
                            reason_code="AGENT_RELOAD_WAIT",
                            reason_ref=operation_id,
                        )
                        return
                    if exc.code != "AGENT_WAITING_APPROVAL":
                        raise
                    approval_ref = (
                        exc.details.get("approval_ref")
                        if isinstance(exc.details, dict)
                        else None
                    )
                    if not isinstance(approval_ref, str):
                        raise ForestError(
                            "AGENT_APPROVAL_REFERENCE_MISSING",
                            "Agent approval wait omitted its durable reference",
                            500,
                        ) from exc
                    wait = await self.approval_wait.defer(
                        actor,
                        approval_ref=approval_ref,
                        delay_seconds=self.approval_poll_interval,
                    )
                    if wait["deferred"]:
                        return
                    # The decision committed before this worker could install
                    # its blocker. Resume the same durable run while the actor
                    # is still owned instead of waiting for another poll.
                    recovery = True
                    batch = None
                    continue
                if type(completion) is not AgentExecutionCompletion:
                    raise ForestError(
                        "AGENT_EXECUTION_RESULT_INVALID",
                        "Agent execution returned an invalid completion",
                        500,
                    )
                answered = tuple(dict.fromkeys(completion.answered_trigger_ids))
                if not answered or len(answered) != len(completion.answered_trigger_ids):
                    raise ForestError(
                        "AGENT_EXECUTION_RESULT_INVALID",
                        "Agent completion must identify each answered trigger once",
                        500,
                    )
                if not completion.already_committed:
                    await self.completion.commit(
                        actor=actor,
                        request=request,
                        completion=completion,
                    )

                successor_run_ref = _ref("run")
                end_index = 0
                while True:
                    end_index += 1
                    end = await self.session_decision.decide(
                        actor=actor,
                        session_ref=session_ref,
                        completed_run_ref=run_ref,
                        successor_run_ref=successor_run_ref,
                        decision_index=end_index,
                    )
                    if end["action"] == "WAIT_INPUT":
                        await self._wait_input()
                        continue
                    if end["action"] == "SUCCESSOR":
                        run_ref = successor_run_ref
                        batch = end["selection"]
                        recovery = False
                        break
                    if end["action"] == "CLOSED":
                        return
                    raise ForestError(
                        "SESSION_HANDOFF_RESULT_INVALID",
                        "Session decision returned an invalid action",
                        500,
                    )
        finally:
            stopped.set()
            await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                await self.scheduler.release_actor(actor)
            except ForestError as exc:
                if exc.code not in {"STALE_CONVERSATION_ACTOR", "RUNTIME_GENERATION_NOT_ACTIVE"}:
                    raise
