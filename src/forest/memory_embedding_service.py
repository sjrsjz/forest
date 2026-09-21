"""Lifecycle-owned durable embedding worker for ``memory.graph``."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.memory_embedding_worker import EmbeddingBudgetClaim, MemoryEmbeddingWorker


def _identity(kind: str, *parts: object) -> str:
    wire = "\0".join(str(part) for part in parts).encode()
    return f"memory-embedding:{kind}:{hashlib.sha256(wire).hexdigest()}"


class CleanMemoryEmbeddingService:
    def __init__(
        self,
        *,
        retrieval,
        payloads,
        provider,
        vector_index,
        profile,
        component_id: str,
        generation_id: str,
        route_revision: str,
        budget,
        request_limit: int,
        token_limit: int,
        reserved_tokens: int,
        policy_revision: str,
        poll_interval: float = 1.0,
    ) -> None:
        self.retrieval = retrieval
        self.budget = budget
        self.request_limit = request_limit
        self.token_limit = token_limit
        self.reserved_tokens = reserved_tokens
        self.policy_revision = policy_revision
        self.poll_interval = poll_interval
        self.owner_id = ("memory-embedding/" + generation_id)[-256:]
        self.worker = MemoryEmbeddingWorker(
            retrieval=retrieval,
            payloads=payloads,
            provider=provider,
            vector_index=vector_index,
            profile=profile,
            component_id=component_id,
            generation_id=generation_id,
            route_revision=route_revision,
            budget=budget,
        )
        self._admission = asyncio.Event()
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._closing = False
        self._task: asyncio.Task | None = None
        self._sequence = 0
        self._retry_delay = poll_interval
        self.failure: str | None = None

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Memory embedding worker already started")
        self._task = asyncio.create_task(self._run(), name=self.owner_id)

    def activate(self) -> None:
        self._admission.set()
        self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    async def quiesce(self) -> None:
        self._admission.clear()
        self._wake.set()
        await self._idle.wait()

    async def close(self) -> None:
        self._closing = True
        self._admission.set()
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return (self._task,) if self._task is not None else ()

    async def _wait(self, timeout: float | None = None) -> None:
        try:
            await asyncio.wait_for(
                self._wake.wait(), timeout=self.poll_interval if timeout is None else timeout
            )
        except TimeoutError:
            pass
        self._wake.clear()

    async def _run(self) -> None:
        while not self._closing:
            await self._admission.wait()
            if self._closing:
                return
            self._idle.clear()
            outcome = "IDLE"
            try:
                try:
                    await self.retrieval.recover_expired_attempts()
                    await self.budget.recover_expired()
                    purpose = None
                    if await self.worker.delete_one_index_ref():
                        outcome = "SUCCESS"
                    elif await self.worker.publish_one_index_ref("QUERY"):
                        outcome = "SUCCESS"
                    elif await self.worker.publish_one_index_ref("DOCUMENT"):
                        outcome = "SUCCESS"
                    elif await self.retrieval.has_pending_facets(purpose="QUERY"):
                        purpose = "QUERY"
                    elif await self.retrieval.has_pending_facets(purpose="DOCUMENT"):
                        purpose = "DOCUMENT"
                    if purpose is not None:
                        outcome = await self._execute_one(purpose)
                except asyncio.CancelledError:
                    raise
                except ForestError as exc:
                    self.failure = exc.code
                    outcome = "BACKOFF"
                except Exception:
                    self.failure = "MEMORY_EMBEDDING_WORKER_FAILED"
                    outcome = "BACKOFF"
            finally:
                self._idle.set()
            if outcome == "BACKOFF":
                await self._wait(self._retry_delay)
                self._retry_delay = min(self._retry_delay * 2, 300)
            elif outcome == "BUDGET":
                await self._wait(60)
            elif outcome == "SUCCESS":
                self.failure = None
                self._retry_delay = self.poll_interval
            if outcome == "IDLE" or not self._admission.is_set():
                await self._wait()

    async def _execute_one(self, purpose: str) -> str:
        self._sequence += 1
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(minutes=2)
        claim_id = _identity("claim", self.owner_id, self._sequence)
        claim = await self.retrieval.claim_pending_batch(
            operation_id=_identity("claim-operation", self.owner_id, self._sequence),
            claim_id=claim_id,
            purpose=purpose,
            executor_component_id="memory.graph",
            executor_generation_id=self.worker.generation_id,
            route_revision=self.worker.route_revision,
            deadline_at=deadline,
        )
        if not claim.permits:
            return "IDLE"
        budget = EmbeddingBudgetClaim(
            reservation_id=_identity("budget", claim_id),
            operation_id=_identity("budget-operation", claim_id),
            policy_revision=self.policy_revision,
            budget_day=now.date(),
            request_limit=self.request_limit,
            token_limit=self.token_limit,
            reserved_tokens=self.reserved_tokens,
        )
        try:
            results = await self.worker.execute_claim(claim, budget_claim=budget)
        except ForestError as exc:
            if exc.code in {
                "MEMORY_EMBEDDING_REQUEST_BUDGET_EXHAUSTED",
                "MEMORY_EMBEDDING_TOKEN_BUDGET_EXHAUSTED",
            }:
                return "BUDGET"
            raise
        if any(result["state"] != "SUCCEEDED" for result in results):
            return "BACKOFF"
        return "SUCCESS"
