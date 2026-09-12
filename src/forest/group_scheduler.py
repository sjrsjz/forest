"""Fair, recoverable P1 conversation execution over the PostgreSQL mailbox.

The callback is a trusted host integration, currently a command service rather
than an LLM Agent. Its input and context are fixed before invocation. A future
Agent runner owns model/tool checkpoints and calls MessageStore at safe points.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from uuid import uuid4

from forest.errors import ForestError
from forest.storage.bot import HostLease
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import ActorLease, MessageStore

Execute = Callable[[ActorLease, dict, dict], Awaitable[list[list[dict]]]]


class GroupScheduler:
    def __init__(
        self,
        messages: MessageStore,
        *,
        lease: HostLease | Callable[[], HostLease],
        execute: Execute,
        delivery: DeliveryStore | None = None,
        max_concurrency: int = 8,
        global_slots: asyncio.Semaphore | None = None,
        poll_interval: float = 0.2,
        actor_ttl: float = 30,
        execution_timeout: float = 120,
        cancellation_timeout: float = 5,
        scan_page_size: int = 128,
        max_queued: int = 1024,
    ):
        if not (
            type(max_concurrency) is int and 1 <= max_concurrency <= 64
            and 0.01 <= poll_interval <= 60
            and 0.15 <= actor_ttl <= 120
            and 0 < execution_timeout <= 3600
            and 0.01 <= cancellation_timeout <= 30
            and type(scan_page_size) is int and 1 <= scan_page_size <= 1024
            and type(max_queued) is int and max_concurrency <= max_queued <= 4096
        ):
            raise ValueError("Invalid group scheduler resource bounds")
        self.messages = messages
        self._lease_source = lease
        self.execute = execute
        self.delivery = delivery or DeliveryStore(messages.store)
        self.max_concurrency = max_concurrency
        self.global_slots = global_slots or asyncio.Semaphore(max_concurrency)
        self.poll_interval, self.actor_ttl = poll_interval, actor_ttl
        self.execution_timeout, self.cancellation_timeout = execution_timeout, cancellation_timeout
        self.scan_page_size, self.max_queued = scan_page_size, max_queued
        self.owner_id = "group-scheduler:" + str(uuid4())
        self._queue: deque[str] = deque()
        self._queued: set[str] = set()
        self._active: dict[str, asyncio.Task] = {}
        self._orphaned: set[asyncio.Task] = set()
        self._backoff: dict[str, float] = {}
        self.errors: dict[str, str] = {}
        self._cursor: str | None = None
        self._wake = asyncio.Event()
        self._runner: asyncio.Task | None = None
        self._close_task: asyncio.Task | None = None
        self._closed = False
        self._tick_lock = asyncio.Lock()

    @property
    def active_conversations(self) -> tuple[str, ...]:
        return tuple(self._active)

    @property
    def task(self) -> asyncio.Task | None:
        return self._runner

    @property
    def failure(self) -> ForestError | None:
        if self._orphaned:
            return ForestError("SCHEDULER_CLEANUP_FAILED", "Execution callback remains owned", 503)
        if not self._closed and self._runner is not None and self._runner.done():
            return ForestError("SCHEDULER_STOPPED", "Conversation scheduling loop stopped", 503)
        return None

    def _lease(self) -> HostLease:
        value = self._lease_source() if callable(self._lease_source) else self._lease_source
        if value is None:
            raise ForestError("STALE_HOST", "Bot has no execution owner", 503)
        return value

    @staticmethod
    def _code(error: BaseException) -> str:
        return error.code if isinstance(error, ForestError) else "GROUP_EXECUTION_FAILED"

    def _error(self, conversation_id: str, error: BaseException) -> None:
        # Only stable codes are exposed. Callback exception text can contain
        # private messages, provider payloads, credentials or platform URLs.
        if conversation_id not in self.errors and len(self.errors) >= self.max_queued:
            self.errors.pop(next(iter(self.errors)))
        self.errors[conversation_id] = self._code(error)
        self._backoff[conversation_id] = asyncio.get_running_loop().time() + max(
            self.poll_interval, 0.25
        )

    async def start(self) -> None:
        if self._closed:
            raise ForestError("SCHEDULER_CLOSED", "Conversation scheduler is closed", 503)
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self._loop(), name=self.owner_id)

    def notify(self) -> None:
        """Optional wakeup hint. PostgreSQL scanning remains the source of work."""
        self._wake.set()

    async def _loop(self) -> None:
        try:
            while not self._closed:
                self._wake.clear()
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._error("scheduler", exc)
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def run_once(self) -> int:
        """Scan one bounded durable page and schedule a fair work quantum."""
        async with self._tick_lock:
            if self._closed:
                return 0
            lease = self._lease()
            await self.messages.reconcile_echoes(lease, limit=min(self.scan_page_size, 256))
            runtime = await self.messages.store.runtime()
            if not runtime["data_admission"] or runtime["status"] != "ACTIVE":
                # Stop local continuation promptly. The target transaction fence
                # independently rejects any callback result arriving afterwards.
                for task in tuple(self._active.values()):
                    if not task.cancelling():
                        task.cancel()
                return 0
            await self.messages.expire_classifications(lease, limit=self.scan_page_size)
            rows = await self.messages.pending(
                lease, limit=self.scan_page_size, after_conversation_id=self._cursor,
            )
            self._cursor = rows[-1]["conversation_id"] if rows else None
            now = asyncio.get_running_loop().time()
            for key in tuple(self._backoff):
                if self._backoff[key] <= now:
                    self._backoff.pop(key)
            for row in rows:
                key = row["conversation_id"]
                if (key in self._queued or key in self._active or key in self._backoff
                        or len(self._queue) >= self.max_queued):
                    continue
                self._queued.add(key)
                self._queue.append(key)
            started = 0
            while self._queue and len(self._active) < self.max_concurrency and not self._closed:
                key = self._queue.popleft()
                self._queued.remove(key)
                if key in self._active or key in self._backoff:
                    continue
                task = asyncio.create_task(self._conversation(key), name="group:" + key)
                self._active[key] = task
                task.add_done_callback(lambda task, key=key: self._completed(key, task))
                started += 1
            return started

    def _completed(self, conversation_id: str, task: asyncio.Task) -> None:
        if self._active.get(conversation_id) is task:
            self._active.pop(conversation_id)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                self._error(conversation_id, error)
        self._wake.set()

    async def _renew(self, actor_box: list[ActorLease], foreground: asyncio.Task) -> None:
        try:
            while True:
                await asyncio.sleep(self.actor_ttl / 3)
                actor_box[0] = await self.messages.renew_actor(actor_box[0], ttl_seconds=self.actor_ttl)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error(actor_box[0].conversation_id, exc)
            if not foreground.cancelling():
                foreground.cancel()

    async def _cancel_execution(self, task: asyncio.Task | None) -> bool:
        if task is None:
            return True
        if not task.done():
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=self.cancellation_timeout)
            if not done:
                self._orphaned.add(task)
                task.add_done_callback(self._orphan_completed)
                return False
        if not task.cancelled():
            with suppress(BaseException):
                task.result()
        return True

    def _orphan_completed(self, task: asyncio.Task):
        self._orphaned.discard(task)
        if not task.cancelled():
            with suppress(BaseException):
                task.result()

    async def _conversation(self, conversation_id: str) -> None:
        actor_box: list[ActorLease] = []
        renewal = execution = None
        callback_stopped = True
        slot_acquired = False
        try:
            actor = await self.messages.acquire_actor(
                self._lease(), conversation_id, self.owner_id, ttl_seconds=self.actor_ttl,
            )
            actor_box.append(actor)
            renewal = asyncio.create_task(
                self._renew(actor_box, asyncio.current_task()), name="renew:" + conversation_id,
            )
            batch = await self.messages.select_input(actor, "input:" + str(uuid4()), mode="IDLE")
            if batch["status"] in {"IDLE", "WAIT_INPUT"}:
                # WAIT_INPUT remains durable without a hot queue.empty() loop.
                self._backoff[conversation_id] = asyncio.get_running_loop().time() + self.poll_interval
                return
            if batch.get("kind") == "SILENT_DRAIN":
                self.errors.pop(conversation_id, None)
                return
            if batch["status"] not in {"INPUT", "INPUT_BUDGET_EXCEEDED", "RESUME"}:
                raise ForestError("SCHEDULER_STATE", "Unexpected conversation preparation result", 409)
            run_id = batch["run_id"]
            context = await self.messages.context(actor_box[0], run_id=run_id)
            if batch["status"] == "INPUT_BUDGET_EXCEEDED" or context["budget_exceeded"]:
                await self.messages.end_run(actor_box[0], run_id=run_id,
                                            reason="context_budget_exceeded", delivery=self.delivery)
                self._error(conversation_id, ForestError("CONTEXT_BUDGET_EXCEEDED", "Input budget exceeded", 422))
                return
            await self.global_slots.acquire()
            slot_acquired = True
            if self._closed:
                raise asyncio.CancelledError
            # Waiting for another Bot's slot cannot extend authority. Re-read
            # guarded canonical context after the wait; no queued tail enters it.
            context = await self.messages.context(actor_box[0], run_id=run_id)
            execution = asyncio.create_task(self.execute(actor_box[0], batch, context), name="execute:" + run_id)
            try:
                parts = await asyncio.wait_for(asyncio.shield(execution), timeout=self.execution_timeout)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                callback_stopped = await self._cancel_execution(execution)
                if not callback_stopped:
                    raise ForestError("SCHEDULER_CLEANUP_FAILED", "Execution callback did not stop", 503) from exc
                await self.messages.end_run(actor_box[0], run_id=run_id,
                                            reason=self._code(exc), delivery=self.delivery)
                self._error(conversation_id, exc)
                return
            self.global_slots.release()
            slot_acquired = False
            if self._closed or asyncio.current_task().cancelling():
                raise asyncio.CancelledError
            try:
                await self.messages.finalize_reply(
                    actor_box[0], "group-reply:" + run_id, run_id=run_id,
                    expected_input_revision=batch["input_revision"], reply_parts=parts, delivery=self.delivery,
                )
            except ForestError as exc:
                if exc.code in {"INVALID_REPLY", "INVALID_MESSAGE", "INVALID_DELIVERY", "DELIVERY_TOO_LARGE"}:
                    await self.messages.end_run(actor_box[0], run_id=run_id,
                                                reason=exc.code, delivery=self.delivery)
                raise
            self.errors.pop(conversation_id, None)
        except asyncio.CancelledError:
            # Closing/pausing leaves uncommitted work durable. It is resumed under
            # a new actor lease, never marked answered because a local task ended.
            raise
        except Exception as exc:
            self._error(conversation_id, exc)
        finally:
            if renewal is not None:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            callback_stopped = await self._cancel_execution(execution) and callback_stopped
            if slot_acquired:
                if execution is not None and not execution.done():
                    # Cancellation is not proof of cleanup. A callback which
                    # ignores it retains real capacity until it actually exits.
                    execution.add_done_callback(lambda _: self.global_slots.release())
                else:
                    self.global_slots.release()
            if actor_box and callback_stopped:
                with suppress(ForestError):
                    await self.messages.release_actor(actor_box[0])

    async def close(self) -> None:
        if self._close_task is None or (
            self._close_task.done() and not self._close_task.cancelled()
            and self._close_task.exception() is not None and not self._orphaned
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:" + self.owner_id)
        await asyncio.shield(self._close_task)

    async def _close(self):
        self._closed = True
        self._wake.set()
        if self._runner is not None:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
        tasks = tuple(self._active.values())
        for task in tasks:
            if not task.cancelling():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._queue.clear()
        self._queued.clear()
        if self._orphaned:
            raise ForestError("SCHEDULER_CLEANUP_FAILED", "Execution callbacks remain owned", 503)
