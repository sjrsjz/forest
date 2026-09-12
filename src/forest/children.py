"""Bounded Bot-owned workers for private analysis, with durable lease recovery."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from forest.errors import ForestError
from forest.models import ModelResult, prepare_model_request


class ChildScheduler:
    def __init__(self, store, host_lease, resolve_client, *, poll_interval=0.2):
        self.store, self.host_lease, self.resolve_client = store, host_lease, resolve_client
        self.poll_interval = poll_interval
        self.owner_id = "children:" + uuid4().hex
        self.task = None
        self.workers = {}
        self.closed = False
        self.errors = {}

    @property
    def failure(self):
        if not self.closed and self.task is not None and self.task.done():
            return ForestError("CHILD_SCHEDULER_STOPPED", "Private analysis scheduler stopped", 503)
        return None

    async def start(self):
        if self.closed:
            raise ForestError("CHILD_SCHEDULER_CLOSED", "Private analysis scheduler is closed", 503)
        if self.task is None:
            self.task = asyncio.create_task(self._loop(), name=self.owner_id)

    async def _loop(self):
        while True:
            try:
                pending = await self.store.pending(self.host_lease())
                for row in pending:
                    identifier = row["child_id"]
                    if identifier in self.workers or len(self.workers) >= 2:
                        continue
                    task = asyncio.create_task(self._execute(identifier), name=identifier)
                    self.workers[identifier] = task
                    task.add_done_callback(
                        lambda task, identifier=identifier: self._finished(identifier, task)
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors["scan"] = getattr(exc, "code", "CHILD_SCAN_FAILED")
            await asyncio.sleep(self.poll_interval)

    def _finished(self, identifier, task):
        self.workers.pop(identifier, None)
        if not task.cancelled() and task.exception() is not None:
            if len(self.errors) >= 128:
                self.errors.pop(next(iter(self.errors)))
            self.errors[identifier] = getattr(task.exception(), "code", "CHILD_EXECUTION_FAILED")

    async def _heartbeat(self, lease, worker):
        try:
            while True:
                await asyncio.sleep(20)
                await self.store.renew(lease)
        except asyncio.CancelledError:
            raise
        except Exception:
            worker.cancel()

    async def _execute(self, child_id):
        lease = heartbeat = None
        try:
            lease = await self.store.claim(self.host_lease(), child_id, self.owner_id)
            if lease is None:
                return
            heartbeat = asyncio.create_task(
                self._heartbeat(lease, asyncio.current_task()), name=child_id + ":lease"
            )
            row = await self.store.load(lease)
            client, capabilities = await self.resolve_client(lease, row)
            if row["attempt"]:
                prepared = self.store.replay(row)
            else:
                media = await self.store.media(lease)
                values = row["spec"]["values"]
                prepared = prepare_model_request(
                    row["snapshot"],
                    capabilities,
                    request_id=child_id + ":request",
                    transport_attempt_id=uuid4().hex,
                    system_prompt="Perform only the requested private analysis using the supplied attributed evidence. You cannot call tools, delegate tasks, send messages, or change parent context. Return your analysis to the parent.",
                    tools=[],
                    media=media,
                    output_tokens=min(
                        values.get("child_max_output_tokens", 1024),
                        values.get("max_output_tokens", 1024),
                    ),
                    stream=values.get("stream", False),
                )
            attempt_id = await self.store.prepare(lease, prepared)
            entered = False

            async def check(request):
                nonlocal entered
                if request != prepared:
                    raise ForestError(
                        "CHILD_REQUEST_TAMPERED", "Child transport changed request bytes", 409
                    )
                entered = True
                await self.store.check_permit(lease, attempt_id, request)

            try:
                remaining = max(0.01, (row["deadline_at"] - datetime.now(UTC)).total_seconds())
                result = await client.dispatch(
                    prepared,
                    permitcheck=check,
                    timeout=min(remaining, row["spec"]["values"].get("model_timeout", 60)),
                )
            except BaseException as exc:
                result = ModelResult(
                    "UNKNOWN" if entered else "NOT_SENT",
                    prepared.request_id,
                    prepared.transport_attempt_id,
                    prepared.payload_digest,
                    error_code="CHILD_TRANSPORT_INTERRUPTED",
                )
                try:
                    await self.store.complete(lease, attempt_id, result)
                except Exception:
                    pass  # The persistent DISPATCHING attempt is recovered as UNKNOWN.
                if isinstance(exc, asyncio.CancelledError):
                    raise
                return
            await self.store.complete(lease, attempt_id, result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.store.reject(
                self.host_lease(),
                child_id,
                getattr(exc, "code", "CHILD_EXECUTION_FAILED"),
                lease=lease,
            )
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

    async def close(self):
        self.closed = True
        tasks = [*self.workers.values()]
        if self.task is not None:
            tasks.append(self.task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.workers.clear()
