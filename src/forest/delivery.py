"""Bounded Delivery execution with a reserved CONTROL preparation slot.

Adapter conversion and HTTP are caller-supplied capabilities. The former returns
exact bytes and its generation reference; only a committed database permit lets
the latter see those bytes. This module never obtains Bot credentials itself.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import uuid4

from forest.errors import ForestError
from forest.storage.bot import HostLease
from forest.storage.delivery import DeliveryClaim, DeliveryStore


@dataclass(frozen=True)
class PreparedDelivery:
    prepared: Any
    send: Callable[..., Awaitable[Any]]
    executor_ref: dict


class DeliveryWorker:
    def __init__(
        self,
        store: DeliveryStore,
        lease: HostLease | Callable[[], HostLease],
        prepare: Callable[[DeliveryClaim], Awaitable[PreparedDelivery]],
        *,
        parallelism: int = 4,
        prepare_timeout: float = 10,
        send_timeout: float = 15,
        interval_seconds: float = 1,
        idle_seconds: float = 0.1,
        on_error: Callable[[str], Any] | None = None,
    ):
        if (
            not 2 <= parallelism <= 32
            or not 0 < prepare_timeout <= 60
            or not 0 < send_timeout <= 60
        ):
            raise ValueError("Delivery requires bounded capacity and timeouts")
        if not 0 <= interval_seconds <= 3600 or not 0.01 <= idle_seconds <= 10:
            raise ValueError("Invalid delivery scheduling interval")
        self.store = store
        self._lease = lease
        self.prepare = prepare
        self.parallelism = parallelism
        self.prepare_timeout = prepare_timeout
        self.send_timeout = send_timeout
        self.interval_seconds = interval_seconds
        self.idle_seconds = idle_seconds
        self.on_error = on_error
        self.worker_id = str(uuid4())
        self._closing = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()
        self.failure: str | None = None

    @property
    def lease(self):
        return self._lease() if callable(self._lease) else self._lease

    async def _error(self, code):
        if self.on_error:
            result = self.on_error(code)
            if inspect.isawaitable(result):
                await result

    async def _release(self, lease, claim, *, code, terminal=False, delay=1):
        try:
            await self.store.release(
                lease, claim, error_code=code, terminal=terminal, delay_seconds=delay
            )
        except ForestError as exc:
            if exc.code not in {"STALE_DELIVERY_OWNER", "STALE_HOST"}:
                await self._error(exc.code)
        except Exception:
            # If permit acknowledgement was lost, release cannot turn its
            # DISPATCHING fact back into PENDING. Recovery will retain UNKNOWN.
            await self._error("DELIVERY_RELEASE_UNAVAILABLE")

    async def run_once(self, *, lanes=("CONTROL", "CHAT")) -> bool:
        lease = self.lease
        claim = await self.store.claim(
            lease, self.worker_id, lanes=lanes, ttl_seconds=self.prepare_timeout + 10
        )
        if claim is None:
            return False
        permit = None
        try:
            async with asyncio.timeout(self.prepare_timeout):
                prepared = await self.prepare(claim)
            if not isinstance(prepared, PreparedDelivery):
                raise ForestError(
                    "INVALID_PREPARED_DELIVERY", "Adapter returned an invalid prepared send", 422
                )
            permit = await self.store.permit(
                lease,
                claim,
                action=prepared.prepared.action,
                wire_body=prepared.prepared.wire_body,
                wire_digest=prepared.prepared.wire_digest,
                executor_ref=prepared.executor_ref,
                timeout_seconds=self.send_timeout,
                interval_seconds=self.interval_seconds,
            )
        except asyncio.CancelledError:
            await asyncio.shield(self._release(lease, claim, code="PREPARE_CANCELLED", delay=0))
            raise
        except ForestError as exc:
            terminal = exc.code in {
                "INVALID_WIRE_PAYLOAD",
                "INVALID_PREPARED_DELIVERY",
                "INVALID_DELIVERY",
                "ONEBOT_INVALID",
                "ONEBOT_TOO_LARGE",
                "INPUT_INVALID",
                "OUTPUT_INVALID",
                "CONTRACT_INCOMPATIBLE",
                "POLICY_CHANGED",
                "CANCELLED",
            }
            await self._release(
                lease,
                claim,
                code=exc.code,
                terminal=terminal,
                delay=0.1 if exc.code == "DELIVERY_RATE_LIMITED" else 1,
            )
            return True
        except Exception:
            await self._release(lease, claim, code="PREPARE_UNAVAILABLE")
            await self._error("PREPARE_UNAVAILABLE")
            return True
        try:
            # A permit is already a durable may-have-sent record. No transport
            # exception or local timeout can establish definite non-delivery.
            await self.store.mark_started(permit)
            remaining = min(
                self.send_timeout, (permit.deadline_at - datetime.now(timezone.utc)).total_seconds()
            )
            if remaining <= 0:
                await self.store.complete(
                    permit, "NOT_SENT", {"error_code": "PERMIT_EXPIRED_BEFORE_SEND"}
                )
                return True
            async with asyncio.timeout(remaining):
                result = await prepared.send(prepared.prepared, timeout=remaining)
            status = getattr(result, "status", None)
            receipt = result.as_dict() if hasattr(result, "as_dict") else {}
            if status not in ("SENT", "NOT_SENT", "UNKNOWN"):
                status, receipt = "UNKNOWN", {"error_code": "INVALID_RECEIPT"}
            await self.store.complete(permit, status, receipt)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(
                    self.store.complete(permit, "UNKNOWN", {"error_code": "SEND_CANCELLED"})
                )
            except Exception:
                await self._error("DELIVERY_OBSERVATION_UNAVAILABLE")
            raise
        except Exception:
            try:
                await self.store.complete(permit, "UNKNOWN", {"error_code": "SEND_RESULT_LOST"})
            except Exception:
                await self._error("DELIVERY_OBSERVATION_UNAVAILABLE")
        return True

    async def _loop(self, slot: int):
        lanes = ("CONTROL",) if slot == 0 else ("CONTROL", "CHAT")
        while not self._closing.is_set():
            try:
                worked = await self.run_once(lanes=lanes)
            except asyncio.CancelledError:
                raise
            except ForestError as exc:
                await self._error(exc.code)
                if exc.code in {"STALE_HOST", "STORAGE_SEALED"}:
                    self.failure = exc.code
                    self._closing.set()
                    return
                worked = False
            except Exception:
                await self._error("DELIVERY_STORE_UNAVAILABLE")
                worked = False
            if not worked:
                try:
                    await asyncio.wait_for(self._closing.wait(), timeout=self.idle_seconds)
                except TimeoutError:
                    pass

    async def _recovery_loop(self):
        while not self._closing.is_set():
            try:
                await self.store.recover(self.lease)
            except asyncio.CancelledError:
                raise
            except ForestError as exc:
                await self._error(exc.code)
                if exc.code in {"STALE_HOST", "STORAGE_SEALED"}:
                    self.failure = exc.code
                    self._closing.set()
                    return
            except Exception:
                await self._error("DELIVERY_RECOVERY_UNAVAILABLE")
            try:
                await asyncio.wait_for(self._closing.wait(), timeout=1)
            except TimeoutError:
                pass

    async def run_forever(self):
        if self._tasks:
            raise RuntimeError("Delivery worker is already running")
        self._tasks = {
            asyncio.create_task(self._loop(slot), name=f"delivery:{slot}")
            for slot in range(self.parallelism)
        }
        self._tasks.add(asyncio.create_task(self._recovery_loop(), name="delivery:recovery"))
        for task in self._tasks:
            task.add_done_callback(self._task_finished)
        try:
            await self._closing.wait()
        finally:
            self._closing.set()
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

    def _task_finished(self, task):
        if self._closing.is_set():
            return
        self.failure = "DELIVERY_TASK_CANCELLED" if task.cancelled() else "DELIVERY_TASK_FAILED"
        if not task.cancelled():
            task.exception()  # Observe the failure; no user payload or exception text is logged.
        self._closing.set()

    async def close(self):
        self._closing.set()
        for task in tuple(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
