"""Whole-package development watch with durable latest-edit publication fences.

Polling uses the package capture scanner, including helpers and resources. There
is one running candidate and one coalesced pending edit; neither source imports
nor route mutation happen here. The kernel owns every Worker lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Callable
from uuid import uuid4

from psycopg.errors import RaiseException

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog, digest_json, source_fingerprint
from forest.storage.watch import WatchStore, _freeze


class DevWatcher:
    def __init__(
        self,
        kernel: Kernel,
        catalog: PackageCatalog,
        source: Path,
        *,
        config: dict | None = None,
        grants: frozenset[str] = frozenset(),
        debounce: float = 0.3,
        max_delay: float = 2.0,
        poll_interval: float = 0.1,
        on_event: Callable[[dict], None] | None = None,
    ):
        if not 0 < poll_interval <= max_delay or not 0 <= debounce <= max_delay <= 2:
            raise ValueError("Watch timing must be bounded by two seconds")
        self.kernel, self.catalog = kernel, catalog
        self.source = Path(os.path.abspath(source))
        self.source_identity = os.path.normcase(str(self.source))
        frozen = _freeze({"config": {} if config is None else config, "grants": sorted(grants)})
        self.config = frozen["config"]
        self.grants = frozenset(frozen["grants"])
        self.store = WatchStore(kernel.store)
        self.debounce, self.max_delay, self.poll_interval = debounce, max_delay, poll_interval
        self.on_event = on_event
        self.watch_id: str | None = None
        self.last_event: dict | None = None
        self.published_count = 0
        self._failure: ForestError | None = None
        self._task: asyncio.Task | None = None
        self._start_task: asyncio.Task | None = None
        self._closing = False
        self._close_task: asyncio.Task | None = None
        self._io: set[asyncio.Task] = set()
        self._observation_lock = asyncio.Lock()
        self._changed = asyncio.Event()
        self._pending: dict | None = None
        self._first_dirty = 0.0
        self._last_dirty = 0.0
        self._fingerprint: str | None = None

    @property
    def task(self) -> asyncio.Task | None:
        return self._task

    @property
    def failure(self) -> ForestError | None:
        if self._failure:
            return self._failure
        if self.kernel.failure:
            return ForestError(self.kernel.failure, "Development kernel is no longer healthy", 503)
        return None

    def _emit(self, status: str, **fields) -> None:
        event = {"status": status, "watch_id": self.watch_id, **fields}
        self.last_event = event
        if self.on_event:
            self.on_event(json.loads(json.dumps(event)))

    async def _thread(self, function, *args, **kwargs):
        # Cancelling an asyncio wrapper does not stop its filesystem thread.
        # Keep ownership until the actual operation finishes, including close.
        task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        self._io.add(task)
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                self._io.discard(task)

    async def start(self) -> None:
        if self._start_task is not None or self._closing:
            raise ForestError("WATCH_ALREADY_STARTED", "Development watch already started")
        self._start_task = asyncio.create_task(self._start(), name="forest-dev-start")
        await asyncio.shield(self._start_task)

    async def _start(self) -> None:
        head = await self.store.initialize(
            self.kernel._authority(), self.source_identity, self.config, self.grants
        )
        self.watch_id = head["watch_id"]
        # Restore committed bytes first. Invalid/deleted workspace contents can
        # become a later desired request but cannot replace the recovery artifact.
        await self._restore()
        if self._closing:
            raise ForestError("WATCH_CLOSING", "Development watch is closing")
        self._task = asyncio.create_task(self._run(), name="forest-dev-watch")

    async def _restore(self) -> None:
        head = await self.store.current(self.kernel._authority(), self.watch_id)
        good = head["last_good"]
        if not good:
            return
        snapshot = await self._thread(self.catalog.get, good["package_digest"])
        if os.path.normcase(str(snapshot.root.absolute())) != os.path.normcase(good["artifact"]):
            raise ForestError(
                "WATCH_ARTIFACT_UNAVAILABLE",
                "The frozen development artifact belongs to a different catalog",
                409,
            )
        operation = "dev-recover:" + uuid4().hex
        await self.store.activate(
            self.kernel._authority(), self.watch_id, good["request_id"], operation, recovering=True
        )
        route = await self.kernel.install(
            snapshot, config=head["config"], grants=frozenset(head["grants"]), operation_id=operation
        )
        self._fingerprint = (
            good["source_digest"] if head["desired_request_id"] == good["request_id"] else None
        )
        self._emit(
            "restored", request_id=good["request_id"], package_digest=snapshot.digest, route=route
        )

    async def _observe(self) -> None:
        async with self._observation_lock:
            head = await self.store.current(self.kernel._authority(), self.watch_id)
            source = Path(head["source_identity"])
            error = None
            try:
                fingerprint = await self._thread(source_fingerprint, source)
            except ForestError as exc:
                error = exc.code
                # Even an unreadable/unstable new source fences an older pending
                # candidate. This marker is never accepted as an artifact digest.
                fingerprint = digest_json({"source_unavailable": error})
            if fingerprint == self._fingerprint:
                return
            request = await self.store.accept(
                self.kernel._authority(), self.watch_id, uuid4().hex, fingerprint
            )
            self._fingerprint = fingerprint
            now = asyncio.get_running_loop().time()
            if self._pending is None:
                self._first_dirty = now
            self._last_dirty = now
            self._pending = {**request, "scan_error": error}
            self._changed.set()
            self._emit("accepted", request_id=request["request_id"], sequence=request["sequence"])

    async def _observer(self) -> None:
        while not self._closing:
            if self.kernel.failure:
                raise ForestError(
                    self.kernel.failure, "Development kernel is no longer healthy", 503
                )
            await self._observe()
            await asyncio.sleep(self.poll_interval)

    async def _next(self) -> dict:
        while True:
            self._changed.clear()
            if self._pending is None:
                await self._changed.wait()
                continue
            now = asyncio.get_running_loop().time()
            remaining = (
                min(self._last_dirty + self.debounce, self._first_dirty + self.max_delay) - now
            )
            if remaining <= 0:
                pending, self._pending = self._pending, None
                return pending
            try:
                await asyncio.wait_for(self._changed.wait(), remaining)
            except TimeoutError:
                pass

    async def _candidate(self, request: dict) -> None:
        request_id = request["request_id"]
        if request["scan_error"]:
            raise ForestError(request["scan_error"], "Package source could not be captured", 422)
        self._emit("building", request_id=request_id)
        head = await self.store.current(self.kernel._authority(), self.watch_id)
        snapshot = await self._thread(self.catalog.build, Path(head["source_identity"]), development=True)
        original_digest = "sha256:" + snapshot.manifest["version"].rsplit("+dev.", 1)[-1]
        await self._observe()
        if original_digest != request["source_digest"] or self._fingerprint != original_digest:
            raise ForestError(
                "WATCH_SUPERSEDED", "Package changed while its candidate was being built"
            )
        await self.store.built(
            self.kernel._authority(),
            self.watch_id,
            request_id,
            source_digest=original_digest,
            package_digest=snapshot.digest,
            artifact=str(snapshot.root.absolute()),
            component_id=snapshot.component["id"],
        )
        operation = "dev-build:" + request_id
        await self.store.activate(self.kernel._authority(), self.watch_id, request_id, operation)
        head = await self.store.current(self.kernel._authority(), self.watch_id)
        self._emit("validating", request_id=request_id, package_digest=snapshot.digest)
        route = await self.kernel.install(
            snapshot, config=head["config"], grants=frozenset(head["grants"]), operation_id=operation
        )
        self.published_count += 1
        self._emit("published", request_id=request_id, package_digest=snapshot.digest, route=route)

    async def _builder(self) -> None:
        while not self._closing:
            request = await self._next()
            try:
                await self._candidate(request)
            except RaiseException as exc:
                # Only known authority rejections are recoverable candidate
                # failures. Never expose arbitrary SQL diagnostics in the CLI.
                if exc.diag.message_primary not in {
                    "WATCH_SUPERSEDED",
                    "WATCH_AUTHORITY_EXPIRED",
                    "WATCH_ACTIVATION_REQUIRED",
                }:
                    raise
                await self._rejected(
                    request,
                    ForestError(exc.diag.message_primary, "Development publication was fenced"),
                )
            except ForestError as exc:
                if exc.code in {
                    "STALE_HOST",
                    "STORAGE_SEALED",
                    "WATCH_AUTHORITY_REVOKED",
                    "KERNEL_CLOSING",
                    "WORKER_CLEANUP_FAILED",
                }:
                    raise
                await self._rejected(request, exc)

    async def _rejected(self, request: dict, error: ForestError) -> None:
        await self.store.failed(
            self.kernel._authority(), self.watch_id, request["request_id"], error.code
        )
        # Compilation details are generated by the static package checker. User
        # hook exception bodies are not relayed by the Worker protocol.
        self._emit("failed", request_id=request["request_id"], error=error.as_dict())

    async def _run(self) -> None:
        children = [
            asyncio.create_task(self._observer(), name="forest-dev-observer"),
            asyncio.create_task(self._builder(), name="forest-dev-builder"),
        ]
        try:
            done, _ = await asyncio.wait(children, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            if not self._closing:
                raise ForestError(
                    "WATCH_STOPPED", "Development supervision stopped unexpectedly", 503
                )
        except asyncio.CancelledError:
            if not self._closing:
                self._failure = ForestError(
                    "WATCH_CANCELLED", "Development supervision was interrupted", 503
                )
            raise
        except Exception as exc:
            self._failure = (
                exc
                if isinstance(exc, ForestError)
                else ForestError(
                    "WATCH_FAILED",
                    "Development supervision failed; restart to reconcile committed artifacts",
                    503,
                )
            )
            self._emit("error", error=self._failure.as_dict())
        finally:
            for task in children:
                task.cancel()
            await asyncio.gather(*children, return_exceptions=True)

    async def wait(self) -> None:
        if self._task is None:
            raise ForestError("WATCH_NOT_STARTED", "Development watch has not started")
        await asyncio.shield(self._task)
        if self.failure:
            raise self.failure

    async def close(self) -> None:
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._close(), name="forest-dev-close")
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self._start_task and not self._start_task.done():
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._io:
            await asyncio.gather(*self._io, return_exceptions=True)
            self._io.clear()
        self._pending = None
        self._emit("stopped")
