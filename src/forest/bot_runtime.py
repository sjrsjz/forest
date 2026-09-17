"""Generic supervision for one Bot-scoped immutable module graph."""

from __future__ import annotations

import asyncio

from forest.errors import ForestError
from forest.runtime_modules import RuntimeModuleRegistry


def _observe(task):
    if not task.cancelled():
        task.exception()


class BotRuntimeSupervisor:
    def __init__(self, bot, vault, packages: dict, *, global_slots=None, job_authority=None,
                 global_model_slots=None, module_packages=None):
        self.bot, self.kernel, self.vault = bot, bot.kernel, vault
        self.global_slots = global_slots
        self.global_model_slots = global_model_slots
        self.packages = dict(packages)
        self.module_packages = {} if module_packages is None else module_packages
        self._closed = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._start_lock = asyncio.Lock()
        self._close_task: asyncio.Task | None = None
        self._started = False
        self._failure: ForestError | None = None
        self.job_authority = job_authority
        from forest.storage.schema_bundles import SchemaBundleInstaller

        self.schema_bundle_installer = SchemaBundleInstaller(bot.store)
        self.module_registry = RuntimeModuleRegistry(self)

    def module(self, component_id: str):
        """Resolve a runtime capability owner by its stable component identity."""
        return self.module_registry.get(component_id)

    def notify(self):
        """Wake durable work through descriptor-declared module ports."""
        self.module_registry.notify_work()
        self.kernel.notify_events()
        self.kernel.notify_component_jobs()

    @property
    def failure(self):
        return self._failure

    @property
    def healthy(self) -> bool:
        """Report host health without exposing owned task internals to Fleet."""
        return (
            self._started
            and not self._closed.is_set()
            and self._failure is None
            and all(not task.done() for task in self._tasks)
        )

    def _fatal(self, code):
        if self._failure is None:
            self._failure = ForestError(code, "Bot runtime supervisor requires recovery", 503)
        self._closed.set()
        adapter = self.module_registry.instances.get("adapter.onebot")
        if adapter is not None:
            adapter.stop_admission()

    def _critical_done(self, task):
        _observe(task)
        if not self._closed.is_set():
            self._fatal("RUNTIME_SUPERVISOR_STOPPED")

    async def start(self):
        async with self._start_lock:
            if self._closed.is_set():
                raise ForestError("BOT_RUNTIME_CLOSED", "Bot runtime is closed", 503)
            if self._started:
                return
            await self.module_registry.start()
            self._tasks = [
                asyncio.create_task(self._health_loop(), name="runtime-health"),
            ]
            self._tasks.extend(self.module_registry.tasks())
            for task in self._tasks:
                task.add_done_callback(self._critical_done)
            self._started = True

    async def _health_loop(self):
        while not self._closed.is_set():
            if (
                self.module_registry.failures()
            ):
                self._fatal("RUNTIME_SUBTASK_FAILED")
                return
            await asyncio.sleep(0.1)

    def _cleanup_lease(self):
        # SQL validates this last lease even after local admission has closed.
        if self.kernel.lease is None:
            raise ForestError("STALE_HOST", "No host lease remains", 409)
        return self.kernel.lease

    async def close(self):
        self._closed.set()
        adapter = self.module_registry.instances.get("adapter.onebot")
        if adapter is not None:
            adapter.stop_admission()
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception())
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:bot-runtime")
            self._close_task.add_done_callback(_observe)
        await asyncio.shield(self._close_task)

    async def _close(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        errors = await self.module_registry.close()
        if errors:
            raise ForestError("RUNTIME_CLEANUP_FAILED", "Runtime modules remain owned", 503)
        self._tasks.clear()
