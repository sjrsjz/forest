"""Deployment-scoped supervisor for independent Bot runtime hosts."""

from __future__ import annotations

import asyncio
import inspect
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from forest.clean_bot_host import CleanBotHost
from forest.errors import ForestError

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


@dataclass
class _BotRunner:
    stop: asyncio.Event
    task: asyncio.Task


class CleanMultiBotSupervisor:
    """Reconcile catalog intent without owning module configuration or Bot facts."""

    def __init__(
        self,
        catalog,
        *,
        settings_resolver,
        host_factory=CleanBotHost,
        host_id: str | None = None,
        incarnation: str | None = None,
        poll_seconds: float = 1.0,
        max_starting: int = 2,
    ) -> None:
        if not 0.05 <= poll_seconds <= 30:
            raise ValueError("poll_seconds is outside its bound")
        if type(max_starting) is not int or not 1 <= max_starting <= 32:
            raise ValueError("max_starting is outside its bound")
        self.catalog = catalog
        self.settings_resolver = settings_resolver
        self.host_factory = host_factory
        self.host_id = host_id or "deployment-host/" + str(uuid4())
        self.incarnation = incarnation or "incarnation/" + str(uuid4())
        self.poll_seconds = poll_seconds
        self._start_slots = asyncio.Semaphore(max_starting)
        self.lease = None
        self.failure: str | None = None
        self._closing = False
        self._wake = asyncio.Event()
        self._runners: dict[str, _BotRunner] = {}
        self._renewal_task: asyncio.Task | None = None
        self._reconcile_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.lease is not None or self._renewal_task is not None:
            raise ForestError(
                "DEPLOYMENT_SUPERVISOR_ALREADY_STARTED",
                "Deployment supervisor is already started",
                409,
            )
        await self.catalog.initialize()
        self.lease = await self.catalog.claim_supervisor(self.host_id, self.incarnation)
        self._renewal_task = asyncio.create_task(
            self._renew(), name="deployment-supervisor-lease/" + self.incarnation
        )
        self._reconcile_task = asyncio.create_task(
            self._reconcile(), name="deployment-supervisor/" + self.incarnation
        )
        self._wake.set()

    def notify(self) -> None:
        self._wake.set()

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(
            task
            for task in (self._renewal_task, self._reconcile_task)
            if task is not None
        ) + tuple(runner.task for runner in self._runners.values())

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        runners = tuple(self._runners.values())
        for runner in runners:
            runner.stop.set()
        if runners:
            await asyncio.gather(*(runner.task for runner in runners), return_exceptions=True)
        self._runners.clear()
        for name in ("_reconcile_task", "_renewal_task"):
            task = getattr(self, name)
            setattr(self, name, None)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        lease, self.lease = self.lease, None
        if lease is not None:
            try:
                await self.catalog.release_supervisor(lease)
            except ForestError as exc:
                if exc.code != "STALE_DEPLOYMENT_SUPERVISOR":
                    raise

    async def _renew(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(10)
                if self._closing:
                    return
                self.lease = await self.catalog.renew_supervisor(self.lease)
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            self._closing = True
            for runner in self._runners.values():
                runner.stop.set()
            self._wake.set()
            raise
        except Exception:
            self.failure = "DEPLOYMENT_SUPERVISOR_RENEWAL_FAILED"
            self._closing = True
            for runner in self._runners.values():
                runner.stop.set()
            self._wake.set()
            raise

    async def _reconcile(self) -> None:
        try:
            while not self._closing:
                self._reap()
                now = datetime.now(timezone.utc)
                for bot in await self.catalog.list_bots():
                    runner = self._runners.get(bot["bot_id"])
                    should_run = (
                        bot["supply_state"] == "READY"
                        and bot["desired_state"] == "ONLINE"
                        and not (
                            bot["observed_state"] == "FAILED"
                            and bot["retry_not_before"] is not None
                            and bot["retry_not_before"] > now
                        )
                    )
                    if should_run and runner is None:
                        stop = asyncio.Event()
                        task = asyncio.create_task(
                            self._run_bot(bot, stop), name="bot-runtime/" + bot["bot_id"]
                        )
                        self._runners[bot["bot_id"]] = _BotRunner(stop, task)
                    elif not should_run and runner is not None:
                        runner.stop.set()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
                except TimeoutError:
                    pass
                self._wake.clear()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            raise
        except Exception:
            self.failure = "DEPLOYMENT_SUPERVISOR_FAILED"
            raise

    def _reap(self) -> None:
        for bot_id, runner in tuple(self._runners.items()):
            if runner.task.done():
                self._runners.pop(bot_id, None)
                failure = runner.task.exception()
                if failure is not None:
                    raise failure

    async def _run_bot(self, bot: dict, stop: asyncio.Event) -> None:
        bot_id = bot["bot_id"]
        host = None
        try:
            async with self._start_slots:
                if stop.is_set():
                    return
                await self.catalog.observe(
                    self.lease, bot_id=bot_id, observed_state="STARTING"
                )
                settings = self.settings_resolver(bot)
                if inspect.isawaitable(settings):
                    settings = await settings
                if getattr(settings, "bot_id", None) != bot_id:
                    raise ForestError(
                        "BOT_SETTINGS_SCOPE_MISMATCH",
                        "Resolved Bot settings belong to another identity",
                        409,
                    )
                host = self.host_factory(settings)
                await host.start()
                graph_id = host.registry.graph_identity.graph_id
            await self.catalog.observe(
                self.lease,
                bot_id=bot_id,
                observed_state="ONLINE",
                graph_id=graph_id,
            )
            await stop.wait()
            await self.catalog.observe(self.lease, bot_id=bot_id, observed_state="STOPPING")
            await host.close()
            host = None
            await self.catalog.observe(self.lease, bot_id=bot_id, observed_state="OFFLINE")
        except asyncio.CancelledError:
            if host is not None:
                await host.close()
            raise
        except Exception as exc:
            if host is not None:
                try:
                    await host.close()
                except Exception:
                    pass
            code = exc.code if isinstance(exc, ForestError) else "BOT_RUNTIME_FAILED"
            try:
                await self.catalog.observe(
                    self.lease,
                    bot_id=bot_id,
                    observed_state="FAILED",
                    error_code=(
                        code
                        if isinstance(code, str) and _ERROR.fullmatch(code)
                        else "BOT_RUNTIME_FAILED"
                    ),
                )
            except ForestError:
                raise
