"""Deployment-scoped supervisor for independent Bot runtime hosts."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from forest.bot_host import BotHost
from forest.errors import ForestError
from forest.storage.deployment_catalog import DeploymentSupervisorLease

_ERROR = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


@dataclass
class _BotRunner:
    stop: asyncio.Event
    task: asyncio.Task | None = None
    host: object | None = None
    management_accepting: bool = False
    management_inflight: int = 0
    management_condition: asyncio.Condition = field(default_factory=asyncio.Condition)


class MultiBotSupervisor:
    """Reconcile catalog intent without owning module configuration or Bot facts."""

    def __init__(
        self,
        catalog,
        *,
        storage_profiles,
        settings_resolver,
        host_factory=BotHost,
        host_id: str | None = None,
        incarnation: str | None = None,
        poll_seconds: float = 1.0,
        max_starting: int = 2,
        provisioning_executor=None,
    ) -> None:
        if not 0.05 <= poll_seconds <= 30:
            raise ValueError("poll_seconds is outside its bound")
        if type(max_starting) is not int or not 1 <= max_starting <= 32:
            raise ValueError("max_starting is outside its bound")
        self.catalog = catalog
        self.storage_profiles = storage_profiles
        self.settings_resolver = settings_resolver
        self.host_factory = host_factory
        self.host_id = host_id or "deployment-host/" + str(uuid4())
        self.incarnation = incarnation or "incarnation/" + str(uuid4())
        self.poll_seconds = poll_seconds
        self._start_slots = asyncio.Semaphore(max_starting)
        self.provisioning_executor = provisioning_executor
        self.lease = None
        self.failure: str | None = None
        self._closing = False
        self._wake = asyncio.Event()
        self._runners: dict[str, _BotRunner] = {}
        self._renewal_task: asyncio.Task | None = None
        self._reconcile_task: asyncio.Task | None = None
        self._provisioning_task: asyncio.Task | None = None

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
        if self.provisioning_executor is not None:
            self._provisioning_task = asyncio.create_task(
                self._provision(), name="deployment-provisioning/" + self.incarnation
            )
        self._wake.set()

    def notify(self) -> None:
        self._wake.set()

    def authority_lease(self, *, allow_closing: bool = False) -> DeploymentSupervisorLease:
        if (
            type(self.lease) is not DeploymentSupervisorLease
            or (self._closing and not allow_closing)
        ):
            raise ForestError(
                "DEPLOYMENT_RUNTIME_UNAVAILABLE",
                "Deployment Supervisor lease is unavailable", 503,
            )
        return self.lease

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(
            task
            for task in (self._renewal_task, self._reconcile_task, self._provisioning_task)
            if task is not None
        ) + tuple(
            runner.task for runner in self._runners.values() if runner.task is not None
        )

    @staticmethod
    def management_scope_id(bot_id: str) -> str:
        return "bot-" + hashlib.sha256(bot_id.encode("utf-8")).hexdigest()[:32]

    async def management_scopes(self) -> tuple[dict, ...]:
        scopes = []
        for bot in await self.catalog.list_bots():
            runner = self._runners.get(bot["bot_id"])
            scopes.append(
                {
                    "scope_id": self.management_scope_id(bot["bot_id"]),
                    "scope_kind": "bot",
                    "bot_id": bot["bot_id"],
                    "label": bot["display_name"],
                    "available": (
                        runner is not None
                        and runner.host is not None
                        and runner.management_accepting
                    ),
                    "desired_state": bot["desired_state"],
                    "observed_state": bot["observed_state"],
                }
            )
        return tuple(scopes)

    @asynccontextmanager
    async def management_host(self, scope_id: str):
        selected = None
        for bot_id, runner in self._runners.items():
            if self.management_scope_id(bot_id) == scope_id:
                selected = runner
                break
        if selected is None:
            raise ForestError("BOT_MANAGEMENT_SCOPE_NOT_FOUND", "Bot scope does not exist", 404)
        async with selected.management_condition:
            if not selected.management_accepting or selected.host is None:
                raise ForestError(
                    "BOT_MANAGEMENT_UNAVAILABLE",
                    "Bot runtime is not available for management",
                    409,
                )
            selected.management_inflight += 1
            host = selected.host
        try:
            yield host
        finally:
            async with selected.management_condition:
                selected.management_inflight -= 1
                selected.management_condition.notify_all()

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        runners = tuple(self._runners.values())
        for runner in runners:
            runner.management_accepting = False
            runner.host = None
            runner.stop.set()
        if runners:
            await asyncio.gather(
                *(runner.task for runner in runners if runner.task is not None),
                return_exceptions=True,
            )
        self._runners.clear()
        for name in ("_provisioning_task", "_reconcile_task", "_renewal_task"):
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
                runner.management_accepting = False
                runner.host = None
                runner.stop.set()
            self._wake.set()
            raise
        except Exception:
            self.failure = "DEPLOYMENT_SUPERVISOR_RENEWAL_FAILED"
            self._closing = True
            for runner in self._runners.values():
                runner.management_accepting = False
                runner.host = None
                runner.stop.set()
            self._wake.set()
            raise

    async def _provision(self) -> None:
        try:
            while not self._closing:
                result = await self.provisioning_executor.run_once(self.lease)
                if result is not None:
                    self._wake.set()
                    continue
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
                except TimeoutError:
                    pass
                self._wake.clear()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            self._closing = True
            for runner in self._runners.values():
                runner.management_accepting = False
                runner.host = None
                runner.stop.set()
            self._wake.set()
            raise
        except Exception:
            self.failure = "DEPLOYMENT_PROVISIONING_WORKER_FAILED"
            self._closing = True
            for runner in self._runners.values():
                runner.management_accepting = False
                runner.host = None
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
                        runner = _BotRunner(stop)
                        runner.task = asyncio.create_task(
                            self._run_bot(bot, runner), name="bot-runtime/" + bot["bot_id"]
                        )
                        self._runners[bot["bot_id"]] = runner
                    elif not should_run and runner is not None:
                        runner.management_accepting = False
                        runner.host = None
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
            if runner.task is not None and runner.task.done():
                self._runners.pop(bot_id, None)
                failure = runner.task.exception()
                if failure is not None:
                    raise failure

    async def _run_bot(self, bot: dict, runner: _BotRunner) -> None:
        bot_id = bot["bot_id"]
        host = None
        try:
            async with self._start_slots:
                if runner.stop.is_set():
                    return
                await self.catalog.observe(
                    self.lease, bot_id=bot_id, observed_state="STARTING"
                )
                storage = await self.storage_profiles.resolve(
                    profile_ref=bot["storage_profile_ref"],
                    expected_bot_id=bot_id,
                    expected_storage_generation=bot["storage_generation"],
                )
                settings = self.settings_resolver(bot, storage)
                if inspect.isawaitable(settings):
                    settings = await settings
                if (
                    getattr(settings, "bot_id", None) != bot_id
                    or getattr(settings, "dsn", None) != storage.dsn
                ):
                    raise ForestError(
                        "BOT_SETTINGS_SCOPE_MISMATCH",
                        "Resolved Bot settings differ from the approved storage profile",
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
            if not runner.stop.is_set():
                runner.host = host
                runner.management_accepting = True
            stop_task = asyncio.create_task(
                runner.stop.wait(), name="bot-stop-wait/" + bot_id
            )
            supervision_task = getattr(host, "_supervision_task", None)
            if supervision_task is None:
                await stop_task
            else:
                done, _pending = await asyncio.wait(
                    (stop_task, supervision_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if supervision_task in done and not runner.stop.is_set():
                    stop_task.cancel()
                    await asyncio.gather(stop_task, return_exceptions=True)
                    await supervision_task
                    raise ForestError(
                        "BOT_RUNTIME_SUPERVISION_STOPPED",
                        "Bot runtime supervision stopped unexpectedly",
                        503,
                    )
            runner.management_accepting = False
            runner.host = None
            async with runner.management_condition:
                await runner.management_condition.wait_for(
                    lambda: runner.management_inflight == 0
                )
            await self.catalog.observe(self.lease, bot_id=bot_id, observed_state="STOPPING")
            await host.close()
            host = None
            await self.catalog.observe(self.lease, bot_id=bot_id, observed_state="OFFLINE")
        except asyncio.CancelledError:
            runner.management_accepting = False
            runner.host = None
            async with runner.management_condition:
                await runner.management_condition.wait_for(
                    lambda: runner.management_inflight == 0
                )
            if host is not None:
                await host.close()
            raise
        except Exception as exc:
            runner.management_accepting = False
            runner.host = None
            async with runner.management_condition:
                await runner.management_condition.wait_for(
                    lambda: runner.management_inflight == 0
                )
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
