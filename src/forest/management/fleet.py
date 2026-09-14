"""Supervise independently leased Bot kernels from the durable deployment directory."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from psycopg.errors import RaiseException

from forest.errors import ForestError
from forest.kernel import DependencyPlan, Kernel, resolve_dependencies
from forest.management.authority import WebAuthority
from forest.management.catalog import BotCatalog
from forest.management.packages import PackageRegistry
from forest.module_validation import validate_component_values
from forest.packages import PackageCatalog, PackageSnapshot
from forest.platform import BindingRuntime
from forest.storage.bot import BotStore
from forest.storage.control import BotControl


@dataclass
class ManagedBot:
    store: BotStore
    kernel: Kernel
    control: BotControl
    applied: dict[str, int] = field(default_factory=dict)
    applied_states: dict[str, int] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    task: asyncio.Task | None = None
    cleanup_task: asyncio.Task | None = None
    ready: bool = False
    retries: dict[str, tuple[int, int, float]] = field(default_factory=dict)
    platform: BindingRuntime | None = None
    directory_generation: int = 1


class BotFleet:
    def __init__(
        self,
        catalog: BotCatalog,
        authority: WebAuthority,
        data_dir: Path,
        *,
        max_concurrent_attaches: int = 4,
        max_bots: int = 256,
        attach_timeout: float = 20,
        reconcile_interval: float = 2,
        retry_base: float = 2,
        retry_max: float = 300,
    ):
        if not (
            1 <= max_concurrent_attaches <= 16
            and 1 <= max_bots <= 1024
            and 0 < attach_timeout <= 120
            and 0 < reconcile_interval <= 60
            and 0 < retry_base <= retry_max <= 3600
        ):
            raise ValueError("Invalid Fleet resource limits")
        self.catalog, self.authority, self.data_dir = catalog, authority, data_dir
        self.bots: dict[str, ManagedBot] = {}
        self.errors: dict[str, str] = {}
        self.packages: dict[str, PackageSnapshot] = {}
        self.package_metadata: dict[str, dict] = {}
        self.artifacts = PackageCatalog(data_dir / "artifacts")
        self.registry = PackageRegistry(catalog, self.artifacts)
        self.package_errors: dict[str, str] = {}
        self.infrastructure_packages: dict[str, PackageSnapshot] = {}
        self.group_slots = asyncio.Semaphore(32)
        self.model_slots = asyncio.Semaphore(32)
        self._closed = False
        self.max_concurrent_attaches, self.max_bots = max_concurrent_attaches, max_bots
        self.attach_timeout, self.reconcile_interval = attach_timeout, reconcile_interval
        self.retry_base, self.retry_max = retry_base, retry_max
        self._attach_slots = asyncio.Semaphore(max_concurrent_attaches)
        self._attach_locks: dict[str, asyncio.Lock] = {}
        self._attach_jobs: dict[str, asyncio.Task] = {}
        self._attach_retries: dict[str, tuple[int, float]] = {}
        self._attaching: set[asyncio.Task] = set()
        self._close_task: asyncio.Task | None = None

    @staticmethod
    def _code(error: Exception, default="BOT_UNAVAILABLE") -> str:
        return error.code if isinstance(error, ForestError) else default

    @staticmethod
    def _usable(bot: ManagedBot) -> bool:
        if not bot.ready or bot.cleanup_task is not None:
            return False
        if bot.platform is not None and (
            getattr(bot.platform, "failure", None) or any(task.done() for task in bot.platform._tasks)
        ):
            return False
        try:
            lease = bot.kernel._authority()
            return lease.expires_at > datetime.now(timezone.utc)
        except ForestError:
            return False

    def _delay(self, failures: int) -> float:
        return min(self.retry_max, self.retry_base * 2 ** min(failures - 1, 20))

    async def _dispose(self, bot: ManagedBot) -> None:
        bot.ready = False
        if bot.task and bot.task is not asyncio.current_task():
            bot.task.cancel()
            await asyncio.gather(bot.task, return_exceptions=True)
        if bot.platform is not None:
            await bot.platform.close()
        await bot.kernel.close()
        if bot.kernel._workers:
            raise ForestError("WORKER_CLEANUP_FAILED", "Old Worker resources remain owned", 503)
        await bot.store.close()

    def _start_cleanup(self, bot: ManagedBot) -> asyncio.Task:
        if bot.cleanup_task is None or (
            bot.cleanup_task.done()
            and (bot.cleanup_task.cancelled() or bot.cleanup_task.exception())
        ):
            bot.cleanup_task = asyncio.create_task(
                self._dispose(bot), name=f"bot-dispose-{bot.store.bot_id}"
            )
            # A request timeout may stop awaiting cleanup; still observe failures,
            # retain the owner, and refuse a replacement until cleanup succeeds.
            bot.cleanup_task.add_done_callback(
                lambda task: None if task.cancelled() else task.exception()
            )
        return bot.cleanup_task

    async def _cleanup(self, bot: ManagedBot) -> None:
        await asyncio.shield(self._start_cleanup(bot))

    async def initialize(self) -> None:
        snapshot = await asyncio.to_thread(
            lambda: PackageCatalog(self.data_dir / "artifacts").build(
                Path(__file__).resolve().parents[1] / "builtin" / "counter"
            )
        )
        self.packages[snapshot.digest] = snapshot
        self.package_metadata[snapshot.digest] = await asyncio.to_thread(self.describe_package, snapshot)
        for name in ("onebot", "reply"):
            package = await asyncio.to_thread(
                PackageCatalog(self.data_dir / "artifacts").build,
                Path(__file__).resolve().parents[1] / "builtin" / name,
            )
            self.infrastructure_packages[package.component["id"]] = package
        for digest in await self.registry.registered():
            try:
                package = await asyncio.to_thread(self.artifacts.get, digest)
                self.package_metadata[digest] = await asyncio.to_thread(self.describe_package, package)
                self.packages[digest] = package
            except (ForestError, OSError) as error:
                self.package_errors[digest] = self._code(error, "PACKAGE_UNAVAILABLE")

    @staticmethod
    def describe_package(p: PackageSnapshot) -> dict:
        return {
                "package_digest": p.digest,
                "package_id": p.manifest["package_id"],
                "version": p.manifest["version"],
                "component_id": p.component["id"],
                "state_schema": p.component["state_schema"],
                "state_migrations": p.component.get("state_migrations", []),
                "config_schema": p.schema(p.component["config_schema"]),
                "grants": p.component.get("capabilities", {}).get("requests", []),
                "services": [
                    {"name": s["name"], "input_schema": p.schema(s["input_schema"])}
                    for s in p.component["provides"]
                ],
            }

    def available_packages(self) -> list[dict]:
        return copy.deepcopy(list(self.package_metadata.values()))

    def validate(self, component_id: str, values: dict) -> None:
        validate_component_values(self.packages, component_id, values)

    async def _resolve_storage(self, bot_id: str) -> dict:
        resolver = getattr(self.catalog, "resolve_storage", None)
        if resolver is not None:
            return await resolver(bot_id)
        return {"dsn": await self.catalog.resolve_dsn(bot_id), "storage_generation": 1}

    async def attach(self, bot_id: str, expected_generation: int | None = None) -> ManagedBot:
        if not isinstance(bot_id, str) or not 1 <= len(bot_id) <= 128:
            raise ForestError("INVALID_BOT", "A bounded Bot identity is required", 422)
        if bot_id not in self._attach_locks:
            if len(self._attach_locks) >= self.max_bots:
                raise ForestError("FLEET_CAPACITY", "Bot supervisor capacity is exhausted", 429)
            self._attach_locks[bot_id] = asyncio.Lock()
        task = asyncio.current_task()
        self._attaching.add(task)
        try:
            async with asyncio.timeout(self.attach_timeout), self._attach_locks[bot_id]:
                if self._closed:
                    raise ForestError("RUNTIME_STOPPING", "Bot supervisor is stopping", 503)
                # Request paths may arrive after the directory head changed but
                # before the background roster poll. Resolve the head before
                # reusing an apparently healthy in-memory owner; the old database
                # fence is authoritative, but serving its stale host is needless.
                pinned_generation = expected_generation is not None
                if not pinned_generation:
                    async with self._attach_slots:
                        current = await self._resolve_storage(bot_id)
                    expected_generation = current["storage_generation"]
                existing = self.bots.get(bot_id)
                if (
                    existing is not None
                    and self._usable(existing)
                    and (expected_generation is None
                         or existing.directory_generation == expected_generation)
                ):
                    if existing.task is None or existing.task.done():
                        existing.task = asyncio.create_task(
                            self._reconcile(existing), name=f"bot-config-{bot_id}"
                        )
                    return existing
                async with self._attach_slots:
                    if existing is not None:
                        await self._cleanup(existing)
                        self.bots.pop(bot_id, None)
                    # Re-read after any potentially long cleanup. Unpinned request
                    # paths follow the newest monotonic head; roster-driven attaches
                    # must still match the generation they were scheduled for.
                    storage = await self._resolve_storage(bot_id)
                    if (
                        pinned_generation
                        and storage["storage_generation"] != expected_generation
                    ):
                        raise ForestError(
                            "STORAGE_HEAD_CHANGED", "Bot storage generation changed", 409,
                            details={"current_generation": storage["storage_generation"]},
                        )
                    store = BotStore(storage["dsn"], bot_id)
                    kernel = Kernel(store, host_id="web-fleet")
                    managed = ManagedBot(
                        store, kernel, BotControl(store),
                        directory_generation=storage["storage_generation"],
                    )
                    self.bots[bot_id] = managed
                    try:
                        await store.open()
                        await store.migrate()
                        await kernel.start()
                        await managed.control.trust(
                            kernel._authority(),
                            issuer_id=self.authority.issuer_id,
                            key_revision=self.authority.key_revision,
                            public_key=self.authority.public_key,
                        )
                        managed.platform = BindingRuntime(
                            managed, self.catalog.vault, self.infrastructure_packages,
                            global_slots=self.group_slots,
                            job_authority=self.authority,
                            global_model_slots=self.model_slots,
                            module_packages=self.packages,
                        )
                        await managed.platform.start()
                        if self._closed:
                            raise ForestError("RUNTIME_STOPPING", "Bot supervisor is stopping", 503)
                    except BaseException:
                        self._start_cleanup(managed)
                        if asyncio.current_task().cancelling():
                            # The expired request must not hold an attach slot
                            # while cleanup continues under its retained owner.
                            raise
                        await self._cleanup(managed)
                        if self.bots.get(bot_id) is managed:
                            self.bots.pop(bot_id, None)
                        raise
                    managed.ready = True
                    managed.task = asyncio.create_task(
                        self._reconcile(managed), name=f"bot-config-{bot_id}"
                    )
                    self.errors.pop(bot_id, None)
                    self._attach_retries.pop(bot_id, None)
                    return managed
        except TimeoutError:
            raise ForestError(
                "BOT_ATTACH_TIMEOUT", "Bot attachment exceeded its deadline", 503
            ) from None
        finally:
            self._attaching.discard(task)

    async def _reconcile_once(self, bot: ManagedBot) -> None:
        if not hasattr(bot, "applied_states"):
            bot.applied_states = {}
        desired = await bot.control.desired(bot.kernel._authority())
        for revision in desired:
            revision.setdefault("values_payload_state", "ACTIVE")
            revision.setdefault("effective_values", None)
            revision.setdefault("effective_values_payload_state", "UNCONFIGURED")
            revision.setdefault("effective_revision", 0)
            revision.setdefault("desired_state_revision", 0)
            revision.setdefault("effective_state_revision", 0)
            revision.setdefault("desired_enabled", True)
            revision.setdefault("effective_enabled", True)
        by_component = {row["component_id"]: row for row in desired}
        snapshots = {}
        synthetic_order = []
        graph_complete = True

        def add_to_plan(component, values):
            self.validate(component, values)
            snapshot = self.packages[values["package_digest"]]
            if isinstance(snapshot, PackageSnapshot):
                snapshots[component] = snapshot
            elif component not in synthetic_order:
                synthetic_order.append(component)

        for revision in desired:
            component = revision["component_id"]
            if revision["desired_enabled"]:
                try:
                    if revision.get("values_payload_state") == "UNAVAILABLE":
                        raise ForestError(
                            "CONTROL_CONTENT_REVOKED", "Desired module content is unavailable", 409
                        )
                    add_to_plan(component, revision["values"])
                except Exception as exc:
                    graph_complete = False
                    bot.errors[component] = self._code(exc, "VALIDATION_FAILED")
                    effective_values = revision.get("effective_values")
                    if effective_values is not None:
                        try:
                            add_to_plan(component, effective_values)
                        except Exception:
                            if component not in synthetic_order:
                                synthetic_order.append(component)
                    elif component not in synthetic_order:
                        synthetic_order.append(component)
                continue

            if not revision["effective_enabled"]:
                if component not in synthetic_order:
                    synthetic_order.append(component)
                continue
            effective_values = revision.get("effective_values")
            if effective_values is not None:
                try:
                    add_to_plan(component, effective_values)
                    continue
                except Exception:
                    pass
            if component not in synthetic_order:
                synthetic_order.append(component)
        try:
            dependency_plan = resolve_dependencies(snapshots)
            if synthetic_order:
                dependency_plan = DependencyPlan(
                    order=dependency_plan.order + synthetic_order,
                    bindings={**dependency_plan.bindings, **{key: {} for key in synthetic_order}},
                    closures={**dependency_plan.closures, **{key: () for key in synthetic_order}},
                    degraded={**dependency_plan.degraded, **{key: [] for key in synthetic_order}},
                )
        except Exception as exc:
            code = self._code(exc, "DEPENDENCY_INVALID")
            for component in snapshots:
                bot.errors[component] = code
            self.errors.pop(bot.store.bot_id, None)
            return

        enabled = {
            component for component, revision in by_component.items()
            if revision["desired_enabled"]
        }
        from forest.storage.module_administration import ModuleAdministration

        module = ModuleAdministration(bot.store, control=bot.control, packages=self.packages)
        for component in reversed(dependency_plan.order):
            revision = by_component[component]
            if revision["desired_enabled"]:
                continue
            state_number = revision["desired_state_revision"]
            if (
                revision["effective_state_revision"] == state_number
                and not revision["effective_enabled"]
                and bot.kernel.components.get(component) is None
            ):
                bot.applied_states[component] = state_number
                bot.errors.pop(component, None)
                continue
            if not graph_complete:
                bot.errors[component] = "DEPENDENCY_GRAPH_INCOMPLETE"
                continue
            blockers = sorted(
                caller for caller in enabled
                if caller != component
                and component in dependency_plan.closures.get(caller, ())
            )
            if blockers:
                bot.errors[component] = "DEPENDENCY_IN_USE"
                continue
            token = (revision["revision"], state_number, False)
            previous = bot.retries.get(component)
            now = asyncio.get_running_loop().time()
            if previous and previous[0] == token and now < previous[2]:
                continue
            try:
                operation_id = "module-disable:" + str(uuid4())
                await module.bind_activation(
                    bot.kernel._authority(), component, state_number, operation_id,
                    enabled=False,
                )
                await bot.kernel.disable(component, operation_id=operation_id)
                bot.applied.pop(component, None)
                bot.applied_states[component] = state_number
                bot.errors.pop(component, None)
                bot.retries.pop(component, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                bot.errors[component] = self._code(
                    exc,
                    "MODULE_STATE_SUPERSEDED"
                    if isinstance(exc, RaiseException) else "DISABLE_FAILED",
                )
                failures = previous[1] + 1 if previous and previous[0] == token else 1
                bot.retries[component] = (
                    token, failures,
                    asyncio.get_running_loop().time() + self._delay(failures),
                )

        for component in dependency_plan.order:
            revision = by_component[component]
            if not revision["desired_enabled"]:
                continue
            component, number = revision["component_id"], revision["revision"]
            state_number = revision["desired_state_revision"]
            loaded = bot.kernel.components.get(component)
            if (
                (loaded is None or not loaded.worker.alive)
                and revision.get("effective_values")
                and revision["effective_revision"] != number
                and revision["effective_enabled"]
                and revision["effective_state_revision"] == state_number
            ):
                try:
                    await self._activate(bot, component, revision["effective_revision"],
                                         revision["effective_values"], state_number,
                                         recover_effective=True)
                    loaded = bot.kernel.components.get(component)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    bot.errors[component] = self._code(exc, "RECOVERY_FAILED")
            if revision.get("values_payload_state") == "UNAVAILABLE":
                bot.errors[component] = "CONTROL_CONTENT_REVOKED"
                continue
            if (
                bot.applied.get(component) == number
                and bot.applied_states.get(component, 0) == state_number
                and loaded is not None
                and loaded.worker.alive
                and loaded.worker.state == "ACTIVE"
            ):
                continue
            token = (number, state_number, True)
            previous = bot.retries.get(component)
            now = asyncio.get_running_loop().time()
            if previous and previous[0] == token and now < previous[2]:
                continue
            try:
                values = revision["values"]
                await self._activate(bot, component, number, values, state_number)
                bot.errors.pop(component, None)
                bot.retries.pop(component, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                bot.errors[component] = self._code(
                    exc,
                    "CONFIGURATION_SUPERSEDED"
                    if isinstance(exc, RaiseException)
                    else "ACTIVATION_FAILED",
                )
                failures = previous[1] + 1 if previous and previous[0] == token else 1
                bot.retries[component] = (
                    token,
                    failures,
                    asyncio.get_running_loop().time() + self._delay(failures),
                )
        self.errors.pop(bot.store.bot_id, None)

    async def _activate(
        self, bot, component, revision, values, state_revision, *, recover_effective=False
    ):
        self.validate(component, values)
        operation_id = "config-activate:" + str(uuid4())
        await bot.control.bind_activation(
            bot.kernel._authority(), component, revision, operation_id,
            **({"recover_effective": True} if recover_effective else {}),
        )
        from forest.storage.module_administration import ModuleAdministration

        await ModuleAdministration(bot.store).bind_activation(
            bot.kernel._authority(), component, state_revision, operation_id, enabled=True
        )
        await bot.kernel.install(self.packages[values["package_digest"]],
            config=values["config"], grants=frozenset(values["grants"]), operation_id=operation_id)
        bot.applied[component] = revision
        bot.applied_states[component] = state_revision

    async def _reconcile(self, bot: ManagedBot) -> None:
        while not self._closed:
            try:
                await self._reconcile_once(bot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors[bot.store.bot_id] = (
                    exc.code if isinstance(exc, ForestError) else "BOT_UNAVAILABLE"
                )
            await asyncio.sleep(self.reconcile_interval)

    async def _attach_background(self, bot_id: str, expected_generation: int | None = None) -> None:
        try:
            await self.attach(bot_id, expected_generation)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.errors[bot_id] = self._code(exc)
            failures = self._attach_retries.get(bot_id, (0, 0))[0] + 1
            self._attach_retries[bot_id] = (
                failures,
                asyncio.get_running_loop().time() + self._delay(failures),
            )

    async def run_forever(self) -> None:
        while not self._closed:
            try:
                rows = await self.catalog.list_ready()
                for key, task in list(self._attach_jobs.items()):
                    if task.done():
                        if not task.cancelled():
                            task.exception()
                        self._attach_jobs.pop(key, None)
                for row in rows:
                    bot_id = row["bot_id"]
                    if (
                        bot_id not in self._attach_locks
                        and len(self._attach_locks) >= self.max_bots
                    ):
                        continue
                    existing = self.bots.get(bot_id)
                    expected_generation = row.get("storage_generation", 1)
                    if (
                        existing is not None
                        and self._usable(existing)
                        and existing.directory_generation == expected_generation
                        and existing.task
                        and not existing.task.done()
                    ):
                        continue
                    if (
                        bot_id in self._attach_jobs
                        or len(self._attach_jobs) >= self.max_concurrent_attaches
                        or asyncio.get_running_loop().time()
                        < self._attach_retries.get(bot_id, (0, 0))[1]
                    ):
                        continue
                    self._attach_jobs[bot_id] = asyncio.create_task(
                        self._attach_background(bot_id, expected_generation),
                        name=f"bot-attach-{bot_id}"
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Directory outage must not stop independently leased live Bots.
                pass
            await asyncio.sleep(self.reconcile_interval)

    def status(self, bot_id: str) -> dict:
        bot = self.bots.get(bot_id)
        attached = bot is not None and self._usable(bot)
        return {
            "attached": attached,
            "storage_generation": bot.directory_generation if bot is not None else None,
            "error_code": self.errors.get(bot_id)
            or ("BOT_RECOVERING" if bot is not None and not attached else None),
            "components": []
            if bot is None
            else [
                {
                    "component_id": key,
                    "loaded_revision": bot.applied.get(key, 0),
                    "error_code": bot.errors.get(key),
                    "worker_state": bot.kernel.components[key].worker.state
                    if key in bot.kernel.components
                    else None,
                }
                for key in sorted(set(bot.applied) | set(bot.errors))
            ],
        }

    async def close(self) -> None:
        if self._close_task is None or (self._close_task.done() and self._close_task.exception()):
            self._close_task = asyncio.create_task(self._close(), name="bot-fleet-close")
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        self._closed = True
        attaching = set(self._attaching) | set(self._attach_jobs.values())
        for task in attaching:
            task.cancel()
        await asyncio.gather(*attaching, return_exceptions=True)
        self._attach_jobs.clear()
        retained = list(self.bots.items())
        results = await asyncio.gather(
            *(self._cleanup(bot) for _, bot in retained), return_exceptions=True
        )
        for (key, bot), result in zip(retained, results):
            if isinstance(result, BaseException):
                self.errors[key] = "BOT_CLEANUP_FAILED"
            elif self.bots.get(key) is bot:
                self.bots.pop(key, None)
        if self.bots:
            raise ForestError(
                "BOT_CLEANUP_FAILED", "Some Bot resources remain owned for cleanup", 503
            )
