"""Dependency-ordered official modules for a Bot runtime.

The supervisor consumes descriptors and lifecycle operations; concrete module
construction lives here instead of in the platform adapter composition root.
Official and packaged modules can converge on this registry contract without
making Agent or Memory kernel types.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import StorageBundleSpec, validate_storage_baseline


@dataclass(frozen=True)
class RuntimeManagementView:
    """Trusted management assets owned by an official runtime module."""

    label: str
    template: str
    module: str
    protocol: str = "forest.trusted-management-view/1"
    style: str | None = None

    def __post_init__(self) -> None:
        if not self.label.strip() or any(ord(char) < 32 for char in self.label):
            raise ValueError("Runtime management view labels must be printable")
        for value, suffix in ((self.template, ".html"), (self.module, ".js")):
            if (
                not value.startswith("/static/")
                or not value.endswith(suffix)
                or ".." in value.split("/")
                or "\\" in value
            ):
                raise ValueError("Runtime management assets must use canonical /static paths")
        if self.protocol != "forest.trusted-management-view/1":
            raise ValueError("Unsupported runtime management view protocol")
        if self.style is not None and (
            not self.style.startswith("/static/")
            or not self.style.endswith(".css")
            or ".." in self.style.split("/")
            or "\\" in self.style
        ):
            raise ValueError("Runtime management styles must use canonical /static paths")


@dataclass(frozen=True)
class RuntimeStorageBundle:
    """Verified immutable SQL owned by one runtime module generation."""

    spec: StorageBundleSpec
    baseline: bytes
    package_digest: str

    def __post_init__(self) -> None:
        validate_storage_baseline(self.baseline, self.spec.baseline_sha256)
        if not isinstance(self.package_digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", self.package_digest
        ):
            raise ForestError(
                "INVALID_STORAGE_BUNDLE", "Storage package digest is invalid", 422
            )

    def as_fact(self) -> dict:
        return {**self.spec.as_fact(), "package_digest": self.package_digest}


@dataclass(frozen=True)
class RuntimeModuleDescriptor:
    component_id: str
    requires: tuple[str, ...]
    factory: Callable[[Any, dict[str, Any]], Any]
    management_view: RuntimeManagementView | None = None
    storage: RuntimeStorageBundle | None = None


class ModuleLifecycleState(StrEnum):
    DISCOVERED = "DISCOVERED"
    VALIDATED = "VALIDATED"
    PREPARED = "PREPARED"
    ACTIVE = "ACTIVE"
    QUIESCING = "QUIESCING"
    QUIESCENT = "QUIESCENT"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


_TRANSITIONS = {
    ModuleLifecycleState.DISCOVERED: {ModuleLifecycleState.VALIDATED},
    ModuleLifecycleState.VALIDATED: {
        ModuleLifecycleState.PREPARED,
        ModuleLifecycleState.STOPPED,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.PREPARED: {
        ModuleLifecycleState.ACTIVE,
        ModuleLifecycleState.STOPPED,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.ACTIVE: {
        ModuleLifecycleState.QUIESCING,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.QUIESCING: {
        ModuleLifecycleState.QUIESCENT,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.QUIESCENT: {
        ModuleLifecycleState.STOPPED,
        ModuleLifecycleState.FAILED,
    },
    ModuleLifecycleState.FAILED: {ModuleLifecycleState.STOPPED},
    ModuleLifecycleState.STOPPED: set(),
}


def _memory(platform, modules: dict[str, Any]):
    from forest.management.memory_api import MemoryManagementContract
    from forest.memory_runtime import MemoryRuntime

    adapter = modules["adapter.onebot"]
    runtime = MemoryRuntime(platform, messages=adapter.messages)
    runtime.management_contract = MemoryManagementContract()
    return runtime


def _agent(platform, modules: dict[str, Any]):
    from forest.agent_runtime import AgentRuntime
    from forest.management.agent_api import AgentManagementContract

    runtime = AgentRuntime(platform, messages=modules["adapter.onebot"].messages)
    runtime.management_contract = AgentManagementContract()
    return runtime


def _conversation(platform, modules: dict[str, Any]):
    from forest.group_scheduler import GroupScheduler

    agent = modules["agent.runtime"]
    adapter = modules["adapter.onebot"]
    return GroupScheduler(
        adapter.messages,
        lease=platform.kernel._authority,
        execute=platform.execute,
        delivery=adapter.delivery,
        global_slots=platform.global_slots,
        finalize_agent=agent.runner.finalize,
        execution_timeout=3600,
    )


class _CommandRoutingRuntime:
    def __init__(self, platform):
        from forest.storage.command_limits import CommandLimitStore
        from forest.storage.command_policies import CommandPolicyStore

        self.policies = CommandPolicyStore(platform.bot.store)
        self.limits = CommandLimitStore(platform.bot.store)


class _MessageRoutingRuntime:
    def __init__(self, platform):
        from forest.storage.message_handler_limits import MessageHandlerLimitStore
        from forest.storage.message_handler_policies import MessageHandlerPolicyStore

        self.policies = MessageHandlerPolicyStore(platform.bot.store)
        self.limits = MessageHandlerLimitStore(platform.bot.store)


def _command_routing(platform, _: dict[str, Any]):
    return _CommandRoutingRuntime(platform)


def _message_routing(platform, _: dict[str, Any]):
    return _MessageRoutingRuntime(platform)


def _onebot(platform, _: dict[str, Any]):
    from forest.onebot_runtime import OneBotRuntime

    return OneBotRuntime(platform)


def _routing_graph(platform, _: dict[str, Any]):
    from forest.management.routing_api import RoutingManagementContract
    from forest.routing_graph import RouteGraphRuntime

    runtime = RouteGraphRuntime(platform)
    runtime.management_contract = RoutingManagementContract()
    return runtime


def _shell(platform, modules: dict[str, Any]):
    from forest.shell import ShellDispatcher

    adapter = modules["adapter.onebot"]
    return ShellDispatcher(
        adapter.messages,
        adapter.bindings,
        adapter.delivery,
        platform.kernel._authority,
        on_change=platform._notify_work,
        module_packages=platform.module_packages,
    )


def _jobs(platform, modules: dict[str, Any]):
    from forest.job_scheduler import JobDispatcher
    from forest.storage.jobs import JobStore

    store = JobStore(modules["adapter.onebot"].messages)
    scheduler = modules["conversation.scheduler"]
    return JobDispatcher(
        store,
        platform.kernel._authority,
        authorize=platform._authorize_job if platform.job_authority is not None else None,
        on_change=scheduler.notify,
    )


OFFICIAL_MODULES = (
    RuntimeModuleDescriptor("adapter.onebot", (), _onebot),
    RuntimeModuleDescriptor(
        "routing.graph",
        (),
        _routing_graph,
        RuntimeManagementView(
            "路由与执行图",
            "/static/modules/routing.graph.html",
            "/static/routing-graph.js",
            style="/static/modules/routing.graph.css",
        ),
    ),
    RuntimeModuleDescriptor(
        "memory.graph",
        ("adapter.onebot",),
        _memory,
        RuntimeManagementView(
            "图文记忆", "/static/modules/memory.graph.html", "/static/memory.js",
            style="/static/modules/memory.graph.css",
        ),
    ),
    RuntimeModuleDescriptor(
        "agent.runtime",
        ("adapter.onebot", "memory.graph"),
        _agent,
        RuntimeManagementView(
            "Agent 与模型", "/static/modules/agent.runtime.html", "/static/agent-panel.js",
            style="/static/modules/agent.runtime.css",
        ),
    ),
    RuntimeModuleDescriptor("command.router", ("routing.graph",), _command_routing),
    RuntimeModuleDescriptor("message.router", ("routing.graph",), _message_routing),
    RuntimeModuleDescriptor(
        "conversation.scheduler",
        ("adapter.onebot", "agent.runtime", "command.router", "message.router"),
        _conversation,
    ),
    RuntimeModuleDescriptor("shell.command", ("adapter.onebot",), _shell),
    RuntimeModuleDescriptor(
        "scheduler.jobs", ("adapter.onebot", "conversation.scheduler"), _jobs
    ),
)


class BotModuleRegistry:
    """Build and supervise modules from an acyclic descriptor graph."""

    def __init__(self, platform, descriptors=OFFICIAL_MODULES):
        self.platform = platform
        self.descriptors = tuple(descriptors)
        self.instances: dict[str, Any] = {}
        self.order: list[str] = []
        self.states: dict[str, ModuleLifecycleState] = {
            descriptor.component_id: ModuleLifecycleState.DISCOVERED
            for descriptor in self.descriptors
        }
        self._lifecycle_lock = None
        self._closed = False
        self.graph_identity = None
        store = getattr(getattr(platform, "bot", None), "store", None)
        if store is None:
            self.graph_store = None
        else:
            from forest.storage.runtime_graph import RuntimeModuleGraphStore

            self.graph_store = RuntimeModuleGraphStore(store)
        self._build()

    def _descriptor_facts(self) -> list[dict]:
        facts = []
        for descriptor in self.descriptors:
            view = descriptor.management_view
            contract = getattr(
                self.instances.get(descriptor.component_id), "management_contract", None
            )
            fact = {
                "protocol": "forest.runtime-module/1",
                "component_id": descriptor.component_id,
                "requires": list(descriptor.requires),
                "factory_ref": (
                    descriptor.factory.__module__ + ":" + descriptor.factory.__qualname__
                ),
                "management_view": (
                    {
                        "label": view.label,
                        "template": view.template,
                        "module": view.module,
                        "protocol": view.protocol,
                        "style": view.style,
                    }
                    if view is not None
                    else None
                ),
                "management_contract": contract.describe() if contract is not None else None,
                "storage": (
                    descriptor.storage.as_fact()
                    if descriptor.storage is not None
                    else None
                ),
            }
            facts.append({**fact, "descriptor_digest": digest_json(fact)})
        return facts

    def _lease(self, *, allow_closing=False):
        if allow_closing and hasattr(self.platform, "_cleanup_lease"):
            return self.platform._cleanup_lease()
        return self.platform.kernel._authority(allow_closing=allow_closing)

    @staticmethod
    def _lost_persistence_authority(exc):
        return isinstance(exc, ForestError) and exc.code in {
            "STALE_HOST",
            "KERNEL_UNAVAILABLE",
            "STORAGE_SEALED",
        }

    async def _record_current_states(self, lease, *, failure_code=None):
        if self.graph_store is None or self.graph_identity is None:
            return
        states = {}
        for component_id, state in self.states.items():
            if state == ModuleLifecycleState.FAILED:
                states[component_id] = ("FAILED", failure_code or "MODULE_LIFECYCLE_FAILED")
            elif state == ModuleLifecycleState.STOPPED:
                states[component_id] = ("STOPPED", None)
            elif state in {ModuleLifecycleState.QUIESCING, ModuleLifecycleState.QUIESCENT}:
                states[component_id] = ("QUIESCING", None)
            elif state == ModuleLifecycleState.ACTIVE:
                states[component_id] = ("ACTIVE", None)
            else:
                states[component_id] = ("PREPARED", None)
        await self.graph_store.record_states(lease, self.graph_identity, states)

    def _transition(self, component_id, state):
        current = self.states[component_id]
        if state not in _TRANSITIONS[current]:
            raise ForestError(
                "RUNTIME_MODULE_LIFECYCLE_INVALID",
                f"Module {component_id} cannot move from {current} to {state}",
            )
        self.states[component_id] = state

    def _build(self) -> None:
        remaining = {item.component_id: item for item in self.descriptors}
        if len(remaining) != len(self.descriptors):
            raise ForestError("DUPLICATE_RUNTIME_MODULE", "Runtime module identities must be unique")
        for descriptor in self.descriptors:
            identity = descriptor.component_id
            if (
                not identity
                or len(identity) > 128
                or identity[0] not in "abcdefghijklmnopqrstuvwxyz0123456789"
                or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for char in identity)
                or len(set(descriptor.requires)) != len(descriptor.requires)
                or identity in descriptor.requires
                or (
                    descriptor.storage is not None
                    and descriptor.storage.spec.component_id != identity
                )
            ):
                raise ForestError(
                    "RUNTIME_MODULE_DESCRIPTOR_INVALID", "Runtime module descriptor is invalid"
                )
            self._transition(identity, ModuleLifecycleState.VALIDATED)
        resolved = set()
        while remaining:
            ready = sorted(
                (item for item in remaining.values()
                 if all(required in resolved for required in item.requires)),
                key=lambda item: item.component_id,
            )
            if not ready:
                raise ForestError(
                    "RUNTIME_MODULE_DEPENDENCY_CYCLE",
                    "Runtime module dependencies are missing or cyclic",
                )
            for descriptor in ready:
                self.order.append(descriptor.component_id)
                resolved.add(descriptor.component_id)
                remaining.pop(descriptor.component_id)

    async def _install_storage(self, descriptor: RuntimeModuleDescriptor) -> None:
        if descriptor.storage is None:
            return
        installer = getattr(self.platform, "schema_bundle_installer", None)
        install = getattr(installer, "install", None)
        if install is None:
            raise ForestError(
                "SCHEMA_BUNDLE_INSTALLER_UNAVAILABLE",
                "A storage-owning runtime module requires the host schema installer",
                503,
            )
        await install(
            descriptor.storage.spec,
            descriptor.storage.baseline,
            descriptor.storage.package_digest,
        )

    async def _prepare_modules(self) -> None:
        for component_id in self.order:
            if self.states[component_id] == ModuleLifecycleState.PREPARED:
                continue
            if self.states[component_id] != ModuleLifecycleState.VALIDATED:
                raise ForestError(
                    "RUNTIME_MODULE_LIFECYCLE_INVALID",
                    "Runtime module graph cannot be prepared from its current state",
                )
            descriptor = next(
                item for item in self.descriptors if item.component_id == component_id
            )
            try:
                await self._install_storage(descriptor)
                instance = descriptor.factory(self.platform, self.instances)
                self.instances[component_id] = instance
                self._transition(component_id, ModuleLifecycleState.PREPARED)
            except BaseException:
                self._transition(component_id, ModuleLifecycleState.FAILED)
                raise

    async def _rollback_preparation(self) -> list[Exception]:
        errors = []
        for component_id in reversed(self.order):
            state = self.states[component_id]
            if state == ModuleLifecycleState.PREPARED:
                try:
                    await self._cleanup(component_id)
                except Exception as exc:
                    errors.append(exc)
            elif state == ModuleLifecycleState.FAILED and component_id not in self.instances:
                self._transition(component_id, ModuleLifecycleState.STOPPED)
            elif state == ModuleLifecycleState.VALIDATED:
                self._transition(component_id, ModuleLifecycleState.STOPPED)
        return errors

    def get(self, component_id: str):
        return self.instances[component_id]

    async def start(self) -> None:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                raise ForestError(
                    "RUNTIME_MODULE_REGISTRY_CLOSED", "Runtime module registry is closed", 503
                )
            if all(self.states[item] == ModuleLifecycleState.ACTIVE for item in self.order):
                return
            lease = None
            try:
                await self._prepare_modules()
                if self.graph_store is not None:
                    lease = self._lease()
                    if self.graph_identity is None:
                        self.graph_identity = await self.graph_store.prepare(
                            lease, self._descriptor_facts()
                        )
            except BaseException as exc:
                rollback_errors = await self._rollback_preparation()
                if rollback_errors:
                    raise ForestError(
                        "RUNTIME_MODULE_PREPARE_ROLLBACK_FAILED",
                        "Runtime module preparation failed and resources remain owned",
                        503,
                        details={"cleanup_errors": len(rollback_errors)},
                    ) from exc
                raise
            started = []
            current = None
            try:
                for component_id in self.order:
                    current = component_id
                    if self.states[component_id] != ModuleLifecycleState.PREPARED:
                        raise ForestError(
                            "RUNTIME_MODULE_LIFECYCLE_INVALID",
                            "Runtime module graph is not prepared for activation",
                        )
                    instance = self.instances[component_id]
                    start = getattr(instance, "start", None)
                    if start is not None:
                        await start()
                    self._transition(component_id, ModuleLifecycleState.ACTIVE)
                    started.append(component_id)
                if self.graph_store is not None:
                    await self.graph_store.publish(lease, self.graph_identity)
            except BaseException as exc:
                failure_code = (
                    exc.code if isinstance(exc, ForestError) else "RUNTIME_MODULE_START_FAILED"
                )
                rollback_errors = []
                if (
                    self.graph_store is not None
                    and current is not None
                    and self.states[current] in {
                        ModuleLifecycleState.PREPARED,
                        ModuleLifecycleState.ACTIVE,
                    }
                ):
                    try:
                        await self.graph_store.record_states(
                            lease,
                            self.graph_identity,
                            {current: ("FAILED", failure_code)},
                        )
                    except Exception as rollback_error:
                        rollback_errors.append(rollback_error)
                if current is not None and self.states[current] == ModuleLifecycleState.PREPARED:
                    self._transition(current, ModuleLifecycleState.FAILED)
                    try:
                        await self._cleanup(current)
                    except Exception as rollback_error:
                        rollback_errors.append(rollback_error)
                for component_id in reversed(started):
                    try:
                        await self._quiesce_and_cleanup(component_id)
                    except Exception as rollback_error:
                        rollback_errors.append(rollback_error)
                try:
                    await self._record_current_states(lease, failure_code=failure_code)
                except Exception as rollback_error:
                    rollback_errors.append(rollback_error)
                if rollback_errors:
                    raise ForestError(
                        "RUNTIME_MODULE_START_ROLLBACK_FAILED",
                        "Runtime module startup failed and resources remain owned",
                        503,
                        details={"start_error": failure_code, "cleanup_errors": len(rollback_errors)},
                    ) from exc
                raise

    async def close(self) -> list[Exception]:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                return []
            lease = None
            if self.graph_store is not None and self.graph_identity is not None:
                try:
                    lease = self._lease(allow_closing=True)
                    await self.graph_store.retire(lease, self.graph_identity)
                except ForestError as exc:
                    if not self._lost_persistence_authority(exc):
                        raise
                    # A fenced owner must still release process-local resources.
                    # Its persistent graph remains historical until the current
                    # owner atomically replaces or retires it.
                    lease = None
            errors = []
            for component_id in reversed(self.order):
                try:
                    state = self.states[component_id]
                    if state == ModuleLifecycleState.ACTIVE:
                        await self._quiesce_and_cleanup(component_id)
                    elif state in {
                        ModuleLifecycleState.PREPARED,
                        ModuleLifecycleState.FAILED,
                        ModuleLifecycleState.QUIESCENT,
                    }:
                        if component_id in self.instances:
                            await self._cleanup(component_id)
                        else:
                            self._transition(component_id, ModuleLifecycleState.STOPPED)
                    elif state == ModuleLifecycleState.VALIDATED:
                        self._transition(component_id, ModuleLifecycleState.STOPPED)
                except Exception as exc:
                    errors.append(exc)
            if lease is not None:
                failure_code = (
                    errors[0].code
                    if errors and isinstance(errors[0], ForestError)
                    else ("RUNTIME_MODULE_CLEANUP_FAILED" if errors else None)
                )
                await self._record_current_states(lease, failure_code=failure_code)
            self._closed = not errors and all(
                state == ModuleLifecycleState.STOPPED for state in self.states.values()
            )
            return errors

    async def _quiesce_and_cleanup(self, component_id):
        self._transition(component_id, ModuleLifecycleState.QUIESCING)
        quiesce = getattr(self.instances[component_id], "quiesce", None)
        if quiesce is not None:
            try:
                await quiesce()
            except BaseException:
                self._transition(component_id, ModuleLifecycleState.FAILED)
                await self._cleanup(component_id)
                raise
        self._transition(component_id, ModuleLifecycleState.QUIESCENT)
        await self._cleanup(component_id)

    async def _cleanup(self, component_id):
        close = getattr(self.instances[component_id], "close", None)
        try:
            if close is not None:
                await close()
        except BaseException:
            if self.states[component_id] != ModuleLifecycleState.FAILED:
                self._transition(component_id, ModuleLifecycleState.FAILED)
            raise
        self._transition(component_id, ModuleLifecycleState.STOPPED)

    def tasks(self):
        result = []
        for component_id in self.order:
            if self.states[component_id] != ModuleLifecycleState.ACTIVE:
                continue
            instance = self.instances[component_id]
            owned = getattr(instance, "owned_tasks", None)
            tasks = tuple(owned()) if owned is not None else (
                (task,) if (task := getattr(instance, "task", None)) is not None else ()
            )
            if any(not isinstance(task, asyncio.Task) for task in tasks):
                raise ForestError(
                    "RUNTIME_MODULE_TASK_INVALID", "Runtime module returned an invalid owned task"
                )
            result.extend(tasks)
        if len({id(task) for task in result}) != len(result):
            raise ForestError(
                "RUNTIME_MODULE_TASK_DUPLICATE", "Runtime modules cannot share task ownership"
            )
        return result

    def failures(self):
        result = {}
        for component_id in self.order:
            failure = getattr(self.instances[component_id], "failure", None)
            if self.states[component_id] == ModuleLifecycleState.FAILED:
                result[component_id] = failure or "MODULE_LIFECYCLE_FAILED"
            elif failure is not None:
                result[component_id] = failure
        return result

    def describe(self) -> list[dict]:
        return [
            {
                "component_id": descriptor.component_id,
                "requires": list(descriptor.requires),
                "state": self.states[descriptor.component_id],
                "graph_id": self.graph_identity.graph_id if self.graph_identity is not None else None,
                "generation_id": (
                    self.graph_identity.generations[descriptor.component_id]
                    if self.graph_identity is not None
                    else None
                ),
                "management_view": (
                    {
                        "kind": "trusted-module",
                        "label": descriptor.management_view.label,
                        "template": descriptor.management_view.template,
                        "module": descriptor.management_view.module,
                        "protocol": descriptor.management_view.protocol,
                        "style": descriptor.management_view.style,
                    }
                    if descriptor.management_view is not None
                    else None
                ),
                "management_contract": (
                    contract.describe()
                    if (contract := getattr(
                        self.instances.get(descriptor.component_id), "management_contract", None
                    )) is not None
                    else None
                ),
                "storage": (
                    descriptor.storage.as_fact()
                    if descriptor.storage is not None
                    else None
                ),
            }
            for descriptor in self.descriptors
        ]
