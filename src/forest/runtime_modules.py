"""Dependency-ordered official modules for a Bot runtime.

The supervisor consumes descriptors and lifecycle operations; concrete module
construction lives here instead of in the platform adapter composition root.
Official and packaged modules can converge on this registry contract without
making Agent or Memory kernel types.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from forest.errors import ForestError


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
class RuntimeModuleDescriptor:
    component_id: str
    requires: tuple[str, ...]
    factory: Callable[[Any, dict[str, Any]], Any]
    management_view: RuntimeManagementView | None = None


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
    ModuleLifecycleState.VALIDATED: {ModuleLifecycleState.PREPARED, ModuleLifecycleState.FAILED},
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


class _OneBotRuntime:
    def __init__(self, platform):
        from forest.delivery import DeliveryWorker
        from forest.shell_ingress import ShellIngressGuard
        from forest.storage.bindings import BindingStore
        from forest.storage.delivery import DeliveryStore
        from forest.storage.messages import MessageStore

        self.platform = platform
        self.bindings = BindingStore(platform.bot.store)
        self.messages = MessageStore(platform.bot.store, self.bindings)
        self.shell_ingress = ShellIngressGuard(platform.bot.store.bot_id)
        self.delivery = DeliveryStore(platform.bot.store, self.bindings)
        self.worker = DeliveryWorker(
            self.delivery,
            platform.kernel._authority,
            platform.prepare,
            on_error=platform._delivery_error,
        )
        self.task = None

    @property
    def failure(self):
        return self.worker.failure

    async def start(self):
        if self.task is None:
            import asyncio

            self.task = asyncio.create_task(
                self.worker.run_forever(), name="binding-delivery"
            )

    async def quiesce(self):
        for connection in self.platform.connections.values():
            connection.accepting = False

    async def close(self):
        await self.worker.close()
        await self.platform._close_adapter_resources()


def _onebot(platform, _: dict[str, Any]):
    return _OneBotRuntime(platform)


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
    RuntimeModuleDescriptor("command.router", (), _command_routing),
    RuntimeModuleDescriptor("message.router", (), _message_routing),
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
        self._build()

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
            ):
                raise ForestError(
                    "RUNTIME_MODULE_DESCRIPTOR_INVALID", "Runtime module descriptor is invalid"
                )
            self._transition(identity, ModuleLifecycleState.VALIDATED)
        while remaining:
            ready = sorted(
                (item for item in remaining.values()
                 if all(required in self.instances for required in item.requires)),
                key=lambda item: item.component_id,
            )
            if not ready:
                raise ForestError(
                    "RUNTIME_MODULE_DEPENDENCY_CYCLE",
                    "Runtime module dependencies are missing or cyclic",
                )
            for descriptor in ready:
                try:
                    instance = descriptor.factory(self.platform, self.instances)
                except BaseException:
                    self._transition(descriptor.component_id, ModuleLifecycleState.FAILED)
                    raise
                self.instances[descriptor.component_id] = instance
                self.order.append(descriptor.component_id)
                self._transition(descriptor.component_id, ModuleLifecycleState.PREPARED)
                remaining.pop(descriptor.component_id)

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
            except BaseException:
                if current is not None and self.states[current] == ModuleLifecycleState.PREPARED:
                    self._transition(current, ModuleLifecycleState.FAILED)
                    await self._cleanup(current)
                for component_id in reversed(started):
                    await self._quiesce_and_cleanup(component_id)
                raise

    async def close(self) -> list[Exception]:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                return []
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
                        await self._cleanup(component_id)
                except Exception as exc:
                    errors.append(exc)
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
                        self.instances[descriptor.component_id], "management_contract", None
                    )) is not None
                    else None
                ),
            }
            for descriptor in self.descriptors
        ]
