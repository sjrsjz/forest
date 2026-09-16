"""Dependency-ordered official modules for a Bot runtime.

The supervisor consumes descriptors and lifecycle operations; concrete module
construction lives here instead of in the platform adapter composition root.
Official and packaged modules can converge on this registry contract without
making Agent or Memory kernel types.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from forest.errors import ForestError


@dataclass(frozen=True)
class RuntimeModuleDescriptor:
    component_id: str
    requires: tuple[str, ...]
    factory: Callable[[Any, dict[str, Any]], Any]


def _memory(platform, _: dict[str, Any]):
    from forest.memory_runtime import MemoryRuntime

    return MemoryRuntime(platform)


def _agent(platform, _: dict[str, Any]):
    from forest.agent_runtime import AgentRuntime

    return AgentRuntime(platform)


def _conversation(platform, modules: dict[str, Any]):
    from forest.group_scheduler import GroupScheduler

    agent = modules["agent.runtime"]
    return GroupScheduler(
        platform.messages,
        lease=platform.kernel._authority,
        execute=platform.execute,
        delivery=platform.delivery,
        global_slots=platform.global_slots,
        finalize_agent=agent.runner.finalize,
        execution_timeout=3600,
    )


def _shell(platform, _: dict[str, Any]):
    from forest.shell import ShellDispatcher

    return ShellDispatcher(
        platform.messages,
        platform.bindings,
        platform.delivery,
        platform.kernel._authority,
        on_change=platform._notify_work,
        module_packages=platform.module_packages,
    )


def _jobs(platform, modules: dict[str, Any]):
    from forest.job_scheduler import JobDispatcher
    from forest.storage.jobs import JobStore

    store = JobStore(platform.messages)
    scheduler = modules["conversation.scheduler"]
    return JobDispatcher(
        store,
        platform.kernel._authority,
        authorize=platform._authorize_job if platform.job_authority is not None else None,
        on_change=scheduler.notify,
    )


OFFICIAL_MODULES = (
    RuntimeModuleDescriptor("memory.graph", (), _memory),
    RuntimeModuleDescriptor("agent.runtime", ("memory.graph",), _agent),
    RuntimeModuleDescriptor(
        "conversation.scheduler", ("agent.runtime",), _conversation
    ),
    RuntimeModuleDescriptor("shell.command", (), _shell),
    RuntimeModuleDescriptor(
        "scheduler.jobs", ("conversation.scheduler",), _jobs
    ),
)


class BotModuleRegistry:
    """Build and supervise modules from an acyclic descriptor graph."""

    def __init__(self, platform, descriptors=OFFICIAL_MODULES):
        self.platform = platform
        self.descriptors = tuple(descriptors)
        self.instances: dict[str, Any] = {}
        self.order: list[str] = []
        self._build()

    def _build(self) -> None:
        remaining = {item.component_id: item for item in self.descriptors}
        if len(remaining) != len(self.descriptors):
            raise ForestError("DUPLICATE_RUNTIME_MODULE", "Runtime module identities must be unique")
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
                instance = descriptor.factory(self.platform, self.instances)
                self.instances[descriptor.component_id] = instance
                self.order.append(descriptor.component_id)
                remaining.pop(descriptor.component_id)

    def get(self, component_id: str):
        return self.instances[component_id]

    async def start(self) -> None:
        started = []
        try:
            for component_id in self.order:
                instance = self.instances[component_id]
                start = getattr(instance, "start", None)
                if start is not None:
                    await start()
                started.append(component_id)
        except BaseException:
            for component_id in reversed(started):
                close = getattr(self.instances[component_id], "close", None)
                if close is not None:
                    try:
                        await close()
                    except Exception:
                        pass
            raise

    async def close(self) -> list[Exception]:
        errors = []
        for component_id in reversed(self.order):
            close = getattr(self.instances[component_id], "close", None)
            if close is None:
                continue
            try:
                await close()
            except Exception as exc:
                errors.append(exc)
        return errors

    def tasks(self):
        return [
            task for component_id in self.order
            if (task := getattr(self.instances[component_id], "task", None)) is not None
        ]

    def failures(self):
        return {
            component_id: failure
            for component_id in self.order
            if (failure := getattr(self.instances[component_id], "failure", None)) is not None
        }

    def describe(self) -> list[dict]:
        return [
            {
                "component_id": descriptor.component_id,
                "requires": list(descriptor.requires),
                "state": getattr(self.instances[descriptor.component_id], "state", "ACTIVE"),
            }
            for descriptor in self.descriptors
        ]
