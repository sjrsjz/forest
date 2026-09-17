"""Dependency-ordered runtime modules for Bot, deployment, and host scopes.

The supervisor consumes descriptors and lifecycle operations; concrete module
construction lives here instead of in the runtime adapter composition root.
Official and packaged modules converge on this registry contract without
making Agent or Memory kernel types. Bot hosts additionally persist their
generation graph; other scope hosts use the same lifecycle without pretending
to own a Bot kernel lease.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from typing import Any
from uuid import uuid4

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import (
    PreparedGenerationBinding,
    PreparedGenerationContext,
    RuntimeStorageBinding,
)
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)


@dataclass(frozen=True)
class RuntimeArtifactIdentity:
    """Immutable code/resource envelope that produced a runtime Generation."""

    package_id: str
    package_version: str
    package_digest: str
    scope: str
    driver: str
    trust_level: str

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.package_id)
            or not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z.+-]{0,127}", self.package_version)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", self.package_digest)
            or self.scope not in {"deployment", "host", "bot"}
            or not re.fullmatch(r"[a-z][a-z0-9._-]*/[1-9][0-9]*", self.driver)
            or self.trust_level not in {"official", "installed"}
        ):
            raise ValueError("Runtime artifact identity is invalid")

    def as_fact(self) -> dict:
        return {
            "package_id": self.package_id,
            "package_version": self.package_version,
            "package_digest": self.package_digest,
            "scope": self.scope,
            "driver": self.driver,
            "trust_level": self.trust_level,
        }


@lru_cache(maxsize=3)
def official_artifact(scope: str = "bot") -> RuntimeArtifactIdentity:
    """Hash the complete current Forest distribution, including trusted UI assets."""

    root = files("forest")
    captured: dict[str, str] = {}

    def walk(node, prefix: str = "") -> None:
        for child in sorted(node.iterdir(), key=lambda item: item.name):
            relative = f"{prefix}/{child.name}" if prefix else child.name
            if child.is_dir():
                if child.name != "__pycache__":
                    walk(child, relative)
            elif child.name != "RECORD" and child.suffix.lower() in {
                ".py", ".sql", ".html", ".css", ".js", ".json", ".toml"
            }:
                captured[relative] = hashlib.sha256(child.read_bytes()).hexdigest()

    walk(root)
    try:
        package_version = version("forest")
    except PackageNotFoundError:  # pragma: no cover - source tree without installation
        package_version = "0.1.0"
    package_digest = digest_json(
        {
            "protocol": "forest.official-distribution/1",
            "package_id": "forest",
            "package_version": package_version,
            "files": captured,
        }
    )
    return RuntimeArtifactIdentity(
        package_id="forest",
        package_version=package_version,
        package_digest=package_digest,
        scope=scope,
        driver="forest.in-process-trusted/1",
        trust_level="official",
    )


@dataclass(frozen=True)
class RuntimeManagementContribution:
    """One generation-bound workspace contribution from a runtime module."""

    contribution_id: str
    label: str
    scope_kinds: tuple[str, ...]
    template: str
    module: str
    view_protocol: str = "forest.trusted-management-view/1"
    style: str | None = None
    group: str = "module"
    order: int = 100

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", self.contribution_id):
            raise ValueError("Management contribution identity is invalid")
        if not self.label.strip() or any(ord(char) < 32 for char in self.label):
            raise ValueError("Management contribution labels must be printable")
        if (
            not self.scope_kinds
            or len(set(self.scope_kinds)) != len(self.scope_kinds)
            or any(scope not in {"deployment", "host", "bot", "binding", "component"}
                   for scope in self.scope_kinds)
        ):
            raise ValueError("Management contribution scopes are invalid")
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", self.group):
            raise ValueError("Management contribution group is invalid")
        if type(self.order) is not int or not -10_000 <= self.order <= 10_000:
            raise ValueError("Management contribution order is invalid")
        for value, suffix in ((self.template, ".html"), (self.module, ".js")):
            if (
                not value.startswith("/static/")
                or not value.endswith(suffix)
                or ".." in value.split("/")
                or "\\" in value
            ):
                raise ValueError("Management contribution assets must use canonical /static paths")
        if self.view_protocol != "forest.trusted-management-view/1":
            raise ValueError("Unsupported runtime management view protocol")
        if self.style is not None and (
            not self.style.startswith("/static/")
            or not self.style.endswith(".css")
            or ".." in self.style.split("/")
            or "\\" in self.style
        ):
            raise ValueError("Runtime management styles must use canonical /static paths")

    def as_fact(self, *, public: bool = False) -> dict:
        surface = {
            "kind": "trusted-module",
            "protocol": self.view_protocol,
            "template": self.template,
            "module": self.module,
            "style": self.style,
        }
        return {
            "protocol": "forest.management-contribution/1",
            "contribution_id": self.contribution_id,
            "label": self.label,
            "scope_kinds": list(self.scope_kinds),
            "group": self.group,
            "order": self.order,
            "surface": surface if public else {key: value for key, value in surface.items()
                                                  if key != "kind"},
        }


@dataclass(frozen=True)
class RuntimeStorageBundle:
    """Verified immutable SQL owned by one runtime module generation."""

    spec: StorageBundleSpec
    baseline: bytes
    package_digest: str
    execution_profile: str = "declarative"

    def __post_init__(self) -> None:
        if self.execution_profile == "declarative":
            validate_storage_baseline(self.baseline, self.spec.baseline_sha256)
        elif self.execution_profile == "official":
            validate_official_storage_baseline(self.baseline, self.spec.baseline_sha256)
        else:
            raise ForestError(
                "INVALID_STORAGE_BUNDLE", "Storage execution profile is invalid", 422
            )
        if not isinstance(self.package_digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", self.package_digest
        ):
            raise ForestError(
                "INVALID_STORAGE_BUNDLE", "Storage package digest is invalid", 422
            )

    def as_fact(self) -> dict:
        return {
            **self.spec.as_fact(),
            "package_digest": self.package_digest,
            "execution_profile": self.execution_profile,
        }


def official_storage_bundle(
    component_id: str,
    schema_id: str,
    repository_contract: str,
    resource: str,
) -> RuntimeStorageBundle:
    """Bind one packaged official baseline to the complete distribution identity."""

    baseline = files("forest").joinpath(resource).read_bytes()
    artifact = official_artifact("bot")
    spec = StorageBundleSpec(
        component_id=component_id,
        schema_id=schema_id,
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract=repository_contract,
    )
    return RuntimeStorageBundle(
        spec,
        baseline,
        artifact.package_digest,
        execution_profile="official",
    )


@dataclass(frozen=True)
class RuntimeModuleDescriptor:
    component_id: str
    requires: tuple[str, ...]
    factory: Callable[[Any, dict[str, Any]], Any]
    artifact: RuntimeArtifactIdentity
    management: tuple[RuntimeManagementContribution, ...] = ()
    storage: RuntimeStorageBundle | None = None
    wakes_on_work: bool = False
    work_notify_order: int = 100

    def __post_init__(self) -> None:
        identities = [item.contribution_id for item in self.management]
        if len(identities) != len(set(identities)):
            raise ValueError("Runtime management contribution identities must be unique")
        if type(self.wakes_on_work) is not bool:
            raise ValueError("Runtime work notification declaration must be boolean")
        if type(self.work_notify_order) is not int or not 0 <= self.work_notify_order <= 10_000:
            raise ValueError("Runtime work notification order is invalid")


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


def _memory(runtime, modules: dict[str, Any]):
    from forest.management.memory_api import MemoryManagementContract
    from forest.memory_runtime import MemoryRuntime

    adapter = modules["adapter.onebot"]
    module = MemoryRuntime(runtime, messages=adapter.messages)
    module.management_contract = MemoryManagementContract()
    return module


def _agent(runtime, modules: dict[str, Any]):
    from forest.agent_runtime import AgentRuntime
    from forest.management.agent_api import AgentManagementContract

    module = AgentRuntime(runtime, messages=modules["adapter.onebot"].messages)
    module.management_contract = AgentManagementContract()
    return module


def _conversation(runtime, modules: dict[str, Any]):
    from forest.group_scheduler import GroupScheduler

    agent = modules["agent.runtime"]
    adapter = modules["adapter.onebot"]
    return GroupScheduler(
        adapter.messages,
        lease=runtime.kernel._authority,
        execute=modules["contract.router"].execute,
        delivery=adapter.delivery,
        global_slots=runtime.global_slots,
        finalize_agent=agent.runner.finalize,
        execution_timeout=3600,
    )


class _CommandRoutingRuntime:
    def __init__(self, runtime, adapter):
        from forest.storage.business_command_diagnostics import BusinessCommandDiagnosticsStore
        from forest.storage.command_limits import CommandLimitStore
        from forest.storage.command_policies import CommandPolicyStore

        self.policies = CommandPolicyStore(runtime.bot.store)
        self.limits = CommandLimitStore(runtime.bot.store)
        self.bindings = adapter.bindings
        self.diagnostics = BusinessCommandDiagnosticsStore(
            runtime.bot.store,
            messages=adapter.messages,
            lease=runtime.kernel._authority(),
            control=getattr(runtime.bot, "control", None),
        )


class _MessageRoutingRuntime:
    def __init__(self, runtime, adapter):
        from forest.storage.message_handler_diagnostics import MessageHandlerDiagnosticsStore
        from forest.storage.message_handler_limits import MessageHandlerLimitStore
        from forest.storage.message_handler_policies import MessageHandlerPolicyStore

        self.policies = MessageHandlerPolicyStore(runtime.bot.store)
        self.limits = MessageHandlerLimitStore(runtime.bot.store)
        self.bindings = adapter.bindings
        self.diagnostics = MessageHandlerDiagnosticsStore(
            runtime.bot.store, control=getattr(runtime.bot, "control", None)
        )


def _command_routing(runtime, modules: dict[str, Any]):
    from forest.management.command_contract import CommandRoutingManagementContract

    module = _CommandRoutingRuntime(runtime, modules["adapter.onebot"])
    module.management_contract = CommandRoutingManagementContract()
    return module


def _message_routing(runtime, modules: dict[str, Any]):
    from forest.management.message_contract import MessageRoutingManagementContract

    module = _MessageRoutingRuntime(runtime, modules["adapter.onebot"])
    module.management_contract = MessageRoutingManagementContract()
    return module


def _onebot(runtime, modules: dict[str, Any]):
    from forest.management.onebot_contract import OneBotManagementContract
    from forest.onebot_runtime import OneBotRuntime

    module = OneBotRuntime(runtime, component_host=modules["component.worker-host"])
    module.management_contract = OneBotManagementContract()
    return module


def _component_workers(runtime, _: dict[str, Any]):
    from forest.component_workers import ComponentWorkerHost
    from forest.management.component_worker_contract import ComponentWorkerManagementContract

    module = ComponentWorkerHost(
        runtime.kernel, runtime.packages, module_packages=runtime.module_packages
    )
    module.management_contract = ComponentWorkerManagementContract()
    return module


def _kernel_runtime(runtime, _: dict[str, Any]):
    from forest.management.kernel_runtime_contract import KernelRuntimeManagementRuntime

    return KernelRuntimeManagementRuntime(runtime)


def _contract_router(runtime, modules: dict[str, Any]):
    from forest.contract_router import ContractExecutionRouter

    return ContractExecutionRouter(
        runtime,
        modules["component.worker-host"],
        modules["adapter.onebot"],
        modules["agent.runtime"],
    )


def _routing_graph(runtime, _: dict[str, Any]):
    from forest.management.routing_api import RoutingManagementContract
    from forest.routing_graph import RouteGraphRuntime

    module = RouteGraphRuntime(runtime)
    module.management_contract = RoutingManagementContract()
    return module


def _audit(runtime, _: dict[str, Any]):
    from forest.audit_runtime import AuditRuntime
    from forest.management.audit_contract import AuditManagementContract

    module = AuditRuntime(runtime.bot.store, control=getattr(runtime.bot, "control", None))
    module.management_contract = AuditManagementContract()
    return module


def _shell(runtime, modules: dict[str, Any]):
    from forest.shell import ShellDispatcher

    adapter = modules["adapter.onebot"]
    return ShellDispatcher(
        adapter.messages,
        adapter.bindings,
        adapter.delivery,
        runtime.kernel._authority,
        audit=modules["kernel.audit"],
        on_change=runtime.notify,
        module_packages=runtime.module_packages,
    )


def _jobs(runtime, modules: dict[str, Any]):
    from forest.job_scheduler import JobAuthorizationPort, JobDispatcher
    from forest.management.job_contract import JobManagementContract
    from forest.storage.jobs import JobStore

    store = JobStore(modules["adapter.onebot"].messages)
    scheduler = modules["conversation.scheduler"]
    authorization = (
        JobAuthorizationPort(runtime.job_authority, runtime.bot.store.bot_id, store)
        if runtime.job_authority is not None
        else None
    )
    module = JobDispatcher(
        store,
        runtime.kernel._authority,
        authorize=authorization.authorize if authorization is not None else None,
        on_change=scheduler.notify,
    )
    module.management_contract = JobManagementContract()
    return module


_OFFICIAL_ARTIFACT = official_artifact("bot")


OFFICIAL_MODULES = (
    RuntimeModuleDescriptor(
        "kernel.runtime",
        (),
        _kernel_runtime,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "runtime",
            "运行与业务接纳",
            ("bot",),
            "/static/modules/kernel.runtime.html",
            "/static/kernel-runtime.js",
            group="runtime",
            order=10,
        ),),
    ),
    RuntimeModuleDescriptor(
        "component.worker-host",
        (),
        _component_workers,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "components",
            "可安装组件",
            ("bot",),
            "/static/modules/component.worker-host.html",
            "/static/component-worker-host.js",
            group="runtime",
            order=20,
        ),),
    ),
    RuntimeModuleDescriptor(
        "adapter.onebot",
        ("component.worker-host",),
        _onebot,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "connections",
            "OneBot 连接与投递",
            ("bot",),
            "/static/modules/adapter.onebot.html",
            "/static/onebot-panel.js",
            style="/static/modules/adapter.onebot.css",
            group="adapter",
            order=10,
        ),),
    ),
    RuntimeModuleDescriptor(
        "routing.graph",
        (),
        _routing_graph,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "routing",
            "路由与执行图",
            ("bot",),
            "/static/modules/routing.graph.html",
            "/static/routing-graph.js",
            style="/static/modules/routing.graph.css",
            group="runtime",
            order=30,
        ),),
    ),
    RuntimeModuleDescriptor(
        "memory.graph",
        ("adapter.onebot",),
        _memory,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "memory",
            "图文记忆", ("bot",), "/static/modules/memory.graph.html", "/static/memory.js",
            style="/static/modules/memory.graph.css",
            group="business",
            order=30,
        ),),
        wakes_on_work=True,
        work_notify_order=30,
    ),
    RuntimeModuleDescriptor(
        "agent.runtime",
        ("adapter.onebot", "memory.graph"),
        _agent,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "agent",
            "Agent 与模型", ("bot",), "/static/modules/agent.runtime.html", "/static/agent-panel.js",
            style="/static/modules/agent.runtime.css",
            group="business",
            order=20,
        ),),
    ),
    RuntimeModuleDescriptor(
        "command.router",
        ("adapter.onebot", "routing.graph"),
        _command_routing,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "commands",
            "业务命令路由",
            ("bot",),
            "/static/modules/command.router.html",
            "/static/command-router.js",
            group="routing",
            order=40,
        ),),
    ),
    RuntimeModuleDescriptor(
        "kernel.audit",
        (),
        _audit,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "audit",
            "审计元数据",
            ("bot",),
            "/static/modules/kernel.audit.html",
            "/static/kernel-audit.js",
            group="runtime",
            order=40,
        ),),
    ),
    RuntimeModuleDescriptor(
        "message.router",
        ("adapter.onebot", "routing.graph"),
        _message_routing,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "messages",
            "普通消息路由",
            ("bot",),
            "/static/modules/message.router.html",
            "/static/message-router.js",
            group="routing",
            order=50,
        ),),
    ),
    RuntimeModuleDescriptor(
        "contract.router",
        ("component.worker-host", "adapter.onebot", "agent.runtime"),
        _contract_router,
        _OFFICIAL_ARTIFACT,
    ),
    RuntimeModuleDescriptor(
        "conversation.scheduler",
        (
            "adapter.onebot",
            "agent.runtime",
            "command.router",
            "message.router",
            "contract.router",
        ),
        _conversation,
        _OFFICIAL_ARTIFACT,
        wakes_on_work=True,
        work_notify_order=10,
    ),
    RuntimeModuleDescriptor(
        "shell.command", ("adapter.onebot", "kernel.audit"), _shell, _OFFICIAL_ARTIFACT
    ),
    RuntimeModuleDescriptor(
        "scheduler.jobs",
        ("adapter.onebot", "conversation.scheduler"),
        _jobs,
        _OFFICIAL_ARTIFACT,
        management=(RuntimeManagementContribution(
            "schedules",
            "定时任务",
            ("bot",),
            "/static/modules/scheduler.jobs.html",
            "/static/jobs-panel.js",
            style="/static/modules/scheduler.jobs.css",
            group="automation",
            order=20,
        ),),
        wakes_on_work=True,
        work_notify_order=20,
    ),
)


class RuntimeModuleRegistry:
    """Build and supervise modules from an acyclic descriptor graph."""

    def __init__(self, runtime, descriptors=OFFICIAL_MODULES):
        self.runtime = runtime
        self.descriptors = tuple(descriptors)
        self.instances: dict[str, Any] = {}
        self.generation_bindings: dict[str, PreparedGenerationBinding] = {}
        self.order: list[str] = []
        self.states: dict[str, ModuleLifecycleState] = {
            descriptor.component_id: ModuleLifecycleState.DISCOVERED
            for descriptor in self.descriptors
        }
        self._lifecycle_lock = None
        self._closed = False
        self.graph_identity = None
        self.activation_id = str(uuid4())
        self.scope_kind = getattr(runtime, "scope_kind", "bot")
        self.generation_authority = getattr(runtime, "generation_authority", None)
        store = getattr(getattr(runtime, "bot", None), "store", None)
        if self.generation_authority is not None:
            self.graph_store = self.generation_authority
        elif store is None:
            self.graph_store = None
        else:
            from forest.storage.runtime_graph import RuntimeModuleGraphStore

            self.graph_store = RuntimeModuleGraphStore(store)
        self._build()

    def _descriptor_fact(self, descriptor: RuntimeModuleDescriptor) -> dict:
        instance = self.instances.get(descriptor.component_id)
        contract = getattr(instance, "management_contract", None)
        fact = {
            "protocol": "forest.runtime-module/1",
            "component_id": descriptor.component_id,
            "requires": list(descriptor.requires),
            "factory_ref": descriptor.factory.__module__ + ":" + descriptor.factory.__qualname__,
            "artifact": descriptor.artifact.as_fact(),
            "management_contributions": [
                contribution.as_fact() for contribution in descriptor.management
            ],
            "management_contract": contract.describe() if contract is not None else None,
            "runtime_contract": getattr(instance, "runtime_contract", None),
            "state_schema": getattr(instance, "state_schema", None),
            "state_contract": getattr(instance, "state_contract", None),
            "storage": descriptor.storage.as_fact() if descriptor.storage is not None else None,
            "wakes_on_work": descriptor.wakes_on_work,
            "work_notify_order": descriptor.work_notify_order,
        }
        return {**fact, "descriptor_digest": digest_json(fact)}

    def _descriptor_facts(self) -> list[dict]:
        return [self._descriptor_fact(descriptor) for descriptor in self.descriptors]

    async def _prepare_generation_binding(
        self, descriptor: RuntimeModuleDescriptor, instance: Any
    ) -> PreparedGenerationBinding:
        prepare = getattr(instance, "prepare_generation", None)
        if (
            prepare is None
            or not hasattr(instance, "runtime_contract")
            or not hasattr(instance, "state_contract")
            or not hasattr(instance, "state_schema")
        ):
            raise ForestError(
                "RUNTIME_GENERATION_PREPARE_UNAVAILABLE",
                f"Module {descriptor.component_id} does not expose the clean preparation contract",
                503,
            )
        fact = self._descriptor_fact(descriptor)
        management_contract = fact["management_contract"]
        contributions = fact["management_contributions"]
        storage = (
            RuntimeStorageBinding(
                descriptor.storage.spec.schema_id,
                descriptor.storage.spec.schema_version,
                descriptor.storage.spec.namespace,
                descriptor.storage.spec.baseline_sha256,
                descriptor.storage.spec.repository_contract,
                descriptor.storage.execution_profile,
            )
            if descriptor.storage is not None
            else None
        )
        context = PreparedGenerationContext(
            activation_id=self.activation_id,
            component_id=descriptor.component_id,
            requires=descriptor.requires,
            package_id=descriptor.artifact.package_id,
            package_version=descriptor.artifact.package_version,
            package_digest=descriptor.artifact.package_digest,
            driver=descriptor.artifact.driver,
            trust_level=descriptor.artifact.trust_level,
            descriptor_digest=fact["descriptor_digest"],
            route_contract_digest=digest_json(instance.runtime_contract),
            management_contract_digest=digest_json(management_contract),
            management_ui_digest=digest_json(contributions),
            state_schema=instance.state_schema,
            state_contract_digest=digest_json(instance.state_contract),
            storage=storage,
        )
        binding = prepare(context)
        if inspect.isawaitable(binding):
            binding = await binding
        if not isinstance(binding, PreparedGenerationBinding):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_INVALID",
                f"Module {descriptor.component_id} returned no prepared Generation binding",
                503,
            )
        expected = {
            "component_id": context.component_id,
            "requires": context.requires,
            "package_id": context.package_id,
            "package_version": context.package_version,
            "package_digest": context.package_digest,
            "driver": context.driver,
            "trust_level": context.trust_level,
            "descriptor_digest": context.descriptor_digest,
            "route_contract_digest": context.route_contract_digest,
            "management_contract_digest": context.management_contract_digest,
            "management_ui_digest": context.management_ui_digest,
            "state_schema": context.state_schema,
            "state_contract_digest": context.state_contract_digest,
            "storage": context.storage,
        }
        if any(getattr(binding, key) != value for key, value in expected.items()):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                f"Module {descriptor.component_id} changed Registry-owned Generation facts",
                409,
            )
        return binding

    def _lease(self, *, allow_closing=False):
        if allow_closing and hasattr(self.runtime, "_cleanup_lease"):
            return self.runtime._cleanup_lease()
        return self.runtime.kernel._authority(allow_closing=allow_closing)

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
                or descriptor.artifact.scope != self.scope_kind
                or (
                    descriptor.storage is not None
                    and (
                        descriptor.storage.spec.component_id != identity
                        or descriptor.storage.package_digest
                        != descriptor.artifact.package_digest
                        or (
                            descriptor.storage.execution_profile == "official"
                            and descriptor.artifact.trust_level != "official"
                        )
                    )
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
        installer = getattr(self.runtime, "schema_bundle_installer", None)
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
            execution_profile=descriptor.storage.execution_profile,
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
                instance = descriptor.factory(self.runtime, self.instances)
                self.instances[component_id] = instance
                if self.generation_authority is not None:
                    self.generation_bindings[component_id] = (
                        await self._prepare_generation_binding(descriptor, instance)
                    )
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
            elif state == ModuleLifecycleState.FAILED:
                if component_id in self.instances:
                    try:
                        await self._cleanup(component_id)
                    except Exception as exc:
                        errors.append(exc)
                else:
                    self._transition(component_id, ModuleLifecycleState.STOPPED)
            elif state == ModuleLifecycleState.VALIDATED:
                self._transition(component_id, ModuleLifecycleState.STOPPED)
        return errors

    def get(self, component_id: str):
        return self.instances[component_id]

    def notify_work(self) -> None:
        """Wake active modules that declared the generic durable-work signal."""

        declared = sorted(
            (item for item in self.descriptors if item.wakes_on_work),
            key=lambda item: (item.work_notify_order, item.component_id),
        )
        for descriptor in declared:
            if self.states[descriptor.component_id] == ModuleLifecycleState.ACTIVE:
                self.instances[descriptor.component_id].notify()

    async def _prepare_graph(self):
        lease = None
        try:
            await self._prepare_modules()
            if self.graph_store is not None:
                lease = self._lease()
                if self.graph_identity is None:
                    if self.generation_authority is not None:
                        self.graph_identity = await self.generation_authority.prepare_graph(
                            lease,
                            [self.generation_bindings[item] for item in self.order],
                            self.activation_id,
                        )
                    else:
                        self.graph_identity = await self.graph_store.prepare(
                            lease, self._descriptor_facts(), self.activation_id
                        )
            return lease
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

    async def prepare(self) -> None:
        """Install storage and construct the graph without admitting runtime work."""

        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._closed:
                raise ForestError(
                    "RUNTIME_MODULE_REGISTRY_CLOSED", "Runtime module registry is closed", 503
                )
            if all(
                self.states[item]
                in {ModuleLifecycleState.PREPARED, ModuleLifecycleState.ACTIVE}
                for item in self.order
            ):
                return
            await self._prepare_graph()

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
            lease = await self._prepare_graph()
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
                    if self.generation_authority is not None:
                        await self.generation_authority.publish_graph(lease, self.graph_identity)
                    else:
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
                    if self.generation_authority is not None:
                        await self.generation_authority.retire_graph(lease, self.graph_identity)
                    else:
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
                "artifact": descriptor.artifact.as_fact(),
                "graph_id": self.graph_identity.graph_id if self.graph_identity is not None else None,
                "generation_id": (
                    self.graph_identity.generations[descriptor.component_id]
                    if self.graph_identity is not None
                    else None
                ),
                "management_contributions": [
                    contribution.as_fact(public=True)
                    for contribution in descriptor.management
                ],
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
