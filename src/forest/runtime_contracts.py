"""Generation-bound in-process Contract Router handles.

The router is kernel infrastructure.  Module factories receive only a narrow
construction context and can request declared service handles; they never see
the registry's instance map or another module object.
"""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from forest.errors import ForestError

_SERVICE_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")


@dataclass(frozen=True)
class FrozenServiceRoute:
    service_id: str
    version: int
    caller_component_id: str
    provider_component_id: str
    provider_generation_id: str
    graph_id: str
    graph_revision: int
    admission_lane: str
    owned_work: bool


@dataclass(frozen=True)
class RuntimeServiceInvocation:
    """Explicit identity and deadline for one routed business operation."""

    operation_id: str
    timeout_seconds: float
    causal_root_kind: str
    causal_root_id: str
    parent_call_id: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.operation_id, str)
            or not 1 <= len(self.operation_id) <= 256
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0 < self.timeout_seconds <= 120
            or not isinstance(self.causal_root_kind, str)
            or not 1 <= len(self.causal_root_kind) <= 128
            or not isinstance(self.causal_root_id, str)
            or not 1 <= len(self.causal_root_id) <= 256
            or (
                self.parent_call_id is not None
                and (
                    not isinstance(self.parent_call_id, str)
                    or not 1 <= len(self.parent_call_id) <= 128
                )
            )
        ):
            raise ValueError("Runtime service invocation is invalid")


@dataclass(frozen=True)
class RoutedServiceEndpoint:
    """Provider endpoint that receives the kernel-sealed route explicitly."""

    routed: Callable

    def __post_init__(self) -> None:
        if not callable(self.routed):
            raise TypeError("Routed service endpoint handler must be callable")

    def invoke_routed(
        self,
        route: FrozenServiceRoute,
        invocation: RuntimeServiceInvocation,
        args: tuple,
        kwargs: dict,
    ):
        return self.routed(route, invocation, *args, **kwargs)


@dataclass(frozen=True)
class RuntimeKernelServiceSpec:
    """Declarative kernel coordinator service contributed by an official module."""

    service_id: str
    version: int
    participants: tuple[str, ...]
    factory: Callable[[RuntimeKernelServiceContext], Callable]

    def __post_init__(self) -> None:
        RuntimeContractRouter._validate(self.service_id, self.version, "DATA")
        if (
            not self.participants
            or len(set(self.participants)) != len(self.participants)
            or any(not isinstance(item, str) or not item for item in self.participants)
            or not callable(self.factory)
        ):
            raise ValueError("Kernel service contribution is invalid")

    def as_fact(self) -> dict:
        return {
            "service_id": self.service_id,
            "version": self.version,
            "participants": list(self.participants),
            "factory_ref": self.factory.__module__ + ":" + self.factory.__qualname__,
        }


class RuntimeKernelServiceContext:
    """Kernel-only participant resolver scoped to one coordinator declaration."""

    __slots__ = ("_registry", "_participants")

    def __init__(self, registry, participants: tuple[str, ...]) -> None:
        self._registry = registry
        self._participants = frozenset(participants)

    @property
    def store(self):
        return self._registry.runtime.bot.store

    @property
    def generation_member_verifier(self):
        return self._registry.generation_authority.verify_generation_member_in

    @property
    def host_lease(self):
        """Current fenced host lease for kernel-owned management coordinators."""

        return self._registry._lease()

    def participant(self, component_id: str):
        if component_id not in self._participants:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNDECLARED",
                "Kernel coordinator requested an undeclared participant",
                503,
            )
        return self._registry.instances[component_id]


class RuntimeServiceHandle:
    """Opaque handle for one service requirement in one immutable graph."""

    __slots__ = ("_router", "_request_key")

    def __init__(self, router: RuntimeContractRouter, request_key: tuple) -> None:
        self._router = router
        self._request_key = request_key

    async def __call__(self, *args, **kwargs):
        return await self._router.invoke(self._request_key, None, args, kwargs)

    async def invoke(
        self,
        *args,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
        timeout_seconds: float = 30,
        parent_call_id: str | None = None,
        **kwargs,
    ):
        invocation = RuntimeServiceInvocation(
            operation_id=operation_id,
            timeout_seconds=timeout_seconds,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            parent_call_id=parent_call_id,
        )
        return await self._router.invoke(self._request_key, invocation, args, kwargs)

    @property
    def route(self) -> FrozenServiceRoute:
        return self._router.route(self._request_key)


class RuntimeModuleFactoryContext:
    """The only cross-module facility available to a contract-aware factory."""

    __slots__ = ("component_id", "_router")

    def __init__(self, component_id: str, router: RuntimeContractRouter) -> None:
        self.component_id = component_id
        self._router = router

    def service(
        self,
        service_id: str,
        version: int = 1,
        *,
        admission_lane: str = "DATA",
        owned_work: bool = False,
    ) -> RuntimeServiceHandle:
        return self._router.request(
            caller_component_id=self.component_id,
            service_id=service_id,
            version=version,
            admission_lane=admission_lane,
            owned_work=owned_work,
        )

    @property
    def graph_admission(self) -> asyncio.Event:
        return self._router.graph_admission

    def agent_tools(self) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        """Discover frozen tool declarations without exposing module instances."""

        return self._router.agent_tools(self.component_id)

    def shell_commands(self) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        """Discover generation-bound command routes from module descriptors."""

        return self._router.shell_commands(self.component_id)

    def job_types(self) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        """Discover durable job executors without exposing module instances."""

        return self._router.job_types(self.component_id)


class RuntimeContractRouter:
    """Resolve runtime service contracts without exposing module instances."""

    def __init__(self, registry) -> None:
        self.registry = registry
        self._providers: dict[tuple[str, int], tuple[str, Any]] = {}
        self._requests: dict[tuple, RuntimeServiceHandle] = {}
        self._routes: dict[tuple, FrozenServiceRoute] = {}
        self._sealed = False
        self.graph_admission = asyncio.Event()

    @staticmethod
    def _validate(service_id: str, version: int, admission_lane: str) -> None:
        if (
            not isinstance(service_id, str)
            or _SERVICE_ID.fullmatch(service_id) is None
            or type(version) is not int
            or version < 1
            or admission_lane not in {"DATA", "CONTROL"}
        ):
            raise ForestError(
                "RUNTIME_SERVICE_REQUIREMENT_INVALID",
                "Runtime service requirement is invalid",
                422,
            )

    def context(self, component_id: str) -> RuntimeModuleFactoryContext:
        return RuntimeModuleFactoryContext(component_id, self)

    def request(
        self,
        *,
        caller_component_id: str,
        service_id: str,
        version: int,
        admission_lane: str,
        owned_work: bool,
    ) -> RuntimeServiceHandle:
        if self._sealed:
            raise ForestError(
                "RUNTIME_SERVICE_GRAPH_SEALED",
                "Runtime service requirements cannot change after graph sealing",
                409,
            )
        self._validate(service_id, version, admission_lane)
        if type(owned_work) is not bool:
            raise ForestError(
                "RUNTIME_SERVICE_REQUIREMENT_INVALID",
                "Runtime service ownership mode is invalid",
                422,
            )
        request_key = (
            caller_component_id,
            service_id,
            version,
            admission_lane,
            owned_work,
        )
        handle = self._requests.get(request_key)
        if handle is None:
            handle = RuntimeServiceHandle(self, request_key)
            self._requests[request_key] = handle
        return handle

    def agent_tools(
        self, caller_component_id: str
    ) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        if caller_component_id != "agent.runtime":
            raise ForestError(
                "RUNTIME_AGENT_TOOL_DISCOVERY_FORBIDDEN",
                "Only the Agent runtime may consume the graph tool directory",
                403,
            )
        discovered = []
        for descriptor in sorted(
            self.registry.descriptors, key=lambda item: item.component_id
        ):
            for contribution in sorted(
                descriptor.agent_tools, key=lambda item: item.name
            ):
                handle = self.request(
                    caller_component_id=caller_component_id,
                    service_id=contribution.service_id,
                    version=contribution.version,
                    admission_lane="DATA",
                    owned_work=True,
                )
                discovered.append((descriptor.component_id, contribution, handle))
        return tuple(discovered)

    def shell_commands(
        self, caller_component_id: str
    ) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        if caller_component_id != "shell.command":
            raise ForestError(
                "RUNTIME_SHELL_COMMAND_DISCOVERY_FORBIDDEN",
                "Only the Shell runtime may consume the graph command directory",
                403,
            )
        discovered = []
        for descriptor in sorted(
            self.registry.descriptors, key=lambda item: item.component_id
        ):
            for contribution in sorted(
                descriptor.shell_commands,
                key=lambda item: (item.command_id, item.command_version),
            ):
                handle = self.request(
                    caller_component_id=caller_component_id,
                    service_id=contribution.service_id,
                    version=contribution.service_version,
                    admission_lane="CONTROL",
                    owned_work=True,
                )
                discovered.append((descriptor.component_id, contribution, handle))
        return tuple(discovered)

    def job_types(
        self, caller_component_id: str
    ) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        if caller_component_id != "job.scheduler":
            raise ForestError(
                "RUNTIME_JOB_TYPE_DISCOVERY_FORBIDDEN",
                "Only the Job scheduler may consume the graph job directory",
                403,
            )
        discovered = []
        for descriptor in sorted(
            self.registry.descriptors, key=lambda item: item.component_id
        ):
            for contribution in sorted(
                descriptor.job_types,
                key=lambda item: (item.job_type, item.job_version),
            ):
                handle = self.request(
                    caller_component_id=caller_component_id,
                    service_id=contribution.service_id,
                    version=contribution.service_version,
                    admission_lane="DATA",
                    owned_work=True,
                )
                discovered.append((descriptor.component_id, contribution, handle))
        return tuple(discovered)

    def register(self, component_id: str, instance: Any) -> None:
        if self._sealed:
            raise RuntimeError("Runtime Contract Router is already sealed")
        contract = getattr(instance, "runtime_contract", None)
        handlers = getattr(instance, "runtime_service_handlers", None)
        if handlers is None:
            return
        handlers = handlers() if callable(handlers) else handlers
        if not isinstance(contract, dict) or not isinstance(handlers, dict):
            raise ForestError(
                "RUNTIME_SERVICE_PROVIDER_INVALID",
                "Runtime service provider declaration is invalid",
                503,
            )
        declared = {
            (item.get("service_id"), item.get("version"))
            for item in contract.get("services", ())
            if isinstance(item, dict)
        }
        if declared != set(handlers):
            raise ForestError(
                "RUNTIME_SERVICE_PROVIDER_INVALID",
                "Runtime service handlers differ from the advertised contract",
                503,
                details={"component_id": component_id},
            )
        for key, handler in handlers.items():
            service_id, version = key
            self._validate(service_id, version, "DATA")
            if (
                not callable(handler)
                and not isinstance(handler, RoutedServiceEndpoint)
            ) or key in self._providers:
                raise ForestError(
                    "RUNTIME_SERVICE_PROVIDER_CONFLICT",
                    "Runtime service has no unique provider endpoint",
                    409,
                    details={"service_id": service_id, "version": version},
                )
            self._providers[key] = (component_id, handler)

    def register_kernel_service(self, spec: RuntimeKernelServiceSpec) -> None:
        if self._sealed:
            raise RuntimeError("Runtime Contract Router is already sealed")
        kernel = self.registry.instances.get("kernel.runtime")
        declared = {
            (item.get("service_id"), item.get("version"))
            for item in getattr(kernel, "runtime_contract", {}).get("services", ())
            if isinstance(item, dict)
        }
        key = (spec.service_id, spec.version)
        if key not in declared or key in self._providers:
            raise ForestError(
                "RUNTIME_KERNEL_SERVICE_INVALID",
                "Kernel coordinator service is not uniquely declared",
                503,
            )
        context = RuntimeKernelServiceContext(self.registry, spec.participants)
        handler = spec.factory(context)
        if not callable(handler):
            raise ForestError(
                "RUNTIME_KERNEL_SERVICE_INVALID",
                "Kernel coordinator factory returned no callable handler",
                503,
            )
        self._providers[key] = ("kernel.runtime", handler)

    def seal(self) -> None:
        if self._sealed:
            return
        graph = self.registry.graph_identity
        if graph is None:
            raise ForestError(
                "RUNTIME_SERVICE_GRAPH_UNAVAILABLE",
                "Runtime service graph has no immutable identity",
                503,
            )
        for request_key in self._requests:
            caller_component_id, service_id, version, admission_lane, owned_work = request_key
            caller = self.registry.instances.get(caller_component_id)
            provider = self._providers.get((service_id, version))
            requirements = {
                (item.get("service_id"), item.get("version"))
                for item in getattr(caller, "runtime_contract", {}).get("requires", ())
                if isinstance(item, dict)
            }
            if (service_id, version) not in requirements:
                raise ForestError(
                    "RUNTIME_SERVICE_REQUIREMENT_UNDECLARED",
                    "Module requested a service absent from its runtime contract",
                    503,
                    details={
                        "component_id": caller_component_id,
                        "service_id": service_id,
                        "version": version,
                    },
                )
            if provider is None:
                raise ForestError(
                    "RUNTIME_SERVICE_PROVIDER_UNAVAILABLE",
                    "Declared runtime service has no provider in this graph",
                    503,
                    details={"service_id": service_id, "version": version},
                )
            provider_component_id, _handler = provider
            identity = self.registry.instances[provider_component_id].generation_identity
            self._routes[request_key] = FrozenServiceRoute(
                service_id=service_id,
                version=version,
                caller_component_id=caller_component_id,
                provider_component_id=provider_component_id,
                provider_generation_id=identity.generation_id,
                graph_id=graph.graph_id,
                graph_revision=graph.graph_revision,
                admission_lane=admission_lane,
                owned_work=owned_work,
            )
        self._sealed = True

    def activate(self) -> None:
        if not self._sealed:
            raise RuntimeError("Runtime Contract Router was not sealed")
        self.graph_admission.set()

    def deactivate(self) -> None:
        self.graph_admission.clear()

    def route(self, request_key: tuple) -> FrozenServiceRoute:
        route = self._routes.get(request_key)
        if route is None:
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_UNAVAILABLE",
                "Runtime service route is not sealed",
                503,
            )
        return route

    async def invoke(
        self,
        request_key: tuple,
        invocation: RuntimeServiceInvocation | None,
        args: tuple,
        kwargs: dict,
    ):
        route = self.route(request_key)
        if self.registry.graph_identity is None or (
            self.registry.graph_identity.graph_id != route.graph_id
            or self.registry.graph_identity.graph_revision != route.graph_revision
        ):
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_STALE",
                "Runtime service handle belongs to another graph",
                409,
            )
        caller_state = getattr(
            self.registry.states[route.caller_component_id], "value", None
        )
        allowed_caller_states = (
            {"ACTIVE", "QUIESCING"} if route.owned_work else {"ACTIVE"}
        )
        if caller_state not in allowed_caller_states:
            raise ForestError(
                "RUNTIME_SERVICE_CALLER_NOT_ACTIVE",
                "Runtime service caller no longer owns this call route",
                409,
            )
        active = getattr(self.registry.states[route.provider_component_id], "value", None)
        if active != "ACTIVE":
            raise ForestError(
                "RUNTIME_SERVICE_PROVIDER_NOT_ACTIVE",
                "Runtime service provider is not active",
                409,
            )
        authority = self.registry.generation_authority
        if authority is not None:
            if route.owned_work:
                async with authority.store.pool.connection() as conn, conn.transaction():
                    await authority.verify_generation_member_in(
                        conn,
                        component_id=route.provider_component_id,
                        generation_id=route.provider_generation_id,
                    )
            else:
                await authority.verify_active_generation(
                    component_id=route.provider_component_id,
                    generation_id=route.provider_generation_id,
                    admission_lane=route.admission_lane,
                )
        provider_component_id, handler = self._providers[(route.service_id, route.version)]
        if provider_component_id != route.provider_component_id:
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_STALE",
                "Runtime service provider changed after graph sealing",
                409,
            )
        if isinstance(handler, RoutedServiceEndpoint):
            if invocation is None:
                raise ForestError(
                    "RUNTIME_SERVICE_INVOCATION_REQUIRED",
                    "Plugin service calls require an explicit operation identity",
                    422,
                )
            result = handler.invoke_routed(route, invocation, args, kwargs)
        else:
            result = handler(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result
