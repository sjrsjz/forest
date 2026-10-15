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
from dataclasses import dataclass, replace
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
    read_only: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.operation_id, str)
            or not 1 <= len(self.operation_id) <= 256
            or isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0 < self.timeout_seconds <= 86_400
            or not isinstance(self.causal_root_kind, str)
            or not 1 <= len(self.causal_root_kind) <= 128
            or not isinstance(self.causal_root_id, str)
            or not 1 <= len(self.causal_root_id) <= 256
            or type(self.read_only) is not bool
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
    """Service endpoint that receives the kernel-sealed route explicitly."""

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

    @property
    def package_registry_reader(self):
        """Cross-scope read port bound to the active deployment package module."""
        reader = getattr(self._registry.runtime.bot, "package_registry_reader", None)
        if not callable(reader):
            raise ForestError(
                "PACKAGE_CATALOG_UNAVAILABLE", "Deployment package registry is unavailable", 503
            )
        return reader

    @property
    def component_candidate_previewer(self):
        """Expose only the Host's read-only candidate preview operation."""

        preview = getattr(self._registry.runtime.bot, "preview_component_change", None)
        if not callable(preview):
            raise ForestError(
                "COMPONENT_PREVIEW_UNAVAILABLE",
                "Bot host cannot preview component candidates",
                503,
            )
        return preview

    def participant(self, component_id: str):
        if component_id not in self._participants:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNDECLARED",
                "Kernel coordinator requested an undeclared participant",
                503,
            )
        instance = self._registry.instances[component_id]
        resolver = getattr(instance, "unit_of_work_participant", None)
        return resolver() if resolver is not None else instance


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
        read_only: bool = False,
        **kwargs,
    ):
        invocation = RuntimeServiceInvocation(
            operation_id=operation_id,
            timeout_seconds=timeout_seconds,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            parent_call_id=parent_call_id,
            read_only=read_only,
        )
        return await self._router.invoke(self._request_key, invocation, args, kwargs)

    @property
    def route(self) -> FrozenServiceRoute:
        return self._router.route(self._request_key)


class RuntimeGenerationHandle:
    """Opaque late-bound identity for one member of the immutable graph."""

    __slots__ = ("_router", "component_id")

    def __init__(self, router: RuntimeContractRouter, component_id: str) -> None:
        self._router = router
        self.component_id = component_id

    @property
    def generation_id(self) -> str:
        graph = self._router.registry.graph_identity
        if graph is None or self.component_id not in graph.generations:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "RuntimeGraph member identity is not sealed",
                503,
            )
        return graph.generations[self.component_id]


class RuntimeUnitOfWorkParticipantHandle:
    """Late-bound participant handle sealed to this Registry's RuntimeGraph."""

    __slots__ = ("_router", "component_id")

    def __init__(self, router: RuntimeContractRouter, component_id: str) -> None:
        self._router = router
        self.component_id = component_id

    def resolve(self):
        graph = self._router.registry.graph_identity
        instance = self._router.registry.instances.get(self.component_id)
        resolver = getattr(instance, "unit_of_work_participant", None)
        if graph is None or self.component_id not in graph.generations or resolver is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "RuntimeGraph does not publish the declared UnitOfWork participant",
                503,
            )
        participant = resolver()
        if (
            getattr(participant, "component_id", None) != self.component_id
            or getattr(participant, "generation_id", None)
            != graph.generations[self.component_id]
        ):
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_STALE",
                "UnitOfWork participant differs from the sealed RuntimeGraph",
                409,
            )
        return participant


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

    def unit_of_work_participant(
        self, component_id: str
    ) -> RuntimeUnitOfWorkParticipantHandle:
        return RuntimeUnitOfWorkParticipantHandle(self._router, component_id)

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

    def event_subscriptions(
        self,
    ) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        """Discover durable event handlers for the routing authority."""

        return self._router.event_subscriptions(self.component_id)

    def event_publications(
        self,
    ) -> tuple[tuple[str, object, RuntimeGenerationHandle], ...]:
        """Discover event publication contracts for the routing authority."""

        return self._router.event_publications(self.component_id)

    def component_jobs(self) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        """Discover component-owned job handlers for the routing authority."""

        return self._router.component_jobs(self.component_id)


class RuntimeContractRouter:
    """Resolve runtime service contracts without exposing module instances."""

    def __init__(self, registry) -> None:
        self.registry = registry
        self._providers: dict[tuple[str, int], tuple[str, Any]] = {}
        self._read_only_services: set[tuple[str, int]] = set()
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
        if (
            self.registry.directory_consumers.get("agent-tools") is None
            or caller_component_id != self.registry.directory_consumers["agent-tools"]
        ):
            raise ForestError(
                "RUNTIME_AGENT_TOOL_DISCOVERY_FORBIDDEN",
                "Only the declared Agent tool directory consumer may read this graph directory",
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
        if (
            self.registry.directory_consumers.get("shell-commands") is None
            or caller_component_id != self.registry.directory_consumers["shell-commands"]
        ):
            raise ForestError(
                "RUNTIME_SHELL_COMMAND_DISCOVERY_FORBIDDEN",
                "Only the declared Shell command directory consumer may read this graph directory",
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
        if (
            self.registry.directory_consumers.get("job-types") is None
            or caller_component_id != self.registry.directory_consumers["job-types"]
        ):
            raise ForestError(
                "RUNTIME_JOB_TYPE_DISCOVERY_FORBIDDEN",
                "Only the declared Job type directory consumer may read this graph directory",
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

    def event_subscriptions(
        self, caller_component_id: str
    ) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        if (
            self.registry.directory_consumers.get("routing-routes") is None
            or caller_component_id != self.registry.directory_consumers["routing-routes"]
        ):
            raise ForestError(
                "RUNTIME_EVENT_DIRECTORY_FORBIDDEN",
                "Only the declared routing directory consumer may read event subscriptions",
                403,
            )
        discovered = []
        for descriptor in sorted(
            self.registry.descriptors, key=lambda item: item.component_id
        ):
            for contribution in sorted(
                descriptor.event_subscriptions,
                key=lambda item: (item.subscriber_id, item.contract_major),
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

    def event_publications(
        self, caller_component_id: str
    ) -> tuple[tuple[str, object, RuntimeGenerationHandle], ...]:
        if (
            self.registry.directory_consumers.get("routing-routes") is None
            or caller_component_id != self.registry.directory_consumers["routing-routes"]
        ):
            raise ForestError(
                "RUNTIME_EVENT_DIRECTORY_FORBIDDEN",
                "Only the declared routing directory consumer may read event publications",
                403,
            )
        return tuple(
            (
                descriptor.component_id,
                contribution,
                RuntimeGenerationHandle(self, descriptor.component_id),
            )
            for descriptor in sorted(
                self.registry.descriptors, key=lambda item: item.component_id
            )
            for contribution in sorted(
                descriptor.event_publications,
                key=lambda item: (item.topic, item.contract),
            )
        )

    def component_jobs(
        self, caller_component_id: str
    ) -> tuple[tuple[str, object, RuntimeServiceHandle], ...]:
        if (
            self.registry.directory_consumers.get("routing-routes") is None
            or caller_component_id != self.registry.directory_consumers["routing-routes"]
        ):
            raise ForestError(
                "RUNTIME_COMPONENT_JOB_DIRECTORY_FORBIDDEN",
                "Only the declared routing directory consumer may read ComponentJobs",
                403,
            )
        discovered = []
        for descriptor in sorted(
            self.registry.descriptors, key=lambda item: item.component_id
        ):
            for contribution in sorted(
                descriptor.component_jobs, key=lambda item: item.handler_id
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
            if any(
                item.get("service_id") == service_id
                and item.get("version") == version
                and item.get("semantics") == "read"
                for item in contract["services"]
            ):
                self._read_only_services.add(key)

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
        if not callable(handler) and not isinstance(handler, RoutedServiceEndpoint):
            raise ForestError(
                "RUNTIME_KERNEL_SERVICE_INVALID",
                "Kernel coordinator factory returned no callable or routed handler",
                503,
            )
        self._providers[key] = ("kernel.runtime", handler)
        if any(
            item.get("service_id") == spec.service_id
            and item.get("version") == spec.version
            and item.get("semantics") == "read"
            for item in kernel.runtime_contract["services"]
        ):
            self._read_only_services.add(key)

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

    def release_provider(self, component_id: str) -> None:
        """Forget closed provider handlers after all its callers have drained."""

        self._providers = {
            key: value for key, value in self._providers.items() if value[0] != component_id
        }
        self._read_only_services.intersection_update(self._providers)

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
            async with authority.store.pool.connection() as conn, conn.transaction():
                if route.owned_work:
                    accepted = await authority.verify_generation_member_in(
                        conn,
                        component_id=route.provider_component_id,
                        generation_id=route.provider_generation_id,
                    )
                else:
                    accepted = await authority.verify_active_route_in(
                        conn,
                        component_id=route.provider_component_id,
                        generation_id=route.provider_generation_id,
                        admission_lane=route.admission_lane,
                    )
                if (
                    accepted.graph_id != route.graph_id
                    or accepted.graph_revision != route.graph_revision
                ):
                    # A handle may outlive its original Graph only when both
                    # exact Generations survive. Hold the Graph head fence
                    # across both membership checks.
                    caller_generation_id = self.registry.graph_identity.generations[
                        route.caller_component_id
                    ]
                    if route.owned_work:
                        await authority.verify_generation_member_in(
                            conn,
                            component_id=route.caller_component_id,
                            generation_id=caller_generation_id,
                        )
                    else:
                        await authority.verify_active_route_in(
                            conn,
                            component_id=route.caller_component_id,
                            generation_id=caller_generation_id,
                            admission_lane=route.admission_lane,
                        )
                    route = replace(
                        route,
                        graph_id=accepted.graph_id,
                        graph_revision=accepted.graph_revision,
                    )
        return await self._dispatch_route(route, invocation, args, kwargs)

    async def invoke_external(
        self,
        route: FrozenServiceRoute,
        invocation: RuntimeServiceInvocation,
        payload: dict,
    ):
        """Admit one host-owned cross-scope route to an exact provider Generation.

        The privileged caller must independently fence its own consumer
        Generation. This port validates the provider through the same Host
        authority and dispatches through the ordinary sealed service handler.
        """

        self._validate(route.service_id, route.version, route.admission_lane)
        if (
            not self._sealed
            or not self.graph_admission.is_set()
            or route.owned_work
            or route.admission_lane != "DATA"
            or not isinstance(route.caller_component_id, str)
            or _SERVICE_ID.fullmatch(route.caller_component_id) is None
        ):
            raise ForestError(
                "RUNTIME_EXTERNAL_ROUTE_INVALID",
                "External route is not admitted by this RuntimeGraph", 409,
            )
        graph = self.registry.graph_identity
        if (
            graph is None
            or graph.generations.get(route.provider_component_id)
            != route.provider_generation_id
        ):
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_STALE",
                "External route does not identify the current provider Generation", 409,
            )
        if graph.graph_id != route.graph_id or graph.graph_revision != route.graph_revision:
            route = replace(
                route, graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            )
        if getattr(self.registry.states.get(route.provider_component_id), "value", None) != (
            "ACTIVE"
        ):
            raise ForestError(
                "RUNTIME_SERVICE_PROVIDER_NOT_ACTIVE",
                "External provider is not accepting calls", 409,
            )
        authority = self.registry.generation_authority
        if authority is None:
            raise ForestError(
                "RUNTIME_GENERATION_AUTHORITY_UNAVAILABLE",
                "External routes require a persistent provider authority", 503,
            )
        async with authority.store.pool.connection() as conn, conn.transaction():
            accepted = await authority.verify_active_route_in(
                conn,
                component_id=route.provider_component_id,
                generation_id=route.provider_generation_id,
                admission_lane="DATA",
            )
            if (
                accepted.graph_id != route.graph_id
                or accepted.graph_revision != route.graph_revision
            ):
                route = replace(
                    route, graph_id=accepted.graph_id,
                    graph_revision=accepted.graph_revision,
                )
        return await self._dispatch_route(route, invocation, (payload,), {})

    async def _dispatch_route(
        self,
        route: FrozenServiceRoute,
        invocation: RuntimeServiceInvocation | None,
        args: tuple,
        kwargs: dict,
    ):
        service_key = (route.service_id, route.version)
        if invocation is not None and invocation.read_only and (
            service_key not in self._read_only_services
        ):
            raise ForestError(
                "SERVICE_READ_ONLY_VIOLATION",
                "Read-only service cannot invoke a mutating or unclassified service",
                403,
            )
        provider = self._providers.get(service_key)
        if provider is None or provider[0] != route.provider_component_id:
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_STALE",
                "Runtime service provider changed after graph sealing",
                409,
            )
        _provider_component_id, handler = provider
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
