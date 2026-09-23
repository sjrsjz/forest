"""Bind immutable plugin packages to the kernel Generation lifecycle.

This module is kernel infrastructure.  It never imports plugin entrypoints in the
host process; only the selected RuntimeDriver may do that inside its Worker.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

from forest.drivers.python_worker import PythonWorker, WorkerIdentity, WorkerLimits
from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.package_capabilities import (
    PackageCallScope,
    PackageDomainCommands,
    PackageDomainQueries,
    PackageJobCallScope,
    PackageOwnedWorkAuthority,
    PackageServiceError,
    PackageServiceOperationLedger,
    PackageServiceRoute,
    PackageStateCommands,
    PackageStateQueries,
    PackageUnitOfWorkAuthority,
)
from forest.package_effects import (
    PackageEffectDriver,
    PackageEffectDriverFactory,
    PackageServiceEffectDriver,
)
from forest.package_owned_work import PackageOwnedWorkSupervisor
from forest.packages import PackageSnapshot, contract_range
from forest.routing_protocol import (
    DurableComponentJobRequest,
    DurableEventDeliveryRequest,
    PublishComponentEvent,
    ScheduleComponentJob,
)
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RoutedServiceEndpoint,
    RuntimeServiceInvocation,
)
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.runtime_modules import (
    OwnedWorkLifecycle,
    RuntimeArtifactIdentity,
    RuntimeComponentJobContribution,
    RuntimeEventPublicationContribution,
    RuntimeEventSubscriptionContribution,
    RuntimeJobTypeContribution,
    RuntimeModuleDescriptor,
)


class PackageGenerationState(StrEnum):
    NEW = "NEW"
    PREPARED = "PREPARED"
    BOUND = "BOUND"
    WORKER_READY = "WORKER_READY"
    ACTIVE = "ACTIVE"
    QUIESCING = "QUIESCING"
    QUIESCENT = "QUIESCENT"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


GenerationVerifier = Callable[..., Awaitable[Any]]
ParticipantInvoker = Callable[..., Awaitable[Any]]


class PackageUnitOfWorkParticipant:
    """Generation-bound package participant exposed only to kernel coordinators."""

    __slots__ = ("component_id", "generation_id", "_invoke")

    def __init__(
        self,
        component_id: str,
        generation_id: str,
        invoke: ParticipantInvoker,
    ) -> None:
        self.component_id = component_id
        self.generation_id = generation_id
        self._invoke = invoke

    async def invoke_in(
        self,
        conn,
        name: str,
        payload: dict,
        *,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        invocation = RuntimeServiceInvocation(
            operation_id=operation_id,
            timeout_seconds=timeout_seconds,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            parent_call_id=parent_call_id,
        )
        return await self._invoke(conn, self.generation_id, name, payload, invocation)


def _major(contract: str) -> int:
    lower, _upper = contract_range(contract)
    return lower[0]


def _worker_limits(component: Mapping[str, Any]) -> WorkerLimits:
    declared = component.get("resources", {})
    defaults = WorkerLimits()
    return WorkerLimits(
        max_frame_bytes=declared.get("max_frame_bytes", defaults.max_frame_bytes),
        max_inflight=declared.get("max_inflight", defaults.max_inflight),
        max_queue_items=declared.get("max_queue_items", defaults.max_queue_items),
        max_state_inflight=declared.get(
            "max_state_inflight", defaults.max_state_inflight
        ),
        max_owned_tasks=declared.get("max_owned_tasks", defaults.max_owned_tasks),
        max_owned_resources=declared.get(
            "max_owned_resources", defaults.max_owned_resources
        ),
    )


def _timeout(component: Mapping[str, Any], name: str, default: float) -> float:
    return float(component.get("lifecycle", {}).get(name, default))


def _handler_service_id(kind: str, component_id: str, handler_id: str) -> str:
    identity = hashlib.sha256(f"{component_id}\0{handler_id}".encode()).hexdigest()
    return f"component.{kind}.{identity}"


class PackageGenerationAdapter:
    """One generic host-side handle for one immutable package Generation."""

    requires_replacement_quiesce = True

    def __init__(
        self,
        *,
        snapshot: PackageSnapshot,
        bot_id: str,
        module_configuration: Any,
        grants: Any,
        service_routes: Mapping[str, Any],
        resolved_contracts: Mapping[str, str],
        generation_verifier: GenerationVerifier,
        generation_member_verifier: GenerationVerifier,
        state_queries: PackageStateQueries | None,
        state_commands: PackageStateCommands | None,
        domain_queries: PackageDomainQueries | None = None,
        domain_commands: PackageDomainCommands | None = None,
        unit_of_work_authority: PackageUnitOfWorkAuthority | None = None,
        owned_work_authority: PackageOwnedWorkAuthority | None = None,
        service_operation_ledger: PackageServiceOperationLedger,
        configuration_validator: Callable[[object], Any] | None = None,
        effect_driver_factories: Mapping[str, PackageEffectDriverFactory] | None = None,
        event_publisher=None,
        component_job_scheduler=None,
        worker_factory: Callable[..., PythonWorker] = PythonWorker,
    ) -> None:
        self.snapshot = snapshot
        self.component = snapshot.component
        self.component_id = self.component["id"]
        self.bot_id = bot_id
        self.module_configuration = module_configuration
        self.grants = grants
        self._service_routes = dict(service_routes)
        self._resolved_contracts = dict(resolved_contracts)
        self._generation_verifier = generation_verifier
        self._generation_member_verifier = generation_member_verifier
        self._state_queries = state_queries
        self._state_commands = state_commands
        self._domain_queries = domain_queries
        self._domain_commands = domain_commands
        self._unit_of_work_authority = unit_of_work_authority
        self._service_operation_ledger = service_operation_ledger
        self._configuration_validator = configuration_validator
        self._effect_driver_factories = dict(effect_driver_factories or {})
        self._effect_drivers: dict[str, PackageEffectDriver] = {}
        self._job_cancellations: dict[str, Callable[[], Awaitable[bool]]] = {}
        self._event_publisher = event_publisher
        self._component_job_scheduler = component_job_scheduler
        self._worker_factory = worker_factory
        self._worker: PythonWorker | None = None
        self._configuration: dict[str, Any] | None = None
        self._configuration_revision: int | None = None
        self._prepared_binding: PreparedGenerationBinding | None = None
        self.generation_identity: BoundGenerationIdentity | None = None
        self.state = PackageGenerationState.NEW
        self._service_schemas = {
            item["name"]: (
                Draft202012Validator(self.snapshot.schema(item["input_schema"])),
                Draft202012Validator(self.snapshot.schema(item["output_schema"])),
            )
            for item in (
                *self.component.get("provides", ()),
                *self.component.get("effect_services", ()),
                *self.component.get("domain_queries", ()),
                *self.component.get("domain_commands", ()),
                *self.component.get("job_types", ()),
            )
        }
        self._participant_declarations = {
            item["name"]: (
                item,
                Draft202012Validator(self.snapshot.schema(item["input_schema"])),
                Draft202012Validator(self.snapshot.schema(item["output_schema"])),
            )
            for item in self.component.get("unit_of_work_participants", ())
        }
        self._owned_work_declarations = tuple(
            self.component.get("owned_work_handlers", ())
        )
        self._owned_work_schemas = {
            item["name"]: (
                Draft202012Validator(self.snapshot.schema(item["input_schema"])),
                Draft202012Validator(self.snapshot.schema(item["output_schema"])),
            )
            for item in self._owned_work_declarations
        }
        self._event_subscriptions = {
            item["subscriber_id"]: (
                item,
                Draft202012Validator(self.snapshot.schema(item["schema"])),
                _handler_service_id("event", self.component_id, item["subscriber_id"]),
            )
            for item in self.component.get("subscribes", ())
        }
        self._component_jobs = {
            item["handler_id"]: (
                item,
                Draft202012Validator(self.snapshot.schema(item["input_schema"])),
                _handler_service_id("job", self.component_id, item["handler_id"]),
            )
            for item in self.component.get("jobs", ())
        }
        self._requested_capabilities = frozenset(
            self.component.get("capabilities", {}).get("requests", ())
        )
        self._durable_service_results = any(
            item == "state.write"
            or item.startswith("domain.command:")
            or item.startswith("event.publish:")
            or item.startswith("job.schedule:")
            or item.startswith("service.call:")
            for item in self._requested_capabilities
        )
        self._service_inflight = 0
        self._services_drained = asyncio.Event()
        self._services_drained.set()
        self.failure: str | None = None
        self._owned_work: PackageOwnedWorkSupervisor | None = None
        if self._owned_work_declarations:
            if owned_work_authority is None:
                raise ForestError(
                    "PACKAGE_OWNED_WORK_AUTHORITY_UNAVAILABLE",
                    "Package owned work authority is unavailable",
                    503,
                )
            self._owned_work = PackageOwnedWorkSupervisor(
                bot_id=self.bot_id,
                component_id=self.component_id,
                declarations=self._owned_work_declarations,
                schemas=self._owned_work_schemas,
                authority=owned_work_authority,
                operation_ledger=self._service_operation_ledger,
                binding=self._owned_work_binding,
                generation_member_verifier=self._generation_member_verifier,
                active=lambda: self.state is PackageGenerationState.ACTIVE,
                failed=self._owned_work_failed,
            )
            self.freeze_owned_admission = self._owned_work.freeze_admission
            self.drain_owned_work = self._owned_work.drain
            self.open_owned_admission = self._owned_work.open_admission
        self._granted_capabilities = frozenset(
            item.capability
            for item in self.grants.entries
            if item.resource_kind == "component" and item.resource_id == self.component_id
        )

        services = [
            {"service_id": item["name"], "version": _major(item["contract"])}
            for item in (
                *self.component.get("provides", ()),
                *self.component.get("effect_services", ()),
                *self.component.get("job_types", ()),
            )
        ]
        services.extend(
            {"service_id": service_id, "version": 1}
            for _item, _validator, service_id in self._event_subscriptions.values()
        )
        services.extend(
            {"service_id": service_id, "version": 1}
            for _item, _validator, service_id in self._component_jobs.values()
        )
        requirements = [
            {"service_id": name, "version": _major(contract)}
            for name, contract in sorted(self._resolved_contracts.items())
        ]
        local_effects = {item["name"]: item for item in self.component.get("effect_services", ())}
        requirements.extend(
            {
                "service_id": name,
                "version": _major(local_effects[name]["contract"]),
            }
            for name in sorted(local_effects)
            if f"service.call:{name}" in self._requested_capabilities
        )
        if any(item.startswith("event.publish:") for item in self._requested_capabilities):
            requirements.append(
                {"service_id": "routing.event.publish", "version": 1}
            )
        if any(item.startswith("job.schedule:") for item in self._requested_capabilities):
            requirements.append(
                {"service_id": "routing.component-job.schedule", "version": 1}
            )
        self.runtime_contract = {
            "protocol": "forest.runtime-services/1",
            "services": services,
            "requires": requirements,
            "domain_ports": [
                {
                    "kind": kind,
                    "name": item["name"],
                    "version": _major(item["contract"]),
                    "function": item["function"],
                }
                for kind, declarations in (
                    ("query", self.component.get("domain_queries", ())),
                    ("command", self.component.get("domain_commands", ())),
                )
                for item in declarations
            ],
            "unit_of_work_participants": [
                {
                    "name": item["name"],
                    "version": _major(item["contract"]),
                    "function": item["function"],
                }
                for item in self.component.get("unit_of_work_participants", ())
            ],
            "owned_work_handlers": [
                {
                    "name": item["name"],
                    "version": _major(item["contract"]),
                    "handler": item["handler"],
                    "max_concurrency": item["max_concurrency"],
                }
                for item in self._owned_work_declarations
            ],
        }
        self.state_schema = self.component["state_schema"]
        state_schema_ref = self.component.get("state_value_schema")
        self.state_contract = (
            self.snapshot.schema(state_schema_ref)
            if state_schema_ref is not None
            else {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            }
        )

    def runtime_service_handlers(self) -> dict[tuple[str, int], RoutedServiceEndpoint]:
        handlers = {
            (item["name"], _major(item["contract"])): self._service_handler(item["name"])
            for item in self.component.get("provides", ())
        }
        handlers.update(
            {
                (item["name"], _major(item["contract"])): self._effect_service_handler(
                    item["name"]
                )
                for item in self.component.get("effect_services", ())
            }
        )
        handlers.update(
            {
                (item["name"], _major(item["contract"])): self._job_type_handler(
                    item["name"]
                )
                for item in self.component.get("job_types", ())
            }
        )
        handlers.update(
            {
                (service_id, 1): self._event_subscription_handler(subscriber_id)
                for subscriber_id, (_item, _validator, service_id) in (
                    self._event_subscriptions.items()
                )
            }
        )
        handlers.update(
            {
                (service_id, 1): self._component_job_handler(handler_id)
                for handler_id, (_item, _validator, service_id) in (
                    self._component_jobs.items()
                )
            }
        )
        return handlers

    def management_ui_artifact(self, expected_sha256: str) -> tuple[dict, bytes]:
        """Read this exact Generation's verified, immutable UI artifact."""

        artifact = self.snapshot.management_ui()
        if artifact is None or artifact[0]["sha256"] != expected_sha256:
            raise ForestError(
                "PACKAGE_MANAGEMENT_UI_NOT_FOUND",
                "Management UI does not belong to the active package Generation",
                404,
            )
        return artifact

    def unit_of_work_participant(self) -> PackageUnitOfWorkParticipant:
        """Resolve a narrow handle bound to this exact package Generation."""

        identity = self.generation_identity
        if (
            not self._participant_declarations
            or self._unit_of_work_authority is None
            or identity is None
            or self._prepared_binding is None
            or self.state not in {
                PackageGenerationState.BOUND,
                PackageGenerationState.WORKER_READY,
                PackageGenerationState.ACTIVE,
                PackageGenerationState.QUIESCING,
            }
        ):
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "Package UnitOfWork participant is unavailable in this Generation",
                409,
            )
        return PackageUnitOfWorkParticipant(
            self.component_id,
            identity.generation_id,
            self._invoke_unit_of_work_participant,
        )

    async def _invoke_unit_of_work_participant(
        self,
        conn,
        expected_generation_id: str,
        name: str,
        payload: dict,
        invocation: RuntimeServiceInvocation,
    ) -> Any:
        identity = self.generation_identity
        prepared = self._prepared_binding
        authority = self._unit_of_work_authority
        declaration_entry = self._participant_declarations.get(name)
        if (
            identity is None
            or prepared is None
            or authority is None
            or identity.generation_id != expected_generation_id
            or self.state not in {
                PackageGenerationState.ACTIVE,
                PackageGenerationState.QUIESCING,
            }
        ):
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_STALE",
                "Package UnitOfWork participant no longer owns this Generation",
                409,
            )
        if declaration_entry is None or not isinstance(payload, dict):
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_INPUT_INVALID",
                "UnitOfWork participant call differs from the package declaration",
                422,
            )
        declaration, input_validator, output_validator = declaration_entry
        if not input_validator.is_valid(payload):
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_INPUT_INVALID",
                "UnitOfWork participant input violates its immutable schema",
                422,
            )
        scope = PackageCallScope(
            bot_id=self.bot_id,
            component_id=self.component_id,
            generation_id=identity.generation_id,
            instance_id=prepared.instance_id,
            worker_incarnation=prepared.worker_incarnation,
            call_id="call:" + uuid4().hex,
            call_operation_id=invocation.operation_id,
            causal_root_kind=invocation.causal_root_kind,
            causal_root_id=invocation.causal_root_id,
        )
        self._service_inflight += 1
        self._services_drained.clear()
        try:
            result = await authority.invoke_participant_in(
                conn,
                scope,
                declaration,
                payload,
                parent_call_id=invocation.parent_call_id,
                timeout_seconds=invocation.timeout_seconds,
            )
            if not output_validator.is_valid(result):
                raise ForestError(
                    "RUNTIME_UOW_PARTICIPANT_OUTPUT_INVALID",
                    "UnitOfWork participant result violates its immutable schema",
                    502,
                )
            return result
        finally:
            self._service_inflight -= 1
            if self._service_inflight == 0:
                self._services_drained.set()

    def _owned_work_binding(self):
        identity = self.generation_identity
        prepared = self._prepared_binding
        worker = self._worker
        if identity is None or prepared is None or worker is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Package owned work has no bound Generation and Worker",
                409,
            )
        return identity, prepared, worker

    def _owned_work_failed(self, code: str) -> None:
        self.failure = code

    def notify(self) -> None:
        if self._owned_work is not None:
            self._owned_work.notify()

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return self._owned_work.owned_tasks() if self._owned_work is not None else ()
    def _check_route(self, route: FrozenServiceRoute) -> None:
        identity = self.generation_identity
        if self.state is not PackageGenerationState.ACTIVE or self._worker is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Package Generation is not accepting calls",
                409,
            )
        if (
            identity is None
            or self._prepared_binding is None
            or route.provider_component_id != self.component_id
            or route.provider_generation_id != identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_STALE",
                "Package invocation route does not identify this Generation",
                409,
            )

    async def _run_service_operation(
        self,
        *,
        route: FrozenServiceRoute,
        invocation: RuntimeServiceInvocation,
        service_id: str,
        input_value: dict,
        durable: bool,
        dispatch: Callable[[PythonWorker, PackageCallScope, str], Awaitable[Any]],
        normalize: Callable[[Any], Any],
        result_valid: Callable[[Any], bool],
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> Any:
        self._check_route(route)
        worker = self._worker
        identity = self.generation_identity
        prepared = self._prepared_binding
        assert worker is not None and identity is not None and prepared is not None
        call_id = "call:" + uuid4().hex
        scope_values = dict(
            bot_id=self.bot_id,
            component_id=self.component_id,
            generation_id=identity.generation_id,
            instance_id=prepared.instance_id,
            worker_incarnation=prepared.worker_incarnation,
            call_id=call_id,
            call_operation_id=invocation.operation_id,
            causal_root_kind=invocation.causal_root_kind,
            causal_root_id=invocation.causal_root_id,
        )
        scope = (
            PackageJobCallScope(
                **scope_values, cancellation_requested=cancellation_requested
            )
            if cancellation_requested is not None
            else PackageCallScope(**scope_values)
        )
        service_route = PackageServiceRoute(
            service_id=service_id,
            graph_id=route.graph_id,
            graph_revision=route.graph_revision,
            admission_lane=route.admission_lane,
            owned_work=route.owned_work,
            parent_call_id=invocation.parent_call_id,
            timeout_seconds=invocation.timeout_seconds,
        )
        self._service_inflight += 1
        self._services_drained.clear()
        try:
            if durable:
                admission = await self._service_operation_ledger.begin(
                    scope, service_route, input_value
                )
                if not admission.execute:
                    if admission.error is not None:
                        raise ForestError(
                            admission.error.code,
                            admission.error.message,
                            admission.error.status,
                            details=admission.error.details,
                        )
                    if not result_valid(admission.result):
                        raise ForestError(
                            "SERVICE_OPERATION_CORRUPT",
                            "Stored plugin operation result violates its contract",
                            500,
                        )
                    return admission.result
            try:
                result = normalize(await dispatch(worker, scope, call_id))
            except ForestError as error:
                if durable:
                    await self._service_operation_ledger.fail(
                        scope,
                        service_route,
                        PackageServiceError(
                            error.code,
                            error.message,
                            error.status,
                            error.details if isinstance(error.details, dict) else None,
                        ),
                    )
                raise
            except Exception:
                if durable:
                    await self._service_operation_ledger.fail(
                        scope,
                        service_route,
                        PackageServiceError(
                            "PLUGIN_SERVICE_FAILED",
                            "Plugin service failed without a typed error",
                            500,
                        ),
                    )
                raise
            if not result_valid(result):
                error = ForestError(
                    "RUNTIME_SERVICE_OUTPUT_INVALID",
                    "Plugin service result violates its immutable contract",
                    502,
                )
                if durable:
                    await self._service_operation_ledger.fail(
                        scope,
                        service_route,
                        PackageServiceError(error.code, error.message, error.status),
                    )
                raise error
            if durable:
                await self._service_operation_ledger.succeed(scope, service_route, result)
            return result
        finally:
            self._service_inflight -= 1
            if self._service_inflight == 0:
                self._services_drained.set()

    def _service_handler(self, service_id: str) -> RoutedServiceEndpoint:
        async def invoke(
            route: FrozenServiceRoute,
            invocation: RuntimeServiceInvocation,
            *args,
            **kwargs,
        ):
            if args and kwargs or len(args) > 1:
                raise ForestError(
                    "RUNTIME_SERVICE_INPUT_INVALID",
                    "Plugin services accept one JSON payload",
                    422,
                )
            payload = args[0] if args else kwargs
            if not isinstance(payload, dict):
                raise ForestError(
                    "RUNTIME_SERVICE_INPUT_INVALID",
                    "Plugin service payload must be an object",
                    422,
                )
            input_validator, output_validator = self._service_schemas[service_id]
            if not input_validator.is_valid(payload):
                raise ForestError(
                    "RUNTIME_SERVICE_INPUT_INVALID",
                    "Plugin service payload violates its immutable schema",
                    422,
                )
            return await self._run_service_operation(
                route=route,
                invocation=invocation,
                service_id=service_id,
                input_value=payload,
                durable=self._durable_service_results,
                dispatch=lambda worker, _scope, call_id: worker.invoke(
                        service_id,
                        payload,
                        call_id=call_id,
                        operation_id=invocation.operation_id,
                        causal_root_kind=invocation.causal_root_kind,
                        causal_root_id=invocation.causal_root_id,
                        timeout=invocation.timeout_seconds,
                        before_send=lambda: self._verify_dispatch(route),
                ),
                normalize=lambda result: result,
                result_valid=output_validator.is_valid,
            )

        async def routed(route, invocation, *args, **kwargs):
            return await invoke(route, invocation, *args, **kwargs)

        return RoutedServiceEndpoint(routed=routed)

    def _effect_service_handler(self, service_id: str) -> RoutedServiceEndpoint:
        declaration = next(
            item
            for item in self.component.get("effect_services", ())
            if item["name"] == service_id
        )

        async def routed(route, invocation, *args, **kwargs):
            if args and kwargs or len(args) > 1:
                raise ForestError(
                    "RUNTIME_SERVICE_INPUT_INVALID",
                    "Plugin effect services accept one JSON payload",
                    422,
                )
            payload = args[0] if args else kwargs
            if not isinstance(payload, dict):
                raise ForestError(
                    "RUNTIME_SERVICE_INPUT_INVALID",
                    "Plugin effect service payload must be an object",
                    422,
                )
            input_validator, output_validator = self._service_schemas[service_id]
            if not input_validator.is_valid(payload):
                raise ForestError(
                    "RUNTIME_SERVICE_INPUT_INVALID",
                    "Plugin effect service payload violates its immutable schema",
                    422,
                )
            driver = self._effect_drivers.get(service_id)
            if not isinstance(driver, PackageServiceEffectDriver):
                raise ForestError(
                    "PACKAGE_EFFECT_DRIVER_NOT_READY",
                    "Package effect driver is not attached to this Generation",
                    503,
                )
            return await self._run_service_operation(
                route=route,
                invocation=invocation,
                service_id=service_id,
                input_value=payload,
                durable=declaration["semantics"] == "at-most-once",
                dispatch=lambda _worker, scope, _call_id: driver.invoke(
                    payload, scope=scope, invocation=invocation
                ),
                normalize=lambda result: result,
                result_valid=output_validator.is_valid,
            )

        return RoutedServiceEndpoint(routed=routed)

    def _job_type_handler(self, service_id: str) -> RoutedServiceEndpoint:
        declaration = next(
            item for item in self.component.get("job_types", ()) if item["name"] == service_id
        )

        async def routed(route, invocation, *args, **kwargs):
            if kwargs or len(args) != 1 or type(args[0]) is not DurableJobExecutionRequest:
                raise ForestError(
                    "JOB_EXECUTION_INPUT_INVALID",
                    "Package job service accepts one durable Job request",
                    422,
                )
            request = args[0]
            if (
                request.job_type != declaration["job_type"]
                or request.job_version != declaration["job_version"]
            ):
                raise ForestError(
                    "JOB_EXECUTION_INPUT_INVALID",
                    "Durable Job request differs from the package declaration",
                    422,
                )
            input_validator, output_validator = self._service_schemas[service_id]
            if not input_validator.is_valid(request.input):
                raise ForestError(
                    "JOB_EXECUTION_INPUT_INVALID",
                    "Durable Job input violates its immutable package schema",
                    422,
                )
            operation_input = {
                "job_id": request.job_id,
                "job_type": request.job_type,
                "job_version": request.job_version,
                "attempt": request.attempt,
                "requested_by": request.requested_by,
                "input": request.input,
            }
            async def dispatch(worker, _scope, call_id):
                self._job_cancellations[call_id] = request.cancellation_requested
                try:
                    return await worker.deliver_job(
                        declaration["handler"],
                        operation_input,
                        call_id=call_id,
                        operation_id=invocation.operation_id,
                        causal_root_kind=invocation.causal_root_kind,
                        causal_root_id=invocation.causal_root_id,
                        timeout=invocation.timeout_seconds,
                        before_send=lambda: self._verify_dispatch(route),
                    )
                finally:
                    self._job_cancellations.pop(call_id, None)

            return await self._run_service_operation(
                route=route,
                invocation=invocation,
                service_id=service_id,
                input_value=operation_input,
                durable=True,
                dispatch=dispatch,
                normalize=lambda result: result,
                result_valid=output_validator.is_valid,
            )

        return RoutedServiceEndpoint(routed=routed)

    def _event_subscription_handler(self, subscriber_id: str) -> RoutedServiceEndpoint:
        _declaration, validator, service_id = self._event_subscriptions[subscriber_id]

        async def routed(route, invocation, request):
            if (
                type(request) is not DurableEventDeliveryRequest
                or request.subscriber_id != subscriber_id
                or not validator.is_valid(request.payload)
            ):
                raise ForestError(
                    "EVENT_DELIVERY_INPUT_INVALID",
                    "Event delivery differs from the immutable subscription",
                    422,
                )
            payload = request.worker_payload()
            return await self._run_service_operation(
                route=route,
                invocation=invocation,
                service_id=service_id,
                input_value=payload,
                durable=True,
                dispatch=lambda worker, _scope, call_id: worker.deliver_event(
                    subscriber_id,
                    payload,
                    call_id=call_id,
                    operation_id=invocation.operation_id,
                    causal_root_kind=invocation.causal_root_kind,
                    causal_root_id=invocation.causal_root_id,
                    timeout=invocation.timeout_seconds,
                    before_send=lambda: self._verify_dispatch(route),
                ),
                normalize=lambda _result: {"acknowledged": True},
                result_valid=lambda result: result == {"acknowledged": True},
            )

        return RoutedServiceEndpoint(routed=routed)

    def _component_job_handler(self, handler_id: str) -> RoutedServiceEndpoint:
        _declaration, validator, service_id = self._component_jobs[handler_id]

        async def routed(route, invocation, request):
            if (
                type(request) is not DurableComponentJobRequest
                or request.handler_id != handler_id
                or not validator.is_valid(request.payload)
            ):
                raise ForestError(
                    "COMPONENT_JOB_INPUT_INVALID",
                    "ComponentJob input differs from the immutable handler contract",
                    422,
                )
            payload = request.worker_payload()
            return await self._run_service_operation(
                route=route,
                invocation=invocation,
                service_id=service_id,
                input_value=payload,
                durable=True,
                dispatch=lambda worker, _scope, call_id: worker.deliver_job(
                    handler_id,
                    payload,
                    call_id=call_id,
                    operation_id=invocation.operation_id,
                    causal_root_kind=invocation.causal_root_kind,
                    causal_root_id=invocation.causal_root_id,
                    timeout=invocation.timeout_seconds,
                    before_send=lambda: self._verify_dispatch(route),
                ),
                normalize=lambda result: result,
                result_valid=lambda result: isinstance(result, dict),
            )

        return RoutedServiceEndpoint(routed=routed)

    async def _verify_dispatch(self, route: FrozenServiceRoute) -> None:
        identity = self.generation_identity
        if self.state is not PackageGenerationState.ACTIVE or identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Package Generation stopped admission before Worker dispatch",
                409,
            )
        if route.owned_work:
            await self._generation_member_verifier(
                component_id=self.component_id,
                generation_id=identity.generation_id,
            )
        else:
            await self._generation_verifier(
                component_id=self.component_id,
                generation_id=identity.generation_id,
                admission_lane=route.admission_lane,
            )

    async def _service_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        service_id = request["service"]
        local = next(
            (
                item
                for item in self.component.get("effect_services", ())
                if item["name"] == service_id
            ),
            None,
        )
        route = self._service_routes.get(service_id)
        contract = local["contract"] if local is not None else self._resolved_contracts.get(service_id)
        if (
            contract is None
            or request["contract_major"] != _major(contract)
            or not self._has_capability(f"service.call:{service_id}")
        ):
            raise ForestError(
                "CAPABILITY_DENIED",
                "Worker requested an undeclared service capability",
                403,
            )
        timeout = self._worker.remaining_timeout(call_id, request["timeout"])
        if local is not None:
            return await self._invoke_local_effect(
                identity,
                call_id,
                local,
                request["payload"],
                operation_id=request["operation_id"],
                timeout_seconds=timeout,
            )
        if route is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE", "Declared service route is unavailable", 503
            )
        return await route.invoke(
            request["payload"],
            operation_id=request["operation_id"],
            timeout_seconds=timeout,
            parent_call_id=call_id,
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
        )

    async def _domain_query_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        return await self._domain_gateway("query", identity, call_id, request)

    async def _domain_command_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        return await self._domain_gateway("command", identity, call_id, request)

    async def _domain_gateway(
        self,
        kind: str,
        identity: WorkerIdentity,
        parent_call_id: str,
        request: dict,
    ) -> Any:
        outer = self._call_scope(identity, parent_call_id)
        declarations = self.component.get(
            "domain_queries" if kind == "query" else "domain_commands", ()
        )
        declaration = next(
            (item for item in declarations if item["name"] == request["name"]), None
        )
        capability = f"domain.{kind}:{request['name']}"
        if (
            declaration is None
            or request["contract_major"] != _major(declaration["contract"])
            or not self._has_capability(capability)
        ):
            raise ForestError(
                "CAPABILITY_DENIED",
                "Worker requested an undeclared domain authority capability",
                403,
            )
        payload = request["payload"]
        if not isinstance(payload, dict):
            raise ForestError(
                "DOMAIN_INPUT_INVALID", "Domain authority payload must be an object", 422
            )
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not input_validator.is_valid(payload):
            raise ForestError(
                "DOMAIN_INPUT_INVALID",
                "Domain authority payload violates its immutable schema",
                422,
            )
        timeout = self._worker.remaining_timeout(parent_call_id, request["timeout"])
        operation_id = (
            request["operation_id"] if kind == "command" else outer.call_operation_id
        )
        scope = PackageCallScope(
            bot_id=outer.bot_id,
            component_id=outer.component_id,
            generation_id=outer.generation_id,
            instance_id=outer.instance_id,
            worker_incarnation=outer.worker_incarnation,
            call_id="call:" + uuid4().hex,
            call_operation_id=operation_id,
            causal_root_kind=outer.causal_root_kind,
            causal_root_id=outer.causal_root_id,
        )
        authority = self._domain_queries if kind == "query" else self._domain_commands
        if authority is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE", "Domain authority port is unavailable", 503
            )
        if kind == "query":
            result = await authority.invoke_query(
                scope,
                declaration,
                payload,
                parent_call_id=parent_call_id,
                timeout_seconds=timeout,
            )
        else:
            result = await authority.invoke_command(
                scope,
                declaration,
                payload,
                parent_call_id=parent_call_id,
                timeout_seconds=timeout,
            )
        if not output_validator.is_valid(result):
            raise ForestError(
                "DOMAIN_OUTPUT_INVALID",
                "Domain authority result violates its immutable schema",
                502,
            )
        return result

    async def _invoke_local_effect(
        self,
        identity: WorkerIdentity,
        call_id: str,
        declaration: dict,
        payload: Any,
        *,
        operation_id: str,
        timeout_seconds: float,
    ) -> Any:
        service_id = declaration["name"]
        if not isinstance(payload, dict):
            raise ForestError(
                "RUNTIME_SERVICE_INPUT_INVALID", "Effect payload must be an object", 422
            )
        input_validator, output_validator = self._service_schemas[service_id]
        if not input_validator.is_valid(payload):
            raise ForestError(
                "RUNTIME_SERVICE_INPUT_INVALID",
                "Effect payload violates its immutable schema",
                422,
            )
        scope = self._call_scope(identity, call_id)
        generation = self.generation_identity
        if generation is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND", "Effect call has no Generation", 409
            )
        driver = self._effect_drivers.get(service_id)
        if not isinstance(driver, PackageServiceEffectDriver):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_NOT_READY",
                "Package effect driver is not attached to this Generation",
                503,
            )
        route = FrozenServiceRoute(
            service_id=service_id,
            version=_major(declaration["contract"]),
            caller_component_id=self.component_id,
            provider_component_id=self.component_id,
            provider_generation_id=generation.generation_id,
            graph_id=generation.graph_id,
            graph_revision=generation.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        invocation = RuntimeServiceInvocation(
            operation_id,
            timeout_seconds,
            scope.causal_root_kind,
            scope.causal_root_id,
            parent_call_id=call_id,
        )
        return await self._run_service_operation(
            route=route,
            invocation=invocation,
            service_id=service_id,
            input_value=payload,
            durable=declaration["semantics"] == "at-most-once",
            dispatch=lambda _worker, effect_scope, _effect_call_id: driver.invoke(
                payload, scope=effect_scope, invocation=invocation
            ),
            normalize=lambda result: result,
            result_valid=output_validator.is_valid,
            cancellation_requested=(
                scope.cancellation_requested
                if isinstance(scope, PackageJobCallScope)
                else None
            ),
        )

    def _has_capability(self, capability: str) -> bool:
        return (
            capability in self._requested_capabilities
            and capability in self._granted_capabilities
        )

    def _call_scope(self, identity: WorkerIdentity, call_id: str) -> PackageCallScope:
        worker = self._worker
        generation = self.generation_identity
        prepared = self._prepared_binding
        if worker is None or identity != worker.identity:
            raise ForestError(
                "WORKER_IDENTITY_MISMATCH",
                "Capability request belongs to another Worker",
                403,
            )
        if generation is None or prepared is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Package capability request has no bound Generation",
                409,
            )
        causal_root_kind, causal_root_id = worker.call_causal_root(call_id)
        values = dict(
            bot_id=self.bot_id,
            component_id=self.component_id,
            generation_id=generation.generation_id,
            instance_id=prepared.instance_id,
            worker_incarnation=prepared.worker_incarnation,
            call_id=call_id,
            call_operation_id=worker.call_operation_id(call_id),
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
        )
        cancellation = self._job_cancellations.get(call_id)
        return (
            PackageJobCallScope(**values, cancellation_requested=cancellation)
            if cancellation is not None
            else PackageCallScope(**values)
        )

    async def _state_gateway(
        self, identity: WorkerIdentity, call_id: str, action: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        if action == "get":
            if not self._has_capability("state.read"):
                raise ForestError("CAPABILITY_DENIED", "State read is not granted", 403)
            if self._state_queries is None:
                raise ForestError("CAPABILITY_UNAVAILABLE", "State queries are unavailable", 503)
            return await self._state_queries.read_state(
                scope, state_key=request["key"]
            )
        if action == "commit":
            if not self._has_capability("state.write"):
                raise ForestError("CAPABILITY_DENIED", "State write is not granted", 403)
            if self._state_commands is None:
                raise ForestError("CAPABILITY_UNAVAILABLE", "State commands are unavailable", 503)
            return await self._state_commands.commit_state(
                scope,
                state_key=request["key"],
                value=request["value"],
                expected_revision=request["expected_revision"],
                operation_id=request["operation_id"],
            )
        raise ForestError("STATE_REQUEST_INVALID", "Unknown state operation", 422)

    async def _event_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        if not self._has_capability(f"event.publish:{request['topic']}"):
            raise ForestError("CAPABILITY_DENIED", "Event publication is not granted", 403)
        if self._event_publisher is None:
            raise ForestError("CAPABILITY_UNAVAILABLE", "Event authority is unavailable", 503)
        return await self._event_publisher.invoke(
            PublishComponentEvent(
                scope.component_id,
                scope.generation_id,
                request["topic"],
                request["contract"],
                request["payload"],
            ),
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=self._worker.remaining_timeout(
                call_id, _timeout(self.component, "service_timeout_s", 30)
            ),
            parent_call_id=call_id,
        )

    async def _job_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        if not self._has_capability(f"job.schedule:{request['handler_id']}"):
            raise ForestError("CAPABILITY_DENIED", "Job scheduling is not granted", 403)
        if self._component_job_scheduler is None:
            raise ForestError("CAPABILITY_UNAVAILABLE", "ComponentJob authority is unavailable", 503)
        return await self._component_job_scheduler.invoke(
            ScheduleComponentJob(
                scope.component_id,
                scope.generation_id,
                request["handler_id"],
                request["payload"],
                request["delay_seconds"],
            ),
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=self._worker.remaining_timeout(
                call_id, _timeout(self.component, "service_timeout_s", 30)
            ),
            parent_call_id=call_id,
        )

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        if self.state is not PackageGenerationState.NEW:
            raise ForestError(
                "LIFECYCLE_INVALID", "Package Generation was already prepared", 409
            )
        desired = await self.module_configuration.desired(include_value=True)
        schema = self.snapshot.schema(self.component["config_schema"])
        errors = sorted(
            Draft202012Validator(schema).iter_errors(desired.value),
            key=lambda item: tuple(str(part) for part in item.absolute_path),
        )
        if errors:
            raise ForestError(
                "CONFIG_INVALID",
                "Package configuration does not match its immutable schema",
                422,
            )
        try:
            configuration = json.loads(
                json.dumps(
                    desired.value,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                )
            )
        except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
            raise ForestError(
                "CONFIG_INVALID", "Package configuration is not finite JSON", 422
            ) from exc
        if self._configuration_validator is not None:
            validation = self._configuration_validator(configuration)
            if inspect.isawaitable(validation):
                await validation
        self._configuration = configuration
        self._configuration_revision = desired.revision
        identity_seed = hashlib.sha256(
            f"{context.activation_id}\0{self.component_id}\0{self.snapshot.digest}".encode()
        ).hexdigest()
        binding = PreparedGenerationBinding(
            component_id=context.component_id,
            requires=context.requires,
            package_id=context.package_id,
            package_version=context.package_version,
            package_digest=context.package_digest,
            driver=context.driver,
            trust_level=context.trust_level,
            descriptor_digest=context.descriptor_digest,
            route_contract_digest=context.route_contract_digest,
            management_contract_digest=context.management_contract_digest,
            management_ui_digest=context.management_ui_digest,
            config_revision=desired.revision,
            config_payload_id=desired.payload_id,
            config_payload_digest=desired.payload_digest,
            instance_id="instance:" + identity_seed,
            worker_incarnation="worker:" + identity_seed,
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )
        self._prepared_binding = binding
        self.state = PackageGenerationState.PREPARED
        return binding

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if (
            self.state is not PackageGenerationState.PREPARED
            or identity.component_id != self.component_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Package Generation identity does not match its prepared component",
                409,
            )
        self.generation_identity = identity
        self.state = PackageGenerationState.BOUND

    async def start(self) -> None:
        if (
            self.state is not PackageGenerationState.BOUND
            or self.generation_identity is None
            or self._prepared_binding is None
            or self._configuration is None
        ):
            raise ForestError(
                "LIFECYCLE_INVALID", "Package Generation is not ready to start", 409
            )
        worker = self._worker_factory(
            self.snapshot,
            WorkerIdentity(
                self._prepared_binding.instance_id,
                self.generation_identity.generation_id,
                self._prepared_binding.worker_incarnation,
                self.bot_id,
            ),
            self._state_gateway,
            limits=_worker_limits(self.component),
            service_handler=self._service_gateway,
            domain_query_handler=self._domain_query_gateway,
            domain_command_handler=self._domain_command_gateway,
            event_handler=self._event_gateway,
            job_handler=self._job_gateway,
        )
        self._worker = worker
        started_drivers: list[PackageEffectDriver] = []
        try:
            await worker.launch(_timeout(self.component, "prepare_timeout_s", 10))
            await worker.prepare(
                self._configuration,
                timeout=_timeout(self.component, "prepare_timeout_s", 10),
            )
            await worker.start(_timeout(self.component, "start_timeout_s", 10))
            health = await worker.health(_timeout(self.component, "start_timeout_s", 10))
            if health.get("healthy") is not True:
                raise ForestError(
                    "WORKER_UNHEALTHY", "Candidate Worker failed its health contract", 503
                )
            # The Worker may now dispatch, but the Adapter gate and persistent
            # route remain closed until activate_generation runs after commit.
            await worker.activate(_timeout(self.component, "start_timeout_s", 10))
            for declaration in self.component.get("effect_services", ()):
                service_id = declaration["name"]
                factory = self._effect_driver_factories[service_id]
                driver = factory(
                    self._configuration,
                    self._configuration_revision,
                    self.generation_identity,
                    declaration,
                )
                if not isinstance(driver, PackageServiceEffectDriver):
                    raise ForestError(
                        "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                        "Effect driver does not implement its declared port",
                        500,
                        details={"service_id": service_id},
                    )
                started_drivers.append(driver)
                await driver.start()
                self._effect_drivers[service_id] = driver
            if self._owned_work is not None:
                self._owned_work.start()
        except BaseException as start_error:
            self.state = PackageGenerationState.FAILED
            if self._owned_work is not None:
                await self._owned_work.close()
            cleanup_results = await asyncio.gather(
                *(driver.close() for driver in reversed(started_drivers)),
                worker.terminate(),
                return_exceptions=True,
            )
            self._effect_drivers.clear()
            cleanup_errors = [
                type(item).__name__
                for item in cleanup_results
                if isinstance(item, BaseException)
            ]
            if cleanup_errors:
                raise ForestError(
                    "PACKAGE_GENERATION_START_ROLLBACK_FAILED",
                    "Package start failed and one or more owned resources did not close",
                    500,
                    details={
                        "start_error": type(start_error).__name__,
                        "cleanup_errors": cleanup_errors,
                    },
                ) from start_error
            raise
        self.state = PackageGenerationState.WORKER_READY

    def activate_generation(self) -> None:
        if self.state is not PackageGenerationState.WORKER_READY:
            raise ForestError(
                "LIFECYCLE_INVALID", "Only a ready Package Generation can activate", 409
            )
        self.state = PackageGenerationState.ACTIVE

    async def quiesce(self) -> None:
        if self.state is not PackageGenerationState.ACTIVE or self._worker is None:
            raise ForestError(
                "LIFECYCLE_INVALID", "Only an active Package Generation can quiesce", 409
            )
        self.state = PackageGenerationState.QUIESCING
        try:
            timeout = _timeout(self.component, "drain_timeout_s", 10)
            async with asyncio.timeout(timeout):
                await self._worker.quiesce(timeout=timeout)
                await self._services_drained.wait()
                for driver in self._effect_drivers.values():
                    await driver.quiesce()
        except BaseException:
            self.state = PackageGenerationState.FAILED
            raise
        self.state = PackageGenerationState.QUIESCENT

    async def resume(self) -> None:
        if self.state is not PackageGenerationState.QUIESCENT or self._worker is None:
            raise ForestError(
                "LIFECYCLE_INVALID", "Only a quiescent Package Generation can resume", 409
            )
        try:
            for driver in self._effect_drivers.values():
                await driver.resume()
            await self._worker.resume(_timeout(self.component, "start_timeout_s", 10))
            health = await self._worker.health(
                _timeout(self.component, "start_timeout_s", 10)
            )
            if health.get("healthy") is not True:
                raise ForestError(
                    "WORKER_UNHEALTHY", "Resumed Worker failed its health contract", 503
                )
        except BaseException:
            self.state = PackageGenerationState.FAILED
            raise
        self.state = PackageGenerationState.WORKER_READY

    async def close(self) -> None:
        if self.state is PackageGenerationState.STOPPED:
            return
        if self.state is PackageGenerationState.ACTIVE:
            raise ForestError(
                "LIFECYCLE_INVALID",
                "An active Package Generation must quiesce before cleanup",
                409,
            )
        if self._owned_work is not None:
            await self._owned_work.close()
        worker = self._worker
        close_operations = [driver.close() for driver in self._effect_drivers.values()]
        if worker is not None:
            close_operations.append(
                worker.stop(timeout=_timeout(self.component, "stop_timeout_s", 5))
            )
        results = await asyncio.gather(*close_operations, return_exceptions=True)
        self._effect_drivers.clear()
        self.state = PackageGenerationState.STOPPED
        failures = [type(item).__name__ for item in results if isinstance(item, BaseException)]
        if failures:
            raise ForestError(
                "PACKAGE_GENERATION_CLOSE_FAILED",
                "One or more Generation-owned resources failed to close",
                500,
                details={"failures": failures},
            )


def package_runtime_descriptor(
    snapshot: PackageSnapshot,
    *,
    bot_id: str,
    module_configuration: Any,
    grants: Any,
    resolved_contracts: Mapping[str, str],
    startup_components: tuple[str, ...],
    generation_verifier: GenerationVerifier,
    generation_member_verifier: GenerationVerifier,
    state_queries: PackageStateQueries | None,
    state_commands: PackageStateCommands | None,
    domain_queries: PackageDomainQueries | None = None,
    domain_commands: PackageDomainCommands | None = None,
    unit_of_work_authority: PackageUnitOfWorkAuthority | None = None,
    owned_work_authority: PackageOwnedWorkAuthority | None = None,
    service_operation_ledger: PackageServiceOperationLedger,
    configuration_validator: Callable[[object], Any] | None = None,
    effect_driver_factories: Mapping[str, PackageEffectDriverFactory] | None = None,
    storage=None,
    management=(),
    management_contract=None,
    worker_factory: Callable[..., PythonWorker] = PythonWorker,
    trust_level: str = "installed",
) -> RuntimeModuleDescriptor:
    """Compile one package snapshot without importing its Python entrypoint."""

    snapshot.verify()
    component = snapshot.component
    if (
        component.get("storage", {}).get("execution_profile") == "official"
        and trust_level != "official"
    ):
        raise ForestError(
            "PACKAGE_STORAGE_TRUST_REQUIRED",
            "Official storage execution requires an official package trust binding",
            403,
        )
    effect_driver_factories = dict(effect_driver_factories or {})
    declared_effect_services = {
        item["name"]
        for item in component.get("effect_services", ())
    }
    if set(effect_driver_factories) != declared_effect_services:
        raise ForestError(
            "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
            "Effect driver bindings must exactly match declared effect services",
            500,
            details={
                "declared": sorted(declared_effect_services),
                "bound": sorted(effect_driver_factories),
            },
        )
    requested = frozenset(component.get("capabilities", {}).get("requests", ()))
    gateway_capabilities = {
        item
        for item in requested
        if not (
            item.startswith("service.call:")
            or item.startswith("event.publish:")
            or item.startswith("job.schedule:")
        )
    }
    installed_capabilities = frozenset(
        capability
        for capability, installed in (
            ("state.read", state_queries is not None),
            ("state.write", state_commands is not None),
        )
        if installed
    ) | frozenset(
        f"domain.query:{item['name']}"
        for item in component.get("domain_queries", ())
        if domain_queries is not None
    ) | frozenset(
        f"domain.command:{item['name']}"
        for item in component.get("domain_commands", ())
        if domain_commands is not None
    )
    unsupported = gateway_capabilities - installed_capabilities
    if unsupported:
        raise ForestError(
            "PACKAGE_CAPABILITY_PORT_UNAVAILABLE",
            "Package requests a host capability with no installed authority",
            503,
            details={"capabilities": sorted(unsupported)},
        )
    if component.get("unit_of_work_participants") and unit_of_work_authority is None:
        raise ForestError(
            "PACKAGE_UOW_AUTHORITY_UNAVAILABLE",
            "Package declares UnitOfWork participants without an installed authority",
            503,
        )
    if component.get("owned_work_handlers") and owned_work_authority is None:
        raise ForestError(
            "PACKAGE_OWNED_WORK_AUTHORITY_UNAVAILABLE",
            "Package declares owned work without an installed authority",
            503,
        )

    event_subscriptions = tuple(
        RuntimeEventSubscriptionContribution(
            subscriber_id=item["subscriber_id"],
            topic=item["topic"],
            contract=item["contract"],
            event_schema=snapshot.schema(item["schema"]),
            service_id=_handler_service_id(
                "event", component["id"], item["subscriber_id"]
            ),
            max_attempts=item.get("max_attempts", 3),
        )
        for item in component.get("subscribes", ())
    )
    event_publications = tuple(
        RuntimeEventPublicationContribution(
            topic=item["topic"],
            contract=item["contract"],
            event_schema=snapshot.schema(item["schema"]),
        )
        for item in component.get("publishes", ())
    )
    component_jobs = tuple(
        RuntimeComponentJobContribution(
            handler_id=item["handler_id"],
            input_schema=snapshot.schema(item["input_schema"]),
            service_id=_handler_service_id(
                "job", component["id"], item["handler_id"]
            ),
            max_attempts=item.get("max_attempts", 3),
            timeout_seconds=item.get("timeout_s", 30),
            max_concurrency=item.get("max_concurrency", 1),
        )
        for item in component.get("jobs", ())
    )
    job_types = tuple(
        RuntimeJobTypeContribution(
            job_type=item["job_type"],
            job_version=item["job_version"],
            service_id=item["name"],
            service_version=_major(item["contract"]),
            input_schema=snapshot.schema(item["input_schema"]),
            max_attempts=item["max_attempts"],
            timeout_seconds=item["timeout_s"],
            max_concurrency=item["max_concurrency"],
        )
        for item in component.get("job_types", ())
    )

    def factory(_runtime, contracts):
        routes = {
            name: contracts.service(
                name,
                _major(contract),
                admission_lane="DATA",
                owned_work=True,
            )
            for name, contract in resolved_contracts.items()
        }
        event_publisher = (
            contracts.service(
                "routing.event.publish", 1, admission_lane="DATA", owned_work=True
            )
            if any(item.startswith("event.publish:") for item in requested)
            else None
        )
        component_job_scheduler = (
            contracts.service(
                "routing.component-job.schedule",
                1,
                admission_lane="DATA",
                owned_work=True,
            )
            if any(item.startswith("job.schedule:") for item in requested)
            else None
        )
        adapter = PackageGenerationAdapter(
            snapshot=snapshot,
            bot_id=bot_id,
            module_configuration=module_configuration,
            grants=grants,
            service_routes=routes,
            resolved_contracts=resolved_contracts,
            generation_verifier=generation_verifier,
            generation_member_verifier=generation_member_verifier,
            state_queries=state_queries,
            state_commands=state_commands,
            domain_queries=domain_queries,
            domain_commands=domain_commands,
            unit_of_work_authority=unit_of_work_authority,
            owned_work_authority=owned_work_authority,
            service_operation_ledger=service_operation_ledger,
            configuration_validator=configuration_validator,
            effect_driver_factories=effect_driver_factories,
            event_publisher=event_publisher,
            component_job_scheduler=component_job_scheduler,
            worker_factory=worker_factory,
        )
        if management_contract is not None:
            adapter.management_contract = management_contract
        return adapter

    return RuntimeModuleDescriptor(
        component_id=component["id"],
        requires=tuple(
            dict.fromkeys(
                (
                    *startup_components,
                    *(
                        ("routing.graph",)
                        if (
                            component.get("publishes")
                            or component.get("subscribes")
                            or component.get("jobs")
                        )
                        else ()
                    ),
                )
            )
        ),
        factory=factory,
        artifact=RuntimeArtifactIdentity(
            package_id=snapshot.manifest["package_id"],
            package_version=snapshot.manifest["version"],
            package_digest=snapshot.digest,
            scope=component["scope"],
            driver="forest.python-worker/1",
            trust_level=trust_level,
        ),
        management=tuple(management),
        storage=storage,
        event_publications=event_publications,
        event_subscriptions=event_subscriptions,
        component_jobs=component_jobs,
        job_types=job_types,
        wakes_on_work=bool(component.get("owned_work_handlers")),
        owned_work_lifecycle=(
            OwnedWorkLifecycle.FREEZE_DRAIN
            if component.get("owned_work_handlers")
            else OwnedWorkLifecycle.NONE
        ),
    )
