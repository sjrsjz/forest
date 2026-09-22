"""Bind immutable plugin packages to the kernel Generation lifecycle.

This module is kernel infrastructure.  It never imports plugin entrypoints in the
host process; only the selected RuntimeDriver may do that inside its Worker.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

from forest.drivers.python_worker import PythonWorker, WorkerIdentity, WorkerLimits
from forest.errors import ForestError
from forest.packages import PackageSnapshot, contract_range
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RoutedServiceEndpoint,
    RuntimeServiceInvocation,
)
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.runtime_modules import RuntimeArtifactIdentity, RuntimeModuleDescriptor


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


StateHandler = Callable[[WorkerIdentity, str, str, dict], Awaitable[Any]]
EventHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
JobHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
GenerationVerifier = Callable[..., Awaitable[Any]]


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
        state_handler: StateHandler | None = None,
        event_handler: EventHandler | None = None,
        job_handler: JobHandler | None = None,
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
        self._state_handler = state_handler
        self._event_handler = event_handler
        self._job_handler = job_handler
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
            for item in self.component.get("provides", ())
        }

        services = [
            {"service_id": item["name"], "version": _major(item["contract"])}
            for item in self.component.get("provides", ())
        ]
        requirements = [
            {"service_id": name, "version": _major(contract)}
            for name, contract in sorted(self._resolved_contracts.items())
        ]
        self.runtime_contract = {
            "protocol": "forest.runtime-services/1",
            "services": services,
            "requires": requirements,
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
        return {
            (item["name"], _major(item["contract"])): self._service_handler(item["name"])
            for item in self.component.get("provides", ())
        }

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
            if self.state is not PackageGenerationState.ACTIVE or self._worker is None:
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_ACTIVE",
                    "Package Generation is not accepting calls",
                    409,
                )
            if (
                route.provider_component_id != self.component_id
                or (
                    self.generation_identity is not None
                    and route.provider_generation_id
                    != self.generation_identity.generation_id
                )
            ):
                raise ForestError(
                    "RUNTIME_SERVICE_ROUTE_STALE",
                    "Package invocation route does not identify this Generation",
                    409,
                )
            result = await self._worker.invoke(
                service_id,
                payload,
                # A call identifies one execution attempt.  Idempotency belongs
                # to operation_id, so retries and concurrent duplicate delivery
                # must never contend for the same live call slot.
                call_id="call:" + uuid4().hex,
                operation_id=invocation.operation_id,
                timeout=invocation.timeout_seconds,
                before_send=lambda: self._verify_dispatch(route),
            )
            if not output_validator.is_valid(result):
                raise ForestError(
                    "RUNTIME_SERVICE_OUTPUT_INVALID",
                    "Plugin service result violates its immutable schema",
                    502,
                )
            return result

        async def routed(route, invocation, *args, **kwargs):
            return await invoke(route, invocation, *args, **kwargs)

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
        if self._worker is None or identity != self._worker.identity:
            raise ForestError(
                "WORKER_IDENTITY_MISMATCH",
                "Service request belongs to another Worker",
                403,
            )
        service_id = request["service"]
        route = self._service_routes.get(service_id)
        contract = self._resolved_contracts.get(service_id)
        requested = set(self.component.get("capabilities", {}).get("requests", ()))
        if (
            route is None
            or contract is None
            or request["contract_major"] != _major(contract)
            or f"service.call:{service_id}" not in requested
        ):
            raise ForestError(
                "CAPABILITY_DENIED",
                "Worker requested an undeclared service capability",
                403,
            )
        timeout = self._worker.remaining_timeout(call_id, request["timeout"])
        return await route.invoke(
            request["payload"],
            operation_id=request["operation_id"],
            timeout_seconds=timeout,
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
            self._state_handler,
            limits=_worker_limits(self.component),
            service_handler=self._service_gateway,
            event_handler=self._event_handler,
            job_handler=self._job_handler,
        )
        self._worker = worker
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
        except BaseException:
            self.state = PackageGenerationState.FAILED
            await worker.terminate()
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
            await self._worker.quiesce(
                timeout=_timeout(self.component, "drain_timeout_s", 10)
            )
        except BaseException:
            self.state = PackageGenerationState.FAILED
            raise
        self.state = PackageGenerationState.QUIESCENT

    async def resume(self) -> None:
        if self.state is not PackageGenerationState.QUIESCENT or self._worker is None:
            raise ForestError(
                "LIFECYCLE_INVALID", "Only a quiescent Package Generation can resume", 409
            )
        await self._worker.resume(_timeout(self.component, "start_timeout_s", 10))
        health = await self._worker.health(_timeout(self.component, "start_timeout_s", 10))
        if health.get("healthy") is not True:
            self.state = PackageGenerationState.FAILED
            raise ForestError(
                "WORKER_UNHEALTHY", "Resumed Worker failed its health contract", 503
            )
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
        worker = self._worker
        try:
            if worker is not None:
                await worker.stop(
                    timeout=_timeout(self.component, "stop_timeout_s", 5)
                )
        finally:
            self.state = PackageGenerationState.STOPPED


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
    storage=None,
    management=(),
    management_contract=None,
    state_handler: StateHandler | None = None,
    event_handler: EventHandler | None = None,
    job_handler: JobHandler | None = None,
    worker_factory: Callable[..., PythonWorker] = PythonWorker,
    trust_level: str = "installed",
) -> RuntimeModuleDescriptor:
    """Compile one package snapshot without importing its Python entrypoint."""

    snapshot.verify()
    component = snapshot.component

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
        adapter = PackageGenerationAdapter(
            snapshot=snapshot,
            bot_id=bot_id,
            module_configuration=module_configuration,
            grants=grants,
            service_routes=routes,
            resolved_contracts=resolved_contracts,
            generation_verifier=generation_verifier,
            generation_member_verifier=generation_member_verifier,
            state_handler=state_handler,
            event_handler=event_handler,
            job_handler=job_handler,
            worker_factory=worker_factory,
        )
        if management_contract is not None:
            adapter.management_contract = management_contract
        return adapter

    return RuntimeModuleDescriptor(
        component_id=component["id"],
        requires=startup_components,
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
    )
