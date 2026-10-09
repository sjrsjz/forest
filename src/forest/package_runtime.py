"""Bind immutable plugin packages to the kernel Generation lifecycle.

This module is kernel infrastructure.  It never imports plugin entrypoints in the
host process; only the selected RuntimeDriver may do that inside its Worker.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

from forest.drivers.python_worker import PythonWorker, WorkerIdentity
from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.package_capabilities import (
    PackageAgentToolEffectAuthority,
    PackageCallScope,
    PackageDomainCommands,
    PackageDomainQueries,
    PackageJobCallScope,
    PackageOwnedWorkAuthority,
    PackageOwnedWorkCallScope,
    PackagePayloadAuthority,
    PackageServiceError,
    PackageServiceOperationLedger,
    PackageServiceRoute,
    PackageStateCommands,
    PackageStateQueries,
    PackageUnitOfWorkAuthority,
    PackageUnitOfWorkCoordinator,
)
from forest.package_contribution_directories import FrozenContributionDirectories
from forest.package_effects import (
    PackageArtifactDownload,
    PackageArtifactDownloadDriver,
    PackageArtifactUploadDriver,
    PackageEffectDriver,
    PackageEffectDriverFactory,
    PackageIngressEffectDriver,
    PackageServiceEffectDriver,
)
from forest.package_owned_work import PackageOwnedWorkSupervisor
from forest.packages import PackageSnapshot, contract_range, digest_json
from forest.routing_protocol import DurableComponentJobRequest, DurableEventDeliveryRequest
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RoutedServiceEndpoint,
    RuntimeServiceInvocation,
)
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.runtime_modules import (
    OwnedWorkLifecycle,
    RuntimeAgentToolContribution,
    RuntimeArtifactIdentity,
    RuntimeComponentJobContribution,
    RuntimeEventPublicationContribution,
    RuntimeEventSubscriptionContribution,
    RuntimeFactMatch,
    RuntimeJobTypeContribution,
    RuntimeModuleDescriptor,
    RuntimeShellCommandContribution,
)
from forest.scope_identity import bind_scope_identity
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)
from forest.unit_of_work import ComponentConfigurationApplyScope
from forest.worker_limits import WorkerLimits


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
_PAYLOAD_CHUNK_BYTES = 96 * 1024
_PAYLOAD_STAGING_BYTES = 32 * 1024 * 1024


def _component_schema_references(component: Mapping[str, Any]) -> tuple[str, ...]:
    references: list[str] = [component["config_schema"]]
    state_value_schema = component.get("state_value_schema")
    if state_value_schema is not None:
        references.append(state_value_schema)
    replacement_facts_schema = component.get("lifecycle", {}).get(
        "replacement_facts_schema"
    )
    if replacement_facts_schema is not None:
        references.append(replacement_facts_schema)
    for declarations in (
        component.get("provides", ()),
        component.get("effect_services", ()),
        component.get("domain_queries", ()),
        component.get("domain_commands", ()),
        component.get("kernel_queries", ()),
        component.get("kernel_commands", ()),
        component.get("job_types", ()),
        component.get("unit_of_work_plans", ()),
        component.get("unit_of_work_participants", ()),
        component.get("owned_work_handlers", ()),
    ):
        for declaration in declarations:
            references.extend(
                (declaration["input_schema"], declaration["output_schema"])
            )
    for declaration in component.get("effect_services", ()):
        ingress = declaration.get("ingress")
        if ingress is not None:
            references.extend((ingress["input_schema"], ingress["output_schema"]))
    for declaration in (
        *component.get("subscribes", ()),
        *component.get("publishes", ()),
    ):
        references.append(declaration["schema"])
    for declaration in component.get("jobs", ()):
        references.append(declaration["input_schema"])
    for declaration in component.get("shell_commands", ()):
        references.append(declaration["argument_schema"])
    for declaration in component.get("agent_tools", ()):
        references.append(declaration["declaration_schema"])
    return tuple(dict.fromkeys(references))


@dataclass
class _StagedPayloadWrite:
    owner_kind: str
    owner_id: str
    media_type: str
    byte_length: int
    digest: str
    parents: tuple[dict, ...]
    content: bytearray = field(default_factory=bytearray)


class PackageUnitOfWorkParticipant:
    """Generation-bound package participant exposed only to kernel coordinators."""

    __slots__ = ("component_id", "generation_id", "_invoke", "_read_only_names")

    def __init__(
        self,
        component_id: str,
        generation_id: str,
        invoke: ParticipantInvoker,
        read_only_names: frozenset[str] = frozenset(),
    ) -> None:
        self.component_id = component_id
        self.generation_id = generation_id
        self._invoke = invoke
        self._read_only_names = read_only_names

    def supports_read_only(self, name: str) -> bool:
        return name in self._read_only_names

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
        read_only: bool = False,
    ) -> Any:
        if read_only and not self.supports_read_only(name):
            raise ForestError(
                "PACKAGE_UOW_READ_PARTICIPANT_INVALID",
                "Read-only UnitOfWork plan requires a declared read-only participant",
                409,
            )
        invocation = RuntimeServiceInvocation(
            operation_id=operation_id,
            timeout_seconds=timeout_seconds,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            parent_call_id=parent_call_id,
            read_only=read_only,
        )
        return await self._invoke(conn, self.generation_id, name, payload, invocation)


def _major(contract: str) -> int:
    lower, _upper = contract_range(contract)
    return lower[0]


def _timeout(component: Mapping[str, Any], name: str, default: float) -> float:
    return float(component.get("lifecycle", {}).get(name, default))


def _handler_service_id(kind: str, component_id: str, handler_id: str) -> str:
    identity = hashlib.sha256(f"{component_id}\0{handler_id}".encode()).hexdigest()
    return f"component.{kind}.{identity}"


class PackageGenerationAdapter:
    """One generic host-side handle for one immutable package Generation."""

    requires_replacement_quiesce = True

    def schema_snapshot(self, reference: str) -> dict:
        """Return an isolated copy of a schema frozen at Generation construction."""

        return deepcopy(self._compiled_schemas[reference])

    def __init__(
        self,
        *,
        snapshot: PackageSnapshot,
        bot_id: str | None,
        scope_id: str | None = None,
        module_configuration: Any,
        prepared_configuration=None,
        grants: Any,
        service_routes: Mapping[str, Any],
        resolved_contracts: Mapping[str, str],
        generation_verifier: GenerationVerifier,
        generation_member_verifier: GenerationVerifier,
        state_queries: PackageStateQueries | None,
        state_commands: PackageStateCommands | None,
        payload_authority: PackagePayloadAuthority | None = None,
        domain_queries: PackageDomainQueries | None = None,
        domain_commands: PackageDomainCommands | None = None,
        kernel_management_queries=None,
        kernel_management_commands=None,
        unit_of_work_authority: PackageUnitOfWorkAuthority | None = None,
        unit_of_work_coordinator: PackageUnitOfWorkCoordinator | None = None,
        configuration_apply_coordinator=None,
        management_action_preflights: Mapping[str, Callable] | None = None,
        shell_management_authorizer: Callable | None = None,
        owned_work_authority: PackageOwnedWorkAuthority | None = None,
        service_operation_ledger: PackageServiceOperationLedger,
        configuration_validator: Callable[[object], Any] | None = None,
        effect_driver_factories: Mapping[str, PackageEffectDriverFactory] | None = None,
        event_publisher=None,
        component_job_scheduler=None,
        contribution_directories: Mapping[str, tuple] | None = None,
        agent_tool_effects: PackageAgentToolEffectAuthority | None = None,
        compiled_schemas: Mapping[str, dict] | None = None,
        worker_factory: Callable[..., PythonWorker] = PythonWorker,
    ) -> None:
        self.snapshot = snapshot
        self.component = snapshot.component
        self.component_id = self.component["id"]
        self.bot_id = bot_id
        self.scope_kind = self.component["scope"]
        self.scope_id = bind_scope_identity(
            scope_kind=self.scope_kind, scope_id=scope_id, bot_id=bot_id
        )
        self.module_configuration = module_configuration
        self._prepared_configuration = prepared_configuration
        self.grants = grants
        self._service_routes = dict(service_routes)
        self._resolved_contracts = dict(resolved_contracts)
        self._generation_verifier = generation_verifier
        self._generation_member_verifier = generation_member_verifier
        self._state_queries = state_queries
        self._state_commands = state_commands
        self._payload_authority = payload_authority
        self._payload_uploads: dict[tuple[str, str], _StagedPayloadWrite] = {}
        self._payload_reserved_bytes = 0
        self._domain_queries = domain_queries
        self._domain_commands = domain_commands
        self._kernel_management_queries = kernel_management_queries
        self._kernel_management_commands = kernel_management_commands
        self._unit_of_work_authority = unit_of_work_authority
        self._unit_of_work_coordinator = unit_of_work_coordinator
        self._configuration_apply_coordinator = configuration_apply_coordinator
        self._management_action_preflights = dict(management_action_preflights or {})
        self._shell_management_authorizer = shell_management_authorizer
        self._management_actions = {
            item["id"]: item
            for item in self.component.get("management_actions", ())
        }
        self._service_operation_ledger = service_operation_ledger
        self._configuration_validator = configuration_validator
        self._effect_driver_factories = dict(effect_driver_factories or {})
        self._effect_drivers: dict[str, PackageEffectDriver] = {}
        self._effect_ingress = {
            item["ingress"]["name"]: (item, item["ingress"])
            for item in self.component.get("effect_services", ())
            if item.get("ingress") is not None
        }
        self._job_cancellations: dict[str, Callable[[], Awaitable[bool]]] = {}
        self._owned_work_calls: set[str] = set()
        self._writable_calls: set[str] = set()
        self._event_publisher = event_publisher
        self._component_job_scheduler = component_job_scheduler
        self._contribution_directories = FrozenContributionDirectories(
            declarations=self.component.get("contribution_directories", ()),
            routes=contribution_directories or {},
            domain_queries=domain_queries,
            domain_query_declarations=self.component.get("domain_queries", ()),
            agent_tool_effects=agent_tool_effects,
        )
        self._worker_factory = worker_factory
        self._compiled_schemas = dict(
            compiled_schemas
            if compiled_schemas is not None
            else snapshot.schemas(_component_schema_references(self.component))
        )
        facts_schema = self.component.get("lifecycle", {}).get(
            "replacement_facts_schema"
        )
        self._replacement_facts_validator = (
            Draft202012Validator(self._compiled_schemas[facts_schema])
            if facts_schema is not None else None
        )
        self._worker: PythonWorker | None = None
        self._configuration: dict[str, Any] | None = None
        self._configuration_revision: int | None = None
        self._prepared_binding: PreparedGenerationBinding | None = None
        self._replacement_facts: dict = {}
        self.generation_identity: BoundGenerationIdentity | None = None
        self.state = PackageGenerationState.NEW
        self._service_schemas = {
            item["name"]: (
                Draft202012Validator(self._compiled_schemas[item["input_schema"]]),
                Draft202012Validator(self._compiled_schemas[item["output_schema"]]),
            )
            for item in (
                *self.component.get("provides", ()),
                *self.component.get("effect_services", ()),
                *self.component.get("domain_queries", ()),
                *self.component.get("domain_commands", ()),
                *self.component.get("kernel_queries", ()),
                *self.component.get("kernel_commands", ()),
                *self.component.get("job_types", ()),
                *self.component.get("unit_of_work_plans", ()),
            )
        }
        self._service_schemas.update(
            {
                ingress["name"]: (
                    Draft202012Validator(self._compiled_schemas[ingress["input_schema"]]),
                    Draft202012Validator(self._compiled_schemas[ingress["output_schema"]]),
                )
                for _effect, ingress in self._effect_ingress.values()
            }
        )
        self._provided_services = {
            item["name"]: item for item in self.component.get("provides", ())
        }
        self._participant_declarations = {
            item["name"]: (
                item,
                Draft202012Validator(self._compiled_schemas[item["input_schema"]]),
                Draft202012Validator(self._compiled_schemas[item["output_schema"]]),
            )
            for item in self.component.get("unit_of_work_participants", ())
        }
        self._owned_work_declarations = tuple(
            self.component.get("owned_work_handlers", ())
        )
        self._owned_work_schemas = {
            item["name"]: (
                Draft202012Validator(self._compiled_schemas[item["input_schema"]]),
                Draft202012Validator(self._compiled_schemas[item["output_schema"]]),
            )
            for item in self._owned_work_declarations
        }
        self._event_subscriptions = {
            item["subscriber_id"]: (
                item,
                Draft202012Validator(self._compiled_schemas[item["schema"]]),
                _handler_service_id("event", self.component_id, item["subscriber_id"]),
            )
            for item in self.component.get("subscribes", ())
        }
        self._component_jobs = {
            item["handler_id"]: (
                item,
                Draft202012Validator(self._compiled_schemas[item["input_schema"]]),
                _handler_service_id("job", self.component_id, item["handler_id"]),
            )
            for item in self.component.get("jobs", ())
        }
        self._shell_commands = {
            (item["command_id"], item["command_version"]): (
                item,
                Draft202012Validator(self._compiled_schemas[item["argument_schema"]]),
            )
            for item in self.component.get("shell_commands", ())
        }
        self._requested_capabilities = frozenset(
            self.component.get("capabilities", {}).get("requests", ())
        )
        self._service_inflight = 0
        self._artifact_inflight = 0
        self._services_drained = asyncio.Event()
        self._services_drained.set()
        self.failure: str | None = None
        self._replacement_operation_id: str | None = None
        self._recovered_generation = False
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
                scope_kind=self.scope_kind,
                scope_id=self.scope_id,
                component_id=self.component_id,
                declarations=self._owned_work_declarations,
                schemas=self._owned_work_schemas,
                authority=owned_work_authority,
                operation_ledger=self._service_operation_ledger,
                binding=self._owned_work_binding,
                generation_member_verifier=self._generation_member_verifier,
                active=lambda: self.state is PackageGenerationState.ACTIVE,
                recovered=lambda: self._recovered_generation,
                failed=self._owned_work_failed,
                call_started=self._begin_owned_work_call,
                call_finished=self._finish_owned_work_call,
            )
            self.freeze_owned_admission = self._owned_work.freeze_admission
            self.drain_owned_work = self._drain_owned_work
            self.open_owned_admission = self._owned_work.open_admission
        self._granted_capabilities = frozenset(
            item.capability
            for item in self.grants.entries
            if item.resource_kind == "component" and item.resource_id == self.component_id
        )

        services = [
            {
                "service_id": item["name"],
                "version": _major(item["contract"]),
                "semantics": item.get("semantics", "at-most-once"),
            }
            for item in (
                *(item for item in self.component.get("provides", ())
                  if item.get("visibility", "public") == "public"),
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
        requirements.extend(self._contribution_directories.requirements())
        requirements = list(
            {
                (item["service_id"], item["version"]): item
                for item in requirements
            }.values()
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
                    "read_only": item.get("read_only", False),
                }
                for item in self.component.get("unit_of_work_participants", ())
            ],
            "unit_of_work_plans": [
                {
                    "name": item["name"],
                    "version": _major(item["contract"]),
                    "read_only": item.get("read_only", False),
                    "steps": [dict(step) for step in item["steps"]],
                }
                for item in self.component.get("unit_of_work_plans", ())
            ],
            "owned_work_handlers": [
                {
                    "name": item["name"],
                    "version": _major(item["contract"]),
                    "handler": item["handler"],
                    "max_concurrency": item["max_concurrency"],
                    "drain_policy": item.get("drain_policy", "inflight"),
                }
                for item in self._owned_work_declarations
            ],
            "contribution_directories": sorted(
                self._contribution_directories.kinds
            ),
        }
        self.state_schema = self.component["state_schema"]
        state_schema_ref = self.component.get("state_value_schema")
        self.state_contract = (
            self._compiled_schemas[state_schema_ref]
            if state_schema_ref is not None
            else {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            }
        )
        self._state_value_validator = (
            Draft202012Validator(self.state_contract)
            if state_schema_ref is not None else None
        )

    def runtime_service_handlers(self) -> dict[tuple[str, int], RoutedServiceEndpoint]:
        handlers = {
            (item["name"], _major(item["contract"])): self._service_handler(item["name"])
            for item in self.component.get("provides", ())
            if item.get("visibility", "public") == "public"
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
        handlers.update(
            {
                (item["service_id"], item["service_version"]): (
                    self._shell_command_handler(item["service_id"])
                )
                for item in self.component.get("shell_commands", ())
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

    async def apply_configuration(
        self,
        *,
        operation_id: str,
        plan_id: str,
        expected_desired_revision: int,
        authorization,
        authorization_guard,
        authorization_replay_guard,
    ) -> dict:
        """Run this Generation's declared standard configuration transaction."""

        identity = self.generation_identity
        binding = self._prepared_binding
        coordinator = self._configuration_apply_coordinator
        if (
            self.component.get("configuration_apply") != "worker-host/1"
            or identity is None
            or binding is None
            or coordinator is None
            or self.state is not PackageGenerationState.ACTIVE
        ):
            raise ForestError(
                "PACKAGE_CONFIGURATION_APPLY_UNAVAILABLE",
                "Active package Generation does not publish configuration apply",
                409,
            )
        self._service_inflight += 1
        self._services_drained.clear()
        try:
            return await coordinator.execute(
                ComponentConfigurationApplyScope(
                    component_id=self.component_id,
                    generation_id=identity.generation_id,
                    call_id="call:" + uuid4().hex,
                    operation_id=operation_id,
                ),
                operation_id=operation_id,
                plan_id=plan_id,
                expected_desired_revision=expected_desired_revision,
                authorization=authorization,
                authorization_guard=authorization_guard,
                authorization_replay_guard=authorization_replay_guard,
            )
        finally:
            self._service_inflight -= 1
            if self._service_inflight == 0:
                self._services_drained.set()

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
            frozenset(
                name for name, (declaration, _input, _output)
                in self._participant_declarations.items()
                if declaration.get("read_only", False)
            ),
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
            scope_kind=self.scope_kind,
            scope_id=self.scope_id,
            component_id=self.component_id,
            generation_id=identity.generation_id,
            instance_id=prepared.instance_id,
            worker_incarnation=prepared.worker_incarnation,
            call_id="call:" + uuid4().hex,
            call_operation_id=invocation.operation_id,
            causal_root_kind=invocation.causal_root_kind,
            causal_root_id=invocation.causal_root_id,
            recovered_generation=self._recovered_generation,
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

    async def begin_replacement(self, operation_id: str) -> None:
        if self._replacement_operation_id not in (None, operation_id):
            raise ForestError(
                "RUNTIME_REPLACEMENT_OPERATION_CONFLICT",
                "Package Generation is already draining for another replacement",
                409,
            )
        self._replacement_operation_id = operation_id
        self._recovered_generation = False
        if self._worker is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Replacement requires a bound Package Worker",
                409,
            )
        await self._worker.prepare_replacement(
            operation_id, timeout=_timeout(self.component, "drain_timeout_s", 10)
        )

    async def abort_replacement(self, operation_id: str) -> None:
        if self._replacement_operation_id != operation_id:
            return
        if self._worker is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Replacement requires a bound Package Worker",
                409,
            )
        await self._worker.abort_replacement(
            operation_id, timeout=_timeout(self.component, "drain_timeout_s", 10)
        )
        self._replacement_operation_id = None
        self._recovered_generation = True

    def replacement_candidate_fact(self) -> dict:
        identity = self.generation_identity
        if self._prepared_binding is None or identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Candidate replacement fact requires a prepared Generation", 409,
            )
        return {
            "generation_id": identity.generation_id,
            "facts": deepcopy(self._replacement_facts),
        }

    async def validate_replacement(
        self, replaced_components: tuple[str, ...], candidate_components: dict
    ) -> None:
        query_name = self.component.get("lifecycle", {}).get(
            "replacement_guard_query"
        )
        if query_name is None:
            return
        declaration = next(
            item for item in self.component["domain_queries"]
            if item["name"] == query_name
        )
        result = await self.invoke_management_query(
            declaration,
            {
                "replaced_components": list(replaced_components),
                "candidate_components": candidate_components,
            },
            timeout_seconds=10,
        )
        if result["safe"] is not True:
            raise ForestError(
                "RUNTIME_REPLACEMENT_GUARD_REJECTED",
                "Package has unresolved work bound to a replacing Generation",
                409,
                details={
                    "component_id": self.component_id,
                    "blocker_count": result["blocker_count"],
                },
            )

    async def _drain_owned_work(self) -> None:
        assert self._owned_work is not None
        try:
            async with asyncio.timeout(_timeout(self.component, "drain_timeout_s", 10)):
                await self._owned_work.drain()
        except TimeoutError as error:
            raise ForestError(
                "RUNTIME_OWNED_DRAIN_TIMEOUT",
                "Package owned work exceeded its declared drain deadline",
                504,
                details={"component_id": self.component_id},
            ) from error

    async def _synchronize_contribution_directories(self) -> None:
        identity = self.generation_identity
        prepared = self._prepared_binding
        if identity is None:
            if not self._contribution_directories.kinds:
                return
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Contribution directory has no bound Generation",
                500,
            )
        requests = self._contribution_directories.synchronization_requests(
            identity, configuration=self._configuration or {}
        )
        if not requests:
            return
        if identity is None or prepared is None or self._domain_commands is None:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Contribution directory synchronization authority is unavailable",
                500,
            )
        declarations = {
            item["name"]: item
            for item in self.component.get("domain_commands", ())
        }
        if self._configuration is None:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Contribution directory synchronization has no frozen configuration",
                500,
            )
        for command_name, payload in requests:
            declaration = declarations[command_name]
            input_validator, output_validator = self._service_schemas[command_name]
            if not input_validator.is_valid(payload):
                raise ForestError(
                    "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                    "Frozen contribution directory violates its synchronization port",
                    500,
                )
            operation_id = "directory-sync:" + hashlib.sha256(
                f"{identity.generation_id}\0{payload['kind']}".encode("utf-8")
            ).hexdigest()
            call_id = "call:" + uuid4().hex
            await self._domain_commands.invoke_preparation_command(
                PackageCallScope(
                    bot_id=self.bot_id,
                    scope_kind=self.scope_kind,
                    scope_id=self.scope_id,
                    component_id=self.component_id,
                    generation_id=identity.generation_id,
                    instance_id=prepared.instance_id,
                    worker_incarnation=prepared.worker_incarnation,
                    call_id=call_id,
                    call_operation_id=operation_id,
                    causal_root_kind="runtime.generation",
                    causal_root_id=identity.generation_id,
                ),
                declaration,
                payload,
                parent_call_id=call_id,
                timeout_seconds=_timeout(self.component, "start_timeout_s", 10),
                result_validator=output_validator.is_valid,
            )

    async def _release_contribution_directories(self) -> None:
        identity = self.generation_identity
        prepared = self._prepared_binding
        if identity is None:
            if not self._contribution_directories.kinds:
                return
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Contribution directory has no bound Generation",
                500,
            )
        requests = self._contribution_directories.cleanup_requests(identity)
        if not requests:
            return
        if identity is None or prepared is None or self._domain_commands is None:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Contribution directory cleanup authority is unavailable",
                500,
            )
        declarations = {
            item["name"]: item
            for item in self.component.get("domain_commands", ())
        }
        for command_name, payload in requests:
            declaration = declarations[command_name]
            input_validator, output_validator = self._service_schemas[command_name]
            if not input_validator.is_valid(payload):
                raise ForestError(
                    "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                    "Frozen contribution directory violates its cleanup port",
                    500,
                )
            operation_id = "directory-cleanup:" + hashlib.sha256(
                f"{identity.generation_id}\0{payload['kind']}".encode("utf-8")
            ).hexdigest()
            call_id = "call:" + uuid4().hex
            await self._domain_commands.invoke_cleanup_command(
                PackageCallScope(
                    bot_id=self.bot_id,
                    scope_kind=self.scope_kind,
                    scope_id=self.scope_id,
                    component_id=self.component_id,
                    generation_id=identity.generation_id,
                    instance_id=prepared.instance_id,
                    worker_incarnation=prepared.worker_incarnation,
                    call_id=call_id,
                    call_operation_id=operation_id,
                    causal_root_kind="runtime.generation",
                    causal_root_id=identity.generation_id,
                ),
                declaration,
                payload,
                parent_call_id=call_id,
                timeout_seconds=_timeout(self.component, "stop_timeout_s", 5),
                result_validator=output_validator.is_valid,
            )

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
        read_only: bool = False,
    ) -> Any:
        self._check_route(route)
        if invocation.read_only and not read_only:
            raise ForestError(
                "SERVICE_READ_ONLY_VIOLATION",
                "Read-only service cannot invoke a mutating service",
                403,
            )
        worker = self._worker
        identity = self.generation_identity
        prepared = self._prepared_binding
        assert worker is not None and identity is not None and prepared is not None
        call_id = "call:" + uuid4().hex
        scope_values = dict(
            bot_id=self.bot_id,
            scope_kind=self.scope_kind,
            scope_id=self.scope_id,
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
        if not read_only:
            self._writable_calls.add(call_id)
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
                if route.owned_work:
                    self._owned_work_calls.add(call_id)
                try:
                    result = normalize(await dispatch(worker, scope, call_id))
                finally:
                    self._owned_work_calls.discard(call_id)
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
            self._writable_calls.discard(call_id)
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
            allowed_callers = self._provided_services[service_id].get("allowed_callers")
            if allowed_callers is not None and route.caller_component_id not in allowed_callers:
                raise ForestError(
                    "RUNTIME_SERVICE_CALLER_DENIED",
                    "The service does not admit this caller component", 403,
                )
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
                durable=(
                    self._provided_services[service_id]["semantics"]
                    == "at-most-once"
                ),
                read_only=(
                    self._provided_services[service_id]["semantics"] == "read"
                ),
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

    def _shell_command_handler(self, service_id: str) -> RoutedServiceEndpoint:
        async def routed(route, invocation, request):
            if type(request) is not ShellCommandExecutionRequest:
                raise ForestError(
                    "SHELL_EXECUTION_INPUT_INVALID",
                    "Shell command request is invalid",
                    422,
                )
            entry = self._shell_commands.get(
                (request.command_id, request.command_version)
            )
            if entry is None or entry[0]["service_id"] != service_id:
                raise ForestError(
                    "SHELL_ROUTE_UNSUPPORTED",
                    "Shell command is not owned by this package service",
                    409,
                )
            declaration, argument_validator = entry
            payload = {
                "protocol": "forest.shell.arguments/1",
                "command_id": request.command_id,
                "arguments": request.arguments,
                "request_id": request.request_id,
                "principal_ref": request.principal_ref,
                "conversation_ref": request.conversation_ref,
            }
            argument_payload = {
                key: payload[key]
                for key in ("protocol", "command_id", "arguments")
            }
            input_validator, output_validator = self._service_schemas[service_id]
            if (
                not argument_validator.is_valid(argument_payload)
                or not input_validator.is_valid(payload)
            ):
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID",
                    "Shell command arguments violate the package contract",
                    422,
                )

            if declaration.get("management_action", False):
                await self._verify_dispatch(route)
                if not request.claim_token or not request.shell_generation_id:
                    raise ForestError(
                        "SHELL_MANAGEMENT_CLAIM_REQUIRED",
                        "Shell management action requires an execution claim",
                        403,
                    )
                authorizer = self._shell_management_authorizer
                contract = getattr(self, "management_contract", None)
                if not callable(authorizer) or contract is None:
                    raise ForestError(
                        "SHELL_MANAGEMENT_AUTHORITY_UNAVAILABLE",
                        "Shell management action authority is unavailable",
                        503,
                    )
                action_id = declaration["management_operation_id"]
                operation_id = "shell:action:" + hashlib.sha256(
                    (self.scope_id + "\0" + request.request_id + "\0" + action_id).encode()
                ).hexdigest()
                plan_digest = contract.authorization_plan_digest(
                    action_id, request.arguments
                )
                authorization = authorizer(
                    request=request,
                    component_id=self.component_id,
                    generation_id=route.provider_generation_id,
                    service_id=service_id,
                    service_version=route.version,
                    action_id=action_id,
                    operation_id=operation_id,
                    input_digest=digest_json(request.arguments),
                    plan_digest=plan_digest,
                )
                result = await contract.action(
                    action_id,
                    operation_id=operation_id,
                    payload=request.arguments,
                    authorization=authorization,
                    authorization_guard=authorization.verify_in,
                )
                request_ref = result.get("request_id") if isinstance(result, dict) else None
                if not isinstance(request_ref, str) or not request_ref:
                    raise ForestError(
                        "SHELL_MANAGEMENT_RECEIPT_INVALID",
                        "Management action returned no durable request reference",
                        502,
                    )
                return ShellCommandExecutionResult(
                    f"{action_id}: {request_ref} 已提交；请查询发布状态。"
                )

            result = await self._run_service_operation(
                route=route,
                invocation=invocation,
                service_id=service_id,
                input_value=payload,
                durable=False,
                dispatch=lambda worker, _scope, call_id: worker.invoke(
                    service_id,
                    payload,
                    call_id=call_id,
                    operation_id=invocation.operation_id,
                    causal_root_kind=invocation.causal_root_kind,
                    causal_root_id=invocation.causal_root_id,
                    timeout=min(
                        invocation.timeout_seconds,
                        float(declaration["timeout_s"]),
                    ),
                    before_send=lambda: self._verify_dispatch(route),
                ),
                normalize=lambda value: value,
                result_valid=output_validator.is_valid,
            )
            return ShellCommandExecutionResult(result["text"])

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
                read_only=declaration["semantics"] == "read",
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
                    if await request.cancellation_requested():
                        raise ForestError(
                            "JOB_CANCELLED", "Job was cancelled before execution", 409
                        )
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
                details={
                    "service_id": service_id,
                    "declared_contract": contract,
                    "requested_contract_major": request["contract_major"],
                    "capability_granted": self._has_capability(
                        f"service.call:{service_id}"
                    ),
                },
            )
        timeout = self._worker.remaining_timeout(call_id, request["timeout"])
        if local is not None:
            if call_id not in self._writable_calls and local["semantics"] != "read":
                raise ForestError(
                    "SERVICE_READ_ONLY_VIOLATION",
                    "Read-only service cannot invoke a mutating effect",
                    403,
                )
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
            read_only=call_id not in self._writable_calls,
        )

    async def _domain_query_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        return await self._domain_gateway("query", identity, call_id, request)

    async def _domain_command_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        return await self._domain_gateway("command", identity, call_id, request)

    async def _unit_of_work_gateway(
        self, identity: WorkerIdentity, parent_call_id: str, request: dict
    ) -> Any:
        outer = self._call_scope(identity, parent_call_id)
        declaration = next(
            (
                item
                for item in self.component.get("unit_of_work_plans", ())
                if item["name"] == request["name"]
            ),
            None,
        )
        if (
            declaration is None
            or request["contract_major"] != _major(declaration["contract"])
            or not self._has_capability(f"uow.execute:{request['name']}")
        ):
            raise ForestError(
                "CAPABILITY_DENIED",
                "Worker requested an undeclared UnitOfWork plan",
                403,
            )
        if not declaration.get("read_only", False):
            self._require_writable_call(parent_call_id)
        coordinator = self._unit_of_work_coordinator
        if coordinator is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE", "UnitOfWork coordinator is unavailable", 503
            )
        payload = request["payload"]
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "PACKAGE_UOW_INPUT_INVALID",
                "UnitOfWork input violates its immutable package schema",
                422,
            )
        timeout = self._worker.remaining_timeout(parent_call_id, request["timeout"])
        scope_values = dict(
            bot_id=outer.bot_id,
            component_id=outer.component_id,
            generation_id=outer.generation_id,
            instance_id=outer.instance_id,
            worker_incarnation=outer.worker_incarnation,
            call_id="call:" + uuid4().hex,
            call_operation_id=request["operation_id"],
            causal_root_kind=outer.causal_root_kind,
            causal_root_id=outer.causal_root_id,
            recovered_generation=outer.recovered_generation,
        )
        scope = (
            PackageJobCallScope(
                **scope_values,
                cancellation_requested=outer.cancellation_requested,
            )
            if isinstance(outer, PackageJobCallScope)
            else (
                PackageOwnedWorkCallScope(**scope_values)
                if isinstance(outer, PackageOwnedWorkCallScope)
                else PackageCallScope(**scope_values)
            )
        )
        result = await coordinator.execute(
            scope,
            declaration,
            payload,
            parent_call_id=parent_call_id,
            timeout_seconds=min(timeout, float(declaration["timeout_s"])),
        )
        if not output_validator.is_valid(result):
            raise ForestError(
                "PACKAGE_UOW_OUTPUT_INVALID",
                "UnitOfWork result violates its immutable package schema",
                502,
            )
        return result

    async def _domain_gateway(
        self,
        kind: str,
        identity: WorkerIdentity,
        parent_call_id: str,
        request: dict,
    ) -> Any:
        outer = self._call_scope(identity, parent_call_id)
        if kind == "command":
            self._require_writable_call(parent_call_id)
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
            scope_kind=outer.scope_kind,
            scope_id=outer.scope_id,
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
            if not output_validator.is_valid(result):
                raise ForestError(
                    "DOMAIN_OUTPUT_INVALID",
                    "Domain authority result violates its immutable schema",
                    502,
                )
            return result
        else:
            return await authority.invoke_command(
                scope,
                declaration,
                payload,
                parent_call_id=parent_call_id,
                timeout_seconds=timeout,
                result_validator=output_validator.is_valid,
            )

    def _management_scope(
        self, operation_id: str, *, causal_root_kind: str = "web.management",
        causal_root_id: str | None = None,
    ) -> tuple[PackageCallScope, str]:
        identity = self.generation_identity
        prepared = self._prepared_binding
        if (
            self.state is not PackageGenerationState.ACTIVE
            or identity is None
            or prepared is None
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Package management requires the active Generation",
                409,
            )
        parent_call_id = "management:" + uuid4().hex
        return (
            PackageCallScope(
                bot_id=self.bot_id,
                scope_kind=self.scope_kind,
                scope_id=self.scope_id,
                component_id=self.component_id,
                generation_id=identity.generation_id,
                instance_id=prepared.instance_id,
                worker_incarnation=prepared.worker_incarnation,
                call_id="call:" + uuid4().hex,
                call_operation_id=operation_id,
                causal_root_kind=causal_root_kind,
                causal_root_id=causal_root_id or operation_id,
            ),
            parent_call_id,
        )

    async def invoke_management_query(
        self,
        declaration: dict,
        payload: dict,
        *,
        timeout_seconds: float,
    ) -> Any:
        domain_query = declaration in self.component.get("domain_queries", ())
        kernel_query = declaration in self.component.get("kernel_queries", ())
        read_service = (
            declaration in self.component.get("provides", ())
            and declaration["semantics"] == "read"
        )
        if not domain_query and not kernel_query and not read_service:
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Management query is not bound to this package Generation",
                409,
            )
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Management query input violates its immutable package schema",
                422,
            )
        operation_id = "management-query:" + uuid4().hex
        scope, parent_call_id = self._management_scope(operation_id)
        if domain_query or kernel_query:
            authority = (
                self._kernel_management_queries if kernel_query else self._domain_queries
            )
            if authority is None:
                raise ForestError(
                    "CAPABILITY_UNAVAILABLE", "Management query authority is unavailable", 503
                )
            result = await authority.invoke_query(
                scope,
                declaration,
                payload,
                parent_call_id=parent_call_id,
                timeout_seconds=timeout_seconds,
            )
        else:
            identity = self.generation_identity
            if identity is None:
                raise ForestError(
                    "RUNTIME_GENERATION_NOT_BOUND", "Management service has no Generation", 409
                )
            route = FrozenServiceRoute(
                service_id=declaration["name"],
                version=_major(declaration["contract"]),
                caller_component_id=self.component_id,
                provider_component_id=self.component_id,
                provider_generation_id=identity.generation_id,
                graph_id=identity.graph_id,
                graph_revision=identity.graph_revision,
                admission_lane="DATA",
                owned_work=False,
            )
            invocation = RuntimeServiceInvocation(
                operation_id=operation_id,
                timeout_seconds=timeout_seconds,
                causal_root_kind=scope.causal_root_kind,
                causal_root_id=scope.causal_root_id,
                parent_call_id=parent_call_id,
            )
            result = await self._service_handler(declaration["name"]).invoke_routed(
                route, invocation, (payload,), {}
            )
        if not output_validator.is_valid(result):
            raise ForestError(
                "MODULE_MANAGEMENT_OUTPUT_INVALID",
                "Management query result violates its immutable package schema",
                502,
            )
        return result

    async def invoke_management_artifact_download(
        self, declaration: dict, payload: dict, *, max_bytes: int, media_type: str
    ) -> PackageArtifactDownload:
        if declaration not in self.component.get("effect_services", ()) or (
            declaration["semantics"] != "read"
        ):
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Artifact operation is not bound to a read-only Generation port", 409,
            )
        input_validator, _output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact input violates its schema", 422
            )
        identity = self.generation_identity
        if self.state is not PackageGenerationState.ACTIVE or identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Package artifact Generation is inactive", 409
            )
        driver = self._effect_drivers.get(declaration["name"])
        if not isinstance(driver, PackageArtifactDownloadDriver):
            raise ForestError(
                "PACKAGE_ARTIFACT_PORT_UNAVAILABLE",
                "Declared artifact driver cannot download bytes", 503,
            )
        if self._artifact_inflight >= 2 or (
            self._service_inflight >= self.component["resources"]["max_inflight"]
        ):
            raise ForestError(
                "PACKAGE_ARTIFACT_BUSY", "Artifact transfer capacity is exhausted", 429,
            )
        self._artifact_inflight += 1
        self._service_inflight += 1
        self._services_drained.clear()
        pending = None
        try:
            await self._generation_verifier(
                component_id=self.component_id, generation_id=identity.generation_id,
                admission_lane="DATA",
            )
            pending = asyncio.create_task(driver.download(payload, max_bytes=max_bytes))
            async with asyncio.timeout(30):
                result = await asyncio.shield(pending)
            if (
                type(result) is not PackageArtifactDownload
                or type(result.content) is not bytes
                or not 1 <= len(result.content) <= max_bytes
                or result.media_type != media_type
                or not isinstance(result.filename, str)
                or not 1 <= len(result.filename) <= 128
                or result.filename.startswith(".")
                or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
                       for char in result.filename)
            ):
                raise ForestError(
                    "PACKAGE_ARTIFACT_RESULT_INVALID",
                    "Artifact driver returned bytes outside its frozen contract", 502,
                )
            return result
        except TimeoutError as exc:
            raise ForestError(
                "PACKAGE_ARTIFACT_TIMEOUT", "Artifact download exceeded its deadline", 504
            ) from exc
        finally:
            if pending is not None and not pending.done():
                try:
                    await asyncio.shield(pending)
                except Exception:
                    pass
            self._artifact_inflight -= 1
            self._service_inflight -= 1
            if self._service_inflight == 0:
                self._services_drained.set()

    @asynccontextmanager
    async def management_artifact_upload(
        self, declaration: dict, payload: dict, *, max_bytes: int
    ):
        if (
            declaration not in self.component.get("effect_services", ())
            or declaration["semantics"] != "idempotent"
        ):
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Artifact upload is not bound to an idempotent Generation port", 409,
            )
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact input violates its schema", 422,
            )
        identity = self.generation_identity
        if self.state is not PackageGenerationState.ACTIVE or identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Package artifact Generation is inactive", 409,
            )
        driver = self._effect_drivers.get(declaration["name"])
        if not isinstance(driver, PackageArtifactUploadDriver):
            raise ForestError(
                "PACKAGE_ARTIFACT_PORT_UNAVAILABLE", "Artifact upload driver is unavailable", 503,
            )
        if self._artifact_inflight >= 2 or (
            self._service_inflight >= self.component["resources"]["max_inflight"]
        ):
            raise ForestError(
                "PACKAGE_ARTIFACT_BUSY", "Artifact transfer capacity is exhausted", 429,
            )
        self._artifact_inflight += 1
        self._service_inflight += 1
        self._services_drained.clear()
        pending = None
        try:
            await self._generation_verifier(
                component_id=self.component_id, generation_id=identity.generation_id,
                admission_lane="DATA",
            )

            async def publish(
                content: bytes, *, operation_id: str, actor_ref: str, content_sha256: str
            ) -> dict:
                nonlocal pending
                if pending is not None or type(content) is not bytes or (
                    not 1 <= len(content) <= max_bytes
                ):
                    raise ForestError(
                        "PACKAGE_ARTIFACT_INPUT_INVALID", "Artifact content is outside its bound", 422,
                    )
                pending = asyncio.create_task(driver.upload(
                    payload, content, operation_id=operation_id,
                    actor_ref=actor_ref, content_sha256=content_sha256,
                    max_bytes=max_bytes,
                ))
                result = await asyncio.shield(pending)
                if not isinstance(result, dict) or not output_validator.is_valid(result):
                    raise ForestError(
                        "PACKAGE_ARTIFACT_RESULT_INVALID",
                        "Artifact driver returned an invalid result", 502,
                    )
                return result

            yield publish
        finally:
            if pending is not None and not pending.done():
                try:
                    await asyncio.shield(pending)
                except Exception:
                    pass
            self._artifact_inflight -= 1
            self._service_inflight -= 1
            if self._service_inflight == 0:
                self._services_drained.set()

    async def invoke_management_artifact_close(
        self, declaration: dict, payload: dict,
    ) -> dict:
        if (
            declaration not in self.component.get("effect_services", ())
            or declaration["semantics"] != "idempotent"
        ):
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Artifact close is not bound to an idempotent Generation port", 409,
            )
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact close input is invalid", 422,
            )
        identity = self.generation_identity
        if self.state is not PackageGenerationState.ACTIVE or identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Artifact Generation is inactive", 409,
            )
        driver = self._effect_drivers.get(declaration["name"])
        close_upload = getattr(driver, "close_upload", None)
        if not callable(close_upload) or self._service_inflight >= self.component["resources"]["max_inflight"]:
            raise ForestError(
                "PACKAGE_ARTIFACT_PORT_UNAVAILABLE", "Artifact close port is unavailable", 503,
            )
        self._service_inflight += 1
        self._services_drained.clear()
        try:
            await self._generation_verifier(
                component_id=self.component_id, generation_id=identity.generation_id,
                admission_lane="DATA",
            )
            result = await close_upload(payload)
            if not isinstance(result, dict) or not output_validator.is_valid(result):
                raise ForestError(
                    "PACKAGE_ARTIFACT_RESULT_INVALID", "Artifact close returned an invalid result", 502,
                )
            return result
        finally:
            self._service_inflight -= 1
            if self._service_inflight == 0:
                self._services_drained.set()

    async def invoke_management_artifact_command_in(
        self, conn, declaration: dict, payload: dict, *, operation_id: str,
    ) -> dict:
        if declaration not in self.component.get("domain_commands", ()):
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Artifact hook is not a declared Domain Command", 409,
            )
        authority = self._domain_commands
        if authority is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE", "Artifact domain authority is unavailable", 503,
            )
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Artifact hook input violates its immutable package schema", 422,
            )
        scope, parent_call_id = self._management_scope(operation_id)
        return await authority.invoke_artifact_command_in(
            conn, scope, declaration, payload, parent_call_id=parent_call_id,
            timeout_seconds=30, result_validator=output_validator.is_valid,
        )

    async def invoke_management_action(
        self,
        declaration: dict,
        payload: dict,
        *,
        action_id: str,
        operation_id: str,
        plan_digest: str | None,
        request_digest: str,
        timeout_seconds: float,
        authorization=None,
        authorization_guard=None,
    ) -> Any:
        if authorization is None or not callable(authorization_guard):
            raise ForestError(
                "MANAGEMENT_AUTHORITY_REQUIRED",
                "Management action requires a transactional authority guard",
                403,
            )
        kernel_command = declaration in self.component.get("kernel_commands", ())
        if not kernel_command and declaration not in self.component.get("domain_commands", ()):
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Management action is not bound to this package Generation",
                409,
            )
        action = self._management_actions.get(action_id)
        if action is None or action["port"] != declaration["name"]:
            raise ForestError(
                "MODULE_MANAGEMENT_BINDING_INVALID",
                "Management action ID does not bind this Command port",
                409,
            )
        authority = (
            self._kernel_management_commands if kernel_command else self._domain_commands
        )
        if authority is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE", "Management command authority is unavailable", 503
            )
        input_validator, output_validator = self._service_schemas[declaration["name"]]
        if not isinstance(payload, dict) or not input_validator.is_valid(payload):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Management action input violates its immutable package schema",
                422,
            )
        scope, parent_call_id = self._management_scope(
            operation_id,
            causal_root_kind=getattr(authorization, "causal_root_kind", "web.management"),
            causal_root_id=getattr(authorization, "causal_root_id", operation_id),
        )

        preflight = self._management_action_preflights.get(action_id)
        if action.get("transaction_preflight") is not None and preflight is None:
            raise ForestError(
                "MODULE_MANAGEMENT_PREFLIGHT_UNAVAILABLE",
                "Declared management transaction preflight is unavailable",
                503,
            )
        guard_scope = (
            preflight(payload, operation_id=operation_id)
            if preflight is not None else nullcontext(None)
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                async with guard_scope as verify_in:
                    if preflight is not None and not callable(verify_in):
                        raise ForestError(
                            "MODULE_MANAGEMENT_PREFLIGHT_INVALID",
                            "Management transaction preflight has no verifier",
                            503,
                        )

                    async def before_write(conn) -> None:
                        await authorization_guard(
                            conn,
                            ticket=authorization,
                            component_id=self.component_id,
                            operation_id=operation_id,
                            action_key=action_id,
                            input_digest=request_digest,
                            plan_digest=plan_digest,
                        )
                        if verify_in is not None:
                            await verify_in(conn)

                    return await authority.invoke_management_action(
                        scope,
                        declaration,
                        payload,
                        parent_call_id=parent_call_id,
                        timeout_seconds=timeout_seconds,
                        before_write=before_write,
                        management_authorization=authorization,
                        result_validator=output_validator.is_valid,
                    )
        except TimeoutError as exc:
            raise ForestError(
                "MODULE_MANAGEMENT_TIMEOUT",
                "Management action exceeded its deadline",
                504,
            ) from exc

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

    async def _invoke_effect_ingress(
        self,
        service_id: str,
        payload: dict,
        *,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
    ) -> dict:
        """Deliver one external frame to the exact Worker owned by this Generation."""

        binding = self._effect_ingress.get(service_id)
        identity = self.generation_identity
        if binding is None or identity is None:
            raise ForestError(
                "PACKAGE_EFFECT_INGRESS_UNAVAILABLE",
                "Effect ingress is not bound to this Package Generation",
                503,
            )
        _effect, declaration = binding
        if (
            not isinstance(payload, dict)
            or not isinstance(operation_id, str)
            or not 1 <= len(operation_id) <= 256
            or not isinstance(causal_root_kind, str)
            or not 1 <= len(causal_root_kind) <= 64
            or not isinstance(causal_root_id, str)
            or not 1 <= len(causal_root_id) <= 256
        ):
            raise ForestError(
                "PACKAGE_EFFECT_INGRESS_INPUT_INVALID",
                "Effect ingress identity or payload is invalid",
                422,
            )
        input_validator, output_validator = self._service_schemas[service_id]
        if not input_validator.is_valid(payload):
            raise ForestError(
                "PACKAGE_EFFECT_INGRESS_INPUT_INVALID",
                "Effect ingress payload violates its immutable schema",
                422,
            )
        graph_id = identity.graph_id
        route = FrozenServiceRoute(
            service_id=service_id,
            version=1,
            caller_component_id=self.component_id,
            provider_component_id=self.component_id,
            provider_generation_id=identity.generation_id,
            graph_id=graph_id,
            graph_revision=identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        invocation = RuntimeServiceInvocation(
            operation_id,
            float(declaration["timeout_s"]),
            causal_root_kind,
            causal_root_id,
        )
        return await self._run_service_operation(
            route=route,
            invocation=invocation,
            service_id=service_id,
            input_value=payload,
            durable=declaration["semantics"] == "at-most-once",
            dispatch=lambda worker, _scope, call_id: worker.invoke(
                service_id,
                payload,
                call_id=call_id,
                operation_id=operation_id,
                causal_root_kind=causal_root_kind,
                causal_root_id=causal_root_id,
                timeout=float(declaration["timeout_s"]),
                before_send=lambda: self._verify_dispatch(route),
            ),
            normalize=lambda result: result,
            result_valid=output_validator.is_valid,
        )

    def _begin_owned_work_call(self, call_id: str) -> None:
        self._owned_work_calls.add(call_id)
        self._writable_calls.add(call_id)

    def _finish_owned_work_call(self, call_id: str) -> None:
        self._writable_calls.discard(call_id)
        self._owned_work_calls.discard(call_id)

    def _require_writable_call(self, call_id: str) -> None:
        if call_id not in self._writable_calls:
            raise ForestError(
                "SERVICE_READ_ONLY_VIOLATION",
                "This Worker call has no mutating service admission",
                403,
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
            scope_kind=self.scope_kind,
            scope_id=self.scope_id,
            component_id=self.component_id,
            generation_id=generation.generation_id,
            instance_id=prepared.instance_id,
            worker_incarnation=prepared.worker_incarnation,
            call_id=call_id,
            call_operation_id=worker.call_operation_id(call_id),
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            recovered_generation=self._recovered_generation,
        )
        cancellation = self._job_cancellations.get(call_id)
        return (
            PackageJobCallScope(**values, cancellation_requested=cancellation)
            if cancellation is not None
            else (
                PackageOwnedWorkCallScope(**values)
                if call_id in self._owned_work_calls
                else PackageCallScope(**values)
            )
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
            self._require_writable_call(call_id)
            if (self._state_value_validator is not None
                    and not self._state_value_validator.is_valid(request["value"])):
                raise ForestError(
                    "COMPONENT_STATE_INPUT_INVALID",
                    "State value violates its package schema", 422,
                )
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

    @staticmethod
    def _payload_reference(value: object) -> dict:
        fields = {
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ForestError(
                "PAYLOAD_REFERENCE_INVALID",
                "Payload access requires one complete exact reference",
                422,
            )
        result = dict(value)
        for name in fields - {"digest"}:
            item = result[name]
            limit = 512 if name == "owner_id" else 256
            if not isinstance(item, str) or not 1 <= len(item.encode("utf-8")) <= limit:
                raise ForestError(
                    "PAYLOAD_REFERENCE_INVALID", "Payload reference identity is invalid", 422
                )
        digest = result["digest"]
        if (
            not isinstance(digest, str)
            or len(digest) != 71
            or not digest.startswith("sha256:")
            or any(char not in "0123456789abcdef" for char in digest[7:])
        ):
            raise ForestError(
                "PAYLOAD_REFERENCE_INVALID", "Payload reference digest is invalid", 422
            )
        return result

    @staticmethod
    def _payload_string(value: object, name: str, limit: int) -> str:
        if (
            not isinstance(value, str)
            or not 1 <= len(value.encode("utf-8")) <= limit
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise ForestError(
                "PAYLOAD_REQUEST_INVALID", f"Payload {name} is invalid", 422
            )
        return value

    def _drop_payload_upload(self, key: tuple[str, str]) -> bool:
        upload = self._payload_uploads.pop(key, None)
        if upload is None:
            return False
        self._payload_reserved_bytes -= upload.byte_length
        return True

    async def _payload_call_closed(
        self, identity: WorkerIdentity, call_id: str
    ) -> None:
        # Identity and call ownership were already verified by PythonWorker.
        for key in tuple(self._payload_uploads):
            if key[0] == call_id:
                self._drop_payload_upload(key)

    async def _payload_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        action = request.get("action")
        if action in {"begin-write", "append-write", "commit-write", "abort-write"}:
            self._require_writable_call(call_id)
        if self._payload_authority is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE", "Payload authority is unavailable", 503
            )
        if action == "read-chunk":
            if set(request) != {"action", "reference", "offset", "max_bytes"}:
                raise ForestError(
                    "PAYLOAD_REQUEST_INVALID", "Payload read request shape is invalid", 422
                )
            if not self._has_capability("payload.read"):
                raise ForestError("CAPABILITY_DENIED", "Payload read is not granted", 403)
            reference = self._payload_reference(request["reference"])
            offset, max_bytes = request["offset"], request["max_bytes"]
            if (
                type(offset) is not int
                or offset < 0
                or type(max_bytes) is not int
                or not 1 <= max_bytes <= _PAYLOAD_CHUNK_BYTES
            ):
                raise ForestError(
                    "PAYLOAD_RANGE_INVALID", "Payload read range is invalid", 422
                )
            result = await self._payload_authority.read_chunk(
                scope,
                reference=reference,
                offset=offset,
                max_bytes=max_bytes,
            )
            content = result.get("content") if isinstance(result, dict) else None
            if type(content) is not bytes or len(content) > max_bytes:
                raise ForestError(
                    "PAYLOAD_AUTHORITY_INVALID",
                    "Payload authority returned an invalid chunk",
                    500,
                )
            return {
                "data_base64": base64.b64encode(content).decode("ascii"),
                "offset": result["offset"],
                "byte_length": result["byte_length"],
                "eof": result["eof"],
            }
        if action == "begin-write":
            expected = {
                "action",
                "upload_id",
                "owner_kind",
                "owner_id",
                "media_type",
                "byte_length",
                "digest",
                "parents",
            }
            if set(request) != expected:
                raise ForestError(
                    "PAYLOAD_REQUEST_INVALID", "Payload write declaration is invalid", 422
                )
            if not self._has_capability("payload.write"):
                raise ForestError("CAPABILITY_DENIED", "Payload write is not granted", 403)
            upload_id = self._payload_string(request["upload_id"], "upload_id", 128)
            key = (call_id, upload_id)
            if key in self._payload_uploads:
                raise ForestError(
                    "PAYLOAD_UPLOAD_CONFLICT", "Payload upload is already active", 409
                )
            byte_length = request["byte_length"]
            if type(byte_length) is not int or not 0 <= byte_length <= _PAYLOAD_STAGING_BYTES:
                raise ForestError(
                    "PAYLOAD_TOO_LARGE", "Payload exceeds the 32 MiB limit", 413
                )
            digest = request["digest"]
            if (
                not isinstance(digest, str)
                or len(digest) != 71
                or not digest.startswith("sha256:")
                or any(char not in "0123456789abcdef" for char in digest[7:])
            ):
                raise ForestError(
                    "PAYLOAD_REQUEST_INVALID", "Payload digest is invalid", 422
                )
            parents = request["parents"]
            if not isinstance(parents, list) or len(parents) > 256:
                raise ForestError(
                    "PAYLOAD_REQUEST_INVALID", "Payload parent set is invalid", 422
                )
            exact_parents = tuple(self._payload_reference(item) for item in parents)
            if (
                len(self._payload_uploads) >= 16
                or self._payload_reserved_bytes + byte_length > _PAYLOAD_STAGING_BYTES
            ):
                raise ForestError(
                    "PAYLOAD_STAGING_EXHAUSTED",
                    "Generation payload staging capacity is exhausted",
                    429,
                )
            self._payload_uploads[key] = _StagedPayloadWrite(
                self._payload_string(request["owner_kind"], "owner_kind", 128),
                self._payload_string(request["owner_id"], "owner_id", 512),
                self._payload_string(request["media_type"], "media_type", 128),
                byte_length,
                digest,
                exact_parents,
            )
            self._payload_reserved_bytes += byte_length
            return {"upload_id": upload_id}
        if action in {"append-write", "commit-write", "abort-write"}:
            expected = (
                {"action", "upload_id", "offset", "data_base64"}
                if action == "append-write"
                else {"action", "upload_id"}
            )
            if set(request) != expected:
                raise ForestError(
                    "PAYLOAD_REQUEST_INVALID", "Payload upload request shape is invalid", 422
                )
            if not self._has_capability("payload.write"):
                raise ForestError("CAPABILITY_DENIED", "Payload write is not granted", 403)
            upload_id = self._payload_string(request["upload_id"], "upload_id", 128)
            key = (call_id, upload_id)
            upload = self._payload_uploads.get(key)
            if upload is None:
                if action == "abort-write":
                    return {"aborted": False}
                raise ForestError(
                    "PAYLOAD_UPLOAD_NOT_FOUND", "Payload upload is not active", 404
                )
            if action == "abort-write":
                self._drop_payload_upload(key)
                return {"aborted": True}
            if action == "append-write":
                if request["offset"] != len(upload.content):
                    raise ForestError(
                        "PAYLOAD_UPLOAD_OFFSET_CONFLICT",
                        "Payload chunks must be appended in exact order",
                        409,
                    )
                try:
                    chunk = base64.b64decode(request["data_base64"], validate=True)
                except (TypeError, ValueError) as exc:
                    raise ForestError(
                        "PAYLOAD_REQUEST_INVALID", "Payload chunk is not valid base64", 422
                    ) from exc
                if (
                    not chunk
                    or len(chunk) > _PAYLOAD_CHUNK_BYTES
                    or len(upload.content) + len(chunk) > upload.byte_length
                ):
                    raise ForestError(
                        "PAYLOAD_UPLOAD_RANGE_INVALID", "Payload chunk range is invalid", 422
                    )
                upload.content.extend(chunk)
                return {"offset": len(upload.content)}
            content = bytes(upload.content)
            if (
                len(content) != upload.byte_length
                or "sha256:" + hashlib.sha256(content).hexdigest() != upload.digest
            ):
                raise ForestError(
                    "PAYLOAD_UPLOAD_INCOMPLETE",
                    "Payload bytes do not match the declared length and digest",
                    409,
                )
            result = await self._payload_authority.commit_bytes(
                scope,
                owner_kind=upload.owner_kind,
                owner_id=upload.owner_id,
                media_type=upload.media_type,
                content=content,
                parents=upload.parents,
            )
            self._drop_payload_upload(key)
            return result
        raise ForestError(
            "PAYLOAD_REQUEST_INVALID", "Unknown payload capability operation", 422
        )

    async def _event_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        self._require_writable_call(call_id)
        if not self._has_capability(f"event.publish:{request['topic']}"):
            raise ForestError("CAPABILITY_DENIED", "Event publication is not granted", 403)
        if self._event_publisher is None:
            raise ForestError("CAPABILITY_UNAVAILABLE", "Event authority is unavailable", 503)
        return await self._event_publisher.invoke(
            {
                "protocol": "forest.event-publication/1",
                "publisher_component_id": scope.component_id,
                "publisher_generation_id": scope.generation_id,
                "topic": request["topic"],
                "contract": request["contract"],
                "payload": request["payload"],
            },
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
        self._require_writable_call(call_id)
        if not self._has_capability(f"job.schedule:{request['handler_id']}"):
            raise ForestError("CAPABILITY_DENIED", "Job scheduling is not granted", 403)
        if self._component_job_scheduler is None:
            raise ForestError("CAPABILITY_UNAVAILABLE", "ComponentJob authority is unavailable", 503)
        return await self._component_job_scheduler.invoke(
            {
                "protocol": "forest.component-job-schedule/1",
                "owner_component_id": scope.component_id,
                "owner_generation_id": scope.generation_id,
                "handler_id": request["handler_id"],
                "payload": request["payload"],
                "delay_seconds": request["delay_seconds"],
            },
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=self._worker.remaining_timeout(
                call_id, _timeout(self.component, "service_timeout_s", 30)
            ),
            parent_call_id=call_id,
        )

    async def _contribution_directory_gateway(
        self, identity: WorkerIdentity, call_id: str, request: dict
    ) -> Any:
        scope = self._call_scope(identity, call_id)
        if request.get("action") not in {"list", "query-handed-off"}:
            self._require_writable_call(call_id)
        worker = self._worker
        if worker is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Contribution directory requires an active Worker",
                409,
            )
        return await self._contribution_directories.invoke(
            request,
            graph_identity=self.generation_identity,
            scope=scope,
            parent_call_id=call_id,
            remaining_timeout=lambda limit: worker.remaining_timeout(call_id, limit),
        )

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        if self.state is not PackageGenerationState.NEW:
            raise ForestError(
                "LIFECYCLE_INVALID", "Package Generation was already prepared", 409
            )
        selected = self._prepared_configuration
        if selected is None:
            selected = await self.module_configuration.desired(include_value=True)
        schema = self._compiled_schemas[self.component["config_schema"]]
        if selected.validation_digest != digest_json(schema):
            raise ForestError(
                "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
                "Generation configuration was not validated for its captured package schema",
                409,
            )
        errors = sorted(
            Draft202012Validator(schema).iter_errors(selected.value),
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
                    selected.value,
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
        self._configuration_revision = selected.revision
        identity_seed = hashlib.sha256(
            f"{context.activation_id}\0{self.component_id}\0{self.snapshot.digest}".encode()
        ).hexdigest()
        binding = PreparedGenerationBinding(
            component_id=context.component_id,
            requires=context.requires,
            retention_requires=context.retention_requires,
            package_id=context.package_id,
            package_version=context.package_version,
            package_digest=context.package_digest,
            driver=context.driver,
            trust_level=context.trust_level,
            descriptor_digest=context.descriptor_digest,
            route_contract_digest=context.route_contract_digest,
            management_contract_digest=context.management_contract_digest,
            management_ui_digest=context.management_ui_digest,
            config_revision=selected.revision,
            config_payload_id=selected.payload_id,
            config_payload_digest=selected.payload_digest,
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
        owner_resolver = getattr(self.module_configuration, "owner_id", None)
        configuration_owner_id = (
            await owner_resolver(self._prepared_binding.config_revision)
            if callable(owner_resolver)
            else None
        )
        worker = self._worker_factory(
            self.snapshot,
            WorkerIdentity(
                instance_id=self._prepared_binding.instance_id,
                generation_id=self.generation_identity.generation_id,
                incarnation=self._prepared_binding.worker_incarnation,
                bot_id=self.bot_id,
                scope_kind=self.scope_kind,
                scope_id=self.scope_id,
                graph_id=self.generation_identity.graph_id,
                graph_revision=self.generation_identity.graph_revision,
                configuration_payload_id=(
                    self._prepared_binding.config_payload_id if configuration_owner_id else None
                ),
                configuration_payload_digest=(
                    self._prepared_binding.config_payload_digest if configuration_owner_id else None
                ),
                configuration_owner_id=configuration_owner_id,
            ),
            self._state_gateway,
            limits=WorkerLimits.from_component(self.component),
            service_handler=self._service_gateway,
            domain_query_handler=self._domain_query_gateway,
            domain_command_handler=self._domain_command_gateway,
            unit_of_work_handler=self._unit_of_work_gateway,
            event_handler=self._event_gateway,
            job_handler=self._job_gateway,
            contribution_directory_handler=self._contribution_directory_gateway,
            payload_handler=self._payload_gateway,
            call_closed_handler=self._payload_call_closed,
        )
        self._worker = worker
        started_drivers: list[PackageEffectDriver] = []
        try:
            await worker.launch(_timeout(self.component, "prepare_timeout_s", 10))
            await worker.prepare(
                self._configuration,
                timeout=_timeout(self.component, "prepare_timeout_s", 10),
            )
            started = await worker.start(_timeout(self.component, "start_timeout_s", 10))
            replacement_facts = started.get("replacement_facts", {}) if isinstance(started, dict) else {}
            if not isinstance(replacement_facts, dict):
                raise ForestError(
                    "REPLACEMENT_FACTS_INVALID",
                    "Prepared Worker returned invalid replacement facts",
                    422,
                )
            if self._replacement_facts_validator is None:
                if replacement_facts:
                    raise ForestError(
                        "REPLACEMENT_FACTS_UNDECLARED",
                        "Plugin published facts without a manifest schema",
                        422,
                    )
            elif not self._replacement_facts_validator.is_valid(replacement_facts):
                raise ForestError(
                    "REPLACEMENT_FACTS_INVALID",
                    "Prepared Worker facts violate the package schema",
                    422,
                )
            self._replacement_facts = deepcopy(replacement_facts)
            health = await worker.health(_timeout(self.component, "start_timeout_s", 10))
            if health.get("healthy") is not True:
                raise ForestError(
                    "WORKER_UNHEALTHY", "Candidate Worker failed its health contract", 503
                )
            await self._synchronize_contribution_directories()
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
                ingress = declaration.get("ingress")
                if ingress is not None:
                    if not isinstance(driver, PackageIngressEffectDriver):
                        raise ForestError(
                            "PACKAGE_EFFECT_INGRESS_BINDING_INVALID",
                            "Effect driver does not implement its declared ingress port",
                            500,
                            details={"service_id": service_id},
                        )
                    ingress_name = ingress["name"]
                    driver.bind_ingress(
                        lambda payload, *, operation_id, causal_root_kind,
                        causal_root_id, name=ingress_name: self._invoke_effect_ingress(
                            name,
                            payload,
                            operation_id=operation_id,
                            causal_root_kind=causal_root_kind,
                            causal_root_id=causal_root_id,
                        )
                    )
                await driver.start()
                self._effect_drivers[service_id] = driver
            if self._owned_work is not None:
                self._owned_work.start()
        except BaseException as start_error:
            self.state = PackageGenerationState.FAILED
            if self._owned_work is not None:
                await self._owned_work.close()
            cleanup_results = await asyncio.gather(
                self._release_contribution_directories(),
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
        try:
            for driver in self._effect_drivers.values():
                if isinstance(driver, PackageIngressEffectDriver):
                    driver.activate_ingress()
        except BaseException:
            self.state = PackageGenerationState.FAILED
            raise

    async def quiesce(self) -> None:
        if self.state is not PackageGenerationState.ACTIVE or self._worker is None:
            raise ForestError(
                "LIFECYCLE_INVALID", "Only an active Package Generation can quiesce", 409
            )
        self.state = PackageGenerationState.QUIESCING
        try:
            timeout = _timeout(self.component, "drain_timeout_s", 10)
            async with asyncio.timeout(timeout):
                for driver in self._effect_drivers.values():
                    if isinstance(driver, PackageIngressEffectDriver):
                        await driver.freeze_ingress()
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
            await self._worker.resume(_timeout(self.component, "start_timeout_s", 10))
            health = await self._worker.health(
                _timeout(self.component, "start_timeout_s", 10)
            )
            if health.get("healthy") is not True:
                raise ForestError(
                    "WORKER_UNHEALTHY", "Resumed Worker failed its health contract", 503
                )
            for driver in self._effect_drivers.values():
                await driver.resume()
        except BaseException:
            self.state = PackageGenerationState.FAILED
            raise
        self.state = PackageGenerationState.WORKER_READY
        self._replacement_operation_id = None
        self._recovered_generation = True

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
        close_operations = [
            self._release_contribution_directories(),
            *(driver.close() for driver in self._effect_drivers.values()),
        ]
        if worker is not None:
            close_operations.append(
                worker.stop(timeout=_timeout(self.component, "stop_timeout_s", 5))
            )
        results = await asyncio.gather(*close_operations, return_exceptions=True)
        self._payload_uploads.clear()
        self._payload_reserved_bytes = 0
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
    bot_id: str | None,
    scope_id: str | None = None,
    module_configuration: Any,
    prepared_configuration=None,
    grants: Any,
    resolved_contracts: Mapping[str, str],
    generation_verifier: GenerationVerifier,
    generation_member_verifier: GenerationVerifier,
    state_queries: PackageStateQueries | None,
    state_commands: PackageStateCommands | None,
    payload_authority: PackagePayloadAuthority | None = None,
    domain_queries: PackageDomainQueries | None = None,
    domain_commands: PackageDomainCommands | None = None,
    kernel_management_queries=None,
    kernel_management_commands=None,
    unit_of_work_authority: PackageUnitOfWorkAuthority | None = None,
    unit_of_work_coordinator_factory: Callable[
        [str, Mapping[str, Any]], PackageUnitOfWorkCoordinator
    ]
    | None = None,
    configuration_apply_coordinator_factory=None,
    owned_work_authority: PackageOwnedWorkAuthority | None = None,
    agent_tool_effects: PackageAgentToolEffectAuthority | None = None,
    service_operation_ledger: PackageServiceOperationLedger,
    configuration_validator: Callable[[object], Any] | None = None,
    effect_driver_factories: Mapping[str, PackageEffectDriverFactory] | None = None,
    storage=None,
    management=(),
    management_contract=None,
    management_contract_factory: Callable[[PackageGenerationAdapter], Any] | None = None,
    worker_factory: Callable[..., PythonWorker] = PythonWorker,
    trust_level: str = "installed",
    plan_only: bool = False,
) -> RuntimeModuleDescriptor:
    """Compile one package snapshot without importing its Python entrypoint."""

    if management_contract is not None and management_contract_factory is not None:
        raise ForestError(
            "PACKAGE_MANAGEMENT_BINDING_INVALID",
            "A package may bind one Generation-owned management contract source",
            500,
        )
    component = snapshot.component
    bind_scope_identity(
        scope_kind=component["scope"], scope_id=scope_id, bot_id=bot_id
    )
    immutable_paths = tuple(component.get("config_immutable_paths", ()))
    if immutable_paths and tuple(getattr(module_configuration, "immutable_paths", ())) != immutable_paths:
        raise ForestError(
            "PACKAGE_CONFIGURATION_BINDING_INVALID",
            "Package immutable configuration paths must be enforced by its repository",
            500,
        )
    compiled_schemas = snapshot.schemas(_component_schema_references(component))
    if (
        component.get("storage", {}).get("execution_profile") == "official"
        and trust_level != "official"
    ):
        raise ForestError(
            "PACKAGE_STORAGE_TRUST_REQUIRED",
            "Official storage execution requires an official package trust binding",
            403,
        )
    if (component.get("kernel_commands") or component.get("kernel_queries")) and (
        trust_level != "official"
        or (component.get("kernel_commands") and kernel_management_commands is None)
        or (component.get("kernel_queries") and kernel_management_queries is None)
    ):
        raise ForestError(
            "KERNEL_MANAGEMENT_AUTHORITY_UNAVAILABLE",
            "Kernel management ports require an official scope authority", 403,
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
            ("payload.read", payload_authority is not None),
            ("payload.write", payload_authority is not None),
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
    ) | frozenset(
        f"uow.execute:{item['name']}"
        for item in component.get("unit_of_work_plans", ())
        if unit_of_work_coordinator_factory is not None
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
    if (
        trust_level != "official"
        and (
            any(plan.get("read_only", False) for plan in component.get("unit_of_work_plans", ()))
            or any(item.get("read_only", False) for item in component.get("unit_of_work_participants", ()))
        )
    ):
        raise ForestError(
            "PACKAGE_UOW_READ_AUTHORITY_UNAVAILABLE",
            "Read-only UnitOfWork plans require reviewed official authority",
            403,
        )
    if component.get("unit_of_work_plans") and unit_of_work_coordinator_factory is None:
        raise ForestError(
            "PACKAGE_UOW_COORDINATOR_UNAVAILABLE",
            "Package declares UnitOfWork plans without an installed coordinator",
            503,
        )
    if component.get("configuration_apply") and configuration_apply_coordinator_factory is None:
        raise ForestError(
            "PACKAGE_CONFIGURATION_APPLY_COORDINATOR_UNAVAILABLE",
            "Package declares configuration apply without the standard coordinator",
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
            event_schema=compiled_schemas[item["schema"]],
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
            event_schema=compiled_schemas[item["schema"]],
        )
        for item in component.get("publishes", ())
    )
    component_jobs = tuple(
        RuntimeComponentJobContribution(
            handler_id=item["handler_id"],
            input_schema=compiled_schemas[item["input_schema"]],
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
            input_schema=compiled_schemas[item["input_schema"]],
            max_attempts=item["max_attempts"],
            timeout_seconds=item["timeout_s"],
            max_concurrency=item["max_concurrency"],
        )
        for item in component.get("job_types", ())
    )
    shell_commands = tuple(
        RuntimeShellCommandContribution(
            command_id=item["command_id"],
            command_version=item["command_version"],
            service_id=item["service_id"],
            service_version=item["service_version"],
            argument_schema=compiled_schemas[item["argument_schema"]],
            management_operation_id=item["management_operation_id"],
            management_action=item.get("management_action", False),
            target_scope=item["target_scope"],
            service_class=item["service_class"],
            required_capabilities=tuple(item["required_capabilities"]),
            option_aliases=tuple(
                tuple(alias) for alias in item.get("option_aliases", ())
            ),
            output_policy=item["output_policy"],
            timeout_seconds=item["timeout_s"],
        )
        for item in component.get("shell_commands", ())
    )
    agent_tools = tuple(
        RuntimeAgentToolContribution(
            name=item["name"],
            service_id=item["service_id"],
            version=item["service_version"],
            declaration=compiled_schemas[item["declaration_schema"]],
            approval_required=item["approval_required"],
            supports_query=item["supports_query"],
        )
        for item in component.get("agent_tools", ())
    )

    def factory(_runtime, contracts):
        preflights = {}
        for action in component.get("management_actions", ()):
            preflight_id = action.get("transaction_preflight")
            if preflight_id is None:
                continue
            resolver = getattr(_runtime, "resolve_management_action_preflight", None)
            if not callable(resolver):
                raise ForestError(
                    "MODULE_MANAGEMENT_PREFLIGHT_UNAVAILABLE",
                    "Declared management transaction preflight is unavailable",
                    503,
                )
            preflight = resolver(
                preflight_id,
                component_id=component["id"],
                package_digest=snapshot.digest,
                trust_level=trust_level,
            )
            if not callable(preflight):
                raise ForestError(
                    "MODULE_MANAGEMENT_PREFLIGHT_UNAVAILABLE",
                    "Declared management transaction preflight has no binding",
                    503,
                )
            preflights[action["id"]] = preflight
        shell_management_authorizer = getattr(
            _runtime, "resolve_shell_management_authorization", None
        )
        if any(
            item.get("management_action", False)
            for item in component.get("shell_commands", ())
        ) and not callable(shell_management_authorizer):
            raise ForestError(
                "SHELL_MANAGEMENT_AUTHORITY_UNAVAILABLE",
                "Declared Shell management action has no authority binding",
                503,
            )
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
        contribution_directories = {}
        for declaration in component.get("contribution_directories", ()):
            kind = declaration["kind"]
            if kind == "job-types":
                contribution_directories[kind] = contracts.job_types()
            elif kind == "shell-commands":
                contribution_directories[kind] = contracts.shell_commands()
            elif kind == "agent-tools":
                contribution_directories[kind] = contracts.agent_tools()
            elif kind == "routing-routes":
                contribution_directories[kind] = {
                    "publications": contracts.event_publications(),
                    "subscriptions": contracts.event_subscriptions(),
                    "component_jobs": contracts.component_jobs(),
                }
        participant_components = sorted(
            {
                step["component_id"]
                for plan in component.get("unit_of_work_plans", ())
                for step in plan["steps"]
            }
        )
        unit_of_work_coordinator = (
            unit_of_work_coordinator_factory(
                component["id"],
                {
                    component_id: contracts.unit_of_work_participant(component_id)
                    for component_id in participant_components
                },
            )
            if unit_of_work_coordinator_factory is not None
            and component.get("unit_of_work_plans")
            else None
        )
        configuration_apply_coordinator = (
            configuration_apply_coordinator_factory(
                component["id"],
                module_configuration,
                contracts.unit_of_work_participant("component.worker-host"),
            )
            if component.get("configuration_apply")
            else None
        )
        adapter = PackageGenerationAdapter(
            snapshot=snapshot,
            bot_id=bot_id,
            scope_id=scope_id,
            module_configuration=module_configuration,
            prepared_configuration=prepared_configuration,
            grants=grants,
            service_routes=routes,
            resolved_contracts=resolved_contracts,
            generation_verifier=generation_verifier,
            generation_member_verifier=generation_member_verifier,
            state_queries=state_queries,
            state_commands=state_commands,
            payload_authority=payload_authority,
            domain_queries=domain_queries,
            domain_commands=domain_commands,
            kernel_management_queries=kernel_management_queries,
            kernel_management_commands=kernel_management_commands,
            unit_of_work_authority=unit_of_work_authority,
            unit_of_work_coordinator=unit_of_work_coordinator,
            configuration_apply_coordinator=configuration_apply_coordinator,
            management_action_preflights=preflights,
            shell_management_authorizer=shell_management_authorizer,
            owned_work_authority=owned_work_authority,
            service_operation_ledger=service_operation_ledger,
            configuration_validator=configuration_validator,
            effect_driver_factories=effect_driver_factories,
            event_publisher=event_publisher,
            component_job_scheduler=component_job_scheduler,
            contribution_directories=contribution_directories,
            agent_tool_effects=agent_tool_effects,
            compiled_schemas=compiled_schemas,
            worker_factory=worker_factory,
        )
        if management_contract is not None:
            adapter.management_contract = management_contract
        elif management_contract_factory is not None:
            adapter.management_contract = management_contract_factory(adapter)
        return adapter

    # UnitOfWork participant handles resolve against the sealed RuntimeGraph at
    # call time.  They are transactional peers, not lifecycle dependencies;
    # turning plan edges into startup edges would reject valid cyclic workflows.
    return RuntimeModuleDescriptor(
        component_id=component["id"],
        requires=tuple(
            dict.fromkeys(
                (
                    *(
                        ("kernel.runtime", "kernel.payload", "kernel.authorization")
                        if component["scope"] == "bot" else ()
                    ),
                    *(
                        ("routing.graph",)
                        if component["scope"] == "bot" and (
                            component.get("publishes")
                            or component.get("subscribes")
                            or component.get("jobs")
                        )
                        else ()
                    ),
                    *(
                        match["component_id"]
                        for match in component.get("lifecycle", {}).get("fact_matches", ())
                    ),
                )
            )
        ),
        factory=None if plan_only else factory,
        artifact=RuntimeArtifactIdentity(
            package_id=snapshot.manifest["package_id"],
            package_version=snapshot.manifest["version"],
            package_digest=snapshot.digest,
            scope=component["scope"],
            driver="forest.python-worker/1",
            trust_level=trust_level,
        ),
        worker_limits=WorkerLimits.from_component(component),
        management=tuple(management),
        storage=storage,
        event_publications=event_publications,
        event_subscriptions=event_subscriptions,
        component_jobs=component_jobs,
        job_types=job_types,
        agent_tools=agent_tools,
        shell_commands=shell_commands,
        wakes_on_work=bool(component.get("owned_work_handlers")),
        owned_work_lifecycle=(
            OwnedWorkLifecycle.FREEZE_DRAIN
            if component.get("owned_work_handlers")
            else OwnedWorkLifecycle.NONE
        ),
        provided_services=tuple(
            (item["name"], item["contract"])
            for item in (
                *component.get("provides", ()),
                *component.get("effect_services", ()),
            )
        ),
        startup_service_requires=tuple(
            (item["name"], item["contract"])
            for item in component.get("requires", ())
            if item.get("startup_dependency") and item["name"] in resolved_contracts
        ),
        retention_requires=tuple(
            sorted(
                (
                    {
                        step["component_id"]
                        for plan in component.get("unit_of_work_plans", ())
                        for step in plan["steps"]
                    }
                    | (
                        {"component.worker-host"}
                        if component.get("configuration_apply")
                        else set()
                    )
                )
                - {component["id"]}
            )
        ),
        retention_service_requires=tuple(sorted(resolved_contracts.items())),
        fact_matches=tuple(
            RuntimeFactMatch(
                match["component_id"], match["own_pointer"], match["other_pointer"]
            )
            for match in component.get("lifecycle", {}).get("fact_matches", ())
        ),
    )
