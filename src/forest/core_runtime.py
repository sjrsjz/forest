"""Bootstrap trusted kernel authorities before admitting the package graph."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from uuid import uuid4

from forest.errors import ForestError
from forest.kernel_audit_participant import KernelAuditUnitOfWorkParticipant
from forest.kernel_authorization_participant import (
    KernelAuthorizationUnitOfWorkParticipant,
)
from forest.kernel_component_candidate_preview import component_candidate_preview_service
from forest.kernel_effect_executor_outcome import effect_executor_outcome_service
from forest.kernel_effect_participant import KernelEffectUnitOfWorkParticipant
from forest.kernel_package_catalog_snapshot import (
    package_catalog_artifact_service,
    package_catalog_page_service,
)
from forest.kernel_payload_participant import KernelPayloadUnitOfWorkParticipant
from forest.kernel_resource_admission import (
    KERNEL_RESOURCE_POLICY_DEFAULT,
    KERNEL_RESOURCE_POLICY_SCHEMA,
    assess_worker_capacity,
    validate_kernel_resource_policy,
)
from forest.kernel_runtime_graph_snapshot import (
    runtime_graph_snapshot_service,
    runtime_status_snapshot_service,
)
from forest.kernel_runtime_tool import RUNTIME_STATUS_TOOL, runtime_status_tool_service
from forest.module_management_contract import (
    AuditManagementContract,
    ModuleConfigurationContract,
)
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeKernelServiceSpec
from forest.runtime_generation import PreparedGenerationBinding
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    RuntimeWorkerCapacity,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import (
    CapabilityGrant,
    GrantSetRef,
    KernelAuthorizationRepository,
)
from forest.storage.effect_authority import KernelEffectRepository
from forest.storage.kernel_audit import KernelAuditRepository
from forest.storage.kernel_runtime import KernelRuntimeRepository
from forest.storage.management_asset_authority import ManagementAssetAuthority
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.payload_authority import KernelPayloadRepository
from forest.unit_of_work import (
    ComponentConfigurationApplyScope,
    PostgresComponentConfigurationApplyCoordinator,
)
from forest.web_operation_policy import (
    WEB_OPERATION_POLICY_DEFAULT,
    WEB_OPERATION_POLICY_SCHEMA,
    validate_web_operation_policy,
)

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}
_KERNEL_READ_SERVICES = frozenset({
    "payload.verify", "payload.read", "authorization.grants",
    "audit.query", "audit.page.snapshot",
    "runtime.graph-replacement.status", "runtime.graph.snapshot",
    "runtime.status.snapshot", "runtime.component-candidate.preview",
    "deployment.package-registry.packages",
    "deployment.package-registry.artifact",
    "agent.tool.runtime-status",
})


_KERNEL_CONFIGURATION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"protocol": {"const": "forest.kernel-module/1"}},
    "required": ["protocol"],
    "additionalProperties": False,
}

_MODULES = {
    "kernel.payload": {
        "schema_id": "forest.kernel.payload",
        "repository_contract": "forest.kernel-payload.repository/1",
        "resource": "storage/official/kernel_payload.sql",
        "requires": (),
        "services": ("payload.put", "payload.verify", "payload.read", "payload.forget"),
        "capabilities": ("payload.read", "payload.write", "payload.forget"),
    },
    "kernel.authorization": {
        "schema_id": "forest.kernel.authorization",
        "repository_contract": "forest.kernel-authorization.repository/1",
        "resource": "storage/official/kernel_authorization.sql",
        "requires": (),
        "services": (
            "authorization.grants",
            "authorization.issue",
            "authorization.consume",
            "authorization.revoke",
        ),
        "capabilities": (
            "authorization.grants",
            "authorization.issue",
            "authorization.consume",
            "authorization.revoke",
        ),
    },
    "kernel.effect": {
        "schema_id": "forest.kernel.effect",
        "repository_contract": "forest.kernel-effect.repository/1",
        "resource": "storage/official/kernel_effect.sql",
        "requires": ("kernel.payload", "kernel.authorization"),
        "services": ("effect.prepare", "effect.handoff", "effect.observe", "effect.resolve"),
        "capabilities": (
            "effect.prepare",
            "effect.handoff",
            "effect.observe",
            "effect.resolve",
        ),
    },
    "kernel.audit": {
        "schema_id": "forest.kernel.audit",
        "repository_contract": "forest.kernel-audit.repository/1",
        "resource": "storage/official/kernel_audit.sql",
        "requires": ("kernel.payload",),
        "services": ("audit.append", "audit.query", "audit.page.snapshot"),
        "capabilities": ("audit.append", "audit.query"),
    },
    "kernel.runtime": {
        "schema_id": "forest.kernel.runtime",
        "repository_contract": "forest.kernel-runtime.repository/1",
        "resource": "storage/official/kernel_runtime.sql",
        "requires": (
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "kernel.audit",
        ),
        "services": (
            "runtime.graph.publish",
            "runtime.graph.retire",
            "runtime.call.admit",
            "runtime.state.publish",
            "runtime.graph.reload",
            "runtime.graph-replacement.request",
            "runtime.graph-replacement.status",
            "runtime.effect-executor-outcome",
            "runtime.graph.snapshot",
            "runtime.status.snapshot",
            "runtime.maintenance.set",
            "runtime.component-candidate.preview",
            "deployment.package-registry.packages",
            "deployment.package-registry.artifact",
            "agent.tool.runtime-status",
        ),
        "capabilities": (
            "runtime.graph.publish",
            "runtime.graph.retire",
            "runtime.call.admit",
            "runtime.state.publish",
            "runtime.graph.reload",
            "runtime.graph-replacement.request",
            "runtime.graph-replacement.status",
            "runtime.effect-executor-outcome",
            "runtime.graph.snapshot",
            "runtime.status.snapshot",
            "runtime.maintenance.set",
            "runtime.component-candidate.preview",
            "deployment.package-registry.packages",
            "deployment.package-registry.artifact",
        ),
    },
}


class PreparedAuthorityModule:
    """Lifecycle shell around one already-installed kernel Repository authority."""

    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        component_id: str,
        service,
        configuration: ModuleConfigurationRepository,
        grants: GrantSetRef,
        services: tuple[str, ...],
        generation_authority,
    ) -> None:
        self.component_id = component_id
        self.service = service
        self.module_configuration = configuration
        self.grants = grants
        self.generation_authority = generation_authority
        self.runtime_contract = {
            "protocol": "forest.runtime-services/1",
            "services": [
                {
                    "service_id": service_id,
                    "version": 1,
                    "semantics": (
                        "read" if service_id in _KERNEL_READ_SERVICES else "idempotent"
                    ),
                }
                for service_id in services
            ],
        }
        self.generation_identity = None
        self.generation_active = False
        self._configuration_apply_coordinator = None
        self._management_inflight = 0
        self._management_drained = asyncio.Event()
        self._management_drained.set()

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired()
        return PreparedGenerationBinding(
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
            config_revision=desired.revision,
            config_payload_id=desired.payload_id,
            config_payload_digest=desired.payload_digest,
            instance_id=f"instance:{self.component_id}:{context.activation_id}",
            worker_incarnation=f"worker:{self.component_id}:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

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
        identity = self.generation_identity
        coordinator = self._configuration_apply_coordinator
        if not self.generation_active or identity is None or coordinator is None:
            raise ForestError(
                "KERNEL_CONFIGURATION_APPLY_UNAVAILABLE",
                "Kernel authority Generation is not active for configuration apply",
                409,
            )
        self._management_inflight += 1
        self._management_drained.clear()
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
            self._management_inflight -= 1
            if self._management_inflight == 0:
                self._management_drained.set()

    async def bind_generation(self, identity) -> None:
        if identity.component_id != self.component_id:
            raise ValueError("Generation identity belongs to another component")
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ValueError("Generation identity is immutable")
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None:
            raise RuntimeError("Generation identity was not bound before prewarm")

    def activate_generation(self) -> None:
        if self.generation_identity is None:
            raise RuntimeError("Generation identity was not bound before activation")
        self.generation_active = True

    async def quiesce(self) -> None:
        self.generation_active = False
        await self._management_drained.wait()

    async def close(self) -> None:
        self.generation_active = False
        await self._management_drained.wait()


def runtime_graph_replacement_request_service(context):
    """Accept a fenced, idempotent graph rebuild intent from a capable module."""

    runtime = context.participant("kernel.runtime").service

    async def request(payload):
        frozen = (
            type(payload) is dict
            and payload.get("protocol") == "forest.runtime-graph-replacement-request/3"
        )
        required = {
            "protocol",
            "operation_id",
            "expected_graph_revision",
            "requested_by",
            "target_component_id",
        }
        if frozen:
            required |= {"selection_revision", "selection_digest"}
        if (
            type(payload) is not dict
            or set(payload) != required
            or payload["protocol"] not in {
                "forest.runtime-graph-replacement-request/2",
                "forest.runtime-graph-replacement-request/3",
            }
            or not isinstance(payload["operation_id"], str)
            or not 1 <= len(payload["operation_id"].encode()) <= 256
            or type(payload["expected_graph_revision"]) is not int
            or payload["expected_graph_revision"] <= 0
            or not isinstance(payload["requested_by"], str)
            or not 1 <= len(payload["requested_by"].encode()) <= 256
            or (
                payload["target_component_id"] is not None
                and (
                    not isinstance(payload["target_component_id"], str)
                    or re.fullmatch(
                        r"[a-z0-9][a-z0-9._-]{0,127}", payload["target_component_id"]
                    ) is None
                )
            )
            or (
                frozen
                and (
                    type(payload["selection_revision"]) is not int
                    or payload["selection_revision"] < 0
                    or not isinstance(payload["selection_digest"], str)
                    or re.fullmatch(r"sha256:[a-f0-9]{64}", payload["selection_digest"])
                    is None
                )
            )
        ):
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID",
                "Graph replacement request is invalid",
                422,
            )
        saved = await runtime.request_graph_replacement(
            context.host_lease,
            operation_id=payload["operation_id"],
            expected_graph_revision=payload["expected_graph_revision"],
            requested_by=payload["requested_by"],
            target_component_id=payload["target_component_id"],
            selection_revision=payload["selection_revision"] if frozen else None,
            selection_digest=payload["selection_digest"] if frozen else None,
        )
        return {
            "protocol": (
                "forest.runtime-graph-replacement-receipt/3" if frozen
                else "forest.runtime-graph-replacement-receipt/2"
            ),
            "operation_id": saved["operation_id"],
            "activation_id": saved["activation_id"],
            "state": saved["state"],
            "expected_graph_revision": saved["expected_graph_revision"],
            "target_component_id": saved["target_component_id"],
            **(
                {
                    "selection_revision": saved["selection_revision"],
                    "selection_digest": saved["selection_digest"],
                }
                if frozen
                else {}
            ),
        }

    return request


def runtime_maintenance_set_service(context):
    """Apply one fenced DATA admission transition; CONTROL remains available."""

    runtime = context.participant("kernel.runtime").service

    async def apply(payload):
        if (
            type(payload) is not dict
            or set(payload) != {"protocol", "expected_maintenance_epoch", "data_admission"}
            or payload["protocol"] != "forest.runtime-maintenance-set-request/1"
            or type(payload["expected_maintenance_epoch"]) is not int
            or payload["expected_maintenance_epoch"] <= 0
            or type(payload["data_admission"]) is not bool
        ):
            raise ForestError(
                "RUNTIME_MAINTENANCE_INPUT_INVALID", "Maintenance request is invalid", 422
            )
        saved = await runtime.set_data_admission(
            context.host_lease,
            expected_maintenance_epoch=payload["expected_maintenance_epoch"],
            data_admission=payload["data_admission"],
        )
        return {
            "protocol": "forest.runtime-maintenance-set-receipt/1",
            "maintenance_epoch": saved["maintenance_epoch"],
            "data_admission": saved["data_admission"],
        }

    return apply


def runtime_graph_replacement_status_service(context):
    """Read one persisted replacement outcome by its stable operation identity."""

    runtime = context.participant("kernel.runtime").service

    async def status(payload):
        if (
            type(payload) is not dict
            or set(payload) != {"protocol", "operation_id"}
            or payload["protocol"] != "forest.runtime-graph-replacement-status-request/1"
        ):
            raise ForestError(
                "RUNTIME_REPLACEMENT_INPUT_INVALID", "Replacement status request is invalid", 422
            )
        saved = await runtime.graph_replacement(payload["operation_id"])
        return {
            "protocol": "forest.runtime-graph-replacement-status/1",
            "operation_id": saved["operation_id"],
            "state": saved["state"],
            "graph_id": saved["result_graph_id"],
            "graph_revision": saved["result_graph_revision"],
            "error_code": saved["error_code"],
        }

    return status


class PreparedAuditAuthorityModule(PreparedAuthorityModule):
    """Expose audit storage and a bounded page through its own Generation."""

    def __init__(
        self,
        component_id,
        service,
        configuration,
        grants,
        services,
        generation_authority,
    ) -> None:
        super().__init__(
            component_id,
            service,
            configuration,
            grants,
            services,
            generation_authority,
        )
        self.management_contract = AuditManagementContract(
            configuration,
            audit=service,
            schema=_KERNEL_CONFIGURATION_SCHEMA,
            apply_callback=self.apply_configuration,
        )

    def runtime_service_handlers(self) -> dict:
        return {
            ("audit.append", 1): self.service.append,
            ("audit.query", 1): self.service.scan,
            ("audit.page.snapshot", 1): self._page,
        }

    async def _page(self, payload):
        fields = {
            "action", "producer_component_id", "origin_kind", "resource_kind",
            "resource_id", "operation_kind", "operation_id", "before", "limit",
        }
        if (
            type(payload) is not dict
            or payload.get("protocol") != "forest.audit-page-request/1"
            or set(payload) - fields - {"protocol"}
        ):
            raise ForestError("AUDIT_QUERY_INVALID", "Audit page request is invalid", 422)
        return {
            "protocol": "forest.audit-page/1",
            **await self.service.scan(**{key: value for key, value in payload.items() if key in fields}),
        }

    def unit_of_work_participant(self):
        return KernelAuditUnitOfWorkParticipant(self, self.service)


class PreparedAuthorizationAuthorityModule(PreparedAuthorityModule):
    """Publish the Bot Web operation policy from the authorization Generation."""

    def __init__(
        self,
        component_id,
        service,
        configuration,
        grants,
        services,
        generation_authority,
    ) -> None:
        super().__init__(
            component_id,
            service,
            configuration,
            grants,
            services,
            generation_authority,
        )
        self.management_contract = ModuleConfigurationContract(
            configuration,
            schema=WEB_OPERATION_POLICY_SCHEMA,
            validator=validate_web_operation_policy,
            apply_callback=self.apply_configuration,
        )

    def unit_of_work_participant(self):
        return KernelAuthorizationUnitOfWorkParticipant(self, self.service)


class PreparedRuntimeAuthorityModule(PreparedAuthorityModule):
    """Publish the Bot-wide Worker admission policy from kernel.runtime."""

    def __init__(self, *args, runtime_host) -> None:
        super().__init__(*args)
        self.runtime_host = runtime_host
        self.management_contract = ModuleConfigurationContract(
            self.module_configuration,
            schema=KERNEL_RESOURCE_POLICY_SCHEMA,
            validator=validate_kernel_resource_policy,
            apply_callback=self.apply_configuration,
        )

    async def apply_configuration(self, **kwargs) -> dict:
        lock = self.runtime_host._replacement_lock
        if lock is None or self.runtime_host.registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
        async with lock:
            return await super().apply_configuration(**kwargs)

    async def validate_capacity_apply(
        self, value: object, revision: int, payload_digest: str
    ) -> None:
        """Reject a policy unable to rebuild the currently active runtime."""

        registry = self.runtime_host.registry
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
        workers = tuple(
            (item.component_id, item.worker_limits)
            for item in registry.descriptors
            if item.worker_limits is not None
        )
        replacement = set(registry.replacement_closure("kernel.runtime"))
        assess_worker_capacity(
            RuntimeWorkerCapacity(
                active=workers,
                preparing=tuple(
                    (component_id, limits)
                    for component_id, limits in workers
                    if component_id in replacement
                ),
            ),
            policy_revision=revision,
            policy_payload_digest=payload_digest,
            policy=value,
        )


class PreparedEffectAuthorityModule(PreparedAuthorityModule):
    """Publish only the typed Kernel Effect UnitOfWork participant ABI."""

    def __init__(
        self,
        component_id,
        service,
        configuration,
        grants,
        services,
        generation_authority,
        payloads,
    ) -> None:
        super().__init__(
            component_id,
            service,
            configuration,
            grants,
            services,
            generation_authority,
        )
        self.payloads = payloads

    def unit_of_work_participant(self):
        return KernelEffectUnitOfWorkParticipant(self, self.payloads)


class PreparedPayloadAuthorityModule(PreparedAuthorityModule):
    """Publish generic Payload write, deletion-fence and revocation participants."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.management_assets = ManagementAssetAuthority(self.service)
        self._scrub_task: asyncio.Task | None = None

    def activate_generation(self) -> None:
        if self._scrub_task is not None:
            raise ForestError(
                "PAYLOAD_GENERATION_ALREADY_ACTIVATED",
                "Payload scrubber already belongs to this Generation",
                409,
            )
        super().activate_generation()
        self._scrub_task = asyncio.create_task(
            self._scrub_revoked(),
            name=f"kernel-payload-scrub:{self.generation_identity.generation_id}",
        )

    async def _scrub_revoked(self) -> None:
        while True:
            expired = await self.management_assets.reap_expired(limit=16)
            result = await self.service.scrub(limit=100)
            await asyncio.sleep(0.1 if result["scrubbed"] or expired["reaped"] else 10.0)

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return (self._scrub_task,) if self._scrub_task is not None else ()

    async def _stop_scrubber(self) -> None:
        if self._scrub_task is not None:
            task = self._scrub_task
            was_done = task.done()
            task.cancel()
            outcome, = await asyncio.gather(task, return_exceptions=True)
            self._scrub_task = None
            if isinstance(outcome, BaseException) and not isinstance(
                outcome, asyncio.CancelledError
            ):
                raise outcome
            if was_done and not isinstance(outcome, asyncio.CancelledError):
                raise ForestError(
                    "PAYLOAD_SCRUBBER_STOPPED",
                    "Payload scrubber stopped before its Generation quiesced",
                    503,
                )

    async def quiesce(self) -> None:
        await super().quiesce()
        await self._stop_scrubber()

    async def close(self) -> None:
        await self._stop_scrubber()
        await super().close()

    def unit_of_work_participant(self):
        return KernelPayloadUnitOfWorkParticipant(self, self.service)


@dataclass(frozen=True)
class KernelModules:
    runtime: KernelRuntimeRepository
    payload: KernelPayloadRepository
    authorization: KernelAuthorizationRepository
    effect: KernelEffectRepository
    audit: KernelAuditRepository
    descriptors: tuple[RuntimeModuleDescriptor, ...]
    configurations: dict[str, ModuleConfigurationRepository]
    grants: dict[str, GrantSetRef]


async def bootstrap_kernel(store, installer) -> KernelModules:
    """Install and initialize the exact kernel authorities needed to publish a graph."""

    await installer.initialize_kernel()
    bundles = {
        component_id: official_storage_bundle(
            component_id,
            definition["schema_id"],
            definition["repository_contract"],
            definition["resource"],
        )
        for component_id, definition in _MODULES.items()
    }
    for component_id in sorted(bundles):
        bundle = bundles[component_id]
        await installer.install(
            bundle.spec,
            bundle.baseline,
            bundle.package_digest,
            execution_profile=bundle.execution_profile,
        )

    payload = KernelPayloadRepository(store, namespace=bundles["kernel.payload"].spec.namespace)
    authorization = KernelAuthorizationRepository(
        store, namespace=bundles["kernel.authorization"].spec.namespace
    )
    runtime = KernelRuntimeRepository(store, bundles["kernel.runtime"].spec.namespace)

    async def verify_effect_fence(
        conn,
        *,
        executor_component_id,
        executor_generation_id,
        fence_kind,
        fence_id,
        fence_epoch,
    ):
        if fence_kind != "runtime.graph":
            raise ForestError(
                "EFFECT_STALE_FENCE", "Executor RuntimeGraph fence is no longer current", 409
            )
        try:
            await runtime.verify_generation_graph_fence_in(
                conn,
                component_id=executor_component_id,
                generation_id=executor_generation_id,
                graph_id=fence_id,
                graph_revision=fence_epoch,
            )
        except ForestError as exc:
            raise ForestError(
                "EFFECT_STALE_FENCE", "Executor RuntimeGraph fence is no longer current", 409
            ) from exc

    effect = KernelEffectRepository(
        store,
        namespace=bundles["kernel.effect"].spec.namespace,
        payload_verifier=payload.verify_in,
        authorization_consumer=authorization.consume_ticket_in,
        fence_verifier=verify_effect_fence,
        fence_kind="runtime.graph",
    )
    audit = KernelAuditRepository(
        store,
        namespace=bundles["kernel.audit"].spec.namespace,
        payload_verifier=payload.verify_in,
    )
    services = {
        "kernel.payload": payload,
        "kernel.authorization": authorization,
        "kernel.effect": effect,
        "kernel.audit": audit,
        "kernel.runtime": runtime,
    }
    configurations = {
        component_id: ModuleConfigurationRepository(
            store,
            component_id=component_id,
            namespace=bundle.spec.namespace,
            payloads=payload,
        )
        for component_id, bundle in bundles.items()
    }
    for component_id, configuration in configurations.items():
        value = (
            WEB_OPERATION_POLICY_DEFAULT
            if component_id == "kernel.authorization"
            else KERNEL_RESOURCE_POLICY_DEFAULT
            if component_id == "kernel.runtime"
            else {"protocol": "forest.kernel-module/1"}
        )
        schema = (
            WEB_OPERATION_POLICY_SCHEMA
            if component_id == "kernel.authorization"
            else KERNEL_RESOURCE_POLICY_SCHEMA
            if component_id == "kernel.runtime"
            else _KERNEL_CONFIGURATION_SCHEMA
        )
        await configuration.initialize(
            operation_id=f"bootstrap:{component_id}:configuration",
            value=value,
            validation_digest=digest_json(schema),
        )

    policy_revision = await authorization.revision()
    grants = {}
    for component_id, definition in _MODULES.items():
        grants[component_id] = await authorization.create_grant_set(
            grantee_kind="component",
            grantee_id=component_id,
            entries=tuple(
                CapabilityGrant(capability, "bot", store.bot_id)
                for capability in definition["capabilities"]
            ),
            expected_policy_revision=policy_revision,
        )

    artifact = official_artifact("bot")

    def authority_factory(component_id, definition):
        prepared_type = {
            "kernel.audit": PreparedAuditAuthorityModule,
            "kernel.authorization": PreparedAuthorizationAuthorityModule,
            "kernel.payload": PreparedPayloadAuthorityModule,
            "kernel.runtime": PreparedRuntimeAuthorityModule,
        }.get(component_id, PreparedAuthorityModule)

        def create(_runtime, _modules):
            arguments = (
                component_id,
                services[component_id],
                configurations[component_id],
                grants[component_id],
                definition["services"],
                runtime,
            )
            if component_id == "kernel.effect":
                return PreparedEffectAuthorityModule(*arguments, payload)
            module = (
                prepared_type(*arguments, runtime_host=_runtime)
                if component_id == "kernel.runtime"
                else prepared_type(*arguments)
            )
            if component_id in {"kernel.audit", "kernel.authorization", "kernel.runtime"}:
                module._configuration_apply_coordinator = (
                    PostgresComponentConfigurationApplyCoordinator(
                        store,
                        runtime,
                        component_id=component_id,
                        configuration=configurations[component_id],
                        control_handle=_modules.unit_of_work_participant(
                            "component.worker-host"
                        ),
                        pre_commit_validator=(
                            module.validate_capacity_apply
                            if component_id == "kernel.runtime" else None
                        ),
                    )
                )
            return module

        return create

    descriptors = tuple(
        RuntimeModuleDescriptor(
            component_id,
            definition["requires"],
            authority_factory(component_id, definition),
            artifact,
            management=(
                (
                    RuntimeManagementContribution(
                        "audit-explorer",
                        "审计记录",
                        ("bot",),
                        "/static/modules/kernel-audit.html",
                        "/static/kernel-audit.js",
                        style="/static/modules/kernel-audit.css",
                        group="security",
                        order=5,
                    ),
                )
                if component_id == "kernel.audit"
                else (
                    RuntimeManagementContribution(
                        "web-operation-policy",
                        "Web 操作策略",
                        ("bot",),
                        "/static/modules/kernel-authorization-policy.html",
                        "/static/kernel-authorization-policy.js",
                        style="/static/modules/kernel-authorization-policy.css",
                        group="security",
                        order=4,
                    ),
                )
                if component_id == "kernel.authorization"
                else (
                    RuntimeManagementContribution(
                        "worker-capacity",
                        "Bot Worker 容量",
                        ("bot",),
                        "/static/modules/kernel-runtime-capacity.html",
                        "/static/kernel-runtime-capacity.js",
                        style="/static/modules/kernel-runtime-capacity.css",
                        group="runtime",
                        order=2,
                    ),
                )
                if component_id == "kernel.runtime"
                else ()
            ),
            storage=bundles[component_id],
            kernel_services=(
                (
                    RuntimeKernelServiceSpec(
                        RUNTIME_STATUS_TOOL.service_id,
                        RUNTIME_STATUS_TOOL.version,
                        ("kernel.runtime", "kernel.payload"),
                        runtime_status_tool_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.graph.snapshot",
                        1,
                        ("kernel.runtime",),
                        runtime_graph_snapshot_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.status.snapshot",
                        1,
                        ("kernel.runtime",),
                        runtime_status_snapshot_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.maintenance.set",
                        1,
                        ("kernel.runtime",),
                        runtime_maintenance_set_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.component-candidate.preview",
                        1,
                        ("kernel.runtime",),
                        component_candidate_preview_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "deployment.package-registry.packages",
                        1,
                        ("kernel.runtime",),
                        package_catalog_page_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "deployment.package-registry.artifact",
                        1,
                        ("kernel.runtime",),
                        package_catalog_artifact_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.graph-replacement.request",
                        1,
                        ("kernel.runtime",),
                        runtime_graph_replacement_request_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.graph-replacement.status",
                        1,
                        ("kernel.runtime",),
                        runtime_graph_replacement_status_service,
                    ),
                    RuntimeKernelServiceSpec(
                        "runtime.effect-executor-outcome",
                        1,
                        ("kernel.runtime", "kernel.effect", "kernel.payload"),
                        effect_executor_outcome_service,
                    ),
                )
                if component_id == "kernel.runtime"
                else ()
            ),
            provided_services=tuple(
                (service_id, "1.0") for service_id in definition["services"]
            ),
            agent_tools=((RUNTIME_STATUS_TOOL,) if component_id == "kernel.runtime" else ()),
            shell_commands=(),
            job_types=(),
            retention_requires=(
                ("component.worker-host",)
                if component_id in {"kernel.audit", "kernel.authorization"}
                else ()
            ),
        )
        for component_id, definition in _MODULES.items()
    )
    return KernelModules(
        runtime,
        payload,
        authorization,
        effect,
        audit,
        descriptors,
        configurations,
        grants,
    )
