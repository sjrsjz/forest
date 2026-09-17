"""Bootstrap the required clean kernel modules before any business graph is admitted."""

from __future__ import annotations

from dataclasses import dataclass

from forest.packages import digest_json
from forest.runtime_generation import PreparedGenerationBinding
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
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
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.payload_authority import KernelPayloadRepository

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
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
        "services": ("audit.append", "audit.query"),
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
        ),
        "capabilities": (
            "runtime.graph.publish",
            "runtime.graph.retire",
            "runtime.call.admit",
            "runtime.state.publish",
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
    ) -> None:
        self.component_id = component_id
        self.service = service
        self.module_configuration = configuration
        self.grants = grants
        self.runtime_contract = {
            "protocol": "forest.runtime-services/1",
            "services": [
                {"service_id": service_id, "version": 1} for service_id in services
            ],
        }
        self.generation_identity = None
        self.generation_active = False

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired()
        return PreparedGenerationBinding(
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
            instance_id=f"instance:{self.component_id}:{context.activation_id}",
            worker_incarnation=f"worker:{self.component_id}:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity) -> None:
        if identity.component_id != self.component_id:
            raise ValueError("Generation identity belongs to another component")
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ValueError("Generation identity is immutable")
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None:
            raise RuntimeError("Generation identity was not bound before prewarm")

    async def activate_generation(self) -> None:
        if self.generation_identity is None:
            raise RuntimeError("Generation identity was not bound before activation")
        self.generation_active = True

    async def quiesce(self) -> None:
        self.generation_active = False

    async def close(self) -> None:
        self.generation_active = False


@dataclass(frozen=True)
class CleanKernelModules:
    runtime: KernelRuntimeRepository
    payload: KernelPayloadRepository
    authorization: KernelAuthorizationRepository
    effect: KernelEffectRepository
    audit: KernelAuditRepository
    descriptors: tuple[RuntimeModuleDescriptor, ...]
    configurations: dict[str, ModuleConfigurationRepository]
    grants: dict[str, GrantSetRef]


async def bootstrap_clean_kernel(store, installer) -> CleanKernelModules:
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
    effect = KernelEffectRepository(
        store,
        namespace=bundles["kernel.effect"].spec.namespace,
        payload_verifier=payload.verify_in,
        authorization_consumer=authorization.consume_ticket_in,
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
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"protocol": {"const": "forest.kernel-module/1"}},
            "required": ["protocol"],
            "additionalProperties": False,
        }
        await configuration.initialize(
            operation_id=f"bootstrap:{component_id}:configuration",
            value={"protocol": "forest.kernel-module/1"},
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
    prepared = {
        component_id: PreparedAuthorityModule(
            component_id,
            services[component_id],
            configurations[component_id],
            grants[component_id],
            definition["services"],
        )
        for component_id, definition in _MODULES.items()
    }
    descriptors = tuple(
        RuntimeModuleDescriptor(
            component_id,
            definition["requires"],
            lambda _runtime, _modules, module=prepared[component_id]: module,
            artifact,
            storage=bundles[component_id],
        )
        for component_id, definition in _MODULES.items()
    )
    return CleanKernelModules(
        runtime,
        payload,
        authorization,
        effect,
        audit,
        descriptors,
        configurations,
        grants,
    )
