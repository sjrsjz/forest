"""Bootstrap the required clean kernel modules before any business graph is admitted."""

from __future__ import annotations

from dataclasses import dataclass

from forest.builtin_audit_shell import AUDIT_SHELL_COMMANDS, audit_shell_service
from forest.builtin_runtime_shell import RUNTIME_SHELL_COMMANDS, runtime_shell_service
from forest.builtin_runtime_tool import RUNTIME_STATUS_TOOL, runtime_status_tool_service
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeKernelServiceSpec
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
        "services": ("audit.append", "audit.query", "shell.audit.execute"),
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
            "kernel.uow.conversation.final-reply",
            "kernel.uow.conversation.select-input",
            "kernel.uow.conversation.session-handoff",
            "kernel.uow.conversation.defer-approval",
            "kernel.uow.onebot.ingress",
            "kernel.uow.shell.result",
            "kernel.uow.shell.binding-fence",
            "kernel.uow.shell.route-fence",
            "kernel.uow.shell.authorization.consume",
            "shell.core-runtime.execute",
            "agent.tool.runtime-status",
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
            "services": [{"service_id": service_id, "version": 1} for service_id in services],
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

    def activate_generation(self) -> None:
        if self.generation_identity is None:
            raise RuntimeError("Generation identity was not bound before activation")
        self.generation_active = True

    async def quiesce(self) -> None:
        self.generation_active = False

    async def close(self) -> None:
        self.generation_active = False


class PreparedAuditAuthorityModule(PreparedAuthorityModule):
    """Expose the audit Repository and its Shell projection from its own Generation."""

    def __init__(self, component_id, service, configuration, grants, services) -> None:
        super().__init__(component_id, service, configuration, grants, services)
        self._shell_execute = audit_shell_service(service)

    def runtime_service_handlers(self) -> dict:
        return {
            ("audit.append", 1): self.service.append,
            ("audit.query", 1): self.service.scan,
            ("shell.audit.execute", 1): self._shell_execute,
        }


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

    async def verify_effect_fence(
        conn,
        *,
        executor_component_id,
        executor_generation_id,
        fence_kind,
        fence_id,
        fence_epoch,
    ):
        graph = await runtime.verify_generation_member_in(
            conn,
            component_id=executor_component_id,
            generation_id=executor_generation_id,
        )
        if (
            fence_kind != "runtime.graph"
            or fence_id != graph.graph_id
            or fence_epoch != graph.graph_revision
        ):
            raise ForestError(
                "EFFECT_STALE_FENCE", "Executor RuntimeGraph fence is no longer current", 409
            )

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
    descriptors = tuple(
        RuntimeModuleDescriptor(
            component_id,
            definition["requires"],
            lambda _runtime, _modules, identity=component_id, service=services[component_id], configuration=configurations[component_id], grant=grants[component_id], provided_services=definition["services"]: (
                (
                    PreparedAuditAuthorityModule
                    if identity == "kernel.audit"
                    else PreparedAuthorityModule
                )(
                    identity,
                    service,
                    configuration,
                    grant,
                    provided_services,
                )
            ),
            artifact,
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
                        "shell.core-runtime.execute",
                        1,
                        ("kernel.runtime",),
                        runtime_shell_service,
                    ),
                )
                if component_id == "kernel.runtime"
                else ()
            ),
            agent_tools=(
                (RUNTIME_STATUS_TOOL,) if component_id == "kernel.runtime" else ()
            ),
            shell_commands=(
                RUNTIME_SHELL_COMMANDS
                if component_id == "kernel.runtime"
                else AUDIT_SHELL_COMMANDS
                if component_id == "kernel.audit"
                else ()
            ),
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
