"""Install and describe the clean Delivery authority module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.delivery_authorization import DeliveryAuthorizationPort
from forest.delivery_worker import CleanDeliveryWorker
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.delivery_authority import DeliveryRepository
from forest.storage.module_configuration import ModuleConfigurationRepository

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class CleanDeliveryRuntime:
    """Generation identity and admission boundary for the Delivery authority."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "delivery.intent.create", "version": 1},
            {"service_id": "delivery.intent.observe", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        authorization,
        adapter,
        payloads,
        module_configuration,
        grants,
    ) -> None:
        self.repository = repository
        self.authorization = authorization
        self.adapter = adapter
        self.payloads = payloads
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_identity: BoundGenerationIdentity | None = None
        self.active = False
        self.worker: CleanDeliveryWorker | None = None

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
            instance_id=f"instance:delivery:{context.activation_id}",
            worker_incarnation=f"worker:delivery:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "delivery":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Delivery received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Delivery Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Delivery Generation identity was not bound",
                503,
            )
        self.worker = CleanDeliveryWorker(
            repository=self.repository,
            authorization=self.authorization,
            adapter=self.adapter,
            payloads=self.payloads,
            owner_id=("delivery/" + self.generation_identity.generation_id)[-256:],
        )
        self.worker.start()

    def activate_generation(self) -> None:
        self.active = True
        self.worker.activate()

    async def quiesce(self) -> None:
        if self.worker is not None:
            await self.worker.quiesce()
        self.active = False

    async def close(self) -> None:
        if self.worker is not None:
            await self.worker.close()
        self.active = False

    def owned_tasks(self) -> tuple:
        return self.worker.owned_tasks() if self.worker is not None else ()


@dataclass(frozen=True)
class CleanDeliveryModule:
    repository: DeliveryRepository
    authorization: DeliveryAuthorizationPort
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_delivery(store, installer, *, kernel, adapter) -> CleanDeliveryModule:
    storage = official_storage_bundle(
        "delivery",
        "forest.delivery",
        "forest.delivery.repository/1",
        "storage/official/delivery.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="delivery",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"protocol": {"const": "forest.delivery/1"}},
        "required": ["protocol"],
        "additionalProperties": False,
    }
    await configuration.initialize(
        operation_id="bootstrap:delivery:configuration",
        value={"protocol": "forest.delivery/1"},
        validation_digest=digest_json(schema),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="delivery",
        entries=(
            CapabilityGrant("delivery.intent", "bot", store.bot_id),
            CapabilityGrant("delivery.send", "bot", store.bot_id, delegable=True),
        ),
        expected_policy_revision=policy_revision,
    )
    repository = DeliveryRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=kernel.payload.verify_in,
        binding_fence_verifier=adapter.verify_transport_fence_in,
        effect_admitter=kernel.effect.admit_in,
        effect_permitter=kernel.effect.permit_in,
        effect_handoff=kernel.effect.mark_handoff_in,
        effect_observer=kernel.effect.observe_in,
        effect_abandoner=kernel.effect.abandon_prepared_in,
    )
    authorization = DeliveryAuthorizationPort(
        authority=kernel.authorization,
        delivery_grants=grants,
    )

    def factory(_runtime, modules):
        return CleanDeliveryRuntime(
            repository=repository,
            authorization=authorization,
            adapter=modules["adapter.onebot"],
            payloads=kernel.payload,
            module_configuration=configuration,
            grants=grants,
        )

    descriptor = RuntimeModuleDescriptor(
        "delivery",
        (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "adapter.onebot",
        ),
        factory,
        official_artifact("bot"),
        storage=storage,
    )
    return CleanDeliveryModule(repository, authorization, descriptor, configuration, grants)
