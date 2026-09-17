"""Generation-bound Conversation Scheduler ingress module."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class CleanConversationSchedulerRuntime:
    """Typed scheduler authority port fenced by its exact Runtime Generation."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "conversation.admit", "version": 1},
            {"service_id": "conversation.classify", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(self, *, repository, module_configuration, grants, generation_verifier):
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_identity: BoundGenerationIdentity | None = None
        self.active = False

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
            instance_id=f"instance:conversation.scheduler:{context.activation_id}",
            worker_incarnation=f"worker:conversation.scheduler:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "conversation.scheduler":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Conversation received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Conversation Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Conversation Generation identity was not bound",
                503,
            )

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False

    async def close(self) -> None:
        self.active = False

    async def _verify(self, conn, *, admission_lane: str) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Conversation Generation is not accepting work",
                409,
            )
        await self.generation_verifier(
            conn,
            component_id="conversation.scheduler",
            generation_id=self.generation_identity.generation_id,
            admission_lane=admission_lane,
        )

    async def admit_message_in(self, conn, **values):
        await self._verify(conn, admission_lane="DATA")
        return await self.repository.admit_message_in(conn, **values)

    async def complete_classification_in(self, conn, **values):
        await self._verify(conn, admission_lane="DATA")
        return await self.repository.complete_classification_in(conn, **values)
