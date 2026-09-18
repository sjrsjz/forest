"""Generation-bound runtime shell for the Bot-global multimodal Memory graph."""

from __future__ import annotations

from dataclasses import dataclass

from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


@dataclass(frozen=True)
class FrozenMemoryProfile:
    endpoint: str
    credential_ref: str
    embedding: EmbeddingProfile
    fusion_policy_revision: str


class CleanMemoryRuntime:
    """Expose attributed graph reads while Memory owns its complete private schema."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "memory.graph.trunks.list", "version": 1},
            {"service_id": "memory.graph.sources.query", "version": 1},
            {"service_id": "memory.graph.relations.query", "version": 1},
        ],
        "requires": [],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        module_configuration,
        grants,
        generation_verifier,
        generation_admission_verifier,
        generation_member_verifier,
    ) -> None:
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_admission_verifier = generation_admission_verifier
        self.generation_member_verifier = generation_member_verifier
        self.generation_identity: BoundGenerationIdentity | None = None
        self.profile: FrozenMemoryProfile | None = None
        self.active = False

    def runtime_service_handlers(self) -> dict:
        return {
            ("memory.graph.trunks.list", 1): self.list_trunks,
            ("memory.graph.sources.query", 1): self.query_sources,
            ("memory.graph.relations.query", 1): self.query_relations,
        }

    @staticmethod
    def _profile(value: object) -> FrozenMemoryProfile:
        if not isinstance(value, dict) or value.get("protocol") != "forest.memory.graph/1":
            raise ForestError("MEMORY_CONFIGURATION_INVALID", "Memory configuration is invalid", 500)
        embedding = value.get("embedding")
        if not isinstance(embedding, dict) or set(embedding) != {
            "deployment_id",
            "model_revision",
            "model",
            "dimensions",
        }:
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID", "Embedding space configuration is invalid", 500
            )
        try:
            profile = EmbeddingProfile(**embedding)
        except (TypeError, ValueError, ForestError) as exc:
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID", "Embedding profile is invalid", 500
            ) from exc
        endpoint = value.get("endpoint")
        credential_ref = value.get("credential_ref")
        fusion = value.get("fusion_policy_revision")
        if any(not isinstance(item, str) or not item for item in (endpoint, credential_ref, fusion)):
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID", "Memory endpoint configuration is invalid", 500
            )
        return FrozenMemoryProfile(endpoint, credential_ref, profile, fusion)

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        profile = self._profile(desired.value)
        await self.repository.initialize_domain(
            operation_id="bootstrap:memory.graph:domain",
            space_id=profile.embedding.space_id,
            embedding_model_kind=profile.embedding.model,
            model_revision_digest=digest_json(profile.embedding.as_dict()),
            dimensions=profile.embedding.dimensions,
            normalization="L2",
            fusion_policy_revision=profile.fusion_policy_revision,
        )
        self.profile = profile
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
            instance_id=f"instance:memory.graph:{context.activation_id}",
            worker_incarnation=f"worker:memory.graph:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "memory.graph":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Memory received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Memory Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None or self.profile is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Memory Generation was not prepared and bound",
                503,
            )

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False

    async def close(self) -> None:
        self.active = False

    async def _admit(self) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Memory Generation is not accepting calls",
                409,
            )
        await self.generation_verifier(
            component_id="memory.graph",
            generation_id=self.generation_identity.generation_id,
            admission_lane="DATA",
        )

    async def list_trunks(self):
        await self._admit()
        return await self.repository.list_trunks()

    async def query_sources(self, **filters):
        await self._admit()
        return await self.repository.query_sources(**filters)

    async def query_relations(self, **filters):
        await self._admit()
        return await self.repository.query_relations(**filters)

    async def _admit_in(self, conn, *, owned_work: bool) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Memory Generation is not accepting transaction work",
                409,
            )
        if owned_work:
            await self.generation_member_verifier(
                conn,
                component_id="memory.graph",
                generation_id=self.generation_identity.generation_id,
            )
        else:
            await self.generation_admission_verifier(
                conn,
                component_id="memory.graph",
                generation_id=self.generation_identity.generation_id,
                admission_lane="DATA",
            )

    async def qualify_input_batch_in(self, conn, **values):
        await self._admit_in(conn, owned_work=True)
        return await self.repository.qualify_input_batch_in(conn, **values)

    async def register_source_in(self, conn, **values):
        await self._admit_in(conn, owned_work=False)
        return await self.repository.register_source_in(conn, **values)

    async def capture_source_unit_in(self, conn, **values):
        await self._admit_in(conn, owned_work=False)
        return await self.repository.capture_source_unit_in(conn, **values)

    async def deletion_epoch_in(self, conn) -> int:
        await self._admit_in(conn, owned_work=True)
        if self.repository.deletion_epoch_guard is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING",
                "Memory deletion epoch authority is unavailable",
                503,
            )
        return await self.repository.deletion_epoch_guard(conn)
