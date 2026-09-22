"""Install and describe the Bot-global multimodal Memory graph module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.builtin_memory_shell import MEMORY_SHELL_COMMANDS
from forest.builtin_memory_tool import MEMORY_EXACT_RECALL_TOOL
from forest.clean_management_contract import CleanMemoryManagementContract
from forest.clean_memory_runtime import CleanMemoryRuntime
from forest.embedding_effect_execution import KernelEmbeddingEffectExecutionPort
from forest.embeddings import (
    SILICONFLOW_VL_MODEL,
    EmbeddingProfile,
    prepare_embedding_request,
)
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.memory_graph_authority import MemoryGraphRepository
from forest.storage.memory_retrieval_authority import MemoryRetrievalRepository
from forest.storage.module_configuration import ModuleConfigurationRepository


def default_memory_configuration() -> dict:
    return {
        "protocol": "forest.memory.graph/1",
        "embedding": {
            "deployment_id": "embedding.multimodal/qiyu-vl/default",
            "model_revision": "operator-declared/1",
            "model": SILICONFLOW_VL_MODEL,
            "dimensions": 4096,
        },
        "fusion_policy_revision": "independent-facets/1",
        "retrieval": {
            "policy_revision": "qiyu-graph-region/1",
            "seed_limit": 4,
            "candidate_limit_per_facet": 64,
            "min_similarity": 0.35,
            "min_graph_relevance": 0.05,
            "max_graph_nodes": 32,
            "max_association_edges_per_node": 32,
            "max_retrieval_chars": 24000,
            "sequence_weight": 0.55,
            "association_weight": 0.90,
        },
        "embedding_budget": {
            "policy_revision": "daily-cost-cap/1",
            "daily_request_limit": 1000,
            "daily_token_limit": 10000000,
            "reservation_tokens": 32768,
        },
    }


class _RoutedEmbeddingProvider:
    def __init__(self, bind, effects) -> None:
        self._bind = bind
        self._effects = effects

    @property
    def route(self):
        return self._bind.route

    async def encode(
        self,
        inputs,
        *,
        request_id,
        transport_attempt_id,
        purpose,
        expected_profile,
        producer_generation_id,
        permits,
        handoff_operation_id,
    ):
        profile_value = await self._bind.invoke(
            {"protocol": "forest.embedding-bind-request/1", "request_id": request_id},
            operation_id="embedding-bind:" + digest_json(request_id)[7:],
            causal_root_kind="memory.embedding",
            causal_root_id=request_id,
        )
        route = self._bind.route
        try:
            profile = EmbeddingProfile(**profile_value["profile"])
        except (KeyError, TypeError, ValueError, ForestError) as exc:
            raise ForestError(
                "EMBEDDING_PROFILE_INVALID", "Embedding profile response is invalid", 502
            ) from exc
        if (
            profile != expected_profile
            or profile.space_id != profile_value["space_id"]
            or profile_value["component_id"] != route.provider_component_id
            or profile_value["generation_id"] != route.provider_generation_id
        ):
            raise ForestError(
                "EMBEDDING_ROUTE_MISMATCH",
                "Memory and Embedding Provider are bound to different spaces",
                409,
            )
        prepared = prepare_embedding_request(
            profile,
            tuple(inputs),
            request_id=request_id,
            transport_attempt_id=transport_attempt_id,
            purpose=purpose,
        )
        return await self._effects.dispatch(
            prepared,
            inputs=tuple(inputs),
            provider_component_id=route.provider_component_id,
            provider_generation_id=route.provider_generation_id,
            profile_ref=profile_value["profile_ref"],
            producer_generation_id=producer_generation_id,
            permits=permits,
            handoff_operation_id=handoff_operation_id,
        )


class _RoutedVectorIndex:
    def __init__(self, *, upsert, search, delete) -> None:
        self._upsert = upsert
        self._search = search
        self._delete = delete

    @staticmethod
    def _operation(kind: str, payload: dict) -> str:
        return f"vector-index:{kind}:" + digest_json(payload)[7:]

    async def upsert(self, **payload):
        payload = {**payload, "vector": list(payload["vector"])}
        return await self._upsert.invoke(
            payload,
            operation_id=self._operation("upsert", payload),
            causal_root_kind="memory.embedding",
            causal_root_id=payload["ref_id"],
        )

    async def search(self, **payload):
        value = await self._search.invoke(
            payload,
            operation_id=self._operation("search", payload),
            causal_root_kind="memory.recall",
            causal_root_id=payload["query_ref_id"],
        )
        return tuple(value["hits"])

    async def delete(self, **payload):
        normalized = {**payload, "ref_ids": list(payload["ref_ids"])}
        operation_id = self._operation("delete", normalized)
        await self._delete.invoke(
            normalized,
            operation_id=operation_id,
            causal_root_kind="memory.forget",
            causal_root_id=operation_id,
        )


@dataclass(frozen=True)
class CleanMemoryModule:
    repository: MemoryGraphRepository
    retrieval: MemoryRetrievalRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_memory(
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict | None = None,
) -> CleanMemoryModule:
    storage = official_storage_bundle(
        "memory.graph",
        "forest.memory.graph",
        "forest.memory-graph.repository/1",
        "storage/official/memory_graph.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="memory.graph",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "protocol",
            "embedding",
            "fusion_policy_revision",
            "retrieval",
            "embedding_budget",
        ],
        "properties": {
            "protocol": {"const": "forest.memory.graph/1"},
            "embedding": {"type": "object"},
            "fusion_policy_revision": {"type": "string", "minLength": 1},
            "retrieval": {"type": "object"},
            "embedding_budget": {"type": "object"},
        },
        "additionalProperties": False,
    }
    try:
        await configuration.desired()
    except ForestError as exc:
        if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        await configuration.initialize(
            operation_id="bootstrap:memory.graph:configuration",
            value=initial_configuration or default_memory_configuration(),
            validation_digest=digest_json(schema),
        )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="memory.graph",
        entries=tuple(
            CapabilityGrant(
                capability,
                "bot",
                store.bot_id,
                delegable=capability == "embedding.invoke",
            )
            for capability in (
                "memory.read",
                "memory.write",
                "memory.embed",
                "memory.forget",
                "embedding.invoke",
            )
        ),
        expected_policy_revision=policy_revision,
    )
    repository = MemoryGraphRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=kernel.payload.verify_in,
        deletion_epoch_guard=kernel.payload.epoch_in,
    )
    retrieval = MemoryRetrievalRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=kernel.payload.verify_in,
        payload_reader=kernel.payload.read_bytes_in,
        deletion_epoch_guard=kernel.payload.epoch_in,
    )

    async def validate_configuration(value):
        try:
            profile = CleanMemoryRuntime._profile(value)
        except ForestError as exc:
            if exc.code != "MEMORY_CONFIGURATION_INVALID":
                raise
            raise ForestError(exc.code, exc.message, 422, details=exc.details) from None
        await repository.initialize_domain(
            operation_id="management:memory.graph:validate-domain",
            space_id=profile.embedding.space_id,
            embedding_model_kind=profile.embedding.model,
            model_revision_digest=digest_json(profile.embedding.as_dict()),
            dimensions=profile.embedding.dimensions,
            normalization="L2",
            fusion_policy_revision=profile.fusion_policy_revision,
        )

    def factory(_runtime, contracts):
        embedding_bind = contracts.service(
            "embedding.profile.bind", 1, owned_work=True
        )
        embedding_dispatch = contracts.service(
            "embedding.encode", 1, owned_work=True
        )
        embedding_effects = KernelEmbeddingEffectExecutionPort(
            retrieval=retrieval,
            effects=kernel.effect,
            payloads=kernel.payload,
            runtime=kernel.runtime,
            authorization=kernel.authorization,
            memory_grants=grants,
            service=embedding_dispatch,
        )
        runtime = CleanMemoryRuntime(
            repository=repository,
            retrieval=retrieval,
            payloads=kernel.payload,
            module_configuration=configuration,
            grants=grants,
            generation_verifier=kernel.runtime.verify_active_generation,
            generation_admission_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
            embedding_provider=_RoutedEmbeddingProvider(
                embedding_bind,
                embedding_effects,
            ),
            vector_index=_RoutedVectorIndex(
                upsert=contracts.service("vector.index.upsert", 1, owned_work=True),
                search=contracts.service("vector.index.search", 1, owned_work=True),
                delete=contracts.service("vector.index.delete", 1, owned_work=True),
            ),
        )
        runtime.management_contract = CleanMemoryManagementContract(
            configuration,
            runtime=runtime,
            schema=schema,
            validator=validate_configuration,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "memory.graph",
        (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "embedding.multimodal",
            "vector.index",
        ),
        factory,
        official_artifact("bot"),
        management=(
            RuntimeManagementContribution(
                "memory-explorer",
                "来源与关系",
                ("bot",),
                "/static/modules/clean-memory.html",
                "/static/clean-memory.js",
                style="/static/modules/clean-memory.css",
                group="data",
                order=5,
            ),
            configuration_management_contribution("记忆配置"),
        ),
        storage=storage,
        wakes_on_work=True,
        work_notify_order=20,
        agent_tools=(MEMORY_EXACT_RECALL_TOOL,),
        shell_commands=MEMORY_SHELL_COMMANDS,
    )
    return CleanMemoryModule(repository, retrieval, descriptor, configuration, grants)
