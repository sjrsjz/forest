"""Install and describe the Bot-global multimodal Memory graph module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_memory_runtime import CleanMemoryRuntime
from forest.embeddings import SILICONFLOW_VL_MODEL
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.memory_graph_authority import MemoryGraphRepository
from forest.storage.module_configuration import ModuleConfigurationRepository


def default_memory_configuration() -> dict:
    return {
        "protocol": "forest.memory.graph/1",
        "endpoint": "https://api.siliconflow.cn/v1",
        "credential_ref": "secret://memory.embedding/default",
        "embedding": {
            "deployment_id": "memory.embedding/qiyu-vl/default",
            "model_revision": "operator-declared/1",
            "model": SILICONFLOW_VL_MODEL,
            "dimensions": 4096,
        },
        "fusion_policy_revision": "independent-facets/1",
    }


@dataclass(frozen=True)
class CleanMemoryModule:
    repository: MemoryGraphRepository
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
            "endpoint",
            "credential_ref",
            "embedding",
            "fusion_policy_revision",
        ],
        "additionalProperties": False,
    }
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
            CapabilityGrant(capability, "bot", store.bot_id)
            for capability in (
                "memory.read",
                "memory.write",
                "memory.embed",
                "memory.forget",
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

    def factory(_runtime, _contracts):
        return CleanMemoryRuntime(
            repository=repository,
            module_configuration=configuration,
            grants=grants,
            generation_verifier=kernel.runtime.verify_active_generation,
            generation_admission_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
        )

    descriptor = RuntimeModuleDescriptor(
        "memory.graph",
        ("kernel.runtime", "kernel.payload", "kernel.authorization", "kernel.effect"),
        factory,
        official_artifact("bot"),
        storage=storage,
        factory_protocol="forest.contract-module-factory/1",
    )
    return CleanMemoryModule(repository, descriptor, configuration, grants)
