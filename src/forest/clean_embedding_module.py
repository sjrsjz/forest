"""Install the Bot-lifetime multimodal embedding Provider."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_embedding_runtime import CleanMultimodalEmbeddingRuntime
from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.embeddings import SILICONFLOW_VL_MODEL
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.embedding_provider_authority import EmbeddingProviderRepository
from forest.storage.module_configuration import ModuleConfigurationRepository

EMBEDDING_CONFIGURATION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": [
        "protocol",
        "endpoint",
        "credential_ref",
        "timeout_seconds",
        "max_connections",
        "profile",
    ],
    "properties": {
        "protocol": {"const": "forest.embedding.multimodal/1"},
        "endpoint": {"type": "string", "minLength": 1, "maxLength": 2048},
        "credential_ref": {"type": "string", "minLength": 1, "maxLength": 512},
        "timeout_seconds": {"type": "number", "minimum": 1, "maximum": 300},
        "max_connections": {"type": "integer", "minimum": 1, "maximum": 32},
        "profile": {"type": "object"},
    },
    "additionalProperties": False,
}


def default_embedding_configuration() -> dict:
    return {
        "protocol": "forest.embedding.multimodal/1",
        "endpoint": "https://api.siliconflow.cn/v1/embeddings",
        "credential_ref": "secret://embedding.multimodal/default",
        "timeout_seconds": 60,
        "max_connections": 4,
        "profile": {
            "deployment_id": "embedding.multimodal/qiyu-vl/default",
            "model_revision": "operator-declared/1",
            "model": SILICONFLOW_VL_MODEL,
            "dimensions": 4096,
        },
    }


@dataclass(frozen=True)
class CleanEmbeddingModule:
    repository: EmbeddingProviderRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_embedding(
    store,
    installer,
    *,
    kernel,
    credential_resolver,
    initial_configuration: dict | None = None,
    client_factory=None,
) -> CleanEmbeddingModule:
    storage = official_storage_bundle(
        "embedding.multimodal",
        "forest.embedding.multimodal",
        "forest.embedding-multimodal.repository/1",
        "storage/official/embedding_multimodal.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="embedding.multimodal",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    try:
        await configuration.desired()
    except ForestError as exc:
        if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        await configuration.initialize(
            operation_id="bootstrap:embedding.multimodal:configuration",
            value=initial_configuration or default_embedding_configuration(),
            validation_digest=digest_json(EMBEDDING_CONFIGURATION_SCHEMA),
        )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="embedding.multimodal",
        entries=(CapabilityGrant("embedding.dispatch", "bot", store.bot_id),),
        expected_policy_revision=policy_revision,
    )
    repository = EmbeddingProviderRepository(store, namespace=storage.spec.namespace)

    async def validate_configuration(value):
        _configuration, profile = CleanMultimodalEmbeddingRuntime._parse_configuration(value)
        await repository.initialize_identity(
            operation_id="management:embedding.multimodal:validate-profile",
            profile=profile,
        )

    def factory(_runtime, _contracts):
        values = {
            "store": store,
            "repository": repository,
            "payloads": kernel.payload,
            "module_configuration": configuration,
            "grants": grants,
            "credential_resolver": credential_resolver,
            "generation_member_verifier": kernel.runtime.verify_generation_member_in,
        }
        if client_factory is not None:
            values["client_factory"] = client_factory
        runtime = CleanMultimodalEmbeddingRuntime(**values)
        runtime.management_contract = CleanModuleConfigurationContract(
            configuration,
            schema=EMBEDDING_CONFIGURATION_SCHEMA,
            validator=validate_configuration,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "embedding.multimodal",
        ("kernel.runtime", "kernel.payload", "kernel.authorization", "kernel.effect"),
        factory,
        official_artifact("bot"),
        management=(configuration_management_contribution("多模态嵌入"),),
        storage=storage,
    )
    return CleanEmbeddingModule(repository, descriptor, configuration, grants)
