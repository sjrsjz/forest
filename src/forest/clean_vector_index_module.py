"""Install the replaceable Milvus vector-index component."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.clean_vector_index_runtime import CleanMilvusVectorIndexRuntime
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.vector_index_authority import VectorIndexIdentityRepository

VECTOR_INDEX_CONFIGURATION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": [
        "protocol", "endpoint", "credential_ref", "timeout_seconds",
        "space_id", "dimensions", "metric",
    ],
    "properties": {
        "protocol": {"const": "forest.vector-index.milvus/1"},
        "endpoint": {"type": "string", "minLength": 1, "maxLength": 2048},
        "credential_ref": {"type": "string", "maxLength": 512},
        "timeout_seconds": {"type": "number", "minimum": 1, "maximum": 300},
        "space_id": {"type": "string", "minLength": 1, "maxLength": 256},
        "dimensions": {"type": "integer", "minimum": 1, "maximum": 4096},
        "metric": {"const": "COSINE"},
    },
    "additionalProperties": False,
}


def default_vector_index_configuration(*, space_id: str, dimensions: int) -> dict:
    return {
        "protocol": "forest.vector-index.milvus/1",
        "endpoint": "http://milvus:19530",
        "credential_ref": "",
        "timeout_seconds": 10,
        "space_id": space_id,
        "dimensions": dimensions,
        "metric": "COSINE",
    }


@dataclass(frozen=True)
class CleanVectorIndexModule:
    repository: VectorIndexIdentityRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_vector_index(
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict,
    token_resolver,
    client_factory=None,
) -> CleanVectorIndexModule:
    storage = official_storage_bundle(
        "vector.index",
        "forest.vector.index",
        "forest.vector-index-milvus.repository/1",
        "storage/official/vector_index_milvus.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="vector.index",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    try:
        await configuration.desired()
    except ForestError as exc:
        if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        await configuration.initialize(
            operation_id="bootstrap:vector.index:configuration",
            value=initial_configuration,
            validation_digest=digest_json(VECTOR_INDEX_CONFIGURATION_SCHEMA),
        )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="vector.index",
        entries=(CapabilityGrant("vector.index.derived", "bot", store.bot_id),),
        expected_policy_revision=policy_revision,
    )
    repository = VectorIndexIdentityRepository(store, namespace=storage.spec.namespace)

    async def validate_configuration(value):
        parsed = CleanMilvusVectorIndexRuntime._parse_configuration(value)
        await repository.initialize(
            operation_id="management:vector.index:validate-identity",
            space_id=parsed["space_id"],
            dimensions=parsed["dimensions"],
            metric=parsed["metric"],
        )

    def factory(_runtime, _contracts):
        values = {
            "repository": repository,
            "module_configuration": configuration,
            "grants": grants,
            "generation_verifier": kernel.runtime.verify_active_generation,
            "token_resolver": token_resolver,
        }
        if client_factory is not None:
            values["client_factory"] = client_factory
        runtime = CleanMilvusVectorIndexRuntime(**values)
        runtime.management_contract = CleanModuleConfigurationContract(
            configuration,
            schema=VECTOR_INDEX_CONFIGURATION_SCHEMA,
            validator=validate_configuration,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "vector.index",
        ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        factory,
        official_artifact("bot"),
        management=(configuration_management_contribution("Milvus 向量索引"),),
        storage=storage,
    )
    return CleanVectorIndexModule(repository, descriptor, configuration, grants)
