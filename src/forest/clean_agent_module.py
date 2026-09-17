"""Install and describe the clean Agent runtime module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_agent_runtime import CleanAgentRuntime
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.agent_runtime_authority import AgentRuntimeRepository
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository


@dataclass(frozen=True)
class CleanAgentModule:
    repository: AgentRuntimeRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_agent(store, installer, *, kernel) -> CleanAgentModule:
    storage = official_storage_bundle(
        "agent.runtime",
        "forest.agent.runtime",
        "forest.agent-runtime.repository/1",
        "storage/official/agent_runtime.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="agent.runtime",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"protocol": {"const": "forest.agent.runtime/1"}},
        "required": ["protocol"],
        "additionalProperties": False,
    }
    await configuration.initialize(
        operation_id="bootstrap:agent.runtime:configuration",
        value={"protocol": "forest.agent.runtime/1"},
        validation_digest=digest_json(schema),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="agent.runtime",
        entries=(
            CapabilityGrant("agent.run", "bot", store.bot_id),
            CapabilityGrant("provider.invoke", "bot", store.bot_id),
            CapabilityGrant("tool.invoke", "bot", store.bot_id),
        ),
        expected_policy_revision=policy_revision,
    )
    repository = AgentRuntimeRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=kernel.payload.verify_in,
    )

    def factory(_runtime, _modules):
        return CleanAgentRuntime(
            repository=repository,
            module_configuration=configuration,
            grants=grants,
            generation_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
        )

    descriptor = RuntimeModuleDescriptor(
        "agent.runtime",
        ("kernel.runtime", "kernel.payload", "kernel.authorization", "kernel.effect"),
        factory,
        official_artifact("bot"),
        storage=storage,
    )
    return CleanAgentModule(repository, descriptor, configuration, grants)
