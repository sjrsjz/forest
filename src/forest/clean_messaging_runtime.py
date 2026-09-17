"""Install and describe clean messaging modules without constructing a Bot monolith."""

from __future__ import annotations

from dataclasses import dataclass

from forest.adapter_onebot_runtime import CleanOneBotRuntime
from forest.conversation_runtime import CleanConversationSchedulerRuntime
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.adapter_onebot_authority import OneBotAdapterRepository
from forest.storage.authorization_authority import CapabilityGrant, GrantSetRef
from forest.storage.conversation_scheduler import ConversationSchedulerRepository
from forest.storage.ingress_unit_of_work import OneBotIngressUnitOfWork
from forest.storage.module_configuration import ModuleConfigurationRepository

_DEFINITIONS = {
    "conversation.scheduler": {
        "schema_id": "forest.conversation.scheduler",
        "repository_contract": "forest.conversation-scheduler.repository/1",
        "resource": "storage/official/conversation_scheduler.sql",
        "requires": ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        "capabilities": ("conversation.admit", "conversation.classify"),
    },
    "adapter.onebot": {
        "schema_id": "forest.adapter.onebot",
        "repository_contract": "forest.adapter-onebot.repository/1",
        "resource": "storage/official/adapter_onebot.sql",
        "requires": (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "conversation.scheduler",
        ),
        "capabilities": ("adapter.onebot.ingress", "adapter.onebot.send"),
    },
}


@dataclass(frozen=True)
class CleanMessagingModules:
    adapter: OneBotAdapterRepository
    conversation: ConversationSchedulerRepository
    descriptors: tuple[RuntimeModuleDescriptor, ...]
    configurations: dict[str, ModuleConfigurationRepository]
    grants: dict[str, GrantSetRef]


async def bootstrap_clean_messaging(
    store,
    installer,
    *,
    kernel,
    secret_resolver,
    http_client_factory=None,
    websocket_client_factory=None,
) -> CleanMessagingModules:
    """Create module-owned authorities and descriptor factories for one Bot."""

    bundles = {
        component_id: official_storage_bundle(
            component_id,
            definition["schema_id"],
            definition["repository_contract"],
            definition["resource"],
        )
        for component_id, definition in _DEFINITIONS.items()
    }
    for component_id in sorted(bundles):
        bundle = bundles[component_id]
        await installer.install(
            bundle.spec,
            bundle.baseline,
            bundle.package_digest,
            execution_profile=bundle.execution_profile,
        )

    configurations = {
        component_id: ModuleConfigurationRepository(
            store,
            component_id=component_id,
            namespace=bundle.spec.namespace,
            payloads=kernel.payload,
        )
        for component_id, bundle in bundles.items()
    }
    for component_id, configuration in configurations.items():
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"protocol": {"type": "string"}},
            "required": ["protocol"],
            "additionalProperties": False,
        }
        await configuration.initialize(
            operation_id=f"bootstrap:{component_id}:configuration",
            value={"protocol": f"forest.{component_id}/1"},
            validation_digest=digest_json(schema),
        )

    policy_revision = await kernel.authorization.revision()
    grants = {}
    for component_id, definition in _DEFINITIONS.items():
        grants[component_id] = await kernel.authorization.create_grant_set(
            grantee_kind="component",
            grantee_id=component_id,
            entries=tuple(
                CapabilityGrant(capability, "bot", store.bot_id)
                for capability in definition["capabilities"]
            ),
            expected_policy_revision=policy_revision,
        )

    conversation_repository = ConversationSchedulerRepository(
        store,
        namespace=bundles["conversation.scheduler"].spec.namespace,
        payload_verifier=kernel.payload.verify_in,
    )
    adapter_repository = OneBotAdapterRepository(
        store,
        namespace=bundles["adapter.onebot"].spec.namespace,
        payload_verifier=kernel.payload.verify_in,
        generation_verifier=kernel.runtime.verify_active_generation_in,
    )

    def conversation_factory(_runtime, _modules):
        return CleanConversationSchedulerRuntime(
            repository=conversation_repository,
            module_configuration=configurations["conversation.scheduler"],
            grants=grants["conversation.scheduler"],
            generation_verifier=kernel.runtime.verify_active_generation_in,
        )

    def adapter_factory(_runtime, modules):
        values = {
            "repository": adapter_repository,
            "module_configuration": configurations["adapter.onebot"],
            "grants": grants["adapter.onebot"],
            "payloads": kernel.payload,
            "secret_resolver": secret_resolver,
            "ingress_port": OneBotIngressUnitOfWork(
                store,
                adapter=adapter_repository,
                conversation=modules["conversation.scheduler"],
            ),
        }
        if http_client_factory is not None:
            values["http_client_factory"] = http_client_factory
        if websocket_client_factory is not None:
            values["websocket_client_factory"] = websocket_client_factory
        return CleanOneBotRuntime(**values)

    artifact = official_artifact("bot")
    factories = {
        "conversation.scheduler": conversation_factory,
        "adapter.onebot": adapter_factory,
    }
    descriptors = tuple(
        RuntimeModuleDescriptor(
            component_id,
            definition["requires"],
            factories[component_id],
            artifact,
            storage=bundles[component_id],
        )
        for component_id, definition in _DEFINITIONS.items()
    )
    return CleanMessagingModules(
        adapter_repository,
        conversation_repository,
        descriptors,
        configurations,
        grants,
    )
