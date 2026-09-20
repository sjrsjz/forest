"""Install and describe the official conversation trigger policy module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.conversation_trigger_policy import ConversationTriggerPolicyRuntime
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository


def default_trigger_policy_configuration() -> dict:
    return {
        "protocol": "forest.conversation.trigger-policy/1",
        "allowed_group_ids": [],
        "group_trigger_mode": "MENTION",
        "private_enabled": True,
    }


@dataclass(frozen=True)
class CleanTriggerPolicyModule:
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_trigger_policy(
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict | None = None,
) -> CleanTriggerPolicyModule:
    storage = official_storage_bundle(
        "conversation.trigger-policy",
        "forest.conversation.trigger-policy",
        "forest.conversation-trigger-policy.repository/1",
        "storage/official/conversation_trigger_policy.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="conversation.trigger-policy",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "protocol",
            "allowed_group_ids",
            "group_trigger_mode",
            "private_enabled",
        ],
        "properties": {
            "protocol": {"const": "forest.conversation.trigger-policy/1"},
            "allowed_group_ids": {
                "type": "array",
                "items": {"type": "string", "pattern": "^[0-9]+$"},
                "uniqueItems": True,
            },
            "group_trigger_mode": {"enum": ["MENTION", "ALL", "DISABLED"]},
            "private_enabled": {"type": "boolean"},
        },
        "additionalProperties": False,
    }
    await configuration.ensure_initialized(
        operation_id="bootstrap:conversation.trigger-policy:configuration",
        value=initial_configuration or default_trigger_policy_configuration(),
        validation_digest=digest_json(schema),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="conversation.trigger-policy",
        entries=(CapabilityGrant("conversation.classify", "bot", store.bot_id),),
        expected_policy_revision=policy_revision,
    )

    def factory(_runtime, _contracts):
        runtime = ConversationTriggerPolicyRuntime(
            module_configuration=configuration,
            grants=grants,
        )
        runtime.management_contract = CleanModuleConfigurationContract(
            configuration,
            schema=schema,
            validator=ConversationTriggerPolicyRuntime._parse_configuration,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "conversation.trigger-policy",
        ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        factory,
        official_artifact("bot"),
        management=(configuration_management_contribution("群聊触发策略"),),
        storage=storage,
        factory_protocol="forest.contract-module-factory/1",
    )
    return CleanTriggerPolicyModule(descriptor, configuration, grants)
