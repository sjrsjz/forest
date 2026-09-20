"""Install and describe the official Chat Completions Provider module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.clean_provider_runtime import CleanChatCompletionsProviderRuntime
from forest.models import ModelCapabilities
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository


def default_provider_configuration() -> dict:
    return {
        "protocol": "forest.provider.chat-completions/1",
        "base_url": "http://127.0.0.1:8000/v1",
        "credential_ref": "secret://provider.chat-completions/default",
        "timeout_seconds": 120,
        "max_connections": 8,
        "capabilities": ModelCapabilities(
            deployment_id="provider.chat-completions/default",
            model="unconfigured",
            capability_revision="configuration/1",
        ).as_dict(),
    }


@dataclass(frozen=True)
class CleanProviderModule:
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_provider(
    store,
    installer,
    *,
    kernel,
    credential_resolver,
    initial_configuration: dict | None = None,
    client_factory=None,
) -> CleanProviderModule:
    storage = official_storage_bundle(
        "provider.chat-completions",
        "forest.provider.chat-completions",
        "forest.provider-chat-completions.repository/1",
        "storage/official/provider_chat_completions.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="provider.chat-completions",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "protocol",
            "base_url",
            "credential_ref",
            "timeout_seconds",
            "max_connections",
            "capabilities",
        ],
        "properties": {
            "protocol": {"const": "forest.provider.chat-completions/1"},
            "base_url": {"type": "string", "minLength": 1, "maxLength": 2048},
            "credential_ref": {"type": "string", "minLength": 1, "maxLength": 512},
            "timeout_seconds": {"type": "number", "minimum": 1, "maximum": 600},
            "max_connections": {"type": "integer", "minimum": 1, "maximum": 128},
            "capabilities": {"type": "object"},
        },
        "additionalProperties": False,
    }
    await configuration.ensure_initialized(
        operation_id="bootstrap:provider.chat-completions:configuration",
        value=initial_configuration or default_provider_configuration(),
        validation_digest=digest_json(schema),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="provider.chat-completions",
        entries=(CapabilityGrant("provider.dispatch", "bot", store.bot_id),),
        expected_policy_revision=policy_revision,
    )

    def factory(_runtime, _modules):
        values = {
            "store": store,
            "module_configuration": configuration,
            "grants": grants,
            "credential_resolver": credential_resolver,
            "generation_verifier": kernel.runtime.verify_active_generation_in,
            "generation_member_verifier": kernel.runtime.verify_generation_member_in,
        }
        if client_factory is not None:
            values["client_factory"] = client_factory
        runtime = CleanChatCompletionsProviderRuntime(**values)
        runtime.management_contract = CleanModuleConfigurationContract(
            configuration,
            schema=schema,
            validator=CleanChatCompletionsProviderRuntime._parse_configuration,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "provider.chat-completions",
        ("kernel.runtime", "kernel.payload", "kernel.authorization", "kernel.effect"),
        factory,
        official_artifact("bot"),
        management=(configuration_management_contribution("模型 Provider"),),
        storage=storage,
        factory_protocol="forest.contract-module-factory/1",
    )
    return CleanProviderModule(descriptor, configuration, grants)
