"""Official Bot bootstrap profile projection and no-I/O authority materialization."""

from __future__ import annotations

from urllib.parse import urlsplit

from jsonschema import Draft202012Validator

from forest.bootstrap_contributions import BootstrapConfigurationContribution
from forest.clean_bot_host import CleanBotHost, CleanBotSettings
from forest.clean_embedding_module import (
    EMBEDDING_CONFIGURATION_SCHEMA,
    default_embedding_configuration,
)
from forest.clean_embedding_runtime import CleanMultimodalEmbeddingRuntime
from forest.clean_provider_module import (
    PROVIDER_CONFIGURATION_SCHEMA,
    default_provider_configuration,
)
from forest.clean_provider_runtime import CleanChatCompletionsProviderRuntime
from forest.clean_shell_module import SHELL_CONFIGURATION_SCHEMA, default_shell_configuration
from forest.clean_shell_runtime import CleanShellRuntime
from forest.clean_trigger_policy_module import (
    TRIGGER_POLICY_CONFIGURATION_SCHEMA,
    default_trigger_policy_configuration,
)
from forest.clean_vector_index_module import (
    VECTOR_INDEX_CONFIGURATION_SCHEMA,
    default_vector_index_configuration,
)
from forest.clean_vector_index_runtime import CleanMilvusVectorIndexRuntime
from forest.conversation_trigger_policy import ConversationTriggerPolicyRuntime
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.runtime_modules import RuntimeManagementContribution

AGENT_BOOTSTRAP_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.agent.runtime.bootstrap/1"},
        "preset_system": {
            "type": "array",
            "items": {"type": "string", "maxLength": 65536},
            "maxItems": 32,
        },
    },
    "required": ["protocol", "preset_system"],
    "additionalProperties": False,
}

ONEBOT_BOOTSTRAP_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.adapter.onebot.bootstrap/1"},
        "binding_id": {"const": "binding/main"},
        "self_id": {"type": "string", "pattern": "^[0-9]+$", "maxLength": 32},
        "websocket_url": {"type": "string", "minLength": 1, "maxLength": 2048},
        "access_token_ref": {"type": "string", "minLength": 10, "maxLength": 512},
    },
    "required": [
        "protocol",
        "binding_id",
        "self_id",
        "websocket_url",
        "access_token_ref",
    ],
    "additionalProperties": False,
}

OFFICIAL_BOOTSTRAP_SCHEMAS = {
    "provider.chat-completions": PROVIDER_CONFIGURATION_SCHEMA,
    "embedding.multimodal": EMBEDDING_CONFIGURATION_SCHEMA,
    "vector.index": VECTOR_INDEX_CONFIGURATION_SCHEMA,
    "conversation.trigger-policy": TRIGGER_POLICY_CONFIGURATION_SCHEMA,
    "shell.command": SHELL_CONFIGURATION_SCHEMA,
    "agent.runtime": AGENT_BOOTSTRAP_SCHEMA,
    "adapter.onebot": ONEBOT_BOOTSTRAP_SCHEMA,
}


def _bootstrap_surface(component_id: str, label: str) -> RuntimeManagementContribution:
    return RuntimeManagementContribution(
        "bootstrap-" + component_id,
        label,
        ("deployment",),
        "/static/modules/bootstrap-component.html",
        "/static/bootstrap-component.js",
        style="/static/modules/bootstrap-component.css",
        group="bootstrap",
    )


def official_bootstrap_contributions() -> tuple[BootstrapConfigurationContribution, ...]:
    """Return the official install catalog without constructing a Bot runtime graph."""

    default_embedding = default_embedding_configuration()
    default_embedding_profile = EmbeddingProfile(**default_embedding["profile"])
    values = (
        (
            "provider.chat-completions",
            "Chat Completions Provider",
            "Model endpoint, credential reference, limits, and declared capabilities.",
            default_provider_configuration(),
            CleanChatCompletionsProviderRuntime._parse_configuration,
        ),
        (
            "embedding.multimodal",
            "Multimodal Embedding Provider",
            "Bot-lifetime text and image embedding identity and endpoint.",
            default_embedding,
            CleanMultimodalEmbeddingRuntime._parse_configuration,
        ),
        (
            "vector.index",
            "Milvus Vector Index",
            "Rebuildable ANN index endpoint bound to the Bot embedding space.",
            default_vector_index_configuration(
                space_id=default_embedding_profile.space_id,
                dimensions=default_embedding_profile.dimensions,
            ),
            CleanMilvusVectorIndexRuntime._parse_configuration,
        ),
        (
            "conversation.trigger-policy",
            "Conversation Trigger Policy",
            "Allowed groups and the conditions that create an Agent turn.",
            default_trigger_policy_configuration(),
            ConversationTriggerPolicyRuntime._parse_configuration,
        ),
        (
            "shell.command",
            "Group Shell",
            "Binding and administrator principals allowed to invoke control commands.",
            default_shell_configuration(),
            CleanShellRuntime._parse_profile,
        ),
        (
            "agent.runtime",
            "Agent Runtime",
            "Core system-prompt preset injected into every Agent run.",
            {
                "protocol": "forest.agent.runtime.bootstrap/1",
                "preset_system": [],
            },
            None,
        ),
        (
            "adapter.onebot",
            "OneBot Adapter",
            "Bot identity, WebSocket endpoint, binding, and access-token reference.",
            {
                "protocol": "forest.adapter.onebot.bootstrap/1",
                "binding_id": "binding/main",
                "self_id": "10000",
                "websocket_url": "ws://127.0.0.1:3001/",
                "access_token_ref": "secret://adapter.onebot/default",
            },
            None,
        ),
    )
    return tuple(
        BootstrapConfigurationContribution(
            component_id=component_id,
            label=label,
            description=description,
            schema=OFFICIAL_BOOTSTRAP_SCHEMAS[component_id],
            initial_value=initial_value,
            surface=_bootstrap_surface(component_id, label),
            order=index * 10,
            validator=validator,
        )
        for index, (component_id, label, description, initial_value, validator) in enumerate(
            values, 1
        )
    )


def official_bootstrap_schema_digests() -> dict[str, str]:
    return {item.component_id: item.schema_digest for item in official_bootstrap_contributions()}


def validate_official_bootstrap_components(components: object) -> dict[str, dict]:
    if not isinstance(components, dict) or set(components) != set(OFFICIAL_BOOTSTRAP_SCHEMAS):
        raise ForestError(
            "OFFICIAL_BOOTSTRAP_COMPONENT_SET_INVALID",
            "Official Bot bootstrap component set differs",
            422,
        )
    for component_id, schema in OFFICIAL_BOOTSTRAP_SCHEMAS.items():
        errors = tuple(Draft202012Validator(schema).iter_errors(components[component_id]))
        if errors:
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_CONFIGURATION_INVALID",
                "Bootstrap component configuration is invalid",
                422,
                details={"component_id": component_id},
            )
    return components


class OfficialBotSettingsResolver:
    """Resolve one frozen official profile into runtime settings without retaining plaintext."""

    def __init__(self, *, secrets) -> None:
        self.secrets = secrets

    async def resolve(self, *, profile, storage) -> CleanBotSettings:
        if (
            profile.bot_id != storage.bot_id
            or profile.storage_profile_ref != storage.profile_ref
            or profile.storage_generation != storage.storage_generation
        ):
            raise ForestError(
                "BOOTSTRAP_PROFILE_SCOPE_MISMATCH",
                "Bootstrap profile differs from resolved Bot storage",
                409,
            )
        components = {item.component_id: item for item in profile.components}
        validate_official_bootstrap_components(
            {component_id: item.config for component_id, item in components.items()}
        )
        expected_digests = official_bootstrap_schema_digests()
        for component_id, item in components.items():
            if item.schema_digest != expected_digests[component_id]:
                raise ForestError(
                    "OFFICIAL_BOOTSTRAP_SCHEMA_MISMATCH",
                    "Bootstrap component schema differs from the installed module",
                    409,
                    details={"component_id": component_id},
                )

        provider, capabilities = CleanChatCompletionsProviderRuntime._parse_configuration(
            components["provider.chat-completions"].config
        )
        embedding, embedding_profile = CleanMultimodalEmbeddingRuntime._parse_configuration(
            components["embedding.multimodal"].config
        )
        vector_index = CleanMilvusVectorIndexRuntime._parse_configuration(
            components["vector.index"].config
        )
        if (
            vector_index["space_id"] != embedding_profile.space_id
            or vector_index["dimensions"] != embedding_profile.dimensions
        ):
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_VECTOR_SPACE_MISMATCH",
                "Vector index and embedding spaces differ",
                409,
            )
        trigger = ConversationTriggerPolicyRuntime._parse_configuration(
            components["conversation.trigger-policy"].config
        )
        shell = CleanShellRuntime._parse_profile(components["shell.command"].config)
        agent = components["agent.runtime"].config
        onebot = components["adapter.onebot"].config
        if shell.binding_ref != onebot["binding_id"]:
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_BINDING_MISMATCH",
                "Shell and OneBot binding references differ",
                409,
            )
        self._origin(provider["base_url"], {"http", "https"}, "model base URL")
        self._origin(embedding["endpoint"], {"http", "https"}, "embedding endpoint")
        self._origin(vector_index["endpoint"], {"http", "https"}, "vector index endpoint")
        self._origin(onebot["websocket_url"], {"ws", "wss"}, "OneBot WebSocket URL")
        model_key = await self._secret(
            provider["credential_ref"], "provider.chat-completions.credential"
        )
        embedding_key = await self._secret(
            embedding["credential_ref"], "embedding.multimodal.credential"
        )
        vector_index_token = (
            await self._secret(vector_index["credential_ref"], "vector.index.credential")
            if vector_index["credential_ref"]
            else ""
        )
        onebot_token = await self._secret(
            onebot["access_token_ref"], "adapter.onebot.access-token"
        )
        return CleanBotSettings(
            bot_id=profile.bot_id,
            dsn=storage.dsn,
            model_base_url=provider["base_url"],
            model_api_key=model_key,
            model=capabilities.model,
            model_deployment_id=capabilities.deployment_id,
            model_context_limit=capabilities.context_limit,
            model_max_output_tokens=capabilities.max_output_tokens,
            model_input_modalities=capabilities.input_modalities,
            embedding_endpoint=embedding["endpoint"],
            embedding_api_key=embedding_key,
            embedding_deployment_id=embedding_profile.deployment_id,
            embedding_model_revision=embedding_profile.model_revision,
            embedding_model=embedding_profile.model,
            embedding_dimensions=embedding_profile.dimensions,
            onebot_self_id=onebot["self_id"],
            onebot_websocket_url=onebot["websocket_url"],
            onebot_access_token=onebot_token,
            allowed_group_ids=trigger["allowed_group_ids"],
            group_trigger_mode=trigger["group_trigger_mode"],
            private_enabled=trigger["private_enabled"],
            shell_administrator_ids=tuple(
                principal.removeprefix("qq:") for principal in shell.administrator_principals
            ),
            agent_system_prompt="",
            model_credential_ref=provider["credential_ref"],
            embedding_credential_ref=embedding["credential_ref"],
            onebot_secret_ref=onebot["access_token_ref"],
            agent_preset_system=tuple(agent["preset_system"]),
            model_timeout_seconds=provider["timeout_seconds"],
            model_max_connections=provider["max_connections"],
            embedding_timeout_seconds=embedding["timeout_seconds"],
            embedding_max_connections=embedding["max_connections"],
            model_capabilities=capabilities,
            vector_index_endpoint=vector_index["endpoint"],
            vector_index_token=vector_index_token,
            vector_index_credential_ref=vector_index["credential_ref"],
            vector_index_timeout_seconds=vector_index["timeout_seconds"],
        )

    async def _secret(self, secret_ref: str, purpose: str) -> str:
        metadata = await self.secrets.metadata(secret_ref)
        if metadata.purpose != purpose:
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_SECRET_PURPOSE_INVALID",
                "Bootstrap secret purpose differs",
                409,
            )
        return await self.secrets.resolve_text(secret_ref)

    @staticmethod
    def _origin(value: str, schemes: set[str], field: str) -> None:
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme in schemes
                and parsed.hostname is not None
                and parsed.username is None
                and parsed.password is None
                and parsed.fragment == ""
            )
        except ValueError:
            valid = False
        if not valid:
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_CONFIGURATION_INVALID",
                f"Invalid {field}",
                422,
            )


class OfficialBotCatalogSettingsResolver:
    """Resolve the immutable bootstrap provenance carried by a READY catalog row."""

    def __init__(self, *, bootstrap_profiles, settings_resolver) -> None:
        self.bootstrap_profiles = bootstrap_profiles
        self.settings_resolver = settings_resolver

    async def __call__(self, bot: dict, storage) -> CleanBotSettings:
        profile = await self.bootstrap_profiles.load(bot["bootstrap_profile_ref"])
        if profile.manifest_digest != bot["bootstrap_manifest_digest"]:
            raise ForestError(
                "BOOTSTRAP_PROFILE_TAMPERED",
                "Ready Bot bootstrap manifest differs",
                409,
            )
        return await self.settings_resolver.resolve(profile=profile, storage=storage)


class OfficialBotProvisioner:
    """Materialize official module authorities without starting the runtime graph."""

    def __init__(
        self, *, settings_resolver, host_factory=CleanBotHost, database_allocator=None
    ) -> None:
        self.settings_resolver = settings_resolver
        self.host_factory = host_factory
        self.database_allocator = database_allocator

    async def __call__(self, *, claim, storage, profile) -> None:
        if claim.bot_id != profile.bot_id or claim.bot_id != storage.bot_id:
            raise ForestError(
                "PROVISIONING_INPUT_TAMPERED",
                "Provisioning identities differ",
                409,
            )
        settings = await self.settings_resolver.resolve(profile=profile, storage=storage)
        if self.database_allocator is not None:
            await self.database_allocator.ensure_database(
                bot_id=claim.bot_id,
                target_dsn=storage.dsn,
            )
        host = self.host_factory(settings)
        components = await host.provision()
        if set(components) != {
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "kernel.audit",
            "provider.chat-completions",
            "embedding.multimodal",
            "vector.index",
            "memory.graph",
            "media.capture",
            "agent.runtime",
            "conversation.trigger-policy",
            "conversation.scheduler",
            "adapter.onebot",
            "delivery",
            "process.executor",
            "job.scheduler",
            "shell.command",
        }:
            raise ForestError(
                "PROVISIONING_MODULE_SET_INVALID",
                "Provisioned official module set differs",
                409,
            )
