"""Production assembly for one clean Bot runtime graph."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import uuid4

from forest.builtin_packages import bootstrap_builtin_package
from forest.chat_completions_effect_driver import (
    chat_completions_effect_driver_factory,
)
from forest.core_runtime import bootstrap_clean_kernel
from forest.embedding_builtin import embedding_configuration_validator_factory
from forest.embedding_effect_driver import multimodal_embedding_effect_driver_factory
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.kernel_payload_effect_driver import kernel_payload_json_effect_driver_factory
from forest.media_capture_effect_driver import media_image_fetch_effect_driver_factory
from forest.models import ModelCapabilities
from forest.onebot_transport_effect_driver import onebot_transport_effect_driver_factory
from forest.package_effects import PackageEffectDriverRegistry
from forest.packages import PackageCatalog
from forest.process_executor_builtin import (
    default_process_executor_configuration,
    process_executor_configuration_validator_factory,
)
from forest.process_executor_effect_driver import native_process_effect_driver_factory
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.schema_bundles import SchemaBundleInstaller
from forest.vector_index_builtin import (
    default_vector_index_configuration,
    vector_index_configuration_validator_factory,
)
from forest.vector_index_effect_driver import milvus_vector_index_effect_driver_factory


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ForestError("BOT_CONFIGURATION_REQUIRED", f"{name} is required", 422)
    return value


def _secret(name: str, file_name: str) -> str:
    inline = os.environ.get(name)
    path = os.environ.get(file_name)
    if bool(inline) == bool(path):
        raise ForestError(
            "BOT_SECRET_CONFIGURATION_INVALID",
            f"Set exactly one of {name} and {file_name}",
            422,
        )
    if inline:
        return inline
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ForestError("BOT_SECRET_UNAVAILABLE", f"Unable to read {file_name}", 503) from exc
    if not value:
        raise ForestError("BOT_SECRET_UNAVAILABLE", f"{file_name} is empty", 503)
    return value


def _optional_secret(name: str, file_name: str) -> str | None:
    inline = os.environ.get(name)
    path = os.environ.get(file_name)
    if not inline and not path:
        return None
    return _secret(name, file_name)


def _positive_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = -1
    if not minimum <= value <= maximum:
        raise ForestError("BOT_CONFIGURATION_INVALID", f"{name} is outside its bound", 422)
    return value


def _bounded_float(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError:
        value = minimum - 1
    if not minimum <= value <= maximum:
        raise ForestError("BOT_CONFIGURATION_INVALID", f"{name} is outside its bound", 422)
    return value


def _origin(name: str, value: str, schemes: set[str]) -> str:
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in schemes
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError
        parsed.hostname.encode("idna")
    except (ValueError, UnicodeError):
        raise ForestError("BOT_CONFIGURATION_INVALID", f"{name} is invalid", 422) from None
    return value


def _bot_dsn() -> str:
    configured = os.environ.get("FOREST_BOT_DSN")
    if configured:
        return configured
    password_file = os.environ.get("FOREST_DATABASE_PASSWORD_FILE")
    if not password_file:
        raise ForestError(
            "BOT_CONFIGURATION_REQUIRED",
            "FOREST_BOT_DSN or FOREST_DATABASE_PASSWORD_FILE is required",
            422,
        )
    try:
        password = Path(password_file).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ForestError(
            "BOT_SECRET_UNAVAILABLE", "Database credential is unavailable", 503
        ) from exc
    host = os.environ.get("FOREST_DATABASE_HOST", "postgres")
    port = _positive_int("FOREST_DATABASE_PORT", 5432, 1, 65535)
    database = os.environ.get("FOREST_DATABASE_NAME", "forest_bot")
    user = os.environ.get("FOREST_DATABASE_USER", "forest")
    if not password or not database or not user:
        raise ForestError("BOT_CONFIGURATION_INVALID", "Database identity is invalid", 422)
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{quote(database, safe='')}"
    )


@dataclass(frozen=True)
class CleanBotSettings:
    bot_id: str
    dsn: str
    model_base_url: str
    model_api_key: str
    model: str
    model_deployment_id: str
    model_max_output_tokens: int
    model_input_modalities: tuple[str, ...]
    embedding_endpoint: str
    embedding_api_key: str
    embedding_deployment_id: str
    embedding_model_revision: str
    embedding_model: str
    embedding_dimensions: int
    onebot_self_id: str
    onebot_websocket_url: str
    onebot_access_token: str
    allowed_group_ids: tuple[str, ...]
    group_trigger_mode: str
    private_enabled: bool
    shell_administrator_ids: tuple[str, ...]
    agent_system_prompt: str
    model_credential_ref: str = "secret://provider.chat-completions/main/1"
    embedding_credential_ref: str = "secret://embedding.multimodal/main/1"
    onebot_secret_ref: str = "secret://onebot/main/1"
    agent_preset_system: tuple[str, ...] = ()
    model_timeout_seconds: float = 120
    model_max_connections: int = 8
    embedding_timeout_seconds: float = 60
    embedding_max_connections: int = 4
    model_capabilities: ModelCapabilities | None = None
    vector_index_endpoint: str = "http://milvus:19530"
    vector_index_token: str = ""
    vector_index_credential_ref: str = ""
    vector_index_timeout_seconds: float = 10

    @classmethod
    def from_environment(cls) -> "CleanBotSettings":
        groups = tuple(
            part.strip()
            for part in os.environ.get("FOREST_ALLOWED_GROUP_IDS", "").split(",")
            if part.strip()
        )
        if any(not item.isdecimal() for item in groups) or len(groups) != len(set(groups)):
            raise ForestError(
                "BOT_CONFIGURATION_INVALID", "FOREST_ALLOWED_GROUP_IDS is invalid", 422
            )
        shell_administrators = tuple(
            part.strip()
            for part in os.environ.get("FOREST_SHELL_ADMIN_IDS", "").split(",")
            if part.strip()
        )
        if any(not item.isdecimal() for item in shell_administrators) or len(
            shell_administrators
        ) != len(set(shell_administrators)):
            raise ForestError("BOT_CONFIGURATION_INVALID", "FOREST_SHELL_ADMIN_IDS is invalid", 422)
        modalities = tuple(
            part.strip()
            for part in os.environ.get("FOREST_MODEL_INPUT_MODALITIES", "text").split(",")
            if part.strip()
        )
        mode = os.environ.get("FOREST_GROUP_TRIGGER_MODE", "MENTION").upper()
        if mode not in {"MENTION", "ALL", "DISABLED"}:
            raise ForestError(
                "BOT_CONFIGURATION_INVALID", "FOREST_GROUP_TRIGGER_MODE is invalid", 422
            )
        private = os.environ.get("FOREST_PRIVATE_ENABLED", "true").lower()
        if private not in {"true", "false"}:
            raise ForestError("BOT_CONFIGURATION_INVALID", "FOREST_PRIVATE_ENABLED is invalid", 422)
        bot_id = os.environ.get("FOREST_BOT_ID", "forest-main")
        self_id = _required("FOREST_ONEBOT_SELF_ID")
        if not self_id.isdecimal():
            raise ForestError("BOT_CONFIGURATION_INVALID", "FOREST_ONEBOT_SELF_ID is invalid", 422)
        return cls(
            bot_id=bot_id,
            dsn=_bot_dsn(),
            model_base_url=_origin(
                "FOREST_MODEL_BASE_URL", _required("FOREST_MODEL_BASE_URL"), {"http", "https"}
            ),
            model_api_key=_secret("FOREST_MODEL_API_KEY", "FOREST_MODEL_API_KEY_FILE"),
            model=_required("FOREST_MODEL_NAME"),
            model_deployment_id=os.environ.get(
                "FOREST_MODEL_DEPLOYMENT_ID", "provider.chat-completions/main"
            ),
            model_max_output_tokens=_positive_int(
                "FOREST_MODEL_MAX_OUTPUT_TOKENS", 4096, 1, 262_144
            ),
            model_input_modalities=modalities,
            embedding_endpoint=_origin(
                "FOREST_EMBEDDING_ENDPOINT",
                _required("FOREST_EMBEDDING_ENDPOINT"),
                {"http", "https"},
            ),
            embedding_api_key=_secret("FOREST_EMBEDDING_API_KEY", "FOREST_EMBEDDING_API_KEY_FILE"),
            embedding_deployment_id=os.environ.get(
                "FOREST_EMBEDDING_DEPLOYMENT_ID",
                "embedding.multimodal/qiyu-vl/default",
            ),
            embedding_model_revision=os.environ.get(
                "FOREST_EMBEDDING_MODEL_REVISION", "operator-declared/1"
            ),
            embedding_model=os.environ.get("FOREST_EMBEDDING_MODEL", "Qwen/Qwen3-VL-Embedding-8B"),
            embedding_dimensions=_positive_int("FOREST_EMBEDDING_DIMENSIONS", 4096, 1, 4096),
            onebot_self_id=self_id,
            onebot_websocket_url=_origin(
                "FOREST_ONEBOT_WEBSOCKET_URL",
                _required("FOREST_ONEBOT_WEBSOCKET_URL"),
                {"ws", "wss"},
            ),
            onebot_access_token=_secret(
                "FOREST_ONEBOT_ACCESS_TOKEN", "FOREST_ONEBOT_ACCESS_TOKEN_FILE"
            ),
            allowed_group_ids=groups,
            group_trigger_mode=mode,
            private_enabled=private == "true",
            shell_administrator_ids=shell_administrators,
            agent_system_prompt=os.environ.get("FOREST_AGENT_SYSTEM_PROMPT", ""),
            vector_index_endpoint=_origin(
                "FOREST_VECTOR_INDEX_ENDPOINT",
                os.environ.get("FOREST_VECTOR_INDEX_ENDPOINT", "http://milvus:19530"),
                {"http", "https"},
            ),
            vector_index_token=(
                _optional_secret("FOREST_VECTOR_INDEX_TOKEN", "FOREST_VECTOR_INDEX_TOKEN_FILE")
                or ""
            ),
            vector_index_credential_ref=os.environ.get(
                "FOREST_VECTOR_INDEX_CREDENTIAL_REF", ""
            ),
            vector_index_timeout_seconds=_bounded_float(
                "FOREST_VECTOR_INDEX_TIMEOUT_SECONDS", 10, 1, 300
            ),
        )


class CleanBotHost:
    """Own generic process resources for one immutable runtime graph."""

    scope_kind = "bot"

    def __init__(
        self,
        settings: CleanBotSettings,
        *,
        model_client_factory=None,
        embedding_client_factory=None,
        vector_index_client_factory=None,
        media_client_factory=None,
        websocket_client_factory=None,
        package_catalog=None,
    ) -> None:
        self.settings = settings
        self.store = CleanBotStore(settings.dsn, settings.bot_id)
        self.schema_bundle_installer = SchemaBundleInstaller(self.store)
        self.generation_authority = None
        self.registry: RuntimeModuleRegistry | None = None
        self._lease = None
        self._renewal_task: asyncio.Task | None = None
        self._replacement_task: asyncio.Task | None = None
        self._supervision_task: asyncio.Task | None = None
        self._replacement_lock: asyncio.Lock | None = None
        self._closing = False
        self._host_id = f"forest:{settings.bot_id}"
        self._incarnation = str(uuid4())
        self._model_client_factory = model_client_factory
        self._embedding_client_factory = embedding_client_factory
        self._vector_index_client_factory = vector_index_client_factory
        self._media_client_factory = media_client_factory
        self._websocket_client_factory = websocket_client_factory
        self.package_catalog = package_catalog or PackageCatalog(
            Path(os.environ.get("FOREST_PACKAGE_CATALOG_ROOT", ".forest/packages"))
        )

    @property
    def bot(self):
        return self

    @property
    def kernel(self):
        return self

    def _authority(self, *, allow_closing: bool = False):
        if self._lease is None or (self._closing and not allow_closing):
            raise ForestError("KERNEL_UNAVAILABLE", "Bot host lease is unavailable", 503)
        return self._lease

    def _cleanup_lease(self):
        return self._authority(allow_closing=True)

    def notify(self) -> None:
        if self.registry is not None:
            self.registry.notify_work()

    async def _assemble_registry(
        self, activation_id: str | None = None
    ) -> RuntimeModuleRegistry:
        capabilities = self.settings.model_capabilities or ModelCapabilities(
            deployment_id=self.settings.model_deployment_id,
            model=self.settings.model,
            capability_revision="operator-declared/1",
            input_modalities=self.settings.model_input_modalities,
            max_output_tokens=self.settings.model_max_output_tokens,
        )
        async def resolve_provider_secret(secret_ref, _revision):
            if secret_ref != self.settings.model_credential_ref:
                raise ForestError(
                    "PROVIDER_CREDENTIAL_UNAVAILABLE",
                    "Provider credential reference differs",
                    409,
                )
            return self.settings.model_api_key

        provider_driver_options = {}
        if self._model_client_factory is not None:
            provider_driver_options["client_factory"] = self._model_client_factory
        provider_effect_factory = chat_completions_effect_driver_factory(
            effects=self._kernel.effect,
            payloads=self._kernel.payload,
            runtime=self._kernel.runtime,
            credential_resolver=resolve_provider_secret,
            **provider_driver_options,
        )
        provider = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/provider_chat_completions"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.provider.chat-completions/1",
                "base_url": self.settings.model_base_url,
                "credential_ref": self.settings.model_credential_ref,
                "timeout_seconds": self.settings.model_timeout_seconds,
                "max_connections": self.settings.model_max_connections,
                "capabilities": capabilities.as_dict(),
            },
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "chat-completions.materialize": provider_effect_factory,
                    "chat-completions.http": provider_effect_factory,
                }
            ),
        )
        embedding_profile = {
            "deployment_id": self.settings.embedding_deployment_id,
            "model_revision": self.settings.embedding_model_revision,
            "model": self.settings.embedding_model,
            "dimensions": self.settings.embedding_dimensions,
        }
        async def resolve_embedding_secret(secret_ref, _revision):
            if secret_ref != self.settings.embedding_credential_ref:
                raise ForestError(
                    "EMBEDDING_CREDENTIAL_UNAVAILABLE",
                    "Embedding credential reference differs",
                    409,
                )
            return self.settings.embedding_api_key

        embedding_driver_options = {}
        if self._embedding_client_factory is not None:
            embedding_driver_options["client_factory"] = self._embedding_client_factory
        embedding = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/embedding_multimodal"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.embedding.multimodal/1",
                "endpoint": self.settings.embedding_endpoint,
                "credential_ref": self.settings.embedding_credential_ref,
                "timeout_seconds": self.settings.embedding_timeout_seconds,
                "max_connections": self.settings.embedding_max_connections,
                "profile": embedding_profile,
            },
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "embedding.multimodal.http": (
                        multimodal_embedding_effect_driver_factory(
                            effects=self._kernel.effect,
                            payloads=self._kernel.payload,
                            runtime=self._kernel.runtime,
                            credential_resolver=resolve_embedding_secret,
                            **embedding_driver_options,
                        )
                    )
                }
            ),
            configuration_validator_factory=(
                embedding_configuration_validator_factory(self.store)
            ),
        )

        async def resolve_vector_index_token(secret_ref, _revision):
            if secret_ref != self.settings.vector_index_credential_ref:
                raise ForestError(
                    "VECTOR_INDEX_CREDENTIAL_UNAVAILABLE",
                    "Vector index credential reference differs",
                    409,
                )
            return self.settings.vector_index_token

        vector_index_driver_options = {}
        if self._vector_index_client_factory is not None:
            vector_index_driver_options["client_factory"] = self._vector_index_client_factory
        vector_index = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/vector_index_milvus"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                **default_vector_index_configuration(
                    space_id=EmbeddingProfile(**embedding_profile).space_id,
                    dimensions=self.settings.embedding_dimensions,
                ),
                "endpoint": self.settings.vector_index_endpoint,
                "credential_ref": self.settings.vector_index_credential_ref,
                "timeout_seconds": self.settings.vector_index_timeout_seconds,
            },
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "vector-index.milvus": milvus_vector_index_effect_driver_factory(
                        bot_id=self.store.bot_id,
                        runtime=self._kernel.runtime,
                        token_resolver=resolve_vector_index_token,
                        **vector_index_driver_options,
                    )
                }
            ),
            configuration_validator_factory=(
                vector_index_configuration_validator_factory(self.store)
            ),
        )
        memory = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/memory_graph"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.memory.graph/2",
                "embedding": {
                    "model_kind": "TEXT_IMAGE",
                    **embedding_profile,
                    "normalization": "L2",
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
            },
            resolved_contracts={"embedding.profile.bind": "1.0"},
            additional_grants=(
                CapabilityGrant("embedding.invoke", "bot", self.store.bot_id),
            ),
        )
        media_driver_options = {}
        if self._media_client_factory is not None:
            media_driver_options["client_factory"] = self._media_client_factory
        media = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/media_capture"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.media.capture/1",
                "timeout_seconds": 15,
                "max_connections": 4,
                "max_attempts": 3,
                "context_wait_seconds": 3,
            },
            resolved_contracts={
                "memory.source.capture": "1.0",
                "memory.source.unavailable": "1.0",
            },
            startup_components=("memory.graph",),
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "media.public-image": media_image_fetch_effect_driver_factory(
                        runtime=self._kernel.runtime,
                        payloads=self._kernel.payload,
                        **media_driver_options,
                    )
                }
            ),
        )
        preset_system = self.settings.agent_preset_system or (
            (self.settings.agent_system_prompt,) if self.settings.agent_system_prompt else ()
        )
        preset_owner_id = "preset/agent.runtime/default"
        preset_payload = await self._kernel.payload.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind="preset.context",
            owner_id=preset_owner_id,
            media_type="application/json",
            content=json.dumps(
                {"system": list(preset_system)},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8"),
            expected_deletion_epoch=await self._kernel.payload.epoch(),
        )
        agent = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/agent_runtime"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.agent.runtime/1",
                "agent_spec_revision": "agentlib-aligned/1",
                "preset_revision": "preset/default/1",
                "preset": {
                    "payload_id": preset_payload.payload_id,
                    "digest": preset_payload.content_digest,
                    "owner_id": preset_owner_id,
                },
                "run_timeout_seconds": 600,
                "max_steps": 64,
                "output_tokens": self.settings.model_max_output_tokens,
                "stream": True,
            },
            resolved_contracts={
                "provider.profile.bind": "1.0",
                "provider.request.materialize": "1.0",
                "provider.model.dispatch": "2.0",
                "conversation.context.snapshot": "2.0",
            },
            startup_components=("provider.chat-completions",),
            additional_grants=(
                CapabilityGrant("provider.invoke", "bot", self.store.bot_id),
            ),
        )
        routing = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/routing_graph"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={"protocol": "forest.routing.graph/2"},
        )
        trigger = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/trigger_policy"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.conversation.trigger-policy/1",
                "allowed_group_ids": list(self.settings.allowed_group_ids),
                "group_trigger_mode": self.settings.group_trigger_mode,
                "private_enabled": self.settings.private_enabled,
            },
        )
        onebot_codec = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/onebot_codec"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={},
        )
        jobs = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/job_scheduler"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={"protocol": "forest.job.scheduler/1"},
            additional_grants=(
                CapabilityGrant("job.read", "bot", self.store.bot_id),
                CapabilityGrant("job.run", "bot", self.store.bot_id),
                CapabilityGrant("job.cancel", "bot", self.store.bot_id),
            ),
        )
        process_executor = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/process_executor"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration=default_process_executor_configuration(),
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "process.native": native_process_effect_driver_factory(
                        runtime=self._kernel.runtime,
                        payloads=self._kernel.payload,
                    )
                }
            ),
            configuration_validator_factory=(
                process_executor_configuration_validator_factory(self.store)
            ),
        )

        async def resolve_onebot_secret(secret_ref, _configuration_revision):
            if secret_ref != self.settings.onebot_secret_ref:
                raise ForestError(
                    "ONEBOT_CREDENTIAL_UNAVAILABLE",
                    "OneBot credential reference differs",
                    409,
                )
            return self.settings.onebot_access_token

        conversation = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/conversation_scheduler"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.conversation.scheduler/2",
                "context_view": {
                    "high_watermark_units": 512,
                    "retain_units": 256,
                    "policy_revision": "context-view-unit-window-v1",
                },
            },
            resolved_contracts={
                "agent.execute": "2.0",
                "agent.execution.policy": "1.0",
            },
            startup_components=("agent.runtime",),
        )
        delivery = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/delivery"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={"protocol": "forest.delivery/1"},
            resolved_contracts={
                "adapter.onebot.delivery.prepare": "2.0",
                "adapter.onebot.delivery.dispatch": "2.0",
            },
            startup_components=("adapter.onebot",),
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "kernel.payload-json": kernel_payload_json_effect_driver_factory(
                        runtime=self._kernel.runtime,
                        payloads=self._kernel.payload,
                    )
                }
            ),
            additional_grants=(
                CapabilityGrant("delivery.intent", "bot", self.store.bot_id),
                CapabilityGrant(
                    "delivery.send", "bot", self.store.bot_id, delegable=True
                ),
            ),
        )
        shell = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/shell_command"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.shell.command/2",
                "binding_ref": "binding/main",
                "administrator_principals": [
                    "qq:" + value for value in self.settings.shell_administrator_ids
                ],
            },
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "kernel.payload-json": kernel_payload_json_effect_driver_factory(
                        runtime=self._kernel.runtime,
                        payloads=self._kernel.payload,
                    )
                }
            ),
            additional_grants=(
                CapabilityGrant(
                    "management.invoke", "bot", self.store.bot_id, delegable=True
                ),
            ),
        )
        adapter_driver_options = {}
        if self._websocket_client_factory is not None:
            adapter_driver_options["client_factory"] = self._websocket_client_factory
        adapter = await bootstrap_builtin_package(
            Path(str(files("forest").joinpath("builtin/adapter_onebot"))),
            self.package_catalog,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            initial_configuration={
                "protocol": "forest.adapter.onebot/2",
                "bindings": [
                    {
                        "binding_ref": "binding/main",
                        "binding_epoch": 1,
                        "self_id": self.settings.onebot_self_id,
                        "credential_ref": self.settings.onebot_secret_ref,
                        "websocket_url": self.settings.onebot_websocket_url,
                        "timeout_seconds": 30,
                        "event_capacity": 512,
                        "pending_capacity": 64,
                        "enabled": True,
                    }
                ],
            },
            resolved_contracts={
                "onebot.codec.normalize": "1.0",
                "onebot.codec.prepare-send": "1.0",
                "conversation.trigger.classify": "1.0",
                "shell.ingress.inspect": "1.0",
            },
            startup_components=(
                "codec.onebot",
                "conversation.trigger-policy",
            ),
            effect_driver_registry=PackageEffectDriverRegistry(
                {
                    "onebot.websocket-v11": onebot_transport_effect_driver_factory(
                        effects=self._kernel.effect,
                        runtime=self._kernel.runtime,
                        credential_resolver=resolve_onebot_secret,
                        **adapter_driver_options,
                    ),
                    "kernel.payload-bytes": kernel_payload_json_effect_driver_factory(
                        runtime=self._kernel.runtime,
                        payloads=self._kernel.payload,
                    ),
                }
            ),
        )
        descriptors = (
            self._kernel.descriptors
            + (
                provider.descriptor,
                embedding.descriptor,
                vector_index.descriptor,
                memory.descriptor,
                media.descriptor,
                agent.descriptor,
                routing.descriptor,
                trigger.descriptor,
                onebot_codec.descriptor,
                process_executor.descriptor,
                jobs.descriptor,
                shell.descriptor,
                adapter.descriptor,
            )
            + (conversation.descriptor, delivery.descriptor)
        )
        return RuntimeModuleRegistry(self, descriptors, activation_id=activation_id)

    async def start(self) -> None:
        await self.store.open()
        try:
            self._kernel = await bootstrap_clean_kernel(self.store, self.schema_bundle_installer)
            self.generation_authority = self._kernel.runtime
            self._replacement_lock = asyncio.Lock()
            registry = await self._assemble_registry()
            self._lease = await self._kernel.runtime.claim_host(self._host_id, self._incarnation)
            # Generation preparation can exceed the 30 second HostLease TTL on a
            # cold install. Lease ownership starts at claim, so renewal must also
            # start there; waiting until publication can fence the very owner that
            # is still preparing the graph.
            self._renewal_task = asyncio.create_task(
                self._renew_lease(), name=f"bot-host-lease:{self.settings.bot_id}"
            )
            await registry.start()
            self.registry = registry
            self._replacement_task = asyncio.create_task(
                self._run_graph_replacements(),
                name=f"bot-graph-replacement:{self.settings.bot_id}",
            )
            self._supervision_task = asyncio.create_task(
                self._supervise_owned_tasks(),
                name=f"bot-runtime-supervision:{self.settings.bot_id}",
            )
        except BaseException:
            await self.close()
            raise

    async def provision(self) -> tuple[str, ...]:
        """Install module authorities and revision-one configuration without starting I/O."""

        if self.registry is not None or self._lease is not None:
            raise ForestError("BOT_HOST_ALREADY_ACTIVE", "Active Bot host cannot provision", 409)
        await self.store.open()
        try:
            self._kernel = await bootstrap_clean_kernel(self.store, self.schema_bundle_installer)
            registry = await self._assemble_registry()
            return tuple(descriptor.component_id for descriptor in registry.descriptors)
        finally:
            await self.store.close()

    async def replace_runtime_graph(
        self,
        *,
        activation_id: str | None = None,
        expected_graph_revision: int | None = None,
    ) -> RuntimeModuleRegistry:
        """Prewarm a candidate, drain the current graph, then publish and resume it."""

        lock = self._replacement_lock
        if lock is None or self.registry is None or self._closing:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
        async with lock:
            if self.registry is None or self._closing:
                raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
            previous = self.registry
            if (
                expected_graph_revision is not None
                and previous.graph_identity.graph_revision != expected_graph_revision
            ):
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT",
                    "RuntimeGraph revision changed before replacement",
                    409,
                )
            candidate = (
                await self._assemble_registry()
                if activation_id is None
                else await self._assemble_registry(activation_id)
            )
            previous_drained = False
            try:
                await candidate.prewarm()
                await previous.drain_owned_work_for_replacement(candidate.activation_id)
                previous_drained = True
                await candidate.start()
            except BaseException as replacement_error:
                cleanup_error = None
                try:
                    cleanup_errors = await candidate.close()
                    if cleanup_errors:
                        cleanup_error = cleanup_errors[0]
                except BaseException as exc:
                    cleanup_error = exc
                if previous_drained:
                    try:
                        await previous.resume_owned_work_after_failed_replacement()
                    except BaseException as resume_error:
                        raise ForestError(
                            "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED",
                            "Candidate publication failed and the previous graph could not resume",
                            503,
                        ) from resume_error
                if cleanup_error is not None:
                    raise ForestError(
                        "RUNTIME_GRAPH_REPLACEMENT_CLEANUP_FAILED",
                        "Candidate publication failed and its resources did not close cleanly",
                        503,
                    ) from cleanup_error
                raise replacement_error
            self.registry = candidate
            errors = await previous.close()
            if errors:
                raise ForestError(
                    "RUNTIME_GRAPH_DRAIN_FAILED",
                    "The replacement graph is active but the superseded graph did not drain",
                    503,
                    details={"errors": len(errors)},
                )
            return candidate

    async def _run_graph_replacements(self) -> None:
        while not self._closing:
            request = await self._kernel.runtime.claim_graph_replacement(
                self._authority(), lease_seconds=120
            )
            if request is None:
                await asyncio.sleep(0.5)
                continue
            try:
                await self._execute_graph_replacement(request)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                evidence = await self._kernel.runtime.graph_for_activation(
                    request["activation_id"]
                )
                status = await self._kernel.runtime.status()
                if (
                    evidence is not None
                    and evidence["state"] in {"ACTIVE", "RETIRED", "STOPPED"}
                    and status["graph_revision"] >= evidence["graph_revision"]
                ):
                    await self._kernel.runtime.finish_graph_replacement(
                        self._authority(),
                        operation_id=request["operation_id"],
                        graph_id=evidence["graph_id"],
                        graph_revision=evidence["graph_revision"],
                    )
                    continue
                error_code = (
                    exc.code
                    if isinstance(exc, ForestError)
                    else "RUNTIME_GRAPH_REPLACEMENT_FAILED"
                )
                await self._kernel.runtime.fail_graph_replacement(
                    self._authority(),
                    operation_id=request["operation_id"],
                    error_code=error_code,
                )

    async def _execute_graph_replacement(self, request: dict) -> None:
        status = await self._kernel.runtime.status()
        if status["graph_revision"] != request["expected_graph_revision"]:
            evidence = await self._kernel.runtime.graph_for_activation(
                request["activation_id"]
            )
            if evidence is not None and evidence["state"] in {
                "ACTIVE",
                "RETIRED",
                "STOPPED",
            }:
                await self._kernel.runtime.finish_graph_replacement(
                    self._authority(),
                    operation_id=request["operation_id"],
                    graph_id=evidence["graph_id"],
                    graph_revision=evidence["graph_revision"],
                )
                return
            await self._kernel.runtime.fail_graph_replacement(
                self._authority(),
                operation_id=request["operation_id"],
                error_code="RUNTIME_GRAPH_REVISION_CONFLICT",
            )
            return
        registry = await self.replace_runtime_graph(
            activation_id=request["activation_id"],
            expected_graph_revision=request["expected_graph_revision"],
        )
        await self._kernel.runtime.finish_graph_replacement(
            self._authority(),
            operation_id=request["operation_id"],
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
        )

    async def _renew_lease(self) -> None:
        while True:
            await asyncio.sleep(10)
            self._lease = await self._kernel.runtime.claim_host(self._host_id, self._incarnation)

    async def _supervise_owned_tasks(self) -> None:
        """Fail the Bot host when a task still owned by its active graph stops."""

        while not self._closing:
            lock = self._replacement_lock
            if lock is None:
                raise ForestError(
                    "RUNTIME_SUPERVISION_UNAVAILABLE",
                    "Bot runtime supervision has no replacement lock",
                    503,
                )
            async with lock:
                registry = self.registry
                watched = tuple(
                    task
                    for task in (
                        self._renewal_task,
                        self._replacement_task,
                        *(registry.tasks() if registry is not None else ()),
                    )
                    if task is not None
                )
            if not watched:
                raise ForestError(
                    "RUNTIME_SUPERVISION_EMPTY",
                    "Active Bot runtime owns no supervised tasks",
                    503,
                )
            done, _pending = await asyncio.wait(
                watched,
                timeout=0.5,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                continue
            async with lock:
                if self._closing:
                    return
                registry = self.registry
                current = {
                    task
                    for task in (
                        self._renewal_task,
                        self._replacement_task,
                        *(registry.tasks() if registry is not None else ()),
                    )
                    if task is not None
                }
            failed = next((task for task in done if task in current), None)
            if failed is None:
                continue
            if failed.cancelled():
                raise ForestError(
                    "RUNTIME_OWNED_TASK_CANCELLED",
                    "An active runtime-owned task was cancelled",
                    503,
                    details={"task": failed.get_name()},
                )
            failure = failed.exception()
            if failure is not None:
                raise failure
            raise ForestError(
                "RUNTIME_OWNED_TASK_STOPPED",
                "An active runtime-owned task stopped unexpectedly",
                503,
                details={"task": failed.get_name()},
            )

    async def close(self) -> None:
        lock = self._replacement_lock
        if lock is None:
            await self._close_locked()
            return
        async with lock:
            await self._close_locked()

    async def _close_locked(self) -> None:
        self._closing = True
        failure = None
        supervision, self._supervision_task = self._supervision_task, None
        if supervision is not None:
            supervision.cancel()
            await asyncio.gather(supervision, return_exceptions=True)
        replacement, self._replacement_task = self._replacement_task, None
        if replacement is not None:
            replacement.cancel()
            await asyncio.gather(replacement, return_exceptions=True)
        registry, self.registry = self.registry, None
        if registry is not None:
            errors = await registry.close()
            if errors:
                failure = ForestError(
                    "RUNTIME_MODULE_CLEANUP_FAILED",
                    "Bot runtime graph did not close cleanly",
                    503,
                    details={
                        "errors": [
                            getattr(error, "code", type(error).__name__)
                            for error in errors
                        ]
                    },
                )
        # The HostLease fences every cleanup write. Keep renewing it until the
        # graph has frozen admission, drained owned work, released contribution
        # directories, stopped Workers, and retired its persistent Generation.
        task, self._renewal_task = self._renewal_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        lease, self._lease = self._lease, None
        if failure is None and lease is not None:
            try:
                await self._kernel.runtime.release_host(lease)
            except ForestError as exc:
                if exc.code != "STALE_HOST":
                    failure = exc
        await self.store.close()
        if failure is not None:
            raise failure


async def run_clean_bot(settings: CleanBotSettings) -> None:
    host = CleanBotHost(settings)
    stop = asyncio.Event()
    management_server = None
    management_task = None
    stop_task = None
    status_file = os.environ.get("FOREST_RUNTIME_STATUS_FILE")
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(event, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(event, lambda *_args: loop.call_soon_threadsafe(stop.set))
    try:
        await host.start()
        management_token = _optional_secret(
            "FOREST_MANAGEMENT_TOKEN", "FOREST_MANAGEMENT_TOKEN_FILE"
        )
        if management_token:
            import uvicorn

            from forest.clean_management_app import create_clean_management_app

            management_host = os.environ.get("FOREST_MANAGEMENT_HOST", "127.0.0.1")
            management_port = _positive_int("FOREST_MANAGEMENT_PORT", 8080, 1, 65535)
            management_server = uvicorn.Server(
                uvicorn.Config(
                    create_clean_management_app(host, access_token=management_token),
                    host=management_host,
                    port=management_port,
                    access_log=False,
                    proxy_headers=False,
                    limit_concurrency=64,
                )
            )
            management_server.install_signal_handlers = lambda: None
            management_task = asyncio.create_task(
                management_server.serve(), name=f"management-web:{settings.bot_id}"
            )
            while not management_server.started:
                if management_task.done():
                    await management_task
                    raise RuntimeError("Management HTTP server stopped during startup")
                await asyncio.sleep(0.01)
        graph = host.registry.graph_identity
        status = {
            "status": "active",
            "bot_id": settings.bot_id,
            "graph_id": graph.graph_id,
            "components": sorted(graph.generations),
            "management": (
                {
                    "host": os.environ.get("FOREST_MANAGEMENT_HOST", "127.0.0.1"),
                    "port": _positive_int("FOREST_MANAGEMENT_PORT", 8080, 1, 65535),
                }
                if management_server is not None
                else None
            ),
        }
        if status_file:
            target = Path(status_file)
            temporary = target.with_name(target.name + ".tmp")
            temporary.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")
            os.replace(temporary, target)
        print(json.dumps(status, ensure_ascii=False), flush=True)
        stop_task = asyncio.create_task(stop.wait(), name=f"host-stop:{settings.bot_id}")
        supervised = tuple(
            task
            for task in (
                stop_task,
                management_task,
                host._renewal_task,
                host._replacement_task,
                host._supervision_task,
            )
            if task is not None
        )
        done, _pending = await asyncio.wait(
            supervised, return_when=asyncio.FIRST_COMPLETED
        )
        if not stop.is_set():
            failed = next(task for task in supervised if task in done)
            await failed
            raise RuntimeError(f"Bot supervisor task stopped unexpectedly: {failed.get_name()}")
    finally:
        try:
            if stop_task is not None and not stop_task.done():
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
            if management_server is not None:
                management_server.should_exit = True
            if management_task is not None and not management_task.done():
                await management_task
            await host.close()
        finally:
            if status_file:
                Path(status_file).unlink(missing_ok=True)
