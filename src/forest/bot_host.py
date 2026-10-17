"""Production assembly for one clean Bot runtime graph."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from dataclasses import dataclass
from datetime import timedelta
from importlib.resources import files
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import uuid4

from forest.bootstrap_materialization import (
    materialize_bootstrap_configuration,
    project_bootstrap_profiles,
)
from forest.builtin_packages import (
    bootstrap_builtin_snapshot,
    bootstrap_installed_package,
)
from forest.chat_completions_effect_driver import (
    chat_completions_effect_driver_factory,
)
from forest.component_graph_preview import (
    SelectionGraphPreview,
    project_selection_graph_in,
)
from forest.component_selection import (
    ComponentSelectionReader,
    ComponentSelectionSnapshot,
    resolve_official_package_snapshots,
    select_package_members,
)
from forest.component_selection_commit import resolve_selection_commit_preflight
from forest.control_driver_binding_gate import ControlDriverBindingGate
from forest.control_effect_driver import control_export_effect_driver_factory
from forest.core_runtime import bootstrap_kernel
from forest.embedding_effect_driver import multimodal_embedding_effect_driver_factory
from forest.errors import ForestError
from forest.kernel_payload_effect_driver import kernel_payload_json_effect_driver_factory
from forest.kernel_resource_admission import assess_worker_capacity_in
from forest.media_capture_effect_driver import media_image_fetch_effect_driver_factory
from forest.model_contract import ModelCapabilities
from forest.onebot_transport_effect_driver import onebot_transport_effect_driver_factory
from forest.package_effects import PackageEffectDriverRegistry
from forest.packages import PackageCatalog, PackageSnapshot, auto_install_official_sources
from forest.process_executor_effect_driver import (
    native_process_effect_driver_factory,
    validate_process_executor_configuration,
)
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
    RuntimeWorkerCapacity,
)
from forest.shell_management_authority import ShellManagementAuthorization
from forest.standalone_bot_bootstrap import standalone_creation_components
from forest.storage.bot_store import BotStore
from forest.storage.schema_bundles import SchemaBundleInstaller
from forest.vector_index_effect_driver import milvus_vector_index_effect_driver_factory

_HOST_LEASE_TTL = timedelta(minutes=2)


@dataclass(frozen=True)
class BotGraphCandidate:
    registry: RuntimeModuleRegistry
    selection: ComponentSelectionSnapshot
    official_descriptors: tuple[RuntimeModuleDescriptor, ...]
    selection_reader: ComponentSelectionReader
    official_defaults: tuple[PackageSnapshot, ...] = ()
    official_effect_drivers: PackageEffectDriverRegistry | None = None


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
class BotSettings:
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
    bootstrap_package_digests: tuple[tuple[str, str], ...] = ()


    @classmethod
    def from_environment(cls) -> "BotSettings":
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


@dataclass(frozen=True)
class BotRuntimeSettings:
    """READY Bot launch identity; module configuration lives in each module ledger."""

    bot_id: str
    dsn: str


@dataclass(frozen=True)
class BotBootstrapSettings:
    """Frozen package-owned creation inputs for one first materialization."""

    bot_id: str
    dsn: str
    components: tuple[tuple[str, dict], ...]
    bootstrap_package_digests: tuple[tuple[str, str], ...]


class BotHost:
    """Own generic process resources for one immutable runtime graph."""

    scope_kind = "bot"

    def __init__(
        self,
        settings: BotSettings | BotBootstrapSettings | BotRuntimeSettings,
        *,
        model_client_factory=None,
        embedding_client_factory=None,
        vector_index_client_factory=None,
        vector_index_audit_client_factory=None,
        media_client_factory=None,
        websocket_client_factory=None,
        credential_resolver=None,
        package_catalog=None,
        package_registry_reader=None,
        host_driver_controller_provider=None,
        driver_binding_gate: ControlDriverBindingGate | None = None,
    ) -> None:
        if isinstance(settings, BotRuntimeSettings) and credential_resolver is None:
            raise ForestError(
                "BOT_RUNTIME_CREDENTIAL_RESOLVER_REQUIRED",
                "READY Bot host requires the purpose-scoped Secret port", 422,
            )
        self.settings = settings
        self.store = BotStore(settings.dsn, settings.bot_id)
        self.schema_bundle_installer = SchemaBundleInstaller(self.store)
        self.generation_authority = None
        self.registry: RuntimeModuleRegistry | None = None
        self._official_descriptors: tuple[RuntimeModuleDescriptor, ...] = ()
        self._official_defaults: tuple[PackageSnapshot, ...] = ()
        self._official_effect_drivers: PackageEffectDriverRegistry | None = None
        self._lease = None
        self._renewal_task: asyncio.Task | None = None
        self._replacement_task: asyncio.Task | None = None
        self._supervision_task: asyncio.Task | None = None
        self._replacement_lock: asyncio.Lock | None = None
        self._fatal_replacement_error: ForestError | None = None
        self._closing = False
        self._host_id = f"forest:{settings.bot_id}"
        self._incarnation = str(uuid4())
        self._model_client_factory = model_client_factory
        self._embedding_client_factory = embedding_client_factory
        self._vector_index_client_factory = vector_index_client_factory
        self._vector_index_audit_client_factory = vector_index_audit_client_factory
        self._media_client_factory = media_client_factory
        self._websocket_client_factory = websocket_client_factory
        self._credential_resolver = credential_resolver
        self.package_catalog = package_catalog or PackageCatalog(
            Path(os.environ.get("FOREST_PACKAGE_CATALOG_ROOT", ".forest/packages"))
        )
        self.package_registry_reader = package_registry_reader
        self._host_driver_controller_provider = host_driver_controller_provider
        self._driver_binding_gate = driver_binding_gate
        self.selection_reader: ComponentSelectionReader | None = None

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

    def authority_lease(self, *, allow_closing: bool = False):
        return self._authority(allow_closing=allow_closing)

    def notify(self) -> None:
        if self.registry is not None:
            self.registry.notify_work()

    async def _resolve_effect_credential(
        self, secret_ref: str, *, purpose: str, initial_ref: str,
        initial_value: str, error_code: str,
    ) -> str:
        if self._credential_resolver is not None:
            if not secret_ref and purpose == "vector.index.credential":
                return ""
            return await self._credential_resolver(secret_ref, purpose)
        if secret_ref != initial_ref:
            raise ForestError(error_code, "Credential reference differs", 409)
        return initial_value

    def resolve_management_action_preflight(
        self, identifier: str, *, component_id: str, package_digest: str,
        trust_level: str,
    ):
        if trust_level != "official":
            return None
        return resolve_selection_commit_preflight(
            self, identifier, component_id=component_id,
            package_digest=package_digest,
        )

    def resolve_shell_management_authorization(
        self, *, request, component_id: str, generation_id: str,
        service_id: str, service_version: int, action_id: str,
        operation_id: str, input_digest: str, plan_digest: str | None,
    ) -> ShellManagementAuthorization:
        return ShellManagementAuthorization(
            self, request, component_id, generation_id, service_id,
            service_version, action_id, operation_id, input_digest, plan_digest,
        )

    async def _admit_worker_capacity(self, capacity: RuntimeWorkerCapacity):
        """Check the runtime-owned desired policy before any Worker preparation."""

        async with self.store.pool.connection() as conn, conn.transaction():
            return await assess_worker_capacity_in(self._kernel, conn, capacity)

    async def _assemble_candidate(
        self, activation_id: str | None = None,
        *, bootstrap_package_digests: dict[str, str] | None = None,
        bootstrap_settings: BotSettings | BotBootstrapSettings | None = None,
        prepared_host_graph: tuple[RuntimeModuleRegistry, dict[str, PackageSnapshot]] | None = None,
    ) -> BotGraphCandidate:
        initial_settings = self.settings if isinstance(self.settings, BotSettings) else None
        async def resolve_provider_secret(secret_ref, _revision):
            return await self._resolve_effect_credential(
                secret_ref, purpose="provider.chat-completions.credential",
                initial_ref=initial_settings.model_credential_ref if initial_settings else "",
                initial_value=initial_settings.model_api_key if initial_settings else "",
                error_code="PROVIDER_CREDENTIAL_UNAVAILABLE",
            )

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
        worker_host_identity = await self._kernel.runtime.last_published_package(
            "component.worker-host"
        )
        if worker_host_identity is None:
            worker_host_source = Path(str(files("forest").joinpath("builtin/component_worker_host")))
            worker_host_snapshot = await asyncio.to_thread(
                self.package_catalog.build_official, worker_host_source, scope="bot"
            )
        else:
            if (
                worker_host_identity["package_id"] != "forest.component-worker-host"
                or worker_host_identity["trust_level"] != "official"
            ):
                raise ForestError(
                    "WORKER_HOST_BOOTSTRAP_INVALID",
                    "Published Worker Host package identity is invalid", 409,
                )
            worker_host_snapshot = await asyncio.to_thread(
                self.package_catalog.get_official_bot,
                worker_host_identity["package_digest"],
            )
            if (
                worker_host_snapshot.component["id"] != "component.worker-host"
                or worker_host_snapshot.manifest["package_id"] != "forest.component-worker-host"
                or worker_host_snapshot.manifest["runtime"] != "python-worker"
            ):
                raise ForestError(
                    "WORKER_HOST_BOOTSTRAP_INVALID",
                    "Published Worker Host package identity is invalid", 409,
                )
        worker_host = await bootstrap_builtin_snapshot(
            worker_host_snapshot,
            self.store,
            self.schema_bundle_installer,
            kernel=self._kernel,
            work_notifier=self.notify,
            initial_configuration=(
                {"protocol": "forest.component-worker-host/1"}
                if bootstrap_settings is not None else None
            ),
        )
        candidate_reader = ComponentSelectionReader(self.store, worker_host.snapshot)
        active_reader = self.selection_reader or candidate_reader
        if active_reader.namespace != candidate_reader.namespace:
            raise ForestError(
                "COMPONENT_SELECTION_BINDING_CHANGED",
                "Selection storage identity cannot change during Graph replacement",
                409,
            )
        if active_reader is candidate_reader:
            selection = await candidate_reader.snapshot()
        else:
            async with active_reader.publication_fence() as selection:
                if await candidate_reader.snapshot() != selection:
                    raise ForestError(
                        "COMPONENT_SELECTION_BINDING_CHANGED",
                        "Candidate selection query differs from the active selection ledger",
                        409,
                    )
        async def resolve_embedding_secret(secret_ref, _revision):
            return await self._resolve_effect_credential(
                secret_ref, purpose="embedding.multimodal.credential",
                initial_ref=initial_settings.embedding_credential_ref if initial_settings else "",
                initial_value=initial_settings.embedding_api_key if initial_settings else "",
                error_code="EMBEDDING_CREDENTIAL_UNAVAILABLE",
            )

        embedding_driver_options = {}
        if self._embedding_client_factory is not None:
            embedding_driver_options["client_factory"] = self._embedding_client_factory
        embedding_effect_factory = multimodal_embedding_effect_driver_factory(
            effects=self._kernel.effect,
            payloads=self._kernel.payload,
            runtime=self._kernel.runtime,
            credential_resolver=resolve_embedding_secret,
            **embedding_driver_options,
        )

        async def resolve_vector_index_token(secret_ref, _revision):
            return await self._resolve_effect_credential(
                secret_ref, purpose="vector.index.credential",
                initial_ref=initial_settings.vector_index_credential_ref if initial_settings else "",
                initial_value=initial_settings.vector_index_token if initial_settings else "",
                error_code="VECTOR_INDEX_CREDENTIAL_UNAVAILABLE",
            )

        vector_index_driver_options = {}
        if self._vector_index_client_factory is not None:
            vector_index_driver_options["client_factory"] = self._vector_index_client_factory
        if self._vector_index_audit_client_factory is not None:
            vector_index_driver_options["audit_client_factory"] = (
                self._vector_index_audit_client_factory
            )
        vector_index_effect_factory = milvus_vector_index_effect_driver_factory(
            bot_id=self.store.bot_id,
            runtime=self._kernel.runtime,
            token_resolver=resolve_vector_index_token,
            **vector_index_driver_options,
        )
        media_driver_options = {}
        if self._media_client_factory is not None:
            media_driver_options["client_factory"] = self._media_client_factory
        media_effect_factory = media_image_fetch_effect_driver_factory(
            runtime=self._kernel.runtime,
            payloads=self._kernel.payload,
            **media_driver_options,
        )
        async def resolve_onebot_secret(secret_ref, _configuration_revision):
            return await self._resolve_effect_credential(
                secret_ref, purpose="adapter.onebot.access-token",
                initial_ref=initial_settings.onebot_secret_ref if initial_settings else "",
                initial_value=initial_settings.onebot_access_token if initial_settings else "",
                error_code="ONEBOT_CREDENTIAL_UNAVAILABLE",
            )

        adapter_driver_options = {}
        if self._websocket_client_factory is not None:
            adapter_driver_options["client_factory"] = self._websocket_client_factory
        onebot_effect_factory = onebot_transport_effect_driver_factory(
            effects=self._kernel.effect,
            runtime=self._kernel.runtime,
            credential_resolver=resolve_onebot_secret,
            **adapter_driver_options,
        )
        effect_factories = {
                "chat-completions.http": provider_effect_factory,
                "chat-completions.result-lookup": provider_effect_factory,
                "embedding.multimodal.http": embedding_effect_factory,
                "vector-index.milvus": vector_index_effect_factory,
                "media.public-image": media_effect_factory,
                "onebot.websocket-v11": onebot_effect_factory,
                "kernel.payload-bytes": kernel_payload_json_effect_driver_factory(
                    runtime=self._kernel.runtime,
                    payloads=self._kernel.payload,
                ),
                "kernel.payload-json": kernel_payload_json_effect_driver_factory(
                    runtime=self._kernel.runtime,
                    payloads=self._kernel.payload,
                ),
                "process.native": native_process_effect_driver_factory(
                    runtime=self._kernel.runtime,
                    payloads=self._kernel.payload,
                ),
        }
        source_refs = {kind: "forest.host-native/1" for kind in effect_factories}
        contract_facts = {}
        controller_provider = self._host_driver_controller_provider
        if prepared_host_graph is not None and controller_provider is None:
            raise ForestError(
                "CONTROL_DRIVER_EXPORT_UNAVAILABLE",
                "Future Host exports require a bound Host controller", 503,
            )
        if controller_provider is not None:
            controller = controller_provider()
            if controller is None and prepared_host_graph is not None:
                raise ForestError(
                    "CONTROL_DRIVER_EXPORT_UNAVAILABLE",
                    "Future Host exports require an active Host controller", 503,
                )
            if controller is not None:
                exports = (
                    controller.driver_exports()
                    if prepared_host_graph is None
                    else controller._driver_exports_for(*prepared_host_graph)
                )
                for export in exports:
                    kind = export["kind"]
                    if kind in effect_factories:
                        raise ForestError(
                            "CONTROL_DRIVER_KIND_CONFLICT",
                            "Control driver kind conflicts with a native Host driver", 409,
                        )
                    effect_factories[kind] = control_export_effect_driver_factory(
                        export=export, controller_provider=controller_provider,
                    )
                    source_refs[kind] = export["source_ref"]
                    contract_facts[kind] = export
        automatic_effect_drivers = PackageEffectDriverRegistry(
            effect_factories,
            configuration_validators={
                "process.native": validate_process_executor_configuration,
            },
            source_refs=source_refs,
            contract_facts=contract_facts,
        )
        automatic = []
        expected_packages = bootstrap_package_digests or {}
        capture_slots = asyncio.Semaphore(4)

        async def capture(source):
            async with capture_slots:
                snapshot = await asyncio.to_thread(
                    self.package_catalog.build_official, source, scope="bot"
                )
                return source, snapshot

        captured = await asyncio.gather(
            *(capture(source) for source in auto_install_official_sources("bot")),
            return_exceptions=True,
        )
        for result in captured:
            if isinstance(result, BaseException):
                raise result
        automatic_sources = tuple(captured)
        default_official_snapshots = tuple(
            snapshot for _source, snapshot in automatic_sources
        )
        if isinstance(bootstrap_settings, BotBootstrapSettings):
            profile_components = dict(bootstrap_settings.components)
        elif isinstance(bootstrap_settings, BotSettings):
            profile_components = standalone_creation_components(
                bootstrap_settings, automatic_sources
            )
        else:
            profile_components = None
        if expected_packages:
            captured_ids = set()
            for _source, snapshot in automatic_sources:
                captured_ids.add(snapshot.component["id"])
                expected = expected_packages.get(snapshot.component["id"])
                if expected is not None and snapshot.digest != expected:
                    raise ForestError(
                        "BOOTSTRAP_PACKAGE_DIGEST_MISMATCH",
                        "Bot bootstrap package differs from its frozen profile",
                        409,
                        details={"component_id": snapshot.component["id"]},
                    )
            if set(expected_packages) - captured_ids:
                raise ForestError(
                    "BOOTSTRAP_PACKAGE_MISSING",
                    "A frozen Bot bootstrap package is absent from the install catalog",
                    409,
                )
        if bootstrap_settings is not None and selection.revision == 0:
            selection = await candidate_reader.initialize_defaults(
                default_official_snapshots
            )
        resolved_official = await asyncio.to_thread(
            resolve_official_package_snapshots,
            selection, tuple(snapshot for _source, snapshot in automatic_sources),
            self.package_catalog,
        )
        automatic_sources = tuple(
            (source, snapshot)
            for (source, _default), snapshot in zip(
                automatic_sources, resolved_official, strict=True
            )
        )
        if expected_packages:
            for _source, snapshot in automatic_sources:
                expected = expected_packages.get(snapshot.component["id"])
                if expected is not None and snapshot.digest != expected:
                    raise ForestError(
                        "BOOTSTRAP_PACKAGE_DIGEST_MISMATCH",
                        "Selected Bot bootstrap package differs from its frozen profile",
                        409,
                        details={"component_id": snapshot.component["id"]},
                    )
        projections = (
            await project_bootstrap_profiles(automatic_sources, profile_components)
            if profile_components is not None else {}
        )
        for source, snapshot in automatic_sources:
            if profile_components is not None:
                initial_configuration = await materialize_bootstrap_configuration(
                    snapshot, profile_components,
                    payloads=self._kernel.payload,
                    projections=projections,
                )
            else:
                initial_configuration = None
            provisioned = await bootstrap_builtin_snapshot(
                snapshot,
                self.store,
                self.schema_bundle_installer,
                kernel=self._kernel,
                work_notifier=self.notify,
                initial_configuration=initial_configuration,
                effect_driver_registry=automatic_effect_drivers,
            )
            automatic.append(provisioned.descriptor)
        official_descriptors = (
            worker_host.descriptor,
            *automatic,
        )
        selected = await asyncio.to_thread(
            select_package_members,
            selection, official_descriptors, self.package_catalog,
        )
        installed = []
        for entry in selected.installed:
            provisioned = await bootstrap_installed_package(
                entry.package_digest,
                self.package_catalog,
                self.store,
                self.schema_bundle_installer,
                kernel=self._kernel,
                work_notifier=self.notify,
                initial_configuration=None,
                approved_capabilities=entry.approved_capabilities,
                effect_driver_registry=automatic_effect_drivers,
            )
            if provisioned.descriptor.component_id != entry.component_id:
                raise ForestError(
                    "COMPONENT_SELECTION_ARTIFACT_CHANGED",
                    "Resolved package component differs from its selection",
                    409,
                )
            installed.append(provisioned.descriptor)
        descriptors = self._kernel.descriptors + selected.official + tuple(installed)
        return BotGraphCandidate(
            RuntimeModuleRegistry(self, descriptors, activation_id=activation_id),
            selection,
            official_descriptors,
            candidate_reader,
            default_official_snapshots,
            automatic_effect_drivers,
        )

    async def start(self) -> None:
        gate = self._driver_binding_gate
        if gate is None:
            await self._start_without_driver_gate()
            return
        async with gate:
            await self._start_without_driver_gate()
            try:
                gate.attach(self)
            except BaseException:
                await self._close_without_driver_gate()
                raise

    async def _start_without_driver_gate(self) -> None:
        await self.store.open()
        try:
            self._kernel = await bootstrap_kernel(self.store, self.schema_bundle_installer)
            self.generation_authority = self._kernel.runtime
            self._replacement_lock = asyncio.Lock()
            assembled = await self._assemble_candidate(
                bootstrap_settings=(
                    self.settings if isinstance(self.settings, BotSettings) else None
                )
            )
            registry = assembled.registry
            self._lease = await self._kernel.runtime.claim_host(
                self._host_id, self._incarnation, ttl=_HOST_LEASE_TTL
            )
            # Cold package preparation and graph replacement may block the event
            # loop longer than one renewal interval. Start renewal at claim and
            # retain the same bounded grace period for every renewal.
            self._renewal_task = asyncio.create_task(
                self._renew_lease(), name=f"bot-host-lease:{self.settings.bot_id}"
            )
            async with assembled.selection_reader.publication_fence(
                expected_revision=assembled.selection.revision,
                expected_digest=assembled.selection.digest,
            ):
                await self._admit_worker_capacity(RuntimeWorkerCapacity(
                    active=(),
                    preparing=tuple(
                        (item.component_id, item.worker_limits)
                        for item in registry.descriptors
                        if item.worker_limits is not None
                    ),
                ))
                await registry.start()
            self.registry = registry
            self._official_descriptors = assembled.official_descriptors
            self._official_defaults = assembled.official_defaults
            self._official_effect_drivers = assembled.official_effect_drivers
            self.selection_reader = assembled.selection_reader
            self._replacement_task = asyncio.create_task(
                self._run_graph_replacements(),
                name=f"bot-graph-replacement:{self.settings.bot_id}",
            )
            self._supervision_task = asyncio.create_task(
                self._supervise_owned_tasks(),
                name=f"bot-runtime-supervision:{self.settings.bot_id}",
            )
        except BaseException:
            await self._close_without_driver_gate()
            raise

    async def provision(self) -> tuple[str, ...]:
        """Install module authorities and revision-one configuration without starting I/O."""

        if not isinstance(self.settings, (BotSettings, BotBootstrapSettings)):
            raise ForestError(
                "BOT_BOOTSTRAP_SETTINGS_REQUIRED",
                "READY Bot launch identity cannot provision initial configuration", 409,
            )
        if self.registry is not None or self._lease is not None:
            raise ForestError("BOT_HOST_ALREADY_ACTIVE", "Active Bot host cannot provision", 409)
        await self.store.open()
        try:
            self._kernel = await bootstrap_kernel(self.store, self.schema_bundle_installer)
            assembled = await self._assemble_candidate(
                bootstrap_package_digests=dict(self.settings.bootstrap_package_digests),
                bootstrap_settings=self.settings,
            )
            return tuple(
                descriptor.component_id for descriptor in assembled.registry.descriptors
            )
        finally:
            await self.store.close()

    async def preview_component_change(
        self,
        *,
        component_id: str,
        source_kind: str,
        package_digest: str | None,
        manifest_digest: str | None,
        approved_capabilities: tuple[str, ...],
        desired_enabled: bool,
        expected_selection_revision: int,
        expected_graph_revision: int,
        expected_configuration_revision: int | None,
    ) -> SelectionGraphPreview:
        """Project a selection write, then inspect its candidate without writing it."""

        if expected_configuration_revision is not None and (
            source_kind != "installed" or not desired_enabled
        ):
            raise ForestError(
                "MODULE_CONFIGURATION_REVISION_REQUIRED",
                "Only an enabled installed package accepts a target configuration revision",
                422,
            )
        lock = self._replacement_lock
        if lock is None or self.registry is None or self._closing:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
        async with lock:
            if self.registry is None or self._closing or not self._official_descriptors:
                raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
            async with self.selection_reader.publication_fence() as current:
                # Establish the configuration snapshot only after the selection fence.
                if current.revision != expected_selection_revision:
                    raise ForestError(
                        "COMPONENT_SELECTION_REVISION_CONFLICT",
                        "Component selection changed before preview",
                        409,
                    )
                async with self.store.pool.connection() as conn, conn.transaction():
                    await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    return await project_selection_graph_in(
                        self.registry,
                        self.selection_reader,
                        self._official_descriptors,
                        self.package_catalog,
                        self.store,
                        kernel=self._kernel,
                        conn=conn,
                        official_defaults=self._official_defaults,
                        official_effect_drivers=self._official_effect_drivers,
                        component_id=component_id,
                        source_kind=source_kind,
                        package_digest=package_digest,
                        manifest_digest=manifest_digest,
                        approved_capabilities=approved_capabilities,
                        desired_enabled=desired_enabled,
                        expected_selection_revision=current.revision,
                        expected_graph_revision=expected_graph_revision,
                        expected_configuration_revision=expected_configuration_revision,
                    )

    async def replace_runtime_graph(
        self,
        *,
        activation_id: str | None = None,
        expected_graph_revision: int | None = None,
        target_component_id: str | None = None,
        selection_revision: int | None = None,
        selection_digest: str | None = None,
    ) -> RuntimeModuleRegistry:
        options = {
            "activation_id": activation_id,
            "expected_graph_revision": expected_graph_revision,
            "target_component_id": target_component_id,
            "selection_revision": selection_revision,
            "selection_digest": selection_digest,
        }
        gate = self._driver_binding_gate
        if gate is None:
            return await self._replace_without_driver_gate(**options)
        async with gate:
            result = await self._replace_without_driver_gate(**options)
            gate.refresh(self)
            return result

    async def _replace_without_driver_gate(
        self,
        *,
        activation_id: str | None = None,
        expected_graph_revision: int | None = None,
        target_component_id: str | None = None,
        selection_revision: int | None = None,
        selection_digest: str | None = None,
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
            assembled = (
                await self._assemble_candidate()
                if activation_id is None
                else await self._assemble_candidate(activation_id)
            )
            if (
                selection_revision is not None
                and (
                    assembled.selection.revision != selection_revision
                    or assembled.selection.digest != selection_digest
                )
            ):
                raise ForestError(
                    "COMPONENT_SELECTION_CHANGED",
                    "Frozen selection changed before candidate assembly",
                    409,
                )
            candidate = assembled.registry
            membership_changed = set(candidate.states) != set(previous.states)
            if membership_changed or target_component_id is not None:
                capacity = candidate.preview_graph_transition(
                    previous, target_component_id=target_component_id
                ).worker_capacity
            else:
                capacity = RuntimeWorkerCapacity(
                    active=tuple(
                        (item.component_id, item.worker_limits)
                        for item in previous.descriptors
                        if item.worker_limits is not None
                    ),
                    preparing=tuple(
                        (item.component_id, item.worker_limits)
                        for item in candidate.descriptors
                        if item.worker_limits is not None
                    ),
                )
            affected = (
                candidate.plan_graph_transition(
                    previous, target_component_id=target_component_id
                )
                if membership_changed
                else (
                    candidate.plan_retained_generations(previous, target_component_id)
                    if target_component_id is not None
                    else None
                )
            )
            previous_drained = False
            try:
                await self._admit_worker_capacity(capacity)
                await candidate.prewarm()
                if affected is None or affected:
                    await previous.drain_owned_work_for_replacement(
                        candidate.activation_id,
                        components=(
                            affected
                            if affected is not None and len(affected) < len(previous.order)
                            else None
                        ),
                        candidate_components=candidate.prepared_replacement_facts(),
                    )
                    previous_drained = True
                async with self.selection_reader.publication_fence(
                    expected_revision=assembled.selection.revision,
                    expected_digest=assembled.selection.digest,
                ):
                    await self._admit_worker_capacity(capacity)
                    await candidate.start()
                    candidate.accept_retained_ownership()
            except BaseException as replacement_error:
                publication_committed = bool(
                    getattr(candidate, "publication_committed", False)
                )
                if (
                    previous_drained
                    and not publication_committed
                    and getattr(candidate, "graph_identity", None) is not None
                ):
                    try:
                        evidence = await self._kernel.runtime.graph_for_activation(
                            candidate.activation_id
                        )
                        publication_committed = evidence is not None and evidence[
                            "state"
                        ] in {"ACTIVE", "RETIRED", "STOPPED"}
                    except BaseException:
                        # A lost publication acknowledgement cannot authorize
                        # reopening the old graph while the DB outcome is unknown.
                        publication_committed = True
                cleanup_error = None
                try:
                    cleanup_errors = await candidate.close()
                    if cleanup_errors:
                        cleanup_error = cleanup_errors[0]
                except BaseException as exc:
                    cleanup_error = exc
                if publication_committed:
                    failure = ForestError(
                        "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE",
                        "The published graph failed local activation; the Bot host must stop",
                        503,
                        details={
                            "activation_error": getattr(
                                replacement_error, "code", type(replacement_error).__name__
                            ),
                            "cleanup_error": (
                                getattr(cleanup_error, "code", type(cleanup_error).__name__)
                                if cleanup_error is not None
                                else None
                            ),
                        },
                    )
                    self._fatal_replacement_error = failure
                    raise failure from replacement_error
                if (
                    isinstance(replacement_error, ForestError)
                    and replacement_error.code
                    == "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED"
                ):
                    self._fatal_replacement_error = replacement_error
                    raise replacement_error
                if previous_drained:
                    try:
                        await previous.resume_owned_work_after_failed_replacement()
                    except BaseException as resume_error:
                        failure = ForestError(
                            "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED",
                            "Candidate publication failed and the previous graph could not resume",
                            503,
                        )
                        self._fatal_replacement_error = failure
                        raise failure from resume_error
                if cleanup_error is not None:
                    failure = ForestError(
                        "RUNTIME_GRAPH_REPLACEMENT_CLEANUP_FAILED",
                        "Candidate publication failed and its resources did not close cleanly",
                        503,
                    )
                    self._fatal_replacement_error = failure
                    raise failure from cleanup_error
                raise replacement_error
            self.registry = candidate
            self._official_descriptors = assembled.official_descriptors
            self._official_defaults = assembled.official_defaults
            self._official_effect_drivers = assembled.official_effect_drivers
            self.selection_reader = assembled.selection_reader
            errors = await previous.close()
            if errors:
                failure = ForestError(
                    "RUNTIME_GRAPH_DRAIN_FAILED",
                    "The replacement graph is active but the superseded graph did not drain",
                    503,
                    details={"errors": len(errors)},
                )
                self._fatal_replacement_error = failure
                raise failure
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
                if isinstance(exc, ForestError) and exc.code in {
                    "RUNTIME_GRAPH_POST_PUBLICATION_FAILURE",
                    "RUNTIME_GRAPH_DRAIN_FAILED",
                    "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED",
                    "RUNTIME_GRAPH_REPLACEMENT_CLEANUP_FAILED",
                }:
                    await self._kernel.runtime.fail_graph_replacement(
                        self._authority(),
                        operation_id=request["operation_id"],
                        error_code=exc.code,
                    )
                    raise
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
            target_component_id=request.get("target_component_id"),
            selection_revision=request.get("selection_revision"),
            selection_digest=request.get("selection_digest"),
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
            self._lease = await self._kernel.runtime.claim_host(
                self._host_id, self._incarnation, ttl=_HOST_LEASE_TTL
            )

    async def _supervise_owned_tasks(self) -> None:
        """Fail the Bot host when a task still owned by its active graph stops."""

        while not self._closing:
            failure = getattr(self, "_fatal_replacement_error", None)
            if failure is not None:
                raise failure
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
        gate = self._driver_binding_gate
        if gate is None:
            await self._close_without_driver_gate()
            return
        async with gate:
            await self._close_without_driver_gate()
            gate.detach(self)

    async def _close_without_driver_gate(self) -> None:
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


async def run_bot(settings: BotSettings) -> None:
    host = BotHost(settings)
    stop = asyncio.Event()
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
        graph = host.registry.graph_identity
        status = {
            "status": "active",
            "bot_id": settings.bot_id,
            "graph_id": graph.graph_id,
            "components": sorted(graph.generations),
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
            await host.close()
        finally:
            if status_file:
                Path(status_file).unlink(missing_ok=True)
