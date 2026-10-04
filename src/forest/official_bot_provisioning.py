"""Official Bot bootstrap profile projection and no-I/O authority materialization."""

from __future__ import annotations

import tomllib
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

import psycopg
from jsonschema import Draft202012Validator
from psycopg_pool import PoolTimeout

from forest.bootstrap_contributions import (
    BootstrapConfigurationContribution,
    BootstrapConfigurationRegistry,
    BootstrapPackageSurface,
)
from forest.bot_host import BotBootstrapSettings, BotHost, BotRuntimeSettings
from forest.builtin_packages import (
    official_builtin_configuration_schema,
)
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.model_contract import ModelCapabilities
from forest.packages import PackageCatalog, auto_install_official_sources
from forest.runtime_modules import RuntimeManagementContribution
from forest.storage.bot_provisioning_receipt import BotProvisioningReceiptAuthority
from forest.storage.bot_store import BotStore

PROVIDER_CONFIGURATION_SCHEMA = official_builtin_configuration_schema("provider_chat_completions")
EMBEDDING_CONFIGURATION_SCHEMA = official_builtin_configuration_schema("embedding_multimodal")
VECTOR_INDEX_CONFIGURATION_SCHEMA = official_builtin_configuration_schema("vector_index_milvus")


def _validated_package_configuration(
    schema: dict, value: object, code: str, message: str
) -> dict:
    if not Draft202012Validator(schema).is_valid(value):
        raise ForestError(code, message, 422)
    return value


def validate_provider_configuration(value: object) -> tuple[dict, ModelCapabilities]:
    configuration = _validated_package_configuration(
        PROVIDER_CONFIGURATION_SCHEMA, value,
        "PROVIDER_CONFIGURATION_INVALID", "Provider configuration is invalid",
    )
    try:
        capabilities = ModelCapabilities(**configuration["capabilities"])
    except (KeyError, TypeError, ForestError) as exc:
        raise ForestError(
            "PROVIDER_CONFIGURATION_INVALID", "Provider capabilities are invalid", 422
        ) from exc
    return {**configuration, "capabilities": capabilities.as_dict()}, capabilities


def validate_embedding_configuration(value: object) -> tuple[dict, EmbeddingProfile]:
    configuration = _validated_package_configuration(
        EMBEDDING_CONFIGURATION_SCHEMA, value,
        "EMBEDDING_CONFIGURATION_INVALID", "Embedding configuration is invalid",
    )
    try:
        profile = EmbeddingProfile(**configuration["profile"])
    except (KeyError, TypeError, ValueError, ForestError) as exc:
        raise ForestError(
            "EMBEDDING_CONFIGURATION_INVALID", "Embedding profile is invalid", 422
        ) from exc
    return configuration, profile


def validate_vector_index_configuration(value: object) -> dict:
    return _validated_package_configuration(
        VECTOR_INDEX_CONFIGURATION_SCHEMA, value,
        "VECTOR_INDEX_CONFIGURATION_INVALID", "Vector index configuration is invalid",
    )

def validate_shell_configuration(value: object) -> dict:
    if not Draft202012Validator(
        official_builtin_configuration_schema("shell_command")
    ).is_valid(value):
        raise ForestError(
            "SHELL_CONFIGURATION_INVALID", "Shell configuration is invalid", 422
        )
    return {
        **value,
        "administrator_principals": tuple(value["administrator_principals"]),
    }


def validate_trigger_policy_configuration(value: object) -> dict:
    if not Draft202012Validator(
        official_builtin_configuration_schema("trigger_policy")
    ).is_valid(value):
        raise ForestError(
            "TRIGGER_POLICY_CONFIGURATION_INVALID",
            "Trigger policy configuration is invalid",
            422,
        )
    return {
        **value,
        "allowed_group_ids": tuple(value["allowed_group_ids"]),
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


def official_bootstrap_contributions(
    package_catalog: PackageCatalog | None = None,
) -> tuple[BootstrapConfigurationContribution, ...]:
    """Capture creation-time declarations from the exact builtin package artifacts."""

    if package_catalog is None:
        with TemporaryDirectory(prefix="forest-bootstrap-") as directory:
            return official_bootstrap_contributions(PackageCatalog(Path(directory)))
    contributions = []
    for source in auto_install_official_sources("bot"):
        manifest = tomllib.loads((source / "module.toml").read_text(encoding="utf-8"))
        if "bootstrap_contribution" not in manifest["components"][0]:
            continue
        snapshot = package_catalog.build(source)
        captured = snapshot.bootstrap_contribution()
        if captured is None:
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_CONTRIBUTION_CHANGED",
                "Creation-time contribution changed while capturing its package",
                409,
            )
        declaration, schema, initial_value = captured
        component_id = snapshot.component["id"]
        label = declaration["label"]
        ui = snapshot.bootstrap_ui()
        surface = (
            _bootstrap_surface(component_id, label)
            if ui is None
            else BootstrapPackageSurface(
                component_id, snapshot.digest, ui[1]
            )
        )
        contributions.append(
            BootstrapConfigurationContribution(
                component_id=component_id,
                label=label,
                description=declaration["description"],
                schema=schema,
                initial_value=initial_value,
                surface=surface,
                package_digest=snapshot.digest,
                order=declaration["order"],
                secret_refs=tuple(
                    (item["field"], item["purpose"], item.get("allow_empty", False))
                    for item in declaration.get("secret_refs", ())
                ),
                origins=tuple(
                    (item["field"], tuple(item["schemes"]))
                    for item in declaration.get("origins", ())
                ),
            )
        )
    return tuple(contributions)


def official_bootstrap_schema_digests(
    registry: BootstrapConfigurationRegistry | None = None,
) -> dict[str, str]:
    registry = registry or BootstrapConfigurationRegistry(official_bootstrap_contributions())
    return registry.schema_digests()


def validate_official_bootstrap_components(
    components: object, *, registry: BootstrapConfigurationRegistry | None = None,
) -> dict[str, dict]:
    registry = registry or BootstrapConfigurationRegistry(official_bootstrap_contributions())
    schemas = {item.component_id: item.schema for item in registry.contributions}
    if not isinstance(components, dict) or set(components) != set(schemas):
        raise ForestError(
            "OFFICIAL_BOOTSTRAP_COMPONENT_SET_INVALID",
            "Official Bot bootstrap component set differs",
            422,
        )
    for component_id, schema in schemas.items():
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
    """Validate a frozen Profile and pass its inputs to package-owned materializers."""

    def __init__(
        self, *, secrets, bootstrap_registry: BootstrapConfigurationRegistry | None = None,
    ) -> None:
        self.secrets = secrets
        self.bootstrap_registry = bootstrap_registry or BootstrapConfigurationRegistry(
            official_bootstrap_contributions()
        )

    async def resolve(self, *, profile, storage) -> BotBootstrapSettings:
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
            {component_id: item.config for component_id, item in components.items()},
            registry=self.bootstrap_registry,
        )
        expected_digests = official_bootstrap_schema_digests(self.bootstrap_registry)
        expected_packages = self.bootstrap_registry.package_digests()
        for component_id, item in components.items():
            if item.schema_digest != expected_digests[component_id]:
                raise ForestError(
                    "OFFICIAL_BOOTSTRAP_SCHEMA_MISMATCH",
                    "Bootstrap component schema differs from the installed module",
                    409,
                    details={"component_id": component_id},
                )
            if item.package_digest != expected_packages[component_id]:
                raise ForestError(
                    "OFFICIAL_BOOTSTRAP_PACKAGE_MISMATCH",
                    "Bootstrap package differs from the captured install catalog",
                    409,
                    details={"component_id": component_id},
                )

        provider_seed = components["provider.chat-completions"].config
        capability_fields = {
            "deployment_id",
            "model",
            "capability_revision",
            "model_revision",
            "input_modalities",
            "supports_tools",
            "supports_streaming",
            "stream_usage",
            "max_output_tokens",
            "image_roles",
            "image_detail",
            "system_role",
            "output_token_parameter",
            "tool_image_mapping",
        }
        validate_provider_configuration(
            {
                **provider_seed,
                "capabilities": {
                    key: value
                    for key, value in provider_seed["capabilities"].items()
                    if key in capability_fields
                },
            }
        )
        _embedding, embedding_profile = validate_embedding_configuration(
            components["embedding.multimodal"].config
        )
        vector_index = validate_vector_index_configuration(
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
        validate_trigger_policy_configuration(
            components["conversation.trigger-policy"].config
        )
        shell = validate_shell_configuration(components["shell.command"].config)
        onebot = components["adapter.onebot"].config
        if shell["binding_ref"] != onebot["binding_id"]:
            raise ForestError(
                "OFFICIAL_BOOTSTRAP_BINDING_MISMATCH",
                "Shell and OneBot binding references differ",
                409,
            )
        for contribution in self.bootstrap_registry.contributions:
            value = components[contribution.component_id].config
            for field, schemes in contribution.origins:
                self._origin(value[field], set(schemes), f"{contribution.component_id}.{field}")
            for field, purpose, allow_empty in contribution.secret_refs:
                secret_ref = value[field]
                if secret_ref or not allow_empty:
                    await self._verify_secret(secret_ref, purpose)
        return BotBootstrapSettings(
            bot_id=profile.bot_id,
            dsn=storage.dsn,
            components=tuple(sorted(
                (component_id, deepcopy(item.config))
                for component_id, item in components.items()
            )),
            bootstrap_package_digests=tuple(sorted(
                (component_id, item.package_digest)
                for component_id, item in components.items()
            )),
        )

    async def _verify_secret(self, secret_ref: str, purpose: str) -> None:
        try:
            await self.secrets.resolve_text_for_purpose(secret_ref, purpose)
        except ForestError as error:
            if error.code == "SECRET_PURPOSE_CONFLICT":
                raise ForestError(
                    "OFFICIAL_BOOTSTRAP_SECRET_PURPOSE_INVALID",
                    "Bootstrap secret purpose differs",
                    409,
                ) from error
            raise

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

    def __init__(self, *, bootstrap_profiles) -> None:
        self.bootstrap_profiles = bootstrap_profiles

    async def __call__(self, bot: dict, storage) -> BotRuntimeSettings:
        profile = await self.bootstrap_profiles.load(bot["bootstrap_profile_ref"])
        if profile.manifest_digest != bot["bootstrap_manifest_digest"]:
            raise ForestError(
                "BOOTSTRAP_PROFILE_TAMPERED",
                "Ready Bot bootstrap manifest differs",
                409,
            )
        if (
            profile.bot_id != bot["bot_id"]
            or profile.bot_id != storage.bot_id
            or profile.storage_profile_ref != storage.profile_ref
            or profile.storage_generation != storage.storage_generation
        ):
            raise ForestError(
                "BOOTSTRAP_PROFILE_SCOPE_MISMATCH",
                "Bootstrap profile differs from resolved Bot storage",
                409,
            )
        return BotRuntimeSettings(bot_id=profile.bot_id, dsn=storage.dsn)


class OfficialBotProvisioner:
    """Materialize official module authorities without starting the runtime graph."""

    def __init__(
        self, *, settings_resolver, host_factory=BotHost, database_allocator=None
    ) -> None:
        self.settings_resolver = settings_resolver
        self.host_factory = host_factory
        self.database_allocator = database_allocator

    async def read_materialization_receipt(self, *, storage) -> dict | None:
        if self.database_allocator is not None:
            self.database_allocator.validate_target(storage.bot_id, storage.dsn)
        receipt_store = BotStore(storage.dsn, storage.bot_id)
        try:
            await receipt_store.open()
            return await BotProvisioningReceiptAuthority(receipt_store).read()
        except (psycopg.Error, PoolTimeout) as exc:
            raise ForestError(
                "PROVISIONING_PROOF_UNAVAILABLE",
                "Bot materialization proof could not be read", 503,
            ) from exc
        finally:
            await receipt_store.close()

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
        await host.provision()
        receipt_store = BotStore(storage.dsn, claim.bot_id)
        try:
            await receipt_store.open()
            await BotProvisioningReceiptAuthority(receipt_store).record(
                claim=claim, storage=storage, profile=profile,
            )
        finally:
            await receipt_store.close()
