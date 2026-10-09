"""Official Bot bootstrap profile projection and no-I/O authority materialization."""

from __future__ import annotations

import tomllib
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

import psycopg
from psycopg_pool import PoolTimeout

from forest.bootstrap_contributions import (
    BootstrapConfigurationContribution,
    BootstrapConfigurationRegistry,
    BootstrapPackageSurface,
)
from forest.bot_host import BotBootstrapSettings, BotHost, BotRuntimeSettings
from forest.errors import ForestError
from forest.packages import PackageCatalog, auto_install_official_sources
from forest.runtime_modules import RuntimeManagementContribution
from forest.storage.bot_provisioning_receipt import BotProvisioningReceiptAuthority
from forest.storage.bot_store import BotStore


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
        self.bootstrap_registry.validate_components(
            {component_id: item.config for component_id, item in components.items()}
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
            await self.secrets.verify_reference_for_purpose(secret_ref, purpose)
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
        self, *, settings_resolver, host_factory=BotHost
    ) -> None:
        self.settings_resolver = settings_resolver
        self.host_factory = host_factory

    async def read_materialization_receipt(self, *, storage) -> dict | None:
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
