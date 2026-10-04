"""Provision immutable official and installed package Generations."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator
from psycopg.errors import InvalidSchemaName, SerializationFailure, UndefinedTable

from forest.errors import ForestError
from forest.package_domain_authority import PostgresPackageDomainAuthority
from forest.package_effects import (
    EMPTY_PACKAGE_EFFECT_DRIVERS,
    PackageEffectDriverRegistry,
)
from forest.package_management import PackageManagementContract
from forest.package_payload_ports import KernelPackagePayloadAuthority
from forest.package_runtime import package_runtime_descriptor
from forest.package_service_ledger import KernelPackageServiceOperationLedger
from forest.package_state_ports import (
    KernelPackageStateCommands,
    KernelPackageStateQueries,
)
from forest.packages import (
    PackageCatalog,
    PackageSnapshot,
    contract_matches,
    contract_range,
    digest_json,
    storage_bundle_digest,
)
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    RuntimePackageManagementContribution,
    RuntimeStorageBundle,
    configuration_management_contribution,
)
from forest.schema_bundles import StorageBundleSpec
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import (
    EffectiveModuleConfiguration,
    ModuleConfigurationRepository,
)
from forest.unit_of_work import (
    PostgresComponentConfigurationApplyCoordinator,
    package_uow_coordinator_factory,
)
from forest.worker_limits import WorkerLimits


@dataclass(frozen=True)
class ProvisionedPackageModule:
    snapshot: PackageSnapshot
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


@dataclass(frozen=True)
class PackageCandidateInspection:
    """Verified Bot package and configuration facts, without provisioning."""

    component_id: str
    trust_level: str
    package_digest: str
    manifest_digest: str
    configuration_revision: int
    configuration_payload_id: str
    configuration_payload_digest: str
    configuration_schema_digest: str
    storage_digest: str | None
    worker_limits: WorkerLimits

    def as_fact(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "trust_level": self.trust_level,
            "package_digest": self.package_digest,
            "manifest_digest": self.manifest_digest,
            "configuration_revision": self.configuration_revision,
            "configuration_payload_id": self.configuration_payload_id,
            "configuration_payload_digest": self.configuration_payload_digest,
            "configuration_schema_digest": self.configuration_schema_digest,
            "storage_digest": self.storage_digest,
            "worker_limits": self.worker_limits.as_fact(),
        }


@dataclass(frozen=True)
class _InstalledPackageLayout:
    schema: dict[str, Any]
    schema_digest: str
    configuration_storage: RuntimeStorageBundle
    storage: RuntimeStorageBundle | None


def _package_json_reference(value: object) -> str:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[a-zA-Z0-9_./-]+\.json", value)
        or PurePosixPath(value).is_absolute()
        or ".." in PurePosixPath(value).parts
    ):
        raise ValueError("Official package JSON reference is invalid")
    return value


def _official_bootstrap_source(package_name: str):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", package_name):
        raise ValueError("Invalid official package name")
    source = files("forest").joinpath("builtin", package_name)
    manifest = tomllib.loads(source.joinpath("module.toml").read_text(encoding="utf-8"))
    component = manifest["components"][0]
    return (
        source,
        _package_json_reference(component.get("bootstrap_config")),
        _package_json_reference(component.get("config_schema")),
    )


def auto_install_builtin_sources() -> tuple[Path, ...]:
    """Discover official packages admitted to manifest-driven Bot assembly."""

    root = files("forest").joinpath("builtin")
    selected = []
    for source in root.iterdir():
        manifest_file = source.joinpath("module.toml")
        if not source.is_dir() or not manifest_file.is_file():
            continue
        manifest = tomllib.loads(manifest_file.read_text(encoding="utf-8"))
        components = manifest.get("components", ())
        if len(components) == 1 and components[0].get("auto_install") is True:
            selected.append((components[0]["id"], Path(str(source))))
    return tuple(source for _, source in sorted(selected))


def official_builtin_configuration(package_name: str, *, overrides: dict | None = None) -> dict:
    """Read an official package's declarative bootstrap seed, before Bot assembly."""

    source, reference, config_schema = _official_bootstrap_source(package_name)
    value = json.loads(source.joinpath(reference).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("Official bootstrap configuration must be a JSON object")
    value.update(overrides or {})
    json.dumps(value, allow_nan=False)
    schema = json.loads(source.joinpath(config_schema).read_text(encoding="utf-8"))
    if not Draft202012Validator(schema).is_valid(value):
        raise ValueError("Official bootstrap configuration violates its package schema")
    return value


def official_builtin_configuration_schema(package_name: str) -> dict:
    """Read the same package schema used for its frozen Generation."""

    source, _reference, config_schema = _official_bootstrap_source(package_name)
    return json.loads(source.joinpath(config_schema).read_text(encoding="utf-8"))


def _required_service_contracts(
    snapshot: PackageSnapshot, overrides: dict[str, str] | None
) -> dict[str, str]:
    declared = {item["name"]: item for item in snapshot.component.get("requires", ())}
    if overrides == {}:
        # Isolated package harnesses may deliberately omit peer providers.
        # Production assembly passes None and binds the manifest set.
        return {}
    supplied = dict(overrides or {})
    if set(supplied) - set(declared):
        raise ForestError(
            "PACKAGE_DEPENDENCY_BINDING_INVALID",
            "A service binding is absent from the package manifest",
            422,
        )
    result = {}
    for name, dependency in declared.items():
        lower, _upper = contract_range(dependency["contract"])
        selected = supplied.get(name, f"{lower[0]}.{lower[1]}")
        if not contract_matches(dependency["contract"], selected):
            raise ForestError(
                "PACKAGE_DEPENDENCY_BINDING_INVALID",
                "A service binding violates the package contract range",
                422,
            )
        result[name] = selected
    return result


def _configuration_storage(snapshot: PackageSnapshot) -> RuntimeStorageBundle:
    """Attach the standard configuration ledger to one official package namespace."""

    component_id = snapshot.component["id"]
    common = files("forest").joinpath("storage/official/module_configuration.sql").read_bytes()
    declared = snapshot.storage_bundle()
    if declared is None:
        schema_id = snapshot.manifest["package_id"]
        schema_version = "1.0.0"
        repository_contract = "forest.module-configuration.repository/1"
        owned = b""
    else:
        metadata, owned = declared
        schema_id = metadata["schema_id"]
        schema_version = metadata["schema_version"]
        repository_contract = metadata["repository_contract"]
    baseline = common + (b"\n" + owned if owned else b"")
    baseline_sha256 = hashlib.sha256(baseline).hexdigest()
    spec = StorageBundleSpec(
        component_id,
        schema_id,
        schema_version,
        baseline_sha256,
        repository_contract,
    )
    storage_digest = storage_bundle_digest(spec, "official")
    return RuntimeStorageBundle(
        spec,
        baseline,
        storage_digest,
        execution_profile="official",
    )


def _installed_configuration_storage(component_id: str) -> RuntimeStorageBundle:
    """Keep trusted configuration SQL outside an installed package's SQL bundle."""

    baseline = files("forest").joinpath("storage/official/module_configuration.sql").read_bytes()
    schema_id = "forest.configuration." + hashlib.sha256(component_id.encode()).hexdigest()[:32]
    spec = StorageBundleSpec(
        component_id,
        schema_id,
        "1.0.0",
        hashlib.sha256(baseline).hexdigest(),
        "forest.module-configuration.repository/1",
    )
    return RuntimeStorageBundle(
        spec,
        baseline,
        storage_bundle_digest(spec, "official"),
        execution_profile="official",
    )


def _installed_domain_storage(snapshot: PackageSnapshot) -> RuntimeStorageBundle | None:
    declared = snapshot.storage_bundle()
    if declared is None:
        return None
    metadata, baseline = declared
    if metadata["execution_profile"] != "declarative":
        raise ForestError(
            "PACKAGE_STORAGE_TRUST_REQUIRED",
            "Installed package SQL must use the declarative profile",
            403,
        )
    spec = StorageBundleSpec(
        snapshot.component["id"],
        metadata["schema_id"],
        metadata["schema_version"],
        metadata["baseline_sha256"],
        metadata["repository_contract"],
    )
    return RuntimeStorageBundle(
        spec,
        baseline,
        storage_bundle_digest(spec, "declarative"),
        execution_profile="declarative",
    )


def _installed_package_layout(
    snapshot: PackageSnapshot,
    approved_capabilities: tuple[str, ...] | None,
    initial_configuration: dict[str, Any] | None = None,
) -> _InstalledPackageLayout:
    component = snapshot.component
    if component["scope"] != "bot":
        raise ForestError(
            "PACKAGE_SCOPE_UNSUPPORTED",
            "Bot package assembly requires a Bot-scoped component",
            422,
        )
    requested = frozenset(component.get("capabilities", {}).get("requests", ()))
    if (
        not isinstance(approved_capabilities, tuple)
        or any(not isinstance(item, str) for item in approved_capabilities)
        or len(approved_capabilities) != len(set(approved_capabilities))
        or frozenset(approved_capabilities) != requested
    ):
        raise ForestError(
            "PACKAGE_CAPABILITY_APPROVAL_REQUIRED",
            "Installed package capabilities require an exact approved set",
            403,
        )
    if snapshot.manifest["runtime"] != "python-worker":
        raise ForestError(
            "PACKAGE_RUNTIME_UNSUPPORTED",
            "Installed package requires the Python Worker runtime",
            422,
        )
    schema = snapshot.schema(component["config_schema"])
    if initial_configuration is not None and not Draft202012Validator(schema).is_valid(
        initial_configuration
    ):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID",
            "Initial package configuration violates its declared schema",
            422,
        )
    storage = _installed_domain_storage(snapshot)
    if any(
        component.get(name)
        for name in (
            "domain_queries", "domain_commands", "unit_of_work_participants",
            "owned_work_handlers",
        )
    ):
        raise ForestError(
            "PACKAGE_DOMAIN_AUTHORITY_UNAVAILABLE",
            "Installed package SQL procedures require a separate reviewed authority",
            422,
        )
    if component.get("effect_services"):
        raise ForestError(
            "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
            "Installed package has no approved host effect driver binding",
            422,
        )
    configuration_storage = _installed_configuration_storage(component["id"])
    if storage is not None and storage.spec.schema_id == configuration_storage.spec.schema_id:
        raise ForestError(
            "PACKAGE_STORAGE_IDENTITY_CONFLICT",
            "Installed package storage collides with its configuration ledger",
            422,
        )
    return _InstalledPackageLayout(
        schema, digest_json(schema), configuration_storage, storage
    )


async def _installed_desired(
    layout: _InstalledPackageLayout,
    configuration: ModuleConfigurationRepository,
    *,
    expected_revision: int | None = None,
    conn=None,
) -> EffectiveModuleConfiguration:
    try:
        desired = (
            await configuration.desired_checked_in(conn)
            if conn is not None else await configuration.desired_checked()
        )
    except ForestError as error:
        if error.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        raise ForestError(
            "PACKAGE_CONFIGURATION_REQUIRED",
            "Installed package configuration must be initialized before selection",
            409,
        ) from error
    except (InvalidSchemaName, UndefinedTable) as error:
        raise ForestError(
            "PACKAGE_CONFIGURATION_REQUIRED",
            "Installed package configuration must be initialized before selection",
            409,
        ) from error
    except SerializationFailure as error:
        raise ForestError(
            "MODULE_CONFIGURATION_REVISION_CONFLICT",
            "Target package configuration changed during inspection",
            409,
        ) from error
    if expected_revision is not None and desired.revision != expected_revision:
        raise ForestError(
            "MODULE_CONFIGURATION_REVISION_CONFLICT",
            "Target package configuration revision changed",
            409,
        )
    if desired.validation_digest != layout.schema_digest or not Draft202012Validator(
        layout.schema
    ).is_valid(desired.value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
            "Installed package configuration was not validated for this artifact schema",
            409,
        )
    return desired


async def _official_desired(
    configuration: ModuleConfigurationRepository,
    schema: dict,
    schema_digest: str,
    validator: Callable | None,
) -> EffectiveModuleConfiguration:
    try:
        desired = await configuration.desired_checked()
    except ForestError as error:
        if error.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        raise ForestError(
            "PACKAGE_CONFIGURATION_REQUIRED",
            "Official package configuration must be initialized before runtime assembly",
            409,
        ) from error
    except (InvalidSchemaName, UndefinedTable) as error:
        raise ForestError(
            "PACKAGE_CONFIGURATION_REQUIRED",
            "Official package configuration must be initialized before runtime assembly",
            409,
        ) from error
    if (
        desired.validation_digest != schema_digest
        or not Draft202012Validator(schema).is_valid(desired.value)
    ):
        raise ForestError(
            "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
            "Official package configuration was not validated for this artifact schema",
            409,
            details={"component_id": configuration.component_id},
        )
    if validator is not None:
        await validator(desired.value)
    return desired


async def _inspect_installed_snapshot(
    snapshot: PackageSnapshot,
    store,
    *,
    kernel,
    approved_capabilities: tuple[str, ...],
    expected_configuration_revision: int | None = None,
    conn=None,
) -> tuple[PackageCandidateInspection, _InstalledPackageLayout, ModuleConfigurationRepository]:
    layout = _installed_package_layout(snapshot, approved_capabilities)
    configuration = ModuleConfigurationRepository(
        store,
        component_id=snapshot.component["id"],
        namespace=layout.configuration_storage.spec.namespace,
        payloads=kernel.payload,
        immutable_paths=tuple(snapshot.component.get("config_immutable_paths", ())),
    )
    desired = await _installed_desired(
        layout, configuration, expected_revision=expected_configuration_revision,
        conn=conn,
    )
    inspected = PackageCandidateInspection(
        snapshot.component["id"], "installed", snapshot.digest, digest_json(snapshot.manifest),
        desired.revision, desired.payload_id, desired.payload_digest,
        layout.schema_digest,
        layout.storage.package_digest if layout.storage is not None else None,
        WorkerLimits.from_component(snapshot.component),
    )
    return inspected, layout, configuration


def _pinned_installed_snapshot(
    package_digest: str, catalog: PackageCatalog, expected_manifest_digest: str
) -> PackageSnapshot:
    snapshot = catalog.get(package_digest)
    if digest_json(snapshot.manifest) != expected_manifest_digest:
        raise ForestError(
            "COMPONENT_SELECTION_ARTIFACT_CHANGED",
            "Selected package declaration differs from its pinned artifact",
            409,
        )
    return snapshot


async def inspect_installed_package(
    package_digest: str,
    catalog: PackageCatalog,
    store,
    *,
    kernel,
    approved_capabilities: tuple[str, ...],
    expected_manifest_digest: str,
    expected_configuration_revision: int,
) -> PackageCandidateInspection:
    """Read one exact installed candidate without schema, grant, or configuration writes."""

    snapshot = _pinned_installed_snapshot(
        package_digest, catalog, expected_manifest_digest
    )
    inspected, _layout, _configuration = await _inspect_installed_snapshot(
        snapshot,
        store,
        kernel=kernel,
        approved_capabilities=approved_capabilities,
        expected_configuration_revision=expected_configuration_revision,
    )
    return inspected


async def inspect_official_candidate_descriptor(
    descriptor: RuntimeModuleDescriptor,
    catalog: PackageCatalog,
    store,
    *,
    kernel,
    conn=None,
) -> PackageCandidateInspection:
    """Pin an assembled official package to its current desired configuration."""

    artifact = descriptor.artifact
    snapshot = await asyncio.to_thread(catalog.get, artifact.package_digest)
    if (
        artifact.trust_level != "official"
        or artifact.driver != "forest.python-worker/1"
        or snapshot.component["id"] != descriptor.component_id
        or snapshot.component["scope"] != "bot"
        or snapshot.manifest["runtime"] != "python-worker"
        or snapshot.manifest["package_id"] != artifact.package_id
        or snapshot.manifest["version"] != artifact.package_version
    ):
        raise ForestError(
            "COMPONENT_SELECTION_ARTIFACT_CHANGED",
            "Official package descriptor differs from its pinned artifact",
            409,
        )
    storage = await asyncio.to_thread(_configuration_storage, snapshot)
    if descriptor.storage is None or descriptor.storage.package_digest != storage.package_digest:
        raise ForestError(
            "COMPONENT_SELECTION_ARTIFACT_CHANGED",
            "Official package storage differs from its pinned artifact",
            409,
        )
    schema = await asyncio.to_thread(snapshot.schema, snapshot.component["config_schema"])
    schema_digest = digest_json(schema)
    configuration = ModuleConfigurationRepository(
        store,
        component_id=descriptor.component_id,
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
        immutable_paths=tuple(snapshot.component.get("config_immutable_paths", ())),
    )
    try:
        desired = (
            await configuration.desired_checked_in(conn)
            if conn is not None else await configuration.desired_checked()
        )
    except (InvalidSchemaName, UndefinedTable) as error:
        raise ForestError(
            "PACKAGE_CONFIGURATION_REQUIRED",
            "Official package configuration is unavailable",
            409,
        ) from error
    except SerializationFailure as error:
        raise ForestError(
            "MODULE_CONFIGURATION_REVISION_CONFLICT",
            "Official package configuration changed during inspection",
            409,
        ) from error
    if desired.validation_digest != schema_digest or not Draft202012Validator(
        schema
    ).is_valid(desired.value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
            "Official package configuration was not validated for this artifact schema",
            409,
        )
    return PackageCandidateInspection(
        descriptor.component_id, "official", snapshot.digest,
        digest_json(snapshot.manifest), desired.revision, desired.payload_id,
        desired.payload_digest, schema_digest, storage.package_digest,
        WorkerLimits.from_component(snapshot.component),
    )


async def bootstrap_builtin_package(
    source: Path,
    catalog: PackageCatalog,
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict[str, Any] | None,
    resolved_contracts: dict[str, str] | None = None,
    effect_driver_registry: PackageEffectDriverRegistry = EMPTY_PACKAGE_EFFECT_DRIVERS,
    configuration_validator_factory: Callable[[str], Callable] | None = None,
    additional_grants: tuple[CapabilityGrant, ...] = (),
) -> ProvisionedPackageModule:
    """Build and provision one builtin through the same immutable package path."""

    snapshot = catalog.build(source)
    return await bootstrap_builtin_snapshot(
        snapshot, store, installer, kernel=kernel,
        initial_configuration=initial_configuration,
        resolved_contracts=resolved_contracts,
        effect_driver_registry=effect_driver_registry,
        configuration_validator_factory=configuration_validator_factory,
        additional_grants=additional_grants,
    )


async def bootstrap_builtin_snapshot(
    snapshot: PackageSnapshot,
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict[str, Any] | None,
    resolved_contracts: dict[str, str] | None = None,
    effect_driver_registry: PackageEffectDriverRegistry = EMPTY_PACKAGE_EFFECT_DRIVERS,
    configuration_validator_factory: Callable[[str], Callable] | None = None,
    additional_grants: tuple[CapabilityGrant, ...] = (),
) -> ProvisionedPackageModule:
    """Provision the exact captured official artifact inspected by its caller."""

    return await _bootstrap_package_snapshot(
        snapshot,
        store,
        installer,
        kernel=kernel,
        initial_configuration=initial_configuration,
        resolved_contracts=resolved_contracts,
        effect_driver_registry=effect_driver_registry,
        configuration_validator_factory=configuration_validator_factory,
        additional_grants=additional_grants,
        trust_level="official",
    )


async def bootstrap_installed_package(
    package_digest: str,
    catalog: PackageCatalog,
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict[str, Any] | None,
    approved_capabilities: tuple[str, ...],
    resolved_contracts: dict[str, str] | None = None,
) -> ProvisionedPackageModule:
    """Bind an approved artifact to its own initialized configuration ledger."""

    snapshot = catalog.get(package_digest)
    if initial_configuration is None:
        await _inspect_installed_snapshot(
            snapshot, store, kernel=kernel,
            approved_capabilities=approved_capabilities,
        )
    return await _bootstrap_package_snapshot(
        snapshot,
        store,
        installer,
        kernel=kernel,
        initial_configuration=initial_configuration,
        resolved_contracts=resolved_contracts,
        approved_capabilities=approved_capabilities,
        trust_level="installed",
    )


def _compile_package_descriptor(
    snapshot: PackageSnapshot,
    store,
    *,
    kernel,
    configuration: ModuleConfigurationRepository,
    configuration_schema: dict[str, Any],
    storage: RuntimeStorageBundle | None,
    grants,
    resolved_contracts: dict[str, str] | None,
    effect_driver_registry: PackageEffectDriverRegistry,
    configuration_validator,
    trust_level: str,
    plan_only: bool = False,
) -> RuntimeModuleDescriptor:
    component_id = snapshot.component["id"]
    effect_driver_factories = effect_driver_registry.resolve(
        tuple(snapshot.component.get("effect_services", ()))
    )
    has_domain_authority = bool(
        snapshot.component.get("domain_queries")
        or snapshot.component.get("domain_commands")
        or snapshot.component.get("unit_of_work_participants")
        or snapshot.component.get("owned_work_handlers")
    )
    domain_authority = (
        PostgresPackageDomainAuthority(
            store,
            kernel.runtime,
            component_id=component_id,
            runtime_package_digest=snapshot.digest,
            storage_spec=storage.spec,
            storage_package_digest=storage.package_digest,
            storage_execution_profile=storage.execution_profile,
            payloads=kernel.payload,
        )
        if has_domain_authority
        else None
    )
    packaged_ui = snapshot.management_ui()
    if packaged_ui is None:
        management = (configuration_management_contribution(component_id),)
    else:
        ui_metadata, _content = packaged_ui
        ui_declaration = snapshot.component["management_ui"]
        management = (
            RuntimePackageManagementContribution(
                ui_declaration.get("contribution_id", "configuration"),
                ui_declaration.get("label", component_id),
                ("bot",),
                component_id,
                ui_metadata["sha256"],
                group=ui_declaration.get("group", "configuration"),
                order=ui_declaration.get("order", 10),
            ),
        )
    return package_runtime_descriptor(
        snapshot,
        bot_id=store.bot_id,
        module_configuration=configuration,
        grants=grants,
        resolved_contracts=_required_service_contracts(snapshot, resolved_contracts),
        generation_verifier=kernel.runtime.verify_active_generation,
        generation_member_verifier=kernel.runtime.verify_generation_member,
        state_queries=KernelPackageStateQueries(kernel.runtime, kernel.payload),
        state_commands=KernelPackageStateCommands(kernel.runtime, kernel.payload),
        payload_authority=KernelPackagePayloadAuthority(kernel.payload),
        domain_queries=domain_authority,
        domain_commands=domain_authority,
        unit_of_work_authority=domain_authority,
        unit_of_work_coordinator_factory=package_uow_coordinator_factory(
            store=store, runtime=kernel.runtime,
        ),
        configuration_apply_coordinator_factory=lambda target, repository, control: (
            PostgresComponentConfigurationApplyCoordinator(
                store,
                kernel.runtime,
                component_id=target,
                configuration=repository,
                control_handle=control,
            )
        ),
        owned_work_authority=domain_authority,
        agent_tool_effects=kernel.effect,
        service_operation_ledger=KernelPackageServiceOperationLedger(
            kernel.runtime, kernel.payload,
        ),
        configuration_validator=configuration_validator,
        effect_driver_factories=effect_driver_factories,
        storage=storage,
        management=management,
        management_contract_factory=lambda adapter: PackageManagementContract(
            configuration,
            adapter=adapter,
            schema=configuration_schema,
            validator=configuration_validator,
        ),
        trust_level=trust_level,
        plan_only=plan_only,
    )


async def compile_installed_candidate_descriptor(
    package_digest: str,
    catalog: PackageCatalog,
    store,
    *,
    kernel,
    approved_capabilities: tuple[str, ...],
    expected_manifest_digest: str,
    expected_configuration_revision: int | None = None,
    conn=None,
) -> tuple[PackageCandidateInspection, RuntimeModuleDescriptor]:
    """Compile an exact package into the production Graph rules without provisioning."""

    snapshot = _pinned_installed_snapshot(
        package_digest, catalog, expected_manifest_digest
    )
    inspected, layout, configuration = await _inspect_installed_snapshot(
        snapshot,
        store,
        kernel=kernel,
        approved_capabilities=approved_capabilities,
        expected_configuration_revision=expected_configuration_revision,
        conn=conn,
    )
    descriptor = _compile_package_descriptor(
        snapshot, store, kernel=kernel, configuration=configuration,
        configuration_schema=layout.schema, storage=layout.storage, grants=None,
        resolved_contracts=None,
        effect_driver_registry=EMPTY_PACKAGE_EFFECT_DRIVERS,
        configuration_validator=None, trust_level="installed", plan_only=True,
    )
    return inspected, descriptor


async def _bootstrap_package_snapshot(
    snapshot: PackageSnapshot,
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict[str, Any] | None,
    resolved_contracts: dict[str, str] | None = None,
    effect_driver_registry: PackageEffectDriverRegistry = EMPTY_PACKAGE_EFFECT_DRIVERS,
    configuration_validator_factory: Callable[[str], Callable] | None = None,
    additional_grants: tuple[CapabilityGrant, ...] = (),
    approved_capabilities: tuple[str, ...] | None = None,
    trust_level: str,
) -> ProvisionedPackageModule:
    component_id = snapshot.component["id"]
    if trust_level == "official":
        configuration_schema = snapshot.schema(snapshot.component["config_schema"])
        configuration_storage = storage = _configuration_storage(snapshot)
    elif trust_level == "installed":
        layout = _installed_package_layout(
            snapshot, approved_capabilities, initial_configuration
        )
        configuration_schema = layout.schema
        configuration_storage = layout.configuration_storage
        storage = layout.storage
    else:
        raise ValueError("Package trust level is invalid")
    configuration = ModuleConfigurationRepository(
        store,
        component_id=component_id,
        namespace=configuration_storage.spec.namespace,
        payloads=kernel.payload,
        immutable_paths=tuple(snapshot.component.get("config_immutable_paths", ())),
    )
    configuration_validator = (
        configuration_validator_factory(configuration_storage.spec.namespace)
        if configuration_validator_factory is not None
        else None
    )
    schema_digest = digest_json(configuration_schema)
    if trust_level == "official" and initial_configuration is None:
        await _official_desired(
            configuration, configuration_schema, schema_digest, configuration_validator
        )
    if trust_level == "installed" and storage is not None:
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    await installer.install(
        configuration_storage.spec,
        configuration_storage.baseline,
        configuration_storage.package_digest,
        execution_profile=configuration_storage.execution_profile,
    )
    if initial_configuration is not None:
        if not Draft202012Validator(configuration_schema).is_valid(initial_configuration):
            raise ForestError(
                "PACKAGE_CONFIGURATION_INVALID",
                "Initial configuration violates the package schema",
                422,
            )
        if configuration_validator is not None:
            await configuration_validator(initial_configuration)
        await configuration.ensure_initialized(
            operation_id=f"bootstrap:{component_id}:configuration",
            value=initial_configuration,
            validation_digest=schema_digest,
        )
    if trust_level == "installed":
        await _installed_desired(layout, configuration)
    elif initial_configuration is not None:
        await configuration.assert_immutable(initial_configuration)
    requested = tuple(snapshot.component.get("capabilities", {}).get("requests", ()))
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id=component_id,
        entries=(
            CapabilityGrant("component.runtime", "bot", store.bot_id),
            *(
                CapabilityGrant(capability, "component", component_id)
                for capability in requested
            ),
            *additional_grants,
        ),
        expected_policy_revision=policy_revision,
    )
    descriptor = _compile_package_descriptor(
        snapshot, store, kernel=kernel, configuration=configuration,
        configuration_schema=configuration_schema, storage=storage, grants=grants,
        resolved_contracts=resolved_contracts,
        effect_driver_registry=effect_driver_registry,
        configuration_validator=configuration_validator,
        trust_level=trust_level,
    )
    return ProvisionedPackageModule(snapshot, descriptor, configuration, grants)
