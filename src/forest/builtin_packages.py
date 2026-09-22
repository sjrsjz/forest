"""Generic provisioning for official builtin plugin packages."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.package_effects import (
    EMPTY_PACKAGE_EFFECT_DRIVERS,
    PackageEffectDriverRegistry,
)
from forest.package_runtime import package_runtime_descriptor
from forest.package_service_ledger import KernelPackageServiceOperationLedger
from forest.package_state_ports import (
    KernelPackageStateCommands,
    KernelPackageStateQueries,
)
from forest.packages import PackageCatalog, PackageSnapshot, digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    RuntimeStorageBundle,
    configuration_management_contribution,
)
from forest.schema_bundles import StorageBundleSpec
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository


@dataclass(frozen=True)
class BuiltinPackageModule:
    snapshot: PackageSnapshot
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


def _configuration_storage(snapshot: PackageSnapshot) -> RuntimeStorageBundle:
    """Attach the standard configuration ledger to one official package namespace."""

    from importlib.resources import files

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
    storage_digest = digest_json(
        {
            "protocol": "forest.builtin-package-storage/1",
            "package_digest": snapshot.digest,
            "baseline_sha256": baseline_sha256,
            "repository_contract": repository_contract,
        }
    )
    return RuntimeStorageBundle(
        StorageBundleSpec(
            component_id=snapshot.component["id"],
            schema_id=schema_id,
            schema_version=schema_version,
            baseline_sha256=baseline_sha256,
            repository_contract=repository_contract,
        ),
        baseline,
        storage_digest,
        execution_profile="official",
    )


async def bootstrap_builtin_package(
    source: Path,
    catalog: PackageCatalog,
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict[str, Any],
    resolved_contracts: dict[str, str] | None = None,
    startup_components: tuple[str, ...] = (),
    effect_driver_registry: PackageEffectDriverRegistry = EMPTY_PACKAGE_EFFECT_DRIVERS,
    configuration_validator_factory: Callable[[str], Callable] | None = None,
) -> BuiltinPackageModule:
    """Build and provision one builtin through the same immutable package path."""

    snapshot = catalog.build(source)
    component_id = snapshot.component["id"]
    storage = _configuration_storage(snapshot)
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id=component_id,
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    configuration_schema = snapshot.schema(snapshot.component["config_schema"])
    configuration_validator = (
        configuration_validator_factory(storage.spec.namespace)
        if configuration_validator_factory is not None
        else None
    )
    if configuration_validator is not None:
        await configuration_validator(initial_configuration)
    await configuration.ensure_initialized(
        operation_id=f"bootstrap:{component_id}:configuration",
        value=initial_configuration,
        validation_digest=digest_json(configuration_schema),
    )
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
        ),
        expected_policy_revision=policy_revision,
    )
    management_contract = CleanModuleConfigurationContract(
        configuration,
        schema=configuration_schema,
        validator=configuration_validator,
    )
    effect_driver_factories = effect_driver_registry.resolve(
        tuple(snapshot.component.get("effect_services", ()))
    )
    descriptor = package_runtime_descriptor(
        snapshot,
        bot_id=store.bot_id,
        module_configuration=configuration,
        grants=grants,
        resolved_contracts=resolved_contracts or {},
        startup_components=tuple(
            dict.fromkeys(
                (
                    "kernel.runtime",
                    "kernel.payload",
                    "kernel.authorization",
                    *startup_components,
                )
            )
        ),
        generation_verifier=kernel.runtime.verify_active_generation,
        generation_member_verifier=kernel.runtime.verify_generation_member,
        state_queries=KernelPackageStateQueries(
            kernel.runtime,
            kernel.payload,
        ),
        state_commands=KernelPackageStateCommands(
            kernel.runtime,
            kernel.payload,
        ),
        service_operation_ledger=KernelPackageServiceOperationLedger(
            kernel.runtime,
            kernel.payload,
        ),
        configuration_validator=configuration_validator,
        effect_driver_factories=effect_driver_factories,
        storage=storage,
        management=(configuration_management_contribution(component_id),),
        management_contract=management_contract,
        trust_level="official",
    )
    return BuiltinPackageModule(snapshot, descriptor, configuration, grants)
