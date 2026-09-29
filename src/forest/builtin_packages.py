"""Generic provisioning for official builtin plugin packages."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.unit_of_work import (
    PostgresComponentConfigurationApplyCoordinator,
    package_uow_coordinator_factory,
)


@dataclass(frozen=True)
class BuiltinPackageModule:
    snapshot: PackageSnapshot
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


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

    from importlib.resources import files

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


async def bootstrap_builtin_package(
    source: Path,
    catalog: PackageCatalog,
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict[str, Any],
    resolved_contracts: dict[str, str] | None = None,
    effect_driver_registry: PackageEffectDriverRegistry = EMPTY_PACKAGE_EFFECT_DRIVERS,
    configuration_validator_factory: Callable[[str], Callable] | None = None,
    additional_grants: tuple[CapabilityGrant, ...] = (),
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
        immutable_paths=tuple(snapshot.component.get("config_immutable_paths", ())),
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
    descriptor = package_runtime_descriptor(
        snapshot,
        bot_id=store.bot_id,
        module_configuration=configuration,
        grants=grants,
        resolved_contracts=_required_service_contracts(snapshot, resolved_contracts),
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
        payload_authority=KernelPackagePayloadAuthority(kernel.payload),
        domain_queries=domain_authority,
        domain_commands=domain_authority,
        unit_of_work_authority=domain_authority,
        unit_of_work_coordinator_factory=package_uow_coordinator_factory(
            store=store,
            runtime=kernel.runtime,
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
            kernel.runtime,
            kernel.payload,
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
        trust_level="official",
    )
    return BuiltinPackageModule(snapshot, descriptor, configuration, grants)
