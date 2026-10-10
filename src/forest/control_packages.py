"""Compile control-scope packages against scope-owned authorities."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from importlib.resources import files

from jsonschema import Draft202012Validator

from forest.deployment_kernel_management import (
    DeploymentKernelManagementAuthority,
    DeploymentKernelQueryAuthority,
)
from forest.errors import ForestError
from forest.host_kernel_management import HostKernelManagementAuthority, HostKernelQueryAuthority
from forest.package_domain_authority import PostgresPackageDomainAuthority
from forest.package_management import PackageManagementContract
from forest.package_runtime import package_runtime_descriptor
from forest.packages import digest_json, storage_bundle_digest
from forest.runtime_modules import (
    RuntimePackageManagementContribution,
    RuntimeStorageBundle,
    configuration_management_contribution,
)
from forest.schema_bundles import StorageBundleSpec
from forest.storage.control_component_state import ControlPackageStateAuthority
from forest.storage.deployment_module_configuration import DeploymentModuleConfigurationRepository
from forest.storage.host_module_configuration import HostModuleConfigurationRepository

_CONTROL_BINDINGS = {
    "deployment": (
        DeploymentModuleConfigurationRepository,
        DeploymentKernelManagementAuthority,
        DeploymentKernelQueryAuthority,
    ),
    "host": (
        HostModuleConfigurationRepository,
        HostKernelManagementAuthority,
        HostKernelQueryAuthority,
    ),
}


def _scope_bindings(runtime):
    try:
        return _CONTROL_BINDINGS[runtime.scope_kind]
    except (AttributeError, KeyError) as exc:
        raise ForestError(
            "PACKAGE_CONTROL_SCOPE_INVALID", "Package requires a control scope", 422,
        ) from exc


@dataclass(frozen=True)
class _BuiltinGrant:
    capability: str
    resource_kind: str
    resource_id: str


@dataclass(frozen=True)
class _BuiltinGrantSet:
    entries: tuple[_BuiltinGrant, ...]
    grant_set_digest: str


def _configuration_storage(component_id: str, scope_kind: str) -> tuple[StorageBundleSpec, bytes]:
    baseline = files("forest.storage.official").joinpath(
        "control_module_configuration.sql"
    ).read_bytes()
    schema_id = f"forest.{scope_kind}.config." + hashlib.sha256(
        component_id.encode()
    ).hexdigest()[:32]
    return StorageBundleSpec(
        component_id=component_id, schema_id=schema_id, schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract=f"forest.{scope_kind}-configuration.repository/1",
        scope=scope_kind,
    ), baseline


def control_storage_bundle(snapshot, scope_kind: str) -> RuntimeStorageBundle:
    """Project the exact SQL bundle installed for a control package."""

    declared = snapshot.storage_bundle()
    if declared is None:
        spec, baseline = _configuration_storage(snapshot.component["id"], scope_kind)
    else:
        metadata, owned_sql = declared
        if metadata["execution_profile"] != "official":
            raise ForestError(
                f"{scope_kind.upper()}_PACKAGE_STORAGE_REQUIRED",
                "Control storage requires the trusted SQL profile", 422,
            )
        common_sql = files("forest.storage.official").joinpath(
            "control_module_configuration.sql"
        ).read_bytes()
        baseline = common_sql + b"\n" + owned_sql
        spec = StorageBundleSpec(
            component_id=snapshot.component["id"], schema_id=metadata["schema_id"],
            schema_version=metadata["schema_version"],
            baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract=metadata["repository_contract"], scope=scope_kind,
        )
    return RuntimeStorageBundle(
        spec, baseline, storage_bundle_digest(spec, "official"),
        execution_profile="official",
    )


async def _prepared_configuration(configuration, binding, schema: dict):
    if binding is None:
        selected = await configuration.desired_checked()
    elif binding.get("source") == "desired":
        selected = await configuration.desired_checked()
    else:
        selected = await configuration.effective(include_value=True)
    if binding is not None and (
        selected.revision != binding.get("revision")
        or selected.payload_id != binding.get("payload_id")
        or selected.payload_digest != binding.get("payload_digest")
    ):
        raise ForestError(
            f"{configuration.scope.upper()}_CONFIGURATION_BINDING_MISMATCH",
            "Graph configuration differs from its frozen revision", 409,
        )
    if not Draft202012Validator(schema).is_valid(selected.value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
            "Control configuration differs from the frozen package schema", 409,
        )
    return selected


def _validate_management_configuration(value, schema: dict) -> None:
    if not Draft202012Validator(schema).is_valid(value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID",
            "Control configuration violates its package schema", 422,
        )


async def bootstrap_builtin_control_package(
    snapshot, runtime, *, plan_only=False, frozen_configuration=None,
):
    """Install an official control package with bounded native port grants."""

    configuration_type, command_type, query_type = _scope_bindings(runtime)
    scope_kind = runtime.scope_kind
    component = snapshot.component
    effects = tuple(component.get("effect_services", ()))
    owned_work = tuple(component.get("owned_work_handlers", ()))
    kernel_work = tuple(
        item for item in owned_work if item.get("authority", "package") == "kernel"
    )
    if kernel_work and (
        len(kernel_work) != len(owned_work)
        or component["id"] not in runtime.kernel_owned_work_authorities
    ):
        raise ForestError(
            f"{scope_kind.upper()}_KERNEL_WORK_UNAVAILABLE",
            "Kernel-owned work requires one exact official authority binding", 503,
        )
    requested = tuple(component.get("capabilities", {}).get("requests", ()))
    upload_ports = {
        item["port"] for item in component.get("management_artifacts", ())
        if item["direction"] == "upload"
    }
    close_ports = {
        item["close_port"] for item in component.get("management_artifacts", ())
        if "close_port" in item
    }
    if any(
        item["kind"] not in runtime.effect_driver_factories
        or (item["name"] in upload_ports and (
            item["semantics"] != "idempotent"
            or f"service.call:{item['name']}" in requested
        ))
        or (item["name"] in close_ports and (
            item["semantics"] != "idempotent"
            or f"service.call:{item['name']}" not in requested
        ))
        or (item["name"] not in upload_ports | close_ports and (
            item["semantics"] not in {"read", "idempotent"}
            or f"service.call:{item['name']}" not in requested
        ))
        for item in effects
    ):
        raise ForestError(
            f"{scope_kind.upper()}_PACKAGE_PORT_UNAVAILABLE",
            "Official control effect port has no permitted native binding", 503,
        )
    artifact_commands = {
        item[key]
        for item in component.get("management_artifacts", ())
        for key in ("reserve_port", "settle_port") if key in item
    }
    kernel_commands = tuple(component.get("kernel_commands", ()))
    kernel_queries = tuple(component.get("kernel_queries", ()))
    kernel_action_ports = {item["name"] for item in kernel_commands}
    action_commands = {
        item["port"] for item in component.get("management_actions", ())
    } - kernel_action_ports
    internal_commands = {
        item["internal_command"] for item in component.get("provides", ())
        if "internal_command" in item
    }
    private_work_commands = {
        name for item in owned_work for name in item.get("private_commands", ())
    }
    declared_commands = {
        item["name"] for item in component.get("domain_commands", ())
    }
    if component["scope"] != scope_kind or component.get("configuration_apply") or (
        declared_commands != (
            artifact_commands | action_commands | internal_commands | private_work_commands
        )
        or bool(private_work_commands & (
            artifact_commands | action_commands | internal_commands
        ))
        or bool(internal_commands & (artifact_commands | action_commands))
        or any(
            item["semantics"] != "read" and "internal_command" not in item
            for item in component.get("provides", ())
        )
        or any(
            (component["id"], item["kind"])
            not in runtime.kernel_management_handlers
            for item in kernel_commands
        )
        or any(
            (component["id"], item["kind"])
            not in runtime.kernel_query_handlers
            for item in kernel_queries
        )
        or (owned_work and not kernel_work and not component.get("storage"))
    ):
        raise ForestError(
            f"{scope_kind.upper()}_PACKAGE_PORT_UNAVAILABLE",
            "Control package write ports must match declared management bindings", 503,
        )
    declared = snapshot.storage_bundle()
    bundle = control_storage_bundle(snapshot, scope_kind)
    spec, baseline = bundle.spec, bundle.baseline
    storage = bundle if declared is not None else None
    if not plan_only:
        await runtime.schema_bundle_installer.install(
            spec, baseline, bundle.package_digest,
            execution_profile="official",
        )
    config_schema = snapshot.schema(component["config_schema"])
    config_value = snapshot.bootstrap_configuration()
    if not Draft202012Validator(config_schema).is_valid(config_value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID", "Official bootstrap configuration is invalid", 422
        )
    configuration = configuration_type(
        runtime.store, component_id=component["id"], namespace=spec.namespace,
        immutable_paths=tuple(component.get("config_immutable_paths", ())),
    )
    if not plan_only:
        await configuration.ensure_initialized(
            operation_id=f"bootstrap:{component['id']}:configuration", value=config_value,
            validation_digest=digest_json(config_schema),
        )
        prepared_configuration = await _prepared_configuration(
            configuration, frozen_configuration, config_schema,
        )
        if prepared_configuration.validation_digest != digest_json(config_schema):
            raise ForestError(
                "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
                "Control configuration differs from the frozen package schema", 409,
            )
    else:
        prepared_configuration = None
    grants = _BuiltinGrantSet(
        tuple(_BuiltinGrant(item, "component", component["id"]) for item in requested),
        digest_json([
            f"forest.{scope_kind}.official-capability-grants/1",
            snapshot.digest, component["id"], requested,
        ]),
    )
    domain = (
        PostgresPackageDomainAuthority(
            runtime.store, runtime.generation_authority,
            component_id=component["id"], runtime_package_digest=snapshot.digest,
            storage_spec=spec, storage_package_digest=storage.package_digest,
            storage_execution_profile="official",
        )
        if storage is not None else None
    )
    kernel_management = (
        command_type(
            runtime.store, runtime.generation_authority,
            component_id=component["id"], ports=kernel_commands,
            handlers=runtime.kernel_management_handlers,
        )
        if kernel_commands else None
    )
    kernel_query_authority = (
        query_type(
            runtime.store, runtime.generation_authority,
            component_id=component["id"], ports=kernel_queries,
            handlers=runtime.kernel_query_handlers,
        )
        if kernel_queries else None
    )
    effect_factories = {
        item["name"]: runtime.effect_driver_factories[item["kind"]]
        for item in effects
    }
    ui = snapshot.management_ui()
    management = (configuration_management_contribution(
        component["id"], scope_kinds=(scope_kind,),
    ),)
    if ui is not None:
        declaration = component["management_ui"]
        management += (RuntimePackageManagementContribution(
            declaration.get("contribution_id", component["id"] + ".workspace"),
            declaration.get("label", component["id"]),
            (scope_kind,), component["id"], ui[0]["sha256"],
            group=declaration.get("group", scope_kind),
            order=declaration.get("order", 10),
        ),)
    descriptor = package_runtime_descriptor(
        snapshot, bot_id=None, scope_id=runtime.scope_id,
        module_configuration=configuration, grants=grants, resolved_contracts={},
        prepared_configuration=prepared_configuration,
        generation_verifier=runtime.generation_authority.verify_active_generation,
        generation_member_verifier=runtime.generation_authority.verify_generation_member,
        state_queries=None, state_commands=None, domain_queries=domain,
        domain_commands=domain,
        kernel_management_queries=kernel_query_authority,
        kernel_management_commands=kernel_management,
        owned_work_authority=(
            runtime.kernel_owned_work_authorities[component["id"]]
            if kernel_work else domain if owned_work else None
        ),
        service_operation_ledger=runtime.service_operation_ledger,
        storage=storage, trust_level="official",
        effect_driver_factories=effect_factories,
        management=management,
        management_contract_factory=lambda adapter: PackageManagementContract(
            configuration, adapter=adapter, schema=config_schema,
            validator=lambda value: _validate_management_configuration(value, config_schema),
            apply_callback=lambda **values: runtime.apply_configuration(
                configuration, validation_digest=digest_json(config_schema), **values
            ),
        ),
        plan_only=plan_only,
    )
    return descriptor


async def bootstrap_installed_control_package(
    snapshot, runtime, *, plan_only=False, frozen_configuration=None,
):
    """Compile a declaration-only control Worker without official SQL authority."""

    configuration_type, _command_type, _query_type = _scope_bindings(runtime)
    scope_kind = runtime.scope_kind
    component = snapshot.component
    allowed = {
        "id", "scope", "state_schema", "reload_mode", "config_schema",
        "bootstrap_config", "resources", "lifecycle", "capabilities",
        "provides", "management_queries", "management_ui", "config_immutable_paths",
        "state_value_schema",
    }
    requested = tuple(component.get("capabilities", {}).get("requests", ()))
    if (
        component["scope"] != scope_kind
        or set(component) - allowed
        or not set(requested) <= {"state.read", "state.write"}
        or ("state.write" in requested and "state.read" not in requested)
        or (requested and "state_value_schema" not in component)
        or any(
            item["semantics"] not in {"read", "at-most-once"}
            or (item["semantics"] == "at-most-once" and "state.write" not in requested)
            for item in component["provides"]
        )
    ):
        raise ForestError(
            f"{scope_kind.upper()}_PACKAGE_PORT_UNAVAILABLE",
            "Installed control package requests unsupported state or service authority",
            422,
        )
    config_schema = snapshot.schema(component["config_schema"])
    config_value = snapshot.bootstrap_configuration()
    if not Draft202012Validator(config_schema).is_valid(config_value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID", "Control bootstrap configuration is invalid", 422
        )
    bundle = control_storage_bundle(snapshot, scope_kind)
    spec, baseline = bundle.spec, bundle.baseline
    if not plan_only:
        await runtime.schema_bundle_installer.install(
            spec, baseline, bundle.package_digest,
            execution_profile="official",
        )
    configuration = configuration_type(
        runtime.store, component_id=component["id"], namespace=spec.namespace,
        immutable_paths=tuple(component.get("config_immutable_paths", ())),
    )
    if not plan_only:
        await configuration.ensure_initialized(
            operation_id=f"bootstrap:{component['id']}:configuration", value=config_value,
            validation_digest=digest_json(config_schema),
        )
        prepared_configuration = await _prepared_configuration(
            configuration, frozen_configuration, config_schema,
        )
        if prepared_configuration.validation_digest != digest_json(config_schema):
            raise ForestError(
                "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
                "Control configuration differs from the frozen package schema", 409,
            )
    else:
        prepared_configuration = None
    grants = _BuiltinGrantSet(
        tuple(_BuiltinGrant(item, "component", component["id"]) for item in requested),
        digest_json([
            f"forest.{scope_kind}.installed-grants/1", snapshot.digest,
            component["id"], requested,
        ]),
    )
    state = (
        ControlPackageStateAuthority(
            runtime, state_schema=component["state_schema"],
            value_schema=snapshot.schema(component["state_value_schema"]),
        )
        if requested else None
    )
    if state is not None:
        await state.verify_contract(component["id"])
    ui = snapshot.management_ui()
    management = (configuration_management_contribution(
        component["id"], scope_kinds=(scope_kind,),
    ),)
    if ui is not None:
        declaration = component["management_ui"]
        management += (RuntimePackageManagementContribution(
            declaration.get("contribution_id", component["id"] + ".workspace"),
            declaration.get("label", component["id"]),
            (scope_kind,), component["id"], ui[0]["sha256"],
            group=declaration.get("group", scope_kind),
            order=declaration.get("order", 10),
        ),)
    return package_runtime_descriptor(
        snapshot, bot_id=None, scope_id=runtime.scope_id,
        module_configuration=configuration, grants=grants, resolved_contracts={},
        prepared_configuration=prepared_configuration,
        generation_verifier=runtime.generation_authority.verify_active_generation,
        generation_member_verifier=runtime.generation_authority.verify_generation_member,
        state_queries=state if "state.read" in requested else None,
        state_commands=state if "state.write" in requested else None,
        domain_queries=None,
        service_operation_ledger=(
            runtime.service_operation_ledger if "state.write" in requested else None
        ),
        storage=None, trust_level="installed",
        management=management,
        management_contract_factory=lambda adapter: PackageManagementContract(
            configuration, adapter=adapter, schema=config_schema,
            validator=lambda value: _validate_management_configuration(value, config_schema),
            apply_callback=lambda **values: runtime.apply_configuration(
                configuration, validation_digest=digest_json(config_schema), **values
            ),
        ),
        plan_only=plan_only,
    )
