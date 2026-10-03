"""Assemble trusted deployment packages against deployment-owned authorities."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from importlib.resources import files

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.package_domain_authority import PostgresPackageDomainAuthority
from forest.package_management import PackageManagementContract
from forest.package_runtime import package_runtime_descriptor
from forest.packages import digest_json, storage_bundle_digest
from forest.runtime_modules import RuntimePackageManagementContribution, RuntimeStorageBundle
from forest.schema_bundles import StorageBundleSpec
from forest.storage.deployment_module_configuration import DeploymentModuleConfigurationRepository


@dataclass(frozen=True)
class _BuiltinGrant:
    capability: str
    resource_kind: str
    resource_id: str


@dataclass(frozen=True)
class _BuiltinGrantSet:
    entries: tuple[_BuiltinGrant, ...]
    grant_set_digest: str


class _ReadOnlyManagementContract(PackageManagementContract):
    """Immutable deployment package configuration has no apply action."""

    def describe(self) -> dict:
        description = super().describe()
        description["configuration"]["operations"] = ["configuration.read"]
        return description

    async def plan(self, **_values):
        raise ForestError("MODULE_MANAGEMENT_UNAVAILABLE", "Configuration is immutable", 404)

    async def apply(self, **_values):
        raise ForestError("MODULE_MANAGEMENT_UNAVAILABLE", "Configuration is immutable", 404)


def _configuration_storage(component_id: str) -> tuple[StorageBundleSpec, bytes]:
    baseline = files("forest.storage.official").joinpath(
        "deployment_module_configuration.sql"
    ).read_bytes()
    schema_id = "forest.deployment.config." + hashlib.sha256(
        component_id.encode()
    ).hexdigest()[:32]
    return StorageBundleSpec(
        component_id=component_id, schema_id=schema_id, schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract="forest.deployment-configuration.repository/1",
        scope="deployment",
    ), baseline


async def bootstrap_builtin_deployment_package(snapshot, runtime, *, plan_only=False):
    """Install an official deployment package with bounded native port grants."""

    component = snapshot.component
    effects = tuple(component.get("effect_services", ()))
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
            item["semantics"] != "read"
            or f"service.call:{item['name']}" not in requested
        ))
        for item in effects
    ):
        raise ForestError(
            "DEPLOYMENT_PACKAGE_PORT_UNAVAILABLE",
            "Official deployment effect port has no permitted native binding", 503,
        )
    artifact_commands = {
        item[key]
        for item in component.get("management_artifacts", ())
        for key in ("reserve_port", "settle_port") if key in item
    }
    declared_commands = {
        item["name"] for item in component.get("domain_commands", ())
    }
    if component["scope"] != "deployment" or component.get("configuration_apply") or (
        declared_commands != artifact_commands or component.get("management_actions")
        or (component.get("owned_work_handlers") and not component.get("storage"))
        or any(item["semantics"] != "read" for item in component.get("provides", ()))
    ):
        raise ForestError(
            "DEPLOYMENT_PACKAGE_PORT_UNAVAILABLE",
            "Deployment package write ports require declared artifact transaction hooks", 503,
        )
    declared = snapshot.storage_bundle()
    if declared is not None:
        metadata, owned_sql = declared
        if metadata["execution_profile"] != "official":
            raise ForestError(
                "DEPLOYMENT_PACKAGE_STORAGE_REQUIRED",
                "Official deployment storage must use the trusted SQL profile", 422,
            )
        common_sql = files("forest.storage.official").joinpath(
            "deployment_module_configuration.sql"
        ).read_bytes()
        baseline = common_sql + b"\n" + owned_sql
        spec = StorageBundleSpec(
            component_id=component["id"], schema_id=metadata["schema_id"],
            schema_version=metadata["schema_version"],
            baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract=metadata["repository_contract"], scope="deployment",
        )
        storage = RuntimeStorageBundle(
            spec, baseline, storage_bundle_digest(spec, "official"),
            execution_profile="official",
        )
    else:
        spec, baseline = _configuration_storage(component["id"])
        storage = None
    if not plan_only:
        await runtime.schema_bundle_installer.install(
            spec, baseline,
            storage.package_digest if storage is not None else storage_bundle_digest(spec, "official"),
            execution_profile="official",
        )
    config_schema = snapshot.schema(component["config_schema"])
    config_value = snapshot.bootstrap_configuration()
    if not Draft202012Validator(config_schema).is_valid(config_value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID", "Official bootstrap configuration is invalid", 422
        )
    configuration = DeploymentModuleConfigurationRepository(
        runtime.store, component_id=component["id"], namespace=spec.namespace,
        immutable_paths=tuple(component.get("config_immutable_paths", ())),
    )
    if not plan_only:
        await configuration.ensure_initialized(
            operation_id=f"bootstrap:{component['id']}:configuration", value=config_value,
            validation_digest=digest_json(config_schema),
        )
        desired = await configuration.desired_checked()
        if desired.validation_digest != digest_json(config_schema) or not (
            Draft202012Validator(config_schema).is_valid(desired.value)
        ):
            raise ForestError(
                "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
                "Deployment configuration differs from the frozen package schema", 409,
            )
    grants = _BuiltinGrantSet(
        tuple(_BuiltinGrant(item, "component", component["id"]) for item in requested),
        digest_json([
            "forest.deployment.official-capability-grants/1",
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
    effect_factories = {
        item["name"]: runtime.effect_driver_factories[item["kind"]]
        for item in effects
    }
    ui = snapshot.management_ui()
    management = ()
    if ui is not None:
        declaration = component["management_ui"]
        management = (RuntimePackageManagementContribution(
            declaration.get("contribution_id", "configuration"),
            declaration.get("label", component["id"]),
            ("deployment",), component["id"], ui[0]["sha256"],
            group=declaration.get("group", "deployment"),
            order=declaration.get("order", 10),
        ),)
    descriptor = package_runtime_descriptor(
        snapshot, bot_id=None, scope_id=runtime.scope_id,
        module_configuration=configuration, grants=grants, resolved_contracts={},
        generation_verifier=runtime.generation_authority.verify_active_generation,
        generation_member_verifier=runtime.generation_authority.verify_generation_member,
        state_queries=None, state_commands=None, domain_queries=domain,
        domain_commands=domain,
        owned_work_authority=domain if component.get("owned_work_handlers") else None,
        service_operation_ledger=runtime.service_operation_ledger,
        storage=storage, trust_level="official",
        effect_driver_factories=effect_factories,
        management=management,
        management_contract_factory=lambda adapter: _ReadOnlyManagementContract(
            configuration, adapter=adapter, schema=config_schema
        ),
        plan_only=plan_only,
    )
    return descriptor


async def bootstrap_installed_deployment_package(snapshot, runtime, *, plan_only=False):
    """Compile a declaration-only deployment Worker without official SQL authority."""

    component = snapshot.component
    allowed = {
        "id", "scope", "state_schema", "reload_mode", "config_schema",
        "bootstrap_config", "resources", "lifecycle", "capabilities",
        "provides", "management_queries", "management_ui", "config_immutable_paths",
    }
    if (
        component["scope"] != "deployment"
        or set(component) - allowed
        or component.get("capabilities", {}).get("requests")
        or any(item["semantics"] != "read" for item in component["provides"])
    ):
        raise ForestError(
            "DEPLOYMENT_PACKAGE_PORT_UNAVAILABLE",
            "Installed deployment packages currently support only pure read services",
            422,
        )
    config_schema = snapshot.schema(component["config_schema"])
    config_value = snapshot.bootstrap_configuration()
    if not Draft202012Validator(config_schema).is_valid(config_value):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID", "Deployment bootstrap configuration is invalid", 422
        )
    spec, baseline = _configuration_storage(component["id"])
    if not plan_only:
        await runtime.schema_bundle_installer.install(
            spec, baseline, storage_bundle_digest(spec, "official"),
            execution_profile="official",
        )
    configuration = DeploymentModuleConfigurationRepository(
        runtime.store, component_id=component["id"], namespace=spec.namespace,
        immutable_paths=tuple(component.get("config_immutable_paths", ())),
    )
    if not plan_only:
        await configuration.ensure_initialized(
            operation_id=f"bootstrap:{component['id']}:configuration", value=config_value,
            validation_digest=digest_json(config_schema),
        )
        desired = await configuration.desired_checked()
        if desired.validation_digest != digest_json(config_schema) or not (
            Draft202012Validator(config_schema).is_valid(desired.value)
        ):
            raise ForestError(
                "PACKAGE_CONFIGURATION_SCHEMA_MISMATCH",
                "Deployment configuration differs from the frozen package schema", 409,
            )
    grants = _BuiltinGrantSet(
        (), digest_json(["forest.deployment.installed-grants/1", snapshot.digest, component["id"]]),
    )
    ui = snapshot.management_ui()
    management = ()
    if ui is not None:
        declaration = component["management_ui"]
        management = (RuntimePackageManagementContribution(
            declaration.get("contribution_id", "configuration"),
            declaration.get("label", component["id"]),
            ("deployment",), component["id"], ui[0]["sha256"],
            group=declaration.get("group", "deployment"),
            order=declaration.get("order", 10),
        ),)
    return package_runtime_descriptor(
        snapshot, bot_id=None, scope_id=runtime.scope_id,
        module_configuration=configuration, grants=grants, resolved_contracts={},
        generation_verifier=runtime.generation_authority.verify_active_generation,
        generation_member_verifier=runtime.generation_authority.verify_generation_member,
        state_queries=None, state_commands=None, domain_queries=None,
        service_operation_ledger=None, storage=None, trust_level="installed",
        management=management,
        management_contract_factory=lambda adapter: _ReadOnlyManagementContract(
            configuration, adapter=adapter, schema=config_schema
        ),
        plan_only=plan_only,
    )
