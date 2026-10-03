"""Scope-owned ports supplied to deployment module generations."""

from __future__ import annotations

from forest.deployment_package_catalog_driver import DeploymentPackageCatalogEffectDriver
from forest.deployment_service_ledger import DeploymentPackageServiceOperationLedger
from forest.errors import ForestError
from forest.storage.deployment_catalog import DeploymentSupervisorLease
from forest.storage.deployment_generation import DeploymentGenerationAuthority
from forest.storage.deployment_schema_bundles import DeploymentSchemaBundleInstaller


class DeploymentRuntimeScope:
    scope_kind = "deployment"

    def __init__(
        self, store, supervisor, *, package_catalog=None,
        kernel_management_handlers=None,
    ) -> None:
        self.store = store
        self.scope_id = store.deployment_id
        self.supervisor = supervisor
        self.generation_authority = DeploymentGenerationAuthority(store)
        self.service_operation_ledger = DeploymentPackageServiceOperationLedger(
            store, self.generation_authority,
        )
        self.schema_bundle_installer = DeploymentSchemaBundleInstaller(store)
        self.package_catalog = package_catalog
        self.kernel_management_handlers = dict(kernel_management_handlers or {})
        self.effect_driver_factories = (
            {
                "deployment.package-catalog": (
                    lambda configuration, revision, identity, declaration: (
                        DeploymentPackageCatalogEffectDriver(
                            configuration, revision, identity, declaration,
                            catalog=package_catalog,
                        )
                    )
                )
            }
            if package_catalog is not None else {}
        )

    def authority_lease(self, *, allow_closing: bool = False) -> DeploymentSupervisorLease:
        lease = self.supervisor.authority_lease(allow_closing=allow_closing)
        if type(lease) is not DeploymentSupervisorLease or lease.deployment_id != self.scope_id:
            raise ForestError(
                "DEPLOYMENT_RUNTIME_UNAVAILABLE",
                "Deployment Supervisor lease is unavailable", 503,
            )
        return lease
