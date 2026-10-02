"""Scope-owned ports supplied to deployment module generations."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.deployment_catalog import DeploymentSupervisorLease
from forest.storage.deployment_generation import DeploymentGenerationAuthority
from forest.storage.deployment_schema_bundles import DeploymentSchemaBundleInstaller


class DeploymentRuntimeScope:
    scope_kind = "deployment"

    def __init__(self, store, supervisor) -> None:
        self.store = store
        self.scope_id = store.deployment_id
        self.supervisor = supervisor
        self.generation_authority = DeploymentGenerationAuthority(store)
        self.schema_bundle_installer = DeploymentSchemaBundleInstaller(store)

    def authority_lease(self, *, allow_closing: bool = False) -> DeploymentSupervisorLease:
        lease = self.supervisor.authority_lease(allow_closing=allow_closing)
        if type(lease) is not DeploymentSupervisorLease or lease.deployment_id != self.scope_id:
            raise ForestError(
                "DEPLOYMENT_RUNTIME_UNAVAILABLE",
                "Deployment Supervisor lease is unavailable", 503,
            )
        return lease
