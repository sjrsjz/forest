"""Scope-owned ports supplied to deployment module generations."""

from __future__ import annotations

from forest.deployment_package_catalog_driver import DeploymentPackageCatalogEffectDriver
from forest.deployment_service_ledger import DeploymentPackageServiceOperationLedger
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.deployment_catalog import DeploymentSupervisorLease
from forest.storage.deployment_generation import DeploymentGenerationAuthority
from forest.storage.deployment_schema_bundles import DeploymentSchemaBundleInstaller


class DeploymentRuntimeScope:
    scope_kind = "deployment"

    def __init__(
        self, store, supervisor, *, package_catalog=None,
        kernel_management_handlers=None,
        kernel_query_handlers=None,
        graph_change_authority=None,
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
        self.graph_change_authority = graph_change_authority
        self.kernel_management_handlers = dict(kernel_management_handlers or {})
        self.kernel_query_handlers = dict(kernel_query_handlers or {})
        self.kernel_owned_work_authorities = {}
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

    async def apply_configuration(
        self, configuration, *, operation_id: str, plan_id: str,
        expected_desired_revision: int, authorization, authorization_guard,
        validation_digest: str, authorization_replay_guard=None,
    ) -> dict:
        if self.graph_change_authority is None or authorization_guard is None:
            raise ForestError(
                "DEPLOYMENT_CONFIGURATION_CHANGE_UNAVAILABLE",
                "Deployment configuration change authority is unavailable", 503,
            )
        body = {
            "plan_id": plan_id,
            "expected_desired_revision": expected_desired_revision,
        }

        async def authorize_in(conn):
            return await authorization_guard(
                conn, ticket=authorization, component_id=configuration.component_id,
                operation_id=operation_id, action_key="configuration.apply",
                input_digest=digest_json(body),
                plan_digest=digest_json({"plan_id": plan_id}),
            )

        result = await self.graph_change_authority.enqueue_configuration(
            lease=self.authority_lease(), ticket=authorization,
            configuration=configuration, plan_id=plan_id,
            expected_desired_revision=expected_desired_revision,
            validation_digest=validation_digest,
            authorize_in=authorize_in,
        )
        return {
            "configuration": result["configuration"],
            "rebuild": {
                "request_id": result["request"]["operation_id"],
                "state": result["request"]["state"],
            },
        }
