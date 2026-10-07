"""Deployment package controller and deployment-only registry query bridge."""

from forest.control_package_controller import ControlPackageController
from forest.errors import ForestError


class DeploymentPackageController(ControlPackageController):
    def __init__(self, runtime, catalog, *, official_sources):
        if getattr(runtime, "scope_kind", None) != "deployment":
            raise ValueError("Deployment controller requires deployment scope")
        super().__init__(runtime, catalog, official_sources=official_sources)

    async def read_package_registry(self, query_id: str, payload: dict) -> dict:
        """Resolve one read through the current deployment registry Generation."""

        if query_id not in {"packages", "artifact"} or not isinstance(payload, dict):
            raise ForestError(
                "DEPLOYMENT_PACKAGE_QUERY_INVALID", "Package registry query is invalid", 422
            )
        registry = self.registry
        if registry.states.get("deployment.package-registry") != "ACTIVE":
            raise ForestError(
                "DEPLOYMENT_PACKAGE_REGISTRY_UNAVAILABLE",
                "Deployment package registry Generation is unavailable", 503,
            )
        return await registry.get("deployment.package-registry").management_contract.query(
            query_id, payload
        )
