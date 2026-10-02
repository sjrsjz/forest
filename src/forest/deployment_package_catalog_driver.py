"""Generation-bound read port for the deployment-wide package artifact catalog."""

from __future__ import annotations

import asyncio

from forest.errors import ForestError

_SERVICES = frozenset({"deployment.catalog.page", "deployment.catalog.artifact"})


class DeploymentPackageCatalogEffectDriver:
    def __init__(self, configuration, configuration_revision, identity, declaration, *, catalog):
        if (
            declaration.get("name") not in _SERVICES
            or declaration.get("kind") != "deployment.package-catalog"
            or declaration.get("semantics") != "read"
            or identity.component_id != "deployment.package-registry"
            or catalog is None
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Deployment package catalog driver received another port", 500,
            )
        self.service_id = declaration["name"]
        self.catalog = catalog
        self.identity = identity
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False

    async def invoke(self, payload, *, scope, invocation) -> dict:
        if (
            not self.accepting
            or scope.scope_kind != "deployment"
            or scope.component_id != "deployment.package-registry"
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Deployment catalog Generation is not admitting reads", 409,
            )
        if self.service_id == "deployment.catalog.page":
            return await asyncio.to_thread(self.catalog.page, **payload)
        return await asyncio.to_thread(self.catalog.artifact, payload["package_digest"])
