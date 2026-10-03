"""Deployment-owned package directory projection."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class DeploymentPackageRegistry(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-package-registry/1"}:
            raise ForestError(
                "DEPLOYMENT_PACKAGE_REGISTRY_CONFIGURATION_INVALID",
                "Deployment package registry configuration is invalid", 422,
            )

    @service("deployment.package-registry.page", semantics="read")
    async def page(self, payload, ctx):
        return await ctx.services.call(
            "deployment.catalog.page", payload,
            operation_id=ctx.operations.stable("catalog-page"), timeout=30,
        )

    @service("deployment.package-registry.artifact", semantics="read")
    async def artifact(self, payload, ctx):
        return await ctx.services.call(
            "deployment.catalog.artifact", payload,
            operation_id=ctx.operations.stable("catalog-artifact"), timeout=30,
        )

    @service("deployment.package-registry.import-receipt", semantics="read")
    async def import_receipt(self, payload, ctx):
        return await ctx.services.call(
            "deployment.catalog.import-receipt", payload,
            operation_id=ctx.operations.stable("catalog-import-receipt"), timeout=30,
        )
