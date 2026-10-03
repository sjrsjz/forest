"""Reloadable provisioning Job projection; durable writes use exact kernel ports."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class DeploymentProvisioning(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-provisioning/1"}:
            raise ForestError(
                "DEPLOYMENT_PROVISIONING_CONFIGURATION_INVALID",
                "Provisioning configuration is invalid", 422,
            )

    @service("deployment.provisioning.jobs.page", semantics="read")
    async def jobs(self, payload, ctx):
        return await ctx.queries.call("deployment.provisioning.jobs.read", payload)
