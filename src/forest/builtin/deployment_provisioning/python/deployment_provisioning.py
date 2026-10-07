"""Reloadable provisioning Job executor; Job state stays in the kernel authority."""

from forest.errors import ForestError
from forest.sdk import Plugin, job


class DeploymentProvisioning(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-provisioning/1"}:
            raise ForestError(
                "DEPLOYMENT_PROVISIONING_CONFIGURATION_INVALID",
                "Provisioning configuration is invalid", 422,
            )

    @job("deployment.provisioning.execute")
    async def execute_job(self, task, ctx):
        return await ctx.services.call(
            "deployment.provisioning.materialize", task["payload"],
            operation_id=ctx.operations.stable("materialize:v1"),
        )
