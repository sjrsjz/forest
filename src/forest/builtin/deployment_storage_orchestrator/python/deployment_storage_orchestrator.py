"""Reloadable Worker which advances one frozen Host storage request."""

from forest.errors import ForestError
from forest.sdk import Plugin, job


class DeploymentStorageOrchestrator(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-storage-orchestrator/1"}:
            raise ForestError(
                "STORAGE_ORCHESTRATOR_CONFIGURATION_INVALID",
                "Storage orchestrator configuration is invalid",
                422,
            )

    @job("deployment.storage-orchestrator.execute")
    async def advance(self, task, ctx):
        return await ctx.services.call(
            "deployment.storage-orchestrator.advance",
            task["payload"],
            operation_id=ctx.operations.stable("advance:v1"),
            timeout=220,
        )
