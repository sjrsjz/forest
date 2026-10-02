"""Deployment audit package lifecycle; audit projection lives in its SQL domain."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class DeploymentAudit(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-audit/1"}:
            raise ForestError(
                "DEPLOYMENT_AUDIT_CONFIGURATION_INVALID",
                "Deployment audit configuration is invalid", 422,
            )

    @service("deployment.audit.query", semantics="read")
    async def query(self, payload, ctx):
        return await ctx.queries.call("deployment.audit.page", payload)
