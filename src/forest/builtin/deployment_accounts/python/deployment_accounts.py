"""Reloadable Web account management projection; credentials stay in the host."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class DeploymentAccounts(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-accounts/1"}:
            raise ForestError(
                "DEPLOYMENT_ACCOUNTS_CONFIGURATION_INVALID",
                "Account management configuration is invalid", 422,
            )

    @service("deployment.accounts.page", semantics="read")
    async def page(self, payload, ctx):
        return await ctx.queries.call("deployment.accounts.metadata.page", payload)
