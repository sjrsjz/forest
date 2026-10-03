"""Reloadable Secret metadata projection; plaintext never enters this Worker."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class DeploymentSecrets(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-secrets/1"}:
            raise ForestError(
                "DEPLOYMENT_SECRETS_CONFIGURATION_INVALID",
                "Secret management configuration is invalid", 422,
            )

    @service("deployment.secrets.page", semantics="read")
    async def page(self, payload, ctx):
        return await ctx.queries.call("deployment.secrets.metadata.page", payload)
