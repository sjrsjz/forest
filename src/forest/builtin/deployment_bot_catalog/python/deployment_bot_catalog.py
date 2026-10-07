"""Reloadable Bot catalog projection; identity and runtime intent remain kernel-owned."""

from forest.errors import ForestError
from forest.sdk import Plugin


class DeploymentBotCatalog(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-bot-catalog/1"}:
            raise ForestError(
                "BOT_CATALOG_CONFIGURATION_INVALID",
                "Bot catalog configuration is invalid", 422,
            )
