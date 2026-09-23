from forest.errors import ForestError
from forest.sdk import Plugin


class Delivery(Plugin):
    """Own Delivery state; durable dispatch handlers are added by the same package."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.delivery/1"}:
            raise ForestError(
                "DELIVERY_CONFIGURATION_INVALID",
                "Delivery configuration protocol is unsupported",
                422,
            )
