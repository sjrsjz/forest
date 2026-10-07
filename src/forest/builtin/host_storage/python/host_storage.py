"""Host-owned physical resource ledger; physical Effects remain separate."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class HostStorage(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.host-storage/1"}:
            raise ForestError(
                "HOST_STORAGE_CONFIGURATION_INVALID",
                "Host storage configuration is invalid", 422,
            )

    @service("host.storage.generations", semantics="read")
    async def generations(self, payload, ctx):
        return await ctx.queries.call("host.storage.generations.read", payload)

    @service("host.storage.reserve", semantics="idempotent")
    async def reserve(self, payload, ctx):
        return await ctx.commands.call(
            "host.storage.reserve-record", payload,
            operation_id=ctx.operations.stable("reserve:v1"),
        )
