"""Deployment audit package lifecycle; audit projection lives in its SQL domain."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class HostAudit(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.host-audit/1"}:
            raise ForestError(
                "HOST_AUDIT_CONFIGURATION_INVALID",
                "Deployment audit configuration is invalid", 422,
            )

    @service("host.audit.query", semantics="read")
    async def query(self, payload, ctx):
        return await ctx.queries.call("host.audit.page", payload)
