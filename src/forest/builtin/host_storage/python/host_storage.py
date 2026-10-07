"""Host-owned physical resource ledger; physical Effects remain separate."""

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


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

    @service("host.storage.materialization.status", semantics="read")
    async def materialization_status(self, payload, ctx):
        return await ctx.queries.call("host.storage.materialization-status-read", payload)

    @service("host.storage.resource", semantics="read")
    async def resource(self, payload, ctx):
        return await ctx.queries.call("host.storage.resource-read", payload)

    @service("host.storage.reference.pin", semantics="idempotent")
    async def pin_reference(self, payload, ctx):
        return await ctx.commands.call(
            "host.storage.reference-pin-record", payload,
            operation_id=ctx.operations.stable("reference-pin:v1"),
        )

    @service("host.storage.reference.release", semantics="idempotent")
    async def release_reference(self, payload, ctx):
        return await ctx.commands.call(
            "host.storage.reference-release-record", payload,
            operation_id=ctx.operations.stable("reference-release:v1"),
        )

    @service("host.storage.reserve", semantics="idempotent")
    async def reserve(self, payload, ctx):
        return await ctx.commands.call(
            "host.storage.reserve-record", payload,
            operation_id=ctx.operations.stable("reserve:v1"),
        )

    @service("host.storage.materialization.enqueue", semantics="idempotent")
    async def enqueue_materialization(self, payload, ctx):
        return await ctx.commands.call(
            "host.storage.materialization-enqueue-record", payload,
            operation_id=ctx.operations.stable("materialization-enqueue:v1"),
        )

    @job("host.storage.materialize")
    async def materialize(self, task, ctx):
        receipt = await ctx.services.call(
            "host.storage.postgres.materialize", task["payload"],
            operation_id=ctx.operations.stable("physical-materialize:v1"),
        )
        return await ctx.commands.call(
            "host.storage.realize-record", receipt,
            operation_id=ctx.operations.stable("materialization-realize:v1"),
        )
