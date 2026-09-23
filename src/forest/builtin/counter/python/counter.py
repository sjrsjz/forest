from forest.sdk import Plugin, job, service


class Counter(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.label = config["label"]
        self.multiplier = config["multiplier"]

    @service("counter.increment", semantics="at-most-once")
    async def increment(self, payload, ctx):
        current = await ctx.queries.call("counter.current", {})
        value = (current["value"] or 0) + payload["increment"] * self.multiplier
        receipt = await ctx.commands.call(
            "counter.set",
            {"value": value, "expected_revision": current["revision"]},
            operation_id=ctx.operations.stable("increment:v1"),
        )
        return {"value": value, "revision": receipt["revision"], "label": self.label}

    @service("counter.defer", semantics="at-most-once")
    async def defer(self, payload, ctx):
        return await ctx.commands.call(
            "counter.enqueue",
            payload,
            operation_id=ctx.operations.stable("enqueue:v1"),
        )

    @service("counter.transaction", semantics="at-most-once")
    async def transaction(self, payload, ctx):
        return await ctx.uow.call(
            "counter.atomic-set",
            payload,
            operation_id=ctx.operations.stable("atomic-set:v1"),
        )

    @service("counter.transaction-reject", semantics="at-most-once")
    async def transaction_reject(self, payload, ctx):
        return await ctx.uow.call(
            "counter.atomic-set-reject",
            payload,
            operation_id=ctx.operations.stable("atomic-set-reject:v1"),
        )

    @service("counter.cross-component-probe", semantics="at-most-once")
    async def cross_component_reject(self, payload, ctx):
        return await ctx.uow.call(
            "counter.cross-component-reject",
            payload,
            operation_id=ctx.operations.stable("cross-component-reject:v1"),
        )

    @job("counter.apply")
    async def apply(self, task, ctx):
        payload = task["payload"]
        return await ctx.commands.call(
            "counter.set",
            payload,
            operation_id=ctx.operations.stable("apply:v1"),
        )
