from forest.sdk import Plugin, job, service


class Counter(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.label = config["label"]
        self.multiplier = config["multiplier"]

    @service("counter.increment")
    async def increment(self, payload, ctx):
        current = await ctx.queries.call("counter.current", {})
        value = (current["value"] or 0) + payload["increment"] * self.multiplier
        receipt = await ctx.commands.call(
            "counter.set",
            {"value": value, "expected_revision": current["revision"]},
            operation_id=ctx.operations.stable("increment:v1"),
        )
        return {"value": value, "revision": receipt["revision"], "label": self.label}

    @service("counter.defer")
    async def defer(self, payload, ctx):
        return await ctx.commands.call(
            "counter.enqueue",
            payload,
            operation_id=ctx.operations.stable("enqueue:v1"),
        )

    @job("counter.apply")
    async def apply(self, task, ctx):
        payload = task["payload"]
        return await ctx.commands.call(
            "counter.set",
            payload,
            operation_id=ctx.operations.stable("apply:v1"),
        )
