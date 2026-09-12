from forest.sdk import Plugin, service


class Counter(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.label = config["label"]
        self.multiplier = config["multiplier"]

    @service("counter.increment")
    async def increment(self, payload, ctx):
        current = await ctx.state.get("counter")
        value = (current["value"] or 0) + payload["increment"] * self.multiplier
        receipt = await ctx.state.commit(
            "counter", value, expected_revision=current["revision"],
            operation_id=ctx.operations.stable("increment:v1"),
        )
        return {"value": value, "revision": receipt["revision"], "label": self.label}
