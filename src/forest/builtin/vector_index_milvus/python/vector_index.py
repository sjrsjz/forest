from forest.sdk import Plugin


class MilvusVectorIndex(Plugin):
    """Package Worker owns declaration and lifecycle; I/O stays in its effect driver."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.space_id = config["space_id"]
        self.dimensions = config["dimensions"]

    def replacement_facts(self):
        return {"space": {
            "space_id": self.space_id,
            "dimensions": self.dimensions,
        }}
