from forest.sdk import service


class ToolPorts:
    @service("tools.deepseek-web.web-search", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def web_search(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.search)

    @service("tools.deepseek-web.web-search.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def web_search_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.deepseek-web.web-fetch", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def web_fetch(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.fetch)

    @service("tools.deepseek-web.web-fetch.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def web_fetch_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)
