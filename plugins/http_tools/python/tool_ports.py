from forest.sdk import service


class ToolPorts:
    @service("tools.http.http-get", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def http_get(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.read_http)

    @service("tools.http.http-get.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def http_get_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.http.http-request", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def http_request(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.write_http)

    @service("tools.http.http-request.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def http_request_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)
