from email.message import Message

from tool_ports import ToolPorts

from forest.errors import ForestError
from forest.sdk import Plugin
from forest.sdk_tools import AgentToolExecutor, ToolOutcome, ToolRejected, plain


class Tools(ToolPorts, Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.config = plain(config)
        self.executor = AgentToolExecutor("tools.http")

    async def read_http(self, command, ctx):
        return await self._request(command, ctx, read_only=True)

    async def write_http(self, command, ctx):
        if command["authority_principal_ref"] not in self.config["write_principals"]:
            raise ToolRejected("HTTP_WRITE_DENIED", "Trigger principal cannot perform HTTP writes", 403)
        return await self._request(command, ctx, read_only=False)

    async def _request(self, command, ctx, *, read_only):
        args = command["arguments"]
        header_rows = args.get("headers", [])
        names = [row["name"].lower() for row in header_rows]
        if len(names) != len(set(names)):
            raise ToolRejected("HTTP_HEADERS_INVALID", "Duplicate request headers are unsupported", 422)
        try:
            response = await ctx.services.call(
                "tools.http.http", {"method": args.get("method", "GET"), "url": args["url"],
                                    "headers": {row["name"]: row["value"] for row in header_rows},
                                    "body": args.get("body"), "read_only": read_only},
                operation_id=ctx.operations.stable("http-request"), timeout=65,
            )
        except ForestError as error:
            if read_only:
                raise ToolRejected(error.code, "HTTP read did not return a response", error.status) from None
            raise
        if response["outcome"] != "RECEIVED":
            return ToolOutcome(response["outcome"], error_code=response["error_code"])
        try:
            body = await ctx.payloads.read_bytes(response["body"])
        except ForestError as error:
            if read_only:
                raise ToolRejected(error.code, "HTTP response body is unavailable", error.status) from None
            raise
        content_type = response["headers"].get("content-type", "application/octet-stream")
        header = Message()
        header["content-type"] = content_type
        encoding = header.get_content_charset("utf-8")
        visible = None
        if (response["headers"].get("content-encoding", "identity") == "identity"
                and (content_type.startswith("text/") or "json" in content_type or "xml" in content_type)):
            try:
                visible = body.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                pass
        limit = self.config["max_result_chars"]
        return {"url": args["url"], "status_code": response["status_code"],
                "http_success": 200 <= response["status_code"] < 300,
                "headers": plain(response["headers"]), "byte_length": response["byte_length"],
                "body_text": visible[:limit] if visible is not None else None,
                "truncated": visible is not None and len(visible) > limit,
                "body_payload": plain(response["body"]), "content_role": "untrusted_http_response"}
