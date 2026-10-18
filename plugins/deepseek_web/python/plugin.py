import json
from urllib.parse import urlsplit

from tool_ports import ToolPorts

from forest.errors import ForestError
from forest.sdk import Plugin
from forest.sdk_tools import AgentToolExecutor, ToolRejected, plain


class Tools(ToolPorts, Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.config = plain(config)
        self.executor = AgentToolExecutor("tools.deepseek-web")

    async def _api(self, path, body, ctx):
        try:
            response = await ctx.services.call(
                "tools.deepseek-web.http",
                {"method": "POST", "url": self.config["endpoint"].rstrip("/") + path,
                 "headers": {"Content-Type": "application/json"},
                 "body": json.dumps(body, ensure_ascii=False), "read_only": True},
                operation_id=ctx.operations.stable("deepseek-request"), timeout=65,
            )
        except ForestError as error:
            raise ToolRejected(error.code, "DeepSeek read did not return usable evidence", error.status) from None
        if response["outcome"] != "RECEIVED":
            raise ToolRejected(response["error_code"], "DeepSeek request did not return a response", 502)
        if response["status_code"] != 200:
            raise ToolRejected("DEEPSEEK_HTTP_" + str(response["status_code"]), "DeepSeek rejected the tool request", 502)
        try:
            value = json.loads(await ctx.payloads.read_bytes(response["body"]))
        except ForestError as error:
            raise ToolRejected(error.code, "DeepSeek evidence is unavailable", error.status) from None
        except (ValueError, UnicodeError):
            raise ToolRejected("DEEPSEEK_RESPONSE_INVALID", "DeepSeek returned invalid JSON", 502) from None
        if not isinstance(value, dict):
            raise ToolRejected("DEEPSEEK_RESPONSE_INVALID", "DeepSeek returned no response object", 502)
        return value, plain(response["body"])

    async def search(self, command, ctx):
        args = command["arguments"]
        if not args["query"].strip():
            raise ToolRejected("SEARCH_QUERY_EMPTY", "Search query cannot be blank", 422)
        requested = min(args.get("count", 5), self.config["max_results"])
        body = {"query": args["query"], "count": requested,
                "need_content": args.get("need_content", False),
                "localization": args.get("localization", "")}
        if args.get("ori_question") is not None:
            body["ori_question"] = args["ori_question"]
        value, source = await self._api("/v1/tools/search", body, ctx)
        rows = value.get("results")
        if not isinstance(rows, list):
            raise ToolRejected("DEEPSEEK_RESPONSE_INVALID", "Search response has no results array", 502)
        results = []
        budget = self.config["max_result_chars"]
        excerpt_limit = max(256, budget // max(1, requested))
        for row in rows[:requested]:
            if (not isinstance(row, dict) or not isinstance(row.get("title"), str)
                    or not isinstance(row.get("url"), str)
                    or not isinstance(row.get("summary"), str)
                    or not isinstance(row.get("content"), str)):
                raise ToolRejected("DEEPSEEK_RESPONSE_INVALID", "Search result fields are invalid", 502)
            result = {"title": row["title"], "url": row["url"], "date": row.get("date"),
                      "site_name": row.get("site_name"), "rerank_score": row.get("rerank_score"),
                      "authority": row.get("authority"),
                      "summary": row["summary"][:excerpt_limit],
                      "content": row["content"][:excerpt_limit] if body["need_content"] else ""}
            if len(json.dumps(results + [result], ensure_ascii=False)) > budget:
                break
            results.append(result)
        return {"query": args["query"], "results": results, "returned": len(results),
                "available": len(rows), "source_payload": source,
                "truncated": len(results) < len(rows) or any(
                    len(row["summary"]) > excerpt_limit
                    or (body["need_content"] and len(row["content"]) > excerpt_limit)
                    for row in rows[:len(results)]),
                "content_role": "untrusted_web_evidence"}

    async def fetch(self, command, ctx):
        url = command["arguments"]["url"]
        try:
            parsed = urlsplit(url)
            if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                    or parsed.username is not None or parsed.password is not None):
                raise ValueError
        except ValueError:
            raise ToolRejected("FETCH_URL_INVALID", "Fetch requires an HTTP(S) URL without credentials", 422) from None
        value, source = await self._api("/v1/tools/fetch", {"url": url}, ctx)
        if not isinstance(value.get("title"), str) or not isinstance(value.get("content"), str):
            raise ToolRejected("DEEPSEEK_RESPONSE_INVALID", "Fetch response fields are invalid", 502)
        content = value["content"]
        limit = self.config["max_result_chars"]
        return {"url": url, "title": value["title"], "content": content[:limit],
                "total_chars": len(content), "truncated": len(content) > limit,
                "source_payload": source, "content_role": "untrusted_web_evidence"}
