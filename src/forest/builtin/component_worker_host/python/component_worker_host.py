"""Component control projection in an isolated, hot-reloadable package Worker."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


class ComponentWorkerHost(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.component-worker-host/1"}:
            raise ForestError(
                "COMPONENT_HOST_CONFIGURATION_INVALID",
                "Component Worker Host configuration is invalid",
                422,
            )

    async def _graph(self, ctx):
        return await ctx.services.call(
            "runtime.graph.snapshot",
            {"protocol": "forest.runtime-graph-snapshot-request/1"},
            operation_id=ctx.operations.stable("graph-snapshot"),
            timeout=10,
        )

    @service("component.host.modules", semantics="read")
    async def modules(self, payload, ctx):
        graph = await self._graph(ctx)
        expected_graph = payload.get("graph_id")
        if expected_graph is not None and expected_graph != graph["graph_id"]:
            raise ForestError(
                "COMPONENT_HOST_GRAPH_CHANGED",
                "RuntimeGraph changed during component pagination",
                409,
            )
        after = payload.get("after", "")
        limit = payload.get("limit", 32)
        members = sorted(graph["members"], key=lambda item: item["component_id"])
        following = [item for item in members if item["component_id"] > after]
        page = following[:limit]
        return {
            "protocol": "forest.component-host-modules/1",
            "bot_id": graph["bot_id"],
            "graph_id": graph["graph_id"],
            "graph_revision": graph["graph_revision"],
            "members": [
                {
                    "component_id": item["component_id"],
                    "status": item["status"],
                    "package_id": item["package_id"],
                    "package_version": item["package_version"],
                }
                for item in page
            ],
            "next_after": page[-1]["component_id"] if len(following) > limit else None,
        }

    @service("component.host.module", semantics="read")
    async def module(self, payload, ctx):
        graph = await self._graph(ctx)
        member = next(
            (
                item for item in graph["members"]
                if item["component_id"] == payload["component_id"]
            ),
            None,
        )
        if member is None:
            raise ForestError(
                "COMPONENT_HOST_MODULE_NOT_FOUND", "Component is not in the active graph", 404
            )
        return {
            "protocol": "forest.component-host-module/1",
            "bot_id": graph["bot_id"],
            "graph_id": graph["graph_id"],
            "graph_revision": graph["graph_revision"],
            "member": member,
        }

    @service("shell.component-host.execute", semantics="at-most-once")
    async def shell(self, payload, ctx):
        graph = await self._graph(ctx)
        return {
            "text": "Forest RuntimeGraph 模块：\n"
            + "\n".join(
                f"{item['component_id']} {item['status']} "
                f"r{item['config_revision']} {item['generation_id']}"
                for item in graph["members"]
            )
        }

    @service("shell.component-host.reload", semantics="idempotent")
    async def reload(self, payload, ctx):
        arguments = payload["arguments"]
        component_id = arguments["component_id"]
        graph = await self._graph(ctx)
        if not any(item["component_id"] == component_id for item in graph["members"]):
            raise ForestError(
                "COMPONENT_HOST_MODULE_NOT_FOUND",
                "Component is not in the active graph",
                404,
            )
        receipt = await ctx.services.call(
            "runtime.graph-replacement.request",
            {
                "protocol": "forest.runtime-graph-replacement-request/1",
                "operation_id": ctx.operations.stable("module-reload-request"),
                "expected_graph_revision": arguments["expected_graph_revision"],
                "requested_by": payload["principal_ref"],
            },
            operation_id=ctx.operations.stable("submit-graph-replacement"),
            timeout=10,
        )
        if receipt["expected_graph_revision"] != graph["graph_revision"]:
            return {
                "text": (
                    f"此前 Graph r{receipt['expected_graph_revision']} 的组件重载请求"
                    f"已记录（{receipt['state']}）；operation={receipt['operation_id']}"
                )
            }
        affected = ", ".join(item["component_id"] for item in graph["members"])
        return {
            "text": (
                f"组件 {component_id} 的重载请求已记录（{receipt['state']}）；"
                f"当前实现会更换整张 Graph 的 {len(graph['members'])} 个 Generation："
                f"{affected}；operation={receipt['operation_id']}"
            )
        }
