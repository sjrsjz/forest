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
