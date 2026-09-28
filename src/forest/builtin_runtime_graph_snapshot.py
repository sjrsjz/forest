"""Read-only RuntimeGraph projection for component control packages."""

from __future__ import annotations

from forest.errors import ForestError


def runtime_graph_snapshot_service(context):
    runtime = context.participant("kernel.runtime").service

    async def read(request):
        if request != {"protocol": "forest.runtime-graph-snapshot-request/1"}:
            raise ForestError(
                "RUNTIME_GRAPH_SNAPSHOT_INPUT_INVALID",
                "RuntimeGraph snapshot request is invalid",
                422,
            )
        status = await runtime.status()
        return {
            "protocol": "forest.runtime-graph-snapshot/1",
            "bot_id": status["bot_id"],
            "graph_id": status["graph_id"],
            "graph_revision": status["graph_revision"],
            "members": [
                {
                    "component_id": item["component_id"],
                    "generation_id": item["generation_id"],
                    "status": item["status"],
                    "config_revision": item["config_revision"],
                    "package_id": item["package_id"],
                    "package_version": item["package_version"],
                    "package_digest": item["package_digest"],
                    "data_admission": item["data_admission"],
                    "control_admission": item["control_admission"],
                }
                for item in status["members"]
            ],
        }

    return read
