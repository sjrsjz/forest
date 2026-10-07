"""Reloadable Worker which owns the Host storage request protocol."""

from forest.errors import ForestError
from forest.sdk import Plugin, job


class DeploymentStorageOrchestrator(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-storage-orchestrator/1"}:
            raise ForestError(
                "STORAGE_ORCHESTRATOR_CONFIGURATION_INVALID",
                "Storage orchestrator configuration is invalid",
                422,
            )

    @job("deployment.storage-orchestrator.execute")
    async def advance(self, task, ctx):
        request = task["payload"]
        request_id = request["request_id"]
        inspected = await ctx.services.call(
            "deployment.storage-orchestrator.inspect", request,
            operation_id=ctx.operations.stable("inspect:v1"), timeout=15,
        )
        if inspected["state"] == "SETTLED":
            return {"request_id": request_id, "state": "SETTLED"}
        try:
            await ctx.services.call(
                "deployment.storage-orchestrator.enqueue", request,
                operation_id=ctx.operations.stable("enqueue:v1"), timeout=30,
            )
            status = await ctx.services.call(
                "deployment.storage-orchestrator.status", request,
                operation_id=ctx.operations.stable("status:v1"), timeout=15,
            )
        except ForestError as exc:
            if exc.code in {
                "STORAGE_REQUEST_HOST_UNAVAILABLE",
                "HOST_STORAGE_GENERATION_UNAVAILABLE",
            }:
                return {
                    "request_id": request_id,
                    "state": "WAITING",
                    "error_code": exc.code,
                }
            raise
        if status["state"] == "FAILED":
            return {
                "request_id": request_id,
                "state": "BLOCKED",
                "error_code": "HOST_MATERIALIZATION_FAILED",
            }
        if status["state"] != "SUCCEEDED":
            return {"request_id": request_id, "state": "WAITING"}
        await ctx.services.call(
            "deployment.storage-orchestrator.pin", request,
            operation_id=ctx.operations.stable("pin:v1"), timeout=30,
        )
        await ctx.services.call(
            "deployment.storage-orchestrator.finalize", request,
            operation_id=ctx.operations.stable("finalize:v1"), timeout=30,
        )
        return {"request_id": request_id, "state": "SETTLED"}
