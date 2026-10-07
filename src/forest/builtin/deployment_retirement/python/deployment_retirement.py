"""Reloadable retirement Worker; the kernel owns Bot identity and its Job."""

from forest.errors import ForestError
from forest.sdk import Plugin, job

_WAITING = {
    "BOT_RETIREMENT_NOT_QUIESCENT",
    "HOST_STORAGE_GENERATION_UNAVAILABLE",
    "STORAGE_REQUEST_HOST_UNAVAILABLE",
}
_BLOCKED = {
    "STORAGE_PROFILE_NOT_HOST_OWNED",
    "STORAGE_PROFILE_RETIREMENT_MISMATCH",
    "HOST_STORAGE_RELEASE_PIN_MISMATCH",
    "HOST_STORAGE_RECLAIM_RELEASE_MISMATCH",
    "HOST_STORAGE_RECLAIM_RESOURCE_MISMATCH",
    "HOST_STORAGE_REFERENCE_CONFLICT",
}


class DeploymentRetirement(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.deployment-retirement/1"}:
            raise ForestError(
                "RETIREMENT_CONFIGURATION_INVALID", "Retirement configuration is invalid", 422,
            )

    @job("deployment.retirement.execute")
    async def execute(self, task, ctx):
        request = task["payload"]
        bot_id = request["bot_id"]
        try:
            for name in ("quiescence", "revoke", "release", "reclaim"):
                await ctx.services.call(
                    "deployment.retirement." + name, request,
                    operation_id=ctx.operations.stable(name + ":v1"), timeout=30,
                )
            status = await ctx.services.call(
                "deployment.retirement.status", request,
                operation_id=ctx.operations.stable("status:v1"), timeout=15,
            )
        except ForestError as exc:
            if exc.code in _WAITING:
                return {"bot_id": bot_id, "state": "WAITING", "error_code": exc.code}
            if exc.code in _BLOCKED:
                return {"bot_id": bot_id, "state": "BLOCKED", "error_code": exc.code}
            raise
        if status["state"] == "FAILED":
            return {
                "bot_id": bot_id, "state": "BLOCKED",
                "error_code": status["last_error_code"] or "HOST_STORAGE_RECLAIM_FAILED",
            }
        if status["state"] != "SUCCEEDED":
            return {"bot_id": bot_id, "state": "WAITING"}
        return {
            "bot_id": bot_id, "state": "READY_TO_PUBLISH",
            "request_id": status["request_id"],
            "resource_id": status["resource_id"],
        }
