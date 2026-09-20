"""Read-only operational jobs contributed by ``kernel.runtime``."""

from __future__ import annotations

from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.runtime_modules import RuntimeJobTypeContribution

RUNTIME_HEALTH_SNAPSHOT_JOB = RuntimeJobTypeContribution(
    "runtime.health-snapshot",
    1,
    "job.runtime.health-snapshot",
    1,
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    max_attempts=3,
    timeout_seconds=30,
    max_concurrency=1,
)


def runtime_health_snapshot_service(context):
    runtime = context.participant("kernel.runtime").service

    async def execute(request: DurableJobExecutionRequest) -> dict:
        if type(request) is not DurableJobExecutionRequest:
            raise ForestError("JOB_EXECUTION_INPUT_INVALID", "Job request is invalid", 422)
        if await request.cancellation_requested():
            raise ForestError("JOB_CANCELLED", "Job was cancelled before execution", 409)
        status = await runtime.status()
        return {
            "protocol": "forest.runtime-health-snapshot/1",
            "bot_id": status["bot_id"],
            "runtime_status": status["status"],
            "graph_id": status["graph_id"],
            "graph_revision": status["graph_revision"],
            "graph_state": status["graph_state"],
            "data_admission": status["data_admission"],
            "control_admission": status["control_admission"],
            "members": [
                {
                    "component_id": item["component_id"],
                    "generation_id": item["generation_id"],
                    "status": item["status"],
                    "config_revision": item["config_revision"],
                }
                for item in status["members"]
            ],
        }

    return execute
