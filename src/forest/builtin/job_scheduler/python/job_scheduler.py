import json

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


class JobScheduler(Plugin):
    """Own the durable queue; executors remain generation-bound contributors."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if _plain(config) != {"protocol": "forest.job.scheduler/1"}:
            raise ForestError(
                "JOB_CONFIGURATION_INVALID",
                "Job scheduler configuration protocol is unsupported",
                422,
            )

    @service("shell.jobs.execute")
    async def shell(self, payload, ctx):
        command_id = payload["command_id"]
        arguments = payload["arguments"]
        if command_id == "jobs.list":
            page = await ctx.queries.call(
                "jobs.query",
                {
                    "limit": arguments.get("limit", 10),
                    **(
                        {"states": [arguments["state"]]}
                        if "state" in arguments
                        else {}
                    ),
                    **(
                        {"job_type": arguments["job_type"]}
                        if "job_type" in arguments
                        else {}
                    ),
                },
            )
            if not page["items"]:
                return {"text": "Forest Jobs：没有匹配记录。"}
            return {
                "text": "Forest Jobs：\n"
                + "\n".join(
                    f"{item['job_id']} {item['job_type']} {item['state']} r{item['revision']}"
                    for item in page["items"]
                )
            }
        if command_id == "jobs.inspect":
            item = await ctx.queries.call(
                "jobs.read", {"job_id": arguments["job_id"]}
            )
            suffix = (
                f" error={item['error_code']}" if item.get("error_code") else ""
            )
            return {
                "text": (
                    f"Forest Job {item['job_id']}: type={item['job_type']} "
                    f"state={item['state']} revision={item['revision']} "
                    f"attempt={item['attempt_count']}/{item['max_attempts']}{suffix}"
                )
            }
        if command_id == "jobs.run":
            try:
                input_value = json.loads(arguments.get("input_json", "{}"))
            except json.JSONDecodeError as exc:
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID", "Job input JSON is invalid", 422
                ) from exc
            if not isinstance(input_value, dict):
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID", "Job input must be an object", 422
                )
            saved = await ctx.commands.call(
                "jobs.enqueue",
                {
                    "job_type": arguments["job_type"],
                    "requested_by": payload["principal_ref"],
                    "source_conversation_ref": payload["conversation_ref"],
                    "input": input_value,
                },
                operation_id="shell-job:" + payload["request_id"],
            )
            return {
                "text": (
                    f"Forest Job {saved['state']}: id={saved['job_id']} "
                    f"type={saved['job_type']} revision={saved['revision']}"
                )
            }
        if command_id == "jobs.cancel":
            saved = await ctx.commands.call(
                "jobs.cancel",
                {
                    "job_id": arguments["job_id"],
                    "expected_revision": arguments["expected_revision"],
                    "requested_by": payload["principal_ref"],
                    "reason_code": arguments.get(
                        "reason_code", "ADMIN_REQUESTED"
                    ),
                },
                operation_id="shell-job-cancel:" + payload["request_id"],
            )
            return {
                "text": (
                    f"Forest Job cancellation accepted: id={saved['job_id']} "
                    f"state={saved['resulting_state']} "
                    f"revision={saved['resulting_revision']}"
                )
            }
        raise ForestError(
            "SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409
        )

    @job("job.scheduler.execute")
    async def execute(self, task, ctx):
        work = _plain(task["payload"])
        cancellation = await ctx.queries.call(
            "job.cancellation-requested",
            {
                "protocol": "forest.job-cancellation-query/1",
                "job_id": work["job_id"],
                "claim_token": work["claim_token"],
            },
        )
        if cancellation["requested"]:
            return {
                "disposition": "CANCELLED",
                "result": None,
                "error_code": "JOB_CANCELLED",
                "retry_delay_seconds": 0,
            }
        try:
            result = await ctx.job_types.execute(
                job_id=work["job_id"],
                job_type=work["job_type"],
                job_version=work["job_version"],
                attempt=task["attempt"],
                requested_by=work["requested_by"],
                input=work["input"],
                claim_token=work["claim_token"],
                target_component_id=work["target_component_id"],
                target_generation_id=work["target_generation_id"],
                target_service_id=work["target_service_id"],
                target_service_version=work["target_service_version"],
                operation_id=(
                    f"job.execute:{work['job_id']}:{task['attempt']}"
                ),
                timeout=work["timeout_seconds"],
            )
            return {
                "disposition": "SUCCEEDED",
                "result": result,
                "error_code": None,
                "retry_delay_seconds": 0,
            }
        except ForestError as error:
            retry = error.status >= 500 and task["attempt"] < work["max_attempts"]
            return {
                "disposition": "RETRY" if retry else "FAILED",
                "result": None,
                "error_code": error.code,
                "retry_delay_seconds": min(60, 2 ** min(task["attempt"] - 1, 5)),
            }
