from forest.errors import ForestError
from forest.sdk import Plugin, job


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


class ProcessExecutor(Plugin):
    """Own ExecutionProfile selection, argv projection and public Job semantics."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = _plain(config)
        if value.get("protocol") != "forest.process.executor/1":
            raise ForestError(
                "PROCESS_EXECUTOR_CONFIGURATION_INVALID",
                "Process executor protocol is unsupported",
                422,
            )
        self.profiles = {item["profile_id"]: item for item in value["profiles"]}

    @staticmethod
    def _argv(profile, arguments):
        result = [profile["executable"]]
        for token in profile["argv"]:
            if "literal" in token:
                result.append(token["literal"])
                continue
            value = arguments.get(token["argument"])
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, (str, int, float)):
                rendered = str(value)
            else:
                raise ForestError(
                    "PROCESS_ARGUMENTS_INVALID",
                    "An argv argument is absent or not scalar",
                    422,
                )
            if "\x00" in rendered or len(rendered.encode()) > 8192:
                raise ForestError(
                    "PROCESS_ARGUMENTS_INVALID", "An argv argument is invalid", 422
                )
            result.append(rendered)
        return result

    @job("native.execute", input_schema="schemas/execute-input.json", max_attempts=1,
         timeout_s=3660, max_concurrency=2)
    async def execute(self, task, ctx):
        request = task.get("input") if isinstance(task, dict) else None
        if not isinstance(request, dict):
            raise ForestError("JOB_EXECUTION_INPUT_INVALID", "Job input is invalid", 422)
        profile_id = request.get("profile_id")
        arguments = request.get("arguments")
        profile = self.profiles.get(profile_id)
        if profile is None:
            raise ForestError(
                "PROCESS_PROFILE_NOT_FOUND", "Execution profile is unavailable", 404
            )
        if not isinstance(arguments, dict):
            raise ForestError("JOB_EXECUTION_INPUT_INVALID", "Job input is invalid", 422)
        argv = self._argv(profile, arguments)
        effect = await ctx.services.call(
            "process.native.spawn",
            {
                "protocol": "forest.process-spawn/1",
                "profile_id": profile_id,
                "arguments": arguments,
                "executable": profile["executable"],
                "executable_sha256": profile["executable_sha256"],
                "argv": argv,
                "working_directory": profile["working_directory"],
                "environment": {
                    "PATH": "/usr/bin:/bin",
                    "LANG": "C.UTF-8",
                    **profile["environment"],
                },
                "run_as": profile["run_as"],
                "network_policy": profile["network_policy"],
                "require_posix_limits": profile["require_posix_limits"],
                "limits": profile["limits"],
            },
            operation_id=ctx.operations.stable("native-process-spawn:v1"),
            timeout=profile["limits"]["wall_seconds"]
            + profile["limits"]["terminate_grace_seconds"]
            + 10,
        )
        return {
            "protocol": "forest.native-execution-result/1",
            "profile_id": profile_id,
            "exit_code": effect["exit_code"],
            "stdout": effect["stdout"],
            "stderr": effect["stderr"],
            "stdout_total_bytes": effect["stdout_total_bytes"],
            "stderr_total_bytes": effect["stderr_total_bytes"],
            "stdout_truncated": effect["stdout_truncated"],
            "stderr_truncated": effect["stderr_truncated"],
            "network_policy": effect["network_policy"],
        }
