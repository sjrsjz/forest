from agent_protocol import protocol_descriptor
from execution import AgentExecution

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


class AgentRuntime(Plugin):
    """Own the immutable single-Agent protocol inside an isolated Generation."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if config["protocol"] != "forest.agent.runtime/1":
            raise ForestError(
                "AGENT_CONFIGURATION_INVALID",
                "Agent runtime protocol is unsupported",
                422,
            )
        self._config = {
            key: (
                dict(value)
                if hasattr(value, "items")
                else list(value)
                if isinstance(value, (tuple, list))
                else value
            )
            for key, value in config.items()
        }

    @service(
        "agent.execute",
        semantics="idempotent",
        contract="2.0",
        input_schema="schemas/execute-input.json",
        output_schema="schemas/execute-output.json",
    )
    async def execute(self, payload, ctx):
        return await AgentExecution(ctx, self._config).execute(payload)

    @service(
        "agent.execution.policy",
        semantics="read",
        contract="1.0",
        input_schema="schemas/execution-policy-input.json",
        output_schema="schemas/execution-policy-output.json",
    )
    async def execution_policy(self, payload, ctx):
        return {
            "protocol": "forest.agent-execution-policy/1",
            "run_timeout_seconds": self._config["run_timeout_seconds"],
        }

    @service(
        "agent.protocol.describe",
        semantics="read",
        contract="1.0",
        input_schema="schemas/protocol-describe-input.json",
        output_schema="schemas/protocol-describe-output.json",
    )
    async def describe_protocol(self, payload, ctx):
        return protocol_descriptor()

    @service(
        "agent.run.query",
        semantics="read",
        contract="1.0",
        input_schema="schemas/run-query-input.json",
        output_schema="schemas/run-snapshot-output.json",
    )
    async def query_run(self, payload, ctx):
        return await ctx.queries.call("agent.run-snapshot", payload)

    @job("agent.reload-resume")
    async def resume_reload(self, task, ctx):
        return await ctx.uow.call(
            "agent.resume-reload",
            task["payload"],
            operation_id=ctx.operations.stable("resume-reload:v1"),
        )
