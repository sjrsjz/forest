"""Provider-side adapter for the existing frozen Agent tool gateway."""

import hashlib
import json

from forest.errors import ForestError


def plain(value):
    if hasattr(value, "items"):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value


class ToolOutcome:
    def __init__(self, status, *, value=None, error_code=None):
        self.status, self.value, self.error_code = status, value, error_code


class ToolRejected(ForestError):
    """A provider-proven terminal failure, including pre-dispatch rejection."""


class AgentToolExecutor:
    """Own a tool's local invocation journal, never select or authorize routes."""

    def __init__(self, component_id):
        self.component_id = component_id

    async def execute(self, command, ctx, handler):
        command = plain(command)
        if (command["component_id"] != self.component_id
                or command["generation_id"] != ctx.identity["generation_id"]):
            raise ForestError("AGENT_TOOL_FENCE_MISMATCH", "Tool command targets another Generation", 409)
        key = "tool:" + hashlib.sha256(command["effect_operation_id"].encode()).hexdigest()
        canonical = {k: v for k, v in command.items() if k != "protocol"}
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"),
                                           ensure_ascii=False).encode()).hexdigest()
        state = await ctx.state.get(key)
        prior = plain(state["value"])
        if prior is not None:
            if prior["command_digest"] != digest:
                raise ForestError("AGENT_TOOL_OPERATION_CONFLICT", "Tool operation input changed", 409)
            return prior["receipt"]
        try:
            value = await handler(command, ctx)
            outcome = value if isinstance(value, ToolOutcome) else ToolOutcome("SUCCEEDED", value=value)
        except ToolRejected as error:
            outcome = ToolOutcome("FAILED", error_code=error.code)
        reference = None
        if outcome.status == "SUCCEEDED":
            reference = await ctx.payloads.put_json(
                plain(outcome.value), owner_kind="tool.result", owner_id=command["tool_call_id"],
            )
        receipt = self._receipt(command, outcome.status, plain(reference), outcome.error_code)
        await ctx.state.commit(
            key, {"command_digest": digest, "receipt": receipt}, expected_revision=state["revision"],
            operation_id=ctx.operations.stable("tool-result"),
        )
        return receipt

    async def query(self, command, ctx):
        command = plain(command)
        if (command["component_id"] != self.component_id
                or command["generation_id"] != ctx.identity["generation_id"]):
            raise ForestError("AGENT_TOOL_FENCE_MISMATCH", "Query targets another Generation", 409)
        key = "tool:" + hashlib.sha256(command["effect_operation_id"].encode()).hexdigest()
        state = await ctx.state.get(key)
        prior = plain(state["value"])
        if prior is None:
            return self._receipt(command, "UNKNOWN", None, "TOOL_RESULT_UNAVAILABLE")
        canonical = {k: v for k, v in command.items() if k != "protocol"}
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"),
                                           ensure_ascii=False).encode()).hexdigest()
        if prior["command_digest"] != digest:
            raise ForestError("AGENT_TOOL_OPERATION_CONFLICT", "Query input differs from original operation", 409)
        return prior["receipt"]

    @staticmethod
    def _receipt(command, status, result, error_code):
        return {"protocol": "forest.agent-tool-receipt/1", "status": status,
                "tool_call_id": command["tool_call_id"], "effect_operation_id": command["effect_operation_id"],
                "effect_attempt_id": command["effect_attempt_id"], "result": result,
                "error_code": error_code, "observation_ref": "tool-observation:" + command["effect_operation_id"]}
