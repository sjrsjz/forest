"""Typed Agent tool coordination through authorization and Effect Gateway ports."""

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from typing import Protocol

from forest.agent_model_executor import AgentToolBinding
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_runtime_authority import AgentPayloadRef


@dataclass(frozen=True)
class ToolAuthorizationRequest:
    tool_call_id: str
    effect_operation_ref: str
    producer_component_id: str
    producer_generation_id: str
    executor_component_id: str
    executor_generation_id: str
    service_id: str
    binding_digest: str
    authority_trigger_ref: str
    authority_principal_ref: str
    arguments: dict
    input_digest: str


@dataclass(frozen=True)
class ToolAuthorizationResult:
    status: str
    authorization_ticket_id: str | None = None
    approval_ref: str | None = None
    error_code: str | None = None
    input_digest: str | None = None
    audience_component_id: str | None = None
    audience_generation_id: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"WAITING", "AUTHORIZED", "DENIED"}:
            raise ValueError("Invalid tool authorization status")
        if self.status == "WAITING" and (
            self.approval_ref is None
            or self.authorization_ticket_id is not None
            or self.error_code is not None
            or self.input_digest is not None
            or self.audience_component_id is not None
            or self.audience_generation_id is not None
        ):
            raise ValueError("Waiting authorization requires only an approval reference")
        if self.status == "AUTHORIZED" and (
            self.authorization_ticket_id is None
            or self.error_code is not None
            or self.input_digest is None
            or self.audience_component_id is None
            or self.audience_generation_id is None
        ):
            raise ValueError("Authorized tool needs an audience-bound ticket receipt")
        if self.status == "DENIED" and (
            self.error_code is None or self.authorization_ticket_id is not None
            or self.input_digest is not None
            or self.audience_component_id is not None
            or self.audience_generation_id is not None
        ):
            raise ValueError("Denied tool authorization needs an error")


class ToolAuthorizationPort(Protocol):
    async def authorize(
        self,
        request: ToolAuthorizationRequest,
        *,
        approval_required: bool,
    ) -> ToolAuthorizationResult: ...


@dataclass(frozen=True)
class ToolDispatchRequest:
    authorization: ToolAuthorizationRequest
    authorization_ticket_id: str
    approval_ref: str | None


@dataclass(frozen=True)
class ToolDispatchResult:
    status: str
    result: AgentPayloadRef | None = None
    error_code: str | None = None
    observation_ref: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
            raise ValueError("Invalid tool dispatch status")
        if self.status == "SUCCEEDED" and (
            type(self.result) is not AgentPayloadRef or self.error_code is not None
        ):
            raise ValueError("Tool success requires only a result")
        if self.status == "FAILED" and (self.error_code is None or self.result is not None):
            raise ValueError("Tool failure requires only an error code")
        if self.status == "UNKNOWN" and self.result is not None:
            raise ValueError("Unknown tool result cannot carry a result")


class ToolExecutionPort(Protocol):
    async def dispatch(self, request: ToolDispatchRequest, *, permit_in) -> ToolDispatchResult: ...

    async def query(self, request: ToolAuthorizationRequest) -> ToolDispatchResult: ...


@dataclass(frozen=True)
class AgentToolStepResult:
    status: str
    tool_call_id: str
    approval_ref: str | None = None
    error_code: str | None = None


async def _call(port, *args, **kwargs):
    value = port(*args, **kwargs)
    return await value if inspect.isawaitable(value) else value


class DurableAgentToolExecutor:
    """Coordinate one persisted tool call without owning a tool or child Agent."""

    def __init__(self, *, agent, payloads) -> None:
        self.agent = agent
        self.payloads = payloads

    async def _request(
        self, owner, binding: AgentToolBinding
    ) -> tuple[dict, dict, ToolAuthorizationRequest]:
        recovery = await self.agent.load_recovery(owner.run_id)
        tool = recovery["current_tool"]
        if tool is None or recovery["state"] not in {
            "WAIT_TOOL",
            "WAIT_APPROVAL",
            "BLOCKED_UNKNOWN",
        }:
            raise ForestError(
                "AGENT_TOOL_RECOVERY_BOUNDARY_INVALID",
                "Run has no current tool proposal",
                409,
            )
        if (
            tool["tool_name"] != binding.name
            or tool["tool_component_id"] != binding.component_id
            or tool["tool_generation_id"] != binding.generation_id
            or tool["tool_service_id"] != binding.service_id
            or tool["tool_contract_digest"] != binding.contract_digest
            or tool["tool_binding_digest"] != binding.binding_digest
            or tool["approval_required"] is not binding.approval_required
            or tool["supports_query"] is not binding.supports_query
        ):
            raise ForestError(
                "AGENT_TOOL_BINDING_CONFLICT",
                "Current tool proposal belongs to another Generation binding",
                409,
            )
        generation = self.agent.generation_identity
        if generation is None or generation.component_id != "agent.runtime":
            raise ForestError(
                "AGENT_TOOL_GENERATION_UNAVAILABLE",
                "Agent Generation identity is unavailable",
                503,
            )
        try:
            wrapper = json.loads(await self.payloads.read_bytes(tool["arguments_payload_id"]))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "AGENT_TOOL_ARGUMENTS_CORRUPT", "Tool arguments are unavailable", 500
            ) from exc
        if (
            not isinstance(wrapper, dict)
            or set(wrapper) != {"authority_trigger_id", "arguments"}
            or wrapper["authority_trigger_id"] != tool["authority_trigger_ref"]
            or not isinstance(wrapper["arguments"], dict)
        ):
            raise ForestError(
                "AGENT_TOOL_ARGUMENTS_CORRUPT", "Tool authority wrapper is invalid", 500
            )
        identity = {
            "tool_call_id": tool["tool_call_id"],
            "effect_operation_ref": tool["effect_operation_ref"],
            "producer_component_id": "agent.runtime",
            "producer_generation_id": generation.generation_id,
            "executor_component_id": binding.component_id,
            "executor_generation_id": binding.generation_id,
            "service_id": binding.service_id,
            "binding_digest": binding.binding_digest,
            "authority_trigger_ref": tool["authority_trigger_ref"],
            "authority_principal_ref": tool["authority_principal_ref"],
            "arguments": wrapper["arguments"],
        }
        return recovery, tool, ToolAuthorizationRequest(
            **identity,
            input_digest=digest_json(identity),
        )

    async def execute(
        self,
        owner,
        binding: AgentToolBinding,
        authorization: ToolAuthorizationPort,
        port: ToolExecutionPort,
    ) -> AgentToolStepResult:
        _recovery, tool, request = await self._request(owner, binding)
        if tool["state"] not in {"PROPOSED", "WAIT_APPROVAL"}:
            raise ForestError(
                "AGENT_TOOL_RECOVERY_REQUIRED",
                "Dispatched tool calls must use reconciliation",
                409,
            )
        decision = await _call(
            authorization.authorize,
            request,
            approval_required=binding.approval_required,
        )
        if type(decision) is not ToolAuthorizationResult:
            raise ForestError(
                "AGENT_TOOL_AUTHORIZATION_INVALID",
                "Authorization port returned an invalid result",
                502,
            )
        previous_approval = tool["approval_ref"]
        if previous_approval is not None and decision.approval_ref != previous_approval:
            raise ForestError(
                "AGENT_TOOL_APPROVAL_CONFLICT",
                "Authorization result refers to another approval",
                409,
            )
        if (
            binding.approval_required
            and decision.status == "AUTHORIZED"
            and decision.approval_ref is None
        ):
            raise ForestError(
                "AGENT_TOOL_APPROVAL_REQUIRED",
                "Tool authorization omitted the required approval",
                403,
            )
        if decision.status == "WAITING":
            if tool["state"] == "PROPOSED":
                await self.agent.observe_tool(
                    owner,
                    observation_operation_id=f"tool-approval-wait:{tool['tool_call_id']}",
                    tool_call_id=tool["tool_call_id"],
                    observed_state="WAIT_APPROVAL",
                    evidence_kind="AUTHORIZATION",
                    approval_ref=decision.approval_ref,
                )
            return AgentToolStepResult(
                "WAIT_APPROVAL", tool["tool_call_id"], decision.approval_ref
            )
        if decision.status == "DENIED":
            evidence = f"approval-denied:{decision.approval_ref or request.input_digest}"
            await self.agent.observe_tool(
                owner,
                observation_operation_id=f"tool-approval-denied:{tool['tool_call_id']}",
                tool_call_id=tool["tool_call_id"],
                observed_state="CANCELLED",
                evidence_kind="AUTHORIZATION",
                effect_observation_ref=evidence,
                approval_ref=decision.approval_ref,
                error_code=decision.error_code,
            )
            return AgentToolStepResult(
                "CANCELLED",
                tool["tool_call_id"],
                decision.approval_ref,
                decision.error_code,
            )

        if (
            decision.input_digest != request.input_digest
            or decision.audience_component_id != request.executor_component_id
            or decision.audience_generation_id != request.executor_generation_id
        ):
            raise ForestError(
                "AGENT_TOOL_AUTHORIZATION_CONFLICT",
                "Authorization ticket receipt does not match the tool input and Generation",
                403,
            )

        dispatch = ToolDispatchRequest(
            request,
            decision.authorization_ticket_id,
            decision.approval_ref,
        )
        handed_off = False

        async def permit_in(conn, *, effect_observation_ref: str):
            nonlocal handed_off
            if handed_off:
                raise ForestError(
                    "AGENT_TOOL_HANDOFF_REPEATED", "Tool handoff was repeated", 409
                )
            await self.agent.observe_tool_in(
                conn,
                owner,
                observation_operation_id=f"tool-dispatch:{tool['tool_call_id']}",
                tool_call_id=tool["tool_call_id"],
                observed_state="DISPATCHING",
                evidence_kind="AUTHORIZATION",
                effect_observation_ref=effect_observation_ref,
                approval_ref=decision.approval_ref,
            )
            handed_off = True

        result = await _call(port.dispatch, dispatch, permit_in=permit_in)
        if not handed_off:
            raise ForestError(
                "AGENT_TOOL_DISPATCH_NOT_PERMITTED",
                "Tool executor returned before the Effect handoff",
                502,
            )
        current = await self.agent.load_recovery(owner.run_id)
        current_tool = current["current_tool"]
        if current_tool is None or current_tool["state"] != "DISPATCHING":
            raise ForestError(
                "AGENT_TOOL_HANDOFF_NOT_COMMITTED",
                "Effect handoff did not commit the Agent transition",
                502,
            )
        return await self._observe(owner, request, result, evidence_kind="EXECUTOR")

    async def reconcile(
        self,
        owner,
        binding: AgentToolBinding,
        port: ToolExecutionPort,
    ) -> AgentToolStepResult:
        _recovery, tool, request = await self._request(owner, binding)
        if tool["state"] == "UNKNOWN":
            return AgentToolStepResult(
                "UNKNOWN", tool["tool_call_id"], tool["approval_ref"], "TOOL_RESULT_UNKNOWN"
            )
        if tool["state"] != "DISPATCHING":
            raise ForestError(
                "AGENT_TOOL_RECOVERY_BOUNDARY_INVALID",
                "Tool call is not awaiting reconciliation",
                409,
            )
        if not binding.supports_query:
            result = ToolDispatchResult(
                "UNKNOWN",
                error_code="TOOL_RESULT_UNQUERYABLE_AFTER_RESTART",
                observation_ref=f"tool-unqueryable:{request.effect_operation_ref}",
            )
        else:
            result = await _call(port.query, request)
        return await self._observe(owner, request, result, evidence_kind="RECONCILIATION")

    async def _observe(self, owner, request, result, *, evidence_kind):
        if type(result) is not ToolDispatchResult:
            raise ForestError(
                "AGENT_TOOL_RESULT_INVALID", "Tool port returned an invalid result", 502
            )
        payload = result.result
        if payload is not None and (
            payload.owner_component_id != request.executor_component_id
            or payload.owner_kind != "tool.result"
            or payload.owner_id != request.tool_call_id
        ):
            raise ForestError(
                "AGENT_TOOL_RESULT_INVALID",
                "Tool result payload belongs to another owner",
                502,
            )
        evidence = result.observation_ref or (
            f"tool:{request.effect_operation_ref}:{result.status.lower()}"
        )
        await self.agent.observe_tool(
            owner,
            observation_operation_id=f"tool-observe:{request.tool_call_id}:{result.status}",
            tool_call_id=request.tool_call_id,
            observed_state=result.status,
            evidence_kind=evidence_kind,
            effect_observation_ref=evidence,
            result=payload,
            error_code=result.error_code if result.status == "FAILED" else None,
        )
        return AgentToolStepResult(
            result.status,
            request.tool_call_id,
            error_code=result.error_code,
        )
