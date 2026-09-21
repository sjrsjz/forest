"""Durable single-Agent model step using the active Provider Generation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from forest.agent_protocol import ModelTurn, parse_model_turn
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_runtime_authority import (
    AgentPayloadRef,
    ModelRequestPlan,
    ToolProposal,
)


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _stable(kind: str, *parts: object) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{kind}:{hashlib.sha256(encoded).hexdigest()}"


def _bounded_excerpt(value: str, limit: int) -> str:
    raw = value.encode("utf-8", errors="replace")
    if len(raw) <= limit:
        return value
    marker = b"\n...[candidate truncated by host]...\n"
    remaining = max(0, limit - len(marker))
    prefix_size = remaining // 2
    suffix_size = remaining - prefix_size
    prefix = raw[:prefix_size].decode("utf-8", errors="ignore").encode("utf-8")
    suffix = raw[-suffix_size:].decode("utf-8", errors="ignore").encode("utf-8")
    return (prefix + marker + suffix).decode("utf-8")


@dataclass(frozen=True)
class AgentModelStep:
    request_id: str
    attempt_id: str
    effect_operation_ref: str
    turn_id: str
    final_reply_ref: str


@dataclass(frozen=True)
class AgentModelStepResult:
    status: str
    turn: ModelTurn | None = None
    final_candidate: AgentPayloadRef | None = None
    text: str | None = None
    error_code: str | None = None
    tool_call_id: str | None = None


@dataclass(frozen=True)
class AgentToolBinding:
    name: str
    component_id: str
    generation_id: str
    service_id: str
    contract_digest: str
    approval_required: bool = True
    supports_query: bool = False

    def __post_init__(self) -> None:
        if (
            not self.name
            or not self.component_id
            or not self.generation_id
            or not self.service_id
            or not self.contract_digest.startswith("sha256:")
            or len(self.contract_digest) != 71
            or any(char not in "0123456789abcdef" for char in self.contract_digest[7:])
            or type(self.approval_required) is not bool
            or type(self.supports_query) is not bool
        ):
            raise ValueError("Tool binding identity is invalid")

    @property
    def binding_digest(self) -> str:
        return digest_json(
            {
                "name": self.name,
                "component_id": self.component_id,
                "generation_id": self.generation_id,
                "service_id": self.service_id,
                "contract_digest": self.contract_digest,
                "approval_required": self.approval_required,
                "supports_query": self.supports_query,
            }
        )


class DurableAgentModelExecutor:
    """Advance exactly one model call in the QiYu-style Agent state machine.

    This class does not schedule another Agent and does not own group queues.
    Conversation invokes it only after fixing the input cut for the next model
    call. Tool execution is deliberately a separate participant; a FunctionCall
    is rejected until that port is supplied rather than executed with ambient
    process authority.
    """

    def __init__(self, *, agent, provider, payloads) -> None:
        self.agent = agent
        self.provider = provider
        self.payloads = payloads

    async def reconcile_after_restart(self, owner) -> AgentModelStepResult:
        """Fence an unqueryable in-flight Chat Completions attempt as UNKNOWN."""

        recovery = await self.agent.load_recovery(owner.run_id)
        request = recovery["current_request"]
        attempt = recovery["current_attempt"]
        if (
            recovery["state"] == "BLOCKED_UNKNOWN"
            and request is not None
            and attempt is not None
            and request["state"] == "UNKNOWN"
            and attempt["state"] == "UNKNOWN"
        ):
            return AgentModelStepResult(
                "UNKNOWN",
                error_code="MODEL_RESULT_UNQUERYABLE_AFTER_RESTART",
            )
        if (
            recovery["state"] != "MODEL_PENDING"
            or request is None
            or attempt is None
            or request["state"] != "DISPATCHING"
            or attempt["state"] != "DISPATCHING"
        ):
            raise ForestError(
                "AGENT_RECOVERY_BOUNDARY_INVALID",
                "Run has no unqueryable in-flight model attempt",
                409,
            )
        await self.agent.observe_model(
            owner,
            observation_operation_id=f"recover-unknown:{attempt['attempt_id']}",
            request_id=request["request_id"],
            attempt_id=attempt["attempt_id"],
            observed_state="UNKNOWN",
            evidence_kind="EXECUTOR",
            effect_observation_ref=f"provider-restart:{attempt['attempt_id']}",
        )
        return AgentModelStepResult(
            "UNKNOWN",
            error_code="MODEL_RESULT_UNQUERYABLE_AFTER_RESTART",
        )

    async def _payload(self, component: str, kind: str, owner_id: str, content: bytes):
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id=component,
            owner_kind=kind,
            owner_id=owner_id,
            media_type="application/json",
            content=content,
            expected_deletion_epoch=epoch,
        )
        return AgentPayloadRef(
            saved.payload_id,
            saved.content_digest,
            component,
            kind,
            owner_id,
        )

    async def _record_protocol_failure(
        self,
        owner,
        step: AgentModelStep,
        candidate: str,
        error: ForestError,
        *,
        allow_repair: bool,
        feedback_byte_limit: int,
    ) -> AgentModelStepResult:
        if (
            type(allow_repair) is not bool
            or type(feedback_byte_limit) is not int
            or not 1024 <= feedback_byte_limit <= 32768
        ):
            raise ForestError(
                "AGENT_PROTOCOL_REPAIR_CONFIGURATION_INVALID",
                "Protocol repair limits are invalid",
                500,
            )
        candidate_digest = "sha256:" + hashlib.sha256(
            candidate.encode("utf-8", errors="surrogatepass")
        ).hexdigest()
        failure_id = _stable("protocol-failure", owner.run_id, step.request_id)
        feedback_body = {
            "repair_count": 1,
            "error_code": error.code,
            "validation_reason": error.message,
            "candidate_digest": candidate_digest,
            "feedback": _bounded_excerpt(candidate, feedback_byte_limit),
        }
        feedback = await self._payload(
            "agent.runtime",
            "protocol.repair.feedback",
            failure_id,
            _canonical(feedback_body),
        )
        target = "REPAIR_READY" if allow_repair else "FAILED"
        checkpoint_id = _stable("checkpoint-protocol-failure", failure_id, target)
        checkpoint = await self._payload(
            "agent.runtime",
            "checkpoint.state",
            checkpoint_id,
            _canonical(
                {
                    "phase": target,
                    "failure_id": failure_id,
                    "failed_request_id": step.request_id,
                    "error_code": error.code,
                    "candidate_digest": candidate_digest,
                }
            ),
        )
        failure = await self.agent.record_protocol_failure(
            owner,
            failure_id=failure_id,
            request_id=step.request_id,
            attempt_id=step.attempt_id,
            error_code=error.code,
            candidate_digest=candidate_digest,
            feedback=feedback,
            checkpoint_id=checkpoint_id,
            checkpoint_state=checkpoint,
            allow_repair=allow_repair,
        )
        return AgentModelStepResult(
            failure["resulting_state"],
            error_code=(
                "AGENT_PROTOCOL_FAILED"
                if failure["resulting_state"] == "FAILED"
                else error.code
            ),
        )

    async def _record_response(
        self,
        owner,
        step: AgentModelStep,
        candidate: str,
        *,
        tools=(),
        tool_bindings: tuple[AgentToolBinding, ...] = (),
        repair_of_request_id: str | None = None,
        allow_protocol_repair: bool = True,
        repair_feedback_byte_limit: int = 8192,
    ) -> AgentModelStepResult:
        """Parse and commit one already durable provider response idempotently."""

        try:
            turn = parse_model_turn(
                candidate,
                allowed_tools=tuple(item["name"] for item in tools),
            )
        except ForestError as exc:
            if not exc.code.startswith("AGENT_PROTOCOL_"):
                raise
            return await self._record_protocol_failure(
                owner,
                step,
                candidate,
                exc,
                allow_repair=allow_protocol_repair,
                feedback_byte_limit=repair_feedback_byte_limit,
            )
        if any(type(item) is not AgentToolBinding for item in tool_bindings):
            raise ForestError(
                "AGENT_TOOL_BINDING_INVALID", "Tool binding set is invalid", 422
            )
        bindings = {item.name: item for item in tool_bindings}
        declared_names = {item.get("name") for item in tools if isinstance(item, dict)}
        if len(bindings) != len(tool_bindings) or set(bindings) != declared_names:
            raise ForestError(
                "AGENT_TOOL_BINDING_INVALID",
                "Tool declarations and runtime bindings differ",
                422,
            )
        turn_payload = await self._payload(
            "agent.runtime",
            "model.turn",
            step.turn_id,
            _canonical(turn.as_dict()),
        )
        assistant_payload = None
        if turn.assistant_text:
            assistant_payload = await self._payload(
                "agent.runtime",
                "assistant.candidate",
                step.turn_id,
                _canonical({"text": turn.assistant_text}),
            )
        proposal = None
        if turn.function_call is not None:
            binding = bindings.get(turn.function_call.name)
            if binding is None:
                raise ForestError(
                    "AGENT_TOOL_PORT_UNAVAILABLE",
                    "Model proposed a tool without an exact typed binding",
                    503,
                )
            wrapper = turn.function_call.arguments
            authority_trigger = wrapper.get("authority_trigger_id")
            arguments = wrapper.get("arguments")
            if set(wrapper) != {"authority_trigger_id", "arguments"} or not isinstance(
                arguments, dict
            ):
                raise ForestError(
                    "AGENT_TOOL_AUTHORITY_INVALID",
                    "Tool call does not bind arguments to one trigger",
                    409,
                )
            call_digest = hashlib.sha256(
                _canonical(
                    {
                        "turn_id": step.turn_id,
                        "binding": {
                            "name": binding.name,
                            "component_id": binding.component_id,
                            "generation_id": binding.generation_id,
                            "service_id": binding.service_id,
                            "contract_digest": binding.contract_digest,
                            "approval_required": binding.approval_required,
                            "supports_query": binding.supports_query,
                        },
                        "arguments": wrapper,
                    }
                )
            ).hexdigest()
            tool_call_id = "tool-call:" + call_digest
            argument_payload = await self._payload(
                "agent.runtime",
                "tool.arguments",
                tool_call_id,
                _canonical(wrapper),
            )
            proposal = ToolProposal(
                tool_call_id=tool_call_id,
                arguments=argument_payload,
                authority_trigger_ref=authority_trigger,
                tool_contract_digest=binding.binding_digest,
                effect_operation_ref="tool-effect:" + call_digest,
            )
        await self.agent.record_model_turn(
            owner,
            turn_id=step.turn_id,
            request_id=step.request_id,
            turn=turn,
            turn_payload=turn_payload,
            assistant_payload=assistant_payload,
            tool=proposal,
            repair_of_request_id=repair_of_request_id,
        )
        if proposal is not None:
            return AgentModelStepResult(
                "TOOL_PROPOSED",
                turn=turn,
                tool_call_id=proposal.tool_call_id,
            )
        if not turn.terminal:
            return AgentModelStepResult("CONTINUE", turn=turn)
        return await self.resume_safe_boundary(owner, step, turn=turn)

    async def resume_parsing(
        self,
        owner,
        step: AgentModelStep,
        *,
        tools=(),
        tool_bindings: tuple[AgentToolBinding, ...] = (),
        repair_feedback_byte_limit: int = 8192,
    ) -> AgentModelStepResult:
        """Replay parsing from the immutable successful transport payload."""

        recovery = await self.agent.load_recovery(owner.run_id)
        request = recovery["current_request"]
        attempt = recovery["current_attempt"]
        if (
            recovery["state"] != "PARSING"
            or request is None
            or attempt is None
            or request["request_id"] != step.request_id
            or attempt["attempt_id"] != step.attempt_id
            or request["state"] != "SUCCEEDED"
            or attempt["state"] != "SUCCEEDED"
            or attempt["result_payload_id"] is None
        ):
            raise ForestError(
                "AGENT_RECOVERY_BOUNDARY_INVALID",
                "Run has no durable model response to parse",
                409,
            )
        try:
            result = json.loads(await self.payloads.read_bytes(attempt["result_payload_id"]))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "MODEL_RESULT_PAYLOAD_CORRUPT",
                "Durable model result cannot be parsed",
                500,
            ) from exc
        if (
            not isinstance(result, dict)
            or result.get("status") != "RESPONDED"
            or result.get("request_id") != step.request_id
            or result.get("transport_attempt_id") != step.attempt_id
            or not isinstance(result.get("text"), str)
        ):
            raise ForestError(
                "MODEL_RESULT_PAYLOAD_CONFLICT",
                "Durable model result differs from the execution head",
                409,
            )
        repair_of_request_id = None
        if request["request_kind"] == "REPAIR":
            failure = recovery["current_protocol_failure"]
            if failure is None:
                raise ForestError(
                    "AGENT_HISTORY_INCOMPLETE",
                    "Repair request has no durable protocol failure",
                    500,
                )
            repair_of_request_id = failure["failed_request_id"]
        return await self._record_response(
            owner,
            step,
            result["text"],
            tools=tools,
            tool_bindings=tool_bindings,
            repair_of_request_id=repair_of_request_id,
            allow_protocol_repair=(
                request["request_kind"] != "REPAIR" and recovery["repair_count"] < 1
            ),
            repair_feedback_byte_limit=repair_feedback_byte_limit,
        )

    async def resume_safe_boundary(
        self,
        owner,
        step: AgentModelStep,
        *,
        turn: ModelTurn | None = None,
    ) -> AgentModelStepResult:
        """Create the final candidate from a committed terminal ModelTurn."""

        recovery = await self.agent.load_recovery(owner.run_id)
        latest = recovery["latest_turn"]
        if (
            recovery["state"] != "SAFE_BOUNDARY"
            or latest is None
            or latest["turn_id"] != step.turn_id
            or latest["request_id"] != step.request_id
            or not latest["terminal"]
            or latest["assistant_payload_id"] is None
        ):
            raise ForestError(
                "AGENT_RECOVERY_BOUNDARY_INVALID",
                "Run has no terminal ModelTurn at the safe boundary",
                409,
            )
        try:
            assistant = json.loads(
                await self.payloads.read_bytes(latest["assistant_payload_id"])
            )
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "MODEL_TURN_PAYLOAD_CORRUPT",
                "Terminal assistant candidate cannot be parsed",
                500,
            ) from exc
        if (
            not isinstance(assistant, dict)
            or set(assistant) != {"text"}
            or not isinstance(assistant["text"], str)
        ):
            raise ForestError(
                "MODEL_TURN_PAYLOAD_CONFLICT",
                "Terminal assistant candidate is invalid",
                409,
            )
        final_candidate = await self._payload(
            "agent.runtime",
            "final.reply",
            step.final_reply_ref,
            _canonical(assistant),
        )
        state = await self._payload(
            "agent.runtime",
            "checkpoint.state",
            f"final-ready:{step.turn_id}",
            _canonical(
                {
                    "phase": "FINAL_READY",
                    "turn_id": step.turn_id,
                    "reply_ref": step.final_reply_ref,
                }
            ),
        )
        await self.agent.transition(
            owner,
            checkpoint_id=f"checkpoint:final-ready:{step.turn_id}",
            expected_phase="SAFE_BOUNDARY",
            target_phase="FINAL_READY",
            state=state,
            final_candidate=final_candidate,
        )
        return AgentModelStepResult(
            "FINAL_READY",
            turn=turn,
            final_candidate=final_candidate,
            text=assistant["text"],
        )

    async def execute(
        self,
        owner,
        step: AgentModelStep,
        snapshot: dict,
        *,
        preset_context=(),
        tools=(),
        media=None,
        evidence=None,
        tool_bindings: tuple[AgentToolBinding, ...] = (),
        output_tokens: int = 1024,
        stream: bool = True,
        request_kind: str = "PRIMARY",
        repair_of_request_id: str | None = None,
        allow_protocol_repair: bool = True,
        repair_feedback_byte_limit: int = 8192,
    ) -> AgentModelStepResult:
        if type(step) is not AgentModelStep:
            raise ForestError("AGENT_MODEL_STEP_INVALID", "Model step identity is invalid", 422)
        run = await self.agent.load(owner.run_id)
        expected = {
            "bot_id": self.agent.bot_id,
            "run_id": owner.run_id,
            "input_revision": run["input_revision"],
            "context_revision": run["context_revision"],
            "context_view_revision": run["context_view_revision"],
            "selected_input_end_seq": run["selected_input_end_seq"],
            "trigger_ids": [item["trigger_ref"] for item in run["triggers"]],
            "input_batch_ids": [item["input_batch_ref"] for item in run["inputs"]],
        }
        if any(snapshot.get(key) != value for key, value in expected.items()):
            raise ForestError(
                "AGENT_MODEL_INPUT_FENCE_MISMATCH",
                "Model snapshot differs from the durable Agent input fence",
                409,
            )
        bound = await self.provider.prepare_request(
            snapshot,
            request_id=step.request_id,
            transport_attempt_id=step.attempt_id,
            preset_context=preset_context,
            tools=tools,
            media=media,
            evidence=evidence,
            output_tokens=output_tokens,
            stream=stream,
        )
        manifest = bound.prepared.manifest
        projection = await self._payload(
            bound.component_id,
            "model.projection",
            step.request_id,
            bound.prepared.manifest_json.encode("utf-8"),
        )
        await self.agent.prepare_model_request(
            owner,
            ModelRequestPlan(
                request_id=step.request_id,
                request_kind=request_kind,
                provider_component_id=bound.component_id,
                provider_generation_id=bound.generation_id,
                provider_profile_ref=bound.provider_profile_ref,
                model_ref=bound.capabilities.model,
                instruction_channel=bound.capabilities.system_role.upper(),
                capability_digest=bound.capability_digest,
                projection=projection,
                projected_content_digest=manifest["projected_content_digest"],
                source_manifest_digest=digest_json(manifest["source_manifest"]),
                output_token_budget=bound.prepared.max_output_tokens,
            ),
        )
        wire = await self._payload(
            bound.component_id,
            "model.wire",
            step.attempt_id,
            bound.prepared.body,
        )
        await self.agent.prepare_model_attempt(
            owner,
            request_id=step.request_id,
            attempt_id=step.attempt_id,
            effect_operation_ref=step.effect_operation_ref,
            wire=wire,
        )

        async def permit(conn, *, request):
            if request is not bound:
                raise ForestError(
                    "PROVIDER_REQUEST_FENCE_MISMATCH", "Provider request changed", 409
                )
            await self.agent.mark_model_handoff_in(
                conn,
                owner,
                request_id=step.request_id,
                attempt_id=step.attempt_id,
            )

        result = await self.provider.dispatch(bound, permit_in=permit)
        observed_state = {
            "RESPONDED": "SUCCEEDED",
            "NOT_SENT": "NOT_APPLIED",
            "UNKNOWN": "UNKNOWN",
        }[result.status]
        result_ref = None
        usage_ref = None
        if result.status == "RESPONDED":
            result_ref = await self._payload(
                bound.component_id,
                "model.result",
                step.attempt_id,
                _canonical(result.as_dict()),
            )
            if result.usage is not None:
                usage_ref = await self._payload(
                    bound.component_id,
                    "model.usage",
                    step.attempt_id,
                    _canonical(result.usage),
                )
        await self.agent.observe_model(
            owner,
            observation_operation_id=f"observe:{step.attempt_id}",
            request_id=step.request_id,
            attempt_id=step.attempt_id,
            observed_state=observed_state,
            evidence_kind="EXECUTOR",
            effect_observation_ref=(
                f"provider:{step.attempt_id}:{observed_state}:"
                f"{result.error_code or '-'}"
            ),
            result=result_ref,
            usage=usage_ref,
        )
        if result.status != "RESPONDED":
            return AgentModelStepResult(
                observed_state,
                error_code=result.error_code,
            )

        return await self._record_response(
            owner,
            step,
            result.text,
            tools=tools,
            tool_bindings=tool_bindings,
            repair_of_request_id=repair_of_request_id,
            allow_protocol_repair=allow_protocol_repair,
            repair_feedback_byte_limit=repair_feedback_byte_limit,
        )
