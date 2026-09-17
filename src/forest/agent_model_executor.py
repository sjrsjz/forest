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
    ) -> AgentModelStepResult:
        if type(step) is not AgentModelStep:
            raise ForestError("AGENT_MODEL_STEP_INVALID", "Model step identity is invalid", 422)
        run = await self.agent.load(owner.run_id)
        expected = {
            "bot_id": self.agent.repository.store.bot_id,
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
                request_kind="PRIMARY",
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
            effect_observation_ref=f"provider:{step.attempt_id}:{observed_state}",
            result=result_ref,
            usage=usage_ref,
        )
        if result.status != "RESPONDED":
            return AgentModelStepResult(
                observed_state,
                error_code=result.error_code,
            )

        turn = parse_model_turn(
            result.text,
            allowed_tools=tuple(item["name"] for item in tools),
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
        )
        if proposal is not None:
            return AgentModelStepResult(
                "TOOL_PROPOSED",
                turn=turn,
                tool_call_id=proposal.tool_call_id,
            )
        if not turn.terminal:
            return AgentModelStepResult("CONTINUE", turn=turn)

        final_candidate = await self._payload(
            "agent.runtime",
            "final.reply",
            step.final_reply_ref,
            _canonical({"text": turn.assistant_text}),
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
            text=turn.assistant_text,
        )
