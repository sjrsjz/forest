"""Production execution port for one QiYu-style Agent Session/Run chain."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from forest.agent_context_projection import ConversationContextProjector
from forest.agent_model_executor import (
    AgentModelStep,
    AgentToolBinding,
    DurableAgentModelExecutor,
)
from forest.agent_tool_executor import DurableAgentToolExecutor
from forest.builtin.agent_runtime.python.agent_protocol import ProtocolRepairView
from forest.conversation_worker import (
    AgentExecutionCompletion,
    AgentExecutionRequest,
    AgentReloadRequested,
)
from forest.errors import ForestError
from forest.session_handoff_uow import agent_input_from_selection
from forest.storage.agent_runtime_authority import AgentPayloadRef
from forest.storage.conversation_scheduler import (
    SchedulerContextFact,
    SchedulerPayloadRef,
)


def _stable(kind: str, *parts: object) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{kind}:{hashlib.sha256(encoded).hexdigest()}"


MODEL_TRANSPORT_MAX_ATTEMPTS = 3
MODEL_TRANSPORT_RETRY_BACKOFF_SECONDS = (1.0, 3.0)
_NON_RETRYABLE_MODEL_TRANSPORT_ERRORS = frozenset(
    {
        "MODEL_HTTP_REJECTED",
    }
)


@dataclass(frozen=True)
class FrozenToolRoute:
    declaration: dict
    binding: AgentToolBinding
    authorization: object
    execution: object

    def __post_init__(self) -> None:
        if (
            not isinstance(self.declaration, dict)
            or self.declaration.get("name") != self.binding.name
        ):
            raise ValueError("Tool declaration and binding differ")


class DurableSingleAgentExecution:
    """Drive one durable Run; scheduling and group ownership stay outside Agent."""

    def __init__(
        self,
        *,
        agent,
        provider,
        model_effects,
        memory,
        media,
        payloads,
        owner_id: str,
        tool_routes: tuple[FrozenToolRoute, ...] = (),
    ) -> None:
        self.agent = agent
        self.model = DurableAgentModelExecutor(
            agent=agent,
            provider=provider,
            model_effects=model_effects,
            payloads=payloads,
        )
        self.tools = DurableAgentToolExecutor(agent=agent, payloads=payloads)
        self.memory = memory
        self.media = media
        self.payloads = payloads
        self.projector = ConversationContextProjector(
            bot_id=agent.bot_id,
            payloads=payloads,
            agent=agent,
        )
        self.owner_id = owner_id
        self.routes = {item.binding.name: item for item in tool_routes}
        if len(self.routes) != len(tool_routes):
            raise ValueError("Tool route names repeat")

    @staticmethod
    def _completion(run: dict, *, already_committed: bool = False):
        reply_ref = _stable("reply", run["run_id"])
        payload_id = run.get("final_candidate_payload_id")
        payload_digest = run.get("final_candidate_payload_digest")
        if payload_id is None or payload_digest is None:
            raise ForestError(
                "AGENT_FINAL_CANDIDATE_MISSING",
                "Completed Agent Run has no final candidate",
                500,
            )
        triggers = tuple(dict.fromkeys(item["trigger_ref"] for item in run["triggers"]))
        return AgentExecutionCompletion(
            facts=(
                SchedulerContextFact(
                    unit_id=_stable("context-reply", run["run_id"]),
                    kind="REPLY",
                    source_component_id="agent.runtime",
                    source_kind="final.reply",
                    source_id=reply_ref,
                    payload=SchedulerPayloadRef(
                        payload_id,
                        payload_digest,
                        "agent.runtime",
                        "final.reply",
                        reply_ref,
                    ),
                ),
            ),
            reply_ref=reply_ref,
            answered_trigger_ids=triggers,
            already_committed=already_committed,
        )

    async def _load_or_start(self, request: AgentExecutionRequest) -> dict:
        try:
            run = await self.agent.load(request.run_ref)
        except ForestError as exc:
            if exc.code != "AGENT_RUN_NOT_FOUND" or request.initial_batch is None:
                raise
            profile = self.agent.profile()
            run = await self.agent.start_run(
                operation_id=_stable("agent-run-start", request.run_ref),
                conversation_ref=request.conversation_id,
                session_id=request.session_ref,
                run_id=request.run_ref,
                input_batch=agent_input_from_selection(request.initial_batch),
                agent_spec_revision=profile.agent_spec_revision,
                config=profile.config,
                preset_revision=profile.preset_revision,
                preset=profile.preset,
                absolute_deadline=datetime.now(timezone.utc)
                + timedelta(seconds=profile.run_timeout_seconds),
            )
        if (
            run["session_id"] != request.session_ref
            or run["session"]["conversation_ref"] != request.conversation_id
        ):
            raise ForestError(
                "AGENT_EXECUTION_FENCE_MISMATCH",
                "Agent Run belongs to another Conversation Session",
                409,
            )
        return run

    def _route(self, name: str) -> FrozenToolRoute:
        route = self.routes.get(name)
        if route is None:
            raise ForestError(
                "AGENT_TOOL_PORT_UNAVAILABLE",
                "Current tool has no frozen typed route",
                503,
            )
        return route

    async def _park_for_reload(self, owner, operation_id: str) -> None:
        recovery = await self.agent.load_recovery(owner.run_id)
        resume_phase = recovery["state"]
        if resume_phase not in {"PREPARING_MODEL", "WAIT_TOOL", "WAIT_APPROVAL"}:
            raise ForestError(
                "AGENT_RELOAD_BOUNDARY_INVALID",
                f"Agent Run cannot enter RELOAD_WAIT from {resume_phase}",
                409,
            )
        checkpoint_id = _stable("checkpoint-reload", owner.run_id, operation_id)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            media_type="application/json",
            content=json.dumps(
                {
                    "phase": "RELOAD_WAIT",
                    "operation_id": operation_id,
                    "resume_phase": resume_phase,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            expected_deletion_epoch=epoch,
        )
        await self.agent.transition(
            owner,
            checkpoint_id=checkpoint_id,
            expected_phase=resume_phase,
            target_phase="RELOAD_WAIT",
            state=AgentPayloadRef(
                saved.payload_id,
                saved.content_digest,
                "agent.runtime",
                "checkpoint.state",
                checkpoint_id,
            ),
            reload_operation_id=operation_id,
            reload_resume_phase=resume_phase,
        )

    async def _apply_requested_cancel(self, owner, recovery: dict | None = None) -> bool:
        recovery = recovery or await self.agent.load_recovery(owner.run_id)
        cancellation = recovery.get("cancel_request")
        if cancellation is None:
            return False
        phase = recovery["state"]
        if phase in {"MODEL_PENDING", "BLOCKED_UNKNOWN", "RELOAD_WAIT"}:
            return False
        if phase in {"COMPLETED", "FAILED", "CANCELLED"}:
            return phase == "CANCELLED"
        if phase in {"WAIT_TOOL", "WAIT_APPROVAL"}:
            tool = recovery.get("current_tool")
            if tool is None:
                raise ForestError(
                    "AGENT_HISTORY_INCOMPLETE",
                    "Agent cancellation found no current tool",
                    500,
                )
            if tool["state"] in {"DISPATCHING", "UNKNOWN"}:
                return False
            await self.agent.observe_tool(
                owner,
                observation_operation_id=_stable(
                    "tool-cancel", tool["tool_call_id"], cancellation["operation_id"]
                ),
                tool_call_id=tool["tool_call_id"],
                observed_state="CANCELLED",
                evidence_kind="AUTHORIZATION",
                effect_observation_ref=cancellation["operation_id"],
                error_code=cancellation["reason_code"],
            )
            recovery = await self.agent.load_recovery(owner.run_id)
            phase = recovery["state"]
        checkpoint_id = _stable(
            "checkpoint-cancel", owner.run_id, cancellation["operation_id"]
        )
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            media_type="application/json",
            content=json.dumps(
                {
                    "phase": "CANCELLED",
                    "operation_id": cancellation["operation_id"],
                    "reason_code": cancellation["reason_code"],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            expected_deletion_epoch=epoch,
        )
        await self.agent.transition(
            owner,
            checkpoint_id=checkpoint_id,
            expected_phase=phase,
            target_phase="CANCELLED",
            state=AgentPayloadRef(
                saved.payload_id,
                saved.content_digest,
                "agent.runtime",
                "checkpoint.state",
                checkpoint_id,
            ),
            error_code=cancellation["reason_code"],
        )
        return True

    async def _fail_model_transport(
        self,
        owner,
        *,
        request_id: str,
        attempt_count: int,
        last_error_code: str | None,
    ) -> None:
        """Durably terminate one model step after a bounded safe retry sequence."""

        failure_code = "AGENT_MODEL_TRANSPORT_FAILED"
        checkpoint_id = _stable(
            "checkpoint-model-transport-failed",
            owner.run_id,
            request_id,
            attempt_count,
        )
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            media_type="application/json",
            content=json.dumps(
                {
                    "phase": "FAILED",
                    "reason_code": failure_code,
                    "request_id": request_id,
                    "attempt_count": attempt_count,
                    "last_error_code": last_error_code,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
            expected_deletion_epoch=epoch,
        )
        await self.agent.transition(
            owner,
            checkpoint_id=checkpoint_id,
            expected_phase="PREPARING_MODEL",
            target_phase="FAILED",
            state=AgentPayloadRef(
                saved.payload_id,
                saved.content_digest,
                "agent.runtime",
                "checkpoint.state",
                checkpoint_id,
            ),
            error_code=failure_code,
        )
        raise ForestError(
            failure_code,
            "Model transport failed within its bounded retry policy",
            503,
            details={
                "request_id": request_id,
                "attempt_count": attempt_count,
                "last_error_code": last_error_code,
            },
        )

    @staticmethod
    def _cancelled(run: dict) -> ForestError:
        return ForestError(
            "AGENT_RUN_CANCELLED",
            "Agent Run was cancelled by a durable management request",
            409,
            details={"run_id": run["run_id"], "reason_code": run.get("error_code")},
        )

    async def _protocol_repair_view(
        self, recovery: dict
    ) -> tuple[ProtocolRepairView, str]:
        failure = recovery.get("current_protocol_failure")
        if (
            not isinstance(failure, dict)
            or failure.get("resulting_state") != "REPAIR_READY"
            or failure.get("repair_step_seq") != recovery.get("next_step_seq")
        ):
            raise ForestError(
                "AGENT_HISTORY_INCOMPLETE",
                "Repair boundary has no durable protocol failure",
                500,
            )
        try:
            feedback = json.loads(await self.payloads.read_bytes(failure["feedback_payload_id"]))
        except (KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "AGENT_PROTOCOL_REPAIR_PAYLOAD_CORRUPT",
                "Protocol repair feedback cannot be restored",
                500,
            ) from exc
        try:
            repair_view = ProtocolRepairView.from_record(feedback)
        except ForestError as exc:
            raise ForestError(
                "AGENT_PROTOCOL_REPAIR_PAYLOAD_CONFLICT",
                "Protocol repair feedback differs from its durable boundary",
                409,
            ) from exc
        if (
            repair_view.repair_count != recovery.get("repair_count")
            or repair_view.error_code != failure.get("error_code")
            or repair_view.candidate_digest != failure.get("candidate_digest")
        ):
            raise ForestError(
                "AGENT_PROTOCOL_REPAIR_PAYLOAD_CONFLICT",
                "Protocol repair feedback differs from its durable boundary",
                409,
            )
        return repair_view, failure["failed_request_id"]

    async def execute(self, request: AgentExecutionRequest) -> AgentExecutionCompletion:
        if type(request) is not AgentExecutionRequest:
            raise ForestError(
                "AGENT_EXECUTION_INPUT_INVALID", "Agent execution request is invalid", 422
            )
        run = await self._load_or_start(request)
        if run["state"] == "COMPLETED":
            return self._completion(run, already_committed=True)
        if run["state"] == "CANCELLED":
            raise self._cancelled(run)
        if run["state"] == "FAILED":
            raise ForestError(
                run.get("error_code") or f"AGENT_RUN_{run['state']}",
                f"Agent Run is already {run['state'].lower()}",
                409,
            )
        owner = await self.agent.acquire_run(
            request.run_ref,
            self.owner_id,
            lease_seconds=300,
        )
        try:
            profile = self.agent.profile()
            declarations = tuple(item.declaration for item in self.routes.values())
            bindings = tuple(item.binding for item in self.routes.values())
            while True:
                recovery = await self.agent.load_recovery(request.run_ref)
                state = recovery["state"]
                if await self._apply_requested_cancel(owner, recovery):
                    raise self._cancelled(await self.agent.load(request.run_ref))
                if state == "COMPLETED":
                    return self._completion(recovery, already_committed=True)
                if state == "FINAL_READY":
                    return self._completion(recovery)
                if recovery["next_step_seq"] > profile.max_steps:
                    raise ForestError(
                        "AGENT_STEP_LIMIT_EXCEEDED",
                        "Agent Run exceeded its configured model-step limit",
                        409,
                    )
                if state in {"PREPARING_MODEL", "REPAIR_READY"}:
                    repair = recovery.get("current_protocol_failure")
                    is_repair = (
                        isinstance(repair, dict)
                        and repair.get("repair_step_seq") == recovery["next_step_seq"]
                    )
                    if recovery["current_request"] is None and not is_repair:
                        new_input = await request.next_model_input()
                        if new_input is not None:
                            await self.agent.append_input(
                                owner,
                                agent_input_from_selection(new_input),
                            )
                            recovery = await self.agent.load_recovery(request.run_ref)
                    run = await self.agent.load(request.run_ref)
                    step_seq = run["next_step_seq"]
                    request_id = _stable("model-request", run["run_id"], step_seq)
                    stored_request = recovery["current_request"]
                    attempt_no = 1
                    attempt_id = _stable("model-attempt", request_id, attempt_no)
                    if stored_request is not None:
                        if stored_request["state"] == "NOT_APPLIED":
                            completed_attempts = stored_request["attempt_count"]
                            if completed_attempts >= MODEL_TRANSPORT_MAX_ATTEMPTS:
                                await self._fail_model_transport(
                                    owner,
                                    request_id=request_id,
                                    attempt_count=completed_attempts,
                                    last_error_code=None,
                                )
                            await asyncio.sleep(
                                MODEL_TRANSPORT_RETRY_BACKOFF_SECONDS[
                                    min(
                                        completed_attempts - 1,
                                        len(MODEL_TRANSPORT_RETRY_BACKOFF_SECONDS) - 1,
                                    )
                                ]
                            )
                        attempt_no = stored_request["attempt_count"] + (
                            1 if stored_request["state"] == "NOT_APPLIED" else 0
                        )
                        attempt_no = max(1, attempt_no)
                        current_attempt = recovery["current_attempt"]
                        attempt_id = (
                            current_attempt["attempt_id"]
                            if current_attempt is not None
                            and stored_request["state"] == "ATTEMPT_PREPARED"
                            else _stable("model-attempt", request_id, attempt_no)
                        )
                    step = AgentModelStep(
                        request_id=request_id,
                        attempt_id=attempt_id,
                        effect_operation_ref=_stable("model-effect", attempt_id),
                        turn_id=_stable("model-turn", run["run_id"], step_seq),
                        final_reply_ref=_stable("reply", run["run_id"]),
                    )
                    logical_step_id = f"{run['run_id']}:step:{step_seq}"
                    repair_of_request_id = None
                    repair_view = None
                    if is_repair:
                        repair_view, repair_of_request_id = await self._protocol_repair_view(
                            recovery
                        )
                        snapshot = {
                            "bot_id": self.agent.bot_id,
                            "run_id": run["run_id"],
                            "logical_step_id": logical_step_id,
                            "input_revision": run["input_revision"],
                            "context_revision": run["context_revision"],
                            "context_view_revision": run["context_view_revision"],
                            "selected_input_end_seq": run["selected_input_end_seq"],
                            "trigger_ids": [item["trigger_ref"] for item in run["triggers"]],
                            "input_batch_ids": [item["input_batch_ref"] for item in run["inputs"]],
                            "units": [],
                        }
                        recalled = None
                        combined_media = None
                    else:
                        context = await request.context_snapshot()
                        snapshot = await self.projector.project(
                            run=run,
                            context=context,
                            logical_step_id=logical_step_id,
                            tool_bindings=bindings,
                        )
                        latest_input = run["inputs"][-1]
                        recalled = await self.memory.recall(
                            recall_id=_stable(
                                "memory-recall",
                                run["run_id"],
                                latest_input["input_revision"],
                                latest_input["input_batch_ref"],
                                latest_input["context_revision"],
                                run["selected_input_end_seq"],
                            ),
                            conversation_id=request.conversation_id,
                            input_batch_ref=latest_input["input_batch_ref"],
                            context_revision=latest_input["context_revision"],
                            input_cut=run["selected_input_end_seq"],
                        )
                        current_media = await self.media.resolve_context(snapshot)
                        recalled_media = {} if recalled is None else recalled.media
                        combined_media = {**current_media, **recalled_media}
                    await request.before_external_step()
                    result = await self.model.execute(
                        owner,
                        step,
                        snapshot,
                        preset_context=() if is_repair else profile.preset_context,
                        tools=declarations,
                        media=combined_media,
                        evidence=None if recalled is None else recalled.evidence,
                        tool_bindings=bindings,
                        output_tokens=profile.output_tokens,
                        stream=profile.stream,
                        request_kind="REPAIR" if is_repair else "PRIMARY",
                        repair_of_request_id=repair_of_request_id,
                        repair_view=repair_view,
                        allow_protocol_repair=(
                            not is_repair
                            and recovery["repair_count"] < 1
                            and recovery["next_step_seq"] < profile.max_steps
                        ),
                    )
                    if result.status == "UNKNOWN":
                        raise ForestError(
                            "AGENT_MODEL_RESULT_UNKNOWN",
                            "Model result is unknown and requires reconciliation",
                            409,
                        )
                    if result.status == "FAILED":
                        raise ForestError(
                            result.error_code or "AGENT_PROTOCOL_FAILED",
                            "Model output failed the bounded Agent protocol repair",
                            422,
                        )
                    if result.status == "NOT_APPLIED" and (
                        attempt_no >= MODEL_TRANSPORT_MAX_ATTEMPTS
                        or any(
                            (result.error_code or "").startswith(code)
                            for code in _NON_RETRYABLE_MODEL_TRANSPORT_ERRORS
                        )
                    ):
                        await self._fail_model_transport(
                            owner,
                            request_id=request_id,
                            attempt_count=attempt_no,
                            last_error_code=result.error_code,
                        )
                    continue
                if state == "MODEL_PENDING":
                    result = await self.model.reconcile_after_restart(owner)
                    if result.status == "UNKNOWN":
                        raise ForestError(
                            "AGENT_MODEL_RESULT_UNKNOWN",
                            result.error_code or "Model result is unknown",
                            409,
                        )
                    continue
                if state in {"PARSING", "SAFE_BOUNDARY"}:
                    request_head = recovery["current_request"]
                    attempt_head = recovery["current_attempt"]
                    latest_turn = recovery["latest_turn"]
                    if (state == "PARSING" and request_head is None) or (
                        state == "SAFE_BOUNDARY" and latest_turn is None
                    ):
                        raise ForestError(
                            "AGENT_HISTORY_INCOMPLETE",
                            "Agent recovery head is missing its durable model step",
                            500,
                        )
                    request_id = (
                        request_head["request_id"]
                        if request_head is not None
                        else latest_turn["request_id"]
                    )
                    step_seq = (
                        request_head["step_seq"]
                        if request_head is not None
                        else latest_turn["step_seq"]
                    )
                    step = AgentModelStep(
                        request_id=request_id,
                        attempt_id=(
                            attempt_head["attempt_id"]
                            if attempt_head is not None
                            else _stable("model-attempt", request_id, 1)
                        ),
                        effect_operation_ref=(
                            attempt_head["effect_operation_ref"]
                            if attempt_head is not None
                            else _stable("model-effect", request_id)
                        ),
                        turn_id=_stable("model-turn", recovery["run_id"], step_seq),
                        final_reply_ref=_stable("reply", recovery["run_id"]),
                    )
                    if state == "PARSING":
                        parsed = await self.model.resume_parsing(
                            owner,
                            step,
                            tools=declarations,
                            tool_bindings=bindings,
                        )
                        if parsed.status == "FAILED":
                            raise ForestError(
                                parsed.error_code or "AGENT_PROTOCOL_FAILED",
                                "Model output failed the bounded Agent protocol repair",
                                422,
                            )
                    else:
                        await self.model.resume_safe_boundary(owner, step)
                    continue
                if state in {"WAIT_TOOL", "WAIT_APPROVAL"}:
                    tool = recovery["current_tool"]
                    if tool is None:
                        raise ForestError(
                            "AGENT_HISTORY_INCOMPLETE", "Agent tool head is missing", 500
                        )
                    route = self._route(tool["tool_name"])
                    if tool["state"] == "DISPATCHING":
                        result = await self.tools.reconcile(
                            owner,
                            route.binding,
                            route.execution,
                        )
                    else:
                        await request.before_external_step()
                        result = await self.tools.execute(
                            owner,
                            route.binding,
                            route.authorization,
                            route.execution,
                        )
                    if result.status == "WAIT_APPROVAL":
                        raise ForestError(
                            "AGENT_WAITING_APPROVAL",
                            "Agent tool call is waiting for approval",
                            409,
                            details={"approval_ref": result.approval_ref},
                        )
                    if result.status == "UNKNOWN":
                        raise ForestError(
                            "AGENT_TOOL_RESULT_UNKNOWN",
                            result.error_code or "Tool result is unknown",
                            409,
                        )
                    continue
                if state in {
                    "RELOAD_WAIT",
                    "BLOCKED_UNKNOWN",
                    "WAIT_INPUT",
                }:
                    raise ForestError(
                        "AGENT_RECOVERY_REQUIRED",
                        f"Agent Run requires explicit recovery from {state}",
                        409,
                    )
                raise ForestError(
                    "AGENT_EXECUTION_STATE_INVALID",
                    f"Agent Run cannot execute from {state}",
                    409,
                )
        except AgentReloadRequested as exc:
            await self._park_for_reload(owner, exc.operation_id)
            raise ForestError(
                "AGENT_RELOAD_WAIT",
                "Agent Run is parked for RuntimeGraph replacement",
                409,
                details={"operation_id": exc.operation_id},
            ) from exc
        except ForestError as exc:
            if exc.code != "AGENT_CANCEL_REQUESTED":
                raise
            if not await self._apply_requested_cancel(owner):
                raise
            raise self._cancelled(await self.agent.load(request.run_ref)) from exc
        finally:
            try:
                await self.agent.release_run(owner)
            except ForestError as exc:
                if exc.code not in {
                    "STALE_AGENT_RUN_OWNER",
                    "RUNTIME_GENERATION_NOT_ACTIVE",
                }:
                    raise
