"""Production execution port for one QiYu-style Agent Session/Run chain."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from forest.agent_context_projection import ConversationContextProjector
from forest.agent_model_executor import (
    AgentModelStep,
    AgentToolBinding,
    DurableAgentModelExecutor,
)
from forest.agent_tool_executor import DurableAgentToolExecutor
from forest.conversation_worker import (
    AgentExecutionCompletion,
    AgentExecutionRequest,
)
from forest.errors import ForestError
from forest.session_handoff_uow import agent_input_from_selection
from forest.storage.conversation_scheduler import (
    SchedulerContextFact,
    SchedulerPayloadRef,
)


def _stable(kind: str, *parts: object) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{kind}:{hashlib.sha256(encoded).hexdigest()}"


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
        memory,
        payloads,
        owner_id: str,
        tool_routes: tuple[FrozenToolRoute, ...] = (),
    ) -> None:
        self.agent = agent
        self.model = DurableAgentModelExecutor(
            agent=agent,
            provider=provider,
            payloads=payloads,
        )
        self.tools = DurableAgentToolExecutor(agent=agent, payloads=payloads)
        self.memory = memory
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

    async def execute(self, request: AgentExecutionRequest) -> AgentExecutionCompletion:
        if type(request) is not AgentExecutionRequest:
            raise ForestError(
                "AGENT_EXECUTION_INPUT_INVALID", "Agent execution request is invalid", 422
            )
        run = await self._load_or_start(request)
        if run["state"] == "COMPLETED":
            return self._completion(run, already_committed=True)
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
                if state == "PREPARING_MODEL":
                    if recovery["current_request"] is None:
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
                    context = await request.context_snapshot()
                    snapshot = await self.projector.project(
                        run=run,
                        context=context,
                        logical_step_id=f"{run['run_id']}:step:{step_seq}",
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
                    result = await self.model.execute(
                        owner,
                        step,
                        snapshot,
                        preset_context=profile.preset_context,
                        tools=declarations,
                        media=None if recalled is None else recalled.media,
                        evidence=None if recalled is None else recalled.evidence,
                        tool_bindings=bindings,
                        output_tokens=profile.output_tokens,
                        stream=profile.stream,
                    )
                    if result.status == "UNKNOWN":
                        raise ForestError(
                            "AGENT_MODEL_RESULT_UNKNOWN",
                            "Model result is unknown and requires reconciliation",
                            409,
                        )
                    continue
                if state == "MODEL_PENDING":
                    result = await self.model.reconcile_after_restart(owner)
                    raise ForestError(
                        "AGENT_MODEL_RESULT_UNKNOWN",
                        result.error_code or "Model result is unknown",
                        409,
                    )
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
                        await self.model.resume_parsing(
                            owner,
                            step,
                            tools=declarations,
                            tool_bindings=bindings,
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
                        )
                    if result.status == "UNKNOWN":
                        raise ForestError(
                            "AGENT_TOOL_RESULT_UNKNOWN",
                            result.error_code or "Tool result is unknown",
                            409,
                        )
                    continue
                if state in {
                    "REPAIR_READY",
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
        finally:
            try:
                await self.agent.release_run(owner)
            except ForestError as exc:
                if exc.code not in {
                    "STALE_AGENT_RUN_OWNER",
                    "RUNTIME_GENERATION_NOT_ACTIVE",
                }:
                    raise
