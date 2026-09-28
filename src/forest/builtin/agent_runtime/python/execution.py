"""Public, serializable execution service for one durable Agent Run."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from context_projection import ContextSnapshotReader
from model_execution import ModelExecution
from tool_execution import ToolExecution
from turn_recording import TurnRecorder, stable, tool_directory

from forest.errors import ForestError

_MAX_TRANSPORT_ATTEMPTS = 3
_BACKOFF_SECONDS = (1.0, 3.0)


def _deadline(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ForestError(
            "AGENT_EXECUTION_INPUT_INVALID", "Agent deadline is invalid", 422
        ) from exc
    if parsed.tzinfo is None:
        raise ForestError(
            "AGENT_EXECUTION_INPUT_INVALID", "Agent deadline must include a timezone", 422
        )
    return parsed.astimezone(UTC)


def _candidate(value: object, *, request_id: str, attempt_id: str) -> str:
    if (
        not isinstance(value, dict)
        or value.get("protocol") != "forest.provider-model-result/1"
        or value.get("status") != "RESPONDED"
        or value.get("request_id") != request_id
        or value.get("transport_attempt_id") != attempt_id
        or not isinstance(value.get("text"), str)
        or not value["text"]
        or value.get("error_code") is not None
    ):
        raise ForestError(
            "PROVIDER_RESULT_INVALID",
            "Durable Provider result differs from the Agent execution head",
            409,
        )
    return value["text"]


class AgentExecution:
    """Drive the QiYu-style single Agent using only package capability handles."""

    def __init__(self, ctx, config: dict) -> None:
        self.ctx = ctx
        self.config = config
        self.model = ModelExecution(ctx)
        self.tools = ToolExecution(ctx)
        self.turns = TurnRecorder(ctx)

    def _configuration_ref(self) -> dict:
        identity = self.ctx.identity
        values = {
            "payload_id": identity.get("configuration_payload_id"),
            "digest": identity.get("configuration_payload_digest"),
            "owner_component_id": "agent.runtime",
            "owner_kind": "module.configuration",
            "owner_id": identity.get("configuration_owner_id"),
        }
        if any(not isinstance(value, str) or not value for value in values.values()):
            raise ForestError(
                "AGENT_GENERATION_FENCE_MISSING",
                "Agent Worker has no frozen configuration Payload identity",
                409,
            )
        return values

    def _preset_ref(self) -> dict:
        value = self.config["preset"]
        return {
            "payload_id": value["payload_id"],
            "digest": value["digest"],
            "owner_component_id": "agent.runtime",
            "owner_kind": "preset.context",
            "owner_id": value["owner_id"],
        }

    async def _preset_context(self) -> list[str]:
        document = await self.ctx.payloads.read_json(self._preset_ref())
        messages = document.get("system") if isinstance(document, dict) else None
        if (
            not isinstance(messages, list)
            or len(messages) > 32
            or any(not isinstance(item, str) for item in messages)
            or sum(len(item.encode("utf-8")) for item in messages) > 32768
        ):
            raise ForestError(
                "AGENT_CONFIGURATION_INVALID", "Agent preset Payload is invalid", 500
            )
        return messages

    @staticmethod
    def _owner(value: dict) -> dict:
        return {
            "run_id": value["run_id"],
            "owner_id": value["owner_id"],
            "epoch": value["epoch"],
        }

    async def _run(self, run_id: str) -> dict:
        return await self.ctx.queries.call("agent.run-snapshot", {"run_id": run_id})

    async def _recovery(self, run_id: str) -> dict:
        return await self.ctx.queries.call("agent.run-recovery", {"run_id": run_id})

    async def _history(self, run_id: str) -> dict:
        return await self.ctx.queries.call("agent.execution-history", {"run_id": run_id})

    async def _start(self, request: dict) -> dict | None:
        started = await self.ctx.uow.call(
            "agent.start-from-conversation",
            {
                "conversation": {
                    "actor": request["actor"],
                    "boundary": "IDLE",
                    "selection_mode": "ANY",
                    "session_ref": request["session_ref"],
                    "run_ref": request["run_ref"],
                    "next_run_ref": None,
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "selection_step_id": "conversation",
                    "deletion_epoch_step_id": "payload_epoch",
                },
                "agent": {
                    "conversation_ref": request["conversation_id"],
                    "session_id": request["session_ref"],
                    "run_id": request["run_ref"],
                    "agent_spec_revision": self.config["agent_spec_revision"],
                    "config": self._configuration_ref(),
                    "preset_revision": self.config["preset_revision"],
                    "preset": self._preset_ref(),
                    "absolute_deadline": request["absolute_deadline"],
                    "selection_step_id": "conversation",
                    "memory_step_id": "memory",
                },
            },
            operation_id=self.ctx.operations.stable(f"start-run:{request['run_ref']}"),
            timeout=25,
        )
        return None if started["status"] == "IDLE" else started["run"]

    async def _prefill(self, request: dict, boundary: str) -> bool:
        for _ in range(8):
            request_id = uuid4().hex
            selected = await self.ctx.services.call(
                "conversation.context.prefill",
                {
                    "protocol": "forest.conversation-context-prefill/1",
                    "request_id": request_id,
                    "actor": request["actor"],
                    "boundary": boundary,
                    "session_ref": None if boundary == "IDLE" else request["session_ref"],
                    "run_ref": None if boundary == "IDLE" else request["run_ref"],
                },
                operation_id=self.ctx.operations.stable(f"prefill:{request_id}"),
                timeout=35,
            )
            if selected["status"] == "CONTINUE":
                return True
            if selected["status"] != "BATCH" or selected["kind"] != "CONTEXT_PREFILL":
                raise ForestError(
                    "AGENT_PREFILL_RESULT_INVALID", "Conversation prefill was invalid", 409
                )
        return False

    async def _load_or_start(self, request: dict) -> dict | None:
        try:
            run = await self._run(request["run_ref"])
        except ForestError as exc:
            if exc.code != "AGENT_RUN_NOT_FOUND" or request["resume"]:
                raise
            if not await self._prefill(request, "IDLE"):
                return {"_prefill_pending": True}
            try:
                run = await self._start(request)
            except ForestError as start_error:
                if start_error.code != "CONVERSATION_PREFILL_REQUIRED":
                    raise
                return {"_prefill_pending": True}
        if run is None:
            return None
        if run.get("_prefill_pending"):
            return run
        if (
            run["session_id"] != request["session_ref"]
            or run["conversation_ref"] != request["conversation_id"]
            or (
                request["absolute_deadline"] is not None
                and _deadline(run["absolute_deadline"])
                != _deadline(request["absolute_deadline"])
            )
        ):
            raise ForestError(
                "AGENT_EXECUTION_FENCE_MISMATCH",
                "Agent Run belongs to another Conversation execution fence",
                409,
            )
        return run

    async def _acquire_once(self, request: dict, owner: dict, step_seq: int) -> dict:
        return await self.ctx.uow.call(
            "agent.acquire-model-input",
            {
                "conversation": {
                    "actor": request["actor"],
                    "boundary": "MODEL_SAFE_POINT",
                    "selection_mode": "ANY",
                    "session_ref": request["session_ref"],
                    "run_ref": request["run_ref"],
                    "next_run_ref": None,
                },
                "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                "memory": {
                    "selection_step_id": "conversation",
                    "deletion_epoch_step_id": "payload_epoch",
                },
                "agent": {
                    "owner": owner,
                    "selection_step_id": "conversation",
                    "memory_step_id": "memory",
                },
            },
            operation_id=self.ctx.operations.stable(
                # CONTINUE/WAIT_INPUT are observations, not a permanent result
                # for this model step. A committed BATCH advances Run revision
                # atomically, so recovery can safely issue a fresh observation.
                f"acquire-model-input:{request['run_ref']}:{step_seq}:{uuid4().hex}"
            ),
            timeout=25,
        )

    async def _acquire_input(self, request: dict, owner: dict, step_seq: int) -> dict:
        for _ in range(2):
            try:
                return await self._acquire_once(request, owner, step_seq)
            except ForestError as exc:
                if exc.code != "CONVERSATION_PREFILL_REQUIRED":
                    raise
                if not await self._prefill(request, "MODEL_SAFE_POINT"):
                    return {"status": "WAIT_INPUT"}
        return {"status": "WAIT_INPUT"}

    @staticmethod
    def _reply(run: dict) -> dict | None:
        candidate = run.get("final_candidate")
        if candidate is None:
            return None
        return {
            "payload_id": candidate["payload_id"],
            "digest": candidate["digest"],
            "owner_component_id": "agent.runtime",
            "owner_kind": "final.reply",
            "owner_id": stable("reply", run["run_id"]),
        }

    @staticmethod
    def _result(request: dict, status: str, run: dict | None = None, **values) -> dict:
        triggers = [] if run is None else list(dict.fromkeys(
            item["trigger_ref"] for item in run["triggers"]
        ))
        return {
            "protocol": "forest.agent-execution/2",
            "status": status,
            "session_ref": request["session_ref"],
            "run_ref": request["run_ref"],
            "reply": None if run is None else AgentExecution._reply(run),
            "answered_trigger_ids": triggers,
            "approval_ref": values.get("approval_ref"),
            "error_code": values.get("error_code"),
        }

    async def _model_snapshot(self, request: dict, run: dict, step_seq: int) -> dict:
        envelope = await self.ctx.services.call(
            "conversation.context.snapshot",
            {"actor": request["actor"]},
            operation_id=self.ctx.operations.stable(
                f"context-snapshot:{run['run_id']}:{run['progress']['input_revision']}"
            ),
            contract_major=2,
            timeout=120,
        )
        reader = ContextSnapshotReader(self.ctx, bot_id=self.ctx.identity["bot_id"])
        snapshot = await reader.load(
            envelope,
            expected={
                "conversation_id": run["conversation_ref"],
                "context_applied_seq": run["progress"]["selected_input_end_seq"],
                "context_revision": run["progress"]["context_revision"],
                "context_view_revision": run["progress"]["context_view_revision"],
            },
        )
        return await reader.model_view(
            snapshot,
            run=run,
            history=await self._history(run["run_id"]),
            logical_step_id=f"{run['run_id']}:step:{step_seq}",
        )

    @staticmethod
    def _memory_query_id(run: dict, step_seq: int) -> str:
        return stable(
            "memory-query", run["run_id"], step_seq,
            run["progress"]["input_revision"],
        )

    async def _cancel_superseded_recall(self, run: dict, step_seq: int) -> None:
        if run["progress"]["input_revision"] < 1:
            return
        query_id = self._memory_query_id(run, step_seq)
        try:
            await self.ctx.services.call(
                "memory.query.cancel",
                {"query_id": query_id, "reason_code": "SUPERSEDED"},
                operation_id=self.ctx.operations.stable(f"memory-cancel:{query_id}"),
                timeout=25,
            )
        except ForestError as exc:
            # A query may have been admitted while its owned work was not yet requested.
            if exc.code != "MEMORY_RECALL_NOT_REQUESTED":
                raise

    async def _recall(self, run: dict, step_seq: int) -> tuple[str, str, dict | None]:
        revision = run["progress"]["input_revision"]
        inputs = [item for item in run["inputs"] if item["input_revision"] == revision]
        if len(inputs) != 1:
            raise ForestError(
                "AGENT_MEMORY_INPUT_MISSING", "Current Agent input batch is not durable", 409
            )
        batch = inputs[0]
        query_id = self._memory_query_id(run, step_seq)
        # The Run start gives every model step a non-extending recall window.
        # Memory accepts new work only within ten minutes of its first request.
        recall_deadline = min(
            _deadline(run["absolute_deadline"]),
            _deadline(run["created_at"]) + timedelta(minutes=9),
        )
        async def observe_work():
            return await self.ctx.services.call(
                "memory.query.work-status",
                {"query_id": query_id},
                operation_id=self.ctx.operations.stable(
                    f"memory-status:{query_id}:{uuid4().hex}"
                ),
                timeout=30,
            )

        try:
            status = await observe_work()
        except ForestError as exc:
            if exc.code != "MEMORY_RECALL_NOT_REQUESTED":
                raise
            try:
                begun = await self.ctx.services.call(
                    "memory.query.begin",
                    {
                        "query_id": query_id,
                        "conversation_ref": run["conversation_ref"],
                        "input_batch_ref": batch["input_batch_ref"],
                        "context_revision": batch["context_revision"],
                    },
                    operation_id=self.ctx.operations.stable(f"memory-begin:{query_id}"),
                    timeout=30,
                )
            except ForestError as begin_error:
                if begin_error.code == "MEMORY_QUERY_CAPTURE_PENDING":
                    return "PENDING", query_id, None
                raise
            if (begun["query_id"] != query_id
                    or begun["input_batch_ref"] != batch["input_batch_ref"]
                    or begun["context_revision"] != batch["context_revision"]):
                raise ForestError(
                    "AGENT_MEMORY_FENCE_MISMATCH", "Memory query differs from Agent input", 409
                )
            requested = await self.ctx.services.call(
                "memory.query.request",
                {"query_id": query_id, "deadline_at": recall_deadline.isoformat()},
                operation_id=self.ctx.operations.stable(f"memory-request:{query_id}"),
                timeout=30,
            )
            if (requested["query_id"] != query_id
                    or _deadline(requested["deadline_at"]) != recall_deadline):
                raise ForestError(
                    "AGENT_MEMORY_FENCE_MISMATCH", "Memory work deadline differs from Agent input", 409
                )
            status = await observe_work()
        if status["query_id"] != query_id or _deadline(status["deadline_at"]) != recall_deadline:
            raise ForestError(
                "AGENT_MEMORY_FENCE_MISMATCH", "Memory work belongs to another query", 409
            )
        if status["state"] == "READY":
            return "READY", query_id, status["evidence_ref"]
        if status["state"] in {"PENDING", "CLAIMED"}:
            return "PENDING", query_id, None
        if status["state"] == "BLOCKED_UNKNOWN":
            return "BLOCKED_UNKNOWN", query_id, None
        raise ForestError(
            status["error_code"] or "AGENT_MEMORY_RECALL_FAILED",
            "Memory recall failed before the model call", 409,
        )

    @staticmethod
    def _repair_snapshot(run: dict, step_seq: int) -> dict:
        return {
            "bot_id": None,
            "run_id": run["run_id"],
            "logical_step_id": f"{run['run_id']}:step:{step_seq}",
            "input_revision": run["progress"]["input_revision"],
            "context_revision": run["progress"]["context_revision"],
            "context_view_revision": run["progress"]["context_view_revision"],
            "selected_input_end_seq": run["progress"]["selected_input_end_seq"],
            "trigger_ids": [item["trigger_ref"] for item in run["triggers"]],
            "input_batch_ids": [item["input_batch_ref"] for item in run["inputs"]],
            "units": [],
        }

    async def execute(self, request: dict) -> dict:
        if request["actor"]["conversation_id"] != request["conversation_id"]:
            raise ForestError(
                "AGENT_EXECUTION_INPUT_INVALID",
                "Conversation actor belongs to another conversation",
                422,
            )
        requested_deadline = request["absolute_deadline"]
        if not request["resume"] and requested_deadline is None:
            raise ForestError(
                "AGENT_EXECUTION_INPUT_INVALID", "A new Agent Run requires a deadline", 422
            )
        deadline = None if requested_deadline is None else _deadline(requested_deadline)
        if not request["resume"] and deadline <= datetime.now(UTC):
            raise ForestError(
                "AGENT_EXECUTION_INPUT_INVALID",
                "A new Agent Run requires a future deadline",
                422,
            )
        if not request["resume"] and deadline > datetime.now(UTC) + timedelta(
            seconds=self.config["run_timeout_seconds"] + 5
        ):
            raise ForestError(
                "AGENT_EXECUTION_INPUT_INVALID",
                "Agent deadline exceeds the frozen run timeout",
                422,
            )
        run = await self._load_or_start(request)
        if run is not None and run.get("_prefill_pending"):
            return self._result(request, "WAIT_INPUT")
        if run is None:
            return self._result(request, "IDLE")
        if run["state"] in {"COMPLETED", "FAILED", "CANCELLED"}:
            return self._result(
                request,
                run["state"],
                run,
                error_code=run.get("error_code"),
            )
        if deadline is None:
            deadline = _deadline(run["absolute_deadline"])
        owner_record = await self.ctx.uow.call(
            "agent.acquire-run-lease",
            {
                "run_id": run["run_id"],
                "owner_id": self.ctx.operations.stable(f"run-owner:{run['run_id']}"),
                "lease_seconds": 300,
            },
            operation_id=self.ctx.operations.stable(f"acquire-run:{run['run_id']}"),
            timeout=15,
        )
        owner = self._owner(owner_record)
        try:
            directory = tool_directory(
                await self.ctx.agent_tools.list(), identity=dict(self.ctx.identity)
            )
            declarations, bindings = directory
            preset_context = await self._preset_context()
            while True:
                if datetime.now(UTC) >= deadline:
                    raise ForestError(
                        "AGENT_DEADLINE_EXCEEDED", "Agent execution deadline expired", 409
                    )
                recovery = await self._recovery(run["run_id"])
                state = recovery["state"]
                run = await self._run(run["run_id"])
                if state in {"COMPLETED", "FAILED", "CANCELLED"}:
                    return self._result(
                        request, state, run, error_code=run.get("error_code")
                    )
                if state == "FINAL_READY":
                    return self._result(request, "FINAL_READY", run)
                if state == "WAIT_INPUT":
                    return self._result(request, "WAIT_INPUT", run)
                if state == "WAIT_APPROVAL":
                    tool = recovery.get("tool_call")
                    if not isinstance(tool, dict):
                        raise ForestError(
                            "AGENT_HISTORY_INCOMPLETE",
                            "Agent approval boundary has no durable ToolCall",
                            500,
                        )
                    binding = bindings.get(tool["tool_name"])
                    if binding is None:
                        raise ForestError(
                            "AGENT_TOOL_ROUTE_STALE",
                            "Durable ToolCall is absent from the frozen directory",
                            409,
                        )
                    resolved = await self.tools.invoke(
                        owner=owner, tool=tool, binding=binding
                    )
                    if resolved["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                        continue
                    return self._result(
                        request,
                        state,
                        run,
                        approval_ref=resolved.get("approval_ref"),
                        error_code=resolved.get("error_code"),
                    )
                if state == "WAIT_TOOL":
                    tool = recovery.get("tool_call")
                    if not isinstance(tool, dict):
                        raise ForestError(
                            "AGENT_HISTORY_INCOMPLETE",
                            "Agent tool boundary has no durable ToolCall",
                            500,
                        )
                    binding = bindings.get(tool["tool_name"])
                    if binding is None:
                        raise ForestError(
                            "AGENT_TOOL_ROUTE_STALE",
                            "Durable ToolCall is absent from the frozen directory",
                            409,
                        )
                    invoked_tool = await self.tools.invoke(
                        owner=owner, tool=tool, binding=binding
                    )
                    if invoked_tool["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                        continue
                    return self._result(
                        request,
                        (
                            "WAIT_APPROVAL"
                            if invoked_tool["status"] == "WAIT_APPROVAL"
                            else "BLOCKED_UNKNOWN"
                        ),
                        run,
                        approval_ref=invoked_tool.get("approval_ref"),
                        error_code=invoked_tool.get("error_code"),
                    )
                if state == "BLOCKED_UNKNOWN" and isinstance(
                    recovery.get("tool_call"), dict
                ):
                    tool = recovery["tool_call"]
                    binding = bindings.get(tool["tool_name"])
                    if binding is None:
                        raise ForestError(
                            "AGENT_TOOL_ROUTE_STALE",
                            "Durable ToolCall is absent from the frozen directory",
                            409,
                        )
                    reconciled = await self.tools.invoke(
                        owner=owner, tool=tool, binding=binding
                    )
                    if reconciled["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                        continue
                    return self._result(
                        request,
                        "BLOCKED_UNKNOWN",
                        run,
                        error_code=reconciled.get("error_code"),
                    )
                if state in {"BLOCKED_UNKNOWN", "MODEL_PENDING"}:
                    return self._result(
                        request,
                        "BLOCKED_UNKNOWN",
                        run,
                        error_code="AGENT_MODEL_RESULT_UNKNOWN",
                    )
                if state == "RELOAD_WAIT":
                    return self._result(request, "RELOAD_WAIT", run)
                if recovery["next_step_seq"] > self.config["max_steps"]:
                    raise ForestError(
                        "AGENT_STEP_LIMIT_EXCEEDED",
                        "Agent Run exceeded its frozen model-step limit",
                        409,
                    )
                if state == "SAFE_BOUNDARY":
                    selected = await self._acquire_input(
                        request, owner, recovery["next_step_seq"]
                    )
                    if selected["status"] == "WAIT_INPUT":
                        return self._result(request, "WAIT_INPUT", run)
                    if selected["status"] == "BATCH":
                        continue
                    finalized = await self.turns.finalize(owner=owner, recovery=recovery)
                    return self._result(request, "FINAL_READY", finalized["run"])
                if state == "PARSING":
                    head = recovery.get("model_request")
                    attempt = recovery.get("model_attempt")
                    if not isinstance(head, dict) or not isinstance(attempt, dict):
                        raise ForestError(
                            "AGENT_HISTORY_INCOMPLETE",
                            "Agent parse boundary has no durable model attempt",
                            500,
                        )
                    result_ref = {
                        "payload_id": attempt["result_payload_id"],
                        "digest": attempt["result_payload_digest"],
                        "owner_component_id": head["provider_component_id"],
                        "owner_kind": "model.result",
                        "owner_id": attempt["attempt_id"],
                    }
                    result = await self.ctx.payloads.read_json(result_ref)
                    repair_of = None
                    if head["request_kind"] == "REPAIR":
                        _repair, repair_of = await self.turns.repair_view(recovery)
                    await self.turns.record(
                        owner=owner,
                        request_id=head["request_id"],
                        attempt_id=attempt["attempt_id"],
                        turn_id=stable(
                            "model-turn", run["run_id"], head["step_seq"]
                        ),
                        candidate=_candidate(
                            result,
                            request_id=head["request_id"],
                            attempt_id=attempt["attempt_id"],
                        ),
                        result_ref=result_ref,
                        declarations=declarations,
                        bindings=bindings,
                        repair_of_request_id=repair_of,
                        allow_repair=(
                            head["request_kind"] != "REPAIR"
                            and recovery["repair_count"] < 1
                            and recovery["next_step_seq"] < self.config["max_steps"]
                        ),
                    )
                    continue
                if state not in {"PREPARING_MODEL", "REPAIR_READY"}:
                    raise ForestError(
                        "AGENT_EXECUTION_STATE_INVALID",
                        f"Agent Run cannot execute from {state}",
                        409,
                    )
                is_repair = state == "REPAIR_READY"
                if not is_repair and recovery.get("model_request") is None:
                    selected = await self._acquire_input(
                        request, owner, recovery["next_step_seq"]
                    )
                    if selected["status"] == "WAIT_INPUT":
                        return self._result(request, "WAIT_INPUT", run)
                    if selected["status"] == "BATCH":
                        await self._cancel_superseded_recall(
                            run, recovery["next_step_seq"]
                        )
                    run = await self._run(run["run_id"])
                    recovery = await self._recovery(run["run_id"])
                step_seq = recovery["next_step_seq"]
                request_id = stable("model-request", run["run_id"], step_seq)
                stored_request = recovery.get("model_request")
                attempt_no = 1
                if isinstance(stored_request, dict):
                    if stored_request["state"] == "UNKNOWN":
                        return self._result(
                            request,
                            "BLOCKED_UNKNOWN",
                            run,
                            error_code="AGENT_MODEL_RESULT_UNKNOWN",
                        )
                    if stored_request["state"] == "NOT_APPLIED":
                        if stored_request["attempt_count"] >= _MAX_TRANSPORT_ATTEMPTS:
                            raise ForestError(
                                "AGENT_MODEL_TRANSPORT_FAILED",
                                "Model transport exhausted its bounded retry policy",
                                503,
                            )
                        delay = _BACKOFF_SECONDS[
                            min(stored_request["attempt_count"] - 1, len(_BACKOFF_SECONDS) - 1)
                        ]
                        await asyncio.sleep(delay)
                        attempt_no = stored_request["attempt_count"] + 1
                    elif recovery.get("model_attempt") is not None:
                        attempt_no = max(1, stored_request["attempt_count"])
                attempt = recovery.get("model_attempt")
                attempt_id = (
                    attempt["attempt_id"]
                    if isinstance(attempt, dict)
                    and stored_request is not None
                    and stored_request["state"] == "ATTEMPT_PREPARED"
                    else stable("model-attempt", request_id, attempt_no)
                )
                repair = None
                repair_of = None
                memory_query_id = None
                evidence_ref = None
                if is_repair:
                    repair, repair_of = await self.turns.repair_view(recovery)
                    snapshot = self._repair_snapshot(run, step_seq)
                    snapshot["bot_id"] = self.ctx.identity["bot_id"]
                    failure = recovery["protocol_failure"]
                    source_refs = [{
                        "payload_id": failure["feedback_payload_id"],
                        "digest": failure["feedback_payload_digest"],
                        "owner_component_id": "agent.runtime",
                        "owner_kind": "protocol.repair.feedback",
                        "owner_id": failure["failure_id"],
                    }]
                else:
                    memory_state, memory_query_id, evidence_ref = await self._recall(
                        run, step_seq
                    )
                    if memory_state == "BLOCKED_UNKNOWN":
                        return self._result(
                            request, "BLOCKED_UNKNOWN", run,
                            error_code="AGENT_MEMORY_RESULT_UNKNOWN",
                        )
                    if memory_state == "PENDING":
                        return self._result(request, "WAIT_MEMORY", run)
                    if stored_request is None:
                        selected = await self._acquire_input(request, owner, step_seq)
                        if selected["status"] == "WAIT_INPUT":
                            return self._result(request, "WAIT_INPUT", run)
                        if selected["status"] == "BATCH":
                            await self._cancel_superseded_recall(run, step_seq)
                            continue
                    snapshot = await self._model_snapshot(request, run, step_seq)
                    source_refs = [self._preset_ref()]
                invoked = await self.model.invoke(
                    owner=owner,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    effect_operation_id=stable("model-effect", attempt_id),
                    snapshot=snapshot,
                    preset_context=[] if is_repair else preset_context,
                    source_refs=source_refs,
                    tools=declarations,
                    evidence_ref=evidence_ref,
                    memory_query_id=memory_query_id,
                    repair=repair,
                    output_tokens=self.config["output_tokens"],
                    stream=self.config["stream"],
                    request_kind=(
                        "REPAIR"
                        if is_repair
                        else "PRIMARY"
                        if step_seq == 1
                        else "CONTINUATION"
                    ),
                )
                if invoked["status"] == "UNKNOWN":
                    return self._result(
                        request,
                        "BLOCKED_UNKNOWN",
                        run,
                        error_code=invoked["error_code"],
                    )
                if invoked["status"] == "NOT_SENT":
                    continue
                await self.turns.record(
                    owner=owner,
                    request_id=request_id,
                    attempt_id=attempt_id,
                    turn_id=stable("model-turn", run["run_id"], step_seq),
                    candidate=_candidate(
                        invoked["result"],
                        request_id=request_id,
                        attempt_id=attempt_id,
                    ),
                    result_ref=invoked["result_ref"],
                    declarations=declarations,
                    bindings=bindings,
                    repair_of_request_id=repair_of,
                    allow_repair=(
                        not is_repair
                        and recovery["repair_count"] < 1
                        and step_seq < self.config["max_steps"]
                    ),
                )
        finally:
            try:
                await self.ctx.uow.call(
                    "agent.release-run-lease",
                    {"owner": owner},
                    operation_id=self.ctx.operations.stable(
                        f"release-run:{run['run_id']}:{owner['epoch']}"
                    ),
                    timeout=15,
                )
            except ForestError as exc:
                if exc.code != "STALE_AGENT_RUN_OWNER":
                    raise
