"""Public, serializable execution service for one durable Agent Run."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import uuid4

from context_projection import ContextSnapshotReader, claim_summaries
from model_execution import ModelExecution
from tool_execution import ToolExecution
from turn_recording import TurnRecorder, stable, tool_directory

from forest.errors import ForestError

_MAX_TRANSPORT_ATTEMPTS = 3
_BACKOFF_SECONDS = (1.0, 3.0)
_RUN_OWNER_LEASE_SECONDS = 300
_RUN_OWNER_RENEW_INTERVAL_SECONDS = 60
_RUN_OWNER_RETRY_SECONDS = 5
_RUN_OWNER_SAFETY_MARGIN_SECONDS = 60


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

    def __init__(
        self, ctx, config: dict, reload_operation: Callable[[], str | None]
    ) -> None:
        self.ctx = ctx
        self.config = config
        self.reload_operation = reload_operation
        self.model = ModelExecution(ctx)
        self.tools = ToolExecution(ctx)
        self.turns = TurnRecorder(ctx)
        self._releasing_owner = False

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

    async def _renew_owner_until_done(self, owner: dict, execution: asyncio.Task) -> None:
        """Keep the Run epoch live while this call awaits external work."""
        renewal = 1
        safe_until = monotonic() + (
            _RUN_OWNER_LEASE_SECONDS - _RUN_OWNER_SAFETY_MARGIN_SECONDS
        )
        await asyncio.sleep(_RUN_OWNER_RENEW_INTERVAL_SECONDS)
        while True:
            if self._releasing_owner:
                return
            try:
                await self.ctx.uow.call(
                    "agent.renew-run-lease",
                    {"owner": owner, "lease_seconds": _RUN_OWNER_LEASE_SECONDS},
                    operation_id=self.ctx.operations.stable(
                        f"renew-run:{owner['run_id']}:{owner['epoch']}:{renewal}"
                    ),
                    timeout=15,
                )
            except ForestError as exc:
                if self._releasing_owner:
                    return
                if (exc.status < 500 and exc.code != "OVERLOADED") or monotonic() >= safe_until:
                    execution.cancel()
                    return
                await asyncio.sleep(_RUN_OWNER_RETRY_SECONDS)
                continue
            except Exception:
                if self._releasing_owner:
                    return
                execution.cancel()
                return
            safe_until = monotonic() + (
                _RUN_OWNER_LEASE_SECONDS - _RUN_OWNER_SAFETY_MARGIN_SECONDS
            )
            renewal += 1
            await asyncio.sleep(_RUN_OWNER_RENEW_INTERVAL_SECONDS)

    async def _run(self, run_id: str) -> dict:
        return await self.ctx.queries.call("agent.run-snapshot", {"run_id": run_id})

    async def _recovery(self, run_id: str) -> dict:
        return await self.ctx.queries.call("agent.run-recovery", {"run_id": run_id})

    async def _park_reload(
        self, request: dict, owner: dict, run: dict, phase: str, operation_id: str
    ) -> dict:
        checkpoint_id = stable(
            "checkpoint-reload", run["run_id"], operation_id,
            run["progress"]["checkpoint_seq"] + 1,
        )
        state = await self.ctx.payloads.put_json(
            {
                "phase": "RELOAD_WAIT",
                "run_id": run["run_id"],
                "reload_operation_id": operation_id,
                "resume_phase": phase,
                "input_revision": run["progress"]["input_revision"],
                "next_step_seq": run["progress"]["next_step_seq"],
            },
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            parents=(self._preset_ref(),),
        )
        self._releasing_owner = True
        parked = await self.ctx.uow.call(
            "agent.park-reload",
            {
                "owner": owner,
                "checkpoint_id": checkpoint_id,
                "state": state,
                "reload_operation_id": operation_id,
                "resume_phase": phase,
            },
            operation_id=self.ctx.operations.stable(f"park-reload:{checkpoint_id}"),
            timeout=15,
        )
        return self._result(request, "RELOAD_WAIT", parked)

    async def _history(self, run_id: str) -> dict:
        return await self.ctx.queries.call("agent.execution-history", {"run_id": run_id})

    async def _fail_exhausted_model(self, request, owner, model_request, model_attempt):
        if (not isinstance(model_attempt, dict)
                or model_attempt["state"] != "NOT_APPLIED"):
            raise ForestError(
                "AGENT_MODEL_FAILURE_BOUNDARY_CLOSED",
                "Model transport is not known to be unsent",
                409,
            )
        request_id = model_request["request_id"]
        attempt_id = model_attempt["attempt_id"]
        frozen = await self.ctx.queries.call(
            "agent.frozen-model-resume",
            {"run_id": owner["run_id"], "request_id": request_id},
        )
        if frozen["state"] != "NOT_APPLIED" or len(frozen["attempts"]) != 3:
            raise ForestError(
                "AGENT_MODEL_FAILURE_BOUNDARY_CLOSED",
                "Model attempts are not all known to be unsent", 409,
            )
        checkpoint_id = stable("checkpoint-model-exhausted", request_id)
        checkpoint = await self.ctx.payloads.put_json(
            {
                "phase": "FAILED",
                "run_id": owner["run_id"],
                "request_id": request_id,
                "attempt_id": attempt_id,
                "attempt_count": _MAX_TRANSPORT_ATTEMPTS,
                "error_code": "AGENT_MODEL_TRANSPORT_FAILED",
            },
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            parents=(self._preset_ref(),),
        )
        await self.ctx.uow.call(
            "agent.fail-exhausted-model",
            {
                "effect": {
                    "protocol": "forest.effect-unsent-proof-request/1",
                    "attempts": [{
                        "effect_operation_id": item["effect_operation_id"],
                        "attempt_id": item["attempt_id"],
                    } for item in frozen["attempts"]],
                },
                "owner": owner,
                "request_id": request_id,
                "attempt_id": attempt_id,
                "checkpoint_id": checkpoint_id,
                "checkpoint_state": checkpoint,
            },
            operation_id=self.ctx.operations.stable(f"model-exhausted:{request_id}"),
            timeout=15,
        )
        run = await self._run(owner["run_id"])
        return self._result(
            request, "FAILED", run, error_code="AGENT_MODEL_TRANSPORT_FAILED"
        )

    async def _fail_unsendable_input(self, request, owner, run, source_code):
        """Settle a frozen input that cannot be sent before preparing an effect."""
        revision = run["progress"]["input_revision"]
        step_seq = run["progress"]["next_step_seq"]
        terminal_code = (
            "AGENT_MODEL_INPUT_BYTES_EXCEEDED"
            if source_code == "MODEL_INPUT_BYTES_EXCEEDED"
            else "AGENT_CONTEXT_REQUIRED_OVER_BUDGET"
        )
        checkpoint_id = stable(
            "checkpoint-unsendable-input", run["run_id"], revision, step_seq,
            source_code,
        )
        checkpoint = await self.ctx.payloads.put_json(
            {
                "phase": "FAILED",
                "run_id": run["run_id"],
                "input_revision": revision,
                "next_step_seq": step_seq,
                "source_error_code": source_code,
                "error_code": terminal_code,
            },
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            parents=(self._preset_ref(),),
        )
        await self.ctx.uow.call(
            "agent.fail-unsendable-input",
            {
                "owner": owner,
                "input_revision": revision,
                "step_seq": step_seq,
                "source_error_code": source_code,
                "checkpoint_id": checkpoint_id,
                "checkpoint_state": checkpoint,
            },
            operation_id=self.ctx.operations.stable(
                f"unsendable-input:{checkpoint_id}"
            ),
            timeout=15,
        )
        failed = await self._run(run["run_id"])
        return self._result(
            request, "FAILED", failed,
            error_code=terminal_code,
        )

    async def _block_frozen_model(self, request, owner, reason_code):
        run = await self._run(owner["run_id"])
        recovery = await self._recovery(owner["run_id"])
        head = recovery.get("model_request")
        if (
            run["state"] != "PREPARING_MODEL"
            or not isinstance(head, dict)
            or head["state"] not in {"READY", "NOT_APPLIED"}
            or (
                run["current"]["model_request_id"] != head["request_id"]
                and not (
                    head["state"] == "READY"
                    and run["current"]["model_request_id"] is None
                )
            )
        ):
            return None
        checkpoint_id = stable(
            "checkpoint-frozen-block", run["run_id"], head["request_id"],
            reason_code, owner["epoch"], self.ctx.identity["graph_id"],
        )
        state = await self.ctx.payloads.put_json(
            {
                "phase": "BLOCKED_UNKNOWN",
                "run_id": run["run_id"],
                "request_id": head["request_id"],
                "reason_code": reason_code,
                "graph_id": self.ctx.identity["graph_id"],
                "graph_revision": self.ctx.identity["graph_revision"],
            },
            owner_kind="checkpoint.state", owner_id=checkpoint_id,
            parents=(self._preset_ref(),),
        )
        blocked = await self.ctx.uow.call(
            "agent.block-frozen-model",
            {
                "owner": owner,
                "request_id": head["request_id"],
                "reason_code": reason_code,
                "checkpoint_id": checkpoint_id,
                "state": state,
            },
            operation_id=stable("block-frozen-model", checkpoint_id),
            timeout=15,
        )
        return self._result(
            request, "BLOCKED_UNKNOWN", blocked, error_code=reason_code
        )

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
                    "session_ref": request["session_ref"],
                    "run_ref": request["run_ref"],
                    "next_run_ref": None,
                },
                operation_id=self.ctx.operations.stable(f"prefill:{request_id}"),
                timeout=35,
            )
            if selected["status"] == "CONTINUE":
                return True
            if selected["status"] != "BATCH" or selected["kind"] not in {
                "CONTEXT_PREFILL", "RECOVERY_PREFILL"
            }:
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
        return {
            "protocol": "forest.agent-execution/2",
            "status": status,
            "session_ref": request["session_ref"],
            "run_ref": request["run_ref"],
            "reply": None if run is None else AgentExecution._reply(run),
            "approval_ref": values.get("approval_ref"),
            "error_code": values.get("error_code"),
        }

    async def _finish_cancel(
        self, request: dict, owner: dict, run: dict, recovery: dict
    ) -> dict:
        intent = recovery["cancel_request"]
        model = recovery.get("model_request")
        attempt = recovery.get("model_attempt")
        tool = recovery.get("tool_call")
        prepared = None
        if (
            isinstance(model, dict)
            and model["state"] == "ATTEMPT_PREPARED"
            and isinstance(attempt, dict)
            and attempt["state"] == "PREPARED"
        ):
            prepared = {
                "kind": "MODEL",
                "effect_operation_id": attempt["effect_operation_ref"],
                "attempt_id": attempt["attempt_id"],
            }
        elif (
            isinstance(tool, dict)
            and tool["state"] in {"PROPOSED", "WAIT_APPROVAL"}
            and tool["effect_attempt_id"] is not None
        ):
            prepared = {
                "kind": "TOOL",
                "effect_operation_id": tool["effect_operation_ref"],
                "attempt_id": tool["effect_attempt_id"],
            }
        checkpoint_id = stable(
            "checkpoint-cancel", run["run_id"], intent["operation_id"],
            run["progress"]["checkpoint_seq"] + 1,
            run["progress"]["input_revision"],
            run["progress"]["next_step_seq"],
        )
        state = await self.ctx.payloads.put_json(
            {
                "phase": "CANCELLED",
                "run_id": run["run_id"],
                "request_id": intent["operation_id"],
                "reason_code": "AGENT_CANCELLED_BY_ADMIN",
                "input_revision": run["progress"]["input_revision"],
                "next_step_seq": run["progress"]["next_step_seq"],
            },
            owner_kind="checkpoint.state",
            owner_id=checkpoint_id,
            parents=(self._preset_ref(),),
        )
        agent = {
            "owner": owner,
            "request_id": intent["operation_id"],
            "checkpoint_id": checkpoint_id,
            "state": state,
            "prepared": prepared,
        }
        plan = "agent.cancel-run"
        payload = agent
        if prepared is not None:
            prepared["abandonment_operation_id"] = stable(
                "abandon-cancel", intent["operation_id"], prepared["attempt_id"]
            )
            plan = "agent.cancel-prepared"
            payload = {
                "effect": {
                    "protocol": "forest.effect-abandon/1",
                    "effect_operation_id": prepared["effect_operation_id"],
                    "attempt_id": prepared["attempt_id"],
                    "abandonment_operation_id": prepared["abandonment_operation_id"],
                    "reason_code": "AGENT_CANCELLED_BY_ADMIN",
                    "cancel_effect": True,
                },
                "agent": agent,
            }
        try:
            cancelled = await self.ctx.uow.call(
                plan, payload,
                operation_id=stable("cancel-run", intent["operation_id"], checkpoint_id),
                timeout=15,
            )
        except ForestError as exc:
            if exc.code not in {"AGENT_CANCEL_EFFECT_PENDING", "EFFECT_NOT_ABANDONABLE"}:
                raise
            return self._result(
                request, "BLOCKED_UNKNOWN", run,
                error_code="AGENT_CANCEL_EFFECT_PENDING",
            )
        return self._result(
            request, "CANCELLED", cancelled,
            error_code="AGENT_CANCELLED_BY_ADMIN",
        )

    async def _model_snapshot(self, request: dict, run: dict, step_seq: int) -> dict:
        envelope = await self.ctx.services.call(
            "conversation.context.snapshot",
            {"actor": request["actor"]},
            operation_id=stable(
                "context-snapshot", run["run_id"],
                run["progress"]["input_revision"], step_seq,
            ),
            contract_major=4,
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
            "trigger_claims": claim_summaries(run["inputs"]),
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
        try:
            owner_record = await self.ctx.uow.call(
                "agent.acquire-run-lease",
                {
                    "run_id": run["run_id"],
                    "owner_id": self.ctx.operations.stable(f"run-owner:{run['run_id']}"),
                    "lease_seconds": _RUN_OWNER_LEASE_SECONDS,
                },
                operation_id=self.ctx.operations.stable(f"acquire-run:{run['run_id']}"),
                timeout=15,
            )
        except ForestError as exc:
            if exc.code != "AGENT_RUN_BUSY":
                raise
            return self._result(request, "WAIT_OWNER", run)
        owner = self._owner(owner_record)
        heartbeat = None
        try:
            execution = asyncio.current_task()
            if execution is None:
                raise ForestError(
                    "AGENT_EXECUTION_TASK_MISSING",
                    "Agent execution has no supervised task", 500,
                )
            heartbeat = self.ctx.tasks.spawn_owned(
                self._renew_owner_until_done(owner, execution),
                name=f"agent-run-owner:{run['run_id']}",
            )
            directory = tool_directory(
                await self.ctx.agent_tools.list(), identity=dict(self.ctx.identity)
            )
            declarations, bindings = directory
            preset_context = await self._preset_context()
            while True:
                recovery = await self._recovery(run["run_id"])
                state = recovery["state"]
                run = await self._run(run["run_id"])
                if state in {"COMPLETED", "FAILED", "CANCELLED"}:
                    return self._result(
                        request, state, run, error_code=run.get("error_code")
                    )
                model_head = recovery.get("model_request")
                model_attempt = recovery.get("model_attempt")
                if (
                    state in {"MODEL_PENDING", "BLOCKED_UNKNOWN"}
                    and isinstance(model_head, dict)
                    and isinstance(model_attempt, dict)
                    and model_attempt["state"] in {"DISPATCHING", "UNKNOWN"}
                    and await self.model.reconcile_committed(
                        owner=owner,
                        request_id=model_head["request_id"],
                        attempt_id=model_attempt["attempt_id"],
                        effect_operation_id=model_attempt["effect_operation_ref"],
                    )
                ):
                    continue
                if recovery.get("cancel_request") is not None:
                    tool = recovery.get("tool_call")
                    model = recovery.get("model_request")
                    attempt = recovery.get("model_attempt")
                    if (
                        isinstance(model, dict)
                        and model["state"] in {"DISPATCHING", "UNKNOWN"}
                    ) or (
                        isinstance(attempt, dict)
                        and attempt["state"] in {"DISPATCHING", "UNKNOWN"}
                    ) or (
                        isinstance(tool, dict)
                        and tool["state"] in {"DISPATCHING", "UNKNOWN"}
                    ):
                        if not (
                            isinstance(tool, dict)
                            and tool["state"] in {"DISPATCHING", "UNKNOWN"}
                        ):
                            return self._result(
                                request, "BLOCKED_UNKNOWN", run,
                                error_code="AGENT_CANCEL_EFFECT_PENDING",
                            )
                    else:
                        return await self._finish_cancel(request, owner, run, recovery)
                if datetime.now(UTC) >= deadline:
                    raise ForestError(
                        "AGENT_DEADLINE_EXCEEDED", "Agent execution deadline expired", 409
                    )
                reload_operation = self.reload_operation()
                if reload_operation is not None and state in {
                    "PREPARING_MODEL", "REPAIR_READY", "WAIT_TOOL",
                    "WAIT_APPROVAL", "SAFE_BOUNDARY",
                }:
                    return await self._park_reload(
                        request, owner, run, state, reload_operation
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
                    try:
                        resolved = await self.tools.invoke(
                            owner=owner, tool=tool, binding=binding
                        )
                    except ForestError as exc:
                        if exc.code != "AGENT_CANCEL_REQUESTED":
                            raise
                        continue
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
                    try:
                        invoked_tool = await self.tools.invoke(
                            owner=owner, tool=tool, binding=binding
                        )
                    except ForestError as exc:
                        if exc.code != "AGENT_CANCEL_REQUESTED":
                            raise
                        continue
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
                    try:
                        reconciled = await self.tools.invoke(
                            owner=owner, tool=tool, binding=binding
                        )
                    except ForestError as exc:
                        if exc.code != "AGENT_CANCEL_REQUESTED":
                            raise
                        continue
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
                        error_code=run.get("error_code") or "AGENT_MODEL_RESULT_UNKNOWN",
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
                prepared_request = recovery.get("model_request")
                prepared_attempt = recovery.get("model_attempt")
                if (
                    state == "PREPARING_MODEL"
                    and isinstance(prepared_request, dict)
                    and prepared_request["state"] == "ATTEMPT_PREPARED"
                ):
                    if (
                        not isinstance(prepared_attempt, dict)
                        or prepared_attempt["state"] != "PREPARED"
                    ):
                        return self._result(
                            request, "BLOCKED_UNKNOWN", run,
                            error_code="AGENT_PREPARED_MODEL_INCONSISTENT",
                        )
                    try:
                        await self.model.recover_prepared(
                            owner=owner,
                            request_id=prepared_request["request_id"],
                            attempt_id=prepared_attempt["attempt_id"],
                            effect_operation_id=prepared_attempt["effect_operation_ref"],
                        )
                    except ForestError as exc:
                        if exc.code == "AGENT_CANCEL_REQUESTED":
                            continue
                        if exc.code in {
                            "MODEL_REQUEST_PROVIDER_REBIND_REQUIRED",
                            "PROVIDER_PROFILE_FENCE_MISMATCH",
                            "AGENT_FROZEN_MODEL_INVALID",
                            "AGENT_FROZEN_RESUME_UNAVAILABLE",
                            "EFFECT_NOT_ABANDONABLE",
                        }:
                            return self._result(
                                request, "BLOCKED_UNKNOWN", run,
                                error_code=exc.code,
                            )
                        raise
                    continue
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
                stored_request = recovery.get("model_request")
                request_id = (
                    stored_request["request_id"]
                    if isinstance(stored_request, dict)
                    else stable("model-request", run["run_id"], step_seq)
                )
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
                            return await self._fail_exhausted_model(
                                request, owner, stored_request,
                                recovery.get("model_attempt"),
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
                frozen_resume = (
                    isinstance(stored_request, dict)
                    and stored_request["state"] in {"READY", "NOT_APPLIED"}
                )
                is_repair = is_repair or (
                    frozen_resume and stored_request["request_kind"] == "REPAIR"
                )
                repair = None
                repair_of = None
                memory_query_id = None
                evidence_ref = None
                if frozen_resume:
                    if is_repair:
                        _, repair_of = await self.turns.repair_view(recovery)
                elif is_repair:
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
                    try:
                        snapshot = await self._model_snapshot(request, run, step_seq)
                    except ForestError as exc:
                        if exc.code not in {
                            "CONTEXT_VIEW_HARD_LIMIT",
                            "CONTEXT_VIEW_MEDIA_OVER_BUDGET",
                            "CONTEXT_VIEW_DECODED_OVER_BUDGET",
                        }:
                            raise
                        return await self._fail_unsendable_input(
                            request, owner, run, exc.code
                        )
                    source_refs = [self._preset_ref()]
                # Recall and Context projection can take time. Recheck just
                # before dispatch so a replacement signalled during either
                # operation does not start another model effect.
                reload_operation = self.reload_operation()
                if reload_operation is not None:
                    latest = await self._recovery(run["run_id"])
                    if latest["state"] in {
                        "PREPARING_MODEL", "REPAIR_READY", "WAIT_TOOL",
                        "WAIT_APPROVAL", "SAFE_BOUNDARY",
                    }:
                        return await self._park_reload(
                            request, owner, await self._run(run["run_id"]),
                            latest["state"], reload_operation,
                        )
                if frozen_resume:
                    try:
                        invoked = await self.model.resume_frozen(
                            owner=owner,
                            request_id=request_id,
                            attempt_id=attempt_id,
                            effect_operation_id=stable("model-effect", attempt_id),
                        )
                    except ForestError as exc:
                        if exc.code == "AGENT_CANCEL_REQUESTED":
                            continue
                        reason_code = {
                            "MODEL_REQUEST_PROVIDER_REBIND_REQUIRED":
                                "AGENT_FROZEN_PROVIDER_INCOMPATIBLE",
                            "PROVIDER_PROFILE_FENCE_MISMATCH":
                                "AGENT_FROZEN_PROVIDER_INCOMPATIBLE",
                            "AGENT_FROZEN_MODEL_INVALID":
                                "AGENT_FROZEN_PAYLOAD_INVALID",
                        }.get(exc.code)
                        if reason_code is None:
                            raise
                        blocked = await self._block_frozen_model(
                            request, owner, reason_code
                        )
                        if blocked is None:
                            raise
                        return blocked
                else:
                    try:
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
                    except ForestError as exc:
                        if exc.code == "MODEL_INPUT_BYTES_EXCEEDED":
                            return await self._fail_unsendable_input(
                                request, owner, run, exc.code
                            )
                        if exc.code != "AGENT_CANCEL_REQUESTED":
                            raise
                        continue
                if invoked["status"] == "UNKNOWN":
                    return self._result(
                        request,
                        "BLOCKED_UNKNOWN",
                        run,
                        error_code=invoked["error_code"],
                    )
                if invoked["status"] == "NOT_SENT":
                    continue
                request_id = invoked["request_id"]
                attempt_id = invoked["attempt_id"]
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
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
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
