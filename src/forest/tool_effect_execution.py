"""Generation-fenced Effect Gateway adapter for one Agent tool binding."""

from __future__ import annotations

import hashlib
import json
from typing import Protocol

from forest.agent_tool_executor import (
    ToolAuthorizationRequest,
    ToolDispatchRequest,
    ToolDispatchResult,
)
from forest.errors import ForestError
from forest.storage.agent_runtime_authority import AgentPayloadRef


class ToolServicePort(Protocol):
    async def dispatch(self, request: ToolAuthorizationRequest) -> ToolDispatchResult: ...

    async def query(self, request: ToolAuthorizationRequest) -> ToolDispatchResult: ...


def _stable_id(prefix: str, value: str) -> str:
    return f"{prefix}:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


class KernelToolExecutionPort:
    """Commit Effect handoff before calling a tool and recover its persisted result."""

    def __init__(
        self,
        *,
        effects,
        payloads,
        runtime,
        authorization,
        service: ToolServicePort,
    ) -> None:
        self.effects = effects
        self.payloads = payloads
        self.runtime = runtime
        self.authorization = authorization
        self.service = service

    @staticmethod
    def _wire(request: ToolAuthorizationRequest) -> bytes:
        return json.dumps(
            {
                "protocol": "forest.agent-tool-dispatch/1",
                "tool_call_id": request.tool_call_id,
                "effect_operation_ref": request.effect_operation_ref,
                "producer_component_id": request.producer_component_id,
                "producer_generation_id": request.producer_generation_id,
                "executor_component_id": request.executor_component_id,
                "executor_generation_id": request.executor_generation_id,
                "service_id": request.service_id,
                "binding_digest": request.binding_digest,
                "authority_trigger_ref": request.authority_trigger_ref,
                "authority_principal_ref": request.authority_principal_ref,
                "arguments": request.arguments,
                "input_digest": request.input_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

    @staticmethod
    def _evidence(result: ToolDispatchResult) -> bytes:
        payload = result.result
        return json.dumps(
            {
                "protocol": "forest.agent-tool-evidence/1",
                "status": result.status,
                "result": (
                    {
                        "payload_id": payload.payload_id,
                        "payload_digest": payload.digest,
                        "owner_component_id": payload.owner_component_id,
                        "owner_kind": payload.owner_kind,
                        "owner_id": payload.owner_id,
                    }
                    if payload is not None
                    else None
                ),
                "error_code": result.error_code,
                "observation_ref": result.observation_ref,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    @staticmethod
    def _result(content: bytes) -> ToolDispatchResult:
        try:
            value = json.loads(content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError("TOOL_EFFECT_EVIDENCE_CORRUPT", "Tool evidence is invalid", 500) from exc
        if not isinstance(value, dict) or value.get("protocol") != "forest.agent-tool-evidence/1":
            raise ForestError("TOOL_EFFECT_EVIDENCE_CORRUPT", "Tool evidence is invalid", 500)
        raw = value.get("result")
        payload = None
        if raw is not None:
            if not isinstance(raw, dict):
                raise ForestError(
                    "TOOL_EFFECT_EVIDENCE_CORRUPT", "Tool result evidence is invalid", 500
                )
            try:
                payload = AgentPayloadRef(
                    raw["payload_id"],
                    raw["payload_digest"],
                    raw["owner_component_id"],
                    raw["owner_kind"],
                    raw["owner_id"],
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ForestError(
                    "TOOL_EFFECT_EVIDENCE_CORRUPT", "Tool result evidence is invalid", 500
                ) from exc
        try:
            return ToolDispatchResult(
                value.get("status"),
                result=payload,
                error_code=value.get("error_code"),
                observation_ref=value.get("observation_ref"),
            )
        except (TypeError, ValueError) as exc:
            raise ForestError(
                "TOOL_EFFECT_EVIDENCE_CORRUPT", "Tool result evidence is invalid", 500
            ) from exc

    async def _settle(
        self,
        request: ToolAuthorizationRequest,
        *,
        attempt_id: str,
        phase: str,
        result: ToolDispatchResult,
    ) -> ToolDispatchResult:
        if type(result) is not ToolDispatchResult:
            raise ForestError("TOOL_RESULT_INVALID", "Tool service returned an invalid result", 502)
        operation_id = _stable_id(
            f"effect-observe:{phase}", f"{request.effect_operation_ref}:{result.status}"
        )
        epoch = await self.payloads.epoch()
        evidence = await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind="effect.evidence",
            owner_id=operation_id,
            media_type="application/json",
            content=self._evidence(result),
            expected_deletion_epoch=epoch,
        )
        await self.effects.observe(
            effect_operation_id=request.effect_operation_ref,
            attempt_id=attempt_id,
            observation_operation_id=operation_id,
            status="UNKNOWN" if result.status == "UNKNOWN" else "SUCCEEDED",
            evidence_kind="reconciliation" if phase == "reconcile" else "executor",
            evidence_payload_id=evidence.payload_id,
            evidence_payload_digest=evidence.content_digest,
        )
        return result

    async def dispatch(self, request: ToolDispatchRequest, *, permit_in) -> ToolDispatchResult:
        if type(request) is not ToolDispatchRequest:
            raise ForestError("TOOL_EFFECT_INPUT_INVALID", "Invalid tool dispatch request", 422)
        authorization = request.authorization
        wire = self._wire(authorization)
        attempt_id = _stable_id("effect-attempt:agent-tool", authorization.tool_call_id)
        epoch = await self.payloads.epoch()
        input_payload = await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind="effect.input",
            owner_id=authorization.effect_operation_ref,
            media_type="application/json",
            content=wire,
            expected_deletion_epoch=epoch,
        )
        wire_payload = await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind="effect.wire",
            owner_id=attempt_id,
            media_type="application/json",
            content=wire,
            expected_deletion_epoch=epoch,
        )
        await self.effects.admit(
            effect_operation_id=authorization.effect_operation_ref,
            input_digest=authorization.input_digest,
            effect_kind="tool.dispatch",
            target_kind="component.service",
            target_id=f"{authorization.executor_component_id}/{authorization.service_id}",
            producer_component_id=authorization.producer_component_id,
            producer_generation_id=authorization.producer_generation_id,
            root_operation_kind="agent.tool",
            root_operation_id=authorization.tool_call_id,
            input_payload_id=input_payload.payload_id,
            input_payload_digest=input_payload.content_digest,
        )
        graph = await self.runtime.verify_generation_member(
            component_id=authorization.executor_component_id,
            generation_id=authorization.executor_generation_id,
        )
        ticket = await self.authorization.ticket(request.authorization_ticket_id)
        async with self.effects.store.pool.connection() as conn, conn.transaction():
            permit = await self.effects.permit_in(
                conn,
                effect_operation_id=authorization.effect_operation_ref,
                attempt_id=attempt_id,
                permit_operation_id=authorization.effect_operation_ref,
                wire_payload_id=wire_payload.payload_id,
                wire_payload_digest=wire_payload.content_digest,
                authorization_ticket_id=request.authorization_ticket_id,
                authorization_action="tool.invoke",
                authorization_resource_kind="bot",
                authorization_resource_id=self.effects.store.bot_id,
                authorization_plan_digest=authorization.binding_digest,
                executor_component_id=authorization.executor_component_id,
                executor_generation_id=authorization.executor_generation_id,
                fence_kind="runtime.graph",
                fence_id=graph.graph_id,
                fence_epoch=graph.graph_revision,
                deadline_at=ticket["expires_at"],
            )
            await self.effects.mark_handoff_in(
                conn,
                permit=permit,
                handoff_operation_id=_stable_id(
                    "effect-handoff:agent-tool", authorization.tool_call_id
                ),
            )
            await permit_in(conn, effect_observation_ref=attempt_id)
        try:
            result = await self.service.dispatch(authorization)
        except Exception:
            result = ToolDispatchResult(
                "UNKNOWN",
                error_code="TOOL_DISPATCH_RESULT_UNKNOWN",
                observation_ref=attempt_id,
            )
        return await self._settle(
            authorization,
            attempt_id=attempt_id,
            phase="dispatch",
            result=result,
        )

    async def query(self, request: ToolAuthorizationRequest) -> ToolDispatchResult:
        if type(request) is not ToolAuthorizationRequest:
            raise ForestError("TOOL_EFFECT_INPUT_INVALID", "Invalid tool query request", 422)
        loaded = await self.effects.load(request.effect_operation_ref)
        effect = loaded["effect"]
        attempt = loaded["attempt"]
        if attempt is None:
            raise ForestError("TOOL_EFFECT_NOT_HANDED_OFF", "Tool effect has no attempt", 409)
        if effect["state"] == "SUCCEEDED":
            accepted = [
                item
                for item in loaded["observations"]
                if item["accepted"] and item["resulting_state"] == "SUCCEEDED"
            ]
            if not accepted or accepted[-1]["evidence_payload_id"] is None:
                raise ForestError(
                    "TOOL_EFFECT_EVIDENCE_MISSING", "Settled tool effect has no evidence", 500
                )
            return self._result(
                await self.payloads.read_bytes(accepted[-1]["evidence_payload_id"])
            )
        if effect["state"] not in {"DISPATCHING", "UNKNOWN"}:
            return ToolDispatchResult(
                "FAILED",
                error_code="TOOL_EFFECT_NOT_APPLIED",
                observation_ref=attempt["attempt_id"],
            )
        try:
            result = await self.service.query(request)
        except Exception:
            result = ToolDispatchResult(
                "UNKNOWN",
                error_code="TOOL_QUERY_RESULT_UNKNOWN",
                observation_ref=attempt["attempt_id"],
            )
        if effect["state"] == "DISPATCHING" and result.status == "UNKNOWN":
            phase = "dispatch"
        else:
            phase = "reconcile"
        return await self._settle(
            request,
            attempt_id=attempt["attempt_id"],
            phase=phase,
            result=result,
        )
