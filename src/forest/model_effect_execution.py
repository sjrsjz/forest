"""Kernel-governed handoff from one Agent model attempt to one Provider Generation."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.models import ModelResult
from forest.packages import digest_json
from forest.storage.agent_runtime_authority import AgentPayloadRef


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _stable(prefix: str, *parts: str) -> str:
    return prefix + ":" + hashlib.sha256("\0".join(parts).encode()).hexdigest()


def _result(value: dict) -> ModelResult:
    try:
        return ModelResult(
            status=value["status"],
            request_id=value["request_id"],
            transport_attempt_id=value["transport_attempt_id"],
            payload_digest=value["payload_digest"],
            text=value["text"],
            tool_calls_json=json.dumps(value["tool_calls"], separators=(",", ":")),
            usage_json=(
                json.dumps(value["usage"], separators=(",", ":"))
                if value["usage"] is not None
                else None
            ),
            finish_reason=value["finish_reason"],
            provider_response_id=value["provider_response_id"],
            provider_model=value["provider_model"],
            error_code=value["error_code"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ForestError(
            "PROVIDER_RESULT_INVALID", "Provider returned an invalid model result", 502
        ) from exc


class KernelModelEffectExecutionPort:
    """Commit Effect and Agent handoff in one transaction before Provider I/O."""

    def __init__(
        self,
        *,
        agent,
        effects,
        payloads,
        runtime,
        authorization,
        agent_grants,
        service,
    ) -> None:
        self.agent = agent
        self.effects = effects
        self.payloads = payloads
        self.runtime = runtime
        self.authorization = authorization
        self.agent_grants = agent_grants
        self.service = service

    async def _payload(self, kind: str, owner_id: str, content: bytes):
        epoch = await self.payloads.epoch()
        return await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind=kind,
            owner_id=owner_id,
            media_type="application/json",
            content=content,
            expected_deletion_epoch=epoch,
        )

    async def dispatch(self, bound, *, owner, request_id: str, attempt_id: str, effect_id: str):
        prepared = bound.prepared
        input_digest = digest_json(
            {
                "profile_ref": bound.provider_profile_ref,
                "capability_digest": bound.capability_digest,
                "payload_digest": prepared.payload_digest,
            }
        )
        previous_tickets = await self.authorization.tickets_for_operation(effect_id)
        ticket_id = _stable(
            "ticket:model", effect_id, attempt_id, str(len(previous_tickets) + 1)
        )
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        await self.authorization.issue_ticket(
            ticket_id=ticket_id,
            issuer_kind="agent.policy",
            issuer_id="agent.runtime/model",
            subject_kind="component",
            subject_id="agent.runtime",
            action="provider.invoke",
            resource_kind="bot",
            resource_id=self.effects.store.bot_id,
            operation_id=effect_id,
            input_digest=input_digest,
            plan_digest=bound.capability_digest,
            grant_set=self.agent_grants,
            expires_at=expires_at,
            audience_component_id=bound.component_id,
            audience_generation_id=bound.generation_id,
        )
        dispatch = {
            "protocol": "forest.provider-dispatch/1",
            "effect_operation_id": effect_id,
            "authorization_ticket_id": ticket_id,
            "component_id": bound.component_id,
            "generation_id": bound.generation_id,
            "profile_ref": bound.provider_profile_ref,
            "capability_digest": bound.capability_digest,
            "prepared": {
                "request_id": prepared.request_id,
                "transport_attempt_id": prepared.transport_attempt_id,
                "projection": bound.projection.as_dict(),
                "wire": bound.wire.as_dict(),
                "payload_digest": prepared.payload_digest,
                "max_output_tokens": prepared.max_output_tokens,
                "stream": prepared.stream,
                "serializer_revision": prepared.serializer_revision,
            },
        }
        wire = _canonical(dispatch)
        input_payload = await self._payload("effect.input", effect_id, wire)
        wire_payload = await self._payload("effect.wire", attempt_id, wire)
        graph = await self.runtime.verify_generation_member(
            component_id=bound.component_id,
            generation_id=bound.generation_id,
        )
        async with self.effects.store.pool.connection() as conn, conn.transaction():
            await self.effects.admit_in(
                conn,
                effect_operation_id=effect_id,
                input_digest=input_digest,
                effect_kind="provider.model.dispatch",
                target_kind="component.service",
                target_id=f"{bound.component_id}/provider.model.dispatch",
                producer_component_id="agent.runtime",
                producer_generation_id=self.agent.generation_identity.generation_id,
                root_operation_kind="agent.model",
                root_operation_id=request_id,
                input_payload_id=input_payload.payload_id,
                input_payload_digest=input_payload.content_digest,
            )
            permit = await self.effects.permit_in(
                conn,
                effect_operation_id=effect_id,
                attempt_id=attempt_id,
                permit_operation_id=effect_id,
                wire_payload_id=wire_payload.payload_id,
                wire_payload_digest=wire_payload.content_digest,
                authorization_ticket_id=ticket_id,
                authorization_action="provider.invoke",
                authorization_resource_kind="bot",
                authorization_resource_id=self.effects.store.bot_id,
                authorization_plan_digest=bound.capability_digest,
                executor_component_id=bound.component_id,
                executor_generation_id=bound.generation_id,
                fence_kind="runtime.graph",
                fence_id=graph.graph_id,
                fence_epoch=graph.graph_revision,
                deadline_at=expires_at,
            )
            await self.agent.prepare_model_attempt_in(
                conn,
                owner,
                request_id=request_id,
                attempt_id=attempt_id,
                effect_operation_ref=effect_id,
                wire=AgentPayloadRef(
                    wire_payload.payload_id,
                    wire_payload.content_digest,
                    "kernel.effect",
                    "effect.wire",
                    attempt_id,
                ),
            )
            await self.effects.mark_handoff_in(
                conn,
                permit=permit,
                handoff_operation_id=_stable("handoff:model", effect_id, attempt_id),
            )
            await self.agent.mark_model_handoff_in(
                conn,
                owner,
                request_id=request_id,
                attempt_id=attempt_id,
            )
        try:
            value = await self.service.invoke(
                dispatch,
                operation_id=effect_id,
                causal_root_kind="agent.model",
                causal_root_id=request_id,
                timeout_seconds=120,
            )
            result = _result(value)
            if (
                result.request_id != prepared.request_id
                or result.transport_attempt_id != prepared.transport_attempt_id
                or result.payload_digest != prepared.payload_digest
            ):
                raise ForestError(
                    "PROVIDER_RESULT_FENCE_MISMATCH",
                    "Provider result belongs to another prepared request",
                    502,
                )
        except Exception:
            result = ModelResult(
                "UNKNOWN",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                error_code="PROVIDER_DISPATCH_RESULT_UNKNOWN",
            )
        observation_id = _stable("observe:model", effect_id, attempt_id)
        evidence = await self._payload(
            "effect.evidence", observation_id, _canonical(result.as_dict())
        )
        status = {
            "RESPONDED": "SUCCEEDED",
            "NOT_SENT": "NOT_APPLIED",
            "UNKNOWN": "UNKNOWN",
        }[result.status]
        await self.effects.observe(
            effect_operation_id=effect_id,
            attempt_id=attempt_id,
            observation_operation_id=observation_id,
            status=status,
            evidence_kind="executor",
            evidence_payload_id=evidence.payload_id,
            evidence_payload_digest=evidence.content_digest,
        )
        return result

    async def query(self, effect_id: str) -> ModelResult:
        loaded = await self.effects.load(effect_id)
        effect, attempt = loaded["effect"], loaded["attempt"]
        if attempt is None or effect["state"] in {"PREPARED", "DISPATCHING"}:
            raise ForestError(
                "PROVIDER_RESULT_UNQUERYABLE",
                "Provider transport has no durable terminal evidence",
                409,
            )
        accepted = [
            item
            for item in loaded["observations"]
            if item["accepted"] and item["resulting_state"] == effect["state"]
        ]
        if not accepted or accepted[-1]["evidence_payload_id"] is None:
            raise ForestError(
                "PROVIDER_EFFECT_EVIDENCE_MISSING",
                "Settled Provider effect has no durable result",
                500,
            )
        try:
            value = json.loads(
                await self.payloads.read_bytes(accepted[-1]["evidence_payload_id"])
            )
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "PROVIDER_EFFECT_EVIDENCE_CORRUPT",
                "Provider effect evidence is invalid",
                500,
            ) from exc
        if not isinstance(value, dict):
            raise ForestError(
                "PROVIDER_EFFECT_EVIDENCE_CORRUPT",
                "Provider effect evidence is invalid",
                500,
            )
        result = _result(value)
        expected = {
            "SUCCEEDED": "RESPONDED",
            "NOT_APPLIED": "NOT_SENT",
            "UNKNOWN": "UNKNOWN",
        }.get(effect["state"])
        if result.status != expected:
            raise ForestError(
                "PROVIDER_EFFECT_EVIDENCE_CONFLICT",
                "Provider result conflicts with Effect terminal state",
                500,
            )
        return result
