"""Memory-owned coordinator for one durable multimodal embedding effect."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, timedelta, timezone

from forest.embeddings import EmbeddingCancelled, EmbeddingResult, EmbeddingVector
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payload_authority import PayloadSource


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _stable(prefix: str, *parts: str) -> str:
    return prefix + ":" + hashlib.sha256("\0".join(parts).encode()).hexdigest()


def _result(value: dict) -> EmbeddingResult:
    try:
        vectors = tuple(
            EmbeddingVector(item["facet_id"], item["index"], tuple(item["values"]))
            for item in value["vectors"]
        )
        return EmbeddingResult(
            value["status"],
            value["request_id"],
            value["transport_attempt_id"],
            value["payload_digest"],
            value["space_id"],
            vectors,
            (
                json.dumps(value["usage"], separators=(",", ":"))
                if value["usage"] is not None
                else None
            ),
            value["error_code"],
            value["http_status"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ForestError(
            "EMBEDDING_RESULT_INVALID", "Embedding Provider returned an invalid result", 502
        ) from exc


class KernelEmbeddingEffectExecutionPort:
    """Atomically hand one Memory batch and its external effect to a Provider."""

    def __init__(
        self,
        *,
        retrieval,
        effects,
        payloads,
        runtime,
        authorization,
        memory_grants,
        service,
    ) -> None:
        self.retrieval = retrieval
        self.effects = effects
        self.payloads = payloads
        self.runtime = runtime
        self.authorization = authorization
        self.memory_grants = memory_grants
        self.service = service

    @staticmethod
    def _sources(inputs) -> tuple[PayloadSource, ...]:
        return tuple(
            PayloadSource(
                source_component_id="memory.graph",
                source_kind="embedding.facet",
                source_id=item.facet_id,
                source_revision=item.source.get("source_revision", 1),
                part_id=item.source.get("part_id", "*"),
                source_trunk_id=item.source.get("trunk_id"),
                observation_conversation_id=item.source.get("conversation_ref"),
                author_subject_id=item.source.get("author_ref"),
            )
            for item in inputs
        )

    async def _payload(self, kind, owner_id, content, *, sources=()):
        epoch = await self.payloads.epoch()
        return await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind=kind,
            owner_id=owner_id,
            media_type="application/json",
            content=content,
            sources=sources,
            expected_deletion_epoch=epoch,
        )

    async def dispatch(
        self,
        prepared,
        *,
        inputs,
        provider_component_id,
        provider_generation_id,
        profile_ref,
        producer_generation_id,
        permits,
        handoff_operation_id,
    ) -> EmbeddingResult:
        effect_id = _stable("effect:embedding", prepared.request_id)
        previous = await self.authorization.tickets_for_operation(effect_id)
        ticket_id = _stable("ticket:embedding", effect_id, str(len(previous) + 1))
        input_digest = digest_json(
            {
                "profile_ref": profile_ref,
                "payload_digest": prepared.payload_digest,
                "space_id": prepared.space_id,
            }
        )
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        await self.authorization.issue_ticket(
            ticket_id=ticket_id,
            issuer_kind="memory.policy",
            issuer_id="memory.graph/embedding",
            subject_kind="component",
            subject_id="memory.graph",
            action="embedding.invoke",
            resource_kind="bot",
            resource_id=self.effects.store.bot_id,
            operation_id=effect_id,
            input_digest=input_digest,
            plan_digest=prepared.space_id,
            grant_set=self.memory_grants,
            expires_at=expires_at,
            audience_component_id=provider_component_id,
            audience_generation_id=provider_generation_id,
        )
        dispatch = {
            "protocol": "forest.embedding-dispatch/1",
            "effect_operation_id": effect_id,
            "authorization_ticket_id": ticket_id,
            "component_id": provider_component_id,
            "generation_id": provider_generation_id,
            "profile_ref": profile_ref,
            "prepared": {
                "request_id": prepared.request_id,
                "transport_attempt_id": prepared.transport_attempt_id,
                "body_base64": base64.b64encode(prepared.body).decode("ascii"),
                "manifest_json": prepared.manifest_json,
                "payload_digest": prepared.payload_digest,
                "space_id": prepared.space_id,
            },
        }
        wire = _canonical(dispatch)
        sources = self._sources(inputs)
        input_payload = await self._payload("effect.input", effect_id, wire, sources=sources)
        wire_payload = await self._payload(
            "effect.wire", prepared.transport_attempt_id, wire, sources=sources
        )
        await self.effects.admit(
            effect_operation_id=effect_id,
            input_digest=input_digest,
            effect_kind="embedding.multimodal.dispatch",
            target_kind="component.service",
            target_id=f"{provider_component_id}/embedding.encode",
            producer_component_id="memory.graph",
            producer_generation_id=producer_generation_id,
            root_operation_kind="memory.embedding",
            root_operation_id=prepared.request_id,
            input_payload_id=input_payload.payload_id,
            input_payload_digest=input_payload.content_digest,
        )
        graph = await self.runtime.verify_generation_member(
            component_id=provider_component_id,
            generation_id=provider_generation_id,
        )
        async with self.effects.store.pool.connection() as conn, conn.transaction():
            permit = await self.effects.permit_in(
                conn,
                effect_operation_id=effect_id,
                attempt_id=prepared.transport_attempt_id,
                permit_operation_id=effect_id,
                wire_payload_id=wire_payload.payload_id,
                wire_payload_digest=wire_payload.content_digest,
                authorization_ticket_id=ticket_id,
                authorization_action="embedding.invoke",
                authorization_resource_kind="bot",
                authorization_resource_id=self.effects.store.bot_id,
                authorization_plan_digest=prepared.space_id,
                executor_component_id=provider_component_id,
                executor_generation_id=provider_generation_id,
                fence_kind="runtime.graph",
                fence_id=graph.graph_id,
                fence_epoch=graph.graph_revision,
                deadline_at=expires_at,
            )
            await self.effects.mark_handoff_in(
                conn,
                permit=permit,
                handoff_operation_id=_stable(
                    "effect-handoff:embedding", prepared.transport_attempt_id
                ),
            )
            await self.retrieval.mark_batch_handoff(
                permits,
                batch_operation_id=handoff_operation_id,
                _connection=conn,
            )
        cancelled = False
        try:
            value = await self.service.invoke(
                dispatch,
                operation_id=effect_id,
                causal_root_kind="memory.embedding",
                causal_root_id=prepared.request_id,
                timeout_seconds=120,
            )
            result = _result(value)
        except EmbeddingCancelled as exc:
            result, cancelled = exc.result, True
        except Exception:
            result = EmbeddingResult(
                "UNKNOWN",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                prepared.space_id,
                error_code="EMBEDDING_DISPATCH_RESULT_UNKNOWN",
            )
        if (
            result.request_id != prepared.request_id
            or result.transport_attempt_id != prepared.transport_attempt_id
            or result.payload_digest != prepared.payload_digest
            or result.space_id != prepared.space_id
        ):
            result = EmbeddingResult(
                "UNKNOWN",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                prepared.space_id,
                error_code="EMBEDDING_RESULT_FENCE_MISMATCH",
            )
        observation_id = _stable(
            "effect-observe:embedding", effect_id, prepared.transport_attempt_id
        )
        evidence = await self._payload(
            "effect.evidence", observation_id, _canonical(result.as_dict())
        )
        await self.effects.observe(
            effect_operation_id=effect_id,
            attempt_id=prepared.transport_attempt_id,
            observation_operation_id=observation_id,
            status={
                "RESPONDED": "SUCCEEDED",
                "NOT_SENT": "NOT_APPLIED",
                "REJECTED": "SUCCEEDED",
                "UNKNOWN": "UNKNOWN",
            }[result.status],
            evidence_kind="executor",
            evidence_payload_id=evidence.payload_id,
            evidence_payload_digest=evidence.content_digest,
        )
        if cancelled:
            raise EmbeddingCancelled(result)
        return result
