"""One-facet VL embedding execution primitive for the clean Memory authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass

from forest.embeddings import (
    EmbeddingCancelled,
    EmbeddingInput,
    EmbeddingProfile,
    prepare_embedding_request,
)
from forest.errors import ForestError
from forest.models import MediaSnapshot
from forest.storage.memory_graph_authority import MemoryPayloadRef
from forest.storage.payload_authority import PayloadSource


def _operation(prefix: str, identity: str) -> str:
    return prefix + hashlib.sha256(identity.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EmbeddingBatchItem:
    facet_id: str
    attempt_id: str
    prepare_operation_id: str


class MemoryEmbeddingWorker:
    """Execute a frozen facet without owning Memory source or visibility facts.

    Scheduling, batching and budgets remain separate runtime responsibilities.
    This primitive establishes the exact network handoff and durable settlement
    semantics used by those higher-level workers.
    """

    def __init__(
        self,
        *,
        retrieval,
        payloads,
        client,
        profile: EmbeddingProfile,
        component_id: str,
        generation_id: str,
        route_revision: str,
    ) -> None:
        if not isinstance(profile, EmbeddingProfile):
            raise ForestError("EMBEDDING_CONFIG_INVALID", "Embedding profile is invalid", 422)
        self.retrieval = retrieval
        self.payloads = payloads
        self.client = client
        self.profile = profile
        self.component_id = component_id
        self.generation_id = generation_id
        self.route_revision = route_revision

    def _input(self, materialized: dict) -> EmbeddingInput:
        source = {
            "bot_id": self.retrieval.store.bot_id,
            "source_unit_id": materialized["source"]["source_unit_id"],
            "source_revision": materialized["source_revision"],
            "source_id": materialized["source"]["source_id"],
            "source_kind": materialized["source"]["source_kind"],
            "realm_id": materialized["source"]["realm_id"],
            "author_ref": materialized["source"]["author_subject_ref"],
            "part_id": materialized["part_id"],
            "payload_id": materialized["input_payload_id"],
            "payload_digest": materialized["input_payload_digest"],
        }
        source_json = json.dumps(source, ensure_ascii=False, separators=(",", ":"))
        if materialized["modality"] == "TEXT":
            return EmbeddingInput(
                materialized["facet_id"],
                text=materialized["text"],
                source_json=source_json,
            )
        image = materialized["image"]
        snapshot = MediaSnapshot(
            bot_id=self.retrieval.store.bot_id,
            media_ref=materialized["input_payload_id"],
            media_revision=1,
            source_unit_id=materialized["source"]["source_unit_id"],
            part_id=materialized["part_id"],
            mime=materialized["media_type"],
            content=image,
            blob_digest=materialized["input_payload_digest"],
            width=materialized["width"],
            height=materialized["height"],
        )
        return EmbeddingInput(
            materialized["facet_id"], image=snapshot, source_json=source_json
        )

    async def execute(
        self,
        *,
        facet_id: str,
        attempt_id: str,
        prepare_operation_id: str,
        expected_deletion_epoch: int,
        deadline_at,
    ) -> dict:
        results = await self.execute_batch(
            items=(EmbeddingBatchItem(facet_id, attempt_id, prepare_operation_id),),
            batch_attempt_id=attempt_id,
            expected_deletion_epoch=expected_deletion_epoch,
            deadline_at=deadline_at,
        )
        return results[0]

    async def execute_batch(
        self,
        *,
        items,
        batch_attempt_id: str,
        expected_deletion_epoch: int,
        deadline_at,
    ) -> tuple[dict, ...]:
        items = tuple(items)
        if (
            not 1 <= len(items) <= 32
            or any(type(item) is not EmbeddingBatchItem for item in items)
            or len({item.facet_id for item in items}) != len(items)
            or len({item.attempt_id for item in items}) != len(items)
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding batch", 422)
        materialized = tuple(
            [
                await self.retrieval.materialize_facet(
                    item.facet_id,
                    expected_deletion_epoch=expected_deletion_epoch,
                )
                for item in items
            ]
        )
        if any(value["space_id"] != self.profile.space_id for value in materialized):
            raise ForestError(
                "EMBEDDING_ROUTE_MISMATCH", "Embedding profile belongs to another Bot space", 409
            )
        purposes = {value["purpose"].lower() for value in materialized}
        if len(purposes) != 1:
            raise ForestError(
                "MEMORY_EMBEDDING_BATCH_MIXED_PURPOSE",
                "Document and query facets need separate requests",
                422,
            )
        prepared = prepare_embedding_request(
            self.profile,
            tuple(self._input(value) for value in materialized),
            request_id="request:" + batch_attempt_id,
            transport_attempt_id=batch_attempt_id,
            purpose=next(iter(purposes)),
        )
        permits = []
        try:
            for item in items:
                permits.append(
                    await self.retrieval.prepare_attempt(
                        operation_id=item.prepare_operation_id,
                        attempt_id=item.attempt_id,
                        facet_id=item.facet_id,
                        executor_component_id=self.component_id,
                        executor_generation_id=self.generation_id,
                        route_revision=self.route_revision,
                        deadline_at=deadline_at,
                    )
                )
        except Exception:
            await self._abort_prepared(tuple(permits), "EMBEDDING_BATCH_PREPARATION_ABORTED")
            raise
        handoff_id = _operation("embedding-batch-handoff:", batch_attempt_id)

        async def permitcheck(actual):
            if actual is not prepared:
                raise ForestError(
                    "EMBEDDING_REQUEST_CHANGED", "Embedding transport changed its request", 409
                )
            await self.retrieval.mark_batch_handoff(
                tuple(permits), batch_operation_id=handoff_id
            )

        try:
            result = await self.client.dispatch(prepared, permitcheck=permitcheck)
        except EmbeddingCancelled as exc:
            await asyncio.shield(
                self._settle_batch(tuple(permits), materialized, exc.result)
            )
            raise
        return await self._settle_batch(tuple(permits), materialized, result)

    async def _abort_prepared(self, permits, error_code):
        for permit in permits:
            await self.retrieval.settle_attempt(
                permit,
                settlement_operation_id=_operation(
                    "embedding-settlement:", permit.attempt_id
                ),
                outcome="NOT_SENT",
                expected_deletion_epoch=permit.accepted_deletion_epoch,
                error_code=error_code,
            )

    async def _settle_batch(self, permits, materialized, result):
        return tuple(
            [
                await self._settle(permit, value, result)
                for permit, value in zip(permits, materialized, strict=True)
            ]
        )

    async def _settle(self, permit, materialized, result) -> dict:
        settlement_id = _operation("embedding-settlement:", permit.attempt_id)
        if result.status == "RESPONDED":
            vectors = [
                vector
                for vector in result.vectors
                if vector.facet_id == materialized["facet_id"]
            ]
            if len(vectors) != 1:
                raise ForestError(
                    "EMBEDDING_RESPONSE_INVALID", "Embedding response facet differs", 502
                )
            body = json.dumps(
                result.as_dict(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            saved = await self.payloads.put_bytes(
                owner_component_id=self.component_id,
                owner_kind="embedding.result",
                owner_id=permit.attempt_id,
                media_type="application/json",
                content=body,
                sources=(
                    PayloadSource(
                        source_component_id="memory.graph",
                        source_kind="embedding.facet",
                        source_id=materialized["facet_id"],
                        source_revision=1,
                        part_id=materialized["part_id"],
                    ),
                ),
                parents=(materialized["input_payload_id"],),
                expected_deletion_epoch=permit.accepted_deletion_epoch,
            )
            return await self.retrieval.settle_attempt(
                permit,
                settlement_operation_id=settlement_id,
                outcome="SUCCEEDED",
                expected_deletion_epoch=permit.accepted_deletion_epoch,
                result=MemoryPayloadRef(
                    saved.payload_id,
                    saved.content_digest,
                    self.component_id,
                    "embedding.result",
                    permit.attempt_id,
                ),
                vector=vectors[0].values,
            )
        outcome = {
            "NOT_SENT": "NOT_SENT",
            "UNKNOWN": "UNKNOWN",
            "REJECTED": "FAILED",
        }[result.status]
        return await self.retrieval.settle_attempt(
            permit,
            settlement_operation_id=settlement_id,
            outcome=outcome,
            expected_deletion_epoch=permit.accepted_deletion_epoch,
            error_code=result.error_code or "EMBEDDING_PROVIDER_FAILURE",
        )
