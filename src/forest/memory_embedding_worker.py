"""One-facet VL embedding execution primitive for the clean Memory authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date

from forest.embeddings import (
    EmbeddingCancelled,
    EmbeddingInput,
    EmbeddingProfile,
)
from forest.errors import ForestError
from forest.models import MediaSnapshot
from forest.storage.memory_graph_authority import MemoryPayloadRef
from forest.storage.memory_retrieval_authority import EmbeddingClaim
from forest.storage.payload_authority import PayloadSource


def _operation(prefix: str, identity: str) -> str:
    return prefix + hashlib.sha256(identity.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EmbeddingBatchItem:
    facet_id: str
    attempt_id: str
    prepare_operation_id: str


@dataclass(frozen=True)
class EmbeddingBudgetClaim:
    reservation_id: str
    operation_id: str
    policy_revision: str
    budget_day: date
    request_limit: int
    token_limit: int
    reserved_tokens: int


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
        provider,
        profile: EmbeddingProfile,
        component_id: str,
        generation_id: str,
        route_revision: str,
        budget=None,
    ) -> None:
        if not isinstance(profile, EmbeddingProfile):
            raise ForestError("EMBEDDING_CONFIG_INVALID", "Embedding profile is invalid", 422)
        self.retrieval = retrieval
        self.payloads = payloads
        self.provider = provider
        self.profile = profile
        self.component_id = component_id
        self.generation_id = generation_id
        self.route_revision = route_revision
        self.budget = budget

    def _input(self, materialized: dict) -> EmbeddingInput:
        source = {
            "bot_id": self.retrieval.store.bot_id,
            "source_unit_id": materialized["source"]["source_unit_id"],
            "source_revision": materialized["source_revision"],
            "source_id": materialized["source"]["source_id"],
            "source_kind": materialized["source"]["source_kind"],
            "platform": materialized["source"]["platform"],
            "realm_id": materialized["source"]["realm_id"],
            "group_source_id": materialized["source"]["external_group_id"],
            "trunk_id": materialized["source"]["trunk_id"],
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
        budget_claim: EmbeddingBudgetClaim | None = None,
    ) -> dict:
        results = await self.execute_batch(
            items=(EmbeddingBatchItem(facet_id, attempt_id, prepare_operation_id),),
            batch_attempt_id=attempt_id,
            expected_deletion_epoch=expected_deletion_epoch,
            deadline_at=deadline_at,
            budget_claim=budget_claim,
        )
        return results[0]

    async def execute_claim(
        self,
        claim: EmbeddingClaim,
        *,
        budget_claim: EmbeddingBudgetClaim | None = None,
    ) -> tuple[dict, ...]:
        if (
            type(claim) is not EmbeddingClaim
            or claim.bot_id != self.retrieval.store.bot_id
            or not claim.permits
            or len(claim.permits) != len(claim.prepare_operation_ids)
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding claim", 422)
        return await self.execute_batch(
            items=tuple(
                EmbeddingBatchItem(
                    permit.facet_id,
                    permit.attempt_id,
                    operation_id,
                )
                for permit, operation_id in zip(
                    claim.permits, claim.prepare_operation_ids, strict=True
                )
            ),
            batch_attempt_id=claim.claim_id,
            expected_deletion_epoch=claim.accepted_deletion_epoch,
            deadline_at=claim.deadline_at,
            budget_claim=budget_claim,
        )

    async def execute_batch(
        self,
        *,
        items,
        batch_attempt_id: str,
        expected_deletion_epoch: int,
        deadline_at,
        budget_claim: EmbeddingBudgetClaim | None = None,
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
        if (self.budget is None) != (budget_claim is None):
            raise ForestError(
                "MEMORY_EMBEDDING_BUDGET_REQUIRED",
                "Embedding worker and request budget configuration differ",
                422,
            )
        if budget_claim is not None:
            if type(budget_claim) is not EmbeddingBudgetClaim:
                raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding budget claim", 422)
            await self.budget.reserve(
                operation_id=budget_claim.operation_id,
                reservation_id=budget_claim.reservation_id,
                batch_ref=batch_attempt_id,
                policy_revision=budget_claim.policy_revision,
                budget_day=budget_claim.budget_day,
                request_limit=budget_claim.request_limit,
                token_limit=budget_claim.token_limit,
                reserved_tokens=budget_claim.reserved_tokens,
                deadline_at=deadline_at,
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
            if budget_claim is not None:
                await self.budget.settle(
                    reservation_id=budget_claim.reservation_id,
                    settlement_operation_id=_operation(
                        "embedding-budget-settlement:", batch_attempt_id
                    ),
                    outcome="RELEASED",
                    error_code="EMBEDDING_BATCH_PREPARATION_ABORTED",
                )
            raise
        handoff_id = _operation("embedding-batch-handoff:", batch_attempt_id)

        async def permit_in(conn, *, request):
            if request.prepared.space_id != self.profile.space_id:
                raise ForestError(
                    "EMBEDDING_ROUTE_MISMATCH",
                    "Embedding Provider returned another Bot space",
                    409,
                )
            await self.retrieval.mark_batch_handoff(
                tuple(permits), batch_operation_id=handoff_id, _connection=conn
            )

        try:
            result = await self.provider.encode(
                tuple(self._input(value) for value in materialized),
                request_id="request:" + batch_attempt_id,
                transport_attempt_id=batch_attempt_id,
                purpose=next(iter(purposes)),
                permit_in=permit_in,
            )
        except EmbeddingCancelled as exc:
            await asyncio.shield(
                self._settle_batch(
                    tuple(permits),
                    materialized,
                    exc.result,
                    batch_attempt_id=batch_attempt_id,
                    budget_claim=budget_claim,
                )
            )
            raise
        return await self._settle_batch(
            tuple(permits),
            materialized,
            result,
            batch_attempt_id=batch_attempt_id,
            budget_claim=budget_claim,
        )

    async def _settle_budget(self, claim, batch_attempt_id, result, *, connection=None):
        settlement_operation_id = _operation(
            "embedding-budget-settlement:", batch_attempt_id
        )
        if result.status == "RESPONDED":
            usage = result.usage
            if not isinstance(usage, dict) or type(usage.get("total_tokens")) is not int:
                raise ForestError(
                    "MEMORY_USAGE_INVALID", "Embedding response has no token receipt", 422
                )
            return await self.budget.settle(
                reservation_id=claim.reservation_id,
                settlement_operation_id=settlement_operation_id,
                outcome="CHARGED",
                charged_tokens=usage["total_tokens"],
                _connection=connection,
            )
        if result.status == "NOT_SENT":
            outcome = "RELEASED"
        elif result.status == "REJECTED":
            outcome = "CHARGED"
        else:
            outcome = "UNKNOWN"
        return await self.budget.settle(
            reservation_id=claim.reservation_id,
            settlement_operation_id=settlement_operation_id,
            outcome=outcome,
            charged_tokens=0 if outcome == "CHARGED" else None,
            error_code=result.error_code or "EMBEDDING_PROVIDER_FAILURE",
            _connection=connection,
        )

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

    def _validate_response(self, materialized, result):
        usage = result.usage
        if (
            not isinstance(usage, dict)
            or type(usage.get("total_tokens")) is not int
            or usage["total_tokens"] < 0
        ):
            raise ForestError(
                "MEMORY_USAGE_INVALID", "Embedding response has no valid token receipt", 422
            )
        vectors = {vector.facet_id: vector.values for vector in result.vectors}
        expected = {value["facet_id"] for value in materialized}
        if len(vectors) != len(result.vectors) or set(vectors) != expected:
            raise ForestError(
                "EMBEDDING_RESPONSE_INVALID", "Embedding response facets differ", 502
            )
        for vector in vectors.values():
            if (
                not isinstance(vector, (tuple, list))
                or len(vector) != self.profile.dimensions
                or any(type(value) not in {int, float} or not math.isfinite(value) for value in vector)
            ):
                raise ForestError(
                    "EMBEDDING_RESPONSE_INVALID", "Embedding response vector is invalid", 502
                )
            norm = math.sqrt(sum(float(value) * float(value) for value in vector))
            if not 0.999 <= norm <= 1.001:
                raise ForestError(
                    "EMBEDDING_RESPONSE_INVALID", "Embedding response vector is not normalized", 502
                )
        return vectors

    async def _settle_invalid_response(
        self, permits, result, *, batch_attempt_id, budget_claim
    ):
        if budget_claim is not None:
            try:
                usage = result.usage
                valid_usage = (
                    isinstance(usage, dict)
                    and type(usage.get("total_tokens")) is int
                    and usage["total_tokens"] >= 0
                )
            except ForestError:
                valid_usage = False
            if valid_usage:
                await self._settle_budget(budget_claim, batch_attempt_id, result)
            else:
                await self.budget.settle(
                    reservation_id=budget_claim.reservation_id,
                    settlement_operation_id=_operation(
                        "embedding-budget-settlement:", batch_attempt_id
                    ),
                    outcome="UNKNOWN",
                    error_code="EMBEDDING_USAGE_INVALID",
                )
        try:
            await self.retrieval.settle_batch(
                permits,
                batch_attempt_id=batch_attempt_id,
                settlement_operation_id=_operation(
                    "embedding-batch-settlement:", batch_attempt_id
                ),
                outcome="FAILED",
                expected_deletion_epoch=permits[0].accepted_deletion_epoch,
                error_code="EMBEDDING_RESPONSE_INVALID",
            )
        except ForestError as exc:
            if exc.code not in {
                "MEMORY_EMBEDDING_NOT_SETTLEABLE",
                "MEMORY_EMBEDDING_SETTLEMENT_CONFLICT",
                "MEMORY_STALE_EMBEDDING_PERMIT",
            }:
                raise

    async def _settle_batch(
        self,
        permits,
        materialized,
        result,
        *,
        batch_attempt_id,
        budget_claim,
    ):
        settlement_id = _operation("embedding-batch-settlement:", batch_attempt_id)
        vectors = {}
        response_ref = None
        if result.status == "RESPONDED":
            try:
                vectors = self._validate_response(materialized, result)
            except ForestError:
                await asyncio.shield(
                    self._settle_invalid_response(
                        permits,
                        result,
                        batch_attempt_id=batch_attempt_id,
                        budget_claim=budget_claim,
                    )
                )
                raise
        try:
            async with self.retrieval.store.pool.connection() as conn, conn.transaction():
                settlement_epoch = permits[0].accepted_deletion_epoch
                if result.status == "RESPONDED":
                    settlement_epoch = await self.payloads.epoch_in(conn)
                    body = json.dumps(
                        result.as_dict(),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ).encode("utf-8")
                    saved = await self.payloads.put_bytes_in(
                        conn,
                        owner_component_id=self.component_id,
                        owner_kind="embedding.batch-response",
                        owner_id=batch_attempt_id,
                        media_type="application/json",
                        content=body,
                        sources=tuple(
                            PayloadSource(
                                source_component_id="memory.graph",
                                source_kind="embedding.facet",
                                source_id=value["facet_id"],
                                source_revision=value["source_revision"],
                                part_id=value["part_id"],
                            )
                            for value in materialized
                        ),
                        parents=tuple(value["input_payload_id"] for value in materialized),
                        expected_deletion_epoch=settlement_epoch,
                    )
                    response_ref = MemoryPayloadRef(
                        saved.payload_id,
                        saved.content_digest,
                        self.component_id,
                        "embedding.batch-response",
                        batch_attempt_id,
                    )
                if budget_claim is not None:
                    await self._settle_budget(
                        budget_claim,
                        batch_attempt_id,
                        result,
                        connection=conn,
                    )
                outcome = {
                    "RESPONDED": "SUCCEEDED",
                    "NOT_SENT": "NOT_SENT",
                    "UNKNOWN": "UNKNOWN",
                    "REJECTED": "FAILED",
                }[result.status]
                return await self.retrieval.settle_batch(
                    permits,
                    batch_attempt_id=batch_attempt_id,
                    settlement_operation_id=settlement_id,
                    outcome=outcome,
                    expected_deletion_epoch=settlement_epoch,
                    result=response_ref,
                    vectors=vectors,
                    error_code=(
                        None
                        if result.status == "RESPONDED"
                        else result.error_code or "EMBEDDING_PROVIDER_FAILURE"
                    ),
                    _connection=conn,
                )
        except BaseException:
            if budget_claim is not None:
                await asyncio.shield(
                    self._settle_budget(budget_claim, batch_attempt_id, result)
                )
            raise
