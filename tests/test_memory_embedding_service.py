from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from forest.embeddings import SILICONFLOW_VL_MODEL, EmbeddingProfile
from forest.errors import ForestError
from forest.memory_embedding_service import CleanMemoryEmbeddingService
from forest.memory_embedding_worker import (
    EmbeddingBatchItem,
    EmbeddingBudgetClaim,
    MemoryEmbeddingWorker,
)
from forest.storage.memory_retrieval_authority import EmbeddingPermit


def _profile() -> EmbeddingProfile:
    return EmbeddingProfile(
        deployment_id="embedding/test",
        model_revision="test/1",
        model=SILICONFLOW_VL_MODEL,
        dimensions=64,
    )


@pytest.mark.asyncio
async def test_embedding_service_keeps_owning_work_after_one_batch_failure():
    attempted = asyncio.Event()

    class Retrieval:
        async def recover_expired_attempts(self):
            return {"not_sent": 0, "unknown": 0}

        async def has_pending_facets(self, *, purpose):
            return purpose == "DOCUMENT"

        async def next_index_deletion(self):
            return None

        async def next_embedded_facet(self, *, purpose):
            return None

    class Budget:
        async def recover_expired(self):
            return {"released": 0, "unknown": 0}

    service = CleanMemoryEmbeddingService(
        retrieval=Retrieval(),
        payloads=object(),
        provider=object(),
        vector_index=object(),
        profile=_profile(),
        component_id="memory.graph",
        generation_id="generation/test",
        route_revision="route/test",
        budget=Budget(),
        request_limit=10,
        token_limit=100,
        reserved_tokens=10,
        policy_revision="policy/test",
        poll_interval=0.01,
    )

    async def fail_batch(_purpose):
        attempted.set()
        raise ForestError("EMBEDDING_INPUT_INVALID", "invalid local facet", 422)

    service._execute_one = fail_batch
    service.start()
    service.activate()
    try:
        await asyncio.wait_for(attempted.wait(), timeout=1)
        await asyncio.sleep(0)
        task = service.owned_tasks()[0]
        assert not task.done()
        assert service.failure == "EMBEDDING_INPUT_INVALID"
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_local_input_failure_is_not_sent_before_budget_reservation():
    deadline = datetime.now(timezone.utc) + timedelta(minutes=2)
    permit = EmbeddingPermit(
        "bot/test",
        "facet/test",
        "attempt/test",
        1,
        "memory.graph",
        "generation/test",
        "route/test",
        0,
        deadline,
    )

    class Retrieval:
        store = SimpleNamespace(bot_id="bot/test")

        def __init__(self):
            self.settlements = []

        async def materialize_facet(self, _facet_id, *, expected_deletion_epoch):
            assert expected_deletion_epoch == 0
            return {
                "facet_id": "facet/test",
                "purpose": "DOCUMENT",
                "modality": "TEXT",
                "space_id": _profile().space_id,
                "source_revision": 1,
                "part_id": "part/text",
                "input_payload_id": "payload/test",
                "input_payload_digest": "sha256:" + "1" * 64,
                "width": None,
                "height": None,
                "text": " \n\t ",
                "source": {
                    "source_unit_id": "unit/test",
                    "source_id": "source/test",
                    "source_kind": "GROUP",
                    "platform": "onebot",
                    "realm_id": "qq",
                    "external_group_id": "group/test",
                    "trunk_id": "trunk/test",
                    "author_subject_ref": "qq:user/test",
                },
            }

        async def prepare_attempt(self, **_values):
            return permit

        async def settle_attempt(self, _permit, **values):
            self.settlements.append(values)
            return {"state": values["outcome"]}

    class Budget:
        reserve_calls = 0

        async def reserve(self, **_values):
            self.reserve_calls += 1

    retrieval = Retrieval()
    budget = Budget()
    worker = MemoryEmbeddingWorker(
        retrieval=retrieval,
        payloads=object(),
        provider=object(),
        vector_index=object(),
        profile=_profile(),
        component_id="memory.graph",
        generation_id="generation/test",
        route_revision="route/test",
        budget=budget,
    )
    with pytest.raises(ForestError) as caught:
        await worker.execute_batch(
            items=(EmbeddingBatchItem("facet/test", "attempt/test", "prepare/test"),),
            batch_attempt_id="batch/test",
            expected_deletion_epoch=0,
            deadline_at=deadline,
            budget_claim=EmbeddingBudgetClaim(
                reservation_id="reservation/test",
                operation_id="reservation-operation/test",
                policy_revision="policy/test",
                budget_day=date(2026, 9, 21),
                request_limit=10,
                token_limit=100,
                reserved_tokens=10,
            ),
        )
    assert caught.value.code == "EMBEDDING_INPUT_INVALID"
    assert budget.reserve_calls == 0
    assert len(retrieval.settlements) == 1
    assert retrieval.settlements[0]["outcome"] == "NOT_SENT"
    assert retrieval.settlements[0]["expected_deletion_epoch"] == 0
    assert retrieval.settlements[0]["error_code"] == "EMBEDDING_LOCAL_INPUT_INVALID"
