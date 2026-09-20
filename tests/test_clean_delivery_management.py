from __future__ import annotations

from datetime import datetime, timezone

import pytest

from forest.clean_delivery_management import CleanDeliveryManagementContract
from forest.errors import ForestError
from forest.packages import digest_json


class FakeRuntime:
    def __init__(self):
        self.query_values = None
        self.resolution_values = None
        self.wakes = 0

    async def query_unknown(self, **values):
        self.query_values = values
        return {"items": [], "has_more": False}

    async def inspect_unknown(self, intent_id):
        return {"intent": {"intent_id": intent_id}, "observations": []}

    async def resolve_unknown(self, **values):
        self.resolution_values = values
        return {
            "observation_operation_id": values["observation_operation_id"],
            "attempt_id": values["attempt_id"],
            "observed_status": values["observed_status"],
            "accepted": True,
            "previous_state": "UNKNOWN",
            "resulting_state": (
                "NOT_APPLIED"
                if values["observed_status"] == "RETRYABLE_NOT_APPLIED"
                else values["observed_status"]
            ),
            "conflict_code": None,
            "observed_at": datetime.now(timezone.utc),
        }

    def notify(self):
        self.wakes += 1


class Authorization:
    web_subject_ref = "deployment-web-subject/admin"


def _contract(runtime):
    return CleanDeliveryManagementContract(
        object(),
        runtime=runtime,
        schema={"type": "object"},
    )


@pytest.mark.asyncio
async def test_delivery_management_projects_stable_unknown_queries():
    runtime = FakeRuntime()
    contract = _contract(runtime)
    snapshot = "2026-09-21T08:00:00+00:00"
    page = await contract.query(
        "delivery.unknown.query",
        {
            "lane_kind": "CHAT",
            "channel_ref": "933179609",
            "snapshot_updated_at": snapshot,
            "limit": 25,
        },
    )
    assert page == {"items": [], "has_more": False}
    assert runtime.query_values == {
        "lane_kind": "CHAT",
        "channel_ref": "933179609",
        "snapshot_updated_at": datetime.fromisoformat(snapshot),
        "limit": 25,
    }
    detail = await contract.query(
        "delivery.unknown.read", {"intent_id": "intent/one"}
    )
    assert detail["intent"]["intent_id"] == "intent/one"


@pytest.mark.asyncio
async def test_delivery_management_reconciliation_records_typed_evidence_and_wakes_retry():
    runtime = FakeRuntime()
    payload = {
        "intent_id": "intent/one",
        "attempt_id": "attempt/one",
        "resolution": "NOT_APPLIED_RETRY",
        "evidence": {
            "source_kind": "PROVIDER_QUERY",
            "source_ref": "provider-request/42",
            "note": "Provider confirms no message was accepted",
        },
    }
    guard = object()
    authorization = Authorization()
    result = await _contract(runtime).action(
        "delivery.unknown.resolve",
        operation_id="web:delivery/reconcile/one",
        payload=payload,
        authorization=authorization,
        authorization_guard=guard,
    )
    assert result.value["accepted"] is True
    assert runtime.wakes == 1
    assert runtime.resolution_values == {
        "observation_operation_id": "web:delivery/reconcile/one",
        "effect_observation_operation_id": "delivery-effect-reconcile:"
        "3ec26b1ab897c6b3a6271e4a81f1a2ec53fdc3309f6c747679d1e582893a5279",
        "intent_id": "intent/one",
        "attempt_id": "attempt/one",
        "observed_status": "RETRYABLE_NOT_APPLIED",
        "error_code": "DELIVERY_RECONCILED_NOT_APPLIED",
        "evidence_details": {
            "protocol": "forest.delivery-reconciliation-evidence/1",
            "source_kind": "PROVIDER_QUERY",
            "source_ref": "provider-request/42",
            "note": "Provider confirms no message was accepted",
            "recorded_by": "deployment-web-subject/admin",
        },
        "authorization": authorization,
        "authorization_guard": guard,
        "authorization_input_digest": digest_json(payload),
    }


@pytest.mark.asyncio
async def test_delivery_management_rejects_untyped_or_empty_evidence():
    contract = _contract(FakeRuntime())
    for evidence in (
        {"source_kind": "LOG", "source_ref": "record/1"},
        {"source_kind": "OPERATOR_OBSERVATION", "source_ref": ""},
        {"source_kind": "PROVIDER_QUERY", "source_ref": "record/1", "extra": True},
    ):
        with pytest.raises(ForestError) as caught:
            await contract.action(
                "delivery.unknown.resolve",
                operation_id="web:invalid",
                payload={
                    "intent_id": "intent/one",
                    "attempt_id": "attempt/one",
                    "resolution": "SENT",
                    "evidence": evidence,
                },
            )
        assert caught.value.code == "MODULE_MANAGEMENT_INPUT_INVALID"
