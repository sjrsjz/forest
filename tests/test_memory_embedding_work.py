import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from forest.builtin.memory_graph.python.embedding_work import dispatch, recover


def _ref(owner_kind, owner_id):
    return {
        "payload_id": owner_kind + "/" + owner_id,
        "digest": "sha256:" + "a" * 64,
        "owner_component_id": "embedding.multimodal",
        "owner_kind": owner_kind,
        "owner_id": owner_id,
    }


@pytest.mark.asyncio
async def test_embedding_worker_handoffs_before_http_and_observes_claim_order():
    events = []
    observed = {}
    space = "sha256:" + "b" * 64
    request_id = "op:embedding-request:claim/one"
    attempt_id = "op:embedding-attempt:claim/one"
    result_ref = _ref("embedding.result", attempt_id)
    body_ref = _ref("embedding.request.body", request_id)
    manifest_ref = _ref("embedding.request.manifest", request_id)
    profile_snapshot = {
        "deployment_id": "embedding/test",
        "model_revision": "test/1",
        "model": "jina-clip-v2",
        "dimensions": 2,
    }
    profile = {
        "embedding": {**profile_snapshot, "model_kind": "TEXT_IMAGE", "normalization": "L2"},
        "embedding_budget": {
            "policy_revision": "daily-cost-cap/1",
            "daily_request_limit": 10,
            "daily_token_limit": 1000,
            "reservation_tokens": 100,
        },
    }
    facets = [{"facet_id": "facet/one"}, {"facet_id": "facet/two"}]

    async def service_call(name, payload, **_kwargs):
        events.append(name)
        if name == "embedding.profile.bind":
            return {
                "protocol": "forest.embedding-profile/2",
                "request_id": request_id,
                "component_id": "embedding.multimodal",
                "generation_id": "generation/one",
                "profile_ref": "profile:" + "c" * 64,
                "space_id": space,
                "profile": profile_snapshot,
                "max_batch_items": 8,
            }
        if name == "embedding.request.materialize":
            assert payload["purpose"] == "DOCUMENT"
            return {
                "protocol": "forest.embedding-materialized/1",
                "request_id": request_id,
                "transport_attempt_id": attempt_id,
                "component_id": "embedding.multimodal",
                "generation_id": "generation/one",
                "profile_ref": "profile:" + "c" * 64,
                "space_id": space,
                "prepared": {
                    "request_id": request_id,
                    "transport_attempt_id": attempt_id,
                    "body": body_ref,
                    "manifest": manifest_ref,
                    "payload_digest": body_ref["digest"],
                    "space_id": space,
                },
            }
        assert name == "embedding.encode"
        assert events[-2] == "memory.handoff-embedding-effect"
        assert payload["prepared"]["body"] == body_ref
        return {
            "status": "RESPONDED",
            "request_id": request_id,
            "transport_attempt_id": attempt_id,
            "payload_digest": body_ref["digest"],
            "space_id": space,
            "result": result_ref,
            "usage": {"total_tokens": 3},
            "error_code": None,
            "http_status": 200,
        }

    async def uow_call(name, payload, **_kwargs):
        events.append(name)
        if name == "memory.prepare-embedding-effect":
            assert payload["effect"]["command"]["parents"] == [body_ref, manifest_ref]
            assert payload["budget"]["reserved_tokens"] == 100
            deadline = datetime.fromisoformat(payload["authorization"]["expires_at"])
            assert 0 < (deadline - datetime.now(UTC)).total_seconds() <= 30
            return {
                "state": "PREPARED",
                "attempt_id": attempt_id,
                "authorization_ticket_id": payload["authorization"]["ticket_id"],
                "permit": {"protocol": "forest.effect-permit/1"},
            }
        if name == "memory.handoff-embedding-effect":
            return {"state": "DISPATCHED"}
        assert name == "memory.observe-embedding-effect"
        observed.update(payload)
        return {"state": "SUCCEEDED"}

    async def read_json(reference):
        assert reference == result_ref
        return {
            "protocol": "forest.embedding-result/1",
            "status": "RESPONDED",
            "request_id": request_id,
            "transport_attempt_id": attempt_id,
            "payload_digest": body_ref["digest"],
            "space_id": space,
            "vectors": [
                {"facet_id": "facet/one", "index": 0, "values": [1.0, 0.0]},
                {"facet_id": "facet/two", "index": 1, "values": [0.0, 1.0]},
            ],
            "usage": {"total_tokens": 3},
            "error_code": None,
            "http_status": 200,
        }

    ctx = SimpleNamespace(
        operations=SimpleNamespace(stable=lambda step: "op:" + step),
        identity={"bot_id": "bot/one", "graph_id": "graph/one", "graph_revision": 1},
        services=SimpleNamespace(call=service_call),
        uow=SimpleNamespace(call=uow_call),
        payloads=SimpleNamespace(read_json=read_json),
    )
    result = await dispatch(
        ctx, {"payload": {"claim_id": "claim/one", "space_id": space,
                          "purpose": "DOCUMENT", "facets": facets}},
        profile,
    )
    assert result == {"claim_id": "claim/one", "state": "SUCCEEDED"}
    assert events == [
        "embedding.profile.bind",
        "embedding.request.materialize",
        "memory.prepare-embedding-effect",
        "memory.handoff-embedding-effect",
        "embedding.encode",
        "memory.observe-embedding-effect",
    ]
    assert observed["memory"]["charged_tokens"] == 3
    assert [item["facet_id"] for item in observed["memory"]["vectors"]] == [
        "facet/one", "facet/two"
    ]
    encoded = json.dumps(
        [1.0, 0.0], sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    assert observed["memory"]["vectors"][0]["vector_digest"] == (
        "sha256:" + hashlib.sha256(encoded).hexdigest()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "transaction", "state"),
    [
        ("ABANDON", "memory.abandon-embedding-effect", "NOT_SENT"),
        ("OBSERVE_UNKNOWN", "memory.observe-embedding-effect", "UNKNOWN"),
    ],
)
async def test_embedding_recovery_never_reissues_http(mode, transaction, state):
    calls = []

    async def uow_call(name, payload, **_kwargs):
        calls.append((name, payload))
        return {"state": state}

    async def forbidden_service(*_args, **_kwargs):
        raise AssertionError("Recovery must not call the embedding Provider")

    ctx = SimpleNamespace(
        operations=SimpleNamespace(stable=lambda step: "op:" + step),
        uow=SimpleNamespace(call=uow_call),
        services=SimpleNamespace(call=forbidden_service),
    )
    result = await recover(ctx, {"payload": {
        "mode": mode,
        "claim_id": "claim/one",
        "attempt_id": "attempt/one",
        "effect_operation_id": "effect/one",
        "reservation_id": "reservation/one",
    }})
    assert result == {"claim_id": "claim/one", "state": state}
    assert [name for name, _payload in calls] == [transaction]
    if mode == "OBSERVE_UNKNOWN":
        assert calls[0][1]["effect"]["status"] == "UNKNOWN"
        assert calls[0][1]["budget"]["state"] == "UNKNOWN"
