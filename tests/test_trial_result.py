import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from forest.builtin.memory_graph.python.trial_result import read_page
from forest.errors import ForestError


@pytest.mark.asyncio
async def test_trial_result_pages_keep_group_and_author_attribution():
    reference = {
        "payload_id": "payload:evidence", "digest": "sha256:" + "a" * 64,
        "owner_component_id": "memory.graph", "owner_kind": "retrieval.evidence",
        "owner_id": "bundle/1",
    }
    calls = []

    class Queries:
        async def call(self, name, payload, *, timeout):
            calls.append((name, payload))
            return {"state": "READY", "evidence_ref": reference}

    class Payloads:
        async def read_bytes(self, supplied):
            assert supplied == reference
            return json.dumps({
                "protocol": "forest.memory-trial-evidence/1",
                "input_kind": "ADMIN_TRIAL", "bot_id": "bot/1",
                "query_id": "trial/1", "bundle_id": "bundle/1",
                "graph_snapshot_ref": "memory-query:trial/1",
                "current_source": None, "missing_count": 0,
                "content_truncated": False, "truncated": False,
                "association_sampled": True,
                "evidence": [{
                    "source_unit_id": f"unit/{index}",
                    "source_id": f"source/{index}",
                    "source_kind": "GROUP", "platform": "qq",
                    "external_group_id": f"group/{index}",
                    "trunk_id": f"trunk/{index}",
                    "author_subject_ref": f"user/{index}",
                    "occurred_at": "2026-09-28T00:00:00+00:00",
                    "is_seed": index == 0, "rank_score": 0.5,
                    "content_truncated": False,
                    "parts": [{"kind": "text", "text": f"text {index}"}],
                } for index in range(5)],
            }).encode()

    ctx = SimpleNamespace(
        identity={"bot_id": "bot/1"}, queries=Queries(), payloads=Payloads()
    )
    first = await read_page(ctx, {
        "query_id": "trial/1", "requested_by": "admin/1", "limit": 4,
    })
    second = await read_page(ctx, {
        "query_id": "trial/1", "requested_by": "admin/1", "limit": 4,
        "after_index": first["next_index"],
    })
    assert first["has_more"] and first["next_index"] == 3
    assert [item["external_group_id"] for item in first["items"]] == [
        "group/0", "group/1", "group/2", "group/3",
    ]
    assert first["items"][0]["author_subject_ref"] == "user/0"
    assert [item["source_unit_id"] for item in second["items"]] == ["unit/4"]
    assert not second["has_more"]
    assert len(calls) == 4
    assert all(payload == {"query_id": "trial/1", "requested_by": "admin/1"}
               for _, payload in calls)
    schema = json.loads((Path(__file__).parents[1]
        / "src/forest/builtin/memory_graph/schemas/trial-result-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(first)
    Draft202012Validator(schema).validate(second)


@pytest.mark.asyncio
async def test_trial_result_rejects_another_bot_evidence():
    class Queries:
        async def call(self, name, payload, *, timeout):
            return {"state": "READY", "evidence_ref": {"payload_id": "payload/1"}}

    class Payloads:
        async def read_bytes(self, supplied):
            return json.dumps({
                "protocol": "forest.memory-trial-evidence/1",
                "input_kind": "ADMIN_TRIAL", "bot_id": "bot/other",
                "query_id": "trial/1", "current_source": None, "evidence": [],
            }).encode()

    ctx = SimpleNamespace(
        identity={"bot_id": "bot/1"}, queries=Queries(), payloads=Payloads()
    )
    with pytest.raises(ForestError) as rejected:
        await read_page(ctx, {"query_id": "trial/1", "requested_by": "admin/1"})
    assert rejected.value.code == "MEMORY_EVIDENCE_INVALID"
