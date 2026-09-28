import json
import base64
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from PIL import Image

from forest.builtin.memory_graph.python.trial_result import read_image, read_page
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


@pytest.mark.asyncio
async def test_trial_image_chunks_are_bounded_and_come_from_attributed_evidence():
    image_bytes = io.BytesIO()
    Image.effect_noise((512, 512), 100).convert("RGB").save(image_bytes, format="PNG")
    raw_image = image_bytes.getvalue()
    media_ref = {
        "payload_id": "image/1", "digest": "sha256:" + hashlib.sha256(raw_image).hexdigest(),
        "owner_component_id": "memory.graph", "owner_kind": "source.part",
        "owner_id": "source/1/part/1",
    }
    evidence_ref = {
        "payload_id": "evidence/1", "digest": "sha256:" + "a" * 64,
        "owner_component_id": "memory.graph", "owner_kind": "retrieval.evidence",
        "owner_id": "bundle/1",
    }
    evidence = {
        "protocol": "forest.memory-trial-evidence/1", "input_kind": "ADMIN_TRIAL",
        "bot_id": "bot/1", "query_id": "trial/1", "current_source": None,
        "evidence": [{"source_unit_id": "unit/1", "parts": [{
            "kind": "image", "part_id": "part/1", "media_type": "image/png",
            "width": 512, "height": 512, "media_ref": media_ref,
        }]}],
    }

    class Queries:
        async def call(self, name, payload, *, timeout):
            assert payload == {"query_id": "trial/1", "requested_by": "admin/1"}
            return {"state": "READY", "evidence_ref": evidence_ref}

    class Payloads:
        async def read_bytes(self, reference):
            assert reference == evidence_ref
            return json.dumps(evidence).encode()

        async def read_chunk(self, reference, *, offset):
            assert reference == media_ref
            content = raw_image[offset:offset + 96 * 1024]
            return {"content": content, "offset": offset,
                    "byte_length": len(raw_image),
                    "eof": offset + len(content) == len(raw_image)}

    ctx = SimpleNamespace(identity={"bot_id": "bot/1"}, queries=Queries(), payloads=Payloads())
    request = {
        "query_id": "trial/1", "requested_by": "admin/1",
        "source_unit_id": "unit/1", "part_id": "part/1",
    }
    chunks = []
    offset = 0
    while True:
        result = await read_image(ctx, {**request, "offset": offset})
        assert result["media_type"] == "image/png"
        assert result["width"] == result["height"] == 512
        assert result["digest"] == media_ref["digest"]
        assert result["offset"] == offset
        chunk = base64.b64decode(result["data_base64"])
        assert 0 < len(chunk) <= 96 * 1024
        chunks.append(chunk)
        offset += len(chunk)
        assert result["eof"] == (offset == len(raw_image))
        if result["eof"]:
            break
    assert len(chunks) > 1
    assert b"".join(chunks) == raw_image
    schema = json.loads((Path(__file__).parents[1]
        / "src/forest/builtin/memory_graph/schemas/trial-image-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(result)
    with pytest.raises(ForestError) as rejected:
        await read_image(ctx, {
            "query_id": "trial/1", "requested_by": "admin/1",
            "source_unit_id": "unit/other", "part_id": "part/1",
        })
    assert rejected.value.code == "MEMORY_TRIAL_IMAGE_NOT_FOUND"
