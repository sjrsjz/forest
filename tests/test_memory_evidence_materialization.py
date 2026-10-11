import json
from types import SimpleNamespace

import pytest

from forest.builtin.memory_graph.python.evidence_materialization import materialize
from forest.errors import ForestError
from forest.packages import digest_json


def _ref(name):
    return {"payload_id": name, "digest": "sha256:" + "a" * 64,
            "owner_component_id": "adapter.onebot", "owner_kind": "source", "owner_id": name}


def _context(*, images=1, dependencies=3, query_parts=1, text=b"abc", budget=256,
             media_budget=12582912,
             author_bytes=None, input_kind="SOURCE_UNIT"):
    documents = {}
    ancestry = {}
    published = []
    raw_parts = [{"part_id": "text", "part_index": 0, "part_kind": "TEXT",
                  "byte_length": len(text), "payload": _ref("text")}]
    raw_parts.extend({"part_id": f"image/{i}", "part_index": i + 1, "part_kind": "IMAGE",
                      "payload": _ref(f"image/{i}"), "media_type": "image/png",
                      "byte_length": 1024, "width": 10, "height": 10}
                     for i in range(images))
    node = {"source_unit_id": "unit/test", "author_payload": _ref("author"), "part_count": len(raw_parts)}
    state = {"evidence_ref": None, "bundle_id": "retrieval/test", "result_count": 1,
             "input_kind": input_kind, "max_retrieval_chars": budget,
             "max_retrieval_media_bytes": media_budget,
             "current_source": None if input_kind == "ADMIN_TRIAL" else {"source_id": "group/query"},
             "truncated": False, "truncation_reason": None,
             "association_sampled": False, "read_count": 1}

    async def query(port, body, **_kwargs):
        if port == "memory.read-retrieval-materialization":
            return state
        if port == "memory.read-query-parts":
            start = body["after_part_index"] + 1
            end = min(query_parts, start + body["limit"])
            return {"parts": [{"payload": _ref(f"query/{i}")} for i in range(start, end)],
                    "next_part_index": end - 1, "has_more": end < query_parts}
        if port == "memory.read-retrieval-parts":
            start = body["after_part_index"] + 1
            parts = raw_parts[start:start + body["limit"]]
            return {"node": node, "parts": parts, "has_more": start + len(parts) < len(raw_parts),
                    "last_part_index": start + len(parts) - 1}
        assert port == "memory.read-retrieval-dependencies"
        start = 0 if body["after_source_unit_id"] is None else int(body["after_source_unit_id"]) + 1
        end = min(dependencies, start + 32)
        return {"available": True, "refs": [{"payload": _ref(f"anchor/{i}")} for i in range(start, end)],
                "next_source_unit_id": str(end - 1), "has_more": end < dependencies}

    async def read(reference):
        if reference["payload_id"] == "text":
            return text
        assert reference["payload_id"] == "author"
        return author_bytes if author_bytes is not None else json.dumps({
            "principal_ref": "qq:1", "display_name_at_event": "Original name",
        }).encode()

    async def put(value, *, owner_kind, owner_id, parents):
        assert len(parents) <= 256
        key = owner_kind + "/" + owner_id
        documents[key] = value
        ancestry[key] = [ref["payload_id"] for ref in parents]
        return {**_ref(key), "owner_component_id": "memory.graph", "owner_kind": owner_kind, "owner_id": owner_id}

    async def publish(port, body, **_kwargs):
        assert port == "memory.publish-retrieval-evidence"
        published.append(body)
        return {**state, **body["memory"]}

    ctx = SimpleNamespace(identity={"bot_id": "bot/test"}, queries=SimpleNamespace(call=query),
                          payloads=SimpleNamespace(read_bytes=read, put_json=put),
                          operations=SimpleNamespace(stable=lambda value: value), uow=SimpleNamespace(call=publish))
    return ctx, documents, ancestry, published


@pytest.mark.asyncio
async def test_evidence_cuts_unicode_text_and_keeps_images_with_source_dependencies():
    ctx, documents, ancestry, published = _context(text=("图" * 300).encode())
    result = await materialize(ctx, "query/test", digest_json)
    document = documents[result["evidence_ref"]["payload_id"]]
    unit = document["evidence"][0]
    assert unit["parts"][0]["text"] == "图" * 256
    assert unit["parts"][0]["truncated"] and document["content_truncated"]
    assert unit["parts"][1]["kind"] == "image"
    assert unit["parts"][1]["projection_state"] == "READY"
    assert document["selected_media_bytes"] == 1024
    assert unit["author_identity"]["display_name_at_event"] == "Original name"
    parents = {parent for group in ancestry.values() for parent in group}
    assert {"text", "image/0", "query/0", "anchor/0", "anchor/1", "anchor/2"} <= parents
    assert len(published) == 1


@pytest.mark.asyncio
async def test_media_budget_is_frozen_in_ranked_evidence_without_losing_source_parts():
    ctx, documents, _ancestry, _published = _context(images=3, media_budget=2048)
    result = await materialize(ctx, "query/test", digest_json)
    document = documents[result["evidence_ref"]["payload_id"]]
    images = [part for part in document["evidence"][0]["parts"] if part["kind"] == "image"]
    assert [part["projection_state"] for part in images] == [
        "READY", "READY", "OMITTED_BUDGET",
    ]
    assert [part["byte_length"] for part in images] == [1024, 1024, 1024]
    assert document["max_retrieval_media_bytes"] == 2048
    assert document["selected_media_bytes"] == 2048
    assert document["content_truncated"]


@pytest.mark.asyncio
async def test_management_trial_evidence_has_independent_revocable_items():
    ctx, documents, ancestry, _published = _context(input_kind="ADMIN_TRIAL")
    result = await materialize(ctx, "query/test", digest_json)
    document = documents[result["evidence_ref"]["payload_id"]]
    assert document["protocol"] == "forest.memory-trial-evidence/2"
    assert document["input_kind"] == "ADMIN_TRIAL"
    assert document["current_source"] is None
    assert "evidence" not in document
    assert document["item_indexes"] == [0]
    unit_ref = document["evidence_refs"][0]
    assert documents[unit_ref["payload_id"]]["source_unit_id"] == "unit/test"
    assert unit_ref["payload_id"] not in ancestry[result["evidence_ref"]["payload_id"]]
    assert "query/0" in {parent for group in ancestry.values() for parent in group}


@pytest.mark.asyncio
async def test_maximum_path_dependencies_and_query_parts_fit_payload_parent_limit():
    ctx, documents, ancestry, _ = _context(images=70, dependencies=4160, query_parts=256)
    result = await materialize(ctx, "query/test", digest_json)
    document = documents[result["evidence_ref"]["payload_id"]]
    assert len(document["evidence"][0]["parts"]) == 64
    assert document["evidence"][0]["part_count"] == 71
    assert document["content_truncated"]
    assert max(map(len, ancestry.values())) <= 256
    parents = {parent for group in ancestry.values() for parent in group}
    assert "anchor/4159" in parents and "query/255" in parents
    assert "image/63" not in parents  # Explicit part truncation, no phantom content.


@pytest.mark.asyncio
async def test_invalid_text_cannot_publish_evidence():
    ctx, _documents, _ancestry, published = _context(text=b"\xff")
    with pytest.raises(ForestError) as invalid:
        await materialize(ctx, "query/test", digest_json)
    assert invalid.value.code == "MEMORY_TEXT_INVALID"
    assert published == []


@pytest.mark.asyncio
@pytest.mark.parametrize("author_bytes,code", [
    (b" " * 65537, "MEMORY_AUTHOR_TOO_LARGE"),
    (b"[]", "MEMORY_AUTHOR_INVALID"),
], ids=["too_large", "not_object"])
async def test_unbounded_or_invalid_identity_cannot_publish(author_bytes, code):
    ctx, _documents, _ancestry, published = _context(author_bytes=author_bytes)
    with pytest.raises(ForestError) as invalid:
        await materialize(ctx, "query/test", digest_json)
    assert invalid.value.code == code
    assert published == []
