"""Provider-owned request materialization retains exact archived facet identity."""

import base64
import hashlib
import json
from types import SimpleNamespace

import pytest

from forest.builtin.embedding_multimodal.python.embedding_provider import (
    MultimodalEmbeddingProvider,
)
from forest.embeddings import PreparedEmbeddingRequest
from forest.errors import ForestError


def _reference(payload_id, content, *, component="memory.graph", owner_kind=None):
    return {
        "payload_id": payload_id,
        "digest": "sha256:" + hashlib.sha256(content).hexdigest(),
        "owner_component_id": component,
        "owner_kind": owner_kind or "archived.source",
        "owner_id": "source-1" if component == "memory.graph" else "request-1",
    }


class _Payloads:
    def __init__(self, items):
        self.items = items
        self.writes = []

    async def read_bytes(self, reference):
        exact, content = self.items[reference["payload_id"]]
        assert reference == exact
        return content

    async def put_bytes(self, content, *, owner_kind, owner_id, media_type, parents):
        assert owner_kind in {"embedding.request.body", "embedding.request.manifest"}
        assert owner_id == "request-1"
        assert media_type == "application/json"
        assert all(parent["payload_id"] in self.items for parent in parents)
        reference = _reference(
            f"prepared-{len(self.writes)}", content,
            component="embedding.multimodal", owner_kind=owner_kind,
        )
        self.items[reference["payload_id"]] = reference, content
        self.writes.append(reference)
        return reference


@pytest.mark.asyncio
@pytest.mark.parametrize("model,dimensions", [
    ("jina-clip-v2", 512),
    ("Qwen/Qwen3-VL-Embedding-8B", 4096),
])
@pytest.mark.parametrize("purpose", ["DOCUMENT", "QUERY"])
async def test_materializer_freezes_text_and_image_with_source_provenance(
    model, dimensions, purpose
):
    profile = {
        "deployment_id": "test-deployment", "model_revision": "test-revision",
        "model": model, "dimensions": dimensions,
    }
    provider = MultimodalEmbeddingProvider()
    await provider.prepare(None, {
        "protocol": "forest.embedding.multimodal/1",
        "endpoint": "https://example.invalid/embeddings",
        "credential_ref": "credential-1",
        "timeout_seconds": 60,
        "max_connections": 1,
        "profile": profile,
    })
    text = "前文：记住这件事；后文".encode("utf-8")
    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lXcAAAAASUVORK5CYII="
    )
    text_ref = _reference("source-text", text)
    image_ref = _reference("source-image", image)
    payloads = _Payloads({
        "source-text": (text_ref, text),
        "source-image": (image_ref, image),
    })
    common = {
        "source_unit_id": "source-1", "source_revision": 1,
        "trunk_id": "trunk-group-1", "conversation_ref": "onebot:group:933179609",
        "author_ref": "onebot:user:42",
    }
    selected = "记住这件事"
    request = {
        "protocol": "forest.embedding-materialize/1",
        "request_id": "request-1", "transport_attempt_id": "attempt-1",
        "component_id": "embedding.multimodal", "generation_id": "generation-1",
        "profile_ref": provider.profile_ref, "space_id": provider.space_id,
        "purpose": purpose,
        "facets": [
            {
                **common, "facet_id": "facet-text", "part_id": "part-text",
                "modality": "TEXT", "selector_start_char": 3,
                "selector_end_char": 3 + len(selected), "input_payload": text_ref,
                "facet_content_digest": "sha256:" + hashlib.sha256(
                    selected.encode("utf-8")
                ).hexdigest(),
                "input_byte_length": len(selected.encode("utf-8")),
                "media_type": "text/plain", "width": None, "height": None,
            },
            {
                **common, "facet_id": "facet-image", "part_id": "part-image",
                "modality": "IMAGE", "selector_start_char": None,
                "selector_end_char": None, "input_payload": image_ref,
                "facet_content_digest": image_ref["digest"],
                "input_byte_length": len(image), "media_type": "image/png",
                "width": 1, "height": 1,
            },
        ],
    }
    ctx = SimpleNamespace(
        identity={"bot_id": "bot-1", "generation_id": "generation-1"},
        payloads=payloads,
    )
    result = await provider.materialize(request, ctx)
    assert result["profile_ref"] == provider.profile_ref
    assert result["space_id"] == provider.space_id
    assert len(payloads.writes) == 2
    prepared = PreparedEmbeddingRequest(
        request_id="request-1", transport_attempt_id="attempt-1",
        body=payloads.items[result["prepared"]["body"]["payload_id"]][1],
        manifest_json=payloads.items[result["prepared"]["manifest"]["payload_id"]][1]
        .decode("utf-8"),
        payload_digest=result["prepared"]["payload_digest"],
        space_id=result["prepared"]["space_id"],
    )
    prepared.verify()
    assert json.loads(prepared.body)["input"][0] == {"text": selected}
    assert prepared.manifest["purpose"] == purpose.lower()
    assert prepared.manifest["facets"][0]["source"]["trunk_id"] == "trunk-group-1"
    assert prepared.manifest["facets"][1]["source"]["author_ref"] == "onebot:user:42"

    request["facets"][0]["facet_content_digest"] = "sha256:" + "0" * 64
    with pytest.raises(ForestError, match="differ from their identity"):
        await provider.materialize(request, ctx)
    assert len(payloads.writes) == 2
