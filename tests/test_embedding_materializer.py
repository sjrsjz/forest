"""Provider-owned request materialization retains exact archived facet identity."""

import base64
import hashlib
import io
import json
import random
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from PIL import Image

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
@pytest.mark.parametrize("trial", [False, True])
async def test_materializer_freezes_large_image_as_attributed_rendition(trial):
    width = height = 2304
    source = Image.frombytes(
        "L", (width, height), random.Random(7).randbytes(width * height)
    )
    encoded = io.BytesIO()
    source.save(encoded, format="PNG")
    original = encoded.getvalue()
    assert 4 * 1024 * 1024 < len(original) < 8 * 1024 * 1024
    reference = _reference(
        "large-source-image", original,
        owner_kind="trial.input" if trial else "archived.source",
    )
    payloads = _Payloads({"large-source-image": (reference, original)})
    provider = MultimodalEmbeddingProvider()
    await provider.prepare(None, {
        "protocol": "forest.embedding.multimodal/1",
        "endpoint": "https://example.invalid/embeddings",
        "credential_ref": "credential-1", "timeout_seconds": 60,
        "max_connections": 1,
        "profile": {"deployment_id": "test-deployment", "model_revision": "test-revision",
                    "model": "Qwen/Qwen3-VL-Embedding-8B", "dimensions": 4096},
    })
    request = {
        "protocol": "forest.embedding-materialize/1",
        "request_id": "request-1", "transport_attempt_id": "attempt-1",
        "component_id": "embedding.multimodal", "generation_id": "generation-1",
        "profile_ref": provider.profile_ref, "space_id": provider.space_id,
        "purpose": "QUERY" if trial else "DOCUMENT",
        "facets": [{
            **({"input_kind": "ADMIN_TRIAL", "input_ref": "query-1",
                "input_revision": 1, "query_id": "query-1"} if trial else {
                    "source_unit_id": "source-1", "source_revision": 1,
                    "trunk_id": "group-trunk",
                    "conversation_ref": "onebot:group:933179609",
                    "author_ref": "onebot:user:42",
                }),
            "facet_id": "facet-image",
            "part_id": "part-image", "modality": "IMAGE",
            "selector_start_char": None, "selector_end_char": None,
            "input_payload": reference, "facet_content_digest": reference["digest"],
            "input_byte_length": len(original), "media_type": "image/png",
            "width": width, "height": height,
        }],
    }
    schema_path = (Path(__file__).parents[1] / "src/forest/builtin/embedding_multimodal"
                   / "schemas/materialize-input.json")
    Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8"))).validate(request)
    ctx = SimpleNamespace(
        identity={"bot_id": "bot-1", "generation_id": "generation-1"},
        payloads=payloads,
    )
    result = await provider.materialize(request, ctx)
    body = payloads.items[result["prepared"]["body"]["payload_id"]][1]
    manifest_json = payloads.items[result["prepared"]["manifest"]["payload_id"]][1]
    prepared = PreparedEmbeddingRequest(
        request_id="request-1", transport_attempt_id="attempt-1",
        body=body, manifest_json=manifest_json.decode("utf-8"),
        payload_digest=result["prepared"]["payload_digest"],
        space_id=result["prepared"]["space_id"],
    )
    prepared.verify()
    image_input = json.loads(body)["input"][0]["image"]
    assert image_input.startswith("data:image/jpeg;base64,")
    rendition = base64.b64decode(image_input.split(",", 1)[1])
    assert len(rendition) <= 4 * 1024 * 1024
    assert prepared.manifest["facets"][0]["source"]["payload_digest"] == reference["digest"]
    if trial:
        assert prepared.manifest["facets"][0]["source"]["input_kind"] == "ADMIN_TRIAL"
    else:
        assert prepared.manifest["facets"][0]["source"]["trunk_id"] == "group-trunk"
    assert prepared.manifest["facets"][0]["capture"]["transform_ref"].startswith(
        "image-transform:sha256:"
    )
    tampered_manifest = prepared.manifest
    tampered_manifest["facets"][0]["capture"]["transform_ref"] = (
        "image-transform:sha256:" + "0" * 64
    )
    with pytest.raises(ForestError, match="provenance"):
        replace(prepared, manifest_json=json.dumps(tampered_manifest)).verify()


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


@pytest.mark.asyncio
@pytest.mark.parametrize("model,dimensions", [
    ("jina-clip-v2", 512),
    ("Qwen/Qwen3-VL-Embedding-8B", 4096),
])
async def test_materializer_preserves_trial_identity_without_source_unit(model, dimensions):
    provider = MultimodalEmbeddingProvider()
    await provider.prepare(None, {
        "protocol": "forest.embedding.multimodal/1",
        "endpoint": "https://example.invalid/embeddings",
        "credential_ref": "credential-1", "timeout_seconds": 60,
        "max_connections": 1,
        "profile": {"deployment_id": "test-deployment", "model_revision": "test-revision",
                    "model": model, "dimensions": dimensions},
    })
    text = "管理试验文本".encode("utf-8")
    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lXcAAAAASUVORK5CYII="
    )
    text_ref = _reference("trial-text", text, owner_kind="trial.input")
    image_ref = _reference("trial-image", image, owner_kind="trial.input")
    payloads = _Payloads({
        "trial-text": (text_ref, text), "trial-image": (image_ref, image),
    })
    request = {
        "protocol": "forest.embedding-materialize/1",
        "request_id": "request-1", "transport_attempt_id": "attempt-1",
        "component_id": "embedding.multimodal", "generation_id": "generation-1",
        "profile_ref": provider.profile_ref, "space_id": provider.space_id,
        "purpose": "QUERY",
        "facets": [
            {"input_kind": "ADMIN_TRIAL", "input_ref": "trial-query-1",
             "input_revision": 1, "query_id": "trial-query-1",
             "facet_id": "trial-text-facet", "part_id": "trial-text-part",
             "modality": "TEXT", "selector_start_char": 0,
             "selector_end_char": len("管理试验文本"), "input_payload": text_ref,
             "facet_content_digest": text_ref["digest"],
             "input_byte_length": len(text), "media_type": "text/plain",
             "width": None, "height": None},
            {"input_kind": "ADMIN_TRIAL", "input_ref": "trial-query-1",
             "input_revision": 1, "query_id": "trial-query-1",
             "facet_id": "trial-image-facet", "part_id": "trial-image-part",
             "modality": "IMAGE", "selector_start_char": None,
             "selector_end_char": None, "input_payload": image_ref,
             "facet_content_digest": image_ref["digest"],
             "input_byte_length": len(image), "media_type": "image/png",
             "width": 1, "height": 1},
        ],
    }
    schema_path = (Path(__file__).parents[1] / "src/forest/builtin/embedding_multimodal"
                   / "schemas/materialize-input.json")
    Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8"))).validate(
        request
    )
    ctx = SimpleNamespace(
        identity={"bot_id": "bot-1", "generation_id": "generation-1"},
        payloads=payloads,
    )
    result = await provider.materialize(request, ctx)
    prepared = PreparedEmbeddingRequest(
        request_id="request-1", transport_attempt_id="attempt-1",
        body=payloads.items[result["prepared"]["body"]["payload_id"]][1],
        manifest_json=payloads.items[result["prepared"]["manifest"]["payload_id"]][1]
        .decode("utf-8"),
        payload_digest=result["prepared"]["payload_digest"],
        space_id=result["prepared"]["space_id"],
    )
    prepared.verify()
    assert all("source_unit_id" not in facet["source"]
               for facet in prepared.manifest["facets"])
    assert prepared.manifest["facets"][1]["capture"]["query_id"] == "trial-query-1"
    request["facets"][1]["query_id"] = "another-query"
    with pytest.raises(ForestError, match="Trial query identity differs"):
        await provider.materialize(request, ctx)
