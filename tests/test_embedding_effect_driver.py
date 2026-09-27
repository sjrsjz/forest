"""The embedding effect carries exact payload references, not image bytes."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from forest.embedding_effect_driver import MultimodalEmbeddingEffectDriver, _canonical
from forest.embeddings import (
    SILICONFLOW_VL_MODEL,
    EmbeddingInput,
    EmbeddingProfile,
    EmbeddingResult,
    EmbeddingVector,
    prepare_embedding_request,
)
from forest.errors import ForestError
from forest.model_contract import MediaSnapshot
from forest.packages import digest_json


def _reference(payload_id, content, owner_kind):
    return {
        "payload_id": payload_id,
        "digest": "sha256:" + hashlib.sha256(content).hexdigest(),
        "owner_component_id": "embedding.multimodal",
        "owner_kind": owner_kind,
        "owner_id": "request-1",
    }


class _Payloads:
    def __init__(self, items):
        self.items = items
        self.read_count = 0
        self.saved = None

    async def epoch(self):
        return 0

    async def put_bytes(self, **values):
        assert values["expected_deletion_epoch"] == 0
        self.saved = values
        return SimpleNamespace(
            payload_id="payload-result",
            content_digest="sha256:" + hashlib.sha256(values["content"]).hexdigest(),
        )

    async def read_verified_batch(self, references):
        self.read_count += 1
        result = []
        for reference in references:
            stored, content = self.items[reference.payload_id]
            assert vars(reference) == stored
            assert reference.digest == "sha256:" + hashlib.sha256(content).hexdigest()
            result.append(content)
        return tuple(result)


class _Effects:
    def __init__(self, payload):
        self.payload = payload

    async def load(self, operation_id):
        assert operation_id == self.payload["effect_operation_id"]
        return {
            "effect": {"state": "DISPATCHING"},
            "attempt": {
                "state": "DISPATCHING",
                "attempt_id": self.payload["prepared"]["transport_attempt_id"],
                "executor_component_id": "embedding.multimodal",
                "executor_generation_id": "generation-1",
                "wire_payload_digest": "sha256:"
                + hashlib.sha256(_canonical(self.payload)).hexdigest(),
            },
        }


class _Runtime:
    async def verify_generation_member(self, *, component_id, generation_id):
        assert (component_id, generation_id) == (
            "embedding.multimodal", "generation-1"
        )


class _Client:
    async def dispatch(self, prepared, *, permitcheck):
        await permitcheck(prepared)
        return EmbeddingResult(
            "NOT_SENT", prepared.request_id, prepared.transport_attempt_id,
            prepared.payload_digest, prepared.space_id,
        )


class _LargeResultClient:
    async def dispatch(self, prepared, *, permitcheck):
        await permitcheck(prepared)
        values = (0.015625,) * 4096
        return EmbeddingResult(
            "RESPONDED", prepared.request_id, prepared.transport_attempt_id,
            prepared.payload_digest, prepared.space_id,
            tuple(
                EmbeddingVector(facet["facet_id"], index, values)
                for index, facet in enumerate(prepared.manifest["facets"])
            ),
            '{"total_tokens":32}',
            http_status=200,
        )


@pytest.mark.asyncio
async def test_large_image_dispatch_uses_exact_payload_references():
    profile = EmbeddingProfile(
        deployment_id="test-deployment", model_revision="test-revision"
    )
    image_bytes = b"image-pixel-data" * 32000
    capture = MediaSnapshot(
        bot_id="bot-1", media_ref="media-1", media_revision=1,
        source_unit_id="source-1", part_id="part-1", mime="image/png",
        content=image_bytes,
        blob_digest="sha256:" + hashlib.sha256(image_bytes).hexdigest(),
        width=100, height=100,
    )
    prepared = prepare_embedding_request(
        profile,
        [EmbeddingInput(
            "facet-1", image=capture,
            source_json=json.dumps({"source_unit_id": "source-1", "part_id": "part-1"}),
        )],
        request_id="request-1", transport_attempt_id="attempt-1",
    )
    body_ref = _reference("payload-body", prepared.body, "embedding.request.body")
    manifest_bytes = prepared.manifest_json.encode("utf-8")
    manifest_ref = _reference(
        "payload-manifest", manifest_bytes, "embedding.request.manifest"
    )
    configuration = {"profile": {
        "deployment_id": profile.deployment_id,
        "model_revision": profile.model_revision,
        "model": profile.model,
        "dimensions": profile.dimensions,
    }}
    payload = {
        "protocol": "forest.embedding-dispatch/2",
        "effect_operation_id": "effect-1",
        "authorization_ticket_id": "ticket-1",
        "component_id": "embedding.multimodal",
        "generation_id": "generation-1",
        "profile_ref": "profile:" + digest_json(configuration)[7:],
        "prepared": {
            "request_id": prepared.request_id,
            "transport_attempt_id": prepared.transport_attempt_id,
            "body": body_ref,
            "manifest": manifest_ref,
            "payload_digest": prepared.payload_digest,
            "space_id": prepared.space_id,
        },
    }
    assert len(prepared.body) > 262144
    assert len(_canonical(payload)) < 262144
    payloads = _Payloads({
        body_ref["payload_id"]: (body_ref, prepared.body),
        manifest_ref["payload_id"]: (manifest_ref, manifest_bytes),
    })
    driver = MultimodalEmbeddingEffectDriver(
        configuration, 1,
        SimpleNamespace(component_id="embedding.multimodal", generation_id="generation-1"),
        {"kind": "embedding.multimodal.http"},
        effects=_Effects(payload), payloads=payloads, runtime=_Runtime(),
        credential_resolver=lambda *_: "unused",
    )
    driver.client = _Client()
    driver.accepting = True
    result = await driver.invoke(
        payload,
        scope=SimpleNamespace(component_id="embedding.multimodal", generation_id="generation-1"),
        invocation=SimpleNamespace(operation_id="effect-1"),
    )
    assert result["status"] == "NOT_SENT"
    assert payloads.read_count == 1

    payload["prepared"]["body"]["owner_component_id"] = "another.component"
    with pytest.raises(ForestError, match="payload references"):
        await driver.invoke(
            payload,
            scope=SimpleNamespace(component_id="embedding.multimodal", generation_id="generation-1"),
            invocation=SimpleNamespace(operation_id="effect-1"),
        )
    assert payloads.read_count == 1


@pytest.mark.asyncio
async def test_maximum_vl_batch_returns_small_receipt_and_managed_result():
    profile = EmbeddingProfile(
        deployment_id="test-deployment", model_revision="test-revision",
        model=SILICONFLOW_VL_MODEL, dimensions=4096,
    )
    prepared = prepare_embedding_request(
        profile,
        [EmbeddingInput(f"facet-{index}", text=f"text {index}") for index in range(32)],
        request_id="request-1", transport_attempt_id="attempt-1",
    )
    manifest_bytes = prepared.manifest_json.encode("utf-8")
    body_ref = _reference("payload-body", prepared.body, "embedding.request.body")
    manifest_ref = _reference(
        "payload-manifest", manifest_bytes, "embedding.request.manifest"
    )
    configuration = {"profile": {
        "deployment_id": profile.deployment_id,
        "model_revision": profile.model_revision,
        "model": profile.model,
        "dimensions": profile.dimensions,
    }}
    payload = {
        "protocol": "forest.embedding-dispatch/2",
        "effect_operation_id": "effect-1",
        "authorization_ticket_id": "ticket-1",
        "component_id": "embedding.multimodal",
        "generation_id": "generation-1",
        "profile_ref": "profile:" + digest_json(configuration)[7:],
        "prepared": {
            "request_id": prepared.request_id,
            "transport_attempt_id": prepared.transport_attempt_id,
            "body": body_ref,
            "manifest": manifest_ref,
            "payload_digest": prepared.payload_digest,
            "space_id": prepared.space_id,
        },
    }
    payloads = _Payloads({
        body_ref["payload_id"]: (body_ref, prepared.body),
        manifest_ref["payload_id"]: (manifest_ref, manifest_bytes),
    })
    driver = MultimodalEmbeddingEffectDriver(
        configuration, 1,
        SimpleNamespace(component_id="embedding.multimodal", generation_id="generation-1"),
        {"kind": "embedding.multimodal.http"},
        effects=_Effects(payload), payloads=payloads, runtime=_Runtime(),
        credential_resolver=lambda *_: "unused",
    )
    driver.client = _LargeResultClient()
    driver.accepting = True
    receipt = await driver.invoke(
        payload,
        scope=SimpleNamespace(component_id="embedding.multimodal", generation_id="generation-1"),
        invocation=SimpleNamespace(operation_id="effect-1"),
    )
    assert receipt["status"] == "RESPONDED"
    assert receipt["result"] == {
        "payload_id": "payload-result",
        "digest": "sha256:" + hashlib.sha256(payloads.saved["content"]).hexdigest(),
        "owner_component_id": "embedding.multimodal",
        "owner_kind": "embedding.result",
        "owner_id": "attempt-1",
    }
    assert len(_canonical(receipt)) < 262144
    assert len(payloads.saved["content"]) > 262144
    assert payloads.saved["parents"] == ("payload-body", "payload-manifest")
    document = json.loads(payloads.saved["content"])
    assert document["protocol"] == "forest.embedding-result/1"
    assert len(document["vectors"]) == 32
    assert all(len(vector["values"]) == 4096 for vector in document["vectors"])
