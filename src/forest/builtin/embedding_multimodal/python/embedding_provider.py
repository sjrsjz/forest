import hashlib
import json

from forest.errors import ForestError
from forest.sdk import Plugin, service


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _profile_snapshot(profile):
    model = profile["model"]
    if model == "jina-clip-v2":
        protocol_revision = "jina-clip-v2-http-2026-09-v1"
        query_task = "retrieval.query"
        truncate = False
    elif model == "Qwen/Qwen3-VL-Embedding-8B":
        protocol_revision = "siliconflow-qwen3-vl-embedding-http-2026-09-v1"
        query_task = None
        truncate = "right"
    else:
        raise ForestError(
            "EMBEDDING_CONFIGURATION_INVALID", "Embedding model is unsupported", 422
        )
    return {
        **profile,
        "protocol_revision": protocol_revision,
        "document_task": None,
        "query_task": query_task,
        "normalized": True,
        "truncate": truncate,
        "image_input": "authorized_capture_bytes_v1",
        "postprocessing": "l2_float32_v1",
        "mixed_strategy": "independent_facets_v1",
    }


class MultimodalEmbeddingProvider(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        plain = _plain(config)
        if plain["protocol"] != "forest.embedding.multimodal/1":
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID", "Embedding protocol is unsupported", 422
            )
        self.profile = plain["profile"]
        snapshot = _profile_snapshot(self.profile)
        self.space_id = _digest(snapshot)
        self.profile_ref = "profile:" + _digest(plain)[7:]

    @service(
        "embedding.profile.bind",
        semantics="read",
        contract="1.0",
        input_schema="schemas/bind-input.json",
        output_schema="schemas/bind-output.json",
    )
    async def bind(self, payload, ctx):
        return {
            "protocol": "forest.embedding-profile/1",
            "request_id": payload["request_id"],
            "component_id": "embedding.multimodal",
            "generation_id": ctx.identity["generation_id"],
            "profile_ref": self.profile_ref,
            "space_id": self.space_id,
            "profile": self.profile,
        }
