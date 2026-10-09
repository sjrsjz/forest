"""Pure, package-owned identity of one immutable multimodal embedding space."""

import hashlib
import json

from forest.embedding_contract import IMAGE_INPUT_POLICY


def digest_json(value):
    content = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(content).hexdigest()


def profile_snapshot(profile):
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
        raise ValueError("Unsupported embedding model")
    return {
        **profile,
        "protocol_revision": protocol_revision,
        "document_task": None,
        "query_task": query_task,
        "normalized": True,
        "truncate": truncate,
        "image_input": IMAGE_INPUT_POLICY,
        "postprocessing": "l2_float32_v1",
        "mixed_strategy": "independent_facets_v1",
    }
