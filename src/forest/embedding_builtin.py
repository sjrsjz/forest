"""Configuration projection for the builtin multimodal Embedding package."""

from __future__ import annotations

import json
from importlib.resources import files

from jsonschema import Draft202012Validator

from forest.embeddings import SILICONFLOW_VL_MODEL, EmbeddingProfile
from forest.errors import ForestError

EMBEDDING_CONFIGURATION_SCHEMA = json.loads(
    files("forest")
    .joinpath("builtin/embedding_multimodal/schemas/config.json")
    .read_text(encoding="utf-8")
)


def default_embedding_configuration() -> dict:
    return {
        "protocol": "forest.embedding.multimodal/1",
        "endpoint": "https://api.siliconflow.cn/v1/embeddings",
        "credential_ref": "secret://embedding.multimodal/default",
        "timeout_seconds": 60,
        "max_connections": 4,
        "profile": {
            "deployment_id": "embedding.multimodal/qiyu-vl/default",
            "model_revision": "operator-declared/1",
            "model": SILICONFLOW_VL_MODEL,
            "dimensions": 4096,
        },
    }


def validate_embedding_configuration(value: object) -> tuple[dict, EmbeddingProfile]:
    if not Draft202012Validator(EMBEDDING_CONFIGURATION_SCHEMA).is_valid(value):
        raise ForestError(
            "EMBEDDING_CONFIGURATION_INVALID", "Embedding configuration is invalid", 422
        )
    try:
        profile = EmbeddingProfile(**value["profile"])
    except (KeyError, TypeError, ValueError, ForestError) as exc:
        raise ForestError(
            "EMBEDDING_CONFIGURATION_INVALID", "Embedding profile is invalid", 422
        ) from exc
    return value, profile
