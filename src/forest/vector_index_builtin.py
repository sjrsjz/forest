"""Configuration and Bot-lifetime identity for the builtin Milvus index package."""

from __future__ import annotations

import json
from importlib.resources import files

from jsonschema import Draft202012Validator

from forest.errors import ForestError

VECTOR_INDEX_CONFIGURATION_SCHEMA = json.loads(
    files("forest")
    .joinpath("builtin/vector_index_milvus/schemas/config.json")
    .read_text(encoding="utf-8")
)


def default_vector_index_configuration(*, space_id: str, dimensions: int) -> dict:
    return {
        "protocol": "forest.vector-index.milvus/1",
        "endpoint": "http://milvus:19530",
        "credential_ref": "",
        "timeout_seconds": 10,
        "space_id": space_id,
        "dimensions": dimensions,
        "metric": "COSINE",
    }


def validate_vector_index_configuration(value: object) -> dict:
    if not Draft202012Validator(VECTOR_INDEX_CONFIGURATION_SCHEMA).is_valid(value):
        raise ForestError(
            "VECTOR_INDEX_CONFIGURATION_INVALID",
            "Vector index configuration is invalid",
            422,
        )
    return value
