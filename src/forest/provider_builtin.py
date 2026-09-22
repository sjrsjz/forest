"""Configuration projection for the builtin Chat Completions package."""

from __future__ import annotations

import json
from importlib.resources import files

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.models import ModelCapabilities

PROVIDER_CONFIGURATION_SCHEMA = json.loads(
    files("forest")
    .joinpath("builtin/provider_chat_completions/schemas/config.json")
    .read_text(encoding="utf-8")
)


def default_provider_configuration() -> dict:
    return {
        "protocol": "forest.provider.chat-completions/1",
        "base_url": "http://127.0.0.1:8000/v1",
        "credential_ref": "secret://provider.chat-completions/default",
        "timeout_seconds": 120,
        "max_connections": 8,
        "capabilities": ModelCapabilities(
            deployment_id="provider.chat-completions/default",
            model="unconfigured",
            capability_revision="configuration/1",
        ).as_dict(),
    }


def validate_provider_configuration(value: object) -> tuple[dict, ModelCapabilities]:
    if not Draft202012Validator(PROVIDER_CONFIGURATION_SCHEMA).is_valid(value):
        raise ForestError(
            "PROVIDER_CONFIGURATION_INVALID", "Provider configuration is invalid", 422
        )
    try:
        capabilities = ModelCapabilities(**value["capabilities"])
    except (KeyError, TypeError, ForestError) as exc:
        raise ForestError(
            "PROVIDER_CONFIGURATION_INVALID", "Provider capabilities are invalid", 422
        ) from exc
    return {**value, "capabilities": capabilities.as_dict()}, capabilities
