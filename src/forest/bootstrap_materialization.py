"""Evaluate bounded, package-owned creation-to-runtime configuration templates."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any

from jsonschema import Draft202012Validator

from forest.errors import ForestError

_COMPONENT_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_OWNER = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._/-]{0,511}\Z")
_PROTOCOL = "forest.bootstrap-materialization/1"


def _invalid(message: str) -> None:
    raise ForestError("INVALID_BOOTSTRAP_MATERIALIZATION", message, 422)


def _reference(value: object) -> tuple[str, tuple[str, ...]]:
    if not isinstance(value, str) or len(value) > 1152 or "#" not in value:
        _invalid("Bootstrap reference must name a component and JSON Pointer")
    component_id, pointer = value.split("#", 1)
    if _COMPONENT_ID.fullmatch(component_id) is None:
        _invalid("Bootstrap reference component identity is invalid")
    if pointer and (not pointer.startswith("/") or len(pointer) > 1024):
        _invalid("Bootstrap reference JSON Pointer is invalid")
    tokens = () if not pointer else tuple(pointer[1:].split("/"))
    if any(not token or re.search(r"~(?![01])", token) for token in tokens):
        _invalid("Bootstrap reference JSON Pointer is invalid")
    return component_id, tuple(token.replace("~1", "/").replace("~0", "~") for token in tokens)


def _validate_expression(value: Any, depth: int = 0) -> None:
    if depth > 32:
        _invalid("Bootstrap materialization expression is too deep")
    if isinstance(value, dict):
        if "$bootstrap" in value:
            if set(value) != {"$bootstrap"}:
                _invalid("Bootstrap reference must be a complete expression")
            _reference(value["$bootstrap"])
            return
        if "$payload_json" in value:
            if set(value) != {"$payload_json"}:
                _invalid("Bootstrap Payload must be a complete expression")
            payload = value["$payload_json"]
            if not isinstance(payload, dict) or set(payload) != {
                "owner_kind", "owner_id", "value"
            }:
                _invalid("Bootstrap Payload declaration is invalid")
            if (
                not isinstance(payload["owner_kind"], str)
                or _OWNER.fullmatch(payload["owner_kind"]) is None
                or not isinstance(payload["owner_id"], str)
                or _OWNER.fullmatch(payload["owner_id"]) is None
            ):
                _invalid("Bootstrap Payload owner is invalid")
            _validate_expression(payload["value"], depth + 1)
            return
        if any(not isinstance(key, str) or key.startswith("$") for key in value):
            _invalid("Bootstrap materialization object key is invalid")
        for nested in value.values():
            _validate_expression(nested, depth + 1)
    elif isinstance(value, list):
        for nested in value:
            _validate_expression(nested, depth + 1)
    elif value is not None and type(value) not in (str, int, float, bool):
        _invalid("Bootstrap materialization literal is invalid")


def validate_materialization_spec(spec: object, *, has_contribution: bool) -> dict:
    if (
        not isinstance(spec, dict)
        or set(spec) != {"protocol", "base", "overlay"}
        or spec["protocol"] != _PROTOCOL
        or spec["base"] not in ("profile", "seed")
        or not isinstance(spec["overlay"], dict)
    ):
        _invalid("Bootstrap materialization declaration is invalid")
    if spec["base"] == "profile" and not has_contribution:
        _invalid("Only a creation-time contributor may use its Profile as base")
    _validate_expression(spec["overlay"])
    return spec


async def materialize_bootstrap_configuration(
    snapshot, profile_components: dict[str, dict], *, payloads,
) -> dict:
    """Materialize one exact package's initial config from frozen creation facts."""

    component_id = snapshot.component["id"]
    contribution = snapshot.bootstrap_contribution()
    spec = snapshot.bootstrap_materialization()
    base = (
        "profile" if contribution is not None else "seed"
    ) if spec is None else spec["base"]
    if base == "profile":
        if component_id not in profile_components:
            raise ForestError(
                "BOOTSTRAP_COMPONENT_MISSING",
                "Package creation component is absent from frozen Profile", 409,
                details={"component_id": component_id},
            )
        configuration = deepcopy(profile_components[component_id])
    else:
        configuration = snapshot.bootstrap_configuration()
    if not isinstance(configuration, dict):
        _invalid("Bootstrap base configuration is not an object")

    async def evaluate(expression: Any) -> Any:
        if isinstance(expression, dict):
            if "$bootstrap" in expression:
                source_id, tokens = _reference(expression["$bootstrap"])
                if source_id not in profile_components:
                    raise ForestError(
                        "BOOTSTRAP_COMPONENT_MISSING",
                        "Template references a component absent from frozen Profile", 409,
                        details={"component_id": source_id},
                    )
                value = profile_components[source_id]
                for token in tokens:
                    if not isinstance(value, dict) or token not in value:
                        raise ForestError(
                            "BOOTSTRAP_REFERENCE_MISSING",
                            "Template references an absent creation field", 409,
                            details={"component_id": source_id},
                        )
                    value = value[token]
                return deepcopy(value)
            if "$payload_json" in expression:
                declaration = expression["$payload_json"]
                value = await evaluate(declaration["value"])
                content = json.dumps(
                    value, sort_keys=True, separators=(",", ":"),
                    ensure_ascii=False, allow_nan=False,
                ).encode("utf-8")
                payload = await payloads.put_bytes(
                    owner_component_id=component_id,
                    owner_kind=declaration["owner_kind"],
                    owner_id=declaration["owner_id"],
                    media_type="application/json",
                    content=content,
                    expected_deletion_epoch=await payloads.epoch(),
                )
                return {
                    "payload_id": payload.payload_id,
                    "digest": payload.content_digest,
                    "owner_id": declaration["owner_id"],
                }
            return {key: await evaluate(value) for key, value in expression.items()}
        if isinstance(expression, list):
            return [await evaluate(value) for value in expression]
        return expression

    if spec is not None:
        configuration.update(await evaluate(spec["overlay"]))
    schema = snapshot.schema(snapshot.component["config_schema"])
    if not Draft202012Validator(schema).is_valid(configuration):
        raise ForestError(
            "PACKAGE_CONFIGURATION_INVALID",
            "Materialized configuration violates the package schema", 422,
            details={"component_id": component_id},
        )
    return configuration
