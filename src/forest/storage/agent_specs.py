"""Run specifications are removable bodies; limits never authorize execution."""

import json
import math

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_payloads import CONTENT_UNAVAILABLE_CODES
from forest.storage.payloads import PayloadStore, _json

# Bounds retain the admitted host-only rev0 contract. Web writes additionally
# pass AgentSettings' narrower limits. No endpoint/prompt/tool schema is kept.
LIMITS = {
    "max_steps": (16, 128), "run_timeout": (600, 86400),
    "model_timeout": (60, 86400), "daily_token_budget": (1000000, 10000000000),
    "run_token_budget": (1000000, 10000000000), "max_output_tokens": (4096, 262144),
}


def values_metadata(values):
    if not isinstance(values, dict):
        raise ForestError("AGENT_SPEC_INVALID", "Agent limits must be an object", 422)
    result = {}
    for key, (default, maximum) in LIMITS.items():
        value = values.get(key, default)
        if key == "model_timeout" and type(value) in {int, float} and math.isfinite(value) and 0 < value <= maximum:
            result[key] = value
            continue
        if type(value) is not int or not 1 <= value <= maximum:
            raise ForestError("AGENT_SPEC_INVALID", "Invalid bounded Agent configuration", 422)
        result[key] = value
    for key, default in (("enabled", True),):
        value = values.get(key, default)
        if type(value) is not bool:
            raise ForestError("AGENT_SPEC_INVALID", "Agent switches must be booleans", 422)
        result[key] = value
    return result


def spec_metadata(spec):
    result = {"values": values_metadata(spec.get("values", spec))}
    for key in ("config_revision", "secret_revision"):
        value = spec.get(key, 0)
        if type(value) is not int or not 0 <= value <= 9223372036854775807:
            raise ForestError("AGENT_SPEC_INVALID", "Invalid Agent configuration revision", 422)
        result[key] = value
    return result


def usage_metadata(usage):
    """Normalize the two provider token dialects into the frozen scalar shape."""
    if not isinstance(usage, dict):
        return None
    result = {}
    for key, alias in (("prompt_tokens", "input_tokens"),
                       ("completion_tokens", "output_tokens"), ("total_tokens", "total_tokens")):
        value = usage.get(key, usage.get(alias))
        if type(value) is int and 0 <= value <= 1000000000:
            result[key] = value
    return result


class AgentSpecStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)

    @staticmethod
    def owner_id(run_id):
        return "agent-run-spec-v1:" + digest_json([run_id])

    async def put_in(self, conn, run_id, spec, *, parents=(), sources=()):
        spec = json.loads(_json(spec))
        spec_metadata(spec)
        epoch = (await self.payloads.lock_in(conn))["deletion_epoch"]
        ref = await self.payloads.put_json_in(
            conn, owner_kind="control.config", owner_id=self.owner_id(run_id), value=spec,
            parents=parents, sources=sources, expected_deletion_epoch=epoch,
        )
        await self.payloads.require_available_in(conn, (ref.payload_id,), expected_deletion_epoch=epoch)
        return ref

    async def hydrate_in(self, conn, row, *, required=True):
        result = dict(row)
        ref = row.get("spec_payload_id")
        if ref is None:
            raise ForestError("AGENT_PAYLOAD_MISSING", "Run specification has no payload identity", 409)
        epoch = (await self.payloads.lock_in(conn))["deletion_epoch"]
        header = await self.payloads._header_in(conn, ref)
        owner = self.owner_id(row["run_id"])
        if (header["owner_kind"] != "control.config" or header["owner_id"] != owner
                or header["digest"] != row["spec_payload_digest"] or header["encoding"] != "json"
                or ref != "payload:" + digest_json([self.store.bot_id, "control.config", owner])):
            raise ForestError("AGENT_PAYLOAD_IDENTITY_CONFLICT", "Run specification identity differs", 409)
        try:
            spec = await self.payloads.read_json_in(conn, ref, expected_deletion_epoch=epoch)
        except ForestError as exc:
            if exc.code not in CONTENT_UNAVAILABLE_CODES:
                raise
            if required:
                raise ForestError("AGENT_MEMORY_REVOKED", "Run specification is unavailable", 409) from None
            return {**result, "spec": None, "spec_payload_state": "UNAVAILABLE"}
        if (not isinstance(spec, dict) or digest_json(spec) != row["spec_digest"]
                or digest_json(spec) != header["digest"] or spec_metadata(spec) != row["spec_metadata"]):
            raise ForestError("AGENT_PAYLOAD_IDENTITY_CONFLICT", "Run specification differs from its receipt", 409)
        return {**result, "spec": spec, "spec_payload_state": "ACTIVE"}
