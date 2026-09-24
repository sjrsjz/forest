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
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class MemoryGraph(Plugin):
    """Own the Bot-global, per-conversation-trunk multimodal Memory domain."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = _plain(config)
        if value.get("protocol") != "forest.memory.graph/2":
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID",
                "Memory Graph configuration protocol is unsupported",
                422,
            )
        embedding = value["embedding"]
        if embedding["model_kind"] != "TEXT_IMAGE":
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID",
                "Memory Graph requires one lifecycle-bound text-image embedding kind",
                422,
            )
        self.profile = value
        self.profile_digest = _digest(value)

    @service(
        "memory.profile.describe",
        semantics="read",
        contract="2.0",
        input_schema="schemas/profile-input.json",
        output_schema="schemas/profile-output.json",
    )
    async def describe_profile(self, payload, ctx):
        return {
            "protocol": "forest.memory-profile/2",
            "component_id": "memory.graph",
            "generation_id": ctx.identity["generation_id"],
            "profile_digest": self.profile_digest,
            "embedding": self.profile["embedding"],
            "fusion_policy_revision": self.profile["fusion_policy_revision"],
            "retrieval": self.profile["retrieval"],
        }
