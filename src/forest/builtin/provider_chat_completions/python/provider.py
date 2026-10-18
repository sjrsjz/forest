import hashlib
import json

from forest.errors import ForestError
from forest.model_contract import ModelCapabilities
from forest.sdk import Plugin, service


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


class ChatCompletionsProvider(Plugin):
    """Own the immutable public profile; host effects own credentials and sockets."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if config["protocol"] != "forest.provider.chat-completions/1":
            raise ForestError(
                "PROVIDER_CONFIGURATION_INVALID", "Provider protocol is unsupported", 422
            )
        plain_config = _plain(config)
        self.capabilities = ModelCapabilities(**plain_config["capabilities"]).as_dict()
        self.capability_digest = _digest(self.capabilities)
        self.profile_ref = "profile:" + _digest(plain_config)[7:]

    def replacement_facts(self):
        return {
            "forest.provider-profile/1": {
                "profile_ref": self.profile_ref,
                "capability_digest": self.capability_digest,
                "model_ref": self.capabilities["model"],
            }
        }

    @service(
        "provider.profile.bind",
        semantics="read",
        contract="1.0",
        input_schema="schemas/bind-input.json",
        output_schema="schemas/bind-output.json",
    )
    async def bind(self, payload, ctx):
        return {
            "protocol": "forest.provider-profile/1",
            "request_id": payload["request_id"],
            "component_id": "provider.chat-completions",
            "generation_id": ctx.identity["generation_id"],
            "profile_ref": self.profile_ref,
            "capability_digest": self.capability_digest,
            "capabilities": self.capabilities,
        }
