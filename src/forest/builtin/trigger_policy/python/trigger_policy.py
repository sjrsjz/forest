import hashlib
import json

from forest.errors import ForestError
from forest.sdk import Plugin, service


def _digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class TriggerPolicy(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.allowed_groups = frozenset(config["allowed_group_ids"])
        self.group_mode = config["group_trigger_mode"]
        self.private_enabled = config["private_enabled"]
        self.rule_revision = "trigger-policy:" + _digest(
            {
                "protocol": config["protocol"],
                "allowed_group_ids": list(config["allowed_group_ids"]),
                "group_trigger_mode": config["group_trigger_mode"],
                "private_enabled": config["private_enabled"],
            }
        )[7:23]

    @service(
        "conversation.trigger.classify",
        contract="1.0",
        input_schema="schemas/input.json",
        output_schema="schemas/output.json",
    )
    async def classify(self, payload, ctx):
        normalized = payload.get("normalized")
        if not isinstance(normalized, dict):
            raise ForestError(
                "TRIGGER_POLICY_INPUT_INVALID", "Normalized message is invalid", 422
            )
        channel_kind = normalized.get("channel_kind")
        channel_id = normalized.get("channel_id")
        self_id = normalized.get("self_id")
        author = normalized.get("author")
        segments = normalized.get("segments")
        if (
            channel_kind not in {"group", "private"}
            or not isinstance(channel_id, str)
            or not isinstance(self_id, str)
            or not isinstance(author, dict)
            or not isinstance(segments, list)
        ):
            raise ForestError(
                "TRIGGER_POLICY_INPUT_INVALID", "Normalized message is invalid", 422
            )

        decision, reason = "NO_TRIGGER", "passive_message"
        if author.get("is_self") is True:
            reason = "self_message"
        elif channel_kind == "private":
            if self.private_enabled:
                decision, reason = "TRIGGER", "private_message"
            else:
                reason = "private_disabled"
        elif channel_id not in self.allowed_groups:
            reason = "group_not_allowed"
        elif self.group_mode == "ALL":
            decision, reason = "TRIGGER", "allowed_group_message"
        elif self.group_mode == "MENTION" and any(
            isinstance(part, dict)
            and part.get("type") == "mention"
            and part.get("external_user_id") == self_id
            for part in segments
        ):
            decision, reason = "TRIGGER", "bot_mentioned"
        elif self.group_mode == "DISABLED":
            reason = "group_trigger_disabled"
        else:
            reason = "mention_required"

        return {
            "decision": decision,
            "reason_code": reason,
            "rule_revision": self.rule_revision,
            "classifier_component_id": "conversation.trigger-policy",
            "classifier_generation_id": ctx.identity["generation_id"],
        }
