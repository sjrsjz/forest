"""Generation-bound trigger policy for normalized conversation messages."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class ConversationTriggerPolicyRuntime:
    """Classify a normalized message without owning ingress or scheduling facts."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [{"service_id": "conversation.trigger.classify", "version": 1}],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(self, *, module_configuration, grants) -> None:
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_identity: BoundGenerationIdentity | None = None
        self.configuration: dict | None = None
        self.configuration_revision: int | None = None
        self.active = False

    def runtime_service_handlers(self) -> dict:
        return {("conversation.trigger.classify", 1): self.classify}

    @staticmethod
    def _parse_configuration(value: object) -> dict:
        if not isinstance(value, dict) or set(value) != {
            "protocol",
            "allowed_group_ids",
            "group_trigger_mode",
            "private_enabled",
        }:
            raise ForestError(
                "TRIGGER_POLICY_CONFIGURATION_INVALID",
                "Trigger policy configuration fields differ from its contract",
                422,
            )
        groups = value["allowed_group_ids"]
        if (
            value["protocol"] != "forest.conversation.trigger-policy/1"
            or not isinstance(groups, list)
            or len(groups) > 4096
            or any(
                not isinstance(group, str)
                or not group.isdecimal()
                or not 1 <= len(group) <= 32
                for group in groups
            )
            or len(groups) != len(set(groups))
            or value["group_trigger_mode"] not in {"MENTION", "ALL", "DISABLED"}
            or type(value["private_enabled"]) is not bool
        ):
            raise ForestError(
                "TRIGGER_POLICY_CONFIGURATION_INVALID",
                "Trigger policy configuration is invalid",
                422,
            )
        return {
            **value,
            "allowed_group_ids": tuple(groups),
        }

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        self.configuration = self._parse_configuration(desired.value)
        self.configuration_revision = desired.revision
        return PreparedGenerationBinding(
            component_id=context.component_id,
            requires=context.requires,
            package_id=context.package_id,
            package_version=context.package_version,
            package_digest=context.package_digest,
            driver=context.driver,
            trust_level=context.trust_level,
            descriptor_digest=context.descriptor_digest,
            route_contract_digest=context.route_contract_digest,
            management_contract_digest=context.management_contract_digest,
            management_ui_digest=context.management_ui_digest,
            config_revision=desired.revision,
            config_payload_id=desired.payload_id,
            config_payload_digest=desired.payload_digest,
            instance_id=f"instance:conversation.trigger-policy:{context.activation_id}",
            worker_incarnation=f"worker:conversation.trigger-policy:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "conversation.trigger-policy":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Trigger Policy received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Trigger Policy Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None or self.configuration is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Trigger Policy Generation was not prepared and bound",
                503,
            )

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False

    async def close(self) -> None:
        self.active = False

    async def classify(self, *, normalized: dict) -> dict:
        if not self.active or self.generation_identity is None or self.configuration is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Trigger Policy Generation is not accepting work",
                409,
            )
        if not isinstance(normalized, dict):
            raise ForestError("TRIGGER_POLICY_INPUT_INVALID", "Normalized message is invalid", 422)
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
            raise ForestError("TRIGGER_POLICY_INPUT_INVALID", "Normalized message is invalid", 422)

        decision = "NO_TRIGGER"
        reason = "passive_message"
        if author.get("is_self") is True:
            reason = "self_message"
        elif channel_kind == "private":
            if self.configuration["private_enabled"]:
                decision, reason = "TRIGGER", "private_message"
            else:
                reason = "private_disabled"
        elif channel_id not in self.configuration["allowed_group_ids"]:
            reason = "group_not_allowed"
        elif self.configuration["group_trigger_mode"] == "ALL":
            decision, reason = "TRIGGER", "allowed_group_message"
        elif self.configuration["group_trigger_mode"] == "MENTION" and any(
            isinstance(part, dict)
            and part.get("type") == "mention"
            and part.get("external_user_id") == self_id
            for part in segments
        ):
            decision, reason = "TRIGGER", "bot_mentioned"
        elif self.configuration["group_trigger_mode"] == "DISABLED":
            reason = "group_trigger_disabled"
        else:
            reason = "mention_required"

        return {
            "decision": decision,
            "reason_code": reason,
            "rule_revision": (
                "trigger-policy:"
                + str(self.configuration_revision)
                + ":"
                + digest_json(self.configuration)[:23]
            ),
            "classifier_component_id": "conversation.trigger-policy",
            "classifier_generation_id": self.generation_identity.generation_id,
        }
