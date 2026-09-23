from collections.abc import Mapping

from forest.errors import ForestError
from forest.sdk import Plugin


class ConversationScheduler(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = dict(config)
        if value.get("protocol") != "forest.conversation.scheduler/2":
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation Scheduler configuration is invalid",
                422,
            )
        raw_view = value.get("context_view")
        view = dict(raw_view) if isinstance(raw_view, Mapping) else raw_view
        if (
            not isinstance(view, dict)
            or set(view)
            != {"high_watermark_units", "retain_units", "policy_revision"}
            or type(view["high_watermark_units"]) is not int
            or type(view["retain_units"]) is not int
            or not 2 <= view["high_watermark_units"] <= 1_000_000
            or not 1 <= view["retain_units"] < view["high_watermark_units"]
            or view["policy_revision"] != "context-view-unit-window-v1"
        ):
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation Context view configuration is invalid",
                422,
            )
