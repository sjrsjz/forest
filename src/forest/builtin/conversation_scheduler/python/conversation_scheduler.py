from collections.abc import Mapping

from forest.errors import ForestError
from forest.sdk import Plugin, job


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
        self._context_view = view

    @job("conversation.silent-drain")
    async def silent_drain(self, task, ctx):
        result = await ctx.uow.call(
            "conversation.commit-silent-input",
            {
                "selection": task["payload"],
                "context_view": self._context_view,
                "payload": {
                    "protocol": "forest.payload-json-from-prior/1",
                    "prior_step_id": "prepare",
                },
            },
            operation_id=ctx.operations.stable("commit-silent-input:v1"),
        )
        if result.get("status") != "BATCH" or result.get("kind") != "SILENT_DRAIN":
            raise ForestError(
                "CONVERSATION_SILENT_DRAIN_INVALID",
                "Silent drain did not commit an input batch",
                409,
            )
        return result
