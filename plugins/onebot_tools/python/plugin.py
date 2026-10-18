from tool_ports import ToolPorts

from forest.errors import ForestError
from forest.sdk import Plugin, service
from forest.sdk_tools import AgentToolExecutor, ToolOutcome, ToolRejected, plain

READ_ACTIONS = {"onebot_group_info": "get_group_info", "onebot_member_info": "get_group_member_info",
                "onebot_members": "get_group_member_list"}
MANAGE_ACTIONS = {"onebot_ban_member": "set_group_ban", "onebot_ban_all": "set_group_whole_ban",
                  "onebot_kick_member": "set_group_kick", "onebot_set_card": "set_group_card",
                  "onebot_set_group_name": "set_group_name"}


class Tools(ToolPorts, Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        self.config = plain(config)
        self.executor = AgentToolExecutor("tools.onebot")

    @service("tools.onebot.configuration.groups", semantics="read",
             input_schema="schemas/groups-page-input.json", output_schema="schemas/groups-page-output.json")
    async def configuration_groups(self, payload, ctx):
        return await ctx.services.call(
            "adapter.onebot.groups.page", plain(payload),
            operation_id=ctx.operations.stable("configuration-groups"), timeout=12,
        )

    def _allowed(self, command):
        tool = command["service_id"].removeprefix("tools.onebot.").replace("-", "_")
        if tool not in self.config["enabled_tools"]:
            raise ToolRejected("ONEBOT_TOOL_DISABLED", "This OneBot tool is disabled", 403)
        group_id = command["arguments"].get("group_id")
        if group_id is not None and group_id not in self.config["allowed_group_ids"]:
            raise ToolRejected("ONEBOT_GROUP_DENIED", "Group is outside this plugin's configured scope", 403)
        if not self.config["binding_ref"]:
            raise ToolRejected("ONEBOT_BINDING_REQUIRED", "Select an account in plugin settings", 409)
        return tool

    async def _read(self, service_id, payload, ctx):
        try:
            return await ctx.services.call(
                service_id, payload,
                operation_id=ctx.operations.stable("onebot-read:" + service_id), timeout=15,
            )
        except ForestError as error:
            raise ToolRejected(error.code, "OneBot read failed before any command handoff", error.status) from None

    async def _query(self, action, arguments, ctx):
        return await self._read("adapter.onebot.api.query", {
            "binding_ref": self.config["binding_ref"], "action": action, "arguments": arguments,
        }, ctx)

    async def _authorize_write(self, command, ctx):
        principal = command["authority_principal_ref"]
        if principal.startswith("qq:") and principal[3:] in self.config["operator_ids"]:
            return
        group_id = command["arguments"].get("group_id")
        if group_id is None or not principal.startswith("qq:"):
            raise ToolRejected("ONEBOT_OPERATOR_REQUIRED", "OneBot operation requires a configured operator", 403)
        member = await self._query("get_group_member_info", {"group_id": group_id, "user_id": principal[3:]}, ctx)
        tool = command["service_id"].removeprefix("tools.onebot.").replace("-", "_")
        if member["data"].get("role") not in self.config["management_permissions"].get(tool, []):
            raise ToolRejected("ONEBOT_MANAGEMENT_DENIED", "Trigger principal lacks the configured group role", 403)

    async def account_info(self, command, ctx):
        self._allowed(command)
        return plain(await self._query("get_login_info", {}, ctx))

    async def groups(self, command, ctx):
        self._allowed(command)
        page = await self._read(
            "adapter.onebot.groups.page", {"binding_ref": self.config["binding_ref"],
                                          **command["arguments"]},
            ctx,
        )
        value = plain(page)
        value["items"] = [row for row in value["items"] if row["group_id"] in self.config["allowed_group_ids"]]
        value["scope_note"] = "Only configured groups on this Bot binding are shown. Cursor follows the platform group directory."
        return value

    async def read_api(self, command, ctx):
        tool = self._allowed(command)
        return plain(await self._query(READ_ACTIONS[tool], command["arguments"], ctx))

    async def send_group(self, command, ctx):
        return await self._send(command, ctx, "GROUP", "group_id")

    async def send_private(self, command, ctx):
        return await self._send(command, ctx, "PRIVATE", "user_id")

    async def _send(self, command, ctx, kind, target):
        self._allowed(command)
        await self._authorize_write(command, ctx)
        result = await ctx.services.call(
            "delivery.submit", {"binding_ref": self.config["binding_ref"], "channel_kind": kind,
                                "channel_ref": command["arguments"][target],
                                "parts": [{"type": "text", "format": "platform-markup",
                                           "text": command["arguments"]["message"]}],
                                "cause_id": command["effect_operation_id"]},
            operation_id=ctx.operations.stable("submit-onebot-message"), timeout=15,
        )
        return {**plain(result), "status_meaning": "Durably queued; delivery status is tracked by Delivery, not yet confirmed SENT."}

    async def manage(self, command, ctx):
        tool = self._allowed(command)
        await self._authorize_write(command, ctx)
        result = await ctx.services.call(
            "adapter.onebot.api.command", {"binding_ref": self.config["binding_ref"],
                                           "action": MANAGE_ACTIONS[tool],
                                           "arguments": command["arguments"]},
            operation_id=ctx.operations.stable("onebot-management"), timeout=15,
        )
        if result["outcome"] != "SUCCEEDED":
            return ToolOutcome(result["outcome"], error_code=result["error_code"])
        return plain(result)

    async def delivery_status(self, command, ctx):
        self._allowed(command)
        value = await self._read("delivery.status", command["arguments"], ctx)
        if value is None:
            return {"found": False}
        value = plain(value)
        if value["binding_ref"] != self.config["binding_ref"]:
            raise ToolRejected("ONEBOT_BINDING_DENIED", "Intent belongs to another binding", 403)
        if value["channel_kind"] == "GROUP":
            if value["channel_ref"] not in self.config["allowed_group_ids"]:
                raise ToolRejected("ONEBOT_GROUP_DENIED", "Intent group is outside the configured scope", 403)
        elif command["authority_principal_ref"] not in {"qq:" + user for user in self.config["operator_ids"]}:
            raise ToolRejected("ONEBOT_OPERATOR_REQUIRED", "Private intent status requires an operator", 403)
        return {"found": True, **value}
