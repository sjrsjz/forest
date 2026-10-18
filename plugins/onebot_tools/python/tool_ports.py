from forest.sdk import service


class ToolPorts:
    @service("tools.onebot.onebot-account-info", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_account_info(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.account_info)

    @service("tools.onebot.onebot-account-info.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_account_info_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-groups", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_groups(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.groups)

    @service("tools.onebot.onebot-groups.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_groups_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-group-info", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_group_info(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.read_api)

    @service("tools.onebot.onebot-group-info.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_group_info_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-member-info", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_member_info(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.read_api)

    @service("tools.onebot.onebot-member-info.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_member_info_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-members", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_members(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.read_api)

    @service("tools.onebot.onebot-members.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_members_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-send-group", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_send_group(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.send_group)

    @service("tools.onebot.onebot-send-group.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_send_group_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-send-private", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_send_private(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.send_private)

    @service("tools.onebot.onebot-send-private.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_send_private_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-ban-member", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_ban_member(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.manage)

    @service("tools.onebot.onebot-ban-member.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_ban_member_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-ban-all", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_ban_all(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.manage)

    @service("tools.onebot.onebot-ban-all.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_ban_all_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-kick-member", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_kick_member(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.manage)

    @service("tools.onebot.onebot-kick-member.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_kick_member_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-set-card", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_set_card(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.manage)

    @service("tools.onebot.onebot-set-card.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_set_card_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-set-group-name", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_set_group_name(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.manage)

    @service("tools.onebot.onebot-set-group-name.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_set_group_name_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)

    @service("tools.onebot.onebot-delivery-status", semantics="at-most-once", input_schema="schemas/execute.json", output_schema="schemas/receipt.json")
    async def onebot_delivery_status(self, payload, ctx):
        return await self.executor.execute(payload, ctx, self.delivery_status)

    @service("tools.onebot.onebot-delivery-status.query", semantics="read", input_schema="schemas/query.json", output_schema="schemas/receipt.json")
    async def onebot_delivery_status_query(self, payload, ctx):
        return await self.executor.query(payload, ctx)
