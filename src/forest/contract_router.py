"""Execution router for a conversation input selected from a frozen RouteGraph."""

from __future__ import annotations

from uuid import uuid4

from forest.errors import ForestError


class ContractExecutionRouter:
    def __init__(self, platform, component_host, adapter, agent) -> None:
        self.kernel = platform.kernel
        self.component_host = component_host
        self.adapter = adapter
        self.agent = agent

    @property
    def failure(self):
        return None

    async def execute(self, actor, selected, context):
        connection = self.adapter.connection(actor.binding_id)
        if connection.row["binding_epoch"] != actor.binding_epoch:
            raise ForestError("STALE_BINDING", "Conversation belongs to an old connection", 409)
        if selected["status"] == "MESSAGE_HANDLER":
            request = selected["request"]
            await self.component_host.ensure((request["component_id"],))
            call_operation_id = selected["attempt"]["call_operation_id"]
            receipt = await self.kernel.invoke_message_handler_with_receipt(
                request,
                selected["event"],
                operation_id=call_operation_id,
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            from forest.group_scheduler import MessageHandlerExecution

            return MessageHandlerExecution(
                call_operation_id,
                receipt.value,
                receipt.result_payload_id,
                receipt.result_digest,
            )
        if selected["status"] == "BUSINESS_COMMAND":
            request = selected["request"]
            await self.component_host.ensure((request["component_id"],))
            call_operation_id = "business-command-call:" + request["request_id"]
            receipt = await self.kernel.invoke_business_command_with_receipt(
                request,
                selected["event"],
                operation_id=call_operation_id,
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            from forest.group_scheduler import BusinessCommandExecution

            return BusinessCommandExecution(
                call_operation_id,
                receipt.value,
                receipt.result_payload_id,
                receipt.result_digest,
            )
        current = await self.adapter.bindings.execution_config(
            self.kernel._authority(),
            actor.binding_id,
            expected_binding_epoch=actor.binding_epoch,
            data=True,
        )
        config = current["config"]
        if selected.get("agent_phase") is not None or config["response_mode"] == "agent":
            return await self.agent.runner.advance(actor, selected, context)
        if config["response_mode"] != "component":
            raise ForestError("RESPONDER_DISABLED", "This Binding has no active responder", 409)
        await self.component_host.ensure(("forest.reply",))
        trigger_ids = {trigger["event_id"] for trigger in context["triggers"]}
        units = [unit for unit in context["units"] if unit.get("event_id") in trigger_ids]
        if not units:
            raise ForestError("TRIGGER_CONTEXT_MISSING", "Trigger source is not visible", 409)
        unit = units[-1]
        text = "".join(
            segment.get("text", "")
            for segment in unit["segments"]
            if segment["type"] == "text"
        )
        result = await self.kernel.invoke_with_receipt(
            "forest.reply",
            "reply.respond",
            {
                "template": config["response_template"],
                "text": text[:8000],
                "sender": unit["author"].get(
                    "display_name", unit["author"]["external_user_id"]
                )[:160],
                "group": context["conversation"]["channel_id"][:160],
            },
            operation_id="response:" + str(uuid4()),
            expected_policy_revision=actor.policy_revision,
            input_parent_ids=(*context["payload_ids"], current["config_payload_id"]),
            causal_root_kind="CONVERSATION_RUN",
            causal_root_id=selected["run_id"],
        )
        from forest.group_scheduler import ComponentReply

        return ComponentReply(result.value["parts"], (result.result_payload_id,))
