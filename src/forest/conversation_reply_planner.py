"""Assemble a final reply plan from public Agent, Scheduler, and Delivery ports."""

from __future__ import annotations

import hashlib

from forest.conversation_transaction_ports import PreparedFinalReplyCommit
from forest.errors import ForestError
from forest.final_reply_uow import FinalReplyPlan
from forest.storage.delivery_authority import DeliveryPart, DeliveryPayloadRef


def _stable(kind: str, *parts: object) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{kind}:{hashlib.sha256(encoded).hexdigest()}"


class DurableFinalReplyPlanner:
    def __init__(self, *, agent, conversation, delivery) -> None:
        self.agent = agent
        self.conversation = conversation
        self.delivery = delivery

    async def prepare(self, actor, request, completion) -> PreparedFinalReplyCommit:
        lane = await self.conversation.context_snapshot(actor)
        if (
            lane["conversation_id"] != request.conversation_id
            or lane.get("binding_ref") is None
            or lane.get("channel_kind") not in {"GROUP", "PRIVATE", "SYSTEM"}
            or lane.get("channel_ref") is None
        ):
            raise ForestError(
                "FINAL_REPLY_ROUTE_INVALID",
                "Conversation lane has no exact reply route",
                409,
            )
        replies = [fact for fact in completion.facts if fact.kind == "REPLY"]
        if len(replies) != 1 or replies[0].source_id != completion.reply_ref:
            raise ForestError(
                "FINAL_REPLY_PLAN_INVALID",
                "Agent completion does not contain one matching reply",
                422,
            )
        prepared = await self.agent.prepare_finalization(
            run_id=request.run_ref,
            reply_ref=completion.reply_ref,
        )
        try:
            run = await self.agent.load(request.run_ref)
            input_batch_refs = tuple(
                item["input_batch_ref"] for item in run.get("inputs", ())
            )
            generation = self.delivery.generation_identity
            if generation is None or generation.component_id != "delivery":
                raise ForestError(
                    "FINAL_REPLY_ROUTE_INVALID",
                    "Delivery Generation is unavailable",
                    503,
                )
            reply = replies[0]
            part_id = _stable("delivery-part", request.run_ref, completion.reply_ref)
            part = DeliveryPart(
                part_operation_id=part_id,
                effect_operation_ref=_stable("delivery-effect", part_id),
                payload=DeliveryPayloadRef(
                    reply.payload.payload_id,
                    reply.payload.digest,
                    reply.payload.owner_component_id,
                    reply.payload.owner_kind,
                    reply.payload.owner_id,
                ),
            )
            return PreparedFinalReplyCommit(
                agent_owner=prepared.owner,
                plan=FinalReplyPlan(
                    session_ref=request.session_ref,
                    run_ref=request.run_ref,
                    completion_checkpoint_id=prepared.checkpoint_id,
                    completion_state=prepared.state,
                    context_operation_id=_stable("context-final-reply", request.run_ref),
                    facts=completion.facts,
                    answered_trigger_ids=completion.answered_trigger_ids,
                    reply_ref=completion.reply_ref,
                    delivery_operation_id=_stable("delivery-intent-create", request.run_ref),
                    intent_id=_stable("delivery-intent", request.run_ref),
                    binding_ref=lane["binding_ref"],
                    ordering_key=request.conversation_id,
                    channel_kind=lane["channel_kind"],
                    channel_ref=lane["channel_ref"],
                    delivery_generation_id=generation.generation_id,
                    parts=(part,),
                    association_input_batch_refs=input_batch_refs,
                ),
                release_handle=prepared,
            )
        except BaseException:
            await self.agent.release_finalization(prepared)
            raise

    async def release(self, prepared: PreparedFinalReplyCommit) -> None:
        handle = prepared.release_handle
        if handle is None:
            raise ForestError(
                "AGENT_FINALIZATION_INPUT_INVALID",
                "Final reply plan omitted its Agent finalization handle",
                500,
            )
        await self.agent.release_finalization(handle)
