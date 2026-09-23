"""Typed projection into the Delivery package's narrow participant ABI."""

from __future__ import annotations

from forest.delivery_protocol import DeliveryPart
from forest.errors import ForestError
from forest.package_runtime import PackageUnitOfWorkParticipant


async def create_delivery_intent_in(
    participant: PackageUnitOfWorkParticipant,
    conn,
    *,
    operation_id: str,
    intent_id: str,
    binding_ref: str,
    lane_kind: str,
    ordering_key: str,
    conversation_ref: str | None,
    channel_kind: str,
    channel_ref: str,
    cause_component_id: str,
    cause_kind: str,
    cause_id: str,
    delivery_generation_id: str,
    reply_ref: str | None,
    parts: tuple[DeliveryPart, ...],
) -> dict:
    """Invoke one exact Delivery Generation without exposing package storage."""

    if (
        type(participant) is not PackageUnitOfWorkParticipant
        or participant.component_id != "delivery"
        or participant.generation_id != delivery_generation_id
        or lane_kind not in {"CHAT", "CONTROL"}
    ):
        raise ForestError(
            "DELIVERY_PARTICIPANT_INVALID",
            "Delivery participant differs from the frozen Generation",
            409,
        )
    payload = {
        "protocol": "forest.delivery-intent/1",
        "intent_id": intent_id,
        "binding_ref": binding_ref,
        "lane_kind": lane_kind,
        "ordering_key": ordering_key,
        "conversation_ref": conversation_ref,
        "channel_kind": channel_kind,
        "channel_ref": channel_ref,
        "cause_component_id": cause_component_id,
        "cause_kind": cause_kind,
        "cause_id": cause_id,
        "delivery_generation_id": delivery_generation_id,
        "reply_ref": reply_ref,
        "parts": [part.canonical() for part in parts],
    }
    return await participant.invoke_in(
        conn,
        (
            "delivery.create-chat-intent"
            if lane_kind == "CHAT"
            else "delivery.create-control-intent"
        ),
        payload,
        operation_id=operation_id,
        causal_root_kind=cause_kind,
        causal_root_id=cause_id,
        parent_call_id="kernel-uow:" + operation_id,
        timeout_seconds=10,
    )
