"""Short-lived exact-audience authorization for Delivery attempts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.authorization_authority import GrantSetRef, KernelAuthorizationRepository
from forest.storage.delivery_authority import DeliveryClaim


def _stable_id(prefix: str, value: str) -> str:
    return f"{prefix}:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class DeliveryAuthorizationReceipt:
    ticket_id: str
    plan_digest: str
    expires_at: datetime


class DeliveryAuthorizationPort:
    """Bind one Delivery semantic input to the active Adapter Generation."""

    def __init__(
        self,
        *,
        authority: KernelAuthorizationRepository,
        delivery_grants: GrantSetRef,
        ticket_lifetime: timedelta = timedelta(seconds=20),
    ) -> None:
        if not timedelta(seconds=1) <= ticket_lifetime <= timedelta(seconds=30):
            raise ValueError("Delivery ticket lifetime must be between one and thirty seconds")
        self.authority = authority
        self.delivery_grants = delivery_grants
        self.ticket_lifetime = ticket_lifetime

    async def authorize(
        self,
        claim: DeliveryClaim,
        *,
        permit_operation_id: str,
        adapter_generation_id: str,
        binding_epoch: int,
        connection_epoch: int,
        client_incarnation: str,
        executor_fence_digest: str,
    ) -> DeliveryAuthorizationReceipt:
        if type(claim) is not DeliveryClaim:
            raise ForestError(
                "DELIVERY_AUTHORIZATION_INPUT_INVALID", "Invalid Delivery claim", 422
            )
        plan_digest = digest_json(
            {
                "binding_ref": claim.binding_ref,
                "binding_epoch": binding_epoch,
                "connection_epoch": connection_epoch,
                "client_incarnation": client_incarnation,
                "adapter_generation_id": adapter_generation_id,
                "executor_fence_digest": executor_fence_digest,
            }
        )
        attempts = await self.authority.tickets_for_operation(permit_operation_id)
        now = datetime.now(timezone.utc)
        reusable = [ticket for ticket in attempts if ticket["expires_at"] > now]
        if reusable:
            ticket = reusable[-1]
            expected = {
                "subject_kind": "component",
                "subject_id": "delivery",
                "action": "delivery.send",
                "resource_kind": "bot",
                "resource_id": self.authority.store.bot_id,
                "input_digest": claim.semantic_digest,
                "plan_digest": plan_digest,
                "grant_set_id": self.delivery_grants.grant_set_id,
                "audience_component_id": "adapter.onebot",
                "audience_generation_id": adapter_generation_id,
            }
            if any(ticket[key] != value for key, value in expected.items()):
                raise ForestError(
                    "DELIVERY_AUTHORIZATION_CONFLICT",
                    "Existing Delivery ticket has another identity",
                    409,
                )
        else:
            attempt_no = len(attempts) + 1
            identity = f"{claim.part_operation_id}:{permit_operation_id}:{attempt_no}"
            ticket = await self.authority.issue_ticket(
                ticket_id=_stable_id("ticket:delivery", identity),
                issuance_operation_id=_stable_id("ticket-issue:delivery", identity),
                issuer_kind="delivery.policy",
                issuer_id="delivery/runtime",
                subject_kind="component",
                subject_id="delivery",
                action="delivery.send",
                resource_kind="bot",
                resource_id=self.authority.store.bot_id,
                operation_id=permit_operation_id,
                input_digest=claim.semantic_digest,
                plan_digest=plan_digest,
                grant_set=self.delivery_grants,
                expires_at=now + self.ticket_lifetime,
                audience_component_id="adapter.onebot",
                audience_generation_id=adapter_generation_id,
            )
        return DeliveryAuthorizationReceipt(
            ticket["ticket_id"],
            plan_digest,
            ticket["expires_at"],
        )
