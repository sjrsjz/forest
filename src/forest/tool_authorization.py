"""Agent tool policy adapter backed by the generic authorization authority."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from forest.agent_tool_executor import (
    ToolAuthorizationRequest,
    ToolAuthorizationResult,
)
from forest.errors import ForestError
from forest.storage.authorization_authority import GrantSetRef, KernelAuthorizationRepository


def _stable_id(prefix: str, value: str) -> str:
    return f"{prefix}:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


class KernelToolAuthorizationPort:
    """Freeze a tool approval and issue an exact Generation-bound short ticket."""

    def __init__(
        self,
        *,
        authority: KernelAuthorizationRepository,
        agent_grants: GrantSetRef,
        ticket_lifetime: timedelta = timedelta(seconds=20),
        approval_lifetime: timedelta = timedelta(minutes=15),
    ) -> None:
        if not timedelta(seconds=1) <= ticket_lifetime <= timedelta(seconds=30):
            raise ValueError("Tool ticket lifetime must be between one and thirty seconds")
        if not timedelta(seconds=1) <= approval_lifetime <= timedelta(hours=24):
            raise ValueError("Tool approval lifetime must be at most twenty-four hours")
        self.authority = authority
        self.agent_grants = agent_grants
        self.ticket_lifetime = ticket_lifetime
        self.approval_lifetime = approval_lifetime

    async def _existing(self, ticket_id: str) -> dict | None:
        try:
            return await self.authority.ticket(ticket_id)
        except ForestError as exc:
            if exc.code == "TICKET_NOT_FOUND":
                return None
            raise

    async def _issue(
        self,
        request: ToolAuthorizationRequest,
        *,
        ticket_id: str,
        issuance_operation_id: str,
        approval_id: str | None,
    ) -> dict:
        existing = await self._existing(ticket_id)
        if existing is not None:
            expected = {
                "subject_kind": "component",
                "subject_id": request.producer_component_id,
                "action": "tool.invoke",
                "resource_kind": "bot",
                "resource_id": self.authority.store.bot_id,
                "operation_id": request.effect_operation_ref,
                "input_digest": request.input_digest,
                "plan_digest": request.binding_digest,
                "grant_set_id": self.agent_grants.grant_set_id,
                "grant_set_digest": self.agent_grants.grant_set_digest,
                "audience_component_id": request.executor_component_id,
                "audience_generation_id": request.executor_generation_id,
            }
            if any(existing[key] != value for key, value in expected.items()):
                raise ForestError(
                    "TOOL_AUTHORIZATION_CONFLICT", "Tool ticket identity differs", 409
                )
            return existing
        expires_at = datetime.now(timezone.utc) + self.ticket_lifetime
        values = dict(
            ticket_id=ticket_id,
            issuer_kind="agent.policy",
            issuer_id="agent.runtime/tool-authorization",
            subject_kind="component",
            subject_id=request.producer_component_id,
            action="tool.invoke",
            resource_kind="bot",
            resource_id=self.authority.store.bot_id,
            operation_id=request.effect_operation_ref,
            issuance_operation_id=issuance_operation_id,
            input_digest=request.input_digest,
            plan_digest=request.binding_digest,
            grant_set=self.agent_grants,
            expires_at=expires_at,
            audience_component_id=request.executor_component_id,
            audience_generation_id=request.executor_generation_id,
        )
        if approval_id is None:
            return await self.authority.issue_ticket(**values)
        async with self.authority.store.pool.connection() as conn, conn.transaction():
            await self.authority.consume_approval_in(
                conn,
                approval_id=approval_id,
                request_kind="agent.tool",
                request_id=request.tool_call_id,
                action="tool.invoke",
                input_digest=request.input_digest,
                plan_digest=request.binding_digest,
                requester_kind="component",
                requester_id=request.producer_component_id,
                consume_operation_id=request.effect_operation_ref,
            )
            return await self.authority.issue_ticket_in(conn, **values)

    async def authorize(
        self,
        request: ToolAuthorizationRequest,
        *,
        approval_required: bool,
    ) -> ToolAuthorizationResult:
        if type(request) is not ToolAuthorizationRequest:
            raise ForestError(
                "TOOL_AUTHORIZATION_INPUT_INVALID", "Invalid tool authorization request", 422
            )
        if request.producer_component_id != "agent.runtime":
            return ToolAuthorizationResult("DENIED", error_code="TOOL_PRODUCER_DENIED")
        approval_id = None
        if approval_required:
            approval_id = _stable_id("approval:agent-tool", request.tool_call_id)
            try:
                approval = await self.authority.approval(approval_id)
            except ForestError as exc:
                if exc.code != "APPROVAL_NOT_FOUND":
                    raise
                approval = await self.authority.request_approval(
                    approval_id=approval_id,
                    request_kind="agent.tool",
                    request_id=request.tool_call_id,
                    action="tool.invoke",
                    input_digest=request.input_digest,
                    plan_digest=request.binding_digest,
                    requester_kind="component",
                    requester_id=request.producer_component_id,
                    approver_scope_kind="bot",
                    approver_scope_id=self.authority.store.bot_id,
                    expected_policy_revision=self.agent_grants.policy_revision,
                    expires_at=datetime.now(timezone.utc) + self.approval_lifetime,
                )
            if approval["state"] == "PENDING":
                return ToolAuthorizationResult("WAITING", approval_ref=approval_id)
            if approval["state"] == "REJECTED":
                return ToolAuthorizationResult(
                    "DENIED",
                    approval_ref=approval_id,
                    error_code="TOOL_APPROVAL_REJECTED",
                )

        tickets = await self.authority.tickets_for_operation(request.effect_operation_ref)
        now = datetime.now(timezone.utc)
        reusable = [ticket for ticket in tickets if ticket["expires_at"] > now]
        if reusable:
            ticket_id = reusable[-1]["ticket_id"]
            issuance_operation_id = reusable[-1]["issuance_operation_id"]
        else:
            attempt_no = len(tickets) + 1
            ticket_id = _stable_id(
                "ticket:agent-tool", f"{request.tool_call_id}:{attempt_no}"
            )
            issuance_operation_id = _stable_id(
                "ticket-issue:agent-tool", f"{request.tool_call_id}:{attempt_no}"
            )
        try:
            ticket = await self._issue(
                request,
                ticket_id=ticket_id,
                issuance_operation_id=issuance_operation_id,
                approval_id=approval_id,
            )
        except ForestError as exc:
            if exc.code in {
                "APPROVAL_EXPIRED",
                "APPROVAL_NOT_APPROVED",
                "POLICY_CONFLICT",
            }:
                return ToolAuthorizationResult(
                    "DENIED", approval_ref=approval_id, error_code=exc.code
                )
            raise
        if ticket["expires_at"] <= datetime.now(timezone.utc):
            return ToolAuthorizationResult(
                "DENIED", approval_ref=approval_id, error_code="TOOL_TICKET_EXPIRED"
            )
        return ToolAuthorizationResult(
            "AUTHORIZED",
            authorization_ticket_id=ticket_id,
            approval_ref=approval_id,
            input_digest=request.input_digest,
            audience_component_id=request.executor_component_id,
            audience_generation_id=request.executor_generation_id,
        )
