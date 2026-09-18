"""Exact, short-lived authorization tickets for Shell management calls."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.storage.shell_command_authority import ShellClaim, ShellCommandRepository


def _stable(kind: str, *parts: object) -> str:
    body = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"shell:{kind}:{hashlib.sha256(body).hexdigest()}"


@dataclass(frozen=True)
class ShellAuthorizationReceipt:
    ticket_id: str
    operation_id: str


class ShellAuthorizationPort:
    def __init__(self, *, authority, grants, repository: ShellCommandRepository) -> None:
        self.authority = authority
        self.grants = grants
        self.repository = repository

    async def issue(
        self,
        claim: ShellClaim,
        *,
        phase: str,
        operation_id: str,
    ) -> ShellAuthorizationReceipt:
        if type(claim) is not ShellClaim or phase not in {"invoke", "result"}:
            raise ForestError(
                "SHELL_AUTHORIZATION_INPUT_INVALID",
                "Shell authorization input is invalid",
                422,
            )
        ticket_id = _stable("ticket", claim.request_id, claim.owner_epoch, phase)
        issuance_id = _stable("ticket-issue", claim.request_id, claim.owner_epoch, phase)
        await self.authority.issue_ticket(
            ticket_id=ticket_id,
            issuance_operation_id=issuance_id,
            issuer_kind="shell.policy",
            issuer_id="shell.command",
            subject_kind="component",
            subject_id="shell.command",
            action="management.invoke",
            resource_kind="bot",
            resource_id=self.authority.store.bot_id,
            operation_id=operation_id,
            input_digest=self.repository.invocation_input_digest(claim),
            plan_digest=claim.required_capabilities_digest,
            grant_set=self.grants,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=20),
            audience_component_id=claim.owner_component_id,
            audience_generation_id=claim.owner_generation_id,
        )
        return ShellAuthorizationReceipt(ticket_id, operation_id)
