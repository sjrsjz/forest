"""Transaction-bound Shell claim for a declared module management action."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeUnitOfWorkParticipantHandle
from forest.shell_command_protocol import ShellCommandExecutionRequest
from forest.storage.kernel_audit import AuditEnvelope


@dataclass(frozen=True)
class ShellManagementAuthorization:
    host: object
    request: ShellCommandExecutionRequest
    component_id: str
    generation_id: str
    service_id: str
    service_version: int
    action_id: str
    operation_id: str
    input_digest: str
    plan_digest: str | None

    @property
    def actor_ref(self) -> str:
        return self.request.principal_ref

    @property
    def causal_root_kind(self) -> str:
        return "shell.request"

    @property
    def causal_root_id(self) -> str:
        return self.request.request_id

    async def verify_in(
        self,
        conn,
        *,
        ticket,
        component_id: str,
        operation_id: str,
        action_key: str,
        input_digest: str,
        plan_digest: str | None,
    ) -> None:
        if (
            ticket is not self
            or component_id != self.component_id
            or operation_id != self.operation_id
            or action_key != self.action_id
            or input_digest != self.input_digest
            or plan_digest != self.plan_digest
            or not self.request.claim_token
            or not self.request.shell_generation_id
        ):
            raise ForestError(
                "SHELL_MANAGEMENT_AUTHORITY_INVALID",
                "Shell management authority does not bind this action",
                403,
            )
        registry = self.host.registry
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "RuntimeGraph is unavailable", 503)
        try:
            participant = RuntimeUnitOfWorkParticipantHandle(
                registry.contract_router, "shell.command"
            ).resolve()
        except ForestError as exc:
            raise ForestError(
                "SHELL_MANAGEMENT_AUTHORITY_UNAVAILABLE",
                "Shell claim verifier is unavailable",
                503,
            ) from exc
        verified = await participant.invoke_in(
            conn,
            "shell.verify-management-claim",
            {
                "protocol": "forest.shell-management-claim/1",
                "request_id": self.request.request_id,
                "claim_token": self.request.claim_token,
                "shell_generation_id": self.request.shell_generation_id,
                "principal_ref": self.request.principal_ref,
                "conversation_ref": self.request.conversation_ref,
                "command_id": self.request.command_id,
                "command_version": self.request.command_version,
                "arguments": self.request.arguments,
                "target_component_id": self.component_id,
                "target_generation_id": self.generation_id,
                "target_service_id": self.service_id,
                "target_service_version": self.service_version,
                "management_operation_id": self.action_id,
            },
            operation_id="shell-verify:" + digest_json(
                [self.operation_id, self.request.claim_token]
            )[7:],
            causal_root_kind="shell.request",
            causal_root_id=self.request.request_id,
            parent_call_id="shell-control:" + self.request.request_id,
            timeout_seconds=10,
        )
        if (
            verified.get("protocol") != "forest.shell-management-claim-verified/1"
            or verified.get("request_id") != self.request.request_id
            or verified.get("principal_ref") != self.actor_ref
        ):
            raise ForestError(
                "SHELL_MANAGEMENT_AUTHORITY_INVALID",
                "Shell claim verifier returned another authority",
                403,
            )
        await self.host._kernel.runtime.verify_active_generation_in(
            conn,
            component_id=self.component_id,
            generation_id=self.generation_id,
            admission_lane="CONTROL",
        )
        audit_id = "audit:shell:" + hashlib.sha256(
            (
                self.host.store.bot_id + "\0" + self.operation_id + "\0"
                + self.request.claim_token
            ).encode()
        ).hexdigest()
        await self.host._kernel.audit.append_in(
            conn,
            AuditEnvelope(
                audit_id=audit_id,
                action=self.action_id,
                severity="SECURITY",
                producer_component_id=self.component_id,
                producer_generation_id=self.generation_id,
                origin_kind="shell.management",
                actor_kind="qq.principal",
                actor_id=self.actor_ref,
                resource_kind="bot.component",
                resource_id=self.component_id,
                operation_kind="shell.action",
                operation_id=self.operation_id,
                call_id=verified["ticket_id"],
            ),
        )
