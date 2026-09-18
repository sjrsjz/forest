"""Caller-owned UnitOfWork for Adapter, Conversation and optional Shell ingress."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from forest.errors import ForestError
from forest.storage.adapter_onebot_authority import OneBotPlatformEvent
from forest.storage.conversation_scheduler import SchedulerPayloadRef
from forest.storage.memory_graph_authority import (
    MemoryOccurrence,
    MemoryPart,
    MemoryPayloadRef,
    MemorySourceIdentity,
)
from forest.storage.shell_command_authority import (
    ShellAdmissionProof,
    ShellPayloadRef,
)


@dataclass(frozen=True)
class ShellIngressCommand:
    request_id: str
    proof: ShellAdmissionProof
    command_id: str
    command_version: int
    arguments: ShellPayloadRef
    idempotency_key: str


@dataclass(frozen=True)
class IngressMemoryCapture:
    identity: MemorySourceIdentity
    source_unit_id: str
    source_revision: int
    unit_kind: str
    author: MemoryPayloadRef
    parts: tuple[MemoryPart, ...]
    occurred_at: datetime


@dataclass(frozen=True)
class IngressClassification:
    decision: str
    classifier_component_id: str
    classifier_generation_id: str
    rule_revision: str
    reason_code: str


class OneBotIngressUnitOfWork:
    """Coordinate participant ports without taking ownership of their facts."""

    def __init__(self, store, *, adapter, conversation, memory=None, shell=None) -> None:
        self.store = store
        self.adapter = adapter
        self.conversation = conversation
        self.memory = memory
        self.shell = shell

    @staticmethod
    def _scheduler_ref(reference) -> SchedulerPayloadRef:
        return SchedulerPayloadRef(
            reference.payload_id,
            reference.digest,
            reference.owner_component_id,
            reference.owner_kind,
            reference.owner_id,
        )

    @staticmethod
    def _guard_shell_identity(
        command: ShellIngressCommand,
        *,
        event: OneBotPlatformEvent,
        binding_id: str,
        expected_binding_epoch: int,
        client_incarnation: str,
    ) -> None:
        if type(command) is not ShellIngressCommand:
            raise ForestError("SHELL_INPUT_INVALID", "Invalid Shell ingress command", 422)
        proof = command.proof
        if (
            type(proof) is not ShellAdmissionProof
            or proof.binding_ref != binding_id
            or proof.binding_epoch != expected_binding_epoch
            or proof.client_incarnation != client_incarnation
            or proof.conversation_ref != event.conversation_ref
            or proof.event_ref != event.event_id
            or proof.principal_ref != event.author_subject_ref
        ):
            raise ForestError(
                "SHELL_PROOF_INVALID", "Shell proof differs from the authenticated event", 403
            )

    async def admit(
        self,
        *,
        operation_id: str,
        binding_id: str,
        expected_binding_epoch: int,
        client_incarnation: str,
        event: OneBotPlatformEvent,
        memory_capture: IngressMemoryCapture | None = None,
        classification: IngressClassification | None = None,
        shell_command: ShellIngressCommand | None = None,
    ) -> dict:
        if shell_command is not None:
            if self.shell is None:
                raise ForestError("SHELL_AUTHORITY_MISSING", "Shell participant is unavailable", 503)
            self._guard_shell_identity(
                shell_command,
                event=event,
                binding_id=binding_id,
                expected_binding_epoch=expected_binding_epoch,
                client_incarnation=client_incarnation,
            )
        scheduler_operation_id = "onebot-event:" + event.event_id
        scheduler_event_id = "onebot:" + event.event_id
        async with self.store.pool.connection() as conn, conn.transaction():
            adapter_result = await self.adapter.record_platform_event_in(
                conn,
                operation_id=operation_id,
                binding_id=binding_id,
                expected_binding_epoch=expected_binding_epoch,
                client_incarnation=client_incarnation,
                event=event,
            )
            conversation_result = await self.conversation.admit_message_in(
                conn,
                operation_id=scheduler_operation_id,
                conversation_id=event.conversation_ref,
                source_component_id="adapter.onebot",
                binding_ref=binding_id,
                platform="onebot-v11",
                channel_kind=event.channel_kind,
                channel_ref=event.channel_ref,
                thread_ref=None,
                event_id=scheduler_event_id,
                source_event_ref=event.event_id,
                event_kind=event.event_kind,
                platform_message_ref=event.platform_message_ref,
                author_subject_ref=event.author_subject_ref,
                content=self._scheduler_ref(event.normalized),
                author=self._scheduler_ref(event.author),
                sent_at=event.sent_at,
                received_at=event.received_at,
                classification_deadline=event.classification_deadline,
            )
            memory_result = None
            if memory_capture is not None and self.memory is not None:
                if type(memory_capture) is not IngressMemoryCapture:
                    raise ForestError(
                        "MEMORY_INPUT_INVALID", "Invalid ingress Memory capture", 422
                    )
                source = await self.memory.register_source_in(
                    conn,
                    operation_id="onebot-memory-source:" + memory_capture.identity.source_id,
                    identity=memory_capture.identity,
                )
                deletion_epoch = await self.memory.deletion_epoch_in(conn)
                captured = await self.memory.capture_source_unit_in(
                    conn,
                    operation_id="onebot-memory-capture:" + event.event_id,
                    source_unit_id=memory_capture.source_unit_id,
                    source_id=memory_capture.identity.source_id,
                    source_revision=memory_capture.source_revision,
                    unit_kind=memory_capture.unit_kind,
                    author_subject_ref=event.author_subject_ref,
                    author=memory_capture.author,
                    parts=memory_capture.parts,
                    occurrence=MemoryOccurrence(
                        occurrence_id="onebot-memory-occurrence:" + event.event_id,
                        binding_ref=binding_id,
                        conversation_ref=event.conversation_ref,
                        event_ref=scheduler_event_id,
                        ingress_seq=conversation_result["ingress_seq"],
                        observed_at=event.received_at,
                    ),
                    occurred_at=memory_capture.occurred_at,
                    expected_deletion_epoch=deletion_epoch,
                )
                memory_result = {"source": source, "capture": captured}
            shell_result = None
            classification_result = None
            if classification is not None:
                if type(classification) is not IngressClassification:
                    raise ForestError(
                        "CONVERSATION_INPUT_INVALID", "Invalid ingress classification", 422
                    )
                is_trigger = classification.decision == "TRIGGER"
                classification_result = await self.conversation.complete_classification_in(
                    conn,
                    operation_id="onebot-classification:" + event.event_id,
                    conversation_id=event.conversation_ref,
                    event_id=scheduler_event_id,
                    decision=classification.decision,
                    classifier_component_id=classification.classifier_component_id,
                    classifier_generation_id=classification.classifier_generation_id,
                    rule_revision=classification.rule_revision,
                    reason_code=classification.reason_code,
                    route_ref=None,
                    trigger_id=("onebot-trigger:" + event.event_id) if is_trigger else None,
                    principal_ref=event.author_subject_ref if is_trigger else None,
                )
            if shell_command is not None:
                shell_result = await self.shell.admit_command_in(
                    conn,
                    operation_id="onebot-shell:" + event.event_id,
                    request_id=shell_command.request_id,
                    proof=shell_command.proof,
                    command_id=shell_command.command_id,
                    command_version=shell_command.command_version,
                    arguments=shell_command.arguments,
                    idempotency_key=shell_command.idempotency_key,
                    received_at=event.received_at,
                    admission_deadline=event.classification_deadline,
                )
                classification_result = await self.conversation.complete_classification_in(
                    conn,
                    operation_id="onebot-shell-classification:" + event.event_id,
                    conversation_id=event.conversation_ref,
                    event_id=scheduler_event_id,
                    decision="CONTROL_COMMAND",
                    classifier_component_id="shell.command",
                    classifier_generation_id=(
                        shell_result["owner_generation_id"] or "shell.command/builtin"
                    ),
                    rule_revision="shell-policy:" + str(shell_result["policy_revision"]),
                    reason_code=shell_result["rejection_code"],
                    route_ref=shell_result["request_id"],
                )
            return {
                "adapter": adapter_result,
                "conversation": conversation_result,
                "memory": memory_result,
                "shell": shell_result,
                "classification": classification_result,
            }
