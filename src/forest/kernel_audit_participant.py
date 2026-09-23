"""Generation-derived Audit append operation for package UnitOfWork plans."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.kernel_audit import AuditEnvelope


class KernelAuditUnitOfWorkParticipant:
    """Append one immutable envelope attributed to the exact caller Generation."""

    component_id = "kernel.audit"

    def __init__(self, runtime, service) -> None:
        self.runtime = runtime
        self.service = service

    @property
    def generation_id(self) -> str:
        identity = self.runtime.generation_identity
        if identity is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "Audit Generation is not bound",
                503,
            )
        return identity.generation_id

    async def invoke_in(self, conn, name: str, payload: dict, **_invocation) -> dict:
        if (
            name != "audit.append"
            or not isinstance(payload, dict)
            or set(payload) != {"protocol", "caller", "input", "prior"}
            or payload.get("protocol") != "forest.uow-step/1"
        ):
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_INPUT_INVALID",
                "Audit UnitOfWork operation is invalid",
                422,
            )
        caller = payload["caller"]
        value = payload["input"]
        fields = {
            "protocol",
            "audit_id",
            "action",
            "origin_kind",
            "severity",
            "actor_kind",
            "actor_id",
            "resource_kind",
            "resource_id",
            "operation_kind",
            "operation_id",
            "call_id",
            "details_payload_id",
            "details_payload_digest",
        }
        if (
            not isinstance(caller, dict)
            or set(caller) != {"component_id", "generation_id"}
            or not isinstance(value, dict)
            or set(value) != fields
            or value.get("protocol") != "forest.audit-append/1"
        ):
            raise ForestError(
                "AUDIT_UOW_INPUT_INVALID", "Audit append input is invalid", 422
            )
        saved = await self.service.append_in(
            conn,
            AuditEnvelope(
                audit_id=value["audit_id"],
                action=value["action"],
                producer_component_id=caller["component_id"],
                producer_generation_id=caller["generation_id"],
                origin_kind=value["origin_kind"],
                severity=value["severity"],
                actor_kind=value["actor_kind"],
                actor_id=value["actor_id"],
                resource_kind=value["resource_kind"],
                resource_id=value["resource_id"],
                operation_kind=value["operation_kind"],
                operation_id=value["operation_id"],
                call_id=value["call_id"],
                details_payload_id=value["details_payload_id"],
                details_payload_digest=value["details_payload_digest"],
            ),
        )
        return {"protocol": "forest.audit-appended/1", "record": saved}
