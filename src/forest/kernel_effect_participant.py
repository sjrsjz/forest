"""Kernel Effect authority exposed as a narrow RuntimeGraph UnitOfWork participant."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from forest.errors import ForestError
from forest.storage.effect_authority import EffectPermit


def _object(value: object, fields: set[str], code: str) -> dict:
    if not isinstance(value, dict) or set(value) != fields:
        raise ForestError(code, "Kernel Effect participant input is invalid", 422)
    return value


def _optional_payload(value: object, code: str) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    payload = _object(value, {"payload_id", "digest"}, code)
    return payload["payload_id"], payload["digest"]


def _datetime(value: object, code: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ForestError(code, "Kernel Effect deadline is invalid", 422)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ForestError(code, "Kernel Effect deadline is invalid", 422) from None
    if parsed.tzinfo is None:
        raise ForestError(code, "Kernel Effect deadline requires a timezone", 422)
    return parsed


def _permit(value: object, bot_id: str) -> EffectPermit:
    permit = _object(
        value,
        {
            "protocol",
            "effect_operation_id",
            "attempt_id",
            "attempt_no",
            "wire_payload_id",
            "wire_payload_digest",
            "executor_component_id",
            "executor_generation_id",
            "fence_kind",
            "fence_id",
            "fence_epoch",
            "deadline_at",
        },
        "EFFECT_UOW_INPUT_INVALID",
    )
    if permit["protocol"] != "forest.effect-permit/1":
        raise ForestError(
            "EFFECT_UOW_INPUT_INVALID", "Kernel Effect permit protocol is invalid", 422
        )
    return EffectPermit(
        bot_id,
        permit["effect_operation_id"],
        permit["attempt_id"],
        permit["attempt_no"],
        permit["wire_payload_id"],
        permit["wire_payload_digest"],
        permit["executor_component_id"],
        permit["executor_generation_id"],
        permit["fence_kind"],
        permit["fence_id"],
        permit["fence_epoch"],
        _datetime(permit["deadline_at"], "EFFECT_UOW_INPUT_INVALID"),
    )


def _permit_value(permit: EffectPermit) -> dict:
    return {
        "protocol": "forest.effect-permit/1",
        "effect_operation_id": permit.effect_operation_id,
        "attempt_id": permit.attempt_id,
        "attempt_no": permit.attempt_no,
        "wire_payload_id": permit.wire_payload_id,
        "wire_payload_digest": permit.wire_payload_digest,
        "executor_component_id": permit.executor_component_id,
        "executor_generation_id": permit.executor_generation_id,
        "fence_kind": permit.fence_kind,
        "fence_id": permit.fence_id,
        "fence_epoch": permit.fence_epoch,
        "deadline_at": permit.deadline_at.isoformat(),
    }


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


class KernelEffectUnitOfWorkParticipant:
    """Translate stable Effect DTOs without understanding the caller's business plan."""

    component_id = "kernel.effect"

    def __init__(self, runtime) -> None:
        self.runtime = runtime

    @property
    def generation_id(self) -> str:
        identity = self.runtime.generation_identity
        if identity is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "Kernel Effect Generation is not bound",
                503,
            )
        return identity.generation_id

    async def invoke_in(
        self,
        conn,
        name: str,
        payload: dict,
        *,
        operation_id: str,
        causal_root_kind: str,
        causal_root_id: str,
        parent_call_id: str,
        timeout_seconds: float,
    ) -> Any:
        del operation_id, parent_call_id, timeout_seconds
        envelope = _object(
            payload,
            {"protocol", "caller", "input", "prior"},
            "EFFECT_UOW_INPUT_INVALID",
        )
        if envelope["protocol"] != "forest.uow-step/1":
            raise ForestError(
                "EFFECT_UOW_INPUT_INVALID", "UnitOfWork step protocol is invalid", 422
            )
        caller = _object(
            envelope["caller"],
            {"component_id", "generation_id"},
            "EFFECT_UOW_INPUT_INVALID",
        )
        value = envelope["input"]
        if name == "effect.prepare":
            return await self._prepare(
                conn,
                value,
                caller=caller,
                causal_root_kind=causal_root_kind,
                causal_root_id=causal_root_id,
            )
        if name == "effect.handoff":
            return await self._handoff(conn, value)
        if name == "effect.observe":
            return await self._observe(conn, value)
        if name == "effect.abandon":
            return await self._abandon(conn, value)
        raise ForestError(
            "RUNTIME_UOW_PARTICIPANT_INPUT_INVALID",
            "Kernel Effect participant operation is undeclared",
            422,
        )

    async def _prepare(self, conn, value: object, *, caller, causal_root_kind, causal_root_id):
        request = _object(
            value,
            {
                "protocol",
                "effect_operation_id",
                "input_digest",
                "effect_kind",
                "target_kind",
                "target_id",
                "input_payload",
                "attempt_id",
                "permit_operation_id",
                "wire_payload",
                "authorization",
                "executor",
                "deadline_at",
            },
            "EFFECT_UOW_INPUT_INVALID",
        )
        if request["protocol"] != "forest.effect-prepare/1":
            raise ForestError("EFFECT_UOW_INPUT_INVALID", "Effect protocol is invalid", 422)
        input_payload_id, input_payload_digest = _optional_payload(
            request["input_payload"], "EFFECT_UOW_INPUT_INVALID"
        )
        wire = _object(
            request["wire_payload"],
            {"payload_id", "digest"},
            "EFFECT_UOW_INPUT_INVALID",
        )
        authorization = _object(
            request["authorization"],
            {"ticket_id", "action", "resource_kind", "resource_id", "plan_digest"},
            "EFFECT_UOW_INPUT_INVALID",
        )
        executor = _object(
            request["executor"],
            {
                "component_id",
                "generation_id",
                "fence_kind",
                "fence_id",
                "fence_epoch",
            },
            "EFFECT_UOW_INPUT_INVALID",
        )
        await self.runtime.service.admit_in(
            conn,
            effect_operation_id=request["effect_operation_id"],
            input_digest=request["input_digest"],
            effect_kind=request["effect_kind"],
            target_kind=request["target_kind"],
            target_id=request["target_id"],
            producer_component_id=caller["component_id"],
            producer_generation_id=caller["generation_id"],
            root_operation_kind=causal_root_kind,
            root_operation_id=causal_root_id,
            input_payload_id=input_payload_id,
            input_payload_digest=input_payload_digest,
        )
        permit = await self.runtime.service.permit_in(
            conn,
            effect_operation_id=request["effect_operation_id"],
            attempt_id=request["attempt_id"],
            permit_operation_id=request["permit_operation_id"],
            wire_payload_id=wire["payload_id"],
            wire_payload_digest=wire["digest"],
            authorization_ticket_id=authorization["ticket_id"],
            authorization_action=authorization["action"],
            authorization_resource_kind=authorization["resource_kind"],
            authorization_resource_id=authorization["resource_id"],
            authorization_plan_digest=authorization["plan_digest"],
            executor_component_id=executor["component_id"],
            executor_generation_id=executor["generation_id"],
            fence_kind=executor["fence_kind"],
            fence_id=executor["fence_id"],
            fence_epoch=executor["fence_epoch"],
            deadline_at=_datetime(request["deadline_at"], "EFFECT_UOW_INPUT_INVALID"),
        )
        return _permit_value(permit)

    async def _handoff(self, conn, value: object) -> dict:
        request = _object(
            value,
            {"protocol", "permit", "handoff_operation_id"},
            "EFFECT_UOW_INPUT_INVALID",
        )
        if request["protocol"] != "forest.effect-handoff/1":
            raise ForestError("EFFECT_UOW_INPUT_INVALID", "Effect protocol is invalid", 422)
        permit = _permit(request["permit"], self.runtime.service.store.bot_id)
        return _json_value(
            await self.runtime.service.mark_handoff_in(
                conn,
                permit=permit,
                handoff_operation_id=request["handoff_operation_id"],
            )
        )

    async def _observe(self, conn, value: object) -> dict:
        request = _object(
            value,
            {
                "protocol",
                "effect_operation_id",
                "attempt_id",
                "observation_operation_id",
                "status",
                "evidence_kind",
                "evidence_payload",
            },
            "EFFECT_UOW_INPUT_INVALID",
        )
        if request["protocol"] != "forest.effect-observe/1":
            raise ForestError("EFFECT_UOW_INPUT_INVALID", "Effect protocol is invalid", 422)
        evidence_id, evidence_digest = _optional_payload(
            request["evidence_payload"], "EFFECT_UOW_INPUT_INVALID"
        )
        return _json_value(
            await self.runtime.service.observe_in(
                conn,
                effect_operation_id=request["effect_operation_id"],
                attempt_id=request["attempt_id"],
                observation_operation_id=request["observation_operation_id"],
                status=request["status"],
                evidence_kind=request["evidence_kind"],
                evidence_payload_id=evidence_id,
                evidence_payload_digest=evidence_digest,
            )
        )

    async def _abandon(self, conn, value: object) -> dict:
        request = _object(
            value,
            {
                "protocol",
                "effect_operation_id",
                "attempt_id",
                "abandonment_operation_id",
                "reason_code",
                "cancel_effect",
            },
            "EFFECT_UOW_INPUT_INVALID",
        )
        if request["protocol"] != "forest.effect-abandon/1":
            raise ForestError("EFFECT_UOW_INPUT_INVALID", "Effect protocol is invalid", 422)
        return _json_value(
            await self.runtime.service.abandon_prepared_in(
                conn,
                effect_operation_id=request["effect_operation_id"],
                attempt_id=request["attempt_id"],
                abandonment_operation_id=request["abandonment_operation_id"],
                reason_code=request["reason_code"],
                cancel_effect=request["cancel_effect"],
            )
        )
