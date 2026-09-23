"""Generation-derived Authorization operations for package UnitOfWork plans."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from forest.errors import ForestError


def _object(value: object, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ForestError(
            "AUTHORIZATION_UOW_INPUT_INVALID",
            "Authorization participant input is invalid",
            422,
        )
    return value


def _datetime(value: object) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ForestError(
            "AUTHORIZATION_UOW_INPUT_INVALID",
            "Authorization expiry is invalid",
            422,
        )
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ForestError(
            "AUTHORIZATION_UOW_INPUT_INVALID",
            "Authorization expiry is invalid",
            422,
        ) from None
    if result.tzinfo is None:
        raise ForestError(
            "AUTHORIZATION_UOW_INPUT_INVALID",
            "Authorization expiry requires a timezone",
            422,
        )
    return result


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    return value


class KernelAuthorizationUnitOfWorkParticipant:
    """Issue tickets only from the caller Generation's sealed capability grant set."""

    component_id = "kernel.authorization"

    def __init__(self, runtime, service) -> None:
        self.runtime = runtime
        self.service = service

    @property
    def generation_id(self) -> str:
        identity = self.runtime.generation_identity
        if identity is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "Authorization Generation is not bound",
                503,
            )
        return identity.generation_id

    async def invoke_in(
        self,
        conn,
        name: str,
        payload: dict,
        **_invocation,
    ) -> dict:
        if name != "authorization.issue":
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_INPUT_INVALID",
                "Authorization participant operation is undeclared",
                422,
            )
        envelope = _object(payload, {"protocol", "caller", "input", "prior"})
        if envelope["protocol"] != "forest.uow-step/1":
            raise ForestError(
                "AUTHORIZATION_UOW_INPUT_INVALID",
                "UnitOfWork step protocol is invalid",
                422,
            )
        caller = _object(envelope["caller"], {"component_id", "generation_id"})
        request = _object(
            envelope["input"],
            {
                "protocol",
                "ticket_id",
                "issuance_operation_id",
                "operation_id",
                "action",
                "resource_kind",
                "resource_id",
                "input_digest",
                "plan_digest",
                "expires_at",
                "audience_component_id",
                "audience_generation_id",
            },
        )
        if request["protocol"] != "forest.authorization-issue/1":
            raise ForestError(
                "AUTHORIZATION_UOW_INPUT_INVALID",
                "Authorization issue protocol is invalid",
                422,
            )
        grant_digest = await self.runtime.service.generation_capability_grants_digest_in(
            conn,
            component_id=caller["component_id"],
            generation_id=caller["generation_id"],
        )
        grant_set = await self.service.grant_set_by_digest_in(
            conn,
            grantee_kind="component",
            grantee_id=caller["component_id"],
            grant_set_digest=grant_digest,
        )
        issued = await self.service.issue_ticket_in(
            conn,
            ticket_id=request["ticket_id"],
            issuance_operation_id=request["issuance_operation_id"],
            issuer_kind="component.policy",
            issuer_id=caller["component_id"],
            subject_kind="component",
            subject_id=caller["component_id"],
            action=request["action"],
            resource_kind=request["resource_kind"],
            resource_id=request["resource_id"],
            operation_id=request["operation_id"],
            input_digest=request["input_digest"],
            plan_digest=request["plan_digest"],
            grant_set=grant_set,
            expires_at=_datetime(request["expires_at"]),
            audience_component_id=request["audience_component_id"],
            audience_generation_id=request["audience_generation_id"],
        )
        return {
            "protocol": "forest.authorization-issued/1",
            "ticket": _json(issued),
        }
