"""Generation-derived Authorization operations for package UnitOfWork plans."""

from __future__ import annotations

from datetime import UTC, datetime
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
    """Issue and consume tickets for an exact caller Generation."""

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

    async def _caller_grant_set(self, conn, caller: dict[str, str]):
        grant_digest = await self.runtime.generation_authority.generation_capability_grants_digest_in(
            conn,
            component_id=caller["component_id"],
            generation_id=caller["generation_id"],
        )
        return await self.service.grant_set_by_digest_in(
            conn,
            grantee_kind="component",
            grantee_id=caller["component_id"],
            grant_set_digest=grant_digest,
        )

    @staticmethod
    def _approval_state(row: dict) -> dict:
        state = row["state"]
        expires_at = row["expires_at"]
        if isinstance(expires_at, str):
            expires_at = _datetime(expires_at)
        if state == "PENDING" and expires_at <= datetime.now(UTC):
            state = "EXPIRED"
        return {
            "protocol": "forest.approval-state/1",
            "approval_id": row["approval_id"],
            "request_kind": row["request_kind"],
            "request_id": row["request_id"],
            "action": row["action"],
            "input_digest": row["input_digest"],
            "plan_digest": row["plan_digest"],
            "state": state,
            "expires_at": _json(expires_at),
        }

    @staticmethod
    def _approval_request(value: object, *, consume: bool = False) -> dict[str, Any]:
        fields = {
            "protocol",
            "approval_id",
            "request_kind",
            "request_id",
            "action",
            "input_digest",
            "plan_digest",
        }
        if consume:
            fields.add("consume_operation_id")
        request = _object(value, fields)
        expected_protocol = (
            "forest.approval-consume/1" if consume else "forest.approval-inspect/1"
        )
        if request["protocol"] != expected_protocol:
            raise ForestError(
                "AUTHORIZATION_UOW_INPUT_INVALID",
                "Approval protocol is invalid",
                422,
            )
        return request

    @staticmethod
    def _assert_approval_identity(row: dict, request: dict, caller: dict) -> None:
        expected = {
            "approval_id": request["approval_id"],
            "request_kind": request["request_kind"],
            "request_id": request["request_id"],
            "action": request["action"],
            "input_digest": request["input_digest"],
            "plan_digest": request["plan_digest"],
            "requester_kind": "component",
            "requester_id": caller["component_id"],
        }
        if any(row.get(key) != value for key, value in expected.items()):
            raise ForestError(
                "APPROVAL_IDENTITY_CONFLICT",
                "Approval identity differs from the caller request",
                409,
            )

    async def invoke_in(
        self,
        conn,
        name: str,
        payload: dict,
        **_invocation,
    ) -> dict:
        if name not in {
            "authorization.issue",
            "authorization.consume",
            "authorization.approval.request",
            "authorization.approval.inspect",
            "authorization.approval.consume",
        }:
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
        if name == "authorization.approval.request":
            request = _object(
                envelope["input"],
                {
                    "protocol",
                    "approval_id",
                    "request_kind",
                    "request_id",
                    "action",
                    "input_digest",
                    "plan_digest",
                    "expires_at",
                },
            )
            if request["protocol"] != "forest.approval-request/1":
                raise ForestError(
                    "AUTHORIZATION_UOW_INPUT_INVALID",
                    "Approval request protocol is invalid",
                    422,
                )
            grant_set = await self._caller_grant_set(conn, caller)
            row = await self.service.request_approval_in(
                conn,
                approval_id=request["approval_id"],
                request_kind=request["request_kind"],
                request_id=request["request_id"],
                action=request["action"],
                input_digest=request["input_digest"],
                plan_digest=request["plan_digest"],
                requester_kind="component",
                requester_id=caller["component_id"],
                approver_scope_kind="bot",
                approver_scope_id=self.service.store.bot_id,
                expected_policy_revision=grant_set.policy_revision,
                expires_at=_datetime(request["expires_at"]),
            )
            return self._approval_state(row)
        if name == "authorization.approval.inspect":
            request = self._approval_request(envelope["input"])
            row = await self.service.approval_in(
                conn, request["approval_id"], update=False
            )
            self._assert_approval_identity(row, request, caller)
            return self._approval_state(row)
        if name == "authorization.approval.consume":
            request = self._approval_request(envelope["input"], consume=True)
            row = await self.service.consume_approval_in(
                conn,
                approval_id=request["approval_id"],
                request_kind=request["request_kind"],
                request_id=request["request_id"],
                action=request["action"],
                input_digest=request["input_digest"],
                plan_digest=request["plan_digest"],
                requester_kind="component",
                requester_id=caller["component_id"],
                consume_operation_id=request["consume_operation_id"],
            )
            return self._approval_state(row)
        if name == "authorization.consume":
            request = _object(
                envelope["input"],
                {
                    "protocol",
                    "ticket_id",
                    "operation_id",
                    "action",
                    "resource_kind",
                    "resource_id",
                    "input_digest",
                    "plan_digest",
                    "audience_component_id",
                    "audience_generation_id",
                },
            )
            if request["protocol"] != "forest.authorization-consume/1":
                raise ForestError(
                    "AUTHORIZATION_UOW_INPUT_INVALID",
                    "Authorization consume protocol is invalid",
                    422,
                )
            if (
                request["audience_component_id"] != caller["component_id"]
                or request["audience_generation_id"] != caller["generation_id"]
            ):
                raise ForestError(
                    "AUTHORIZATION_UOW_INPUT_INVALID",
                    "Authorization ticket audience differs from the caller Generation",
                    422,
                )
            consumed = await self.service.consume_ticket_in(
                conn,
                ticket_id=request["ticket_id"],
                operation_id=request["operation_id"],
                action=request["action"],
                resource_kind=request["resource_kind"],
                resource_id=request["resource_id"],
                input_digest=request["input_digest"],
                plan_digest=request["plan_digest"],
                audience_component_id=request["audience_component_id"],
                audience_generation_id=request["audience_generation_id"],
            )
            return {
                "protocol": "forest.authorization-consumed/1",
                "consumption": _json(consumed),
            }
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
        grant_set = await self._caller_grant_set(conn, caller)
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
