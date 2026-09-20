"""Short-lived signed delegation from deployment Web identity to a target scope."""

from __future__ import annotations

import hmac
import inspect
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import random_token

_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")
_ROLE = {"ADMIN", "AUDITOR"}


def _text(value: object, field_name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ForestError("WEB_AUTHORITY_TICKET_INVALID", f"Invalid {field_name}", 403)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= maximum or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("WEB_AUTHORITY_TICKET_INVALID", f"Invalid {field_name}", 403)
    return value


def _digest(value: object, field_name: str, *, optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ForestError("WEB_AUTHORITY_TICKET_INVALID", f"Invalid {field_name}", 403)
    return value


@dataclass(frozen=True)
class WebAuthorityTicket:
    protocol: str
    ticket_id: str
    issuer_id: str
    key_id: str
    web_subject_ref: str
    web_role: str
    session_ref: str
    session_revision: int
    target_scope_kind: str
    target_scope_id: str
    target_component_id: str
    target_generation_id: str
    target_graph_revision: int
    target_operation_ref: str
    action_key: str
    input_digest: str
    plan_digest: str | None
    issued_at: datetime
    expires_at: datetime
    signature: str = field(repr=False)

    def payload(self) -> dict:
        value = asdict(self)
        value.pop("signature")
        value["issued_at"] = self.issued_at.astimezone(timezone.utc).isoformat()
        value["expires_at"] = self.expires_at.astimezone(timezone.utc).isoformat()
        return value


class WebAuthorityTicketAuthority:
    protocol = "forest.web-authority-ticket/1"

    def __init__(
        self,
        *,
        issuer_id: str,
        active_key_id: str,
        key_resolver,
        session_validator=None,
    ) -> None:
        self.issuer_id = _text(issuer_id, "issuer_id", 128)
        self.active_key_id = _text(active_key_id, "key_id", 256)
        self.key_resolver = key_resolver
        self.session_validator = session_validator

    async def issue(
        self,
        *,
        session,
        target_scope_kind: str,
        target_scope_id: str,
        target_component_id: str,
        target_generation_id: str,
        target_graph_revision: int,
        target_operation_ref: str,
        action_key: str,
        input_digest: str,
        plan_digest: str | None = None,
        ttl_seconds: int = 30,
        now: datetime | None = None,
    ) -> WebAuthorityTicket:
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 30:
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid ticket lifetime", 403)
        subject = getattr(session, "subject", None)
        role = getattr(subject, "role", None)
        session_revision = getattr(session, "session_revision", None)
        if role not in _ROLE or type(session_revision) is not int or session_revision < 1:
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid Web session", 403)
        if self.session_validator is not None:
            checked = self.session_validator(session)
            if inspect.isawaitable(checked):
                await checked
        issued_at = now or datetime.now(timezone.utc)
        if not isinstance(issued_at, datetime) or issued_at.tzinfo is None:
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid issue time", 403)
        ticket = WebAuthorityTicket(
            self.protocol,
            "web-ticket/" + random_token(),
            self.issuer_id,
            self.active_key_id,
            _text(getattr(subject, "subject_id", None), "web_subject_ref", 128),
            role,
            _text(getattr(session, "session_ref", None), "session_ref", 128),
            session_revision,
            _text(target_scope_kind, "target_scope_kind", 64),
            _text(target_scope_id, "target_scope_id", 128),
            _text(target_component_id, "target_component_id", 128),
            _text(target_generation_id, "target_generation_id", 256),
            target_graph_revision,
            _text(target_operation_ref, "target_operation_ref", 256),
            _text(action_key, "action_key", 200),
            _digest(input_digest, "input_digest"),
            _digest(plan_digest, "plan_digest", optional=True),
            issued_at.astimezone(timezone.utc),
            (issued_at + timedelta(seconds=ttl_seconds)).astimezone(timezone.utc),
            "hmac-sha256:" + "0" * 64,
        )
        self._validate_shape(ticket)
        signature = await self._signature(ticket, self.active_key_id)
        return WebAuthorityTicket(**{**asdict(ticket), "signature": signature})

    async def verify(
        self,
        ticket: WebAuthorityTicket,
        *,
        target_scope_kind: str,
        target_scope_id: str,
        target_component_id: str,
        target_generation_id: str,
        target_graph_revision: int,
        target_operation_ref: str,
        action_key: str,
        input_digest: str,
        plan_digest: str | None = None,
        required_role: str,
        now: datetime | None = None,
    ) -> None:
        self._validate_shape(ticket)
        if required_role not in _ROLE:
            raise ValueError("Required Web management role is invalid")
        expected = {
            "target_scope_kind": target_scope_kind,
            "target_scope_id": target_scope_id,
            "target_component_id": target_component_id,
            "target_generation_id": target_generation_id,
            "target_graph_revision": target_graph_revision,
            "target_operation_ref": target_operation_ref,
            "action_key": action_key,
            "input_digest": input_digest,
            "plan_digest": plan_digest,
        }
        if any(getattr(ticket, key) != value for key, value in expected.items()):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_TARGET_MISMATCH",
                "Web authority ticket does not match the target operation",
                403,
            )
        current = now or datetime.now(timezone.utc)
        if not isinstance(current, datetime) or current.tzinfo is None:
            raise ValueError("Ticket verification time requires a timezone")
        current = current.astimezone(timezone.utc)
        if (
            ticket.issued_at > current + timedelta(seconds=5)
            or ticket.expires_at <= current
            or ticket.expires_at > ticket.issued_at + timedelta(seconds=30)
        ):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_EXPIRED", "Web authority ticket has expired", 403
            )
        expected_signature = await self._signature(ticket, ticket.key_id)
        if not hmac.compare_digest(ticket.signature, expected_signature):
            raise ForestError(
                "WEB_AUTHORITY_TICKET_SIGNATURE_INVALID",
                "Web authority ticket signature is invalid",
                403,
            )
        if required_role == "ADMIN" and ticket.web_role != "ADMIN":
            raise ForestError(
                "MANAGEMENT_AUTHORIZATION_DENIED",
                "Administrator role is required",
                403,
            )

    async def _signature(self, ticket: WebAuthorityTicket, key_id: str) -> str:
        key = self.key_resolver(key_id)
        if inspect.isawaitable(key):
            key = await key
        if not isinstance(key, bytes) or len(key) != 32:
            raise ForestError(
                "WEB_AUTHORITY_TICKET_KEY_UNAVAILABLE",
                "Web authority ticket key is unavailable",
                503,
            )
        message = digest_json(ticket.payload()).encode("ascii")
        return "hmac-sha256:" + hmac.digest(key, message, "sha256").hex()

    def _validate_shape(self, ticket: object) -> None:
        if not isinstance(ticket, WebAuthorityTicket) or ticket.protocol != self.protocol:
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid Web authority ticket", 403)
        for field_name, maximum in (
            ("ticket_id", 256),
            ("issuer_id", 128),
            ("key_id", 256),
            ("web_subject_ref", 128),
            ("session_ref", 128),
            ("target_scope_kind", 64),
            ("target_scope_id", 128),
            ("target_component_id", 128),
            ("target_generation_id", 256),
            ("target_operation_ref", 256),
            ("action_key", 200),
        ):
            _text(getattr(ticket, field_name), field_name, maximum)
        if ticket.issuer_id != self.issuer_id or ticket.web_role not in _ROLE:
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid ticket issuer", 403)
        if (
            type(ticket.session_revision) is not int
            or ticket.session_revision < 1
            or type(ticket.target_graph_revision) is not int
            or ticket.target_graph_revision < 1
            or not isinstance(ticket.issued_at, datetime)
            or ticket.issued_at.tzinfo is None
            or not isinstance(ticket.expires_at, datetime)
            or ticket.expires_at.tzinfo is None
        ):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid ticket revision", 403)
        _digest(ticket.input_digest, "input_digest")
        _digest(ticket.plan_digest, "plan_digest", optional=True)
        if not isinstance(ticket.signature, str) or not ticket.signature.startswith("hmac-sha256:"):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Invalid ticket signature", 403)
