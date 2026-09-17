"""Clean authority for generic capability grants, tickets and revocation fences."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_SHA256_RE = re.compile(r"sha256:[a-f0-9]{64}\Z")


def _bounded(value, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("AUTHORIZATION_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _digest(value) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError(
            "AUTHORIZATION_INPUT_INVALID", "Authorization identity is not canonical", 422
        ) from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _validated_digest(value, field: str, *, optional: bool = False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ForestError("AUTHORIZATION_INPUT_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True, order=True)
class CapabilityGrant:
    capability: str
    resource_kind: str
    resource_id: str
    delegable: bool = False

    def validate(self) -> None:
        _bounded(self.capability, "capability", 200)
        _bounded(self.resource_kind, "resource_kind", 64)
        _bounded(self.resource_id, "resource_id", 512)
        if type(self.delegable) is not bool:
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Invalid delegable flag", 422)


@dataclass(frozen=True)
class GrantSetRef:
    grant_set_id: str
    grant_set_digest: str
    policy_revision: int
    entries: tuple[CapabilityGrant, ...]


class KernelAuthorizationRepository:
    """Persist exact grants and consume tickets against a monotonic policy fence."""

    def __init__(self, store, *, namespace: str | None = None) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.kernel.authorization")

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    async def _clock_in(self, conn, *, expected=None, update=False) -> int:
        row = await (
            await conn.execute(
                sql.SQL("SELECT policy_revision FROM {} WHERE bot_id=%s {}").format(
                    self._table("policy_clock"),
                    sql.SQL("FOR UPDATE" if update else "FOR SHARE"),
                ),
                (self.store.bot_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "AUTHORIZATION_AUTHORITY_MISSING", "Authorization clock is unavailable", 503
            )
        if expected is not None and (
            type(expected) is not int or expected < 1 or row["policy_revision"] != expected
        ):
            raise ForestError("POLICY_CONFLICT", "Authorization policy changed", 409)
        return row["policy_revision"]

    async def revision(self) -> int:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._clock_in(conn)

    @staticmethod
    def _entries(entries) -> tuple[CapabilityGrant, ...]:
        values = tuple(entries)
        if len(values) > 256 or any(type(item) is not CapabilityGrant for item in values):
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Invalid capability set", 422)
        for item in values:
            item.validate()
        unique = tuple(sorted(set(values)))
        if not unique:
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Capability set is empty", 422)
        return unique

    async def create_grant_set_in(
        self,
        conn,
        *,
        grantee_kind: str,
        grantee_id: str,
        entries,
        expected_policy_revision: int,
        audience_component_id: str | None = None,
        audience_generation_id: str | None = None,
        delegation_depth: int = 0,
        expires_at: datetime | None = None,
    ) -> GrantSetRef:
        _bounded(grantee_kind, "grantee_kind", 64)
        _bounded(grantee_id, "grantee_id", 256)
        if (audience_component_id is None) != (audience_generation_id is None):
            raise ForestError(
                "AUTHORIZATION_INPUT_INVALID", "Grant audience identity is incomplete", 422
            )
        _bounded(audience_component_id, "audience_component_id", 128, optional=True)
        _bounded(audience_generation_id, "audience_generation_id", 256, optional=True)
        if type(delegation_depth) is not int or not 0 <= delegation_depth <= 16:
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Invalid delegation depth", 422)
        if expires_at is not None and (
            not isinstance(expires_at, datetime) or expires_at.tzinfo is None
        ):
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Grant expiry needs a timezone", 422)
        grants = self._entries(entries)
        revision = await self._clock_in(conn, expected=expected_policy_revision)
        canonical = {
            "grantee_kind": grantee_kind,
            "grantee_id": grantee_id,
            "audience_component_id": audience_component_id,
            "audience_generation_id": audience_generation_id,
            "policy_revision": revision,
            "delegation_depth": delegation_depth,
            "expires_at": expires_at.astimezone(timezone.utc).isoformat()
            if expires_at
            else None,
            "entries": [asdict(item) for item in grants],
        }
        digest = _digest(canonical)
        grant_set_id = "grant:" + digest.removeprefix("sha256:")
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,grant_set_id,grant_set_digest,grantee_kind,"
                "grantee_id,audience_component_id,audience_generation_id,policy_revision,"
                "delegation_depth,expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING"
            ).format(self._table("grant_sets")),
            (
                self.store.bot_id,
                grant_set_id,
                digest,
                grantee_kind,
                grantee_id,
                audience_component_id,
                audience_generation_id,
                revision,
                delegation_depth,
                expires_at,
            ),
        )
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND grant_set_id=%s").format(
                    self._table("grant_sets")
                ),
                (self.store.bot_id, grant_set_id),
            )
        ).fetchone()
        expected = {
            "grant_set_digest": digest,
            "grantee_kind": grantee_kind,
            "grantee_id": grantee_id,
            "audience_component_id": audience_component_id,
            "audience_generation_id": audience_generation_id,
            "policy_revision": revision,
            "delegation_depth": delegation_depth,
            "expires_at": expires_at,
        }
        if existing is None or any(existing[key] != value for key, value in expected.items()):
            raise ForestError("GRANT_SET_ID_CONFLICT", "Grant set identity differs", 409)
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT capability,resource_kind,resource_id,delegable FROM {} "
                    "WHERE bot_id=%s AND grant_set_id=%s ORDER BY entry_index"
                ).format(self._table("grant_entries")),
                (self.store.bot_id, grant_set_id),
            )
        ).fetchall()
        if not rows:
            for index, grant in enumerate(grants):
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,grant_set_id,entry_index,capability,"
                        "resource_kind,resource_id,delegable) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    ).format(self._table("grant_entries")),
                    (
                        self.store.bot_id,
                        grant_set_id,
                        index,
                        grant.capability,
                        grant.resource_kind,
                        grant.resource_id,
                        grant.delegable,
                    ),
                )
        else:
            persisted = tuple(
                CapabilityGrant(
                    row["capability"],
                    row["resource_kind"],
                    row["resource_id"],
                    row["delegable"],
                )
                for row in rows
            )
            if persisted != grants:
                raise ForestError("GRANT_SET_ID_CONFLICT", "Grant set entries differ", 409)
        return GrantSetRef(grant_set_id, digest, revision, grants)

    async def create_grant_set(self, **values) -> GrantSetRef:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.create_grant_set_in(conn, **values)

    async def issue_ticket_in(
        self,
        conn,
        *,
        ticket_id: str,
        issuer_kind: str,
        issuer_id: str,
        subject_kind: str,
        subject_id: str,
        action: str,
        resource_kind: str,
        resource_id: str,
        operation_id: str,
        input_digest: str,
        plan_digest: str | None,
        grant_set: GrantSetRef,
        expires_at: datetime,
        audience_component_id: str | None = None,
        audience_generation_id: str | None = None,
    ) -> dict:
        for field, value, limit in (
            ("ticket_id", ticket_id, 256),
            ("issuer_kind", issuer_kind, 64),
            ("issuer_id", issuer_id, 256),
            ("subject_kind", subject_kind, 64),
            ("subject_id", subject_id, 256),
            ("action", action, 200),
            ("resource_kind", resource_kind, 64),
            ("resource_id", resource_id, 512),
            ("operation_id", operation_id, 256),
        ):
            _bounded(value, field, limit)
        _validated_digest(input_digest, "input_digest")
        _validated_digest(plan_digest, "plan_digest", optional=True)
        if (audience_component_id is None) != (audience_generation_id is None):
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Ticket audience is incomplete", 422)
        _bounded(audience_component_id, "audience_component_id", 128, optional=True)
        _bounded(audience_generation_id, "audience_generation_id", 256, optional=True)
        if not isinstance(expires_at, datetime) or expires_at.tzinfo is None:
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Ticket expiry needs a timezone", 422)
        replay = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND "
                    "(ticket_id=%s OR operation_id=%s)"
                ).format(self._table("tickets")),
                (self.store.bot_id, ticket_id, operation_id),
            )
        ).fetchone()
        replay_expected = {
            "ticket_id": ticket_id,
            "operation_id": operation_id,
            "issuer_kind": issuer_kind,
            "issuer_id": issuer_id,
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "action": action,
            "resource_kind": resource_kind,
            "resource_id": resource_id,
            "input_digest": input_digest,
            "plan_digest": plan_digest,
            "grant_set_id": grant_set.grant_set_id,
            "grant_set_digest": grant_set.grant_set_digest,
            "policy_revision": grant_set.policy_revision,
            "expires_at": expires_at,
        }
        if replay is not None:
            if any(replay[key] != value for key, value in replay_expected.items()) or (
                audience_component_id is not None
                and (
                    replay["audience_component_id"] != audience_component_id
                    or replay["audience_generation_id"] != audience_generation_id
                )
            ):
                raise ForestError("TICKET_ID_CONFLICT", "Ticket identity is already used", 409)
            return dict(replay)
        now = datetime.now(timezone.utc)
        if not now < expires_at <= now + timedelta(seconds=30):
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Ticket lifetime is invalid", 422)
        revision = await self._clock_in(conn, expected=grant_set.policy_revision)
        entry = await (
            await conn.execute(
                sql.SQL(
                    "SELECT g.*,e.delegable FROM {} g JOIN {} e USING (bot_id,grant_set_id) "
                    "WHERE g.bot_id=%s AND g.grant_set_id=%s AND e.capability=%s "
                    "AND e.resource_kind=%s AND e.resource_id=%s"
                ).format(self._table("grant_sets"), self._table("grant_entries")),
                (
                    self.store.bot_id,
                    grant_set.grant_set_id,
                    action,
                    resource_kind,
                    resource_id,
                ),
            )
        ).fetchone()
        if (
            entry is None
            or entry["grant_set_digest"] != grant_set.grant_set_digest
            or entry["policy_revision"] != revision
            or entry["grantee_kind"] != subject_kind
            or entry["grantee_id"] != subject_id
            or (entry["expires_at"] is not None and entry["expires_at"] <= now)
            or (entry["expires_at"] is not None and expires_at > entry["expires_at"])
        ):
            raise ForestError("CAPABILITY_DENIED", "Grant set does not permit this target", 403)
        grant_audience = (
            entry["audience_component_id"],
            entry["audience_generation_id"],
        )
        requested_audience = (audience_component_id, audience_generation_id)
        if grant_audience[0] is not None:
            if audience_component_id is not None and requested_audience != grant_audience:
                raise ForestError(
                    "CAPABILITY_DENIED", "Grant set is bound to another audience", 403
                )
            ticket_audience = grant_audience
        elif audience_component_id is not None:
            if not entry["delegable"]:
                raise ForestError(
                    "CAPABILITY_DENIED", "Capability cannot be delegated to an audience", 403
                )
            ticket_audience = requested_audience
        else:
            ticket_audience = (None, None)
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,ticket_id,issuer_kind,issuer_id,subject_kind,subject_id,"
                "audience_component_id,audience_generation_id,action,resource_kind,resource_id,operation_id,input_digest,plan_digest,"
                "grant_set_id,grant_set_digest,policy_revision,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING"
            ).format(self._table("tickets")),
            (
                self.store.bot_id,
                ticket_id,
                issuer_kind,
                issuer_id,
                subject_kind,
                subject_id,
                ticket_audience[0],
                ticket_audience[1],
                action,
                resource_kind,
                resource_id,
                operation_id,
                input_digest,
                plan_digest,
                grant_set.grant_set_id,
                grant_set.grant_set_digest,
                revision,
                expires_at,
            ),
        )
        ticket = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND ticket_id=%s").format(
                    self._table("tickets")
                ),
                (self.store.bot_id, ticket_id),
            )
        ).fetchone()
        expected = {
            "operation_id": operation_id,
            "issuer_kind": issuer_kind,
            "issuer_id": issuer_id,
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "audience_component_id": ticket_audience[0],
            "audience_generation_id": ticket_audience[1],
            "action": action,
            "resource_kind": resource_kind,
            "resource_id": resource_id,
            "input_digest": input_digest,
            "plan_digest": plan_digest,
            "grant_set_id": grant_set.grant_set_id,
            "grant_set_digest": grant_set.grant_set_digest,
            "policy_revision": revision,
            "expires_at": expires_at,
        }
        if ticket is None or any(ticket[key] != value for key, value in expected.items()):
            raise ForestError("TICKET_ID_CONFLICT", "Ticket identity is already used", 409)
        return dict(ticket)

    async def issue_ticket(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.issue_ticket_in(conn, **values)

    async def ticket(self, ticket_id: str) -> dict:
        _bounded(ticket_id, "ticket_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND ticket_id=%s").format(
                        self._table("tickets")
                    ),
                    (self.store.bot_id, ticket_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("TICKET_NOT_FOUND", "Authorization ticket is unavailable", 404)
        return dict(row)

    async def consume_ticket_in(
        self,
        conn,
        *,
        ticket_id: str,
        operation_id: str,
        action: str,
        resource_kind: str,
        resource_id: str,
        input_digest: str,
        plan_digest: str | None,
        audience_component_id: str | None = None,
        audience_generation_id: str | None = None,
    ) -> dict:
        for field, value, limit in (
            ("ticket_id", ticket_id, 256),
            ("operation_id", operation_id, 256),
            ("action", action, 200),
            ("resource_kind", resource_kind, 64),
            ("resource_id", resource_id, 512),
        ):
            _bounded(value, field, limit)
        if (audience_component_id is None) != (audience_generation_id is None):
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Ticket audience is incomplete", 422)
        _bounded(audience_component_id, "audience_component_id", 128, optional=True)
        _bounded(audience_generation_id, "audience_generation_id", 256, optional=True)
        _validated_digest(input_digest, "input_digest")
        _validated_digest(plan_digest, "plan_digest", optional=True)
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND ticket_id=%s").format(
                    self._table("ticket_consumptions")
                ),
                (self.store.bot_id, ticket_id),
            )
        ).fetchone()
        expected = {
            "operation_id": operation_id,
            "action": action,
            "resource_kind": resource_kind,
            "resource_id": resource_id,
            "audience_component_id": audience_component_id,
            "audience_generation_id": audience_generation_id,
            "input_digest": input_digest,
            "plan_digest": plan_digest,
        }
        if existing is not None:
            if any(existing[key] != value for key, value in expected.items()):
                raise ForestError("TICKET_CONSUMPTION_CONFLICT", "Ticket replay differs", 409)
            return dict(existing)
        ticket = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND ticket_id=%s FOR UPDATE").format(
                    self._table("tickets")
                ),
                (self.store.bot_id, ticket_id),
            )
        ).fetchone()
        if ticket is None:
            raise ForestError("TICKET_NOT_FOUND", "Authorization ticket is unavailable", 404)
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND ticket_id=%s").format(
                    self._table("ticket_consumptions")
                ),
                (self.store.bot_id, ticket_id),
            )
        ).fetchone()
        if existing is not None:
            if any(existing[key] != value for key, value in expected.items()):
                raise ForestError("TICKET_CONSUMPTION_CONFLICT", "Ticket replay differs", 409)
            return dict(existing)
        revision = await self._clock_in(conn, expected=ticket["policy_revision"])
        if (
            ticket["operation_id"] != operation_id
            or ticket["action"] != action
            or ticket["resource_kind"] != resource_kind
            or ticket["resource_id"] != resource_id
            or ticket["audience_component_id"] != audience_component_id
            or ticket["audience_generation_id"] != audience_generation_id
            or ticket["input_digest"] != input_digest
            or ticket["plan_digest"] != plan_digest
            or ticket["expires_at"] <= datetime.now(timezone.utc)
        ):
            raise ForestError("TICKET_REJECTED", "Authorization ticket is stale or differs", 403)
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,ticket_id,operation_id,action,resource_kind,resource_id,"
                    "audience_component_id,audience_generation_id,input_digest,plan_digest,policy_revision) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                ).format(self._table("ticket_consumptions")),
                (
                    self.store.bot_id,
                    ticket_id,
                    operation_id,
                    action,
                    resource_kind,
                    resource_id,
                    audience_component_id,
                    audience_generation_id,
                    input_digest,
                    plan_digest,
                    revision,
                ),
            )
        ).fetchone()
        return dict(row)

    async def consume_ticket(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.consume_ticket_in(conn, **values)

    @staticmethod
    def approval_decision_digest(*, approval_id: str, decision: str) -> str:
        _bounded(approval_id, "approval_id", 256)
        if decision not in {"APPROVED", "REJECTED"}:
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Invalid approval decision", 422)
        return _digest({"approval_id": approval_id, "decision": decision})

    async def request_approval_in(
        self,
        conn,
        *,
        approval_id: str,
        request_kind: str,
        request_id: str,
        action: str,
        input_digest: str,
        plan_digest: str | None,
        requester_kind: str,
        requester_id: str,
        approver_scope_kind: str,
        approver_scope_id: str,
        expected_policy_revision: int,
        expires_at: datetime,
    ) -> dict:
        for field, value, limit in (
            ("approval_id", approval_id, 256),
            ("request_kind", request_kind, 64),
            ("request_id", request_id, 256),
            ("action", action, 200),
            ("requester_kind", requester_kind, 64),
            ("requester_id", requester_id, 256),
            ("approver_scope_kind", approver_scope_kind, 64),
            ("approver_scope_id", approver_scope_id, 512),
        ):
            _bounded(value, field, limit)
        _validated_digest(input_digest, "input_digest")
        _validated_digest(plan_digest, "plan_digest", optional=True)
        if not isinstance(expires_at, datetime) or expires_at.tzinfo is None:
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Approval expiry needs a timezone", 422)
        now = datetime.now(timezone.utc)
        if not now < expires_at <= now + timedelta(hours=24):
            raise ForestError("AUTHORIZATION_INPUT_INVALID", "Approval lifetime is invalid", 422)
        expected = {
            "approval_id": approval_id,
            "request_kind": request_kind,
            "request_id": request_id,
            "action": action,
            "input_digest": input_digest,
            "plan_digest": plan_digest,
            "requester_kind": requester_kind,
            "requester_id": requester_id,
            "approver_scope_kind": approver_scope_kind,
            "approver_scope_id": approver_scope_id,
            "policy_revision": expected_policy_revision,
            "expires_at": expires_at,
        }
        replay = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND "
                    "(approval_id=%s OR (request_kind=%s AND request_id=%s))"
                ).format(self._table("approvals")),
                (self.store.bot_id, approval_id, request_kind, request_id),
            )
        ).fetchone()
        if replay is not None:
            if any(replay[key] != value for key, value in expected.items()):
                raise ForestError("APPROVAL_ID_CONFLICT", "Approval identity differs", 409)
            return dict(replay)
        revision = await self._clock_in(conn, expected=expected_policy_revision)
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,approval_id,request_kind,request_id,action,"
                "input_digest,plan_digest,requester_kind,requester_id,approver_scope_kind,"
                "approver_scope_id,policy_revision,expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING"
            ).format(self._table("approvals")),
            (
                self.store.bot_id,
                approval_id,
                request_kind,
                request_id,
                action,
                input_digest,
                plan_digest,
                requester_kind,
                requester_id,
                approver_scope_kind,
                approver_scope_id,
                revision,
                expires_at,
            ),
        )
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND "
                    "(approval_id=%s OR (request_kind=%s AND request_id=%s))"
                ).format(self._table("approvals")),
                (self.store.bot_id, approval_id, request_kind, request_id),
            )
        ).fetchone()
        expected["policy_revision"] = revision
        if row is None or any(row[key] != value for key, value in expected.items()):
            raise ForestError("APPROVAL_ID_CONFLICT", "Approval identity differs", 409)
        return dict(row)

    async def request_approval(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.request_approval_in(conn, **values)

    async def approval(self, approval_id: str) -> dict:
        _bounded(approval_id, "approval_id", 256)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND approval_id=%s").format(
                        self._table("approvals")
                    ),
                    (self.store.bot_id, approval_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("APPROVAL_NOT_FOUND", "Approval is unavailable", 404)
        return dict(row)

    async def decide_approval_in(
        self,
        conn,
        *,
        approval_id: str,
        decision: str,
        decided_by_kind: str,
        decided_by_id: str,
        decision_operation_id: str,
        authorization_ticket_id: str,
        audience_component_id: str | None = None,
        audience_generation_id: str | None = None,
    ) -> dict:
        for field, value, limit in (
            ("approval_id", approval_id, 256),
            ("decided_by_kind", decided_by_kind, 64),
            ("decided_by_id", decided_by_id, 256),
            ("decision_operation_id", decision_operation_id, 256),
            ("authorization_ticket_id", authorization_ticket_id, 256),
        ):
            _bounded(value, field, limit)
        decision_digest = self.approval_decision_digest(
            approval_id=approval_id, decision=decision
        )
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND approval_id=%s FOR UPDATE").format(
                    self._table("approvals")
                ),
                (self.store.bot_id, approval_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("APPROVAL_NOT_FOUND", "Approval is unavailable", 404)
        if row["state"] != "PENDING":
            if (
                (
                    row["state"] == decision
                    or (row["state"] == "CONSUMED" and decision == "APPROVED")
                )
                and row["decided_by_kind"] == decided_by_kind
                and row["decided_by_id"] == decided_by_id
                and row["decision_operation_id"] == decision_operation_id
            ):
                receipt = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT t.subject_kind,t.subject_id FROM {} c JOIN {} t "
                            "USING (bot_id,ticket_id) WHERE c.bot_id=%s AND c.ticket_id=%s "
                            "AND c.operation_id=%s"
                        ).format(
                            self._table("ticket_consumptions"), self._table("tickets")
                        ),
                        (self.store.bot_id, authorization_ticket_id, decision_operation_id),
                    )
                ).fetchone()
                if (
                    receipt is None
                    or receipt["subject_kind"] != decided_by_kind
                    or receipt["subject_id"] != decided_by_id
                ):
                    raise ForestError(
                        "APPROVAL_DECISION_CONFLICT", "Decision authorization differs", 409
                    )
                return dict(row)
            raise ForestError("APPROVAL_DECISION_CONFLICT", "Approval was already decided", 409)
        now = datetime.now(timezone.utc)
        if row["expires_at"] <= now:
            raise ForestError("APPROVAL_EXPIRED", "Approval has expired", 409)
        await self._clock_in(conn, expected=row["policy_revision"])
        await self.consume_ticket_in(
            conn,
            ticket_id=authorization_ticket_id,
            operation_id=decision_operation_id,
            action="approval.decide",
            resource_kind=row["approver_scope_kind"],
            resource_id=row["approver_scope_id"],
            input_digest=decision_digest,
            plan_digest=row["plan_digest"],
            audience_component_id=audience_component_id,
            audience_generation_id=audience_generation_id,
        )
        ticket = await (
            await conn.execute(
                sql.SQL("SELECT subject_kind,subject_id FROM {} WHERE bot_id=%s AND ticket_id=%s").format(
                    self._table("tickets")
                ),
                (self.store.bot_id, authorization_ticket_id),
            )
        ).fetchone()
        if (
            ticket is None
            or ticket["subject_kind"] != decided_by_kind
            or ticket["subject_id"] != decided_by_id
        ):
            raise ForestError("APPROVAL_PRINCIPAL_CONFLICT", "Decision principal differs", 403)
        decided = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,decided_by_kind=%s,decided_by_id=%s,"
                    "decision_operation_id=%s,decided_at=clock_timestamp() "
                    "WHERE bot_id=%s AND approval_id=%s RETURNING *"
                ).format(self._table("approvals")),
                (
                    decision,
                    decided_by_kind,
                    decided_by_id,
                    decision_operation_id,
                    self.store.bot_id,
                    approval_id,
                ),
            )
        ).fetchone()
        return dict(decided)

    async def decide_approval(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.decide_approval_in(conn, **values)

    async def consume_approval_in(
        self,
        conn,
        *,
        approval_id: str,
        request_kind: str,
        request_id: str,
        action: str,
        input_digest: str,
        plan_digest: str | None,
        requester_kind: str,
        requester_id: str,
        consume_operation_id: str,
    ) -> dict:
        for field, value, limit in (
            ("approval_id", approval_id, 256),
            ("request_kind", request_kind, 64),
            ("request_id", request_id, 256),
            ("action", action, 200),
            ("requester_kind", requester_kind, 64),
            ("requester_id", requester_id, 256),
            ("consume_operation_id", consume_operation_id, 256),
        ):
            _bounded(value, field, limit)
        _validated_digest(input_digest, "input_digest")
        _validated_digest(plan_digest, "plan_digest", optional=True)
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND approval_id=%s FOR UPDATE").format(
                    self._table("approvals")
                ),
                (self.store.bot_id, approval_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("APPROVAL_NOT_FOUND", "Approval is unavailable", 404)
        expected = {
            "request_kind": request_kind,
            "request_id": request_id,
            "action": action,
            "input_digest": input_digest,
            "plan_digest": plan_digest,
            "requester_kind": requester_kind,
            "requester_id": requester_id,
        }
        if any(row[key] != value for key, value in expected.items()):
            raise ForestError("APPROVAL_CONSUMPTION_CONFLICT", "Approval identity differs", 409)
        if row["state"] == "CONSUMED":
            if row["consume_operation_id"] == consume_operation_id:
                return dict(row)
            raise ForestError("APPROVAL_CONSUMPTION_CONFLICT", "Approval was already consumed", 409)
        if row["state"] != "APPROVED":
            raise ForestError("APPROVAL_NOT_APPROVED", "Approval is not approved", 409)
        if row["expires_at"] <= datetime.now(timezone.utc):
            raise ForestError("APPROVAL_EXPIRED", "Approval has expired", 409)
        await self._clock_in(conn, expected=row["policy_revision"])
        consumed = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='CONSUMED',consume_operation_id=%s,"
                    "consumed_at=clock_timestamp() WHERE bot_id=%s AND approval_id=%s RETURNING *"
                ).format(self._table("approvals")),
                (consume_operation_id, self.store.bot_id, approval_id),
            )
        ).fetchone()
        return dict(consumed)

    async def consume_approval(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.consume_approval_in(conn, **values)

    async def revoke_in(
        self,
        conn,
        *,
        operation_id: str,
        selector_kind: str,
        selector_id: str,
        expected_policy_revision: int,
    ) -> dict:
        _bounded(operation_id, "operation_id", 256)
        _bounded(selector_kind, "selector_kind", 64)
        _bounded(selector_id, "selector_id", 512)
        input_digest = _digest(
            [expected_policy_revision, selector_kind, selector_id]
        )
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                    self._table("revocations")
                ),
                (self.store.bot_id, operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("REVOCATION_ID_CONFLICT", "Revocation input differs", 409)
            return dict(existing)
        revision = await self._clock_in(conn, update=True)
        existing = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND operation_id=%s").format(
                    self._table("revocations")
                ),
                (self.store.bot_id, operation_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["input_digest"] != input_digest:
                raise ForestError("REVOCATION_ID_CONFLICT", "Revocation input differs", 409)
            return dict(existing)
        if revision != expected_policy_revision:
            raise ForestError("POLICY_CONFLICT", "Authorization policy changed", 409)
        row = await (
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,input_digest,previous_revision,"
                    "policy_revision,selector_kind,selector_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                ).format(self._table("revocations")),
                (
                    self.store.bot_id,
                    operation_id,
                    input_digest,
                    revision,
                    revision + 1,
                    selector_kind,
                    selector_id,
                ),
            )
        ).fetchone()
        await conn.execute(
            sql.SQL("UPDATE {} SET policy_revision=%s WHERE bot_id=%s").format(
                self._table("policy_clock")
            ),
            (revision + 1, self.store.bot_id),
        )
        return dict(row)

    async def revoke(self, **values) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.revoke_in(conn, **values)
