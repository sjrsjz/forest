"""Repository contract for the clean ``kernel.audit`` schema bundle."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]*\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SEVERITIES = frozenset({"INFO", "WARNING", "ERROR", "SECURITY"})


def _identity(value: str | None, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or _IDENTITY.fullmatch(value) is None
    ):
        raise ForestError("AUDIT_ENVELOPE_INVALID", f"Invalid {field}", 422)
    return value


def _opaque(value: str | None, field: str, limit: int, *, optional: bool = False):
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("AUDIT_ENVELOPE_INVALID", f"Invalid {field}", 422)
    return value


def _pair(kind, identity, label, *, identity_limit=256):
    if (kind is None) != (identity is None):
        raise ForestError(
            "AUDIT_ENVELOPE_INVALID", f"{label} kind and identity must appear together", 422
        )
    if kind is not None:
        _identity(kind, f"{label}_kind", 64)
        _opaque(identity, f"{label}_id", identity_limit)


@dataclass(frozen=True)
class AuditEnvelope:
    audit_id: str
    action: str
    producer_component_id: str
    origin_kind: str
    severity: str = "INFO"
    producer_generation_id: str | None = None
    actor_kind: str | None = None
    actor_id: str | None = None
    resource_kind: str | None = None
    resource_id: str | None = None
    operation_kind: str | None = None
    operation_id: str | None = None
    call_id: str | None = None
    details_payload_id: str | None = None
    details_payload_digest: str | None = None

    def validate(self) -> None:
        _identity(self.audit_id, "audit_id", 128)
        _identity(self.action, "action", 128)
        _identity(self.producer_component_id, "producer_component_id", 128)
        _identity(self.origin_kind, "origin_kind", 64)
        if self.severity not in _SEVERITIES:
            raise ForestError("AUDIT_ENVELOPE_INVALID", "Invalid severity", 422)
        _opaque(
            self.producer_generation_id,
            "producer_generation_id",
            256,
            optional=True,
        )
        _pair(self.actor_kind, self.actor_id, "actor")
        _pair(self.resource_kind, self.resource_id, "resource", identity_limit=512)
        _pair(self.operation_kind, self.operation_id, "operation")
        _opaque(self.call_id, "call_id", 256, optional=True)
        if (self.details_payload_id is None) != (self.details_payload_digest is None):
            raise ForestError(
                "AUDIT_ENVELOPE_INVALID",
                "Audit details payload identity is incomplete",
                422,
            )
        if self.details_payload_id is not None:
            _opaque(self.details_payload_id, "details_payload_id", 256)
            if (
                not isinstance(self.details_payload_digest, str)
                or _DIGEST.fullmatch(self.details_payload_digest) is None
            ):
                raise ForestError(
                    "AUDIT_ENVELOPE_INVALID", "Invalid details payload digest", 422
                )


class KernelAuditRepository:
    """Append and scan redacted audit envelopes without hydrating payload bodies."""

    _COLUMNS = tuple(AuditEnvelope.__dataclass_fields__)

    def __init__(
        self,
        store,
        *,
        namespace: str | None = None,
        payload_verifier=None,
    ) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.kernel.audit")
        self.payload_verifier = payload_verifier

    def _table(self):
        return sql.SQL("{}.records").format(sql.Identifier(self.namespace))

    async def append(self, envelope: AuditEnvelope) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self.append_in(conn, envelope)

    async def append_in(self, conn, envelope: AuditEnvelope) -> dict:
        envelope.validate()
        if envelope.details_payload_id is not None:
            if self.payload_verifier is None:
                raise ForestError(
                    "AUDIT_PAYLOAD_PORT_REQUIRED",
                    "Audit details require the kernel.payload verification port",
                    503,
                )
            await self.payload_verifier(
                conn,
                payload_id=envelope.details_payload_id,
                digest=envelope.details_payload_digest,
                owner_component_id="kernel.audit",
                owner_kind="audit.details",
                owner_id=envelope.audit_id,
            )
        values = asdict(envelope)
        columns = ("bot_id", *self._COLUMNS)
        column_sql = sql.SQL(",").join(map(sql.Identifier, columns))
        placeholders = sql.SQL(",").join(sql.Placeholder() for _ in columns)
        await conn.execute(
            sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
                self._table(), column_sql, placeholders
            ),
            (self.store.bot_id, *(values[column] for column in self._COLUMNS)),
        )
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND audit_id=%s").format(
                    self._table()
                ),
                (self.store.bot_id, envelope.audit_id),
            )
        ).fetchone()
        if row is None:  # pragma: no cover - protected by the same transaction
            raise ForestError("AUDIT_APPEND_FAILED", "Audit record was not stored", 503)
        if any(row[column] != values[column] for column in self._COLUMNS):
            raise ForestError(
                "AUDIT_ID_CONFLICT",
                "Audit identity already belongs to another immutable envelope",
                409,
            )
        return self._project(row)

    async def scan(
        self,
        *,
        action: str | None = None,
        producer_component_id: str | None = None,
        origin_kind: str | None = None,
        resource_kind: str | None = None,
        resource_id: str | None = None,
        operation_kind: str | None = None,
        operation_id: str | None = None,
        before: str | None = None,
        limit: int = 50,
    ) -> dict:
        """Return one stable metadata page; payload bodies remain behind ``kernel.payload``."""

        filters = {
            "action": (action, 128),
            "producer_component_id": (producer_component_id, 128),
            "origin_kind": (origin_kind, 64),
        }
        for field, (value, size) in filters.items():
            _identity(value, field, size, optional=True)
        _pair(resource_kind, resource_id, "resource", identity_limit=512)
        _pair(operation_kind, operation_id, "operation")
        _identity(before, "before", 128, optional=True)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ForestError("AUDIT_QUERY_INVALID", "Audit limit is outside 1..100", 422)

        async with self.store.pool.connection() as conn, conn.transaction():
            cursor = None
            if before is not None:
                cursor = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT created_at,audit_id FROM {} "
                            "WHERE bot_id=%s AND audit_id=%s"
                        ).format(self._table()),
                        (self.store.bot_id, before),
                    )
                ).fetchone()
                if cursor is None:
                    raise ForestError(
                        "AUDIT_CURSOR_NOT_FOUND", "Audit cursor is unavailable", 404
                    )

            clauses = [sql.SQL("bot_id=%s")]
            params: list[object] = [self.store.bot_id]
            for field, (value, _size) in filters.items():
                if value is not None:
                    clauses.append(sql.SQL("{}=%s").format(sql.Identifier(field)))
                    params.append(value)
            for kind_field, id_field, kind, identity in (
                ("resource_kind", "resource_id", resource_kind, resource_id),
                ("operation_kind", "operation_id", operation_kind, operation_id),
            ):
                if kind is not None:
                    clauses.extend(
                        (
                            sql.SQL("{}=%s").format(sql.Identifier(kind_field)),
                            sql.SQL("{}=%s").format(sql.Identifier(id_field)),
                        )
                    )
                    params.extend((kind, identity))
            if cursor is not None:
                clauses.append(sql.SQL("(created_at,audit_id)<(%s,%s)"))
                params.extend((cursor["created_at"], cursor["audit_id"]))
            params.append(limit + 1)
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE {} "
                        "ORDER BY created_at DESC,audit_id DESC LIMIT %s"
                    ).format(self._table(), sql.SQL(" AND ").join(clauses)),
                    params,
                )
            ).fetchall()
        visible = rows[:limit]
        return {
            "records": [self._project(row) for row in visible],
            "next_before": visible[-1]["audit_id"] if len(rows) > limit else None,
        }

    @staticmethod
    def _project(row) -> dict:
        return {
            "audit_id": row["audit_id"],
            "action": row["action"],
            "severity": row["severity"],
            "producer_component_id": row["producer_component_id"],
            "producer_generation_id": row["producer_generation_id"],
            "origin_kind": row["origin_kind"],
            "actor_kind": row["actor_kind"],
            "actor_id": row["actor_id"],
            "resource_kind": row["resource_kind"],
            "resource_id": row["resource_id"],
            "operation_kind": row["operation_kind"],
            "operation_id": row["operation_id"],
            "call_id": row["call_id"],
            "details_payload_id": row["details_payload_id"],
            "details_payload_digest": row["details_payload_digest"],
            "created_at": row["created_at"].isoformat(),
        }
