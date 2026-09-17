"""Persistent per-Bot request and token budget for multimodal embeddings."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace

_ERROR_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


def _bounded(value, field: str, limit: int):
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("MEMORY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _digest(value) -> str:
    try:
        wire = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError("MEMORY_INPUT_INVALID", "Budget input is not canonical", 422) from exc
    return "sha256:" + hashlib.sha256(wire).hexdigest()


class MemoryEmbeddingBudgetRepository:
    """Reserve once per Provider request; UNKNOWN keeps its token exposure."""

    def __init__(self, store, *, namespace: str | None = None) -> None:
        self.store = store
        self.namespace = namespace or module_schema_namespace("forest.memory.graph")

    def _table(self, name: str):
        return sql.SQL("{}.{}").format(sql.Identifier(self.namespace), sql.Identifier(name))

    @asynccontextmanager
    async def _transaction(self, connection=None):
        if connection is not None:
            yield connection
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            yield conn

    @staticmethod
    def _validate_day(value):
        if type(value) is not date:
            raise ForestError("MEMORY_INPUT_INVALID", "Budget day must be an explicit date", 422)

    async def reserve(
        self,
        *,
        operation_id: str,
        reservation_id: str,
        batch_ref: str,
        policy_revision: str,
        budget_day: date,
        request_limit: int,
        token_limit: int,
        reserved_tokens: int,
        deadline_at: datetime,
        _connection=None,
    ) -> dict:
        for value, field in (
            (operation_id, "operation_id"),
            (reservation_id, "reservation_id"),
            (batch_ref, "batch_ref"),
            (policy_revision, "policy_revision"),
        ):
            _bounded(value, field, 256)
        self._validate_day(budget_day)
        if type(request_limit) is not int or not 1 <= request_limit <= 1_000_000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding request limit", 422)
        if type(token_limit) is not int or not 1 <= token_limit <= 1_000_000_000_000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding token limit", 422)
        if type(reserved_tokens) is not int or not 1 <= reserved_tokens <= 1_000_000_000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid embedding token reservation", 422)
        if not isinstance(deadline_at, datetime) or deadline_at.tzinfo is None:
            raise ForestError("MEMORY_INPUT_INVALID", "Budget deadline needs a timezone", 422)
        deadline_at = deadline_at.astimezone(timezone.utc)
        if deadline_at <= datetime.now(timezone.utc):
            raise ForestError("MEMORY_INPUT_INVALID", "Budget deadline already expired", 422)
        input_digest = _digest(
            {
                "reservation_id": reservation_id,
                "batch_ref": batch_ref,
                "policy_revision": policy_revision,
                "budget_day": budget_day.isoformat(),
                "request_limit": request_limit,
                "token_limit": token_limit,
                "reserved_tokens": reserved_tokens,
                "deadline_at": deadline_at.isoformat(),
            }
        )
        async with self._transaction(_connection) as conn:
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND "
                        "(reservation_id=%s OR operation_id=%s)"
                    ).format(self._table("embedding_budget_reservations")),
                    (self.store.bot_id, reservation_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if (
                    replay["reservation_id"] != reservation_id
                    or replay["operation_id"] != operation_id
                    or replay["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "MEMORY_BUDGET_RESERVATION_CONFLICT",
                        "Embedding budget reservation differs",
                        409,
                    )
                return {**dict(replay), "duplicate": True}
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,budget_day) VALUES (%s,%s) ON CONFLICT DO NOTHING"
                ).format(self._table("embedding_budget_days")),
                (self.store.bot_id, budget_day),
            )
            day = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND budget_day=%s FOR UPDATE"
                    ).format(self._table("embedding_budget_days")),
                    (self.store.bot_id, budget_day),
                )
            ).fetchone()
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND "
                        "(reservation_id=%s OR operation_id=%s)"
                    ).format(self._table("embedding_budget_reservations")),
                    (self.store.bot_id, reservation_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if (
                    replay["reservation_id"] != reservation_id
                    or replay["operation_id"] != operation_id
                    or replay["input_digest"] != input_digest
                ):
                    raise ForestError(
                        "MEMORY_BUDGET_RESERVATION_CONFLICT",
                        "Embedding budget reservation differs",
                        409,
                    )
                return {**dict(replay), "duplicate": True}
            used_requests = (
                day["reserved_requests"] + day["settled_requests"] + day["unknown_requests"]
            )
            exposed_tokens = (
                day["reserved_tokens"] + day["charged_tokens"] + day["unknown_tokens"]
            )
            if used_requests + 1 > request_limit:
                raise ForestError(
                    "MEMORY_EMBEDDING_REQUEST_BUDGET_EXHAUSTED",
                    "Daily embedding request budget is exhausted",
                    429,
                )
            if exposed_tokens + reserved_tokens > token_limit:
                raise ForestError(
                    "MEMORY_EMBEDDING_TOKEN_BUDGET_EXHAUSTED",
                    "Daily embedding token budget is exhausted",
                    429,
                )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,reservation_id,operation_id,input_digest,batch_ref,"
                        "policy_revision,budget_day,request_limit,token_limit,reserved_tokens,"
                        "deadline_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table("embedding_budget_reservations")),
                    (
                        self.store.bot_id,
                        reservation_id,
                        operation_id,
                        input_digest,
                        batch_ref,
                        policy_revision,
                        budget_day,
                        request_limit,
                        token_limit,
                        reserved_tokens,
                        deadline_at,
                    ),
                )
            ).fetchone()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET reserved_requests=reserved_requests+1,"
                    "reserved_tokens=reserved_tokens+%s,revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND budget_day=%s"
                ).format(self._table("embedding_budget_days")),
                (reserved_tokens, self.store.bot_id, budget_day),
            )
            return {**dict(row), "duplicate": False}

    async def settle(
        self,
        *,
        reservation_id: str,
        settlement_operation_id: str,
        outcome: str,
        charged_tokens: int | None = None,
        error_code: str | None = None,
        _connection=None,
    ) -> dict:
        _bounded(reservation_id, "reservation_id", 256)
        _bounded(settlement_operation_id, "settlement_operation_id", 256)
        if outcome not in {"RELEASED", "CHARGED", "UNKNOWN"}:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid budget outcome", 422)
        if (outcome == "CHARGED") != (charged_tokens is not None):
            raise ForestError("MEMORY_INPUT_INVALID", "Charged token receipt is incomplete", 422)
        if charged_tokens is not None and (
            type(charged_tokens) is not int or not 0 <= charged_tokens <= 1_000_000_000_000
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid charged token count", 422)
        if error_code is not None and (
            not isinstance(error_code, str) or _ERROR_RE.fullmatch(error_code) is None
        ):
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid budget error code", 422)
        async with self._transaction(_connection) as conn:
            located = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT budget_day FROM {} WHERE bot_id=%s AND reservation_id=%s"
                    ).format(self._table("embedding_budget_reservations")),
                    (self.store.bot_id, reservation_id),
                )
            ).fetchone()
            if located is None:
                raise ForestError(
                    "MEMORY_BUDGET_RESERVATION_NOT_FOUND",
                    "Embedding budget reservation does not exist",
                    404,
                )
            day = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND budget_day=%s FOR UPDATE"
                    ).format(self._table("embedding_budget_days")),
                    (self.store.bot_id, located["budget_day"]),
                )
            ).fetchone()
            reservation = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND reservation_id=%s FOR UPDATE"
                    ).format(self._table("embedding_budget_reservations")),
                    (self.store.bot_id, reservation_id),
                )
            ).fetchone()
            if reservation["state"] != "RESERVED":
                if (
                    reservation["state"] != outcome
                    or reservation["settlement_operation_id"] != settlement_operation_id
                    or reservation["charged_tokens"] != charged_tokens
                    or reservation["error_code"] != error_code
                ):
                    raise ForestError(
                        "MEMORY_BUDGET_SETTLEMENT_CONFLICT",
                        "Embedding budget settlement differs",
                        409,
                    )
                return {**dict(reservation), "duplicate": True, "day": dict(day)}
            request_changes = {
                "RELEASED": (0, 0),
                "CHARGED": (1, 0),
                "UNKNOWN": (0, 1),
            }[outcome]
            charged_delta = charged_tokens or 0
            unknown_delta = reservation["reserved_tokens"] if outcome == "UNKNOWN" else 0
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET reserved_requests=reserved_requests-1,"
                    "reserved_tokens=reserved_tokens-%s,settled_requests=settled_requests+%s,"
                    "unknown_requests=unknown_requests+%s,charged_tokens=charged_tokens+%s,"
                    "unknown_tokens=unknown_tokens+%s,revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND budget_day=%s"
                ).format(self._table("embedding_budget_days")),
                (
                    reservation["reserved_tokens"],
                    request_changes[0],
                    request_changes[1],
                    charged_delta,
                    unknown_delta,
                    self.store.bot_id,
                    reservation["budget_day"],
                ),
            )
            reservation = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,settlement_operation_id=%s,charged_tokens=%s,"
                        "error_code=%s,settled_at=clock_timestamp() WHERE bot_id=%s "
                        "AND reservation_id=%s RETURNING *"
                    ).format(self._table("embedding_budget_reservations")),
                    (
                        outcome,
                        settlement_operation_id,
                        charged_tokens,
                        error_code,
                        self.store.bot_id,
                        reservation_id,
                    ),
                )
            ).fetchone()
            day = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND budget_day=%s").format(
                        self._table("embedding_budget_days")
                    ),
                    (self.store.bot_id, reservation["budget_day"]),
                )
            ).fetchone()
            return {**dict(reservation), "duplicate": False, "day": dict(day)}

    async def snapshot(self, budget_day: date) -> dict:
        self._validate_day(budget_day)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND budget_day=%s").format(
                        self._table("embedding_budget_days")
                    ),
                    (self.store.bot_id, budget_day),
                )
            ).fetchone()
        return dict(row) if row is not None else {
            "bot_id": self.store.bot_id,
            "budget_day": budget_day,
            "reserved_requests": 0,
            "settled_requests": 0,
            "unknown_requests": 0,
            "reserved_tokens": 0,
            "charged_tokens": 0,
            "unknown_tokens": 0,
            "revision": 0,
        }

    async def recover_expired(self, *, limit: int = 100) -> int:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ForestError("MEMORY_INPUT_INVALID", "Invalid budget recovery limit", 422)
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT reservation_id FROM {} WHERE bot_id=%s AND state='RESERVED' "
                        "AND deadline_at<=clock_timestamp() ORDER BY deadline_at,reservation_id "
                        "LIMIT %s"
                    ).format(self._table("embedding_budget_reservations")),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
        recovered = 0
        for row in rows:
            reservation_id = row["reservation_id"]
            try:
                await self.settle(
                    reservation_id=reservation_id,
                    settlement_operation_id="budget-recovery:"
                    + hashlib.sha256(reservation_id.encode("utf-8")).hexdigest(),
                    outcome="UNKNOWN",
                    error_code="EMBEDDING_BUDGET_RECOVERY_UNCERTAIN",
                )
                recovered += 1
            except ForestError as exc:
                if exc.code != "MEMORY_BUDGET_SETTLEMENT_CONFLICT":
                    raise
        return recovered
