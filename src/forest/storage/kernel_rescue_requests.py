"""Durable, Bot-scoped Core rescue queue and result ledger.

The caller owns the transaction.  A queued row is locked and completed in that
same transaction as its authority checks and effect; there is no committed
intermediate claim that could outlive a crashed executor.
"""

from __future__ import annotations

import json

from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError


class KernelRescueRequestRepository:
    def __init__(self, store, namespace: str, runtime, payloads) -> None:
        self.store = store
        self.namespace = namespace
        self.runtime = runtime
        self.payloads = payloads

    def _table(self):
        return sql.Identifier(self.namespace, "rescue_requests")

    async def next_queued_in(self, conn, lease) -> dict | None:
        """Lock one P0-first request until the caller commits or rolls back."""

        await self.runtime.verify_host_lease_in(conn, lease)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND state='QUEUED' "
                    "ORDER BY CASE service_class WHEN 'P0' THEN 0 ELSE 1 END,"
                    "created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1"
                ).format(self._table()),
                (self.store.bot_id,),
            )
        ).fetchone()
        return dict(row) if row is not None else None

    async def complete_in(
        self,
        conn,
        lease,
        *,
        request_id: str,
        state: str,
        result_value: dict,
        result_code: str | None,
        payload_ref: dict | None,
    ) -> dict:
        """Settle only a locked, still-queued request in the effect transaction."""

        await self.runtime.verify_host_lease_in(conn, lease)
        if state not in {"SUCCEEDED", "FAILED", "DENIED"}:
            raise ForestError("CORE_RESCUE_RESULT_INVALID", "Invalid rescue result state", 422)
        if (
            not isinstance(request_id, str)
            or not 1 <= len(request_id.encode()) <= 256
            or not isinstance(result_value, dict)
            or result_value.get("protocol") != "forest.core-rescue-result/1"
            or result_value.get("request_id") != request_id
            or result_value.get("state") != state
            or len(json.dumps(result_value, ensure_ascii=False).encode()) > 8192
            or (state != "SUCCEEDED" and not result_code)
            or (result_code is not None and (
                not isinstance(result_code, str)
                or not 1 <= len(result_code.encode()) <= 128
            ))
        ):
            raise ForestError("CORE_RESCUE_RESULT_INVALID", "Invalid rescue result", 422)
        if payload_ref is not None and (
            not isinstance(payload_ref, dict)
            or not isinstance(payload_ref.get("payload_id"), str)
            or not isinstance(payload_ref.get("digest"), str)
        ):
            raise ForestError("CORE_RESCUE_RESULT_INVALID", "Invalid rescue payload", 422)
        if payload_ref is not None:
            await self.payloads.verify_in(
                conn,
                payload_id=payload_ref["payload_id"],
                digest=payload_ref["digest"],
                owner_component_id="kernel.runtime",
                owner_kind="core.rescue.result",
                owner_id=request_id,
            )
        row = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state=%s,result_code=%s,result_value=%s,"
                    "result_payload_id=%s,result_payload_digest=%s,delivery_state=%s,"
                    "completed_at=clock_timestamp() WHERE bot_id=%s AND request_id=%s "
                    "AND state='QUEUED' RETURNING *"
                ).format(self._table()),
                (
                    state, result_code, Jsonb(result_value),
                    None if payload_ref is None else payload_ref["payload_id"],
                    None if payload_ref is None else payload_ref["digest"],
                    "SUPPRESSED" if payload_ref is None else "PENDING",
                    self.store.bot_id, request_id,
                ),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "CORE_RESCUE_REQUEST_STALE", "Rescue request is no longer queued", 409
            )
        return dict(row)

    async def next_pending_delivery_in(self, conn, lease) -> dict | None:
        await self.runtime.verify_host_lease_in(conn, lease)
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s AND delivery_state='PENDING' "
                    "ORDER BY completed_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1"
                ).format(self._table()),
                (self.store.bot_id,),
            )
        ).fetchone()
        return dict(row) if row is not None else None

    async def mark_delivery_in(
        self, conn, lease, *, request_id: str, intent_id: str | None
    ) -> dict:
        """Queue an authenticated intent, or permanently suppress revoked output."""

        await self.runtime.verify_host_lease_in(conn, lease)
        if intent_id is not None and (
            not isinstance(intent_id, str) or not 1 <= len(intent_id.encode()) <= 256
        ):
            raise ForestError("CORE_RESCUE_RESULT_INVALID", "Invalid intent ID", 422)
        row = await (
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET delivery_state=%s,delivery_intent_id=%s "
                    "WHERE bot_id=%s AND request_id=%s AND delivery_state='PENDING' "
                    "RETURNING *"
                ).format(self._table()),
                (
                    "SUPPRESSED" if intent_id is None else "QUEUED",
                    intent_id, self.store.bot_id, request_id,
                ),
            )
        ).fetchone()
        if row is None:
            raise ForestError(
                "CORE_RESCUE_DELIVERY_STALE", "Rescue delivery is no longer pending", 409
            )
        return dict(row)

    async def result(self, request_id: str) -> dict:
        if not isinstance(request_id, str) or not 1 <= len(request_id.encode()) <= 256:
            raise ForestError("CORE_RESCUE_REQUEST_INVALID", "Invalid request ID", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND request_id=%s").format(
                        self._table()
                    ),
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
        if row is None:
            raise ForestError("CORE_RESCUE_REQUEST_NOT_FOUND", "Rescue request not found", 404)
        return dict(row)
