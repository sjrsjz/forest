"""Durable identity and transport facts owned by ``embedding.multimodal``."""

from __future__ import annotations

import json

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json


class EmbeddingProviderRepository:
    def __init__(self, store, *, namespace: str) -> None:
        self.store = store
        self.namespace = namespace

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    async def initialize_identity(self, *, operation_id: str, profile) -> dict:
        facts = {
            "deployment_id": profile.deployment_id,
            "model_revision": profile.model_revision,
            "model": profile.model,
            "dimensions": profile.dimensions,
            "space_id": profile.space_id,
        }
        input_digest = digest_json(["forest.embedding-provider.identity/1", facts])
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR SHARE").format(
                        self._table("embedding_provider_identity")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if row is None:
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "INSERT INTO {}(bot_id,initialization_operation_id,input_digest,"
                            "deployment_id,model_revision,model,dimensions,space_id) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                        ).format(self._table("embedding_provider_identity")),
                        (
                            self.store.bot_id,
                            operation_id,
                            input_digest,
                            profile.deployment_id,
                            profile.model_revision,
                            profile.model,
                            profile.dimensions,
                            profile.space_id,
                        ),
                    )
                ).fetchone()
            expected = tuple(facts.values())
            actual = tuple(row[key] for key in facts)
            if actual != expected:
                raise ForestError(
                    "EMBEDDING_PROFILE_IMMUTABLE",
                    "A Bot cannot change its embedding model or vector space after creation",
                    409,
                )
            return dict(row)

    async def prepare_attempt(
        self,
        *,
        attempt_id: str,
        request_id: str,
        provider_generation_id: str,
        configuration_revision: int,
        prepared,
        purpose: str,
        request_payload_id: str,
    ) -> dict:
        values = (
            self.store.bot_id,
            attempt_id,
            request_id,
            provider_generation_id,
            configuration_revision,
            prepared.space_id,
            purpose,
            prepared.input_count,
            prepared.payload_digest,
            request_payload_id,
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,attempt_id,request_id,provider_generation_id,"
                    "configuration_revision,space_id,purpose,input_count,request_payload_digest,"
                    "request_payload_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT DO NOTHING"
                ).format(self._table("embedding_transport_attempts")),
                values,
            )
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND "
                        "(attempt_id=%s OR request_id=%s)"
                    ).format(
                        self._table("embedding_transport_attempts")
                    ),
                    (self.store.bot_id, attempt_id, request_id),
                )
            ).fetchall()
            if len(rows) != 1:
                raise ForestError(
                    "EMBEDDING_ATTEMPT_CONFLICT", "Embedding attempt identity differs", 409
                )
            row = rows[0]
            expected = values[2:10]
            actual = tuple(
                row[key]
                for key in (
                    "request_id",
                    "provider_generation_id",
                    "configuration_revision",
                    "space_id",
                    "purpose",
                    "input_count",
                    "request_payload_digest",
                    "request_payload_id",
                )
            )
            if actual != expected:
                raise ForestError(
                    "EMBEDDING_ATTEMPT_CONFLICT", "Embedding attempt identity differs", 409
                )
            return dict(row)

    async def mark_sent_in(self, conn, *, attempt_id: str) -> dict:
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE").format(
                    self._table("embedding_transport_attempts")
                ),
                (self.store.bot_id, attempt_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("EMBEDDING_ATTEMPT_NOT_FOUND", "Embedding attempt is missing", 404)
        if row["state"] == "PREPARED":
            row = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state='SENT',sent_at=clock_timestamp() "
                        "WHERE bot_id=%s AND attempt_id=%s RETURNING *"
                    ).format(self._table("embedding_transport_attempts")),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
        elif row["state"] != "SENT":
            raise ForestError(
                "EMBEDDING_ATTEMPT_NOT_SENDABLE", "Embedding attempt is already terminal", 409
            )
        return dict(row)

    async def settle(
        self,
        *,
        attempt_id: str,
        result,
        result_payload_id: str,
        result_payload_digest: str,
    ) -> dict:
        usage_json = (
            None
            if result.usage is None
            else json.dumps(result.usage, sort_keys=True, separators=(",", ":"))
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {} WHERE bot_id=%s AND attempt_id=%s FOR UPDATE"
                    ).format(self._table("embedding_transport_attempts")),
                    (self.store.bot_id, attempt_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError(
                    "EMBEDDING_ATTEMPT_NOT_FOUND", "Embedding attempt is missing", 404
                )
            if row["state"] in {"RESPONDED", "REJECTED", "NOT_SENT", "UNKNOWN"}:
                expected = (
                    result.status,
                    result_payload_id if result.status == "RESPONDED" else None,
                    result_payload_digest if result.status == "RESPONDED" else None,
                    usage_json,
                    result.error_code,
                    result.http_status,
                )
                actual = tuple(
                    row[key]
                    for key in (
                        "state",
                        "result_payload_id",
                        "result_payload_digest",
                        "usage_json",
                        "error_code",
                        "http_status",
                    )
                )
                if actual != expected:
                    raise ForestError(
                        "EMBEDDING_SETTLEMENT_CONFLICT", "Embedding settlement differs", 409
                    )
                return dict(row)
            allowed = {"PREPARED", "SENT"} if result.status == "NOT_SENT" else {"SENT"}
            if row["state"] not in allowed:
                raise ForestError(
                    "EMBEDDING_SETTLEMENT_CONFLICT", "Embedding transport state differs", 409
                )
            saved = await (
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET state=%s,result_payload_id=%s,result_payload_digest=%s,"
                        "usage_json=%s,error_code=%s,http_status=%s,settled_at=clock_timestamp() "
                        "WHERE bot_id=%s AND attempt_id=%s RETURNING *"
                    ).format(self._table("embedding_transport_attempts")),
                    (
                        result.status,
                        result_payload_id if result.status == "RESPONDED" else None,
                        result_payload_digest if result.status == "RESPONDED" else None,
                        usage_json,
                        result.error_code,
                        result.http_status,
                        self.store.bot_id,
                        attempt_id,
                    ),
                )
            ).fetchone()
            return dict(saved)
