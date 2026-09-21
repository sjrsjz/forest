"""PostgreSQL authority for one Bot-lifetime derived vector-index binding."""

from __future__ import annotations

import hashlib

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json


class VectorIndexIdentityRepository:
    def __init__(self, store, *, namespace: str) -> None:
        self.store = store
        self.namespace = namespace

    def _table(self):
        return sql.Identifier(self.namespace, "vector_index_identity")

    @staticmethod
    def collection_name(bot_id: str, space_id: str) -> str:
        suffix = hashlib.sha256((bot_id + "\0" + space_id).encode()).hexdigest()[:32]
        return "forest_memory_" + suffix

    async def initialize(
        self,
        *,
        operation_id: str,
        space_id: str,
        dimensions: int,
        metric: str = "COSINE",
    ) -> dict:
        if not operation_id or not space_id or not 1 <= dimensions <= 4096 or metric != "COSINE":
            raise ForestError("VECTOR_INDEX_CONFIGURATION_INVALID", "Vector index identity is invalid", 422)
        collection_name = self.collection_name(self.store.bot_id, space_id)
        input_digest = digest_json(
            ["forest.vector-index.identity/1", space_id, dimensions, metric, collection_name]
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("vector-index:" + self.store.bot_id,),
            )
            existing = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s FOR UPDATE").format(self._table()),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["input_digest"] != input_digest
                    or existing["space_id"] != space_id
                    or existing["dimensions"] != dimensions
                    or existing["metric"] != metric
                    or existing["collection_name"] != collection_name
                ):
                    raise ForestError(
                        "VECTOR_INDEX_SPACE_IMMUTABLE",
                        "Vector index space belongs to another Bot-lifetime embedding profile",
                        409,
                    )
                return dict(existing)
            row = await (
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,initialization_operation_id,input_digest,space_id,"
                        "dimensions,metric,collection_name) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                    ).format(self._table()),
                    (
                        self.store.bot_id,
                        operation_id,
                        input_digest,
                        space_id,
                        dimensions,
                        metric,
                        collection_name,
                    ),
                )
            ).fetchone()
            return dict(row)
