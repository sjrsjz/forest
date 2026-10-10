"""Temporary host binding repository for the derived Milvus collection."""

from __future__ import annotations

from psycopg import sql


class VectorIndexBindingRepository:
    """Inspect and freeze the current Milvus collection identity."""

    def __init__(self, store, *, namespace: str) -> None:
        self.store = store
        self.namespace = namespace

    def _table(self):
        return sql.Identifier(self.namespace, "vector_index_refs")

    def _binding_table(self):
        return sql.Identifier(self.namespace, "vector_collection_bindings")

    def _collection_lock_key(self) -> str:
        return f"vector-collection:{self.store.bot_id}"

    async def lock_collection_state(self, conn) -> tuple[dict | None, bool]:
        """Serialize collection inspection/binding against ref mutations."""
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (self._collection_lock_key(),),
        )
        binding = await (
            await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE bot_id=%s "
                    "ORDER BY binding_revision DESC LIMIT 1"
                ).format(self._binding_table()),
                (self.store.bot_id,),
            )
        ).fetchone()
        live = await (
            await conn.execute(
                sql.SQL(
                    "SELECT EXISTS(SELECT 1 FROM {} WHERE bot_id=%s "
                    "AND state IN ('PENDING','ACTIVE')) AS has_live"
                ).format(self._table()),
                (self.store.bot_id,),
            )
        ).fetchone()
        return (dict(binding) if binding is not None else None, bool(live["has_live"]))

    async def bind_collection(
        self, conn, *, revision: int, space_id: str,
        collection_name: str, collection_id: int,
    ) -> None:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,binding_revision,space_id,"
                "collection_name,collection_id) VALUES(%s,%s,%s,%s,%s)"
            ).format(self._binding_table()),
            (self.store.bot_id, revision, space_id, collection_name, collection_id),
        )
