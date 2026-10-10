"""PostgreSQL tombstone authority for the derived Milvus reference index."""

from __future__ import annotations

from psycopg import sql


class VectorIndexRefRepository:
    """Serialize Milvus mutations for a ref and retain permanent delete fences."""

    def __init__(self, store, *, namespace: str) -> None:
        self.store = store
        self.namespace = namespace

    def _table(self):
        return sql.Identifier(self.namespace, "vector_index_refs")

    def _binding_table(self):
        return sql.Identifier(self.namespace, "vector_collection_bindings")

    def _collection_lock_key(self) -> str:
        return f"vector-collection:{self.store.bot_id}"

    async def lock_collection_mutation(self, conn) -> None:
        """Let ref mutations run concurrently, but exclude collection replacement."""
        await conn.execute(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s, 0))",
            (self._collection_lock_key(),),
        )

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
        active = await (
            await conn.execute(
                sql.SQL(
                    "SELECT EXISTS(SELECT 1 FROM {} WHERE bot_id=%s "
                    "AND state='ACTIVE') AS has_active"
                ).format(self._table()),
                (self.store.bot_id,),
            )
        ).fetchone()
        return (dict(binding) if binding is not None else None, bool(active["has_active"]))

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

    async def lock(self, conn, ref_id: str) -> dict | None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"vector-ref:{self.store.bot_id}:{ref_id}",),
        )
        row = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE bot_id=%s AND ref_id=%s FOR UPDATE").format(
                    self._table()
                ),
                (self.store.bot_id, ref_id),
            )
        ).fetchone()
        return dict(row) if row is not None else None

    async def record_active(self, conn, *, ref_id: str, input_digest: str) -> None:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,ref_id,state,input_digest) "
                "VALUES(%s,%s,'ACTIVE',%s)"
            ).format(self._table()),
            (self.store.bot_id, ref_id, input_digest),
        )

    async def record_tombstone(self, conn, *, ref_id: str, existing: dict | None) -> None:
        if existing is None:
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,ref_id,state,tombstoned_at,next_sweep_at) "
                    "VALUES(%s,%s,'TOMBSTONED',clock_timestamp(),"
                    "clock_timestamp()+interval '30 seconds')"
                ).format(self._table()),
                (self.store.bot_id, ref_id),
            )
        elif existing["state"] == "ACTIVE":
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET state='TOMBSTONED',input_digest=NULL,"
                    "tombstoned_at=clock_timestamp(),"
                    "next_sweep_at=clock_timestamp()+interval '30 seconds' "
                    "WHERE bot_id=%s AND ref_id=%s"
                ).format(self._table()),
                (self.store.bot_id, ref_id),
            )

    async def claim_due_sweep(self, *, limit: int = 128) -> list[str]:
        async with self.store.pool.connection() as conn, conn.transaction():
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT ref_id,sweep_count FROM {} WHERE bot_id=%s "
                        "AND state='TOMBSTONED' AND next_sweep_at<=clock_timestamp() "
                        "ORDER BY next_sweep_at,ref_id LIMIT %s FOR UPDATE SKIP LOCKED"
                    ).format(self._table()),
                    (self.store.bot_id, limit),
                )
            ).fetchall()
            for row in rows:
                delay = min(30 * (2 ** min(row["sweep_count"] + 1, 12)), 86400)
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET sweep_count=sweep_count+1,"
                        "next_sweep_at=clock_timestamp()+(%s*interval '1 second') "
                        "WHERE bot_id=%s AND ref_id=%s"
                    ).format(self._table()),
                    (delay, self.store.bot_id, row["ref_id"]),
                )
            return [row["ref_id"] for row in rows]
