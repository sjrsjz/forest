"""Immutable Bot embedding-space identity owned by ``embedding.multimodal``."""

from __future__ import annotations

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
