"""Deployment account reservations; Bot databases retain Binding authority.

Registration is a recoverable two-database workflow. A reservation never says
the target accepted it. Retrying requires a fresh administrator session and the
original subject, then reuses the target operation and immutable payload.
"""

from __future__ import annotations

from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json


class BindingCatalog:
    def __init__(self, catalog):
        self.catalog, self.store = catalog, catalog.store

    async def reserve(self, *, bot_id: str, self_id: str, display_name: str,
                      operation_id: str, subject_id: str, session_token_hash: str) -> dict:
        payload = {"self_id": self_id, "display_name": display_name}
        semantic = digest_json({"bot_id": bot_id, **payload})
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.catalog._authorize(conn, subject_id, session_token_hash)
            # Serialize both cross-account operation reuse and cross-Bot account claims.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                               ("binding-op:" + operation_id,))
            old = await (await conn.execute(
                "SELECT * FROM platform_bindings WHERE operation_id=%s", (operation_id,),
            )).fetchone()
            if old:
                if old["input_digest"] != semantic or old["subject_id"] != subject_id:
                    raise ForestError("IDEMPOTENCY_CONFLICT", "Registration input changed", 409)
                await self.catalog._authorize(conn, subject_id, session_token_hash)
                return old
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                               ("onebot-account:" + self_id,))
            existing = await (await conn.execute(
                "SELECT binding_id FROM platform_bindings WHERE platform='onebot-v11' AND self_id=%s",
                (self_id,),
            )).fetchone()
            if existing:
                raise ForestError("ACCOUNT_ALREADY_BOUND", "This platform account is already reserved", 409)
            bot = await (await conn.execute(
                "SELECT stage FROM bot_catalog WHERE bot_id=%s", (bot_id,),
            )).fetchone()
            if not bot or bot["stage"] != "READY":
                raise ForestError("BOT_NOT_READY", "Create a ready Bot before registering an account", 409)
            payload["binding_id"] = str(uuid4())
            row = await (await conn.execute(
                "INSERT INTO platform_bindings(binding_id,bot_id,platform,self_id,operation_id,"
                "subject_id,input_digest,payload) VALUES (%s,%s,'onebot-v11',%s,%s,%s,%s,%s) RETURNING *",
                (payload["binding_id"], bot_id, self_id, operation_id, subject_id, semantic, Jsonb(payload)),
            )).fetchone()
            await conn.execute(
                "INSERT INTO audit_records(audit_id,subject_id,action,resource_key,request_id,details) "
                "VALUES (%s,%s,'binding.reserve',%s,%s,%s)",
                (str(uuid4()), subject_id, "binding:" + row["binding_id"], operation_id,
                 Jsonb({"bot_id": bot_id, "binding_id": row["binding_id"]})),
            )
            await self.catalog._authorize(conn, subject_id, session_token_hash)
            return row

    async def registration(self, *, bot_id: str, binding_id: str, subject_id: str,
                           session_token_hash: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.catalog._authorize(conn, subject_id, session_token_hash)
            row = await (await conn.execute(
                "SELECT * FROM platform_bindings WHERE bot_id=%s AND binding_id=%s", (bot_id, binding_id),
            )).fetchone()
            if row is None:
                raise ForestError("BINDING_NOT_FOUND", "Binding reservation does not exist", 404)
            if row["subject_id"] != subject_id:
                raise ForestError("REGISTRATION_OWNER_REQUIRED", "The original administrator must finish registration", 403)
            await self.catalog._authorize(conn, subject_id, session_token_hash)
            return row

    async def registered(self, *, bot_id: str, binding_id: str, subject_id: str,
                         session_token_hash: str) -> None:
        """Called only after a successful target BindingStore.create receipt."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.catalog._authorize(conn, subject_id, session_token_hash)
            await conn.execute(
                "UPDATE platform_bindings SET stage='REGISTERED' WHERE bot_id=%s AND binding_id=%s AND subject_id=%s",
                (bot_id, binding_id, subject_id),
            )
            await self.catalog._authorize(conn, subject_id, session_token_hash)

    async def list(self, *, bot_id: str, subject_id: str, session_token_hash: str) -> list[dict]:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.catalog._authorize(
                conn, subject_id, session_token_hash, require_admin=False
            )
            rows = await (await conn.execute(
                "SELECT binding_id,self_id,stage,payload->>'display_name' AS display_name "
                "FROM platform_bindings WHERE bot_id=%s ORDER BY created_at,binding_id", (bot_id,),
            )).fetchall()
            await self.catalog._authorize(
                conn, subject_id, session_token_hash, require_admin=False
            )
            return rows

    async def resolve(self, binding_id: str) -> str:
        """Host routing only; receiving still requires the Binding secret and lease."""
        async with self.store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT bot_id FROM platform_bindings WHERE binding_id=%s AND stage='REGISTERED'", (binding_id,),
            )).fetchone()
            if row is None:
                raise ForestError("BINDING_NOT_FOUND", "Binding is unavailable", 404)
            return row["bot_id"]
