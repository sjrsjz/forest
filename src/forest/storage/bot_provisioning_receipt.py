"""Immutable proof that a dedicated Bot database finished materialization."""

from __future__ import annotations

import psycopg
from psycopg import sql

from forest.errors import ForestError
from forest.schema_bundles import module_schema_namespace


class BotProvisioningReceiptAuthority:
    def __init__(self, bot_store) -> None:
        self.store = bot_store
        self.table = sql.SQL("{}.provisioning_receipts").format(
            sql.Identifier(module_schema_namespace("forest.kernel.runtime"))
        )

    async def record(self, *, claim, storage, profile) -> dict:
        facts = {
            "bot_id": claim.bot_id,
            "deployment_id": claim.deployment_id,
            "job_id": claim.job_id,
            "storage_profile_ref": claim.storage_profile_ref,
            "storage_generation": claim.storage_generation,
            "bootstrap_profile_ref": claim.bootstrap_profile_ref,
            "bootstrap_manifest_digest": claim.bootstrap_manifest_digest,
        }
        if (
            facts["bot_id"] != self.store.bot_id
            or storage.bot_id != facts["bot_id"]
            or storage.profile_ref != facts["storage_profile_ref"]
            or storage.storage_generation != facts["storage_generation"]
            or profile.bot_id != facts["bot_id"]
            or profile.profile_ref != facts["bootstrap_profile_ref"]
            or profile.storage_profile_ref != facts["storage_profile_ref"]
            or profile.storage_generation != facts["storage_generation"]
            or profile.manifest_digest != facts["bootstrap_manifest_digest"]
        ):
            raise ForestError(
                "PROVISIONING_INPUT_TAMPERED",
                "Provisioning receipt differs from the frozen Bot identity", 409,
            )
        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                await self._verify_bot_in(conn)
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,deployment_id,job_id,storage_profile_ref,"
                        "storage_generation,bootstrap_profile_ref,bootstrap_manifest_digest) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (bot_id) DO NOTHING"
                    ).format(self.table),
                    tuple(facts.values()),
                )
                row = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(self.table),
                        (self.store.bot_id,),
                    )
                ).fetchone()
                if row is None or any(row[key] != value for key, value in facts.items()):
                    raise ForestError(
                        "PROVISIONING_RECEIPT_CONFLICT",
                        "Bot database carries another provisioning receipt", 409,
                    )
                return dict(row)
        except psycopg.IntegrityError as exc:
            raise ForestError(
                "PROVISIONING_RECEIPT_CONFLICT",
                "Bot database carries another provisioning receipt", 409,
            ) from exc

    async def read(self) -> dict | None:
        async with self.store.pool.connection() as conn:
            await self._verify_bot_in(conn)
            row = await (
                await conn.execute(
                    sql.SQL("SELECT * FROM {} WHERE bot_id=%s").format(self.table),
                    (self.store.bot_id,),
                )
            ).fetchone()
            return None if row is None else dict(row)

    async def _verify_bot_in(self, conn) -> None:
        row = await (
            await conn.execute(
                "SELECT bot_id FROM forest_kernel.bot_identity WHERE singleton FOR SHARE"
            )
        ).fetchone()
        if row is None or row["bot_id"] != self.store.bot_id:
            raise ForestError(
                "PROVISIONING_BOT_SCOPE_MISMATCH",
                "Materialization proof belongs to another Bot", 409,
            )
