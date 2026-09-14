"""Migration 070 permanently excludes generation-9 Bot writers."""

import psycopg
import pytest
from test_config_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore

pytestmark = pytest.mark.postgres


async def test_generation_nine_pool_cannot_reacquire_after_receipt_cutover(
    upgrade_database, tmp_path, monkeypatch
):
    old_root = resources(tmp_path / "schema-069", cutoff="069")
    new_root = resources(tmp_path / "schema-070", cutoff="070")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    monkeypatch.setattr(bot_module, "WRITER_GENERATION", 9)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    new = None
    try:
        await old.migrate()
        lease = await old.acquire_host("generation-nine", "before-070", ttl_seconds=60)
        await old.release_host(lease)
        async with old.pool.connection() as old_connection:
            shown = await (
                await old_connection.execute("SHOW forest.writer_generation")
            ).fetchone()
            assert shown["forest.writer_generation"] == "9"
            before = await (
                await old_connection.execute(
                    "SELECT count(*) AS count FROM public.forest_bot_schema_migrations"
                )
            ).fetchone()

            monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
            monkeypatch.setattr(bot_module, "WRITER_GENERATION", 10)
            new = BotStore(upgrade_database, "bot-a")
            await new.open()
            await new.migrate()
            async with new.pool.connection() as conn:
                gate = await (
                    await conn.execute(
                        "SELECT minimum_generation FROM public.bot_schema_writer_gate"
                    )
                ).fetchone()
                after = await (
                    await conn.execute(
                        "SELECT count(*) AS count FROM public.forest_bot_schema_migrations"
                    )
                ).fetchone()
                assert gate["minimum_generation"] == 10
                assert after["count"] == before["count"] + 1
                relation = await (
                    await conn.execute(
                        "SELECT to_regclass('public.bot_suppressed_input_occurrences') AS name"
                    )
                ).fetchone()
                assert relation["name"] == "bot_suppressed_input_occurrences"

            with pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState,
                match="WRITER_SCHEMA_TOO_OLD",
            ):
                await old.acquire_host(
                    "generation-nine", "after-070-reconnect", ttl_seconds=60
                )
            with pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState,
                match="WRITER_SCHEMA_TOO_OLD",
            ):
                await old_connection.execute(
                    "UPDATE public.bot_host_lease "
                    "SET expires_at=clock_timestamp()+interval '1 minute'"
                )

            current = await new.acquire_host(
                "generation-ten", "after-070", ttl_seconds=60
            )
            assert current.owner_epoch > lease.owner_epoch
    finally:
        if new is not None:
            await new.close()
        await old.close()
