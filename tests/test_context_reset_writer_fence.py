"""Migration 071 permanently excludes writers that interpret every request as COMPACT."""

import psycopg
import pytest
from test_config_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "helper_name",
    ["migration_v11.py", "shell_payloads_v12.py", "shell_registry_v12.py"],
)
def test_v11_checksum_binds_reset_writer_and_shell_contract(
    tmp_path, monkeypatch, helper_name
):
    root = resources(tmp_path / ("checksum-071-" + helper_name), cutoff="071")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    current = root / "bot_migrations" / "071_context_reset.sql"
    prior = root / "bot_migrations" / "070_suppressed_occurrence_receipts.sql"
    before = {
        path.name: _migration_checksum(path.name, path.read_text(encoding="utf-8"))
        for path in (current, prior)
    }
    helper = root / helper_name
    helper.write_text(
        helper.read_text(encoding="utf-8") + "\n# changed v11 dependency\n",
        encoding="utf-8",
    )
    assert _migration_checksum(current.name, current.read_text(encoding="utf-8")) != before[
        current.name
    ]
    assert _migration_checksum(prior.name, prior.read_text(encoding="utf-8")) == before[
        prior.name
    ]


async def test_generation_ten_pool_cannot_write_after_context_reset_cutover(
    upgrade_database, tmp_path, monkeypatch
):
    old_root = resources(tmp_path / "schema-070", cutoff="070")
    new_root = resources(tmp_path / "schema-071", cutoff="071")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    monkeypatch.setattr(bot_module, "WRITER_GENERATION", 10)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    new = None
    try:
        await old.migrate()
        lease = await old.acquire_host("generation-ten", "before-071", ttl_seconds=60)
        await old.release_host(lease)
        async with old.pool.connection() as old_connection:
            monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
            monkeypatch.setattr(bot_module, "WRITER_GENERATION", 11)
            new = BotStore(upgrade_database, "bot-a")
            await new.open()
            await new.migrate()
            async with new.pool.connection() as conn:
                gate = await (await conn.execute(
                    "SELECT minimum_generation FROM public.bot_schema_writer_gate"
                )).fetchone()
                column = await (await conn.execute(
                    "SELECT is_nullable,column_default FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='bot_context_management_requests' "
                    "AND column_name='mode'"
                )).fetchone()
            assert gate["minimum_generation"] == 11
            assert column == {"is_nullable": "NO", "column_default": None}

            with pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState,
                match="WRITER_SCHEMA_TOO_OLD",
            ):
                await old_connection.execute(
                    "UPDATE public.bot_host_lease "
                    "SET expires_at=clock_timestamp()+interval '1 minute'"
                )
            current = await new.acquire_host(
                "generation-eleven", "after-071", ttl_seconds=60
            )
            assert current.owner_epoch > lease.owner_epoch
    finally:
        if new is not None:
            await new.close()
        await old.close()
