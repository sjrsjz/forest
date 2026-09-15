"""Writer generation 22 fences pre-execution Bot connections."""

import psycopg
import pytest
from test_config_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "helper_name,changes_prior",
    [("migration_v21.py", True), ("migration_v22.py", False)],
)
def test_v22_checksum_binds_complete_writer_gate_chain(
    tmp_path, monkeypatch, helper_name, changes_prior
):
    root = resources(tmp_path / ("checksum-083-" + helper_name), cutoff="083")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    current = root / "bot_migrations" / "083_message_handler_execution.sql"
    prior = root / "bot_migrations" / "082_message_handler_chains.sql"
    before = {
        path.name: _migration_checksum(path.name, path.read_text(encoding="utf-8"))
        for path in (current, prior)
    }
    helper = root / helper_name
    helper.write_text(
        helper.read_text(encoding="utf-8") + "\n# changed v22 dependency\n",
        encoding="utf-8",
    )
    assert _migration_checksum(current.name, current.read_text(encoding="utf-8")) != before[
        current.name
    ]
    changed_prior = _migration_checksum(prior.name, prior.read_text(encoding="utf-8"))
    assert (changed_prior != before[prior.name]) is changes_prior


async def test_generation_twenty_one_pool_cannot_write_after_execution_cutover(
    upgrade_database, tmp_path, monkeypatch
):
    old_root = resources(tmp_path / "schema-082", cutoff="082")
    new_root = resources(tmp_path / "schema-083", cutoff="083")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    monkeypatch.setattr(bot_module, "WRITER_GENERATION", 21)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    new = None
    try:
        await old.migrate()
        lease = await old.acquire_host("generation-twenty-one", "before-083", ttl_seconds=60)
        await old.release_host(lease)
        async with old.pool.connection() as old_connection:
            monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
            monkeypatch.setattr(bot_module, "WRITER_GENERATION", 22)
            new = BotStore(upgrade_database, "bot-a")
            await new.open()
            await new.migrate()
            async with new.pool.connection() as conn:
                gate = await (
                    await conn.execute(
                        "SELECT minimum_generation FROM public.bot_schema_writer_gate"
                    )
                ).fetchone()
                tables = await (
                    await conn.execute(
                        "SELECT to_regclass('public.bot_message_handler_attempts') AS attempts,"
                        "to_regclass('public.bot_message_handler_results') AS results,"
                        "to_regclass('public.bot_message_handler_failures') AS failures"
                    )
                ).fetchone()
            assert gate["minimum_generation"] == 22
            assert tables == {
                "attempts": "bot_message_handler_attempts",
                "results": "bot_message_handler_results",
                "failures": "bot_message_handler_failures",
            }
            with pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState,
                match="WRITER_SCHEMA_TOO_OLD",
            ):
                await old_connection.execute(
                    "UPDATE public.bot_host_lease "
                    "SET expires_at=clock_timestamp()+interval '1 minute'"
                )
            current = await new.acquire_host(
                "generation-twenty-two", "after-083", ttl_seconds=60
            )
            assert current.owner_epoch > lease.owner_epoch
    finally:
        if new is not None:
            await new.close()
        await old.close()
