"""The 067 cutover permanently excludes pre-v9 Core Shell writers."""

import psycopg
import pytest
from psycopg.rows import dict_row
from test_config_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "helper_name", ["migration_v7.py", "shell_payloads_v9.py", "shell_registry_v9.py"]
)
def test_v7_checksum_binds_gate_and_v9_core_contract(tmp_path, monkeypatch, helper_name):
    root = resources(tmp_path / ("checksum-067-" + helper_name), cutoff="067")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    current = root / "bot_migrations" / "067_schema_writer_v7.sql"
    prior = root / "bot_migrations" / "066_schema_writer_v6.sql"
    before = {
        path.name: _migration_checksum(path.name, path.read_text(encoding="utf-8"))
        for path in (current, prior)
    }
    helper = root / helper_name
    helper.write_text(
        helper.read_text(encoding="utf-8") + "\n# changed v7 dependency\n",
        encoding="utf-8",
    )
    assert _migration_checksum(current.name, current.read_text(encoding="utf-8")) != before[
        current.name
    ]
    assert _migration_checksum(prior.name, prior.read_text(encoding="utf-8")) == before[
        prior.name
    ]


async def test_v6_session_cannot_reacquire_after_core_protocol_cutover(
    upgrade_database, tmp_path, monkeypatch,
):
    old_root = resources(tmp_path / "schema-066", cutoff="066")
    new_root = resources(tmp_path / "schema-067", cutoff="067")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    await old.migrate()
    old_connection = await psycopg.AsyncConnection.connect(
        upgrade_database,
        options="-c forest.writer_generation=6",
        row_factory=dict_row,
    )
    new = BotStore(upgrade_database, "bot-a")
    await new.open()
    try:
        monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
        await new.migrate()
        async with new.pool.connection() as conn:
            gate = await (
                await conn.execute(
                    "SELECT minimum_generation FROM public.bot_schema_writer_gate"
                )
            ).fetchone()
        assert gate["minimum_generation"] == 7
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"
        ):
            await old_connection.execute(
                "UPDATE public.bot_host_lease "
                "SET expires_at=clock_timestamp()+interval '1 minute'"
            )
        fresh = await new.acquire_host("v7", "after-067", ttl_seconds=60)
        assert fresh.owner_epoch >= 1
    finally:
        await old_connection.close()
        await new.close()
        await old.close()
