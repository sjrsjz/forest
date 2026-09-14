"""The 060 cutover permanently excludes pre-payload Job writers."""

import psycopg
import pytest
from psycopg.rows import dict_row
from test_config_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

pytestmark = pytest.mark.postgres


def test_v3_checksums_bind_gate_helper_and_job_handler(tmp_path, monkeypatch):
    root = resources(tmp_path / "checksum-061", cutoff="061")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    gate = root / "bot_migrations" / "060_schema_writer_v3.sql"
    jobs = root / "bot_migrations" / "061_job_payloads.sql"
    prior = root / "bot_migrations" / "049_binding_template_dependencies.sql"
    original = {
        path.name: _migration_checksum(path.name, path.read_text(encoding="utf-8"))
        for path in (gate, jobs, prior)
    }
    helper = root / "migration_v3.py"
    helper.write_text(helper.read_text(encoding="utf-8") + "\n# changed v3 helper\n", encoding="utf-8")
    assert _migration_checksum(gate.name, gate.read_text(encoding="utf-8")) != original[gate.name]
    assert _migration_checksum(jobs.name, jobs.read_text(encoding="utf-8")) != original[jobs.name]
    assert _migration_checksum(prior.name, prior.read_text(encoding="utf-8")) == original[prior.name]


async def test_v2_session_cannot_reacquire_after_atomic_job_payload_cutover(
    upgrade_database, tmp_path, monkeypatch,
):
    old_root = resources(tmp_path / "schema-059", cutoff="059")
    new_root = resources(tmp_path / "schema-061", cutoff="061")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    await old.migrate()
    lease = await old.acquire_host("v2", "before-060", ttl_seconds=60)
    await old.release_host(lease)

    old_connection = await psycopg.AsyncConnection.connect(
        upgrade_database,
        options="-c forest.writer_generation=2",
        row_factory=dict_row,
    )
    new = BotStore(upgrade_database, "bot-a")
    await new.open()
    try:
        monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
        await new.migrate()
        async with new.pool.connection() as conn:
            gate = await (await conn.execute(
                "SELECT minimum_generation FROM public.bot_schema_writer_gate"
            )).fetchone()
        assert gate["minimum_generation"] == 3
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"):
            await old_connection.execute(
                "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '1 minute'"
            )
        fresh = await new.acquire_host("v3", "after-061", ttl_seconds=60)
        assert fresh.owner_epoch > lease.owner_epoch
    finally:
        await old_connection.close()
        await new.close()
        await old.close()
