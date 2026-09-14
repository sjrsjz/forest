"""The 069 cutover permanently excludes pre-v11 module lifecycle writers."""

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_config_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "helper_name",
    ["migration_v9.py", "shell_payloads_v11.py", "shell_registry_v11.py"],
)
def test_v9_checksum_binds_writer_fence_and_v11_contract(
    tmp_path, monkeypatch, helper_name
):
    root = resources(tmp_path / ("checksum-069-" + helper_name), cutoff="069")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    current = root / "bot_migrations" / "069_module_lifecycle.sql"
    prior = root / "bot_migrations" / "068_shell_priority_qualification.sql"
    before = {
        path.name: _migration_checksum(path.name, path.read_text(encoding="utf-8"))
        for path in (current, prior)
    }
    helper = root / helper_name
    helper.write_text(
        helper.read_text(encoding="utf-8") + "\n# changed v9 dependency\n",
        encoding="utf-8",
    )
    assert _migration_checksum(current.name, current.read_text(encoding="utf-8")) != before[
        current.name
    ]
    assert _migration_checksum(prior.name, prior.read_text(encoding="utf-8")) == before[
        prior.name
    ]


async def test_v8_session_is_fenced_and_scoped_issuers_keep_least_privilege(
    upgrade_database, tmp_path, monkeypatch,
):
    old_root = resources(tmp_path / "schema-068", cutoff="068")
    new_root = resources(tmp_path / "schema-069", cutoff="069")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    await store.migrate()
    old_connection = await psycopg.AsyncConnection.connect(
        upgrade_database,
        options="-c forest.writer_generation=8",
        row_factory=dict_row,
    )
    scoped = {
        "reader": ["config.read"],
        "planner": ["config.read", "config.plan"],
        "applier": ["config.apply"],
        "reloader": ["config.plan", "config.apply"],
        "operator": ["config.read", "config.plan", "config.apply", "message.cancel"],
        "empty-disabled": [],
    }
    try:
        async with store.pool.connection() as conn:
            await conn.cursor().executemany(
                "INSERT INTO public.bot_web_issuers"
                "(bot_id,issuer_id,key_revision,public_key,enabled,actions) "
                "VALUES ('bot-a',%s,1,'test-key',%s,%s)",
                [
                    (issuer, issuer != "empty-disabled", Jsonb(actions))
                    for issuer, actions in scoped.items()
                ],
            )
        monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
        await store.migrate()
        async with store.pool.connection() as conn:
            gate = await (
                await conn.execute(
                    "SELECT minimum_generation FROM public.bot_schema_writer_gate"
                )
            ).fetchone()
            rows = await (
                await conn.execute(
                    "SELECT issuer_id,enabled,actions FROM public.bot_web_issuers "
                    "WHERE issuer_id=ANY(%s) ORDER BY issuer_id",
                    (list(scoped),),
                )
            ).fetchall()
        assert gate["minimum_generation"] == 9
        actual = {row["issuer_id"]: (row["enabled"], set(row["actions"])) for row in rows}
        assert actual["reader"] == (
            True, {"config.read", "module.read", "module.validate"}
        )
        assert actual["planner"] == (
            True, {"config.read", "config.plan", "module.read", "module.validate"}
        )
        assert actual["applier"] == (
            True, {"config.apply", "module.enable"}
        )
        assert actual["reloader"] == (
            True, {"config.plan", "config.apply", "module.reload", "module.enable"}
        )
        assert actual["operator"] == (
            True,
            {
                "config.read", "config.plan", "config.apply", "message.cancel",
                "module.read", "module.validate", "module.reload", "module.enable",
                "module.disable",
            },
        )
        assert actual["empty-disabled"] == (False, set())
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"
        ):
            await old_connection.execute(
                "UPDATE public.bot_host_lease "
                "SET expires_at=clock_timestamp()+interval '1 minute'"
            )
        fresh = await store.acquire_host("v9", "after-069", ttl_seconds=60)
        assert fresh.owner_epoch >= 1
    finally:
        await old_connection.close()
        await store.close()
