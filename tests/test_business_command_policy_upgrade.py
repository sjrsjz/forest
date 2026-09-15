"""Migration 073 upgrades existing issuer authority for command policies."""

import pytest
from psycopg.types.json import Jsonb
from test_business_command_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore

pytestmark = pytest.mark.postgres


async def test_existing_bot_issuer_receives_command_policy_actions(
    upgrade_database, tmp_path, monkeypatch
):
    old_root = resources(tmp_path / "schema-072", cutoff="072")
    new_root = resources(tmp_path / "schema-073", cutoff="073")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    upgraded = None
    try:
        await old.migrate()
        async with old.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_web_issuers("
                "bot_id,issuer_id,key_revision,public_key,actions) VALUES (%s,%s,1,%s,%s)",
                ("bot-a", "existing-web", "existing-key", Jsonb(["config.read"])),
            )
        await old.close()
        monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
        upgraded = BotStore(upgrade_database, "bot-a")
        await upgraded.open()
        await upgraded.migrate()
        async with upgraded.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT actions FROM public.bot_web_issuers WHERE bot_id='bot-a'"
            )).fetchone()
            tables = await (await conn.execute(
                "SELECT to_regclass('public.bot_command_policy_heads') AS heads,"
                "to_regclass('public.bot_command_policy_revisions') AS revisions"
            )).fetchone()
        assert set(row["actions"]) == {
            "config.read", "command.policy.read", "command.policy.plan", "command.policy.apply"
        }
        assert tables == {
            "heads": "bot_command_policy_heads", "revisions": "bot_command_policy_revisions"
        }
    finally:
        if upgraded is not None:
            await upgraded.close()
        elif not old.pool.closed:
            await old.close()
