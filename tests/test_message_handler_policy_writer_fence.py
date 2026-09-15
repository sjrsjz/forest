"""Writer generation 20 fences pre-policy Bot connections."""

import psycopg
import pytest
from test_business_command_limits_writer_fence import resources
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "helper_name,changes_prior",
    [("migration_v19.py", True), ("migration_v20.py", False)],
)
def test_v20_checksum_binds_complete_writer_gate_chain(
    tmp_path, monkeypatch, helper_name, changes_prior
):
    root = resources(tmp_path / ("checksum-081-" + helper_name), cutoff="081")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    current = root / "bot_migrations" / "081_message_handler_policies.sql"
    prior = root / "bot_migrations" / "080_message_handler_registry.sql"
    before = {
        path.name: _migration_checksum(path.name, path.read_text(encoding="utf-8"))
        for path in (current, prior)
    }
    helper = root / helper_name
    helper.write_text(
        helper.read_text(encoding="utf-8") + "\n# changed v20 dependency\n",
        encoding="utf-8",
    )
    assert _migration_checksum(current.name, current.read_text(encoding="utf-8")) != before[
        current.name
    ]
    changed_prior = _migration_checksum(prior.name, prior.read_text(encoding="utf-8"))
    assert (changed_prior != before[prior.name]) is changes_prior


async def test_generation_nineteen_pool_cannot_write_after_handler_policy_cutover(
    upgrade_database, tmp_path, monkeypatch
):
    old_root = resources(tmp_path / "schema-080", cutoff="080")
    new_root = resources(tmp_path / "schema-081", cutoff="081")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old_root)
    monkeypatch.setattr(bot_module, "WRITER_GENERATION", 19)
    old = BotStore(upgrade_database, "bot-a")
    await old.open()
    new = None
    try:
        await old.migrate()
        async with old.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_web_issuers("
                "bot_id,issuer_id,key_revision,public_key,actions) VALUES "
                "('bot-a','legacy-web',1,'legacy-key',"
                "'[\"command.policy.read\",\"command.policy.apply\"]'::jsonb)"
            )
        lease = await old.acquire_host("generation-nineteen", "before-081", ttl_seconds=60)
        await old.release_host(lease)
        async with old.pool.connection() as old_connection:
            monkeypatch.setattr(bot_module, "files", lambda namespace: new_root)
            monkeypatch.setattr(bot_module, "WRITER_GENERATION", 20)
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
                        "SELECT to_regclass('public.bot_message_handler_policy_heads') AS heads,"
                        "to_regclass('public.bot_message_handler_policy_revisions') AS revisions"
                    )
                ).fetchone()
                issuer = await (
                    await conn.execute(
                        "SELECT actions FROM public.bot_web_issuers "
                        "WHERE bot_id='bot-a' AND issuer_id='legacy-web'"
                    )
                ).fetchone()
            assert gate["minimum_generation"] == 20
            assert tables == {
                "heads": "bot_message_handler_policy_heads",
                "revisions": "bot_message_handler_policy_revisions",
            }
            assert {
                "message.handler.policy.read",
                "message.handler.policy.plan",
                "message.handler.policy.apply",
            } <= set(issuer["actions"])
            with pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState,
                match="WRITER_SCHEMA_TOO_OLD",
            ):
                await old_connection.execute(
                    "UPDATE public.bot_host_lease "
                    "SET expires_at=clock_timestamp()+interval '1 minute'"
                )
            current = await new.acquire_host(
                "generation-twenty", "after-081", ttl_seconds=60
            )
            assert current.owner_epoch > lease.owner_epoch
    finally:
        if new is not None:
            await new.close()
        await old.close()
