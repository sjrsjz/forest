"""Upgrade original 001 authorities containing facts, not just empty databases."""

from __future__ import annotations

import hashlib
import os
from importlib.resources import files
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from retention_upgrade import content as retained_content

from forest.storage.bot import BotStore
from forest.storage.postgres import PostgresStore


@pytest_asyncio.fixture
async def upgrade_database():
    test_dsn = os.environ.get("FOREST_TEST_DSN")
    if not test_dsn:
        pytest.skip("FOREST_TEST_DSN must select a private PostgreSQL test instance")
    name = "forest_upgrade_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(test_dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    try:
        yield make_conninfo(test_dsn, dbname=name)
    finally:
        assert name.startswith("forest_upgrade_") and len(name) == 47
        assert int(name.removeprefix("forest_upgrade_"), 16) >= 0
        assert name != conninfo_to_dict(test_dsn)["dbname"]
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def original_schema(dsn, *, bot):
    version = "001_kernel.sql" if bot else "001_deployment.sql"
    root = "bot_migrations" if bot else "migrations"
    source = files("forest.storage").joinpath(root, version).read_text(encoding="utf-8")
    ledger = "forest_bot_schema_migrations" if bot else "forest_schema_migrations"
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        async with conn.transaction():
            await conn.execute(sql.SQL(
                "CREATE TABLE {} (version text PRIMARY KEY,checksum text NOT NULL,"
                "applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
            ).format(sql.Identifier(ledger)))
            await conn.execute(source)
            await conn.execute(sql.SQL("INSERT INTO {}(version,checksum) VALUES (%s,%s)").format(
                sql.Identifier(ledger)), (version, hashlib.sha256(source.encode()).hexdigest()))


@pytest.mark.postgres
async def test_deployment_001_upgrade_preserves_account_setup_and_live_session(upgrade_database):
    await original_schema(upgrade_database, bot=False)
    async with await psycopg.AsyncConnection.connect(upgrade_database, autocommit=True) as conn:
        await conn.execute(
            "INSERT INTO web_accounts(id,username,password_hash,role) VALUES ('old-admin','admin','keep-hash','admin')"
        )
        await conn.execute(
            "INSERT INTO web_setup_state(singleton,setup_id,plan_digest,account_id) "
            "VALUES (true,'old-setup','old-plan','old-admin')"
        )
        await conn.execute(
            "INSERT INTO web_sessions(token_hash,csrf_hash,account_id,expires_at) "
            "VALUES ('old-token','old-csrf','old-admin',clock_timestamp()+interval '1 hour')"
        )
    store = PostgresStore(upgrade_database)
    try:
        await store.open()
        await store.migrate()
        assert (await store.find_account("admin"))["password_hash"] == "keep-hash"
        assert (await store.setup_status("old-setup"))["plan_digest"] == "old-plan"
        assert (await store.get_session("old-token"))["account_id"] == "old-admin"
        async with store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT revision FROM web_sessions WHERE token_hash='old-token'"
            )).fetchone()
            assert row["revision"] == 1
        await store.revoke_session("old-token")
        async with store.pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT revision FROM web_sessions WHERE token_hash='old-token'"
            )).fetchone()
            assert row["revision"] == 2
        assert await store.get_session("old-token") is None
    finally:
        await store.close()


@pytest.mark.postgres
async def test_bot_001_upgrade_preserves_results_state_and_backfills_root_identity(upgrade_database):
    await original_schema(upgrade_database, bot=True)
    async with await psycopg.AsyncConnection.connect(upgrade_database, autocommit=True) as conn:
        await conn.execute("INSERT INTO forest_bot_identity(bot_id) VALUES ('old-bot')")
        await conn.execute("INSERT INTO bot_runtime_state(bot_id) VALUES ('old-bot')")
        await conn.execute("INSERT INTO bot_host_lease(bot_id) VALUES ('old-bot')")
        await conn.execute(
            "INSERT INTO bot_generations(bot_id,generation_id,component_id,package_digest,config_digest,config,"
            "instance_id,worker_incarnation,host_incarnation,host_owner_epoch,status) VALUES "
            "('old-bot','old-gen','counter','package','config','{}','instance','worker','host',1,'ACTIVE')"
        )
        await conn.execute(
            "INSERT INTO bot_calls(bot_id,call_id,operation_id,component_id,generation_id,route_epoch,"
            "worker_incarnation,host_id,host_incarnation,owner_epoch,storage_generation,maintenance_epoch,"
            "policy_revision,deadline_at,input_digest,status,result) VALUES "
            "('old-bot','old-call','old-op','counter','old-gen',1,'worker','host-id','host',1,1,1,1,"
            "clock_timestamp(),'input','COMPLETED','{\"value\":7}')"
        )
        await conn.execute(
            "INSERT INTO bot_component_state(bot_id,component_id,key,revision,value) "
            "VALUES ('old-bot','counter','counter',7,'7')"
        )
    store = BotStore(upgrade_database, "old-bot")
    try:
        await store.open()
        await store.migrate()
        async with store.pool.connection() as conn:
            call = await (await conn.execute(
                "SELECT root_call_id,parent_call_id,call_depth,dependency_components,status,input_digest,"
                "result,result_payload_id,result_digest "
                "FROM bot_calls WHERE call_id='old-call'"
            )).fetchone()
            original_call = {"root_call_id": "old-call", "parent_call_id": None, "call_depth": 0,
                             "dependency_components": [], "status": "COMPLETED", "input_digest": "input"}
            assert {key: call[key] for key in original_call} == original_call
            # The old columns are sealed after 037. Independently audit their
            # referenced bytes rather than requiring a second plaintext copy.
            assert call["result"] is None and call["result_payload_id"]
            assert call["result_digest"] == "sha256:" + hashlib.sha256(b'{"value":7}').hexdigest()
            assert await retained_content(conn, "old-bot", call["result_payload_id"]) == {"value": 7}
            state = await (await conn.execute(
                "SELECT revision,value,value_payload_id,value_digest FROM bot_component_state WHERE key='counter'"
            )).fetchone()
            assert state["revision"] == 7 and state["value"] is None and state["value_payload_id"]
            assert state["value_digest"] == "sha256:" + hashlib.sha256(b"7").hexdigest()
            assert await retained_content(conn, "old-bot", state["value_payload_id"]) == 7
            grants = await (await conn.execute(
                "SELECT capability_grants FROM bot_generations WHERE generation_id='old-gen'"
            )).fetchone()
            assert grants["capability_grants"] == []  # Migration invents no historical grants.
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                await conn.execute("UPDATE bot_calls SET root_call_id='rewritten' WHERE call_id='old-call'")
    finally:
        await store.close()
