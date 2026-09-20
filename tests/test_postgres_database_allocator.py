from __future__ import annotations

import asyncio
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from forest.errors import ForestError
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator


def test_allocator_derives_stable_database_without_exposing_credentials():
    allocator = LocalPostgresDatabaseAllocator(
        "postgresql://forest:do-not-print@postgres:5432/forest_deployment"
    )
    assert allocator.database_name("bot/main") == "forest_bot_f83841cfcd19fac75ef9571f"
    assert allocator.role_name("bot/main") == "forest_role_f83841cfcd19fac75ef9571f"
    assert allocator.target_dsn("bot/main", "x" * 32) == (
        "postgresql://forest_role_f83841cfcd19fac75ef9571f:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@"
        "postgres:5432/"
        "forest_bot_f83841cfcd19fac75ef9571f"
    )
    assert "do-not-print" not in repr(allocator)


@pytest.mark.postgres
async def test_allocator_creates_once_under_concurrent_replay_and_rejects_other_targets():
    root_dsn = os.environ.get("FOREST_TEST_DSN")
    if not root_dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    bot_id = "bot/allocator/" + uuid4().hex
    allocator = LocalPostgresDatabaseAllocator(root_dsn)
    name = allocator.database_name(bot_id)
    role_name = allocator.role_name(bot_id)
    target_dsn = allocator.new_target_dsn(bot_id)
    parsed = urlsplit(root_dsn)
    admin_dsn = urlunsplit(parsed._replace(path="/postgres"))
    admin = await psycopg.AsyncConnection.connect(
        admin_dsn,
        autocommit=True,
        row_factory=dict_row,
    )
    try:
        await asyncio.gather(
            *(allocator.ensure_database(bot_id=bot_id, target_dsn=target_dsn) for _ in range(4))
        )
        row = await (
            await admin.execute(
                "SELECT pg_get_userbyid(datdba) AS owner FROM pg_database WHERE datname=%s",
                (name,),
            )
        ).fetchone()
        assert row == {"owner": role_name}
        role = await (
            await admin.execute(
                "SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls,"
                "rolcanlogin FROM pg_roles WHERE rolname=%s",
                (role_name,),
            )
        ).fetchone()
        assert role == {
            "rolsuper": False,
            "rolcreaterole": False,
            "rolcreatedb": False,
            "rolreplication": False,
            "rolbypassrls": False,
            "rolcanlogin": True,
        }
        target = await psycopg.AsyncConnection.connect(target_dsn)
        await target.close()
        with pytest.raises(ForestError) as failure:
            await allocator.ensure_database(bot_id=bot_id, target_dsn=root_dsn)
        assert failure.value.code == "STORAGE_ALLOCATION_SCOPE_MISMATCH"
    finally:
        assert name.startswith("forest_bot_") and len(name) == 35
        await admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )
        await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role_name)))
        await admin.close()
