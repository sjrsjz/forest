import asyncio
from pathlib import Path

import pytest
from psycopg import sql
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_topology_revision_serializes_writers_and_rollback_leaves_no_gap(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"), store, installer,
        kernel=kernel, initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64

    async def insert_relation(conn, name):
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions("
            "bot_id,contribution_id,operation_id,input_digest,relation_id,"
            "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
            "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
            "(%s,%s,%s,%s,%s,'ASSOCIATES_WITH','ENTITY','left',"
            "'ENTITY',%s,'RULE',500000,0,false)"
        ).format(namespace), (
            store.bot_id, "contribution/" + name, "operation/" + name,
            digest, "relation/" + name, "right/" + name,
        ))

    async def second_writer():
        async with store.pool.connection() as conn, conn.transaction():
            await insert_relation(conn, "second")

    async with store.pool.connection() as first, first.transaction():
        await insert_relation(first, "first")
        contender = asyncio.create_task(second_writer())
        await asyncio.sleep(0.1)
        assert not contender.done()
    await asyncio.wait_for(contender, 5)

    with pytest.raises(RuntimeError, match="rollback"):
        async with store.pool.connection() as conn, conn.transaction():
            await insert_relation(conn, "aborted")
            raise RuntimeError("rollback")
    async with store.pool.connection() as conn, conn.transaction():
        await insert_relation(conn, "third")
    async with store.pool.connection() as conn, conn.transaction():
        await insert_relation(conn, "coalesced")
        await conn.execute(sql.SQL(
            "UPDATE {}.relation_contributions SET active=false "
            "WHERE bot_id=%s AND contribution_id='contribution/coalesced'"
        ).format(namespace), (store.bot_id,))
    async with store.pool.connection() as conn:
        rows = await (await conn.execute(sql.SQL(
            "SELECT revision,entity_id,after_value->>'active' AS active,"
            "before_value IS NULL AS created "
            "FROM {}.graph_topology_events "
            "WHERE bot_id=%s ORDER BY revision,event_kind,entity_id"
        ).format(namespace), (store.bot_id,))).fetchall()
        head = await (await conn.execute(sql.SQL(
            "SELECT revision FROM {}.graph_topology_journal_heads WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchone()
    assert rows == [
        {"revision": 1, "entity_id": "contribution/first",
         "active": "true", "created": True},
        {"revision": 2, "entity_id": "contribution/second",
         "active": "true", "created": True},
        {"revision": 3, "entity_id": "contribution/third",
         "active": "true", "created": True},
        {"revision": 4, "entity_id": "contribution/coalesced",
         "active": "false", "created": True},
    ]
    assert head["revision"] == 4
    async with store.pool.connection() as conn:
        published = await (await conn.execute(sql.SQL(
            "SELECT active_graph_revision FROM {}.graph_projection_heads "
            "WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchone()
        edges = await (await conn.execute(sql.SQL(
            "SELECT contribution_id,from_revision,to_revision "
            "FROM {}.graph_edge_versions WHERE bot_id=%s "
            "ORDER BY from_revision"
        ).format(namespace), (store.bot_id,))).fetchall()
    assert published["active_graph_revision"] == 4
    assert edges == [
        {"contribution_id": "contribution/first",
         "from_revision": 1, "to_revision": None},
        {"contribution_id": "contribution/second",
         "from_revision": 2, "to_revision": None},
        {"contribution_id": "contribution/third",
         "from_revision": 3, "to_revision": None},
        {"contribution_id": "contribution/coalesced",
         "from_revision": 4, "to_revision": 4},
    ]


@pytest.mark.postgres
async def test_empty_query_freezes_real_graph_head(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "empty-graph-packages"), store, installer,
        kernel=kernel, initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_queries("
            "bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'empty/query','empty/operation',%s,'ADMIN_TRIAL','TEXT',"
            "'independent-facets/1',0)"
        ).format(namespace), (store.bot_id, "sha256:" + "a" * 64))
    async with store.pool.connection() as conn:
        row = await (await conn.execute(sql.SQL(
            "SELECT q.graph_generation_id,q.graph_revision,"
            "h.active_generation_id,h.active_graph_revision "
            "FROM {}.memory_queries q JOIN {}.graph_projection_heads h "
            "ON h.bot_id=q.bot_id WHERE q.bot_id=%s AND q.query_id='empty/query'"
        ).format(namespace, namespace), (store.bot_id,))).fetchone()
    assert row == {
        "graph_generation_id": "primary", "graph_revision": 0,
        "active_generation_id": "primary", "active_graph_revision": 0,
    }
