import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.kernel_payload_participant import KernelPayloadUnitOfWorkParticipant
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

    async def graph_command(function, context, body):
        if function == "memory_publish_graph_rebuild":
            body = {
                "protocol": "forest.uow-step/1",
                "caller": {"component_id": "memory.graph", "generation_id": "test"},
                "input": {
                    "protocol": "forest.memory-graph-publish/1",
                    "deletion_epoch_step_id": "payload_epoch",
                    **body,
                },
                "prior": {"payload_epoch": {
                    "protocol": "forest.payload-epoch/1", "deletion_epoch": 0,
                }},
            }
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.{}(%s::jsonb,%s::jsonb) AS result"
            ).format(namespace, sql.Identifier(function)), (
                Jsonb(context), Jsonb(body),
            ))).fetchone()
        return row["result"]

    async def begin_same_candidate():
        return await graph_command(
            "memory_begin_graph_rebuild",
            {"bot_id": store.bot_id, "operation_id": "rebuild/concurrent"},
            {"generation_id": "candidate/concurrent"},
        )

    first, duplicate = await asyncio.gather(
        begin_same_candidate(), begin_same_candidate())
    assert first["ok"] and duplicate == first
    for revision in range(1, 5):
        replayed = await graph_command(
            "memory_replay_graph_revision", {"bot_id": store.bot_id},
            {"generation_id": "candidate/concurrent", "revision": revision},
        )
        assert replayed["ok"] and replayed["value"]["replayed_through"] == revision
    async with store.pool.connection() as conn:
        rebuilt = await (await conn.execute(sql.SQL(
            "SELECT contribution_id,from_revision,to_revision "
            "FROM {}.graph_edge_versions "
            "WHERE bot_id=%s AND generation_id='candidate/concurrent' "
            "ORDER BY from_revision"
        ).format(namespace), (store.bot_id,))).fetchall()
    assert rebuilt == edges[:3]
    async with store.pool.connection() as conn, conn.transaction():
        await insert_relation(conn, "late")
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_queries("
            "bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'graph/old','graph/old-operation',%s,'ADMIN_TRIAL','TEXT',"
            "'independent-facets/1',0)"
        ).format(namespace), (store.bot_id, digest))
    publish_context = {"bot_id": store.bot_id, "operation_id": "publish/concurrent"}
    stale = await graph_command(
        "memory_publish_graph_rebuild", publish_context,
        {"generation_id": "candidate/concurrent",
         "expected_active_generation_id": "primary",
         "expected_active_revision": 4},
    )
    assert stale["error"]["code"] == "MEMORY_GRAPH_PUBLISH_STALE"
    still_behind = await graph_command(
        "memory_publish_graph_rebuild", publish_context,
        {"generation_id": "candidate/concurrent",
         "expected_active_generation_id": "primary",
         "expected_active_revision": 5},
    )
    assert still_behind["error"]["code"] == "MEMORY_GRAPH_PUBLISH_STALE"
    caught_up = await graph_command(
        "memory_replay_graph_revision", {"bot_id": store.bot_id},
        {"generation_id": "candidate/concurrent", "revision": 5},
    )
    assert caught_up["value"]["replayed_through"] == 5
    payload_participant = KernelPayloadUnitOfWorkParticipant(
        SimpleNamespace(generation_identity=SimpleNamespace(
            generation_id="payload/test")),
        kernel.payload,
    )
    async def advance_payload_epoch():
        async with store.pool.connection() as other, other.transaction():
            await other.execute(sql.SQL(
                "UPDATE {}.deletion_clock "
                "SET deletion_epoch=deletion_epoch+1 WHERE bot_id=%s"
            ).format(sql.Identifier(kernel.payload.namespace)), (store.bot_id,))

    async with store.pool.connection() as conn, conn.transaction():
        epoch = await payload_participant.invoke_in(
            conn, "payload.capture-deletion-epoch", {
                "protocol": "forest.uow-step/1",
                "caller": {"component_id": "memory.graph",
                           "generation_id": "test"},
                "input": {"protocol": "forest.payload-epoch-capture/1"},
                "prior": {},
            })
        clock_writer = asyncio.create_task(advance_payload_epoch())
        await asyncio.sleep(0.1)
        assert not clock_writer.done()
        body = {
            "protocol": "forest.memory-graph-publish/1",
            "deletion_epoch_step_id": "payload_epoch",
            "generation_id": "candidate/concurrent",
            "expected_active_generation_id": "primary",
            "expected_active_revision": 5,
        }
        envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "memory.graph", "generation_id": "test"},
            "input": body,
            "prior": {"payload_epoch": epoch},
        }
        published = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_publish_graph_rebuild(%s::jsonb,%s::jsonb) AS result"
        ).format(namespace), (
            Jsonb(publish_context), Jsonb(envelope),
        ))).fetchone())["result"]
    await asyncio.wait_for(clock_writer, 5)
    assert published["value"] == {
        "generation_id": "candidate/concurrent",
        "previous_generation_id": "primary",
        "graph_revision": 5,
        "deletion_epoch": 0,
    }
    assert await graph_command(
        "memory_publish_graph_rebuild", publish_context,
        {"generation_id": "candidate/concurrent",
         "expected_active_generation_id": "primary",
         "expected_active_revision": 5},
    ) == published
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_queries("
            "bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'graph/new','graph/new-operation',%s,'ADMIN_TRIAL','TEXT',"
            "'independent-facets/1',0)"
        ).format(namespace), (store.bot_id, digest))
        await insert_relation(conn, "after-switch")
    async with store.pool.connection() as conn:
        frozen = await (await conn.execute(sql.SQL(
            "SELECT query_id,graph_generation_id,graph_revision "
            "FROM {}.memory_queries WHERE bot_id=%s "
            "AND query_id IN ('graph/old','graph/new') ORDER BY query_id"
        ).format(namespace), (store.bot_id,))).fetchall()
        head = await (await conn.execute(sql.SQL(
            "SELECT active_generation_id,active_graph_revision "
            "FROM {}.graph_projection_heads WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchone()
        generation = await (await conn.execute(sql.SQL(
            "SELECT generation_id FROM {}.graph_revisions "
            "WHERE bot_id=%s AND graph_revision=6"
        ).format(namespace), (store.bot_id,))).fetchone()
    assert frozen == [
        {"query_id": "graph/new", "graph_generation_id": "candidate/concurrent",
         "graph_revision": 5},
        {"query_id": "graph/old", "graph_generation_id": "primary",
         "graph_revision": 5},
    ]
    assert head == {"active_generation_id": "candidate/concurrent",
                    "active_graph_revision": 6}
    assert generation["generation_id"] == "candidate/concurrent"


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
