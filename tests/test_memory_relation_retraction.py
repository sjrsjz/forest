import asyncio
from pathlib import Path

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_relation_retraction_converges_and_removes_new_recall_edges(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"), store, installer, kernel=kernel,
        initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions "
            "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
            "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
            "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
            "(%s,'contribution/review','relation/create',%s,'relation/review',"
            "'ASSOCIATES_WITH','ENTITY','entity/left','ENTITY','entity/right',"
            "'MANUAL',900000,0,false)"
        ).format(namespace), (store.bot_id, "sha256:" + "a" * 64))

    async def invoke(operation, *, reason="INCORRECT", bot_id=None):
        body = {
            "relation_id": "relation/review", "contribution_id": "contribution/review",
            "reason_code": reason, "requested_by": "admin/test",
        }
        async with store.pool.connection() as conn, conn.transaction():
            result = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_retract_management_relation(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": bot_id or store.bot_id, "operation_id": operation}),
                Jsonb(body),
            ))).fetchone()
        return result["result"]

    async with store.pool.connection() as conn:
        before = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_relations(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}), Jsonb({"limit": 10}),
        ))).fetchone()
    assert [item["contribution_id"] for item in before["result"]["value"]["items"]] == [
        "contribution/review"
    ]
    foreign = await invoke("review/foreign", bot_id="bot/foreign")
    assert foreign["error"]["code"] == "MEMORY_RELATION_NOT_FOUND"
    first, second = await asyncio.gather(invoke("review/one"), invoke("review/two"))
    success = first if first["ok"] else second
    rejection = second if first["ok"] else first
    assert success["value"]["contribution_id"] == "contribution/review"
    assert rejection["error"]["code"] == "MEMORY_RELATION_ALREADY_RETRACTED"
    successful_operation = "review/one" if first["ok"] else "review/two"
    assert await invoke(successful_operation) == success
    changed = await invoke(successful_operation, reason="STALE")
    assert changed["error"]["code"] == "MEMORY_RELATION_RETRACTION_CONFLICT"
    async with store.pool.connection() as conn:
        record = await (await conn.execute(sql.SQL(
            "SELECT c.active,r.actor_subject_ref,r.reason_code FROM "
            "{}.relation_contributions c JOIN {}.relation_retractions r "
            "ON r.bot_id=c.bot_id AND r.contribution_id=c.contribution_id "
            "WHERE c.bot_id=%s AND c.contribution_id='contribution/review'"
        ).format(namespace, namespace), (store.bot_id,))).fetchone()
        page = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_relations(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}), Jsonb({"limit": 10}),
        ))).fetchone()
    assert record == {
        "active": False, "actor_subject_ref": "admin/test", "reason_code": "INCORRECT",
    }
    assert page["result"]["value"]["items"] == []
