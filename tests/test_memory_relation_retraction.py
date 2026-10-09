import asyncio
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
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

    async def invoke(operation, *, reason="INCORRECT", bot_id=None,
                     relation="relation/review", contribution="contribution/review"):
        body = {
            "relation_id": relation, "contribution_id": contribution,
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

    async def add_and_retract(suffix):
        relation = f"relation/review/{suffix}"
        contribution = f"contribution/review/{suffix}"
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.relation_contributions "
                "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                "(%s,%s,%s,%s,%s,'ASSOCIATES_WITH','ENTITY',%s,"
                "'ENTITY',%s,'MANUAL',900000,0,false)"
            ).format(namespace), (
                store.bot_id, contribution, f"relation/create/{suffix}",
                "sha256:" + "a" * 64, relation,
                f"entity/left/{suffix}", f"entity/right/{suffix}",
            ))
        result = await invoke(
            f"review/{suffix}", relation=relation, contribution=contribution,
        )
        assert result["ok"] is True

    async def history(body):
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_query_relation_retractions(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}), Jsonb(body),
            ))).fetchone()
        return row["result"]

    await add_and_retract("two")
    await add_and_retract("three")
    first_page = await history({"limit": 1, "reason_code": "INCORRECT"})
    assert first_page["ok"] is True
    output_schema = json.loads((
        Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/query-retractions-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(output_schema).validate(first_page["value"])
    assert [item["contribution_id"] for item in first_page["value"]["items"]] == [
        "contribution/review/three"
    ]
    await add_and_retract("late")
    cursor = first_page["value"]
    continuation = {
        "limit": 1, "reason_code": "INCORRECT",
        "snapshot_created_at": cursor["snapshot_created_at"],
        "snapshot_xid": cursor["snapshot_xid"],
        "cursor_filter_digest": cursor["filter_digest"],
        "after_created_at": cursor["next_cursor"]["created_at"],
        "after_operation_id": cursor["next_cursor"]["operation_id"],
    }
    second_page = await history(continuation)
    assert [item["contribution_id"] for item in second_page["value"]["items"]] == [
        "contribution/review/two"
    ]
    continuation.update(
        after_created_at=second_page["value"]["next_cursor"]["created_at"],
        after_operation_id=second_page["value"]["next_cursor"]["operation_id"],
    )
    third_page = await history(continuation)
    assert [item["contribution_id"] for item in third_page["value"]["items"]] == [
        "contribution/review"
    ]
    assert third_page["value"]["has_more"] is False
    stale_filter = await history({**continuation, "reason_code": "STALE"})
    assert stale_filter["error"]["code"] == "MEMORY_QUERY_INVALID"
    fresh = await history({"limit": 1, "reason_code": "INCORRECT"})
    assert fresh["value"]["items"][0]["contribution_id"] == "contribution/review/late"
    assert (await history({"limit": 1, "reason_code": "STALE"}))["value"]["items"] == []
