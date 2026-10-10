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
        digest = "sha256:" + "a" * 64
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources "
            "(bot_id,source_id,registration_operation_id,input_digest,source_kind,"
            "platform,realm_id,external_group_id,identity_incarnation,identity_digest,"
            "trunk_id) VALUES (%s,'source/review','source/register',%s,'GROUP',"
            "'onebot','qq','933179609',1,%s,'trunk/review')"
        ).format(namespace), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trunks(bot_id,trunk_id,source_id) "
            "VALUES (%s,'trunk/review','source/review')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_units "
            "(bot_id,source_unit_id,capture_operation_id,input_digest,source_id,"
            "trunk_id,source_order_seq,source_revision,unit_kind,modality,part_count,"
            "content_manifest_digest,author_subject_ref,author_payload_id,"
            "author_payload_digest,author_owner_component_id,author_owner_kind,"
            "author_owner_id,accepted_deletion_epoch,eligibility_state,"
            "eligible_at,eligible_xid,occurred_at) VALUES "
            "(%s,'unit/review','capture/review',%s,'source/review','trunk/review',"
            "1,1,'MESSAGE','TEXT',20,%s,'qq:42','payload/author',%s,"
            "'memory.graph','test','unit/review',0,'ELIGIBLE',"
            "clock_timestamp(),pg_current_xact_id(),clock_timestamp())"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_parts "
            "(bot_id,source_unit_id,part_id,part_index,part_kind,media_type,"
            "payload_id,payload_digest,payload_owner_component_id,"
            "payload_owner_kind,payload_owner_id) "
            "SELECT %s,'unit/review','part/review/'||n,n,'TEXT','text/plain',"
            "'payload/review/'||n,%s,'memory.graph','test','unit/review' "
            "FROM generate_series(0,19) n"
        ).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions "
            "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
            "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
            "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
            "(%s,'contribution/review','relation/create',%s,'relation/review',"
            "'ASSOCIATES_WITH','ENTITY','entity/left','ENTITY','entity/right',"
            "'MANUAL',900000,0,false)"
        ).format(namespace), (store.bot_id, "sha256:" + "a" * 64))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_evidence "
            "(bot_id,contribution_id,evidence_index,source_unit_id,"
            "source_revision,part_id) "
            "SELECT %s,'contribution/review',n,'unit/review',1,'part/review/'||n "
            "FROM generate_series(0,19) n"
        ).format(namespace), (store.bot_id,))

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
    assert before["result"]["value"]["items"][0]["review_state"] == "UNREVIEWED"

    async def approve(operation, *, relation="relation/review"):
        body = {
            "relation_id": relation, "contribution_id": "contribution/review",
            "requested_by": "admin/test",
        }
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_approve_management_relation(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id, "operation_id": operation}),
                Jsonb(body),
            ))).fetchone()
        return row["result"]

    first_approval, second_approval = await asyncio.gather(
        approve("approve/one"), approve("approve/two"),
    )
    approved = first_approval if first_approval["ok"] else second_approval
    refused = second_approval if first_approval["ok"] else first_approval
    assert approved["value"]["contribution_id"] == "contribution/review"
    assert refused["error"]["code"] == "MEMORY_RELATION_ALREADY_APPROVED"
    approval_operation = "approve/one" if first_approval["ok"] else "approve/two"
    assert await approve(approval_operation) == approved
    assert (await approve(approval_operation, relation="another"))["error"]["code"] == (
        "MEMORY_RELATION_APPROVAL_CONFLICT"
    )
    async with store.pool.connection() as conn:
        approved_page = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_relations(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"limit": 10, "review_states": ["APPROVED"]}),
        ))).fetchone()
        unreviewed_page = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_relations(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"limit": 10, "review_states": ["UNREVIEWED"]}),
        ))).fetchone()
    approved_item = approved_page["result"]["value"]["items"][0]
    assert approved_item["review_state"] == "APPROVED"
    assert approved_item["reviewed_by"] == "admin/test"
    assert unreviewed_page["result"]["value"]["items"] == []
    async with store.pool.connection() as conn:
        approved_graph = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_neighbourhood(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"endpoint_kind": "ENTITY", "endpoint_id": "entity/left",
                   "review_states": ["APPROVED"], "max_hops": 1}),
        ))).fetchone()
        unreviewed_graph = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_neighbourhood(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"endpoint_kind": "ENTITY", "endpoint_id": "entity/left",
                   "review_states": ["UNREVIEWED"], "max_hops": 1}),
        ))).fetchone()
    assert len(approved_graph["result"]["value"]["edges"]) == 1
    assert unreviewed_graph["result"]["value"]["edges"] == []
    neighbourhood_schema = json.loads((
        Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/query-neighbourhood-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(neighbourhood_schema).validate(
        approved_graph["result"]["value"]
    )
    relation_schema = json.loads((
        Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/query-relations-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(relation_schema).validate(approved_page["result"]["value"])

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
            f"review/history/{suffix}", relation=relation, contribution=contribution,
        )
        assert result["ok"] is True, result

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

    async def evidence(body, bot_id=None):
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_query_retraction_evidence(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": bot_id or store.bot_id}), Jsonb(body),
            ))).fetchone()
        return row["result"]

    evidence_request = {
        "relation_id": "relation/review", "contribution_id": "contribution/review",
        "snapshot_xid": third_page["value"]["snapshot_xid"], "limit": 16,
    }
    old_snapshot = await evidence({
        **evidence_request,
        "snapshot_xid": before["result"]["value"]["snapshot_xid"],
    })
    assert old_snapshot["error"]["code"] == "MEMORY_RELATION_RETRACTION_NOT_FOUND"
    assert (await evidence(evidence_request, bot_id="bot/foreign"))["error"][
        "code"
    ] == "MEMORY_RELATION_RETRACTION_NOT_FOUND"
    first_evidence = await evidence(evidence_request)
    assert first_evidence["ok"] is True
    evidence_schema = json.loads((
        Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/query-retraction-evidence-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(evidence_schema).validate(first_evidence["value"])
    assert first_evidence["value"]["total_count"] == 20
    assert len(first_evidence["value"]["items"]) == 16
    assert first_evidence["value"]["has_more"] is True
    assert first_evidence["value"]["items"][0]["source"] == {
        "part_id": "part/review/0", "trunk_id": "trunk/review",
        "source_id": "source/review", "source_kind": "GROUP",
        "platform": "onebot", "realm_id": "qq",
        "external_group_id": "933179609", "author_subject_ref": "qq:42",
    }
    second_evidence = await evidence({
        **evidence_request,
        "after_evidence_index": first_evidence["value"]["next_after"],
    })
    assert [item["evidence_index"] for item in second_evidence["value"]["items"]] == [
        16, 17, 18, 19,
    ]
    assert second_evidence["value"]["has_more"] is False

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "UPDATE {}.source_units SET eligibility_state='SUPPRESSED',"
            "eligibility_revision=eligibility_revision+1 "
            "WHERE bot_id=%s AND source_unit_id='unit/review'"
        ).format(namespace), (store.bot_id,))
    redacted = await evidence(evidence_request)
    assert redacted["value"]["items"][0] == {
        "evidence_index": 0, "source_unit_id": "unit/review",
        "source_revision": 1, "available": False, "source": None,
    }
