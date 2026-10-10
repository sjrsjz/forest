from pathlib import Path

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.facet_preparation import document_facets
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog, digest_json


@pytest.mark.postgres
async def test_text_projection_pages_and_forget_follow_source_authority(
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
    space = "sha256:" + "b" * 64
    content = "hello " + "\u68ee\u6797\u673a\u5668\u4eba" + " world"
    needle = "\u68ee\u6797\u673a\u5668\u4eba"
    def reference(n):
        return {
            "payload_id": f"payload/{n}", "digest": digest,
            "owner_component_id": "memory.graph", "owner_kind": "test",
            "owner_id": f"unit/{n}",
        }
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domains "
            "(bot_id,initialization_operation_id,input_digest,space_id,"
            "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,"
            "normalization,fusion_policy_revision) VALUES "
            "(%s,'domain/search',%s,%s,'TEXT_IMAGE',%s,4096,8,'L2','facets/1')"
        ).format(namespace), (store.bot_id, digest, space, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources "
            "(bot_id,source_id,registration_operation_id,input_digest,source_kind,"
            "platform,realm_id,external_group_id,identity_incarnation,"
            "identity_digest,trunk_id) VALUES "
            "(%s,'source/search','register/search',%s,'GROUP','onebot','qq',"
            "'933179609',1,%s,'trunk/search')"
        ).format(namespace), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trunks(bot_id,trunk_id,source_id) "
            "VALUES (%s,'trunk/search','source/search')"
        ).format(namespace), (store.bot_id,))
        for n in (1, 2, 3):
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units "
                "(bot_id,source_unit_id,capture_operation_id,input_digest,source_id,"
                "trunk_id,source_order_seq,source_revision,unit_kind,modality,"
                "part_count,content_manifest_digest,author_subject_ref,"
                "author_payload_id,author_payload_digest,author_owner_component_id,"
                "author_owner_kind,author_owner_id,accepted_deletion_epoch,"
                "eligibility_state,eligible_at,eligible_xid,occurred_at) VALUES "
                "(%s,%s,%s,%s,'source/search','trunk/search',%s,1,'message',"
                "'TEXT',1,%s,%s,%s,%s,'memory.graph','test',%s,0,"
                "'ELIGIBLE',clock_timestamp(),pg_current_xact_id(),"
                "clock_timestamp()-(%s*interval '1 second'))"
            ).format(namespace), (
                store.bot_id, f"unit/{n}", f"capture/{n}", digest, n, digest,
                f"author/{n}", f"author-payload/{n}", digest, f"unit/{n}", n,
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_parts "
                "(bot_id,source_unit_id,part_id,part_index,part_kind,media_type,"
                "payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id) VALUES "
                "(%s,%s,%s,0,'TEXT','text/plain',%s,%s,'memory.graph','test',%s)"
            ).format(namespace), (
                store.bot_id, f"unit/{n}", f"part/{n}", f"payload/{n}",
                digest, f"unit/{n}",
            ))

    async def prepare(n, value=content, search_text=None, operation=None):
        part = {
            "part_id": f"part/{n}", "part_kind": "TEXT",
            "payload": reference(n),
        }
        facets = document_facets(f"unit/{n}", 1, space, [part], {f"part/{n}": value})
        assert len(facets) == 1
        facets[0]["search_text"] = value if search_text is None else search_text
        body = {
            "protocol": "forest.memory-document-facets/1",
            "source_unit_id": f"unit/{n}", "source_revision": 1,
            "space_id": space, "batch_index": 0, "facets": facets,
        }
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_prepare_facets(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({
                    "bot_id": store.bot_id, "generation_id": "generation/search",
                    "operation_id": operation or f"prepare/{n}",
                    "participant_input_digest": digest_json(body),
                }),
                Jsonb({
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "memory.graph",
                        "generation_id": "generation/search",
                    },
                    "input": body, "prior": {},
                }),
            ))).fetchone()
        return row["result"]

    async def page(filters):
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_query_sources(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}), Jsonb(filters),
            ))).fetchone()
        return row["result"]

    forged = await prepare(1, search_text="forged")
    assert forged["error"]["code"] == "MEMORY_FACET_PART_MISMATCH"
    assert (await prepare(1))["ok"]
    assert (await prepare(2))["ok"]
    first = (await page({"text_contains": needle, "limit": 1}))["value"]
    assert [item["source_unit_id"] for item in first["items"]] == ["unit/1"]
    assert first["has_more"]
    assert needle not in str(first)
    cursor = {
        "text_contains": needle, "limit": 1,
        "snapshot_accepted_at": first["snapshot_accepted_at"],
        "snapshot_xid": first["snapshot_xid"],
        "after_occurred_at": first["next_cursor"]["occurred_at"],
        "after_source_unit_id": first["next_cursor"]["source_unit_id"],
        "cursor_filter_digest": first["filter_digest"],
    }
    assert (await prepare(3))["ok"]
    old_page = (await page(cursor))["value"]
    assert [item["source_unit_id"] for item in old_page["items"]] == ["unit/2"]
    assert not old_page["has_more"]
    changed = await page({**cursor, "text_contains": "hello"})
    assert changed["error"]["code"] == "MEMORY_QUERY_INVALID"
    fresh = (await page({"text_contains": needle}))["value"]
    assert [item["source_unit_id"] for item in fresh["items"]] == [
        "unit/1", "unit/2", "unit/3",
    ]
    # Rebuild a lost derived projection from the immutable document facet.
    async def rebuild_call(function, context, body):
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT {}." + function + "(%s,%s) AS result"
            ).format(namespace), (Jsonb(context), Jsonb(body)))).fetchone()
        return row["result"]

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.facet_preparation_work "
            "(bot_id,source_unit_id,state,prepared_facet_count) "
            "VALUES (%s,'unit/1','READY',1)"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "DELETE FROM {}.source_text_facets "
            "WHERE bot_id=%s AND source_unit_id='unit/1'"
        ).format(namespace), (store.bot_id,))
    missing = (await page({"text_contains": needle}))["value"]
    assert [item["source_unit_id"] for item in missing["items"]] == ["unit/2", "unit/3"]
    request = {"source_unit_id": "unit/1", "requested_by": "admin/search"}
    rebuild_context = {"bot_id": store.bot_id, "operation_id": "rebuild/search/1"}
    queued = await rebuild_call("memory_enqueue_text_projection_rebuild",
                                rebuild_context, request)
    assert queued["ok"] and queued["value"]["source_revision"] == 1
    assert await rebuild_call("memory_enqueue_text_projection_rebuild",
                              rebuild_context, request) == queued
    unauthorized = await rebuild_call("memory_query_text_projection_rebuild",
                                      {"bot_id": store.bot_id},
                                      {"request_id": "rebuild/search/1",
                                       "requested_by": "admin/other"})
    assert unauthorized["error"]["code"] == "MEMORY_TEXT_REBUILD_NOT_FOUND"
    claim = await rebuild_call("memory_claim_text_projection_rebuild",
                               {"bot_id": store.bot_id}, {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "claim/rebuild/search/1", "lease_seconds": 960,
    })
    assert claim["ok"] and claim["value"]["payload"]["source_unit_id"] == "unit/1"
    assert claim["value"]["payload"]["claim_token"] == "claim/rebuild/search/1"
    prepared_facet = document_facets("unit/1", 1, space, [{
        "part_id": "part/1", "part_kind": "TEXT", "payload": reference(1),
    }], {"part/1": content})[0]
    staged = {
        "request_id": "rebuild/search/1",
        "claim_token": "claim/rebuild/search/1",
        "facets": [{
            "facet_id": prepared_facet["facet_id"], "part_id": "part/1",
            "selector_start_char": prepared_facet["selector_start_char"],
            "selector_end_char": prepared_facet["selector_end_char"],
            "content": content,
            "content_digest": prepared_facet["facet_content_digest"],
        }],
    }
    forged_rebuild = await rebuild_call("memory_stage_text_projection_rebuild",
                                       {"bot_id": store.bot_id},
                                       {**staged, "facets": [{**staged["facets"][0],
                                                            "content": "forged"}]})
    assert forged_rebuild["error"]["code"] == "MEMORY_TEXT_REBUILD_FACET_MISMATCH"
    assert (await rebuild_call("memory_stage_text_projection_rebuild",
                               {"bot_id": store.bot_id}, staged))["ok"]
    assert (await rebuild_call("memory_stage_text_projection_rebuild",
                               {"bot_id": store.bot_id}, staged))["value"][
        "staged_facet_count"] == 1
    published = await rebuild_call("memory_publish_text_projection_rebuild",
                                   {"bot_id": store.bot_id}, {
        "request_id": "rebuild/search/1",
        "claim_token": "claim/rebuild/search/1", "facet_count": 1,
    })
    assert published["ok"], published
    settled = await rebuild_call("memory_settle_text_projection_rebuild",
                                 {"bot_id": store.bot_id}, {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": "rebuild/search/1",
        "claim_token": "claim/rebuild/search/1", "attempt": 1,
        "outcome": {"status": "SUCCEEDED", "result": published["value"]},
    })
    assert settled["ok"], settled
    status = await rebuild_call("memory_query_text_projection_rebuild",
                                {"bot_id": store.bot_id}, {
        "request_id": "rebuild/search/1",
        "requested_by": "admin/search",
    })
    assert status["value"]["state"] == "SUCCEEDED"
    assert status["value"]["facet_count"] == 1
    old_snapshot = (await page({
        "text_contains": needle,
        "snapshot_accepted_at": missing["snapshot_accepted_at"],
        "snapshot_xid": missing["snapshot_xid"],
    }))["value"]
    assert [item["source_unit_id"] for item in old_snapshot["items"]] == [
        "unit/2", "unit/3",
    ]
    repaired = (await page({"text_contains": needle}))["value"]
    assert [item["source_unit_id"] for item in repaired["items"]] == [
        "unit/1", "unit/2", "unit/3",
    ]

    async with store.pool.connection() as conn:
        frozen = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_freeze_forget_selection(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "operation_id": "freeze/search"}),
            Jsonb({
                "selection_id": "selection/search",
                "requested_by": "admin/search",
                "filters": {"text_contains": needle},
            }),
        ))).fetchone())["result"]
    assert frozen["ok"], frozen
    assert frozen["value"]["member_count"] == 3
    short = await page({"text_contains": "a"})
    assert short["error"]["code"] == "MEMORY_QUERY_INVALID"

    # The text clone is deleted in the same Memory forget effects transaction.
    async with store.pool.connection() as conn, conn.transaction():
        effects = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_apply_forget_effects(%s,%s,%s,%s,%s,%s) AS result"
        ).format(namespace), (
            store.bot_id, "forget/search/2", digest,
            ["author-payload/2"], 1, ["unit/2"],
        ))).fetchone())["result"]
        assert effects["suppressed_source_units"] == 1
        remaining = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.source_text_facets "
            "WHERE bot_id=%s AND source_unit_id='unit/2'"
        ).format(namespace), (store.bot_id,))).fetchone())["count"]
        assert remaining == 0
    after_forget = (await page({"text_contains": needle}))["value"]
    assert [item["source_unit_id"] for item in after_forget["items"]] == [
        "unit/1", "unit/3",
    ]
