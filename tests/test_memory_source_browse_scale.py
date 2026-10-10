"""Large group-source browse preserves filtered keyset semantics."""

from __future__ import annotations

import time
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
async def test_group_source_browse_at_one_hundred_thousand_units(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "browse-scale-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest_a = "sha256:" + "a" * 64
    digest_b = "sha256:" + "b" * 64
    async with store.pool.connection() as conn, conn.transaction():
        for group, digest in (("933179609", digest_a), ("933179610", digest_b)):
            source = f"source/{group}"
            trunk = f"trunk/{group}"
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_sources (bot_id,source_id,"
                "registration_operation_id,input_digest,source_kind,platform,realm_id,"
                "external_group_id,identity_incarnation,identity_digest,trunk_id) "
                "VALUES (%s,%s,%s,%s,'GROUP','onebot','fixture',%s,1,%s,%s)"
            ).format(namespace), (
                store.bot_id, source, f"register/{group}", digest, group, digest,
                trunk,
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_trunks (bot_id,trunk_id,source_id) "
                "VALUES (%s,%s,%s)"
            ).format(namespace), (store.bot_id, trunk, source))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units (bot_id,source_unit_id,"
                "capture_operation_id,input_digest,source_id,trunk_id,"
                "source_order_seq,source_revision,unit_kind,modality,part_count,"
                "content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,"
                "eligible_at,eligible_xid,occurred_at) "
                "SELECT %s,'unit/'||%s||'/'||lpad(n::text,6,'0'),"
                "'capture/'||%s||'/'||n,%s,%s,%s,n,1,'MESSAGE','TEXT',1,%s,"
                "'qq:20001','payload/'||%s||'/'||n,%s,'memory.graph','AUTHOR',"
                "'author/'||%s||'/'||n,0,'ELIGIBLE',clock_timestamp(),"
                "pg_current_xact_id(),"
                "TIMESTAMPTZ '2025-01-01' + n*INTERVAL '1 second' "
                "FROM generate_series(1,50000) n"
            ).format(namespace), (
                store.bot_id, group, group, digest, source, trunk, digest,
                group, digest, group,
            ))
        await conn.execute(sql.SQL("ANALYZE {}.source_units").format(namespace))
        await conn.execute(sql.SQL("ANALYZE {}.memory_sources").format(namespace))

    async def page(request):
        started = time.perf_counter()
        async with store.pool.connection() as conn:
            result = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_query_sources(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}), Jsonb(request),
            ))).fetchone()
        elapsed = time.perf_counter() - started
        assert result["result"]["ok"], result
        return result["result"]["value"], elapsed

    first, first_seconds = await page({"external_group_id": "933179609", "limit": 50})
    assert first["has_more"] is True
    assert [item["source_unit_id"] for item in first["items"]] == [
        f"unit/933179609/{number:06d}" for number in range(50000, 49950, -1)
    ]
    assert all(item["external_group_id"] == "933179609" for item in first["items"])

    cursor = {
        "external_group_id": "933179609", "limit": 50,
        "snapshot_accepted_at": first["snapshot_accepted_at"],
        "snapshot_xid": first["snapshot_xid"],
        "cursor_filter_digest": first["filter_digest"],
        "after_occurred_at": first["next_cursor"]["occurred_at"],
        "after_source_unit_id": first["next_cursor"]["source_unit_id"],
    }
    second, second_seconds = await page(cursor)
    assert [item["source_unit_id"] for item in second["items"]] == [
        f"unit/933179609/{number:06d}" for number in range(49950, 49900, -1)
    ]

    # This is the same filter/snapshot-bound cursor advanced to a known deep
    # key; it exercises the 25,000th position without 500 HTTP round trips.
    deep, deep_seconds = await page({
        **cursor,
        "after_occurred_at": "2025-01-01T06:56:40Z",
        "after_source_unit_id": "unit/933179609/025000",
    })
    assert [item["source_unit_id"] for item in deep["items"]] == [
        f"unit/933179609/{number:06d}" for number in range(24999, 24949, -1)
    ]
    no_facets, coverage_seconds = await page({
        "external_group_id": "933179609", "index_coverage": "NO_FACETS",
        "limit": 50,
    })
    assert [item["source_unit_id"] for item in no_facets["items"]] == [
        item["source_unit_id"] for item in first["items"]
    ]
    unknown, unknown_seconds = await page({
        "external_group_id": "933179609", "only_unknown_facets": True,
        "limit": 50,
    })
    assert unknown["items"] == [] and unknown["has_more"] is False
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions "
            "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
            "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
            "weight_micros,admitted_deletion_epoch,cross_trunk) "
            "SELECT %s,'scale/rel/'||lpad(n::text,5,'0'),"
            "'scale/op/'||lpad(n::text,5,'0'),%s,"
            "'scale/relation/'||lpad(n::text,5,'0'),"
            "'ASSOCIATES_WITH','SOURCE_UNIT',"
            "'unit/933179609/'||lpad((CASE WHEN n=8193 THEN 50000 ELSE n END)"
            "::text,6,'0'),'SOURCE_UNIT',"
            "'unit/933179610/'||lpad(n::text,6,'0'),"
            "'RULE',500000,0,true FROM generate_series(1,8193) n"
        ).format(namespace), (store.bot_id, digest_a))
        await conn.execute(sql.SQL("ANALYZE {}.relation_contributions").format(namespace))
    related, relation_seconds = await page({
        "external_group_id": "933179609", "relation_kind": "ASSOCIATES_WITH",
        "relation_cross_trunk": True, "limit": 1,
    })
    assert [item["source_unit_id"] for item in related["items"]] == [
        "unit/933179609/050000"
    ]
    assert related["has_more"] is True
    unrelated, _ = await page({
        "external_group_id": "933179609", "relation_cross_trunk": False,
        "limit": 1,
    })
    assert unrelated["items"] == []
    # The sparse missing-projection path must not omit a late source when
    # 8193 distinct sources exceed the bounded candidate threshold.
    async with store.pool.connection() as conn, conn.transaction():
        members = (
            "SELECT n FROM generate_series(1,8192) n "
            "UNION ALL SELECT 50000"
        )
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_parts "
            "(bot_id,source_unit_id,part_id,part_index,part_kind,media_type,"
            "payload_id,payload_digest,payload_owner_component_id,"
            "payload_owner_kind,payload_owner_id) "
            "SELECT %s,'unit/933179609/'||lpad(n::text,6,'0'),"
            "'part/'||n,0,'TEXT','text/plain','payload/'||n,%s,"
            "'memory.graph','test','unit/933179609/'||lpad(n::text,6,'0') "
            "FROM (" + members + ") members"
        ).format(namespace), (store.bot_id, digest_a))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.facet_preparation_work "
            "(bot_id,source_unit_id,state,prepared_facet_count) "
            "SELECT %s,'unit/933179609/'||lpad(n::text,6,'0'),'READY',1 "
            "FROM (" + members + ") members"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_facets "
            "(bot_id,facet_id,purpose,input_kind,input_ref,input_revision,"
            "source_unit_id,source_revision,part_id,modality,selector_start_char,"
            "selector_end_char,input_payload_id,input_payload_digest,"
            "facet_content_digest,input_media_type,input_byte_length,space_id,"
            "accepted_deletion_epoch) "
            "SELECT %s,'facet/'||n,'DOCUMENT','SOURCE_UNIT',"
            "'unit/933179609/'||lpad(n::text,6,'0'),1,"
            "'unit/933179609/'||lpad(n::text,6,'0'),1,'part/'||n,"
            "'TEXT',0,1,'payload/'||n,%s,%s,'text/plain',1,"
            "'space/scale',0 FROM (" + members + ") members"
        ).format(namespace), (store.bot_id, digest_a, digest_a))
        await conn.execute(sql.SQL("ANALYZE {}.embedding_facets").format(namespace))
        await conn.execute(sql.SQL("ANALYZE {}.facet_preparation_work").format(namespace))
    missing_text, missing_text_seconds = await page({
        "external_group_id": "933179609",
        "only_missing_text_projection": True, "limit": 1,
    })
    assert [item["source_unit_id"] for item in missing_text["items"]] == [
        "unit/933179609/050000"
    ]
    assert missing_text["has_more"] is True
    print(
        "source_browse_seconds first={:.3f} second={:.3f} deep={:.3f} "
        "coverage={:.3f} unknown={:.3f} relation={:.3f} missing_text={:.3f}".format(
            first_seconds, second_seconds, deep_seconds,
            coverage_seconds, unknown_seconds, relation_seconds,
            missing_text_seconds,
        )
    )
