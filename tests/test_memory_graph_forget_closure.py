"""Payload closure resolves every live source without a full Bot-wide source scan."""

from pathlib import Path

import pytest
from psycopg import sql
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_forget_closure_joins_author_and_part_payloads_once(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "forget-closure-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    ns = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources (bot_id,source_id,registration_operation_id,"
            "input_digest,source_kind,identity_incarnation,identity_digest) "
            "VALUES (%s,'source/closure','register/closure',%s,'DIRECT',1,%s)"
        ).format(ns), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_units (bot_id,source_unit_id,capture_operation_id,"
            "input_digest,source_id,source_order_seq,source_revision,unit_kind,modality,"
            "part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
            "author_payload_digest,author_owner_component_id,author_owner_kind,"
            "author_owner_id,accepted_deletion_epoch,eligibility_state,eligible_at,"
            "eligible_xid,occurred_at) "
            "SELECT %s,'unit/'||name,'capture/'||name,%s,'source/closure',seq,1,"
            "'CHAT','TEXT',1,%s,'author/test',author_payload,%s,'memory.graph',"
            "'AUTHOR','author/'||name,0,state,"
            "CASE WHEN state='ELIGIBLE' THEN clock_timestamp() END,"
            "CASE WHEN state='ELIGIBLE' THEN pg_current_xact_id() END,"
            "TIMESTAMPTZ '2026-01-01' + seq*INTERVAL '1 second' "
            "FROM (VALUES ('a',1,'payload/author-a','ELIGIBLE'),"
            "('b',2,'payload/author-b','ELIGIBLE'),"
            "('c',3,'payload/author-c','ELIGIBLE'),"
            "('d',4,'payload/author-a','SUPPRESSED')) unit(name,seq,author_payload,state)"
        ).format(ns), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_suppressions (bot_id,operation_id,input_digest,"
            "source_unit_id,deletion_epoch,reason_code) "
            "VALUES (%s,'forget/earlier',%s,'unit/d',1,'PUBLIC_FORGET')"
        ).format(ns), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_parts (bot_id,source_unit_id,part_id,part_index,"
            "part_kind,media_type,payload_id,payload_digest,payload_owner_component_id,"
            "payload_owner_kind,payload_owner_id) "
            "VALUES (%s,'unit/a','part/a',0,'TEXT','text/plain','payload/shared',%s,"
            "'memory.graph','SOURCE_PART','part/a'),"
            "(%s,'unit/b','part/b',0,'TEXT','text/plain','payload/author-a',%s,"
            "'memory.graph','SOURCE_PART','part/b'),"
            "(%s,'unit/c','part/c',0,'TEXT','text/plain','payload/shared',%s,"
            "'memory.graph','SOURCE_PART','part/c')"
        ).format(ns), (store.bot_id, digest, store.bot_id, digest, store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_contributions (bot_id,contribution_id,operation_id,"
            "input_digest,relation_id,relation_kind,left_kind,left_id,right_kind,right_id,"
            "origin_kind,weight_micros,admitted_deletion_epoch,cross_trunk) "
            "SELECT %s,'relation/'||name,'operation/'||name,%s,'edge/'||name,"
            "'ASSOCIATES_WITH',left_kind,left_id,right_kind,right_id,'RULE',500000,0,false "
            "FROM (VALUES "
            "('left','SOURCE_UNIT','unit/a','ENTITY','entity/left'),"
            "('right','ENTITY','entity/right','SOURCE_UNIT','unit/b'),"
            "('evidence','ENTITY','entity/evidence-left','ENTITY','entity/evidence-right'),"
            "('unrelated','ENTITY','entity/other-left','ENTITY','entity/other-right')"
            ") edge(name,left_kind,left_id,right_kind,right_id)"
        ).format(ns), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
            "source_unit_id,source_revision,part_id) "
            "VALUES (%s,'relation/evidence',0,'unit/c',1,'part/c')"
        ).format(ns), (store.bot_id,))
        selected = ["payload/author-a", "payload/shared"]
        candidates = await (await conn.execute(sql.SQL(
            "SELECT source_unit_id FROM {}.memory_payload_source_candidates(%s,%s) "
            "ORDER BY source_unit_id"
        ).format(ns), (store.bot_id, selected))).fetchall()
        assert [row["source_unit_id"] for row in candidates] == [
            "unit/a", "unit/b", "unit/c", "unit/d",
        ]
        impact = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_source_forget_impact(%s,%s) AS result"
        ).format(ns), (store.bot_id, selected))).fetchone()
        assert impact["result"]["source_unit_count"] == 3
        assert impact["result"]["relation_retraction_count"] == 3
        applied = await (await conn.execute(sql.SQL(
            "SELECT {}.memory_apply_forget_effects(%s,%s,%s,%s,%s,%s) AS result"
        ).format(ns), (store.bot_id, "forget/closure", digest, selected, 1,
                       ["unit/a", "unit/b", "unit/c"]))).fetchone()
        assert applied["result"]["suppressed_source_units"] == 3
        assert applied["result"]["retracted_relations"] == 3
        relations = await (await conn.execute(sql.SQL(
            "SELECT contribution_id,active FROM {}.relation_contributions "
            "WHERE bot_id=%s ORDER BY contribution_id"
        ).format(ns), (store.bot_id,))).fetchall()
        assert [(row["contribution_id"], row["active"]) for row in relations] == [
            ("relation/evidence", False), ("relation/left", False),
            ("relation/right", False), ("relation/unrelated", True),
        ]
        states = await (await conn.execute(sql.SQL(
            "SELECT source_unit_id,eligibility_state FROM {}.source_units "
            "WHERE bot_id=%s ORDER BY source_unit_id"
        ).format(ns), (store.bot_id,))).fetchall()
        assert [(row["source_unit_id"], row["eligibility_state"]) for row in states] == [
            ("unit/a", "SUPPRESSED"), ("unit/b", "SUPPRESSED"),
            ("unit/c", "SUPPRESSED"), ("unit/d", "SUPPRESSED"),
        ]
        suppressions = await (await conn.execute(sql.SQL(
            "SELECT count(*) FROM {}.source_suppressions WHERE bot_id=%s "
            "AND operation_id='forget/closure'"
        ).format(ns), (store.bot_id,))).fetchone()
        assert suppressions["count"] == 3
