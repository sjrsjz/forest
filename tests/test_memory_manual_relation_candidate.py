import hashlib
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
async def test_manual_relation_requires_approval_in_query_snapshot(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"), store, installer, kernel=kernel,
        initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64

    async def call(function, context, body):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.{}(%s::jsonb,%s::jsonb) AS result"
            ).format(namespace, sql.Identifier(function)), (
                Jsonb(context), Jsonb(body),
            ))).fetchone()
        return row["result"]

    async with store.pool.connection() as conn, conn.transaction():
        for name in ("a", "b"):
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_sources(bot_id,source_id,registration_operation_id,"
                "input_digest,source_kind,platform,realm_id,external_group_id,"
                "identity_incarnation,identity_digest,trunk_id) VALUES "
                "(%s,%s,%s,%s,'GROUP','onebot','qq',%s,1,%s,%s)"
            ).format(namespace), (
                store.bot_id, "source/" + name, "register/" + name, digest,
                "group/" + name,
                "sha256:" + hashlib.sha256(name.encode()).hexdigest(), "trunk/" + name,
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_trunks(bot_id,trunk_id,source_id) VALUES (%s,%s,%s)"
            ).format(namespace), (store.bot_id, "trunk/" + name, "source/" + name))
        for name, source, sequence, state in (
            ("seed", "a", 1, "ELIGIBLE"),
            ("target", "b", 1, "ELIGIBLE"),
            ("query-input", "a", 2, "REGISTERED"),
        ):
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units(bot_id,source_unit_id,capture_operation_id,"
                "input_digest,source_id,trunk_id,source_order_seq,source_revision,unit_kind,"
                "modality,part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,author_owner_id,"
                "accepted_deletion_epoch,eligibility_state,eligible_at,occurred_at) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,1,'GROUP_MESSAGE','TEXT',1,%s,%s,%s,%s,"
                "'adapter.onebot','context.input',%s,0,%s,"
                "CASE WHEN %s='ELIGIBLE' THEN clock_timestamp() ELSE NULL END,"
                "clock_timestamp())"
            ).format(namespace), (
                store.bot_id, name, "capture/" + name, digest, "source/" + source,
                "trunk/" + source, sequence, digest, "qq:author/" + source,
                "author/" + name, digest, name, state, state,
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_parts(bot_id,source_unit_id,part_id,part_index,part_kind,"
                "media_type,payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id,byte_length) VALUES "
                "(%s,%s,'text',0,'TEXT','text/plain',%s,%s,'adapter.onebot','context.input',%s,1)"
            ).format(namespace), (store.bot_id, name, "text/" + name, digest, name))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_occurrences(bot_id,occurrence_id,source_unit_id,"
                "binding_ref,conversation_ref,event_ref,ingress_seq,input_batch_ref,"
                "context_revision,eligibility_state,observed_at) VALUES "
                "(%s,%s,%s,'binding/main',%s,%s,%s,%s,1,%s,clock_timestamp())"
            ).format(namespace), (
                store.bot_id, "occ/" + name, name, "conv/" + source, name, sequence,
                "batch/query" if state == "REGISTERED" else "batch/" + name,
                "ADMITTED" if state == "REGISTERED" else "ELIGIBLE",
            ))

    body = {
        "relation_kind": "ASSOCIATES_WITH", "left_source_unit_id": "seed",
        "left_part_id": "text", "right_source_unit_id": "target",
        "right_part_id": "text", "weight_micros": 900000,
        "requested_by": "admin/reviewer",
    }
    parts = await call("memory_query_source_parts", {"bot_id": store.bot_id},
                       {"source_unit_id": "target", "limit": 16})
    assert parts["ok"], parts
    assert parts["value"]["items"] == [{
        "part_index": 0, "part_id": "text", "part_kind": "TEXT",
        "media_type": "text/plain",
    }]
    assert parts["value"]["external_group_id"] == "group/b"
    parts_schema = json.loads((Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/query-source-parts-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(parts_schema).validate(parts["value"])
    unavailable = await call("memory_query_source_parts", {"bot_id": store.bot_id},
                             {"source_unit_id": "query-input"})
    assert unavailable["error"]["code"] == "MEMORY_SOURCE_UNIT_NOT_AVAILABLE"
    context = {"bot_id": store.bot_id, "operation_id": "candidate/one"}
    invalid = await call("memory_create_management_relation_candidate", context,
                         {**body, "right_part_id": "missing"})
    assert invalid["error"]["code"] == "MEMORY_RELATION_CANDIDATE_SOURCE_UNAVAILABLE"
    created = await call("memory_create_management_relation_candidate", context, body)
    assert created["ok"], created
    assert await call("memory_create_management_relation_candidate", context, body) == created
    changed = await call("memory_create_management_relation_candidate", context,
                         {**body, "weight_micros": 800000})
    assert changed["error"]["code"] == "MEMORY_RELATION_CANDIDATE_CONFLICT"
    duplicate = await call("memory_create_management_relation_candidate",
                           {"bot_id": store.bot_id, "operation_id": "candidate/two"}, body)
    assert duplicate["error"]["code"] == "MEMORY_RELATION_CANDIDATE_EXISTS"
    candidate = created["value"]
    schema = json.loads((Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/create-relation-candidate-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(candidate)
    page = await call("memory_query_relations", {"bot_id": store.bot_id},
                      {"limit": 10, "origin_kinds": ["MANUAL"]})
    item = page["value"]["items"][0]
    assert item["proposed_by"] == "admin/reviewer"
    assert item["cross_trunk"] and item["review_state"] == "UNREVIEWED"
    assert {e["external_group_id"] for e in item["evidence"]} == {"group/a", "group/b"}

    async def query(name):
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_queries(bot_id,query_id,operation_id,input_digest,"
                "current_source_id,input_batch_ref,context_revision,query_mode,"
                "fusion_policy_revision,accepted_deletion_epoch) VALUES "
                "(%s,%s,%s,%s,'source/a','batch/query',1,'TEXT','independent-facets/1',0)"
            ).format(namespace), (store.bot_id, name, "begin/" + name, digest))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_query_parts(bot_id,query_id,query_part_index,"
                "source_unit_id,source_revision,occurrence_id,part_id,modality) VALUES "
                "(%s,%s,0,'query-input',1,'occ/query-input','text','TEXT')"
            ).format(namespace), (store.bot_id, name))
        result = await call("memory_read_graph_neighbours", {"bot_id": store.bot_id},
                            {"query_id": name, "source_unit_id": "seed", "association_limit": 8})
        assert result["ok"], result
        return result["value"]

    async def sources_with_review(state, snapshot=None):
        filters = {"limit": 10, "relation_kind": "ASSOCIATES_WITH",
                   "relation_review_state": state}
        if snapshot is not None:
            filters["snapshot_accepted_at"] = snapshot["snapshot_accepted_at"]
            filters["snapshot_xid"] = snapshot["snapshot_xid"]
        result = await call("memory_query_sources", {"bot_id": store.bot_id}, filters)
        assert result["ok"], result
        return result["value"]

    before_sources = await sources_with_review("UNREVIEWED")
    assert {item["source_unit_id"] for item in before_sources["items"]} == {
        "seed", "target"}
    assert (await sources_with_review("APPROVED"))["items"] == []
    before = await query("query/before")
    assert not [edge for edge in before["edges"] if edge["relation"] == "ASSOCIATED"]
    approval = await call("memory_approve_management_relation",
                          {"bot_id": store.bot_id, "operation_id": "approve/one"},
                          {"relation_id": candidate["relation_id"],
                           "contribution_id": candidate["contribution_id"],
                           "requested_by": "admin/reviewer"})
    assert approval["ok"], approval
    assert {item["source_unit_id"] for item in
            (await sources_with_review("UNREVIEWED", before_sources))["items"]} == {
                "seed", "target"}
    assert (await sources_with_review("UNREVIEWED"))["items"] == []
    assert {item["source_unit_id"] for item in
            (await sources_with_review("APPROVED"))["items"]} == {"seed", "target"}
    assert not [edge for edge in (await call(
        "memory_read_graph_neighbours", {"bot_id": store.bot_id},
        {"query_id": "query/before", "source_unit_id": "seed", "association_limit": 8},
    ))["value"]["edges"] if edge["relation"] == "ASSOCIATED"]
    after = await query("query/after")
    assert [(edge["target_id"], edge["contribution_id"]) for edge in after["edges"]
            if edge["relation"] == "ASSOCIATED"] == [
                ("target", candidate["contribution_id"])
            ]
    retracted = await call("memory_retract_management_relation",
                           {"bot_id": store.bot_id, "operation_id": "retract/one"},
                           {"relation_id": candidate["relation_id"],
                            "contribution_id": candidate["contribution_id"],
                            "reason_code": "INCORRECT", "requested_by": "admin/reviewer"})
    assert retracted["ok"], retracted
    assert (await sources_with_review("UNREVIEWED"))["items"] == []
    assert (await sources_with_review("APPROVED"))["items"] == []
    final = await query("query/final")
    assert not [edge for edge in final["edges"] if edge["relation"] == "ASSOCIATED"]
