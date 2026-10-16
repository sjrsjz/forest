import asyncio
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
    assert (before["graph_generation_id"], before["graph_revision"]) == ("primary", 2)
    assert not [edge for edge in before["edges"] if edge["relation"] == "ASSOCIATED"]
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "SELECT 1 FROM {}.graph_topology_journal_heads "
            "WHERE bot_id=%s FOR UPDATE"
        ).format(namespace), (store.bot_id,))
        pending_approval = asyncio.create_task(call(
            "memory_approve_management_relation",
            {"bot_id": store.bot_id, "operation_id": "approve/one"},
            {"relation_id": candidate["relation_id"],
             "contribution_id": candidate["contribution_id"],
             "requested_by": "admin/reviewer"},
        ))
        await asyncio.sleep(0.1)
        assert not pending_approval.done()
        await conn.execute(sql.SQL(
            "SELECT 1 FROM {}.relation_contributions "
            "WHERE bot_id=%s AND contribution_id=%s FOR UPDATE NOWAIT"
        ).format(namespace), (store.bot_id, candidate["contribution_id"]))
    approval = await pending_approval
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
    assert (after["graph_generation_id"], after["graph_revision"]) == ("primary", 3)
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
    assert (final["graph_generation_id"], final["graph_revision"]) == ("primary", 4)
    assert not [edge for edge in final["edges"] if edge["relation"] == "ASSOCIATED"]

    async with store.pool.connection() as conn, conn.transaction():
        rows = await (await conn.execute(sql.SQL(
            "SELECT revision,event_kind,entity_id FROM {}.graph_topology_events "
            "WHERE bot_id=%s ORDER BY revision,event_kind,entity_id"
        ).format(namespace), (store.bot_id,))).fetchall()
    assert [(row["revision"], row["event_kind"], row["entity_id"]) for row in rows] == [
        (1, "SOURCE_UNIT_VISIBILITY", "seed"),
        (1, "SOURCE_UNIT_VISIBILITY", "target"),
        (2, "RELATION_CONTRIBUTION", candidate["contribution_id"]),
        (3, "RELATION_APPROVAL", candidate["contribution_id"]),
        (4, "RELATION_CONTRIBUTION", candidate["contribution_id"]),
        (4, "RELATION_RETRACTION", candidate["contribution_id"]),
    ]

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "UPDATE {}.source_occurrences SET eligibility_state='ELIGIBLE' "
            "WHERE bot_id=%s AND source_unit_id='query-input'"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "UPDATE {}.source_units SET eligibility_state='ELIGIBLE', "
            "eligibility_revision=eligibility_revision+1,eligible_at=clock_timestamp() "
            "WHERE bot_id=%s AND source_unit_id='query-input'"
        ).format(namespace), (store.bot_id,))
    async with store.pool.connection() as conn, conn.transaction():
        row = await (await conn.execute(sql.SQL(
            "SELECT revision,event_kind,before_value->>'eligibility_state' AS before_state, "
            "after_value->>'eligibility_state' AS after_state "
            "FROM {}.graph_topology_events WHERE bot_id=%s AND revision=5"
        ).format(namespace), (store.bot_id,))).fetchone()
    assert row == {
        "revision": 5, "event_kind": "SOURCE_UNIT_VISIBILITY",
        "before_state": "REGISTERED", "after_state": "ELIGIBLE",
    }
    async with store.pool.connection() as conn:
        head = await (await conn.execute(sql.SQL(
            "SELECT active_generation_id,active_graph_revision "
            "FROM {}.graph_projection_heads WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchone()
        nodes = await (await conn.execute(sql.SQL(
            "SELECT source_unit_id,from_revision,to_revision "
            "FROM {}.graph_node_versions WHERE bot_id=%s ORDER BY source_unit_id"
        ).format(namespace), (store.bot_id,))).fetchall()
        edges = await (await conn.execute(sql.SQL(
            "SELECT contribution_id,from_revision,to_revision "
            "FROM {}.graph_edge_versions WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchall()
    assert head == {"active_generation_id": "primary", "active_graph_revision": 5}
    assert nodes == [
        {"source_unit_id": "query-input", "from_revision": 5, "to_revision": None},
        {"source_unit_id": "seed", "from_revision": 1, "to_revision": None},
        {"source_unit_id": "target", "from_revision": 1, "to_revision": None},
    ]
    assert edges == [{
        "contribution_id": candidate["contribution_id"],
        "from_revision": 3, "to_revision": 4,
    }]

    rebuild_context = {"bot_id": store.bot_id, "operation_id": "rebuild/one"}
    begin = await call("memory_begin_graph_rebuild", rebuild_context,
                       {"generation_id": "candidate/one"})
    assert begin["ok"], begin
    Draft202012Validator(json.loads((Path(__file__).parents[1] /
        "src/forest/builtin/memory_graph/schemas/graph-rebuild-begin-output.json"
    ).read_text(encoding="utf-8"))).validate(begin["value"])
    assert begin["value"] == {
        "generation_id": "candidate/one",
        "base_generation_id": "primary",
        "base_revision": 5,
        "replayed_through": 0,
    }
    assert await call("memory_begin_graph_rebuild", rebuild_context,
                      {"generation_id": "candidate/one"}) == begin
    premature = await call("memory_replay_graph_revision",
                           {"bot_id": store.bot_id},
                           {"generation_id": "candidate/one", "revision": 2})
    assert premature["error"]["code"] == "MEMORY_GRAPH_REPLAY_NOT_NEXT"
    for revision in range(1, 6):
        applied = await call("memory_replay_graph_revision",
                             {"bot_id": store.bot_id},
                             {"generation_id": "candidate/one",
                              "revision": revision})
        assert applied["value"] == {
            "generation_id": "candidate/one",
            "replayed_through": revision, "applied": True,
        }
    duplicate = await call("memory_replay_graph_revision",
                           {"bot_id": store.bot_id},
                           {"generation_id": "candidate/one", "revision": 3})
    assert duplicate["value"]["replayed_through"] == 5
    assert duplicate["value"]["applied"] is False
    async with store.pool.connection() as conn:
        rebuilt_nodes = await (await conn.execute(sql.SQL(
            "SELECT source_unit_id,from_revision,to_revision FROM {}.graph_node_versions "
            "WHERE bot_id=%s AND generation_id='candidate/one' "
            "ORDER BY source_unit_id"
        ).format(namespace), (store.bot_id,))).fetchall()
        rebuilt_edges = await (await conn.execute(sql.SQL(
            "SELECT contribution_id,from_revision,to_revision FROM {}.graph_edge_versions "
            "WHERE bot_id=%s AND generation_id='candidate/one'"
        ).format(namespace), (store.bot_id,))).fetchall()
    assert rebuilt_nodes == nodes
    assert rebuilt_edges == edges

    old_target = await call(
        "memory_read_graph_neighbours", {"bot_id": store.bot_id},
        {"query_id": "query/after", "source_unit_id": "target",
         "association_limit": 8},
    )
    assert old_target["value"]["node"]["source_unit_id"] == "target"
    switched = await call(
        "memory_publish_graph_rebuild",
        {"bot_id": store.bot_id, "operation_id": "publish/one"},
        {"generation_id": "candidate/one",
         "expected_active_generation_id": "primary",
         "expected_active_revision": 5},
    )
    assert switched["value"] == {
        "generation_id": "candidate/one",
        "previous_generation_id": "primary",
        "graph_revision": 5,
        "deletion_epoch": 0,
    }
    new_view = await query("query/switched")
    assert (new_view["graph_generation_id"], new_view["graph_revision"]) == (
        "candidate/one", 5)
    unchanged_old = await call(
        "memory_read_graph_neighbours", {"bot_id": store.bot_id},
        {"query_id": "query/after", "source_unit_id": "target",
         "association_limit": 8},
    )
    assert unchanged_old["value"] == old_target["value"]
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "UPDATE {}.memory_sources SET status='RETIRED' "
            "WHERE bot_id=%s AND source_id='source/b'"
        ).format(namespace), (store.bot_id,))
    suppressed_old = await call(
        "memory_read_graph_neighbours", {"bot_id": store.bot_id},
        {"query_id": "query/after", "source_unit_id": "target",
         "association_limit": 8},
    )
    assert suppressed_old["value"]["node"] is None
    assert suppressed_old["value"]["graph_generation_id"] == "primary"
    suppressed_new = await call(
        "memory_read_graph_neighbours", {"bot_id": store.bot_id},
        {"query_id": "query/switched", "source_unit_id": "target",
         "association_limit": 8},
    )
    assert suppressed_new["value"]["node"] is None
    assert suppressed_new["value"]["graph_generation_id"] == "candidate/one"
