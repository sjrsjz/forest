import hashlib
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from psycopg import sql
from psycopg.errors import CheckViolation, ForeignKeyViolation, ObjectNotInPrerequisiteState
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.graph_region import expand
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_neighbour_sampling_is_bounded_snapshot_visible_and_source_attributed(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "neighbour-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    validator = Draft202012Validator(package.snapshot.schema("schemas/read-graph-neighbours-output.json"))
    digest = "sha256:" + "a" * 64

    async with store.pool.connection() as conn:
        async def execute(statement, values):
            return await conn.execute(sql.SQL(statement).format(namespace), values)

        for source in ("a", "b"):
            await execute(
                "INSERT INTO {}.memory_sources (bot_id,source_id,registration_operation_id,"
                "input_digest,source_kind,platform,realm_id,external_group_id,identity_incarnation,"
                "identity_digest,trunk_id) VALUES (%s,%s,%s,%s,'GROUP','qq','realm',%s,1,%s,%s)",
                (store.bot_id, source, "register/" + source, digest, "group/" + source,
                 "sha256:" + hashlib.sha256(source.encode()).hexdigest(), "trunk/" + source),
            )
            await execute(
                "INSERT INTO {}.memory_trunks (bot_id,trunk_id,source_id) VALUES (%s,%s,%s)",
                (store.bot_id, "trunk/" + source, source),
            )

        async def unit(name, source, sequence, ingress, *, query_input=False):
            await execute(
                "INSERT INTO {}.source_units (bot_id,source_unit_id,capture_operation_id,"
                "input_digest,source_id,trunk_id,source_order_seq,source_revision,unit_kind,"
                "modality,part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,author_owner_id,"
                "accepted_deletion_epoch,eligibility_state,eligible_at,occurred_at) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,1,'GROUP_MESSAGE','TEXT',1,%s,%s,%s,%s,"
                "'adapter.onebot','message.author',%s,0,%s,"
                "CASE WHEN %s THEN NULL ELSE clock_timestamp() END,clock_timestamp())",
                (store.bot_id, name, "capture/" + name, digest, source, "trunk/" + source,
                 sequence, digest, "qq:author/" + source, "author/" + name, digest, name,
                 "REGISTERED" if query_input else "ELIGIBLE", query_input),
            )
            await execute(
                "INSERT INTO {}.source_parts (bot_id,source_unit_id,part_id,part_index,part_kind,"
                "media_type,payload_id,payload_digest,payload_owner_component_id,payload_owner_kind,"
                "payload_owner_id,byte_length) VALUES "
                "(%s,%s,'text',0,'TEXT','text/plain',%s,%s,'adapter.onebot','context.input',%s,1)",
                (store.bot_id, name, "text/" + name, digest, name),
            )
            await execute(
                "INSERT INTO {}.source_occurrences (bot_id,occurrence_id,source_unit_id,"
                "binding_ref,conversation_ref,event_ref,ingress_seq,input_batch_ref,context_revision,"
                "eligibility_state,observed_at) VALUES "
                "(%s,%s,%s,'binding/main',%s,%s,%s,%s,1,%s,clock_timestamp())",
                (store.bot_id, "occ/" + name, name, "conv/" + source, name, ingress,
                 "batch/query" if query_input else "batch/" + name,
                 "ADMITTED" if query_input else "ELIGIBLE"),
            )

        await unit("before", "a", 1, 1)
        await unit("seed", "a", 2, 2)
        await unit("after", "a", 3, 3)
        await unit("query-input", "a", 4, 10, query_input=True)
        await unit("tail", "a", 5, 20)
        for index in range(40):
            await unit(f"n{index:02}", "b", index + 1, index + 1)

        async def contribution(name, target, *, missing_part=False, reverse=False, revision=1):
            async with conn.transaction():
                await execute(
                    "INSERT INTO {}.relation_contributions (bot_id,contribution_id,operation_id,"
                    "input_digest,relation_id,relation_kind,left_kind,left_id,right_kind,right_id,"
                    "origin_kind,weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                    "(%s,%s,%s,%s,%s,'ASSOCIATES_WITH','SOURCE_UNIT',%s,'SOURCE_UNIT',%s,'RULE',500000,0,true)",
                    (store.bot_id, name, "op/" + name, digest, "relation/" + name,
                     target if reverse else "seed", "seed" if reverse else target),
                )
                await execute(
                    "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                    "source_unit_id,source_revision,part_id) VALUES (%s,%s,0,'seed',%s,%s)",
                    (store.bot_id, name, revision, "missing" if missing_part else "text"),
                )
                if name.startswith("edge/"):
                    await execute(
                        "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                        "source_unit_id,source_revision,part_id) VALUES (%s,%s,1,'after',1,'text')",
                        (store.bot_id, name),
                    )

        for index in range(40):
            await contribution(f"edge/{index}", f"n{index:02}", reverse=bool(index % 2))
        pivot = hashlib.md5(b"query/one\x1fseed", usedforsecurity=False).hexdigest()
        await conn.execute("SET enable_seqscan=off")
        try:
            for side in ("left", "right"):
                plan = await (
                    await execute(
                        "EXPLAIN (COSTS OFF) SELECT contribution_id FROM {}.relation_contributions "
                        f"WHERE bot_id=%s AND {side}_kind='SOURCE_UNIT' "
                        f"AND {side}_id='seed' AND "
                        f"{'right' if side == 'left' else 'left'}_kind='SOURCE_UNIT' "
                        "AND relation_kind='ASSOCIATES_WITH' AND active "
                        "AND sample_key >= %s ORDER BY sample_key,contribution_id LIMIT 6",
                        (store.bot_id, pivot),
                    )
                ).fetchall()
                assert f"relation_association_{side}_sample_idx" in "\n".join(
                    row["QUERY PLAN"] for row in plan
                )
        finally:
            await conn.execute("RESET enable_seqscan")
        async def browse_relations(values):
            row = await (
                await execute(
                    "SELECT {}.memory_query_relations(%s::jsonb,%s::jsonb) AS result",
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "endpoint_kind": "SOURCE_UNIT", "endpoint_id": "seed", **values,
                    })),
                )
            ).fetchone()
            return row["result"]

        first_edges = (await browse_relations({"cross_trunk": True, "limit": 5}))["value"]
        assert len(first_edges["items"]) == 5 and first_edges["has_more"]
        assert all(edge["cross_trunk"] and edge["direction"] == "UNDIRECTED"
                   for edge in first_edges["items"])
        cursor = {
            "snapshot_xid": first_edges["snapshot_xid"],
            "after_relation_id": first_edges["next_cursor"]["relation_id"],
            "after_contribution_id": first_edges["next_cursor"]["contribution_id"],
            "cursor_filter_digest": first_edges["filter_digest"],
        }
        second_edges = (await browse_relations({
            "cross_trunk": True, "limit": 5, **cursor,
        }))["value"]
        assert len(second_edges["items"]) == 5
        assert not ({edge["contribution_id"] for edge in first_edges["items"]}
                    & {edge["contribution_id"] for edge in second_edges["items"]})
        assert (await browse_relations({
            "cross_trunk": False, "limit": 5, **cursor,
        }))["error"]["code"] == "MEMORY_QUERY_INVALID"
        async def search_relations(values):
            row = await (
                await execute(
                    "SELECT {}.memory_search_relations(%s::jsonb,%s::jsonb) AS result",
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(values)),
                )
            ).fetchone()
            return row["result"]

        evidence_filter = {
            "evidence_trunk_id": "trunk/a",
            "evidence_external_group_id": "group/a",
            "evidence_author_subject_ref": "qq:author/a",
            "limit": 5,
        }
        global_first = (await search_relations(evidence_filter))["value"]
        assert len(global_first["items"]) == 5 and global_first["has_more"]
        assert all(item["cross_trunk"] for item in global_first["items"])
        assert (await search_relations({
            **evidence_filter, "evidence_external_group_id": "group/b",
        }))["value"]["items"] == []
        global_cursor = {
            "snapshot_xid": global_first["snapshot_xid"],
            "after_relation_id": global_first["next_cursor"]["relation_id"],
            "after_contribution_id": global_first["next_cursor"]["contribution_id"],
            "cursor_filter_digest": global_first["filter_digest"],
        }
        global_second = (await search_relations({
            **evidence_filter, **global_cursor,
        }))["value"]
        assert len(global_second["items"]) == 5
        assert not ({item["contribution_id"] for item in global_first["items"]}
                    & {item["contribution_id"] for item in global_second["items"]})
        assert (await search_relations({
            **evidence_filter, **global_cursor,
            "evidence_author_subject_ref": "qq:author/b",
        }))["error"]["code"] == "MEMORY_QUERY_INVALID"
        with pytest.raises(ForeignKeyViolation):
            await contribution("invalid/evidence", "n00", missing_part=True)
        with pytest.raises(ForeignKeyViolation):
            await contribution("invalid/revision", "n00", revision=2)
        await contribution("invalid/tail", "tail")
        await contribution("late/unit", "late-unit")

        async def register_query(name):
            await execute(
                "INSERT INTO {}.memory_queries (bot_id,query_id,operation_id,input_digest,"
                "current_source_id,input_batch_ref,context_revision,query_mode,"
                "fusion_policy_revision,accepted_deletion_epoch) VALUES "
                "(%s,%s,%s,%s,'a','batch/query',1,'TEXT','independent-facets/1',0)",
                (store.bot_id, name, "begin/" + name, digest),
            )
            await execute(
                "INSERT INTO {}.memory_query_parts (bot_id,query_id,query_part_index,source_unit_id,"
                "source_revision,occurrence_id,part_id,modality) VALUES "
                "(%s,%s,0,'query-input',1,'occ/query-input','text','TEXT')",
                (store.bot_id, name),
            )

        async def neighbours(query, node="seed", limit=5):
            row = await (
                await execute(
                    "SELECT {}.memory_read_graph_neighbours(%s::jsonb,%s::jsonb) AS result",
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "query_id": query, "source_unit_id": node, "association_limit": limit,
                    })),
                )
            ).fetchone()
            assert row["result"]["ok"], row["result"]
            result = row["result"]["value"]
            validator.validate(result)
            return result

        await execute(
            "INSERT INTO {}.memory_domains (bot_id,initialization_operation_id,input_digest,"
            "space_id,embedding_model_kind,model_revision_digest,dimensions,max_batch_items,normalization,"
            "fusion_policy_revision) VALUES (%s,'bind/test',%s,'space/test','TEXT_IMAGE',%s,4,8,'L2','independent-facets/1')",
            (store.bot_id, digest, digest),
        )

        async def ready_facet(name, source, query=None):
            await execute(
                "INSERT INTO {}.embedding_facets (bot_id,facet_id,purpose,query_id,input_kind,"
                "input_ref,input_revision,source_unit_id,"
                "source_revision,part_id,modality,selector_start_char,selector_end_char,"
                "input_payload_id,input_payload_digest,facet_content_digest,input_media_type,"
                "input_byte_length,space_id,accepted_deletion_epoch,state,index_ref_id,vector_digest,"
                "result_payload_id,result_payload_digest,published_at) VALUES "
                "(%s,%s,%s,%s,'SOURCE_UNIT',%s,1,%s,1,'text','TEXT',0,1,%s,%s,%s,'text/plain',1,'space/test',0,"
                "'READY',%s,%s,%s,%s,clock_timestamp())",
                (store.bot_id, name, "QUERY" if query else "DOCUMENT", query, source, source,
                 "input/" + name, digest, digest, name, digest, "result/" + name, digest),
            )

        await ready_facet("document/seed", "seed")
        await register_query("query/one")
        await register_query("query/two")
        async def progress(query):
            result = (await (await execute(
                "SELECT {}.memory_read_query_progress(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": store.bot_id}), Jsonb({"query_id": query})),
            )).fetchone())["result"]
            assert result["ok"], result
            value = result["value"]
            Draft202012Validator(package.snapshot.schema("schemas/query-progress-output.json")).validate(value)
            return value

        assert (await progress("query/two"))["phase"] == "NEED_PREPARATION"
        await execute(
            "INSERT INTO {}.query_facet_preparations (bot_id,query_id,facet_count,accepted_deletion_epoch) "
            "VALUES (%s,'query/two',0,0)", (store.bot_id,),
        )
        assert (await progress("query/two"))["phase"] == "NEED_MATCHING"
        await register_query("query/status")
        await execute(
            "INSERT INTO {}.embedding_facets (bot_id,facet_id,purpose,query_id,input_kind,"
            "input_ref,input_revision,source_unit_id,"
            "source_revision,part_id,modality,selector_start_char,selector_end_char,"
            "input_payload_id,input_payload_digest,facet_content_digest,input_media_type,"
            "input_byte_length,space_id,accepted_deletion_epoch) VALUES "
            "(%s,'query/status-facet','QUERY','query/status','SOURCE_UNIT','query-input',1,'query-input',1,'text','TEXT',0,1,"
            "'input/status',%s,%s,'text/plain',1,'space/test',0)",
            (store.bot_id,digest,digest),
        )
        # A partial preparation is not a complete preparation receipt.
        assert (await progress("query/status"))["phase"] == "NEED_PREPARATION"
        await execute(
            "INSERT INTO {}.query_facet_preparations (bot_id,query_id,facet_count,accepted_deletion_epoch) "
            "VALUES (%s,'query/status',1,0)", (store.bot_id,),
        )
        assert (await progress("query/status"))["phase"] == "WAIT_EMBEDDING"
        for state, phase in (("PREPARED","WAIT_EMBEDDING"), ("DISPATCHED","WAIT_EMBEDDING"),
                             ("UNKNOWN","BLOCKED_UNKNOWN")):
            await execute(
                "UPDATE {}.embedding_facets SET state=%s WHERE bot_id=%s AND facet_id='query/status-facet'",
                (state,store.bot_id),
            )
            result = await progress("query/status")
            assert result["phase"] == phase and result["ready_count"] == 0
        deadline = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        request = {"protocol":"forest.uow-step/1", "caller":{
            "component_id":"memory.graph","generation_id":"generation/test"},
            "input":{"protocol":"forest.memory-recall-request/1",
                     "query_id":"query/status","deadline_at":deadline},"prior":{}}
        async def request_work(body):
            return (await (await execute(
                "SELECT {}.memory_request_recall(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id":store.bot_id}),Jsonb(body)),
            )).fetchone())["result"]
        requested = await request_work(request)
        assert requested["ok"] and await request_work(request) == requested
        changed = {**request,"input":{**request["input"],"deadline_at":(
            datetime.now(timezone.utc)+timedelta(minutes=6)).isoformat()}}
        assert (await request_work(changed))["error"]["code"] == "MEMORY_RECALL_REQUEST_CONFLICT"
        async def claim(token):
            return (await (await execute(
                "SELECT {}.memory_claim_recall(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id":store.bot_id}),Jsonb({"protocol":"forest.owned-work-claim-request/1",
                    "claim_token":token,"lease_seconds":210})),
            )).fetchone())["result"]
        claimed = await claim("recall-claim/one")
        assert claimed["ok"] and claimed["value"]["payload"]["query_id"] == "query/status"
        assert await claim("recall-claim/one") == claimed
        assert (await claim("recall-claim/two"))["value"] is None
        settlement = {"protocol":"forest.owned-work-settle-request/1",
            "work_id":"query/status","claim_token":"recall-claim/one","attempt":1,
            "outcome":{"status":"SUCCEEDED","result":{"query_id":"query/status"}}}
        async def settle(body):
            return (await (await execute(
                "SELECT {}.memory_settle_recall(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id":store.bot_id}),Jsonb(body)),
            )).fetchone())["result"]
        settled = await settle(settlement)
        assert settled["ok"] and await settle(settlement) == settled
        assert (await claim("recall-claim/one"))["error"]["code"] == "MEMORY_RECALL_CLAIM_TOKEN_REUSED"
        assert (await settle({**settlement,"outcome":{"status":"FAILED", "error":{
            "code":"TEST","status":500,"retryable":True,"unknown":False}}}))["error"]["code"] == "MEMORY_RECALL_SETTLEMENT_CONFLICT"
        blocked = (await (await execute(
            "SELECT {}.memory_read_recall_work(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id":store.bot_id}),Jsonb({"query_id":"query/status"})),
        )).fetchone())["result"]
        assert blocked["value"]["state"] == "BLOCKED_UNKNOWN"
        await execute(
            "UPDATE {}.query_recall_work SET next_attempt_at=clock_timestamp()-interval '1 second' "
            "WHERE bot_id=%s AND query_id='query/status'",(store.bot_id,),
        )
        second = await claim("recall-claim/two")
        assert second["value"]["attempt"] == 2
        await execute(
            "UPDATE {}.query_recall_work SET lease_deadline=clock_timestamp()-interval '1 second' "
            "WHERE bot_id=%s AND query_id='query/status'",(store.bot_id,),
        )
        third = await claim("recall-claim/three")
        assert third["value"]["attempt"] == 3
        stale = {**settlement,"claim_token":"recall-claim/two","attempt":2}
        assert (await settle(stale))["error"]["code"] == "MEMORY_RECALL_CLAIM_LOST"
        await execute(
            "UPDATE {}.embedding_facets SET state='SUPPRESSED' "
            "WHERE bot_id=%s AND facet_id='query/status-facet'",(store.bot_id,),
        )
        assert (await progress("query/status"))["phase"] == "FAILED"
        final = {**settlement,"claim_token":"recall-claim/three","attempt":3}
        assert (await settle(final))["ok"]
        finished = (await (await execute(
            "SELECT {}.memory_read_recall_work(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id":store.bot_id}),Jsonb({"query_id":"query/status"})),
        )).fetchone())["result"]
        assert finished["value"]["state"] == "FAILED"
        assert finished["value"]["error_code"] == "MEMORY_RECALL_EMBEDDING_FAILED"
        expired_request = {**request,"input":{**request["input"],
            "query_id":"query/two","deadline_at":(
                datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}}
        assert (await request_work(expired_request))["error"]["code"] == "MEMORY_RECALL_DEADLINE_INVALID"
        await execute(
            "INSERT INTO {}.query_recall_work(bot_id,query_id,deadline_at) "
            "VALUES (%s,'query/two',clock_timestamp()-interval '1 second')",(store.bot_id,),
        )
        assert (await claim("recall-claim/expired"))["value"] is None
        expired = (await (await execute(
            "SELECT {}.memory_read_recall_work(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id":store.bot_id}),Jsonb({"query_id":"query/two"})),
        )).fetchone())["result"]
        assert expired["value"]["state"] == "FAILED"
        assert expired["value"]["error_code"] == "MEMORY_RECALL_DEADLINE"
        async def cancel_work(query):
            body = {"protocol":"forest.uow-step/1","caller":{
                "component_id":"memory.graph","generation_id":"generation/test"},
                "input":{"protocol":"forest.memory-recall-cancel/1",
                         "query_id":query,"reason_code":"SUPERSEDED"},"prior":{}}
            return (await (await execute(
                "SELECT {}.memory_cancel_recall(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id":store.bot_id}),Jsonb(body)),
            )).fetchone())["result"]
        assert (await cancel_work("query/status"))["value"]["status"] == "TERMINAL"
        await register_query("query/cancel-pending")
        pending_request = {**request,"input":{**request["input"],
            "query_id":"query/cancel-pending"}}
        assert (await request_work(pending_request))["ok"]
        cancelled = await cancel_work("query/cancel-pending")
        assert cancelled["value"]["status"] == "CANCELLED"
        assert await cancel_work("query/cancel-pending") == cancelled
        await register_query("query/cancel-claimed")
        claimed_request = {**request,"input":{**request["input"],
            "query_id":"query/cancel-claimed"}}
        assert (await request_work(claimed_request))["ok"]
        in_flight = await claim("recall-claim/cancelled")
        assert in_flight["value"]["work_id"] == "query/cancel-claimed"
        waiting_cancel = await cancel_work("query/cancel-claimed")
        assert waiting_cancel["value"]["status"] == "CANCELLING"
        cancelling = (await (await execute(
            "SELECT {}.memory_read_recall_work(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id":store.bot_id}),Jsonb({"query_id":"query/cancel-claimed"})),
        )).fetchone())["result"]
        assert cancelling["value"]["state"] == "CANCELLING"
        cancel_settlement = {**settlement,"work_id":"query/cancel-claimed",
            "claim_token":"recall-claim/cancelled","attempt":1,
            "outcome":{"status":"SUCCEEDED","result":{"query_id":"query/cancel-claimed"}}}
        assert (await settle(cancel_settlement))["ok"]
        assert (await cancel_work("query/cancel-claimed"))["value"]["status"] == "CANCELLED"
        assert (await claim("recall-claim/no-cancelled"))["value"] is None
        first = await neighbours("query/one")
        assert first == await neighbours("query/one")
        assert first["association_sampled"]
        assert len(first["edges"]) == 7  # Five sampled associations plus two sequence seeks.
        assert {edge["target_id"] for edge in first["edges"] if edge["relation"] == "SEQUENCE"} == {
            "before", "after",
        }
        def selected(page):
            return {edge["contribution_id"] for edge in page["edges"]
                    if edge["relation"] == "ASSOCIATED"}
        assert selected(first) != selected(await neighbours("query/two"))
        all_before = await neighbours("query/one", limit=128)
        assert len(selected(all_before)) == 40 and not all_before["association_sampled"]
        assert all(name.startswith("edge/") for name in selected(all_before))
        other = await neighbours("query/one", "n01")
        assert other["node"]["external_group_id"] == "group/b"
        assert other["node"]["trunk_id"] == "trunk/b"
        assert other["node"]["author_subject_ref"] == "qq:author/b"
        assert "seed" in {edge["target_id"] for edge in other["edges"]}
        assert (await neighbours("query/one", "tail"))["node"] is None

        # Fixed matching precedes the pure expansion and final write authority.
        policy = {**_configuration()["retrieval"], "sequence_weight": 1.0}
        await ready_facet("query/facet", "query-input", "query/one")
        await execute(
            "INSERT INTO {}.query_match_preparations (bot_id,query_id,operation_id,input_digest,"
            "space_id,policy,facet_count,accepted_deletion_epoch) VALUES "
            "(%s,'query/one','match/test',%s,'space/test',%s,1,0)",
            (store.bot_id, digest, Jsonb(policy)),
        )
        await execute(
            "INSERT INTO {}.query_match_receipts (bot_id,query_id,query_facet_id,operation_id,"
            "input_digest,accepted_deletion_epoch,match_count) VALUES "
            "(%s,'query/one','query/facet','receipt/test',%s,0,1)",
            (store.bot_id, digest),
        )
        await execute(
            "INSERT INTO {}.query_seed_matches (bot_id,query_id,query_facet_id,source_unit_id,"
            "source_revision,document_facet_id,candidate_rank,score) VALUES "
            "(%s,'query/one','query/facet','seed',1,'document/seed',1,0.9)",
            (store.bot_id,),
        )

        async def read_neighbours(_port, body, **_kwargs):
            return await neighbours(body["query_id"], body["source_unit_id"], body["association_limit"])

        region = await expand(
            SimpleNamespace(queries=SimpleNamespace(call=read_neighbours)), "query/one",
            [{"source_unit_id": "seed", "rank_score": 1.0 / 61}], policy,
        )
        assert len(region["nodes"]) == 32 and region["truncated"]
        fields = ("source_unit_id", "source_revision", "rank_score", "activation", "is_seed",
                  "path_parent_source_unit_id", "path_edge_id")
        body = {**region, "nodes": [{key: item[key] for key in fields} for item in region["nodes"]],
                "protocol": "forest.memory-retrieval-freeze/1", "query_id": "query/one",
                "deletion_epoch_step_id": "epoch"}

        async def freeze(nodes):
            envelope = {"protocol": "forest.uow-step/1", "caller": {"component_id": "memory.graph"},
                        "input": {**body, "nodes": nodes},
                        "prior": {"epoch": {"protocol": "forest.payload-epoch/1", "deletion_epoch": 0}}}
            async with conn.transaction():
                row = await (await execute(
                    "SELECT {}.memory_freeze_retrieval(%s::jsonb,%s::jsonb) AS result",
                    (Jsonb({"bot_id": store.bot_id, "operation_id": "freeze/test",
                            "participant_input_digest": digest}), Jsonb(envelope)),
                )).fetchone()
            return row["result"]

        for index, mutation, code in (
            (0, {"rank_score": 0.1}, "MEMORY_RETRIEVAL_STALE"),
            (1, {"activation": 0.123}, "MEMORY_RETRIEVAL_INVALID"),
            (1, {"path_edge_id": "fake/edge"}, "MEMORY_RETRIEVAL_STALE"),
            (1, {"path_parent_source_unit_id": body["nodes"][-1]["source_unit_id"]}, "MEMORY_RETRIEVAL_STALE"),
            (1, {"source_revision": 2}, "MEMORY_RETRIEVAL_STALE"),
        ):
            changed = [dict(item) for item in body["nodes"]]
            changed[index].update(mutation)
            rejected = await freeze(changed)
            assert not rejected["ok"] and rejected["error"]["code"] == code
            assert (await (await execute("SELECT count(*) AS n FROM {}.retrieval_bundles", ())).fetchone())["n"] == 0
        frozen = await freeze(body["nodes"])
        assert frozen["ok"], frozen
        assert frozen["value"]["result_count"] == 32
        assert frozen["value"]["seed_count"] == 1
        assert await freeze([]) == frozen  # Historic metadata is immutable, not a new region.
        assert (await (await execute("SELECT count(*) AS n FROM {}.retrieval_relations", ())).fetchone())["n"] == 31
        parts_validator = Draft202012Validator(package.snapshot.schema("schemas/read-retrieval-parts-output.json"))

        async def read_parts(index, after=-1):
            result = (await (await execute(
                "SELECT {}.memory_read_retrieval_parts(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": store.bot_id}), Jsonb({
                    "query_id": "query/one", "item_index": index, "after_part_index": after, "limit": 1,
                })),
            )).fetchone())["result"]
            assert result["ok"], result
            parts_validator.validate(result["value"])
            return result["value"]

        seed_parts = await read_parts(0)
        assert seed_parts["node"]["matched_part_ids"] == ["text"]
        assert seed_parts["node"]["is_current_source"]
        assert seed_parts["parts"][0]["payload"]["payload_id"] == "text/seed"
        assert not seed_parts["has_more"]
        assert (await read_parts(0, after=0))["parts"] == []
        assert (await read_parts(63))["node"] is None
        associated_index = next(i for i, item in enumerate(body["nodes"]) if item["source_unit_id"].startswith("n"))
        cross_source = await read_parts(associated_index)
        assert cross_source["node"]["external_group_id"] == "group/b"
        assert not cross_source["node"]["is_current_source"]
        assert cross_source["node"]["matched_part_ids"] == []
        dependencies = (await (await execute(
            "SELECT {}.memory_read_retrieval_dependencies(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": store.bot_id}), Jsonb({
                "query_id": "query/one", "item_index": associated_index, "after_source_unit_id": None,
            })),
        )).fetchone())["result"]
        assert dependencies["ok"] and dependencies["value"]["available"]
        Draft202012Validator(package.snapshot.schema("schemas/read-retrieval-dependencies-output.json")).validate(dependencies["value"])
        dependency_ids = {ref["source_unit_id"] for ref in dependencies["value"]["refs"]}
        assert {"seed", "after", body["nodes"][associated_index]["source_unit_id"]} <= dependency_ids
        with pytest.raises(CheckViolation):
            await execute(
                "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                "source_unit_id,source_revision,part_id) VALUES (%s,'edge/0',64,'seed',1,'text')",
                (store.bot_id,),
            )
        sequence_index = next(i for i, item in enumerate(body["nodes"]) if item["source_unit_id"] in {"before", "after"})
        assert (await read_parts(sequence_index))["node"] is not None
        # Private SQL checks the selection; the public manifest independently
        # verifies the candidate's exact Kernel Payload reference.
        async def publish(indices, missing):
            envelope = {"protocol": "forest.uow-step/1", "caller": {"component_id": "memory.graph"},
                        "input": {"protocol": "forest.memory-evidence-publish/1", "query_id": "query/one",
                                  "item_indexes": indices, "missing_count": missing, "content_truncated": False,
                                  "deletion_epoch_step_id": "epoch", "evidence_ref": {
                                      "payload_id": "evidence/test", "digest": digest,
                                      "owner_component_id": "memory.graph", "owner_kind": "retrieval.evidence",
                                      "owner_id": frozen["value"]["bundle_id"] + "/test"}},
                        "prior": {"epoch": {"protocol": "forest.payload-epoch/1", "deletion_epoch": 0}}}
            async with conn.transaction():
                return (await (await execute(
                    "SELECT {}.memory_publish_retrieval_evidence(%s::jsonb,%s::jsonb) AS result",
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(envelope)),
                )).fetchone())["result"]

        rejected = await publish([63], 31)
        assert not rejected["ok"] and rejected["error"]["code"] == "MEMORY_EVIDENCE_INVALID"
        omitted = await publish([0], 31)
        assert not omitted["ok"] and omitted["error"]["code"] == "MEMORY_EVIDENCE_INVALID"
        assert (await (await execute("SELECT count(*) AS n FROM {}.retrieval_materializations", ())).fetchone())["n"] == 0
        published = await publish(list(range(32)), 0)
        assert published["ok"], published
        Draft202012Validator(package.snapshot.schema("schemas/read-retrieval-materialization-output.json")).validate(published["value"])
        assert published["value"]["item_indexes"] == list(range(32))
        with pytest.raises(ObjectNotInPrerequisiteState):
            await execute("UPDATE {}.retrieval_items SET activation=0.2", ())

        await unit("late-unit", "b", 41, 41)
        await contribution("late/relation", "n00")
        with pytest.raises(ForeignKeyViolation):
            await execute(
                "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                "source_unit_id,source_revision,part_id) VALUES (%s,'edge/0',2,'before',1,'text')",
                (store.bot_id,),
            )
        old = selected(await neighbours("query/one", limit=128))
        assert len(old) == 40 and not {"late/unit", "late/relation"} & old
        await register_query("query/new")
        new = selected(await neighbours("query/new", limit=128))
        assert len(new) == 42 and {"late/unit", "late/relation", "edge/0"} <= new

        async with conn.transaction():
            await execute(
                "INSERT INTO {}.relation_contributions (bot_id,contribution_id,operation_id,"
                "input_digest,relation_id,relation_kind,left_kind,left_id,right_kind,right_id,"
                "origin_kind,weight_micros,admitted_deletion_epoch,cross_trunk) "
                "SELECT %s,'fanout/'||n,'fanout-op/'||n,%s,'fanout-rel/'||n,"
                "'ASSOCIATES_WITH','SOURCE_UNIT','seed','SOURCE_UNIT',"
                "'n'||lpad((n %% 40)::text,2,'0'),'RULE',500000,0,true "
                "FROM generate_series(1,20000) n",
                (store.bot_id, digest),
            )
            await execute(
                "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                "source_unit_id,source_revision,part_id) "
                "SELECT %s,'fanout/'||n,0,'seed',1,'text' "
                "FROM generate_series(1,20000) n",
                (store.bot_id,),
            )
        await register_query("query/fanout-a")
        await register_query("query/fanout-b")
        started = time.perf_counter()
        fanout_a = await neighbours("query/fanout-a", limit=5)
        elapsed = time.perf_counter() - started
        fanout_b = await neighbours("query/fanout-b", limit=5)
        sampled_a = [edge["edge_id"] for edge in fanout_a["edges"]
                     if edge["relation"] == "ASSOCIATED"]
        sampled_b = [edge["edge_id"] for edge in fanout_b["edges"]
                     if edge["relation"] == "ASSOCIATED"]
        assert fanout_a["association_sampled"] and fanout_b["association_sampled"]
        assert len(sampled_a) == len(sampled_b) == 5
        assert sampled_a == [edge["edge_id"] for edge in
                             (await neighbours("query/fanout-a", limit=5))["edges"]
                             if edge["relation"] == "ASSOCIATED"]
        assert sampled_a != sampled_b
        print(f"20k-edge neighbour sample: {elapsed:.3f}s")

        async with conn.transaction():
            await execute(
                "INSERT INTO {}.relation_contributions (bot_id,contribution_id,operation_id,"
                "input_digest,relation_id,relation_kind,left_kind,left_id,right_kind,right_id,"
                "origin_kind,weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                "(%s,'evidence/paged','op/evidence/paged',%s,'relation/evidence/paged',"
                "'ASSOCIATES_WITH','SOURCE_UNIT','seed','SOURCE_UNIT','n00','RULE',500000,0,true)",
                (store.bot_id, digest),
            )
            await execute(
                "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                "source_unit_id,source_revision,part_id) "
                "SELECT %s,'evidence/paged',n,'n'||lpad(n::text,2,'0'),1,'text' "
                "FROM generate_series(0,19) n",
                (store.bot_id,),
            )
        snapshot = (await (await conn.execute("SELECT pg_current_snapshot()::text AS snapshot")).fetchone())["snapshot"]
        evidence_validator = Draft202012Validator(
            package.snapshot.schema("schemas/query-relation-evidence-output.json")
        )

        async def relation_evidence(values, *, bot_id=store.bot_id):
            row = await (await execute(
                "SELECT {}.memory_query_relation_evidence(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot_id}), Jsonb({
                    "relation_id": "relation/evidence/paged", "contribution_id": "evidence/paged",
                    "snapshot_xid": snapshot, **values,
                })),
            )).fetchone()
            return row["result"]

        first_evidence = await relation_evidence({"limit": 16})
        assert first_evidence["ok"], first_evidence
        evidence_validator.validate(first_evidence["value"])
        assert len(first_evidence["value"]["items"]) == 16
        assert first_evidence["value"]["total_count"] == 20
        assert first_evidence["value"]["has_more"]
        second_evidence = await relation_evidence({
            "after_evidence_index": first_evidence["value"]["next_after"], "limit": 16,
        })
        assert second_evidence["ok"], second_evidence
        evidence_validator.validate(second_evidence["value"])
        assert [item["evidence_index"] for item in second_evidence["value"]["items"]] == [16, 17, 18, 19]
        assert not second_evidence["value"]["has_more"]
        assert second_evidence["value"]["next_after"] is None
        assert all(item["external_group_id"] == "group/b" and item["trunk_id"] == "trunk/b"
                   and item["author_subject_ref"] == "qq:author/b"
                   for item in second_evidence["value"]["items"])
        assert (await relation_evidence({}, bot_id="another-bot"))["error"]["code"] == "MEMORY_RELATION_NOT_VISIBLE"
        assert (await relation_evidence({"snapshot_xid": "invalid"}))["error"]["code"] == "MEMORY_QUERY_INVALID"

        await execute(
            "UPDATE {}.source_units SET eligibility_state='SUPPRESSED',"
            "eligibility_revision=eligibility_revision+1 WHERE bot_id=%s AND source_unit_id='n01'",
            (store.bot_id,),
        )
        assert (await relation_evidence({}))["error"]["code"] == "MEMORY_RELATION_NOT_VISIBLE"
        assert "edge/1" not in selected(await neighbours("query/new", limit=128))
        await execute(
            "UPDATE {}.memory_sources SET status='RETIRED' WHERE bot_id=%s AND source_id='b'",
            (store.bot_id,),
        )
        assert selected(await neighbours("query/new", limit=128)) == set()
        assert (await read_parts(associated_index))["node"] is None
        unavailable = (await (await execute(
            "SELECT {}.memory_read_retrieval_materialization(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": store.bot_id}), Jsonb({"query_id": "query/one"})),
        )).fetchone())["result"]
        assert not unavailable["ok"] and unavailable["error"]["code"] == "MEMORY_EVIDENCE_UNAVAILABLE"
        await execute(
            "UPDATE {}.source_units SET eligibility_state='SUPPRESSED',"
            "eligibility_revision=eligibility_revision+1 WHERE bot_id=%s AND source_unit_id='seed'",
            (store.bot_id,),
        )
        hidden = await read_parts(sequence_index)
        assert hidden["node"] is None and hidden["parts"] == []
        assert not hidden["has_more"]
