"""One committed reply must learn at most one durable cross-trunk relation."""

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.graph_region import expand
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_association_waits_for_archive_and_repeated_recall_does_not_add_weight(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "association-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    schema = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    bot = store.bot_id

    async with store.pool.connection() as conn:
        async def execute(statement, params=()):
            return await conn.execute(sql.SQL(statement).format(schema), params)

        for group in ("question", "memory"):
            await execute(
                "INSERT INTO {}.memory_sources(bot_id,source_id,registration_operation_id,"
                "input_digest,source_kind,platform,realm_id,external_group_id,"
                "identity_incarnation,identity_digest,trunk_id) VALUES "
                "(%s,%s,%s,%s,'GROUP','qq','realm',%s,1,%s,%s)",
                (bot, group, "register/" + group, digest, group,
                 "sha256:" + hashlib.sha256(group.encode()).hexdigest(),
                 "trunk/" + group),
            )
            await execute(
                "INSERT INTO {}.memory_trunks(bot_id,trunk_id,source_id) VALUES (%s,%s,%s)",
                (bot, "trunk/" + group, group),
            )
        for unit, source, state in (
            ("trigger", "question", "REGISTERED"),
            ("seed", "memory", "ELIGIBLE"),
        ):
            await execute(
                "INSERT INTO {}.source_units(bot_id,source_unit_id,capture_operation_id,"
                "input_digest,source_id,trunk_id,source_order_seq,source_revision,unit_kind,"
                "modality,part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,author_owner_id,"
                "accepted_deletion_epoch,eligibility_state,eligible_at,occurred_at) VALUES "
                "(%s,%s,%s,%s,%s,%s,1,1,'GROUP_MESSAGE','TEXT',1,%s,%s,%s,%s,"
                "'adapter.onebot','message.author',%s,0,%s,"
                "CASE WHEN %s='ELIGIBLE' THEN clock_timestamp() ELSE NULL END,clock_timestamp())",
                (bot, unit, "capture/" + unit, digest, source, "trunk/" + source,
                 digest, "author/" + unit, "author-payload/" + unit, digest,
                 unit, state, state),
            )
            await execute(
                "INSERT INTO {}.source_parts(bot_id,source_unit_id,part_id,part_index,"
                "part_kind,media_type,payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id,byte_length) VALUES "
                "(%s,%s,'text',0,'TEXT','text/plain',%s,%s,'adapter.onebot',"
                "'context.input',%s,1)",
                (bot, unit, "payload/" + unit, digest, unit),
            )
            await execute(
                "INSERT INTO {}.source_occurrences(bot_id,occurrence_id,source_unit_id,"
                "binding_ref,conversation_ref,event_ref,ingress_seq,input_batch_ref,"
                "context_revision,eligibility_state,observed_at) VALUES "
                "(%s,%s,%s,'binding/main',%s,%s,1,%s,1,%s,clock_timestamp())",
                (bot, "occ/" + unit, unit, "conv/" + source, "event/" + unit,
                 "batch/query" if unit == "trigger" else "batch/seed",
                 "ADMITTED" if unit == "trigger" else "ELIGIBLE"),
            )
        await execute(
            "INSERT INTO {}.memory_domains(bot_id,initialization_operation_id,input_digest,"
            "space_id,embedding_model_kind,model_revision_digest,dimensions,normalization,"
            "fusion_policy_revision) VALUES "
            "(%s,'bind/test',%s,'space/test','TEXT_IMAGE',%s,4,'L2','independent-facets/1')",
            (bot, digest, digest),
        )
        await execute(
            "INSERT INTO {}.memory_queries(bot_id,query_id,operation_id,input_digest,"
            "current_source_id,input_batch_ref,context_revision,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'query/test','begin/test',%s,'question','batch/query',1,'TEXT',"
            "'independent-facets/1',0)", (bot, digest),
        )
        await execute(
            "INSERT INTO {}.memory_query_parts(bot_id,query_id,query_part_index,"
            "source_unit_id,source_revision,occurrence_id,part_id,modality) VALUES "
            "(%s,'query/test',0,'trigger',1,'occ/trigger','text','TEXT')", (bot,),
        )
        await execute(
            "INSERT INTO {}.query_match_preparations(bot_id,query_id,operation_id,"
            "input_digest,space_id,policy,facet_count,accepted_deletion_epoch) VALUES "
            "(%s,'query/test','match/test',%s,'space/test','{{}}'::jsonb,0,0)",
            (bot, digest),
        )
        await execute(
            "INSERT INTO {}.retrieval_bundles(bot_id,bundle_id,query_id,operation_id,"
            "input_digest,accepted_deletion_epoch,read_count,truncated,association_sampled,"
            "seed_count,result_count) VALUES "
            "(%s,'bundle/test','query/test','freeze/test',%s,0,1,false,false,1,1)",
            (bot, digest),
        )
        await execute(
            "INSERT INTO {}.retrieval_items(bot_id,bundle_id,item_index,source_unit_id,"
            "source_revision,rank_score,activation,is_seed) VALUES "
            "(%s,'bundle/test',0,'seed',1,0.8,1,true)", (bot,),
        )

        seed = {"bundle_id": "bundle/test", "query_id": "query/test",
                "source_unit_id": "seed", "source_revision": 1,
                "rank_score": 0.8, "part_ids": ["text"]}

        async def commit(operation, reply, candidate=seed):
            context = {"bot_id": bot, "operation_id": operation,
                       "participant_input_digest": digest}
            envelope = {"protocol": "forest.uow-step/1",
                        "caller": {"component_id": "conversation.scheduler",
                                   "generation_id": "generation/test"},
                        "input": {"conversation_ref": "conv/question", "reply_ref": reply},
                        "prior": {
                            "agent": {"state": "COMPLETED",
                                      "conversation_ref": "conv/question",
                                      "learning": {"protocol": "forest.agent-learning/1",
                                                   "proposals": [{"request_id": "request/test",
                                                                  "input_revision": 1,
                                                                  "trigger_ref": "trigger/test",
                                                                  "seed": candidate}],
                                                   "truncated": False}},
                            "conversation": {"reply_ref": reply,
                                             "answered_events": [{
                                                 "trigger_ref": "trigger/test",
                                                 "event_ref": "event/trigger"}]}}}
            row = await (await execute(
                "SELECT {}.memory_associate_final_reply(%s::jsonb,%s::jsonb) AS result",
                (Jsonb(context), Jsonb(envelope)),
            )).fetchone()
            return row["result"]

        forged = await commit("reply/forged", "reply/forged",
                              {**seed, "source_unit_id": "not-a-frozen-seed"})
        assert forged["ok"] and forged["value"] == {
            "protocol": "forest.association-journal/1",
            "journaled": 0, "promoted": 0, "skipped": 1,
        }
        first = await commit("reply/one", "reply/one")
        assert first["ok"] and first["value"]["journaled"] == 1
        assert first["value"]["promoted"] == 0
        assert (await commit("reply/one", "reply/one"))["ok"]
        assert (await (await execute(
            "SELECT count(*) AS count FROM {}.relation_contributions WHERE bot_id=%s",
            (bot,),
        )).fetchone())["count"] == 0

        await execute(
            "UPDATE {}.source_occurrences SET eligibility_state='ELIGIBLE' "
            "WHERE bot_id=%s AND occurrence_id='occ/trigger'", (bot,),
        )
        await execute(
            "UPDATE {}.source_units SET eligibility_state='ELIGIBLE',"
            "eligibility_revision=eligibility_revision+1,eligible_at=clock_timestamp(),"
            "updated_at=clock_timestamp() WHERE bot_id=%s AND source_unit_id='trigger'",
            (bot,),
        )
        journal = await (await execute(
            "SELECT journal_id FROM {}.association_journal WHERE bot_id=%s "
            "AND operation_id='reply/one'", (bot,),
        )).fetchone()
        promoted = await (await execute(
            "SELECT {}.memory_promote_association(%s,%s) AS promoted",
            (bot, journal["journal_id"]),
        )).fetchone()
        assert promoted["promoted"] is True
        second = await commit("reply/two", "reply/two")
        assert second["ok"] and second["value"]["promoted"] == 1
        relation = await (await execute(
            "SELECT count(*) AS count,bool_and(cross_trunk) AS cross_trunk "
            "FROM {}.relation_contributions WHERE bot_id=%s",
            (bot,),
        )).fetchone()
        assert relation == {"count": 1, "cross_trunk": True}
        assert (await (await execute(
            "SELECT count(*) AS count FROM {}.relation_evidence WHERE bot_id=%s",
            (bot,),
        )).fetchone())["count"] == 2

    # A later question in the original group sees the promoted edge to the
    # other trunk, with both endpoints still carrying their own source labels.
    async with store.pool.connection() as conn:
        async def execute_later(statement, params=()):
            return await conn.execute(sql.SQL(statement).format(schema), params)

        await execute_later(
            "INSERT INTO {}.source_units(bot_id,source_unit_id,capture_operation_id,"
            "input_digest,source_id,trunk_id,source_order_seq,source_revision,unit_kind,"
            "modality,part_count,content_manifest_digest,author_subject_ref,author_payload_id,"
            "author_payload_digest,author_owner_component_id,author_owner_kind,author_owner_id,"
            "accepted_deletion_epoch,eligibility_state,eligible_at,occurred_at) VALUES "
            "(%s,'later','capture/later',%s,'question','trunk/question',2,1,"
            "'GROUP_MESSAGE','TEXT',1,%s,'author/later','author-payload/later',%s,"
            "'adapter.onebot','message.author','later',0,'REGISTERED',NULL,clock_timestamp())",
            (bot, digest, digest, digest),
        )
        await execute_later(
            "INSERT INTO {}.source_parts(bot_id,source_unit_id,part_id,part_index,"
            "part_kind,media_type,payload_id,payload_digest,payload_owner_component_id,"
            "payload_owner_kind,payload_owner_id,byte_length) VALUES "
            "(%s,'later','text',0,'TEXT','text/plain','payload/later',%s,"
            "'adapter.onebot','context.input','later',1)",
            (bot, digest),
        )
        await execute_later(
            "INSERT INTO {}.source_occurrences(bot_id,occurrence_id,source_unit_id,"
            "binding_ref,conversation_ref,event_ref,ingress_seq,input_batch_ref,"
            "context_revision,eligibility_state,observed_at) VALUES "
            "(%s,'occ/later','later','binding/main','conv/question','event/later',"
            "2,'batch/later',1,'ADMITTED',clock_timestamp())",
            (bot,),
        )
        await execute_later(
            "INSERT INTO {}.memory_queries(bot_id,query_id,operation_id,input_digest,"
            "current_source_id,input_batch_ref,context_revision,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'query/later','begin/later',%s,'question','batch/later',1,'TEXT',"
            "'independent-facets/1',0)",
            (bot, digest),
        )
        await execute_later(
            "INSERT INTO {}.memory_query_parts(bot_id,query_id,query_part_index,"
            "source_unit_id,source_revision,occurrence_id,part_id,modality) VALUES "
            "(%s,'query/later',0,'later',1,'occ/later','text','TEXT')",
            (bot,),
        )
        observed = []
        for unit in ("seed", "trigger"):
            row = await (await execute_later(
                "SELECT {}.memory_read_graph_neighbours(%s::jsonb,%s::jsonb) AS result",
                (
                    Jsonb({"bot_id": bot}),
                    Jsonb({
                        "query_id": "query/later",
                        "source_unit_id": unit,
                        "association_limit": 8,
                    }),
                ),
            )).fetchone()
            assert row["result"]["ok"], row["result"]
            observed.append(row["result"]["value"])

        class GraphQueries:
            async def call(self, name, request, *, timeout):
                assert name == "memory.read-graph-neighbours" and timeout == 10
                row = await (await execute_later(
                    "SELECT {}.memory_read_graph_neighbours(%s::jsonb,%s::jsonb) AS result",
                    (Jsonb({"bot_id": bot}), Jsonb(request)),
                )).fetchone()
                assert row["result"]["ok"], row["result"]
                return row["result"]["value"]

        region = await expand(
            SimpleNamespace(queries=GraphQueries()),
            "query/later", [{"source_unit_id": "seed", "rank_score": 0.8}],
            {
                "max_graph_nodes": 3, "max_association_edges_per_node": 8,
                "sequence_weight": 0.5, "association_weight": 0.9,
                "min_graph_relevance": 0.01,
            },
        )
    assert observed[0]["node"]["external_group_id"] == "memory"
    assert observed[0]["node"]["author_subject_ref"] == "author/seed"
    assert observed[1]["node"]["external_group_id"] == "question"
    assert observed[1]["node"]["author_subject_ref"] == "author/trigger"
    assert any(
        edge["relation"] == "ASSOCIATED" and edge["target_id"] == "trigger"
        for edge in observed[0]["edges"]
    )
    assert [node["source_unit_id"] for node in region["nodes"]] == ["seed", "trigger"]
    assert region["nodes"][1]["trunk_id"] == "trunk/question"
    assert region["nodes"][1]["external_group_id"] == "question"
    assert region["nodes"][1]["path_parent_source_unit_id"] == "seed"
    assert region["nodes"][1]["activation"] == 0.9
