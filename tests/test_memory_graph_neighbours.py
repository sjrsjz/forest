import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from psycopg import sql
from psycopg.errors import ForeignKeyViolation, ObjectNotInPrerequisiteState
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.graph_region import expand
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_neighbour_sampling_is_bounded_snapshot_visible_and_source_attributed(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "neighbour-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
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
                    "origin_kind,weight_micros,admitted_deletion_epoch) VALUES "
                    "(%s,%s,%s,%s,%s,'ASSOCIATES_WITH','SOURCE_UNIT',%s,'SOURCE_UNIT',%s,'RULE',500000,0)",
                    (store.bot_id, name, "op/" + name, digest, "relation/" + name,
                     target if reverse else "seed", "seed" if reverse else target),
                )
                await execute(
                    "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                    "source_unit_id,source_revision,part_id) VALUES (%s,%s,0,'seed',%s,%s)",
                    (store.bot_id, name, revision, "missing" if missing_part else "text"),
                )

        for index in range(40):
            await contribution(f"edge/{index}", f"n{index:02}", reverse=bool(index % 2))
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
            "space_id,embedding_model_kind,model_revision_digest,dimensions,normalization,"
            "fusion_policy_revision) VALUES (%s,'bind/test',%s,'space/test','TEXT_IMAGE',%s,4,'L2','independent-facets/1')",
            (store.bot_id, digest, digest),
        )

        async def ready_facet(name, source, query=None):
            await execute(
                "INSERT INTO {}.embedding_facets (bot_id,facet_id,purpose,query_id,source_unit_id,"
                "source_revision,part_id,modality,selector_start_char,selector_end_char,"
                "input_payload_id,input_payload_digest,facet_content_digest,input_media_type,"
                "input_byte_length,space_id,accepted_deletion_epoch,state,index_ref_id,vector_digest,"
                "result_payload_id,result_payload_digest,published_at) VALUES "
                "(%s,%s,%s,%s,%s,1,'text','TEXT',0,1,%s,%s,%s,'text/plain',1,'space/test',0,"
                "'READY',%s,%s,%s,%s,clock_timestamp())",
                (store.bot_id, name, "QUERY" if query else "DOCUMENT", query, source,
                 "input/" + name, digest, digest, name, digest, "result/" + name, digest),
            )

        await ready_facet("document/seed", "seed")
        await register_query("query/one")
        await register_query("query/two")
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
        policy = _configuration()["retrieval"]
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
        with pytest.raises(ObjectNotInPrerequisiteState):
            await execute("UPDATE {}.retrieval_items SET activation=0.2", ())

        await unit("late-unit", "b", 41, 41)
        await contribution("late/relation", "n00")
        with pytest.raises(ForeignKeyViolation):
            await execute(
                "INSERT INTO {}.relation_evidence (bot_id,contribution_id,evidence_index,"
                "source_unit_id,source_revision,part_id) VALUES (%s,'edge/0',1,'after',1,'text')",
                (store.bot_id,),
            )
        old = selected(await neighbours("query/one", limit=128))
        assert len(old) == 40 and not {"late/unit", "late/relation"} & old
        await register_query("query/new")
        new = selected(await neighbours("query/new", limit=128))
        assert len(new) == 42 and {"late/unit", "late/relation", "edge/0"} <= new

        await execute(
            "UPDATE {}.source_units SET eligibility_state='SUPPRESSED',"
            "eligibility_revision=eligibility_revision+1 WHERE bot_id=%s AND source_unit_id='n01'",
            (store.bot_id,),
        )
        assert "edge/1" not in selected(await neighbours("query/new", limit=128))
        await execute(
            "UPDATE {}.memory_sources SET status='RETIRED' WHERE bot_id=%s AND source_id='b'",
            (store.bot_id,),
        )
        assert selected(await neighbours("query/new", limit=128)) == set()
