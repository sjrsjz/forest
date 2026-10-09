"""Cross-trunk association must retain attribution and converge with forget."""

import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.graph_region import expand
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


@pytest.mark.postgres
async def test_association_waits_for_archive_and_repeated_recall_does_not_add_weight(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
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
                "(%s,%s,%s,%s,%s,%s,1,1,'GROUP_MESSAGE','TEXT',2,%s,%s,%s,%s,"
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
                "INSERT INTO {}.source_parts(bot_id,source_unit_id,part_id,part_index,"
                "part_kind,media_type,payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id,byte_length) VALUES "
                "(%s,%s,'text-alt',1,'TEXT','text/plain',%s,%s,'adapter.onebot',"
                "'context.input',%s,1)",
                (bot, unit, "payload-alt/" + unit, digest, unit),
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
            "space_id,embedding_model_kind,model_revision_digest,dimensions,max_batch_items,normalization,"
            "fusion_policy_revision) VALUES "
            "(%s,'bind/test',%s,'space/test','TEXT_IMAGE',%s,4,8,'L2','independent-facets/1')",
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
            "protocol": "forest.association-journal/2",
            "journaled": 0, "skipped": 1,
        }
        first = await commit("reply/one", "reply/one")
        assert first["ok"] and first["value"]["journaled"] == 1
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
        await execute(
            "UPDATE {}.association_journal SET promotion_ready=true "
            "WHERE bot_id=%s AND operation_id='reply/one'", (bot,),
        )

        async def settle_next(token):
            claim = (await (await execute(
                "SELECT {}.memory_claim_association_promotion(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb({
                    "protocol": "forest.owned-work-claim-request/1",
                    "claim_token": token, "lease_seconds": 60,
                })),
            )).fetchone())["result"]
            assert claim["ok"] and claim["value"] is not None
            work = claim["value"]
            advanced = (await (await execute(
                "SELECT {}.memory_promote_claimed_association(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb(work["payload"])),
            )).fetchone())["result"]
            assert advanced["ok"] and advanced["value"]["promoted"] is True
            replay = (await (await execute(
                "SELECT {}.memory_promote_claimed_association(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb(work["payload"])),
            )).fetchone())["result"]
            assert replay == advanced
            settlement = {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": work["work_id"], "claim_token": token,
                "attempt": work["attempt"],
                "outcome": {"status": "SUCCEEDED", "result": advanced["value"]},
            }
            settled = (await (await execute(
                "SELECT {}.memory_settle_association_promotion(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb(settlement)),
            )).fetchone())["result"]
            assert settled["ok"] and settled["value"]["work_id"] == work["work_id"]
            assert (await (await execute(
                "SELECT {}.memory_settle_association_promotion(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb(settlement)),
            )).fetchone())["result"] == settled

        await settle_next("association/one")
        second = await commit("reply/two", "reply/two")
        assert second["ok"] and second["value"]["journaled"] == 1
        await settle_next("association/two")
        async def journal_page(filters):
            row = await (await execute(
                "SELECT {}.memory_query_association_journals(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb(filters)),
            )).fetchone()
            return row["result"]

        first_page = await journal_page({
            "state": "PROMOTED", "trigger_external_group_id": "question",
            "seed_external_group_id": "memory", "limit": 1,
        })
        assert first_page["ok"], first_page
        page = first_page["value"]
        Draft202012Validator(json.loads((
            Path(__file__).parents[1] / "src/forest/builtin/memory_graph/schemas/"
            "query-association-journals-output.json"
        ).read_text(encoding="utf-8"))).validate(page)
        assert page["has_more"] and len(page["items"]) == 1
        assert page["items"][0]["trigger_evidence"]["trunk_id"] == "trunk/question"
        assert page["items"][0]["seed_evidence"]["external_group_id"] == "memory"
        assert page["items"][0]["cross_trunk"] is True
        assert page["items"][0]["relation_active"] is True
        next_filters = {
            "state": "PROMOTED", "trigger_external_group_id": "question",
            "seed_external_group_id": "memory", "limit": 1,
            "snapshot_xid": page["snapshot_xid"],
            "cursor_filter_digest": page["filter_digest"],
            "after_created_at": page["next_cursor"]["created_at"],
            "after_journal_id": page["next_cursor"]["journal_id"],
        }
        second_page = await journal_page(next_filters)
        assert second_page["ok"] and len(second_page["value"]["items"]) == 1
        assert second_page["value"]["has_more"] is False
        assert (await journal_page({**next_filters,
            "seed_external_group_id": "question"}))["error"]["code"] == (
                "MEMORY_QUERY_INVALID"
            )
        await execute(
            "INSERT INTO {}.association_journal("
            "bot_id,journal_id,operation_id,input_digest,reply_ref,bundle_id,"
            "query_id,trigger_ref,trigger_source_unit_id,trigger_source_revision,"
            "trigger_part_id,seed_source_unit_id,seed_source_revision,seed_part_id,"
            "seed_rank_score,promotion_ready) VALUES "
            "(%s,'journal/stale','reply/stale',%s,'reply/stale','bundle/test',"
            "'query/test','trigger/stale','trigger',1,'text-alt','seed',1,'text-alt',0.8,true)",
            (bot, digest),
        )
        pending = await journal_page({"state": "PENDING_ARCHIVE", "journal_id": "journal/stale"})
        assert pending["ok"] and pending["value"]["items"][0]["journal_id"] == (
            "journal/stale"
        )
        assert not (await journal_page({
            "state": "PENDING_ARCHIVE", "journal_id": "journal/stale",
            "snapshot_xid": page["snapshot_xid"],
        }))["value"]["items"]

        async def claim_only(token):
            result = (await (await execute(
                "SELECT {}.memory_claim_association_promotion(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb({
                    "protocol": "forest.owned-work-claim-request/1",
                    "claim_token": token, "lease_seconds": 60,
                })),
            )).fetchone())["result"]
            assert result["ok"] and result["value"] is not None
            return result["value"]

        old_claim = await claim_only("association/expired")
        await execute(
            "UPDATE {}.association_journal SET lease_deadline=clock_timestamp()-interval '1 second' "
            "WHERE bot_id=%s AND journal_id='journal/stale'", (bot,),
        )
        current_claim = await claim_only("association/current")
        assert current_claim["work_id"] == old_claim["work_id"]
        assert current_claim["attempt"] == old_claim["attempt"] + 1
        stale = (await (await execute(
            "SELECT {}.memory_promote_claimed_association(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": bot}), Jsonb(old_claim["payload"])),
        )).fetchone())["result"]
        assert stale["ok"] is False
        assert stale["error"]["code"] == "MEMORY_ASSOCIATION_CLAIM_LOST"
        current = (await (await execute(
            "SELECT {}.memory_promote_claimed_association(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": bot}), Jsonb(current_claim["payload"])),
        )).fetchone())["result"]
        assert current["ok"] and current["value"]["state"] == "PROMOTED"
        for label, trigger, seed_unit in (
            ("a", "trigger", "seed"), ("b", "seed", "trigger")
        ):
            await execute(
                "INSERT INTO {}.association_journal("
                "bot_id,journal_id,operation_id,input_digest,reply_ref,bundle_id,"
                "query_id,trigger_ref,trigger_source_unit_id,trigger_source_revision,"
                "trigger_part_id,seed_source_unit_id,seed_source_revision,seed_part_id,"
                "seed_rank_score) VALUES "
                "(%s,%s,%s,%s,%s,'bundle/test','query/test',%s,%s,1,'text',%s,1,'text',0.8)",
                (bot, "journal/concurrent/" + label, "reply/concurrent/" + label,
                 digest, "reply/concurrent/" + label, "trigger/concurrent/" + label,
                 trigger, seed_unit),
            )

        entered_a, entered_b, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def archive_like(trigger, label, entered):
            async with store.pool.connection() as archive_conn:
                async with archive_conn.transaction():
                    await archive_conn.execute(sql.SQL(
                        "SELECT 1 FROM {}.source_units "
                        "WHERE bot_id=%s AND source_unit_id=%s FOR UPDATE"
                    ).format(schema), (bot, trigger))
                    entered.set()
                    await release.wait()
                    await archive_conn.execute(sql.SQL(
                        "UPDATE {}.association_journal SET promotion_ready=true "
                        "WHERE bot_id=%s AND journal_id=%s"
                    ).format(schema), (bot, "journal/concurrent/" + label))

        archive_a = asyncio.create_task(archive_like("trigger", "a", entered_a))
        archive_b = asyncio.create_task(archive_like("seed", "b", entered_b))
        await asyncio.wait_for(asyncio.gather(entered_a.wait(), entered_b.wait()), 3)
        release.set()
        await asyncio.wait_for(asyncio.gather(archive_a, archive_b), 3)

        async def promote_concurrent(token):
            async with store.pool.connection() as worker_conn:
                async def run(function, value):
                    row = await (await worker_conn.execute(sql.SQL(
                        "SELECT {}." + function + "(%s::jsonb,%s::jsonb) AS result"
                    ).format(schema), (Jsonb({"bot_id": bot}), Jsonb(value)))).fetchone()
                    assert row["result"]["ok"], row["result"]
                    return row["result"]["value"]

                work = await run("memory_claim_association_promotion", {
                    "protocol": "forest.owned-work-claim-request/1",
                    "claim_token": token, "lease_seconds": 60,
                })
                assert work["work_id"].startswith("journal/concurrent/")
                result = await run("memory_promote_claimed_association", work["payload"])
                assert result["state"] == "PROMOTED"
                await run("memory_settle_association_promotion", {
                    "protocol": "forest.owned-work-settle-request/1",
                    "work_id": work["work_id"], "claim_token": token,
                    "attempt": work["attempt"],
                    "outcome": {"status": "SUCCEEDED", "result": result},
                })
                return work["work_id"]

        promoted_journals = await asyncio.wait_for(asyncio.gather(
            promote_concurrent("association/concurrent/a"),
            promote_concurrent("association/concurrent/b"),
        ), 5)
        assert set(promoted_journals) == {
            "journal/concurrent/a", "journal/concurrent/b",
        }
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

        relations = (await (await execute(
            "SELECT {}.memory_query_relations(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": bot}), Jsonb({
                "endpoint_kind": "SOURCE_UNIT", "endpoint_id": "trigger", "limit": 10,
            })),
        )).fetchone())["result"]
        assert relations["ok"], relations
        contribution = relations["value"]["items"][0]
        assert contribution["evidence_count"] == 2
        assert contribution["association_evidence_count"] == 5
        snapshot = relations["value"]["snapshot_xid"]
        journal_items = []
        after = None
        while True:
            request = {"relation_id": contribution["relation_id"],
                       "contribution_id": contribution["contribution_id"],
                       "snapshot_xid": snapshot, "limit": 2}
            if after is not None:
                request["after_journal_id"] = after
            result = (await (await execute(
                "SELECT {}.memory_query_association_evidence(%s::jsonb,%s::jsonb) AS result",
                (Jsonb({"bot_id": bot}), Jsonb(request)),
            )).fetchone())["result"]
            assert result["ok"], result
            page = result["value"]
            output_schema = json.loads((
                Path(__file__).parents[1]
                / "src/forest/builtin/memory_graph/schemas/query-association-evidence-output.json"
            ).read_text(encoding="utf-8"))
            Draft202012Validator(output_schema).validate(page)
            assert page["total_count"] == 5
            journal_items.extend(page["items"])
            if not page["has_more"]:
                break
            after = page["next_after"]
        assert len(journal_items) == 5
        assert [item["journal_id"] for item in journal_items] == sorted(
            item["journal_id"] for item in journal_items
        )
        assert {item["trigger_evidence"]["external_group_id"] for item in journal_items} == {
            "question", "memory",
        }
        stale_evidence = next(item for item in journal_items
                              if item["journal_id"] == "journal/stale")
        assert stale_evidence["trigger_evidence"]["part_id"] == "text-alt"
        assert stale_evidence["seed_evidence"]["part_id"] == "text-alt"
        assert stale_evidence["trigger_evidence"]["source_id"] == "question"
        assert stale_evidence["seed_evidence"]["source_id"] == "memory"

        await execute(
            "INSERT INTO {}.association_journal("
            "bot_id,journal_id,operation_id,input_digest,reply_ref,bundle_id,"
            "query_id,trigger_ref,trigger_source_unit_id,trigger_source_revision,"
            "trigger_part_id,seed_source_unit_id,seed_source_revision,seed_part_id,"
            "seed_rank_score,promotion_ready) VALUES "
            "(%s,'journal/late','reply/late',%s,'reply/late','bundle/test',"
            "'query/test','trigger/late','trigger',1,'text-alt','seed',1,'text',0.8,true)",
            (bot, digest),
        )
        late = await (await execute(
            "SELECT {}.memory_promote_association(%s,%s) AS promoted",
            (bot, "journal/late"),
        )).fetchone()
        assert late["promoted"] is True
        old_snapshot_page = (await (await execute(
            "SELECT {}.memory_query_association_evidence(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": bot}), Jsonb({
                "relation_id": contribution["relation_id"],
                "contribution_id": contribution["contribution_id"],
                "snapshot_xid": snapshot, "limit": 32,
            })),
        )).fetchone())["result"]
        assert old_snapshot_page["ok"] and old_snapshot_page["value"]["total_count"] == 5
        fresh_relations = (await (await execute(
            "SELECT {}.memory_query_relations(%s::jsonb,%s::jsonb) AS result",
            (Jsonb({"bot_id": bot}), Jsonb({
                "endpoint_kind": "SOURCE_UNIT", "endpoint_id": "trigger", "limit": 10,
            })),
        )).fetchone())["result"]
        assert fresh_relations["value"]["items"][0]["association_evidence_count"] == 6

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

    # Forget takes source locks before touching the pending journal. Promotion
    # must do the same; otherwise its evidence FK and the journal update form
    # an opposing lock order under concurrent transactions.
    async with store.pool.connection() as conn:
        await conn.execute(sql.SQL(
            "UPDATE {}.source_units SET eligibility_state='ELIGIBLE',"
            "eligibility_revision=eligibility_revision+1,eligible_at=clock_timestamp() "
            "WHERE bot_id=%s AND source_unit_id='later'"
        ).format(schema), (bot,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.association_journal("
            "bot_id,journal_id,operation_id,input_digest,reply_ref,bundle_id,"
            "query_id,trigger_ref,trigger_source_unit_id,trigger_source_revision,"
            "trigger_part_id,seed_source_unit_id,seed_source_revision,seed_part_id,"
            "seed_rank_score) VALUES "
            "(%s,'journal/race','reply/race',%s,'reply/race','bundle/test',"
            "'query/test','trigger/race','later',1,'text','seed',1,'text',0.8)"
        ).format(schema), (bot, digest))

    entered = asyncio.Event()
    promoter_pid = None

    async def promote():
        nonlocal promoter_pid
        async with store.pool.connection() as promotion_conn:
            async with promotion_conn.transaction():
                promoter_pid = (await (await promotion_conn.execute(
                    "SELECT pg_backend_pid() AS pid"
                )).fetchone())["pid"]
                await promotion_conn.execute("SET LOCAL lock_timeout='5s'")
                entered.set()
                result = await (await promotion_conn.execute(sql.SQL(
                    "SELECT {}.memory_promote_association(%s,%s) AS promoted"
                ).format(schema), (bot, "journal/race"))).fetchone()
                return result["promoted"]

    async with store.pool.connection() as forgetting_conn:
        async with forgetting_conn.transaction():
            blocker_pid = (await (await forgetting_conn.execute(
                "SELECT pg_backend_pid() AS pid"
            )).fetchone())["pid"]
            await forgetting_conn.execute(sql.SQL(
                "SELECT 1 FROM {}.source_units WHERE bot_id=%s "
                "AND source_unit_id='later' FOR UPDATE"
            ).format(schema), (bot,))
            pending = asyncio.create_task(promote())
            await entered.wait()
            async with asyncio.timeout(3):
                while True:
                    blockers = (await (await forgetting_conn.execute(
                        "SELECT pg_blocking_pids(%s) AS pids", (promoter_pid,)
                    )).fetchone())["pids"]
                    if blocker_pid in blockers:
                        break
                    await asyncio.sleep(0.02)
            # A promotion holding journal FOR UPDATE would fail this NOWAIT
            # probe, proving a lock-order cycle before a deadlock timeout.
            await forgetting_conn.execute(sql.SQL(
                "SELECT 1 FROM {}.association_journal WHERE bot_id=%s "
                "AND journal_id='journal/race' FOR UPDATE NOWAIT"
            ).format(schema), (bot,))
            forgotten = await (await forgetting_conn.execute(sql.SQL(
                "SELECT {}.memory_apply_forget_effects(%s,%s,%s,%s,%s,%s) AS value"
            ).format(schema), (
                bot, "forget/race", digest, ["payload/later"], 1, ["later"],
            ))).fetchone()
            assert forgotten["value"]["suppressed_source_units"] == 1
    assert await pending is False
    async with store.pool.connection() as conn:
        journal_state = (await (await conn.execute(sql.SQL(
            "SELECT state FROM {}.association_journal WHERE bot_id=%s "
            "AND journal_id='journal/race'"
        ).format(schema), (bot,))).fetchone())["state"]
        active_relation_count = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.relation_contributions "
            "WHERE bot_id=%s AND active AND (left_id='later' OR right_id='later')"
        ).format(schema), (bot,))).fetchone())["count"]
    assert journal_state == "SUPPRESSED"
    assert active_relation_count == 0

    # The opposite committed order keeps the old journal as history while
    # retracting the already promoted contribution in the forget transaction.
    async with store.pool.connection() as conn:
        retracted = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_apply_forget_effects(%s,%s,%s,%s,%s,%s) AS value"
        ).format(schema), (
            bot, "forget/promoted", digest, ["payload/trigger"], 2, ["trigger"],
        ))).fetchone())["value"]
        assert retracted["retracted_relations"] == 1
        active = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.relation_contributions "
            "WHERE bot_id=%s AND active"
        ).format(schema), (bot,))).fetchone())["count"]
        assert active == 0
        revoked = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_association_evidence(%s::jsonb,%s::jsonb) AS result"
        ).format(schema), (Jsonb({"bot_id": bot}), Jsonb({
            "relation_id": contribution["relation_id"],
            "contribution_id": contribution["contribution_id"],
            "snapshot_xid": snapshot, "limit": 16,
        })))).fetchone())["result"]
        assert revoked["error"]["code"] == "MEMORY_RELATION_NOT_VISIBLE"
