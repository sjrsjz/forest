"""Real c93b6a4 001-020 authorities, upgraded before any new source writer runs.

The old model rows exercise byte/fact preservation, not restartable Provider
requests. Legacy plaintext intentionally remains after this P4 source backfill.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from retention_upgrade import assert_preserved
from test_p3_jobs_upgrade import RELEASED, old_authority
from test_upgrade import upgrade_database as upgrade_database

from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres

# Obtained once from c93b6a4bad9ef685b8bdc6b58edebf706fb0a5df Git blobs.
# 001-017 were separately compared with that commit and equal RELEASED.
# Runtime tests need neither a Git executable nor a .git checkout.
P3_RELEASED = {
    **RELEASED,
    "018_jobs.sql": "b94dd80be998fc28d6695dce4b1786c070642ed2558134307872559315b6f17e",
    "019_children.sql": "7a141a8ec95271f8185671497e89e29c2414d52917e1046d856c7ac6c81b60b4",
    "020_shell_commands.sql": "ccf828ca8ea60d719c779dda7bc944003f8f1a453524985f1469330c9ab7f157",
}

OLD_TABLES = (
    "bot_conversations",
    "bot_message_inbox",
    "bot_inbound_media_refs",
    "bot_context_units",
    "bot_context_input_batches",
    "bot_context_compactions",
    "bot_conversation_sessions",
    "bot_conversation_runs",
    "bot_conversation_triggers",
    "bot_conversation_replies",
    "bot_calls",
    "bot_shell_requests",
    "bot_agent_runs",
    "bot_agent_steps",
    "bot_model_request_manifests",
    "bot_model_transport_attempts",
    "bot_model_budget_reservations",
    "bot_agent_budget_days",
    "bot_jobs",
    "bot_job_revisions",
    "bot_job_occurrences",
    "bot_web_issuers",
)
MEMORY_TABLES = (
    "bot_memory_clock",
    "bot_memory_sources",
    "bot_memory_source_units",
    "bot_memory_source_occurrences",
    "bot_memory_source_eligibility",
    "bot_memory_source_media",
    "bot_memory_backfill_heads",
    "bot_payload_headers",
    "bot_payload_sources",
    "bot_payload_derivations",
    "bot_payload_bodies",
)


async def snapshot(conn, tables):
    return {
        name: await (
            await conn.execute(
                sql.SQL("SELECT t.* FROM public.{} t ORDER BY to_jsonb(t)::text").format(
                    sql.Identifier(name)
                )
            )
        ).fetchall()
        for name in tables
    }


def expected_after_memory_permissions(before):
    """Memory migrations add only grants implied by each old capability."""
    issuers = []
    for issuer in before["bot_web_issuers"]:
        actions = set(issuer["actions"])
        mapped = {permission for permission, required in (
            ("memory.config.read", "config.read"), ("memory.config.plan", "config.plan"),
            ("memory.config.apply", "config.apply"), ("memory.read", "message.read"),
        ) if required in actions}
        actions |= mapped
        additions = {
            permission
            for permission, required in (
                ("memory.forget.read", {"memory.read"}),
                ("memory.forget.plan", {"memory.read", "memory.config.plan"}),
                ("memory.forget.apply", {"memory.read", "memory.config.apply"}),
                ("memory.association.read", {"memory.read"}),
                ("memory.association.plan", {"memory.read", "memory.config.plan"}),
                ("memory.association.apply", {"memory.read", "memory.config.apply"}),
            )
            if required <= actions
        }
        actions |= additions
        if "message.read" in actions:
            actions.add("bot.runtime.read")
        if {"config.apply", "message.cancel"} <= actions:
            actions.add("bot.maintenance")
        issuers.append({**issuer, "actions": sorted(actions)})
    return {**before, "bot_web_issuers": issuers}


async def p3_authority(dsn):
    """Write only old SQL tables; no modern ingest monkeypatch or source API."""
    await old_authority(dsn)
    migrations = files("forest.storage").joinpath("bot_migrations")
    now = datetime.now(UTC)
    author = {
        "external_user_id": "4",
        "display_name": "same display name",
        "principal_ref": "principal-4",
        "identity_origin": "adapter_verified",
        "is_self": False,
    }
    job_author = {
        "external_user_id": "job:old-job",
        "display_name": "Old reminder",
        "is_self": False,
        "identity_origin": "host_job",
        "principal_ref": "delegation:old-delegation",
        "job_id": "old-job",
        "occurrence_id": "old-occurrence",
        "delegation_id": "old-delegation",
        "tool_capabilities": [],
    }
    values = {
        "name": "Old reminder",
        "enabled": False,
        "kind": "conversation.trigger",
        "conversation_id": "job-conversation",
        "prompt": "cancelled-job-prompt-canary",
        "schedule": {
            "kind": "interval",
            "timezone": "Asia/Shanghai",
            "start_at": now.isoformat(),
            "end_at": None,
            "interval_seconds": 600,
            "cron": None,
            "dst_fold": "first",
        },
        "misfire": "coalesce",
        "misfire_grace_seconds": 60,
        "catch_up_limit": 3,
        "max_overlap": 1,
        "delegation_expires_at": (now + timedelta(days=1)).isoformat(),
        "max_runs": 1000,
    }
    delegation = {
        "delegation_id": "old-delegation",
        "subject_id": "old-admin",
        "principal_ref": job_author["principal_ref"],
        "issuer_id": "old-issuer",
        "key_revision": 1,
        "policy_revision": 1,
        "expires_at": values["delegation_expires_at"],
        "tool_capabilities": [],
        "schedule_algorithm": "forest-calendar-v1",
    }
    async with await psycopg.AsyncConnection.connect(
        dsn, autocommit=True, row_factory=dict_row
    ) as conn:
        async with conn.transaction():
            for name, checksum in P3_RELEASED.items():
                source = migrations.joinpath(name).read_text(encoding="utf-8")
                assert hashlib.sha256(source.encode()).hexdigest() == checksum
                if int(name[:3]) >= 18:
                    await conn.execute(source)
                    await conn.execute(
                        "INSERT INTO public.forest_bot_schema_migrations(version,checksum) "
                        "VALUES (%s,%s)",
                        (name, checksum),
                    )
            # The old authority has no payload/source tables at the point data
            # is written. Ordinary undispatched tail must stay ineligible.
            assert (
                await (
                    await conn.execute("SELECT to_regclass('public.bot_memory_sources') AS name")
                ).fetchone()
            )["name"] is None
            await conn.execute(
                "UPDATE public.bot_conversations SET next_ingress_seq=5,"
                "classified_through_seq=4,mailbox_revision=4 WHERE conversation_id='conversation'"
            )
            await conn.execute(
                "INSERT INTO public.bot_message_inbox(bot_id,conversation_id,event_id,ingress_seq,"
                "platform_message_id,source_event_id,input_digest,author,segments,sent_at,"
                "trigger_decision,estimated_tokens) VALUES ('bot-a','conversation','event-4',4,"
                "'4','source-event-4','old-tail',%s,%s,%s,'NO_TRIGGER',20)",
                (Jsonb(author), Jsonb([{"type": "text", "text": "undrained-tail-canary"}]), now),
            )
            # A cancelled job and a restricted Shell command were silently
            # drained in separate idle conversations by the old host.
            for name, origin, decision, disposition, event_author, parts in (
                (
                    "job",
                    "job",
                    "TRIGGER",
                    "READABLE",
                    job_author,
                    [{"type": "text", "text": values["prompt"]}],
                ),
                (
                    "shell",
                    "platform_input",
                    "CONTROL_COMMAND",
                    "RESTRICTED",
                    author,
                    [{"type": "text", "text": "[control command: job.cancel]"}],
                ),
            ):
                conversation = name + "-conversation"
                await conn.execute(
                    "INSERT INTO public.bot_conversations(bot_id,conversation_id,binding_id,platform,"
                    "channel_kind,channel_id,group_source_id,next_ingress_seq,classified_through_seq,"
                    "context_applied_seq,context_revision,mailbox_revision,processed_mailbox_revision) "
                    "VALUES ('bot-a',%s,'binding','onebot-v11','group',%s,%s,2,1,1,1,1,1)",
                    (conversation, name + "-group", name + "-trunk"),
                )
                await conn.execute(
                    "INSERT INTO public.bot_message_inbox(bot_id,conversation_id,event_id,ingress_seq,"
                    "platform_message_id,source_event_id,input_digest,author,segments,sent_at,"
                    "trigger_decision,estimated_tokens,origin,context_disposition) "
                    "VALUES ('bot-a',%s,%s,1,%s,%s,%s,%s,%s,%s,%s,20,%s,%s)",
                    (
                        conversation,
                        name + "-event",
                        name + "-message",
                        "source-" + name,
                        "old-" + name + "-digest",
                        Jsonb(event_author),
                        Jsonb(parts),
                        now,
                        decision,
                        origin,
                        disposition,
                    ),
                )
                await conn.execute(
                    "INSERT INTO public.bot_context_input_batches(bot_id,input_batch_id,operation_id,"
                    "conversation_id,kind,start_seq,end_seq,selected_input_end_seq,trigger_ids,context_revision) "
                    "VALUES ('bot-a',%s,%s,%s,'SILENT_DRAIN',1,1,1,'[]',1)",
                    (name + "-batch", name + "-drain", conversation),
                )
                await conn.execute(
                    "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,"
                    "unit_index,kind,event_id,input_batch_id,estimated_tokens) "
                    "VALUES ('bot-a',%s,%s,1,1,'INPUT',%s,%s,20)",
                    (conversation, name + "-event", name + "-event", name + "-batch"),
                )
            await conn.execute(
                "INSERT INTO public.bot_conversation_triggers(bot_id,trigger_id,conversation_id,event_id,"
                "ingress_seq,principal_ref,status,terminal_reason) VALUES ('bot-a','job-trigger',"
                "'job-conversation','job-event',1,'delegation:old-delegation','CANCELLED','JOB_CANCELLED')"
            )
            await conn.execute(
                "INSERT INTO public.bot_jobs(bot_id,job_id,revision,definition_revision,conversation_id,"
                "enabled,delegation_epoch,emitted_count) VALUES ('bot-a','old-job',2,1,'job-conversation',false,2,1)"
            )
            await conn.execute(
                "INSERT INTO public.bot_job_revisions(bot_id,job_id,revision,values,delegation,definition_digest,"
                "operation_id) VALUES ('bot-a','old-job',1,%s,%s,%s,'old-job-apply')",
                (Jsonb(values), Jsonb(delegation), digest_json([values, delegation])),
            )
            await conn.execute(
                "INSERT INTO public.bot_job_occurrences(bot_id,occurrence_id,job_id,job_revision,"
                "delegation_epoch,origin,scheduled_for,expires_at,status,event_id,trigger_id,"
                "attempt_count,error_code) VALUES ('bot-a','old-occurrence','old-job',1,1,'MANUAL',"
                "%s,%s,'CANCELLED','job-event','job-trigger',1,'JOB_CANCELLED')",
                (now, now + timedelta(days=1)),
            )
            await conn.execute(
                "INSERT INTO public.bot_shell_requests(bot_id,command_id,event_id,conversation_id,binding_id,"
                "command_name,raw_digest,gate_version,arguments,priority,status,outcome) "
                "VALUES ('bot-a','typed-shell','shell-event','shell-conversation','binding','job.cancel',"
                "'old-typed-shell-digest','shell-gate-v2',%s,0,'COMPLETED',%s)",
                (
                    Jsonb(
                        {
                            "job_id": "old-job",
                            "expected_revision": 1,
                            "scope": "pending",
                            "reason": "old-private-shell-reason",
                        }
                    ),
                    Jsonb({"code": "WEB_AUTHORITY_REJECTED"}),
                ),
            )
        return await snapshot(conn, OLD_TABLES)


async def connect_sources(dsn):
    store = BotStore(dsn, "bot-a")
    await store.open()
    await store.migrate()
    lease = await store.acquire_host("upgrade-host", "upgrade-" + uuid4().hex, ttl_seconds=120)
    async with store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_bindings SET host_incarnation=%s,host_owner_epoch=%s "
            "WHERE bot_id='bot-a' AND binding_id='binding'",
            (lease.incarnation, lease.owner_epoch),
        )
    return store, lease, MemorySourceStore(MessageStore(store))


async def test_actual_p3_upgrade_preserves_nonempty_history_and_exact_wire(upgrade_database):
    before = await p3_authority(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    try:
        await store.open()
        await store.migrate()
        await store.migrate()
        async with store.pool.connection() as conn:
            await assert_preserved(conn, expected_after_memory_permissions(before))
            ledger = await (
                await conn.execute(
                    "SELECT version,checksum FROM public.forest_bot_schema_migrations ORDER BY version"
                )
            ).fetchall()
            assert {
                r["version"]: r["checksum"] for r in ledger if r["version"] < "021"
            } == P3_RELEASED
            assert {r["version"] for r in ledger if "021" <= r["version"] < "023"} == {
                "021_payloads.sql",
                "022_memory_sources.sql",
            }
            migrated = (await snapshot(conn, MEMORY_TABLES))["bot_memory_source_units"]
            assert len(migrated) == 6 and all(row["kind"] == "INPUT" for row in migrated)
        assert [u["unit_id"] for u in before["bot_context_units"] if not u["visible"]] == [
            "event-1"
        ]
        reply = before["bot_conversation_replies"][0]
        assert reply["parts"] == [[{"type": "text", "text": "old answer"}]]
        assert reply["delivery"] == {"status": "DELIVERED", "intent_id": "old-intent"}
        attempts = {r["transport_attempt_id"]: r for r in before["bot_model_transport_attempts"]}
        assert len(attempts) == 5 and all(r["request_body"] == b"{}" for r in attempts.values())
        assert attempts["uncertain"]["status"] == "UNKNOWN"
        assert attempts["unsent"]["status"] == "PERMITTED"
        shells = {r["command_id"]: r for r in before["bot_shell_requests"]}
        assert (shells["legacy-shell"]["arguments"], shells["legacy-shell"]["priority"]) == ({}, 1)
        assert shells["legacy-shell"]["gate_version"] == "shell-gate-v1"
        assert shells["typed-shell"]["arguments"]["reason"] == "old-private-shell-reason"
        assert before["bot_job_occurrences"][0]["event_id"] == "job-event"
    finally:
        await store.close()


@pytest.mark.parametrize("batch_limit", [1, 2])
async def test_old_sources_backfill_restart_qualification_and_original_provenance(
    upgrade_database, batch_limit
):
    before = await p3_authority(upgrade_database)
    store, lease, sources = await connect_sources(upgrade_database)
    try:
        first = await sources.backfill(lease, limit=batch_limit)
        assert first["scanned"] == batch_limit
        assert first["processed"] == (1 if batch_limit == 2 else 0)
        assert first["more_possible"]
        async with store.pool.connection() as conn:
            partial = await snapshot(conn, MEMORY_TABLES)
        await store.release_host(lease)
        await store.close()
        store, lease, sources = await connect_sources(upgrade_database)
        async with store.pool.connection() as conn:
            assert await snapshot(conn, MEMORY_TABLES) == partial
        processed, scanned = first["processed"], first["scanned"]
        for _ in range(12):
            result = await sources.backfill(lease, limit=batch_limit)
            assert 0 <= result["processed"] <= result["scanned"] <= batch_limit
            processed += result["processed"]
            scanned += result["scanned"]
            if not result["more_possible"]:
                break
        else:
            pytest.fail("Bounded backfill did not finish its persistent cursor")
        # 029 registered all six original inputs before retiring their old
        # bodies. The resumable cursor only adds the remaining legacy reply.
        assert processed == 1 and scanned == 7
        async with store.pool.connection() as conn:
            final = await snapshot(conn, MEMORY_TABLES)
            await assert_preserved(conn, expected_after_memory_permissions(before))
        units = {r["source_event_id"]: r for r in final["bot_memory_source_units"]}
        assert set(units) == {
            "source-event-1",
            "source-event-2",
            "source-event-3",
            "source-event-4",
            "source-job",
            "source-shell",
            "source-reply:old-reply",
        }
        assert all(r["order_origin"] == "RECONSTRUCTED" for r in units.values())
        assert all(r["source_revision"] == 1 for r in units.values())
        roots = {r["source_id"]: r for r in final["bot_memory_sources"]}
        for root in roots.values():
            positions = sorted(
                r["source_order_seq"] for r in units.values() if r["source_id"] == root["source_id"]
            )
            assert positions == list(range(1, root["next_order_seq"]))
            assert root["has_reconstructed_order"] and root["identity_schema"] == 1
        assert (roots["trunk-a"]["channel_id"], roots["trunk-a"]["realm_id"]) == (
            "group-a",
            "qq-global",
        )
        occurrences = {r["occurrence_id"]: r for r in final["bot_memory_source_occurrences"]}
        eligible = {
            occurrences[r["occurrence_id"]]["source_unit_id"]: r
            for r in final["bot_memory_source_eligibility"]
        }
        assert len(eligible) == 6
        assert eligible[units["source-event-1"]["source_unit_id"]]["input_batch_id"] == "old-batch"
        assert units["source-event-4"]["source_unit_id"] not in eligible
        reply = units["source-reply:old-reply"]
        assert not reply["lineage_complete"] and reply["identity_origin"] == "host_output"
        retrieved = (await sources.read(lease, "conversation", input_cut=3))["sources"]
        assert {r["source_event_id"] for r in retrieved} == {
            "source-event-1",
            "source-event-2",
            "source-event-3",
        }
        by_event = {r["source_event_id"]: r for r in retrieved}
        assert by_event["source-event-1"]["author"]["principal_ref"] == "principal-1"
        assert by_event["source-event-2"]["author"]["principal_ref"] == "principal-2"
        assert (
            by_event["source-event-1"]["author"]["display_name"]
            == by_event["source-event-2"]["author"]["display_name"]
        )
        assert by_event["source-event-3"]["parts"][0]["type"] == "image"
        assert by_event["source-event-3"]["parts"][0]["media_ref"].startswith("source-media:")
        diagnostic = await sources.payloads.read_json(
            lease, reply["payload_id"], expected_deletion_epoch=0
        )
        assert diagnostic["author"]["external_user_id"] == "1000"
        assert diagnostic["parts"] == [{"type": "text", "text": "old answer", "part_id": "0"}]
        links = final["bot_memory_source_media"]
        assert (
            len(links) == 1 and links[0]["media_ref"] == "old-media" and links[0]["part_id"] == "0"
        )
        job_root = roots[units["source-job"]["source_id"]]
        assert (job_root["source_kind"], job_root["visibility"], job_root["platform"]) == (
            "SYSTEM",
            "source_restricted",
            "forest",
        )
        assert job_root["owner_conversation_id"] == "job-conversation"
        assert job_root["target_group_source_id"] == "job-trunk"
        for conversation in ("conversation", "job-conversation", "shell-conversation"):
            result = await sources.read(
                lease, conversation, input_cut=3 if conversation == "conversation" else 1
            )
            assert not {"source-job", "source-shell", "source-event-4"}.intersection(
                r["source_event_id"] for r in result["sources"]
            )
            assert "old-signed-token" not in str(result)
            assert "old-private-shell-reason" not in str(result)
            assert "cancelled-job-prompt-canary" not in str(result)
        # Another actual pool/host incarnation must see identical stable IDs,
        # source positions, payload bodies, eligibility, and completed cursors.
        await store.release_host(lease)
        await store.close()
        store, lease, sources = await connect_sources(upgrade_database)
        assert await sources.backfill(lease, limit=1) == {
            "processed": 0,
            "scanned": 0,
            "more_possible": False,
        }
        async with store.pool.connection() as conn:
            assert await snapshot(conn, MEMORY_TABLES) == final
            await assert_preserved(conn, expected_after_memory_permissions(before))
    finally:
        await store.close()
