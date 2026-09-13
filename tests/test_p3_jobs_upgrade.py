"""Nonempty 001-017 authorities upgraded to Jobs, child accounts and typed Shell."""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_p3_upgrade import attach, p2_authority
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent import AgentStore
from forest.storage.bot import BotStore
from forest.storage.jobs import JobStore
from forest.storage.messages import MessageStore
from forest.storage.tree_budget import account_in, reserve_in

pytestmark = pytest.mark.postgres

# Frozen from the released 001-017 Git blobs, not recomputed from current SQL.
# Old checksums are part of the upgrade contract even for a non-Git installation.
RELEASED = {
    "001_kernel.sql": "6e86ed2eacd57270f0a33132b22ecf7f13beaa8f2c6427c75ede3fc3fdf32462",
    "002_call_trees.sql": "6d081922bf8401f8427c3ca8edfda78ce9f8117558298b36519a1055e971d8f2",
    "003_control.sql": "0976ea5d195c7a975cd59982f51f6b54a33ad240b5f0123dfaf776186ab8bfaa",
    "004_bindings.sql": "b20db9600481732058e7cc94ab3439dbc1cdff19559ad671ba0d84771b323755",
    "005_messages.sql": "8e1fc4ffa3a7465f44e48fd529e15ea9b897483c101a7c1e63bde9531a2ac5ea",
    "006_delivery.sql": "fdc18526052d4b5451f2a056d0fc8f3b76e0e93492f194e160ca14c7b646b336",
    "007_shell_gate.sql": "205bde2d7bd6fe9660686b219fce12c326889990a4eacbc296151139de2d4a87",
    "008_control_calls.sql": "38c9321167c6b660b60a9f199178aaa16e795ba3228ea1c23e207957d20fb519",
    "009_echo_aliases.sql": "e779484a920ea59e4337c4d8fbe71e8cd31a36533477dde16ee7dda223d22c5e",
    "010_state_migration.sql": "9fc6db1f106ea166feac3940e729653cf18f33828f7d98d059c0fa53b0c6a7c8",
    "011_watch.sql": "d00903c548c06a1da00f8eb8213eea7c9dc221e91b759b522c4af28028362cf4",
    "012_web_config_recovery.sql": "c0afcbaf9423ad6fc0fdadef6f806644f9fb952283b1b2f2a3597ab376c91427",
    "013_agent.sql": "10feeff0d1846bb04171260d07de47e2f908910a7d5c25dae35baca8b7258148",
    "014_agent_config.sql": "6b63c0150a3057f2dd7bea8f008ec49241e10f7e8cadfc211db8e0647a24b8ac",
    "015_agent_tool_fences.sql": "c0ffca0e42a3f7dabb1a36d7e4af5be2a108892b2f8c1852f6aa2b465b98bc92",
    "016_media.sql": "09150be91839b8e47c0118b35089f8b54a0c6da7c8479a2e6ff366706521b790",
    "017_agent_management_grants.sql": "0bfbc52102ec5fdf74031f73154912cec8a497774aee99136b147b46aad2e8a9",
}


async def old_authority(dsn):
    private, public = await p2_authority(dsn)
    migrations = files("forest.storage").joinpath("bot_migrations")
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        async with conn.transaction():
            for name, checksum in RELEASED.items():
                source = migrations.joinpath(name).read_text(encoding="utf-8")
                assert hashlib.sha256(source.encode()).hexdigest() == checksum
                if int(name[:3]) > 12:
                    await conn.execute(source)
                    await conn.execute("INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                                       (name, checksum))
            for run_id in ("old-run", "open-run"):
                await conn.execute(
                    "INSERT INTO public.bot_agent_runs(bot_id,run_id,conversation_id,session_id,session_epoch,spec,spec_digest,"
                    "actor_epoch,actor_host_incarnation,phase,input_revision,selected_input_end_seq,deadline_at) "
                    "VALUES ('bot-a',%s,'conversation',%s,1,'{}',%s,1,'old-host',%s,1,3,clock_timestamp()+interval '1 hour')",
                    (
                        run_id,
                        run_id.replace("run", "session"),
                        digest_json({}),
                        "COMPLETED" if run_id == "old-run" else "MODEL_PENDING",
                    ),
                )
            today = datetime.now(UTC).date()
            yesterday = today - timedelta(days=1)
            for day, reserved, charged in ((today, 3000, 105), (yesterday, 0, 90)):
                await conn.execute("INSERT INTO public.bot_agent_budget_days(bot_id,budget_day,reserved_tokens,charged_tokens) VALUES ('bot-a',%s,%s,%s)",
                                   (day, reserved, charged))
            attempts = (
                ("open-run", "past-paid", "SETTLED", 700, 90, yesterday),
                ("open-run", "paid", "SETTLED", 500, 75, today),
                ("open-run", "unsent", "RESERVED", 1000, None, today),
                ("open-run", "uncertain", "UNKNOWN", 2000, None, today),
                ("old-run", "closed-paid", "SETTLED", 300, 30, today),
            )
            for index, (run, attempt, status, reserved, charged, day) in enumerate(attempts, 1):
                step, request = "step:" + attempt, "request:" + attempt
                await conn.execute(
                    "INSERT INTO public.bot_agent_steps(bot_id,run_id,logical_step_id,step_seq,input_revision,selected_input_end_seq,snapshot,snapshot_digest) "
                    "VALUES ('bot-a',%s,%s,%s,1,3,'{}','old-snapshot')", (run, step, index),
                )
                await conn.execute("INSERT INTO public.bot_model_request_manifests(bot_id,model_request_id,logical_step_id,manifest,manifest_digest) VALUES ('bot-a',%s,%s,'{}','old-manifest')",
                                   (request, step))
                await conn.execute(
                    "INSERT INTO public.bot_model_transport_attempts(bot_id,transport_attempt_id,operation_id,model_request_id,run_id,logical_step_id,"
                    "model_call_epoch,owner_epoch,actor_epoch,host_incarnation,wire_digest,request_body,prepared_metadata,status,deadline_at) "
                    "VALUES ('bot-a',%s,%s,%s,%s,%s,1,1,1,'old-host','old-wire',%s,'{}',%s,clock_timestamp()+interval '1 hour')",
                    (attempt, "operation:" + attempt, request, run, step, b"{}", "UNKNOWN" if status == "UNKNOWN" else "RESPONDED" if status == "SETTLED" else "PERMITTED"),
                )
                await conn.execute("INSERT INTO public.bot_model_budget_reservations(bot_id,transport_attempt_id,budget_day,reserved_tokens,charged_tokens,status) VALUES ('bot-a',%s,%s,%s,%s,%s)",
                                   (attempt, day, reserved, charged, status))
            await conn.execute(
                "INSERT INTO public.bot_shell_requests(bot_id,command_id,event_id,conversation_id,binding_id,command_name,raw_digest) "
                "VALUES ('bot-a','legacy-shell','event-1','conversation','binding','unsupported','original-source-digest')"
            )
    return private, public


@pytest.mark.parametrize("account_first", [True, False])
async def test_upgrade_backfills_unsettled_and_paid_cost_once_across_days(upgrade_database, account_first):
    await old_authority(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    try:
        await attach(store)
        agent = AgentStore(MessageStore(store))
        async with store.pool.connection() as conn, conn.transaction():
            assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_agent_tree_budgets")).fetchone())["n"] == 0
            if account_first:
                row = await account_in(conn, "bot-a", "open-run")
                assert (row["reserved_tokens"], row["charged_tokens"]) == (3000, 165)
                with pytest.raises(ForestError, match="exhausted") as caught:
                    async with conn.transaction():
                        await reserve_in(conn, "bot-a", "open-run", 1, 3165)
                assert caught.value.code == "RUN_TOKEN_BUDGET_EXCEEDED"
            # Settlement itself must lazily seed the account before decrementing
            # an old reservation. Re-observation cannot charge or refund twice.
            await agent._settle(conn, "unsent", status="NOT_SENT", usage=None)
            await agent._settle(conn, "unsent", status="NOT_SENT", usage=None)
            row = await account_in(conn, "bot-a", "open-run")
            assert (row["reserved_tokens"], row["charged_tokens"]) == (2000, 165)
            await agent._settle(conn, "uncertain", status="UNKNOWN", usage=None)
            assert (await account_in(conn, "bot-a", "open-run"))["reserved_tokens"] == 2000
            await agent._settle(conn, "uncertain", status="RESPONDED", usage={"total_tokens": 120})
            await agent._settle(conn, "uncertain", status="RESPONDED", usage={"total_tokens": 120})
            row = await account_in(conn, "bot-a", "open-run")
            assert (row["reserved_tokens"], row["charged_tokens"]) == (0, 285)
            closed = await account_in(conn, "bot-a", "old-run")
            assert (closed["reserved_tokens"], closed["charged_tokens"]) == (0, 30)
            days = await (await conn.execute("SELECT reserved_tokens,charged_tokens FROM public.bot_agent_budget_days ORDER BY budget_day")).fetchall()
            assert days == [{"reserved_tokens": 0, "charged_tokens": 90}, {"reserved_tokens": 0, "charged_tokens": 225}]
        await store.migrate()
        async with store.pool.connection() as conn, conn.transaction():
            assert (await account_in(conn, "bot-a", "open-run"))["charged_tokens"] == 285
            ledger = await (await conn.execute("SELECT version,checksum FROM public.forest_bot_schema_migrations WHERE version<'018' ORDER BY version")).fetchall()
            assert {row["version"]: row["checksum"] for row in ledger} == RELEASED
    finally:
        await store.close()


async def test_upgrade_keeps_legacy_shell_source_arguments_and_priority(upgrade_database):
    await old_authority(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    try:
        lease, messages, _ = await attach(store)
        claim = await messages.claim_control(lease, "legacy-shell", "upgrade-control", ttl_seconds=30)
        assert claim["command_name"] == "unsupported"
        assert claim["arguments"] == {} and claim["priority"] == 1
        assert claim["gate_version"] == "shell-gate-v1"
        assert claim["raw_digest"] == "original-source-digest"
        assert claim["event_id"] == "event-1" and claim["conversation_id"] == "conversation"
        async with store.pool.connection() as conn:
            for update in ("arguments='{\"job_id\":\"injected\"}'", "priority=0", "gate_version='shell-gate-v2'", "command_name='job.run'"):
                with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                    await conn.execute("UPDATE public.bot_shell_requests SET " + update + " WHERE command_id='legacy-shell'")
        await store.migrate()
    finally:
        await store.close()


@pytest.mark.parametrize("scope", ["full", "readonly", "config_readonly", "empty", "disabled", "invoke_only"])
async def test_upgrade_job_grants_preserve_existing_issuer_restrictions(upgrade_database, scope):
    private, public = await old_authority(upgrade_database)
    selected = {
        "readonly": ["message.read"], "config_readonly": ["config.read"], "empty": [],
        "invoke_only": ["service.invoke"],
    }
    async with await psycopg.AsyncConnection.connect(upgrade_database, autocommit=True, row_factory=dict_row) as conn:
        before = await (await conn.execute("SELECT * FROM public.bot_web_issuers WHERE issuer_id='old-issuer'")).fetchone()
        actions = selected.get(scope, before["actions"])
        await conn.execute("UPDATE public.bot_web_issuers SET actions=%s,enabled=%s WHERE issuer_id='old-issuer'",
                           (Jsonb(actions), scope != "disabled"))
    store = BotStore(upgrade_database, "bot-a")
    try:
        await attach(store)
        async with store.pool.connection() as conn:
            after = await (await conn.execute("SELECT * FROM public.bot_web_issuers WHERE issuer_id='old-issuer'")).fetchone()
        expected = set(actions)
        for permission, required in (("job.read", "message.read"), ("job.plan", "config.plan"),
                                     ("job.apply", "config.apply"), ("job.run", "service.invoke"),
                                     ("job.cancel", "message.cancel"), ("job.replay", "service.invoke"),
                                     # migrate() targets the current schema, including 023.
                                     ("memory.config.read", "config.read"), ("memory.config.plan", "config.plan"),
                                     ("memory.config.apply", "config.apply"), ("memory.read", "message.read")):
            if required in actions:
                expected.add(permission)
        if "message.read" in actions:
            expected.add("bot.runtime.read")
        if {"config.apply", "message.cancel"} <= set(actions):
            expected.add("bot.maintenance")
        if "memory.read" in expected:
            expected.update({"memory.forget.read", "memory.association.read"})
        if {"memory.read", "memory.config.plan"} <= expected:
            expected.update({"memory.forget.plan", "memory.association.plan"})
        if {"memory.read", "memory.config.apply"} <= expected:
            expected.update({"memory.forget.apply", "memory.association.apply"})
        if {"config.read", "message.read"}.intersection(actions):
            expected.add("event.read")
            if "service.invoke" in actions:
                expected.add("event.replay")
            if "config.plan" in actions:
                expected.add("event.retire.plan")
            if {"config.apply", "message.cancel"} <= set(actions):
                expected.add("event.retire.apply")
            expected.add("component_job.read")
            if "service.invoke" in actions:
                expected.add("component_job.replay")
            if "config.plan" in actions:
                expected.add("component_job.retire.plan")
            if {"config.apply", "message.cancel"} <= set(actions):
                expected.add("component_job.retire.apply")
        assert set(after["actions"]) == expected
        if scope in {"readonly", "config_readonly", "empty", "invoke_only"}:
            assert not {"memory.config.plan", "memory.config.apply"}.intersection(after["actions"])
        if "message.read" not in actions:
            assert "memory.read" not in after["actions"]
        assert after["enabled"] is (scope != "disabled")
        assert after["key_revision"] == before["key_revision"] and after["public_key"] == public
        now = datetime.now(UTC)
        claims = {"ticket_id": str(uuid4()), "issuer_id": "old-issuer", "subject_id": "old-admin", "session_ref": "session",
                  "bot_id": "bot-a", "operation_id": "upgrade-job-read", "action": "job.read", "input_digest": digest_json({}),
                  "plan_digest": "", "issued_at": now.isoformat(), "expires_at": (now + timedelta(seconds=20)).isoformat(),
                  "key_revision": 1, "account_revision": 1, "session_revision": 1, "policy_revision": 1}
        ticket = {"claims": claims, "signature": base64.b64encode(private.sign(digest_json(claims).encode("ascii"))).decode()}
        jobs = JobStore(MessageStore(store))
        if "job.read" in expected and scope != "disabled":
            result = await jobs.read(ticket, "upgrade-job-read")
            assert result["jobs"] == [] and result["occurrences"] == []
        else:
            with pytest.raises(ForestError) as caught:
                await jobs.read(ticket, "upgrade-job-read")
            assert caught.value.code == "WEB_AUTHORITY_REJECTED"
        await store.migrate()
    finally:
        await store.close()
