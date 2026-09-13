"""Nonempty released 001-022 data, then the actual current migration chain.

Old rows are seeded using only tables present in 394c7cb. The pending Agent
request is a valid frozen protocol request, not a placeholder JSON document.
"""

from __future__ import annotations

import base64
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from legacy_022 import DeliveryStore as LegacyDeliveryStore
from legacy_022 import MemorySourceStore as LegacyMemorySourceStore
from legacy_022 import MessageStore as LegacyMessageStore
from psycopg.types.json import Jsonb
from retention_upgrade import assert_preserved, content
from test_agent_config import apply as apply_agent
from test_agent_config import plan as plan_agent
from test_messages import event, owner
from test_models import completion, respond, server
from test_p4_upgrade import MEMORY_TABLES, P3_RELEASED, snapshot
from test_upgrade import upgrade_database as upgrade_database

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.errors import ForestError
from forest.kernel import Kernel
from forest.models import ModelCapabilities, prepare_model_request
from forest.packages import digest_json
from forest.platform import BindingRuntime
from forest.security import SecretVault
from forest.storage.agent_config import AgentConfig
from forest.storage.bot import BotStore, _migration_checksum
from forest.storage.control import BotControl
from forest.storage.memory_config import MemoryConfig
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres

# Frozen from 394c7cb Git blobs, not recomputed from current files at test time.
RELEASED = {
    **P3_RELEASED,
    "021_payloads.sql": "b56edd570454d2b65d6d1710dd124dbaa65cf85091ec3e864c19b1ca6cdd1116",
    "022_memory_sources.sql": "ae57fb41c4d2a01d3d744c6705acc118045e903e6d644adb5cdde346881c3f86",
}
OLD_ACTIONS = [
    "config.read",
    "config.plan",
    "config.apply",
    "message.read",
    "service.invoke",
    "agent.config.read",
    "agent.config.plan",
    "agent.config.apply",
    "agent.read",
]
OLD_TABLES = (
    *MEMORY_TABLES,
    "bot_message_inbox",
    "bot_context_units",
    "bot_context_input_batches",
    "bot_conversation_sessions",
    "bot_conversation_runs",
    "bot_conversation_triggers",
    "bot_conversation_replies",
    "bot_send_intents",
    "bot_agent_runs",
    "bot_agent_steps",
    "bot_model_request_manifests",
    "bot_model_transport_attempts",
    "bot_agent_budget_days",
    "bot_model_budget_reservations",
    "bot_agent_config_heads",
    "bot_agent_config_revisions",
    "bot_agent_config_plans",
    "bot_web_operations",
)
API_KEY = "preserve-this-old-encrypted-model-key"


class OldEnvironment(SimpleNamespace):
    async def ticket(self, action, operation, payload, *, issuer="old-full", **changes):
        now = datetime.now(UTC)
        claims = {
            "ticket_id": str(uuid4()),
            "issuer_id": issuer,
            "subject_id": "old-admin",
            "session_ref": "old-session",
            "bot_id": "bot-a",
            "operation_id": operation,
            "action": action,
            "input_digest": digest_json(payload),
            "plan_digest": "",
            "issued_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=25)).isoformat(),
            "key_revision": 1,
            "account_revision": 1,
            "session_revision": 1,
            "policy_revision": 1,
            **changes,
        }
        return {
            "claims": claims,
            "signature": base64.b64encode(self.private.sign(digest_json(claims).encode())).decode(),
        }


async def old_schema(dsn):
    async with (
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn,
        conn.transaction(),
    ):
        await conn.execute(
            "CREATE TABLE public.forest_bot_schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
        )
        for name, checksum in RELEASED.items():
            source = (
                files("forest.storage").joinpath("bot_migrations", name).read_text(encoding="utf8")
            )
            assert hashlib.sha256(source.encode()).hexdigest() == checksum
            await conn.execute(source)
            await conn.execute(
                "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                (name, checksum),
            )
        await conn.execute("INSERT INTO public.forest_bot_identity(bot_id) VALUES ('bot-a')")
        await conn.execute("INSERT INTO public.bot_runtime_state(bot_id) VALUES ('bot-a')")
        await conn.execute("INSERT INTO public.bot_host_lease(bot_id) VALUES ('bot-a')")


async def seed_agent(env, actor, batch, *, completed=False):
    """Insert valid old checkpoint columns without calling new memory guards."""
    context = await env.messages.context(actor, run_id=batch["run_id"])
    run_id, step_id = batch["run_id"], "old-model-step:" + batch["run_id"]
    units = [
        {
            "unit_id": item["unit_id"],
            "role": item["context_role"],
            "source": item["source"],
            "author": item["author"],
            "parts": item["segments"],
        }
        for item in context["units"]
    ]
    value = {
        "bot_id": "bot-a",
        "run_id": run_id,
        "logical_step_id": step_id,
        "input_revision": batch["input_revision"],
        "context_revision": context["context_revision"],
        "context_view_revision": context["context_view_revision"],
        "selected_input_end_seq": batch["end_seq"],
        "trigger_ids": batch["trigger_ids"],
        "triggers": context["triggers"],
        "input_batch_ids": [batch["input_batch_id"]],
        "conversation": context["conversation"],
        "units": units,
        "required_unit_ids": [item["unit_id"] for item in units],
    }
    prepared = prepare_model_request(
        value,
        ModelCapabilities("old-deployment", "old-model", "old-capability"),
        request_id="old-request:" + run_id,
        transport_attempt_id="old-attempt:" + run_id,
        output_tokens=128,
        stream=False,
    )
    spec = {"config_revision": 1, "secret_revision": 1, "values": env.values, "tools": []}
    reply = None
    if completed:
        reply = await env.messages.finalize_reply(
            actor,
            "old-final",
            run_id=run_id,
            expected_input_revision=batch["input_revision"],
            reply_parts=[[{"type": "text", "text": "Already completed before upgrade"}]],
            delivery=LegacyDeliveryStore(env.store),
        )
    async with env.store.pool.connection() as conn, conn.transaction():
        session = await (
            await conn.execute(
                "SELECT * FROM public.bot_conversation_sessions WHERE session_id=%s",
                (batch["session_id"],),
            )
        ).fetchone()
        await conn.execute(
            "INSERT INTO public.bot_agent_runs(bot_id,run_id,conversation_id,session_id,session_epoch,spec,spec_digest,actor_epoch,"
            "actor_host_incarnation,phase,input_revision,selected_input_end_seq,deadline_at,current_step_id,next_step_seq) "
            "VALUES ('bot-a',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+interval '1 hour',%s,2)",
            (
                run_id,
                actor.conversation_id,
                batch["session_id"],
                session["session_epoch"],
                Jsonb(spec),
                digest_json(spec),
                actor.actor_epoch,
                actor.host.incarnation,
                "COMPLETED" if completed else "RETRY_MODEL",
                batch["input_revision"],
                batch["end_seq"],
                step_id,
            ),
        )
        await conn.execute(
            "INSERT INTO public.bot_agent_steps(bot_id,run_id,logical_step_id,step_seq,input_revision,selected_input_end_seq,"
            "snapshot,snapshot_digest,status,model_call_epoch,model_request_id,transport_attempt_id) VALUES ('bot-a',%s,%s,1,%s,%s,%s,%s,%s,1,%s,%s)",
            (
                run_id,
                step_id,
                batch["input_revision"],
                batch["end_seq"],
                Jsonb(value),
                digest_json(value),
                "FINAL_COMMITTED" if completed else "RETRY_MODEL",
                prepared.request_id,
                prepared.transport_attempt_id,
            ),
        )
        if completed:
            await conn.execute(
                "UPDATE public.bot_agent_runs SET final_step_id=%s,final_input_revision=%s,final_reply_id=%s,published_step_seq=1 WHERE run_id=%s",
                (step_id, batch["input_revision"], reply["reply_id"], run_id),
            )
        await conn.execute(
            "INSERT INTO public.bot_model_request_manifests(bot_id,model_request_id,logical_step_id,manifest,manifest_digest) VALUES ('bot-a',%s,%s,%s,%s)",
            (
                prepared.request_id,
                step_id,
                Jsonb(prepared.manifest),
                digest_json(prepared.manifest),
            ),
        )
        metadata = {
            "estimated_input_tokens": prepared.estimated_input_tokens,
            "max_output_tokens": prepared.max_output_tokens,
            "stream": prepared.stream,
            "serializer_revision": prepared.serializer_revision,
        }
        await conn.execute(
            "INSERT INTO public.bot_model_transport_attempts(bot_id,transport_attempt_id,operation_id,model_request_id,run_id,logical_step_id,"
            "model_call_epoch,owner_epoch,actor_epoch,host_incarnation,wire_digest,request_body,prepared_metadata,status,deadline_at) "
            "VALUES ('bot-a',%s,%s,%s,%s,%s,1,1,%s,%s,%s,%s,%s,%s,clock_timestamp()+interval '1 hour')",
            (
                prepared.transport_attempt_id,
                "old-permit:" + run_id,
                prepared.request_id,
                run_id,
                step_id,
                actor.actor_epoch,
                actor.host.incarnation,
                prepared.payload_digest,
                prepared.body,
                Jsonb(metadata),
                "RESPONDED" if completed else "NOT_SENT",
            ),
        )
        await conn.execute(
            "INSERT INTO public.bot_agent_budget_days(bot_id,budget_day,charged_tokens) VALUES ('bot-a',CURRENT_DATE,%s) "
            "ON CONFLICT(bot_id,budget_day) DO UPDATE SET charged_tokens=bot_agent_budget_days.charged_tokens+EXCLUDED.charged_tokens",
            (15 if completed else 0,),
        )
        await conn.execute(
            "INSERT INTO public.bot_model_budget_reservations(bot_id,transport_attempt_id,budget_day,reserved_tokens,charged_tokens,status) "
            "VALUES ('bot-a',%s,CURRENT_DATE,%s,%s,'SETTLED')",
            (prepared.transport_attempt_id, prepared.reserved_tokens, 15 if completed else 0),
        )
    await env.messages.release_actor(actor)
    return SimpleNamespace(run_id=run_id, actor=actor, batch=batch, prepared=prepared, reply=reply)


@asynccontextmanager
async def old_bot(dsn, path, *, endpoint="http://127.0.0.1:8000/v1"):
    await old_schema(dsn)
    store = BotStore(dsn, "bot-a")
    await store.open()
    env = OldEnvironment(
        store=store, vault=SecretVault(path / "vault"), private=Ed25519PrivateKey.generate()
    )
    try:
        env.lease = await store.acquire_host("old-host", "old-incarnation", ttl_seconds=300)
        async with store.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_web_issuers(bot_id,issuer_id,key_revision,public_key,actions) VALUES ('bot-a','old-full',1,%s,%s)",
                (
                    base64.b64encode(env.private.public_key().public_bytes_raw()).decode(),
                    Jsonb(OLD_ACTIONS),
                ),
            )
            await conn.execute(
                "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,host_incarnation,host_owner_epoch,enabled,receive_enabled,state) "
                "VALUES ('bot-a','binding-a','onebot-v11','1000','old bot','old-binding',%s,%s,true,true,'ONLINE')",
                (env.lease.incarnation, env.lease.owner_epoch),
            )
        env.values = {
            **DEFAULT_AGENT_SETTINGS,
            "enabled": True,
            "model": "old-model",
            "endpoint": endpoint,
            "stream": False,
            "max_output_tokens": 128,
            "model_timeout": 15,
        }
        await apply_agent(env, await plan_agent(env, values=env.values, secret=API_KEY))
        env.messages = LegacyMessageStore(store)
        env.sources = LegacyMemorySourceStore(env.messages)
        original = event(
            1, group="source-group", user="photographer", text="old native picture caption"
        )
        original["segments"].append(
            {
                "type": "image",
                "media_ref": {
                    "kind": "onebot-source",
                    "uri": "https://old.invalid/image?original-token=private",
                },
            }
        )
        env.source = await env.messages.ingest(
            env.lease, "binding-a", original, decision="NO_TRIGGER"
        )
        actor = await owner(env.messages, env.lease, env.source["conversation_id"])
        await env.messages.select_input(actor, "old-silent-drain")
        await env.messages.release_actor(actor)
        for number, name in ((2, "completed"), (3, "retry")):
            receipt = await env.messages.ingest(
                env.lease, "binding-a", event(number, group=name + "-group"), decision="TRIGGER"
            )
            actor = await owner(env.messages, env.lease, receipt["conversation_id"])
            batch = await env.messages.select_input(actor, "old-select:" + name)
            setattr(env, name, await seed_agent(env, actor, batch, completed=name == "completed"))
        async with store.pool.connection() as conn:
            assert (
                await (
                    await conn.execute(
                        "SELECT to_regclass('public.bot_memory_config_heads') AS name"
                    )
                ).fetchone()
            )["name"] is None
            env.before = await snapshot(conn, OLD_TABLES)
            env.ledger = await snapshot(conn, ("forest_bot_schema_migrations",))
        yield env
    finally:
        await store.close()


async def upgrade(env):
    await env.store.release_host(env.lease)
    await env.store.migrate()
    env.lease = await env.store.acquire_host("upgraded-host", "upgraded-" + uuid4().hex, ttl_seconds=300)
    async with env.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_bindings SET host_incarnation=%s,host_owner_epoch=%s WHERE bot_id='bot-a'",
            (env.lease.incarnation, env.lease.owner_epoch),
        )
    env.messages = MessageStore(env.store)
    env.sources = MemorySourceStore(env.messages)


async def test_nonempty_022_upgrade_preserves_payloads_eligibility_config_and_exact_requests(
    upgrade_database, tmp_path
):
    async with old_bot(upgrade_database, tmp_path) as env:
        for name in (
            "bot_memory_source_units",
            "bot_memory_source_eligibility",
            "bot_payload_bodies",
            "bot_agent_runs",
            "bot_model_transport_attempts",
        ):
            assert env.before[name]
        async with env.store.pool.connection() as conn:
            issuer = (await snapshot(conn, ("bot_web_issuers",)))["bot_web_issuers"][0]
        await upgrade(env)
        await env.store.migrate()
        async with env.store.pool.connection() as conn:
            await assert_preserved(conn, env.before, append_only=MEMORY_TABLES)
            all_ledger = (await snapshot(conn, ("forest_bot_schema_migrations",)))[
                "forest_bot_schema_migrations"
            ]
            assert [r for r in all_ledger if r["version"] < "023"] == env.ledger[
                "forest_bot_schema_migrations"
            ]
            assert {
                r["version"]: r["checksum"] for r in all_ledger if r["version"] < "023"
            } == RELEASED
            assert {int(r["version"][:3]) for r in all_ledger}.issuperset(range(1, 29))
            for row in all_ledger:
                source = (
                    files("forest.storage")
                    .joinpath("bot_migrations", row["version"])
                    .read_text(encoding="utf8")
                )
                assert row["checksum"] == _migration_checksum(row["version"], source)
            after_issuer = (await snapshot(conn, ("bot_web_issuers",)))["bot_web_issuers"][0]
            assert {k: v for k, v in after_issuer.items() if k != "actions"} == {
                k: v for k, v in issuer.items() if k != "actions"
            }
            assert set(after_issuer["actions"]) == set(OLD_ACTIONS) | {
                "memory.config.read",
                "memory.config.plan",
                "memory.config.apply",
                "memory.read",
            }
            for table in (
                "bot_memory_config_revisions",
                "bot_memory_embedding_requests",
                "bot_memory_queries",
                "bot_agent_memory_bindings",
            ):
                assert not (await snapshot(conn, (table,)))[table]
        saved = await AgentConfig(env.store, env.vault).runtime(env.lease)
        assert env.vault.decrypt(saved["secret_ciphertext"]) == API_KEY
        assert saved["values"] == env.values and saved["revision"] == saved["secret_revision"] == 1
        public = await AgentConfig(env.store, env.vault).read(
            await env.ticket("agent.config.read", "old-config-read", {}), "old-config-read"
        )
        assert public["has_api_key"] and public["secret_revision"] == 1
        assert API_KEY not in json.dumps(public) and saved["secret_ciphertext"] not in json.dumps(
            public
        )
        view = await env.sources.read(env.lease, env.source["conversation_id"], input_cut=1)
        source = next(
            item for item in view["sources"] if item["source"]["channel_id"] == "source-group"
        )
        assert source["author"]["external_user_id"] == "photographer"
        assert [part["type"] for part in source["parts"]] == ["text", "image"]
        assert source["parts"][1]["media_ref"].startswith("source-media:")
        assert "original-token" not in json.dumps(source["parts"])
        assert {row["status"] for row in env.before["bot_model_transport_attempts"]} == {
            "RESPONDED",
            "NOT_SENT",
        }


async def test_upgrade_maps_each_memory_action_without_elevating_restricted_issuers(
    upgrade_database, tmp_path
):
    scopes = {
        "readonly": (["config.read", "message.read"], True),
        "config-only": (["config.read"], True),
        "plan-only": (["config.plan"], True),
        "apply-only": (["config.apply"], True),
        "invoke-only": (["service.invoke"], True),
        "disabled": (OLD_ACTIONS, False),
    }
    mapping = {
        "memory.config.read": "config.read",
        "memory.config.plan": "config.plan",
        "memory.config.apply": "config.apply",
        "memory.read": "message.read",
    }
    async with old_bot(upgrade_database, tmp_path) as env:
        public = base64.b64encode(env.private.public_key().public_bytes_raw()).decode()
        async with env.store.pool.connection() as conn:
            for issuer, (actions, enabled) in scopes.items():
                await conn.execute(
                    "INSERT INTO public.bot_web_issuers(bot_id,issuer_id,key_revision,public_key,actions,enabled) VALUES ('bot-a',%s,1,%s,%s,%s)",
                    (issuer, public, Jsonb(actions), enabled),
                )
            before = await snapshot(conn, ("bot_web_issuers",))
        await upgrade(env)
        control = BotControl(env.store)
        async with env.store.pool.connection() as conn:
            actual = {
                row["issuer_id"]: row
                for row in (await snapshot(conn, ("bot_web_issuers",)))["bot_web_issuers"]
            }
        for row in before["bot_web_issuers"]:
            expected = set(row["actions"]) | {
                action for action, old in mapping.items() if old in row["actions"]
            }
            after = actual[row["issuer_id"]]
            assert set(after["actions"]) == expected
            assert {k: v for k, v in after.items() if k != "actions"} == {
                k: v for k, v in row.items() if k != "actions"
            }
            for action in mapping:
                operation = row["issuer_id"] + ":" + action
                ticket = await env.ticket(action, operation, {}, issuer=row["issuer_id"])
                async with env.store.pool.connection() as conn, conn.transaction():
                    if action in expected and row["enabled"]:
                        assert (await control._guard(conn, ticket, action, operation, {}))[
                            "issuer_id"
                        ] == row["issuer_id"]
                    else:
                        with pytest.raises(ForestError) as denied:
                            await control._guard(conn, ticket, action, operation, {})
                        assert denied.value.code == "WEB_AUTHORITY_REJECTED"
        read = await MemoryConfig(env.store, env.vault).read(
            await env.ticket("memory.config.read", "read-default", {}, issuer="readonly"),
            "read-default",
        )
        assert read["revision"] == 0 and not read["values"]["enabled"] and not read["has_api_key"]
        assert "memory.read" not in actual["config-only"]["actions"]
        assert not {"memory.config.plan", "memory.config.apply"}.intersection(
            actual["readonly"]["actions"]
        )
        await env.store.migrate()
        async with env.store.pool.connection() as conn:
            assert {
                row["issuer_id"]: row
                for row in (await snapshot(conn, ("bot_web_issuers",)))["bot_web_issuers"]
            } == actual


async def test_legacy_exact_retry_and_next_run_complete_with_memory_unconfigured(
    upgrade_database, tmp_path
):
    async def handler(writer, request):
        assert request["headers"]["Authorization"] == "Bearer " + API_KEY
        await respond(writer, json.dumps(completion("Completed after upgrade")).encode())

    async with (
        server(handler) as (endpoint, requests),
        old_bot(upgrade_database, tmp_path, endpoint=endpoint) as env,
    ):
        await upgrade(env)
        kernel = Kernel(env.store)
        kernel.lease = env.lease
        runtime = BindingRuntime(SimpleNamespace(store=env.store, kernel=kernel), env.vault, {})
        try:
            actor = await owner(
                runtime.messages, env.lease, env.retry.actor.conversation_id, "upgrade-recovery"
            )
            selected = await runtime.messages.select_input(actor, "resume-old")
            view = await runtime.messages.context(actor, run_id=selected["run_id"])
            assert selected["run_id"] == env.retry.run_id and selected["status"] == "RESUME"
            disposition = await runtime.agent.runner.advance(actor, selected, view)
            assert disposition.status == "FINAL_READY"
            reply = await runtime.agent.runner.state.commit_final(
                actor, selected["run_id"], "finish-old", runtime.delivery
            )
            assert reply["parts"] == [[{"type": "text", "text": "Completed after upgrade"}]]
            assert len(requests) == 1 and requests[0]["body"] == env.retry.prepared.body
            async with env.store.pool.connection() as conn:
                attempts = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_model_transport_attempts WHERE run_id=%s ORDER BY created_at",
                        (env.retry.run_id,),
                    )
                ).fetchall()
                assert len(attempts) == 2 and [row["status"] for row in attempts] == [
                    "NOT_SENT",
                    "RESPONDED",
                ]
                assert all(row["request_body"] is None for row in attempts)
                for row in attempts:
                    assert await content(conn, "bot-a", row["request_payload_id"]) == env.retry.prepared.body
                assert all(
                    row["wire_digest"] == env.retry.prepared.payload_digest for row in attempts
                )
                assert all(
                    row["model_request_id"] == env.retry.prepared.request_id for row in attempts
                )
                assert len({row["transport_attempt_id"] for row in attempts}) == 2
                assert not (await snapshot(conn, ("bot_agent_memory_bindings",)))[
                    "bot_agent_memory_bindings"
                ]
            await runtime.messages.release_actor(actor)

            # A fresh model boundary invokes the new retrieval hook, which must
            # choose DISABLED without contacting embedding/media providers.
            new = await runtime.messages.ingest(
                env.lease,
                "binding-a",
                event(4, group="retry-group", text="new request after upgrade"),
                decision="TRIGGER",
            )
            actor = await owner(
                runtime.messages, env.lease, new["conversation_id"], "upgrade-next-run"
            )
            selected = await runtime.messages.select_input(actor, "select-new")
            view = await runtime.messages.context(actor, run_id=selected["run_id"])
            assert (
                await runtime.agent.runner.advance(actor, selected, view)
            ).status == "FINAL_READY"
            await runtime.agent.runner.state.commit_final(
                actor, selected["run_id"], "finish-new", runtime.delivery
            )
            assert len(requests) == 2 and b"new request after upgrade" in requests[1]["body"]
            async with env.store.pool.connection() as conn:
                bindings = (await snapshot(conn, ("bot_agent_memory_bindings",)))[
                    "bot_agent_memory_bindings"
                ]
                assert (
                    len(bindings) == 1
                    and bindings[0]["status"] == "DISABLED"
                    and bindings[0]["config_revision"] == 0
                )
                assert bindings[0]["query_id"] is None
                for table in (
                    "bot_memory_queries",
                    "bot_memory_embedding_requests",
                    "bot_memory_config_revisions",
                ):
                    assert not (await snapshot(conn, (table,)))[table]
            await runtime.messages.release_actor(actor)
        finally:
            await runtime.close()
            await kernel.close()


async def test_old_checksum_mismatch_rejects_upgrade_before_any_new_memory_table(
    upgrade_database, tmp_path
):
    async with old_bot(upgrade_database, tmp_path) as env:
        async with env.store.pool.connection() as conn:
            await conn.execute(
                "UPDATE public.forest_bot_schema_migrations SET checksum='corrupted-old-release' WHERE version='022_memory_sources.sql'"
            )
        with pytest.raises(ForestError) as rejected:
            await env.store.migrate()
        assert rejected.value.code == "MIGRATION_CHANGED"
        async with env.store.pool.connection() as conn:
            assert (
                await (
                    await conn.execute(
                        "SELECT to_regclass('public.bot_memory_config_heads') AS name"
                    )
                ).fetchone()
            )["name"] is None
            assert await snapshot(conn, OLD_TABLES) == env.before
            await conn.execute(
                "UPDATE public.forest_bot_schema_migrations SET checksum=%s WHERE version='022_memory_sources.sql'",
                (RELEASED["022_memory_sources.sql"],),
            )
        await upgrade(env)
        async with env.store.pool.connection() as conn:
            await assert_preserved(conn, env.before, append_only=MEMORY_TABLES)
