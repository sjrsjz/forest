"""Real released bodies keep narrow local recovery, never global legacy scope."""

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_agent_store import agent_turn
from test_memory_pipeline_upgrade import LegacyDeliveryStore, old_bot, seed_agent, upgrade
from test_messages import event, owner
from test_models import completion, respond, server
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.memory_types import SourceRef
from forest.packages import PackageCatalog
from forest.platform import BindingRuntime
from forest.storage import payload_history
from forest.storage.agent import AgentStore
from forest.storage.payload_scope import guard_payload_scope_in
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


async def row(env, query, args=()):
    async with env.store.pool.connection() as conn:
        return await (await conn.execute(query, args)).fetchone()


async def check(env, actor, ids, *, run_id=None, cut=None):
    async with env.store.pool.connection() as conn, conn.transaction():
        binding, conversation = await env.messages._actor(conn, actor)
        await guard_payload_scope_in(
            conn, env.store, conversation,
            conversation["context_applied_seq"] if cut is None else cut,
            ids, legacy_run_id=run_id,
        )
        await env.messages._actor_final(conn, actor, binding)


async def published(env):
    return (await row(env,
        "SELECT payload_id FROM public.bot_conversation_replies WHERE reply_id=%s",
        (env.completed.reply["reply_id"],)))["payload_id"]


async def test_published_local_history_and_each_mixed_cache_branch_have_independent_proofs(
    upgrade_database, tmp_path,
):
    async with old_bot(upgrade_database, tmp_path) as env:
        private = event(80, group="old-private", text="unpublished private model input")
        private["channel_kind"] = "private"
        received = await env.messages.ingest(env.lease, "binding-a", private, decision="TRIGGER")
        actor = await owner(env.messages, env.lease, received["conversation_id"])
        batch = await env.messages.select_input(actor, "private-old-input")
        private_run = await seed_agent(env, actor, batch)
        await upgrade(env)
        local = await published(env)
        hidden = (await row(env,
            "SELECT snapshot_payload_id FROM public.bot_agent_steps WHERE run_id=%s",
            (private_run.run_id,)))["snapshot_payload_id"]
        actor = await owner(env.messages, env.lease, env.completed.actor.conversation_id, "local-history")
        await check(env, actor, [local])
        original = await row(env,
            "SELECT * FROM public.bot_memory_source_units WHERE source_unit_id="
            "(SELECT source_unit_id FROM public.bot_memory_source_occurrences WHERE reply_id=%s)",
            (env.completed.reply["reply_id"],))
        assert original and not original["lineage_complete"]
        # A same-Bot shared group is still not a license for incomplete history.
        other = await owner(env.messages, env.lease, env.retry.actor.conversation_id, "other-history")
        with pytest.raises(ForestError, match="unavailable") as rejected:
            await check(env, other, [local])
        assert rejected.value.code == "PAYLOAD_SCOPE_DENIED"
        kernel = Kernel(env.store)
        kernel.lease = env.lease
        try:
            package = PackageCatalog(tmp_path / "packages").build(EXAMPLE)
            await kernel.install(package, grants=frozenset({"state.read", "state.write"}))
            await kernel.invoke_with_receipt(
                "example.counter", "counter.increment", {"increment": 3},
                operation_id="old-private-cache", input_parent_ids=(hidden,),
            )
            receipt = await kernel.invoke_with_receipt(
                "example.counter", "counter.increment", {"increment": 1},
                operation_id="local-and-private-cache", input_parent_ids=(local,),
            )
            assert receipt.value == {"value": 4, "revision": 2}
            with pytest.raises(ForestError) as rejected:
                await check(env, actor, [receipt.result_payload_id], run_id=private_run.run_id)
            assert rejected.value.code == "PAYLOAD_SCOPE_DENIED"
            assert (await row(env,
                "SELECT status FROM public.bot_calls WHERE operation_id='local-and-private-cache'"))["status"] == "COMPLETED"
            assert not await row(env, "SELECT 1 FROM public.bot_memory_config_heads")
        finally:
            await kernel.close()
        assert original == await row(env,
            "SELECT * FROM public.bot_memory_source_units WHERE source_unit_id=%s", (original["source_unit_id"],))


async def test_old_run_requires_exact_migrated_owner_current_cut_and_unknown_leaf_proof(
    upgrade_database, tmp_path,
):
    async with old_bot(upgrade_database, tmp_path) as env:
        await upgrade(env)
        actor = await owner(env.messages, env.lease, env.retry.actor.conversation_id, "old-proof")
        saved = await row(env, "SELECT snapshot_payload_id FROM public.bot_agent_steps WHERE run_id=%s", (env.retry.run_id,))
        await check(env, actor, [saved["snapshot_payload_id"]], run_id=env.retry.run_id)
        accepted = []
        for run_id, cut in ((None, 1), (env.completed.run_id, 1), (env.retry.run_id, 0)):
            try:
                await check(env, actor, [saved["snapshot_payload_id"]], run_id=run_id, cut=cut)
            except ForestError as rejected:
                assert rejected.code == "PAYLOAD_SCOPE_DENIED"
            else:
                accepted.append({"run_id": run_id, "input_cut": cut})
        assert not accepted, "Unproved legacy snapshot scope was admitted: " + json.dumps(accepted)
        payloads = PayloadStore(env.store)
        forged = await payloads.put_json(
            env.lease, owner_kind="model.snapshot", owner_id="legacy-agent:" + "0" * 64,
            value="unpublished body with an owner-looking string",
            sources=(SourceRef(payload_history.legacy_marker(env.store.bot_id)),),
            expected_deletion_epoch=0,
        )
        mixed = await payloads.put_json(
            env.lease, owner_kind="kernel.result", owner_id=uuid4().hex, value="mixed",
            parents=(forged.payload_id, saved["snapshot_payload_id"]), expected_deletion_epoch=0,
        )
        with pytest.raises(ForestError) as rejected:
            await check(env, actor, [mixed.payload_id], run_id=env.retry.run_id)
        assert rejected.value.code == "PAYLOAD_SCOPE_DENIED"


async def test_local_history_dag_is_bounded_and_forget_revokes_old_retry_without_new_attempt(
    upgrade_database, tmp_path, monkeypatch,
):
    async with old_bot(upgrade_database, tmp_path) as env:
        await upgrade(env)
        actor = await owner(env.messages, env.lease, env.completed.actor.conversation_id, "bounded-history")
        local = await published(env)
        payloads = PayloadStore(env.store)
        parent = local
        for index in range(5):
            ref = await payloads.put_json(
                env.lease, owner_kind="kernel.result", owner_id="chain:" + str(index),
                value={"index": index}, parents=(parent,), expected_deletion_epoch=0,
            )
            parent = ref.payload_id
        await check(env, actor, [parent])
        with monkeypatch.context() as context:
            context.setattr(payload_history, "MAX_NODES", 3)
            with pytest.raises(ForestError):
                await check(env, actor, [parent])
        with monkeypatch.context() as context:
            context.setattr(payload_history, "MAX_EDGES", 3)
            with pytest.raises(ForestError):
                await check(env, actor, [parent])
        await check(env, actor, [parent])
        retry = await owner(env.messages, env.lease, env.retry.actor.conversation_id, "forget-old-retry")
        state = AgentStore(env.messages)
        await env.messages.select_input(retry, "recover-before-forget")
        await state.ensure_run(retry, env.retry.run_id, {
            "config_revision": 1, "secret_revision": 1, "values": env.values, "tools": [],
        })
        assert (await state.replay_prepared(retry, env.retry.run_id)).body == env.retry.prepared.body
        with monkeypatch.context() as context:
            context.setattr(payload_history, "MAX_METADATA_BYTES", 1)
            with pytest.raises(ForestError) as rejected:
                await state.replay_prepared(retry, env.retry.run_id)
            assert rejected.value.code == "AGENT_MEMORY_REVOKED"
        before = await row(env,
            "SELECT transport_attempt_id,wire_digest,status,request_payload_id FROM public.bot_model_transport_attempts WHERE run_id=%s",
            (env.retry.run_id,))
        source = await row(env,
            "SELECT o.source_unit_id FROM public.bot_message_inbox i "
            "JOIN public.bot_memory_source_occurrences o USING(bot_id,event_id) WHERE i.conversation_id=%s",
            (env.retry.actor.conversation_id,))
        await payloads.forget_sources(env.lease, "forget-real-old-input", (SourceRef(source["source_unit_id"]),),
            expected_deletion_epoch=0)
        with pytest.raises(ForestError) as rejected:
            await state.replay_prepared(retry, env.retry.run_id)
        assert rejected.value.code == "AGENT_MEMORY_REVOKED"
        assert before == await row(env,
            "SELECT transport_attempt_id,wire_digest,status,request_payload_id FROM public.bot_model_transport_attempts WHERE run_id=%s",
            (env.retry.run_id,))
        assert (await row(env, "SELECT count(*) AS n FROM public.bot_model_transport_attempts WHERE run_id=%s", (env.retry.run_id,)))["n"] == 1
        with pytest.raises(ForestError):
            await check(env, actor, [parent])


async def test_old_snapshot_rechecks_actual_input_readability_after_admission(upgrade_database, tmp_path):
    async with old_bot(upgrade_database, tmp_path) as env:
        await upgrade(env)
        actor = await owner(env.messages, env.lease, env.retry.actor.conversation_id, "old-readable")
        saved = await row(env, "SELECT snapshot_payload_id FROM public.bot_agent_steps WHERE run_id=%s", (env.retry.run_id,))
        await check(env, actor, [saved["snapshot_payload_id"]], run_id=env.retry.run_id)
        async with env.store.pool.connection() as conn, conn.transaction():
            await env.store._host(conn, env.lease)
            await conn.execute(
                "UPDATE public.bot_message_inbox SET context_disposition='RESTRICTED' WHERE bot_id=%s AND conversation_id=%s",
                (env.store.bot_id, actor.conversation_id),
            )
        with pytest.raises(ForestError) as rejected:
            await check(env, actor, [saved["snapshot_payload_id"]], run_id=env.retry.run_id)
        assert rejected.value.code == "PAYLOAD_SCOPE_DENIED"
        local = await published(env)
        reader = await owner(env.messages, env.lease, env.completed.actor.conversation_id, "old-published-readable")
        await check(env, reader, [local])
        async with env.store.pool.connection() as conn, conn.transaction():
            await env.store._host(conn, env.lease)
            await conn.execute(
                "UPDATE public.bot_message_inbox SET context_disposition='RESTRICTED' WHERE bot_id=%s AND conversation_id=%s",
                (env.store.bot_id, reader.conversation_id),
            )
        with pytest.raises(ForestError) as rejected:
            await check(env, reader, [local])
        assert rejected.value.code == "PAYLOAD_SCOPE_DENIED"


async def test_two_published_legacy_replies_continue_after_the_first_view_is_clipped(upgrade_database, tmp_path):
    async def handler(writer, request):
        await respond(
            writer,
            json.dumps(completion(agent_turn("new answer with old local history"))).encode(),
        )

    async with server(handler) as (endpoint, requests), old_bot(upgrade_database, tmp_path, endpoint=endpoint) as env:
        second = await env.messages.ingest(env.lease, "binding-a",
            event(81, group="completed-group", text="second old question"), decision="TRIGGER")
        actor = await owner(env.messages, env.lease, second["conversation_id"], "second-old-run")
        selected = await env.messages.select_input(actor, "second-old-cut")
        reply = await env.messages.finalize_reply(actor, "second-old-reply", run_id=selected["run_id"],
            expected_input_revision=selected["input_revision"],
            reply_parts=[[{"type": "text", "text": "second old reply"}]], delivery=LegacyDeliveryStore(env.store))
        await env.messages.release_actor(actor)
        async with env.store.pool.connection() as conn:
            await conn.execute("UPDATE public.bot_context_units SET visible=false WHERE reply_id=%s",
                (env.completed.reply["reply_id"],))
        await upgrade(env)
        latest = (await row(env, "SELECT payload_id FROM public.bot_conversation_replies WHERE reply_id=%s",
            (reply["reply_id"],)))["payload_id"]
        assert await row(env,
            "SELECT 1 FROM public.bot_payload_sources p JOIN public.bot_memory_source_units u USING(bot_id,payload_id) "
            "JOIN public.bot_memory_source_occurrences o ON o.bot_id=u.bot_id AND o.source_unit_id=u.source_unit_id "
            "WHERE o.reply_id=%s AND p.source_unit_id=(SELECT source_unit_id FROM public.bot_memory_source_occurrences WHERE reply_id=%s)",
            (reply["reply_id"], env.completed.reply["reply_id"]))
        actor = await owner(env.messages, env.lease, second["conversation_id"], "read-clipped-ancestor")
        await check(env, actor, [latest])
        await env.messages.release_actor(actor)
        kernel = Kernel(env.store)
        kernel.lease = env.lease
        runtime = BindingRuntime(SimpleNamespace(store=env.store, kernel=kernel), env.vault, {})
        try:
            received = await runtime.messages.ingest(env.lease, "binding-a",
                event(82, group="completed-group", text="continue after clipped legacy history"), decision="TRIGGER")
            actor = await owner(runtime.messages, env.lease, received["conversation_id"], "current-after-history")
            selected = await runtime.messages.select_input(actor, "current-cut")
            view = await runtime.messages.context(actor, run_id=selected["run_id"])
            assert env.completed.reply["reply_id"] not in {u["reply_id"] for u in view["units"]}
            agent = runtime.module("agent.runtime")
            assert (await agent.runner.advance(actor, selected, view)).status == "FINAL_READY"
            await agent.runner.state.commit_final(
                actor, selected["run_id"], "current-final", runtime.delivery
            )
            assert len(requests) == 1 and b"second old reply" in requests[0]["body"]
            assert not await row(env, "SELECT 1 FROM public.bot_memory_config_heads")
        finally:
            await runtime.close()
            await kernel.close()
