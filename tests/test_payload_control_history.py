"""Real 038 control configuration keeps its narrow Host scope after cutover."""

from dataclasses import replace
from types import SimpleNamespace

import psycopg
import pytest
import pytest_asyncio
from legacy_control import FrozenBotControl
from psycopg.types.json import Jsonb
from test_control_results_upgrade import schema_038
from test_kernel import GRANTS, package
from test_memory_sources import drain
from test_messages import event, owner
from test_upgrade import upgrade_database as upgrade_database
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.control import BotControl
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore
from forest.storage.payload_control_history import old_config_leaves_in
from forest.storage.payload_history import legacy_marker
from forest.storage.payload_scope import guard_payload_scope_in
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def configured(environment, upgrade_database, tmp_path, request):
    env = environment
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    kernel = None
    try:
        await schema_038(store)
        old = FrozenBotControl(store)
        lease = await store.acquire_host("old-host", "old-incarnation", ttl_seconds=120)
        await old.trust(lease, issuer_id=env.authority.issuer_id,
            key_revision=env.authority.key_revision, public_key=env.authority.public_key)
        legacy = replace(env, store=store, control=old, lease=lease)
        snapshot = package(tmp_path / "package")
        component = "example.counter"
        values = {"package_digest": snapshot.digest, "config": {}, "grants": sorted(GRANTS)}
        payload = {"component_id": component, "expected_revision": 0, "values": values}
        plan = await old.plan(await legacy.ticket("config.plan", "old-plan", payload), "old-plan", payload)
        apply = {"component_id": component, **{k: plan[k] for k in ("plan_id", "plan_digest", "expected_revision")}}
        await old.apply(await legacy.ticket("config.apply", "old-apply", apply), "old-apply", apply)
        revision = 2 if getattr(request, "param", False) else 1
        # Also cover a supported historical plan with no surviving operation
        # receipt. It is imported by the frozen 041 fallback, never synthesized
        # as a newly-authorized independent source after cutover.
        async with store.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_config_plans(bot_id,plan_id,component_id,subject_id,issuer_id,"
                "expected_revision,values,plan_digest,expires_at) "
                "VALUES (%s,'old-untracked','old-component',%s,%s,0,%s,'old-untracked-digest','2030-01-01Z')",
                (store.bot_id, env.subject, env.authority.issuer_id, Jsonb(values)),
            )
            if revision == 2:
                # Legacy SQL allowed a revision to reference a non-apply
                # operation. 041 explicitly treats this as untracked; do not
                # fabricate an apply receipt or a new independent source.
                for number in (2, 3):
                    await conn.execute(
                        "INSERT INTO public.bot_config_revisions(bot_id,component_id,revision,previous_revision,"
                        "values,policy_revision,plan_id,operation_id) VALUES (%s,%s,%s,%s,%s,1,%s,'old-plan')",
                        (store.bot_id, component, number, number - 1, Jsonb(values), plan["plan_id"]),
                    )
                await conn.execute("UPDATE public.bot_config_heads SET desired_revision=2 "
                    "WHERE bot_id=%s AND component_id=%s", (store.bot_id, component))
        await store.release_host(lease)
        await store.migrate()
        kernel = Kernel(store)
        await kernel.start()
        controls = BotControl(store)
        await controls.bind_activation(kernel.lease, component, revision, "recover-old-config")
        await kernel.install(snapshot, config={}, grants=GRANTS, operation_id="recover-old-config")
        receipt = await kernel.invoke_with_receipt(component, "counter.increment", {"increment": 1},
            operation_id="actual-worker")
        async with store.pool.connection() as conn:
            await conn.execute(
                "INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,"
                "host_incarnation,host_owner_epoch,enabled,receive_enabled,state) "
                "VALUES ('bot-a','binding-a','onebot-v11','1000','test','registry',%s,%s,true,true,'ONLINE')",
                (kernel.lease.incarnation, kernel.lease.owner_epoch),
            )
        messages = MessageStore(store)
        received = await messages.ingest(kernel.lease, "binding-a", event(1), decision="TRIGGER")
        actor = await owner(messages, kernel.lease, received["conversation_id"])
        selected = await messages.select_input(actor, "select")
        yield SimpleNamespace(store=store, kernel=kernel, lease=kernel.lease, messages=messages,
            actor=actor, selected=selected, receipt=receipt,
            env=replace(env, store=store, control=controls, lease=kernel.lease), values=values)
    finally:
        if kernel is not None:
            await kernel.close()
        await store.close()


async def row(env, query, args=()):
    async with env.store.pool.connection() as conn:
        return await (await conn.execute(query, args)).fetchone()


async def check(env, refs):
    async with env.store.pool.connection() as conn, conn.transaction():
        binding, conversation = await env.messages._actor(conn, env.actor)
        await guard_payload_scope_in(conn, env.store, conversation,
            env.selected["end_seq"], refs, legacy_run_id=env.selected["run_id"])
        await env.messages._actor_final(conn, env.actor, binding)


async def leaves(env, ids):
    async with env.store.pool.connection() as conn, conn.transaction():
        await env.store._host(conn, env.lease)
        await PayloadStore(env.store).lock_in(conn)
        return await old_config_leaves_in(conn, env.store.bot_id, ids)


async def test_real_old_web_config_new_worker_and_reply_preserve_scope_then_forget(configured):
    env = configured
    assert env.receipt.value == {"revision": 1, "value": 1}
    saved = await row(env, "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
        (env.receipt.result_payload_id,))
    assert any(ref["source_unit_id"] == legacy_marker(env.store.bot_id) for ref in saved["manifest"]["sources"])
    assert not await row(env, "SELECT 1 FROM public.bot_memory_config_heads")
    arguments = dict(run_id=env.selected["run_id"], expected_input_revision=env.selected["input_revision"],
        reply_parts=[[{"type": "text", "text": "counter=1"}]], delivery=DeliveryStore(env.store),
        result_parent_ids=(env.receipt.result_payload_id,))
    reply = await env.messages.finalize_reply(env.actor, "reply", **arguments)
    assert (await env.messages.finalize_reply(env.actor, "reply", **arguments))["reply_id"] == reply["reply_id"]
    assert (await row(env, "SELECT count(*) AS n FROM public.bot_send_intents"))["n"] == 1
    source = await row(env, "SELECT u.lineage_complete FROM public.bot_memory_source_units u "
        "JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) WHERE o.reply_id=%s", (reply["reply_id"],))
    assert source["lineage_complete"] is False  # This compatibility proof is not cross-group RAG qualification.
    await check(env, [env.receipt.result_payload_id])
    payloads = PayloadStore(env.store)
    await payloads.forget_sources(env.lease, "forget-old-configuration",
        (SourceRef(legacy_marker(env.store.bot_id)),), expected_deletion_epoch=0)
    with pytest.raises(ForestError) as denied:
        await check(env, [env.receipt.result_payload_id])
    assert denied.value.code == "PAYLOAD_SCOPE_DENIED"
    assert (await row(env, "SELECT status FROM public.bot_calls WHERE operation_id='actual-worker'"))["status"] == "COMPLETED"


async def test_frozen_bridge_owner_and_family_are_required_for_every_unknown_leaf(configured):
    env = configured
    tracked = await row(env, "SELECT * FROM public.bot_web_operations WHERE operation_id='old-apply'")
    untracked = await row(env, "SELECT * FROM public.bot_control_value_sources WHERE source_id='old-untracked'")
    assert tracked["input_legacy_unknown"] and untracked["legacy_untracked"]
    assert await leaves(env, [tracked["result_payload_id"], untracked["payload_id"]]) == {
        tracked["result_payload_id"], untracked["payload_id"],
    }
    await check(env, [untracked["payload_id"]])
    payloads = PayloadStore(env.store)
    forged = await payloads.put_json(env.lease, owner_kind="control.config",
        owner_id="legacy-control:" + digest_json(["config", "PLAN", "not-a-real-old-plan"]),
        value={"values": env.values}, sources=(SourceRef(legacy_marker(env.store.bot_id)),),
        expected_deletion_epoch=0)
    assert await leaves(env, [forged.payload_id]) == set()
    # An existing plan's identifier cannot attach this counterfeit body or grant
    # the migration-only legacy flag to a newly inserted bridge.
    with pytest.raises(psycopg.Error):
        async with env.store.pool.connection() as conn, conn.transaction():
            await conn.execute("INSERT INTO public.bot_control_value_sources(bot_id,family,source_kind,source_id,"
                "payload_id,payload_digest,legacy_untracked) VALUES (%s,'config','PLAN','old-untracked',%s,%s,true)",
                (env.store.bot_id, forged.payload_id, forged.digest))
    mixed = await payloads.put_json(env.lease, owner_kind="kernel.result", owner_id="mixed-cache",
        value="mixed", parents=(env.receipt.result_payload_id, forged.payload_id), expected_deletion_epoch=0)
    with pytest.raises(ForestError) as denied:
        await check(env, [mixed.payload_id])
    assert denied.value.code == "PAYLOAD_SCOPE_DENIED"


async def test_new_real_configuration_receipt_cannot_hide_unknown_parent(configured, monkeypatch):
    env = configured
    payloads = PayloadStore(env.store)
    unknown = await payloads.put_json(env.lease, owner_kind="kernel.result", owner_id="unpublished-cache",
        value="unpublished", sources=(SourceRef(legacy_marker(env.store.bot_id)),), expected_deletion_epoch=0)
    original = env.env.control._record

    async def derived(conn, claims, result, *, parents=()):
        await original(conn, claims, result, parents=(*parents, env.receipt.result_payload_id, unknown.payload_id))

    monkeypatch.setattr(env.env.control, "_record", derived)
    payload = {"component_id": "example.counter", "expected_revision": 1, "values": env.values}
    plan = await env.env.control.plan(await env.env.ticket("config.plan", "new-plan", payload), "new-plan", payload)
    current = await row(env, "SELECT * FROM public.bot_web_operations WHERE operation_id='new-plan'")
    assert not current["input_legacy_unknown"] and current["input_payload_id"]
    assert await row(env, "SELECT 1 FROM public.bot_control_value_sources WHERE source_id=%s AND payload_id=%s",
        (plan["plan_id"], current["result_payload_id"]))
    assert await leaves(env, [current["result_payload_id"]]) == set()
    with pytest.raises(ForestError) as denied:
        await check(env, [current["result_payload_id"]])
    assert denied.value.code == "PAYLOAD_SCOPE_DENIED"


async def test_proven_old_configuration_does_not_hide_actual_private_source(configured):
    env = configured
    value = event(2, group="private-owner", text="private input")
    value["channel_kind"] = "private"
    incoming = await env.messages.ingest(env.lease, "binding-a", value, decision="NO_TRIGGER")
    await drain(env.messages, env.lease, incoming, operation="private-drain")
    private = await row(env, "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s", (incoming["event_id"],))
    mixed = await PayloadStore(env.store).put_json(env.lease, owner_kind="kernel.result", owner_id="private-and-config",
        value="mixed", parents=(env.receipt.result_payload_id, private["payload_id"]), expected_deletion_epoch=0)
    with pytest.raises(ForestError) as denied:
        await check(env, [mixed.payload_id])
    assert denied.value.code == "PAYLOAD_SCOPE_DENIED" and denied.value.details is None


@pytest.mark.parametrize("configured", [True], indirect=True)
async def test_untracked_revision_uses_real_activation_not_a_hash_scan_or_guess(configured):
    env = configured
    adopted = await row(env, "SELECT * FROM public.bot_control_value_sources WHERE source_kind='REVISION' AND source_id=%s",
        (digest_json(["example.counter", 2]),))
    unused = await row(env, "SELECT * FROM public.bot_control_value_sources WHERE source_kind='REVISION' AND source_id=%s",
        (digest_json(["example.counter", 3]),))
    assert adopted["legacy_untracked"] and unused["legacy_untracked"]
    assert await leaves(env, [adopted["payload_id"], unused["payload_id"]]) == {adopted["payload_id"]}
    await check(env, [env.receipt.result_payload_id])
    with pytest.raises(ForestError) as denied:
        await check(env, [unused["payload_id"]])
    assert denied.value.code == "PAYLOAD_SCOPE_DENIED"
