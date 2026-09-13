"""Real f86 Kernel/Context receipts retain unknown old Binding prerequisites."""

import json

import pytest
from legacy_041 import run_legacy_041
from test_binding_config_upgrade import LEGACY_SEED as BINDING_SEED
from test_bot_store import database as database
from test_config_writer_fence import resources

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage import bot as bot_module
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.migration_v1 import legacy_source_ref
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres

SEED_FUNCTIONS = r"""
        from forest.storage.messages import MessageStore
        messages=MessageStore(store,binding)
        async def context_for(binding_id,self_id,number):
            receipt=await messages.ingest(lease,binding_id,dict(platform='onebot-v11',self_id=self_id,
                channel_kind='group',channel_id='shared-group',thread_id='',platform_message_id=str(number),
                sent_at='2026-09-13T00:00:00Z',author=dict(external_user_id='user-'+str(number),
                display_name='original-user',is_self=False),segments=[dict(type='text',text='original-chat-'+str(number))]),
                decision='TRIGGER')
            actor=await messages.acquire_actor(lease,receipt['conversation_id'],'actor-'+str(number),ttl_seconds=120)
            selected=await messages.select_input(actor,'select-'+str(number))
            return receipt,await messages.context(actor,run_id=selected['run_id'])
        async def call(name,parents,service='reply.respond'):
            value=dict(template='OLD-TEMPLATE-1',text='original-chat-1',sender='original-user',group='shared-group')
            identity=dict(component='forest.reply',service=service,contract_major=1,payload=value)
            if parents: identity['input_parent_ids']=parents
            grant=await store.admit(lease,'forest.reply',name,'operation:'+name,digest_json(identity),
                input_value=value,input_parent_ids=tuple(parents))
            return await store.finish_call(grant,dict(parts=[dict(type='text',text='OLD-TEMPLATE-1 original-chat-1')]))
"""
SEED_CALLS = r"""
                receipt,view=await context_for('binding-old','10001',1)
                await store.register_generation(lease,'forest.reply','old-generation','package-old',digest_json({}),
                    'old-worker',config={})
                await store.mark_ready(lease,'old-generation')
                operation=await store.begin_operation(lease,'forest.reply','old-publish','old-plan',
                    'old-generation',0,ttl_seconds=60)
                await store.freeze(lease,'old-publish',operation['op_epoch'])
                await store.publish(lease,'old-publish',operation['op_epoch'])
                calls={}
                calls['template']=await call('old-template',view['payload_ids'])
                calls['wrong_service']=await call('other-service',view['payload_ids'],'reply.other')
                calls['no_context']=await call('no-context',[])
                p=PayloadStore(store)
                descendant=await p.put_json(lease,owner_kind='model.snapshot',owner_id='old-template-copy',
                    value={'cached':'OLD-TEMPLATE-1'},parents=(calls['template']['result_payload_id'],),
                    expected_deletion_epoch=0)
"""
SEED_AFTER = r"""
        other=dict(binding_id='binding-other',self_id='10002',display_name='independent')
        await binding.create(ticket('binding.create','other-create',other),'other-create',other)
        payload=dict(binding_id='binding-other',expected_revision=0,values=values,
            credentials_changed=True,secret_digest=hashlib.sha256(secret.encode()).hexdigest())
        plan=await binding.plan(ticket('binding.plan','other-plan',payload),'other-plan',payload,secret)
        payload=dict(binding_id='binding-other',expected_revision=0,plan_id=plan['plan_id'],plan_digest=plan['plan_digest'])
        await binding.apply(ticket('binding.apply','other-apply',payload),'other-apply',payload)
        await binding.activate(lease,'binding-other',1,enabled=True,client_incarnation='other-client',expected_binding_epoch=1)
        other_receipt,other_view=await context_for('binding-other','10002',2)
        calls['mixed_binding']=await call('mixed-binding',sorted(view['payload_ids']+other_view['payload_ids']))
        disposable=await p.put_json(lease,owner_kind='model.snapshot',owner_id='independent-delete-target',
            value={'discard':True},sources=(SourceRef('unrelated-source'),),expected_deletion_epoch=0)
        if PARAMETERS['mode']=='predeleted':
            await p.forget_sources(lease,'before-upgrade',(SourceRef('unrelated-source'),),expected_deletion_epoch=0)
            while (await p.scrub(lease,limit=256))['remaining']: pass
        if PARAMETERS['mode']=='missing_body':
            async with store.pool.connection() as conn:
                source=await (await conn.execute("SELECT source_unit_id FROM public.bot_payload_domains "
                    "WHERE kind='SERVICE_INPUT' AND owner_id='old-template'")).fetchone()
            await p.forget_sources(lease,'before-upgrade',(SourceRef(source['source_unit_id']),),expected_deletion_epoch=0)
            while (await p.scrub(lease,limit=256))['remaining']: pass
"""
SEED_CAPTURE = r"""
        async with store.pool.connection() as conn:
            result['headers']=await (await conn.execute('SELECT * FROM public.bot_payload_headers ORDER BY payload_id')).fetchall()
            result['sources']=await (await conn.execute('SELECT * FROM public.bot_payload_sources ORDER BY payload_id,source_unit_id,source_revision,part_id')).fetchall()
            result['derivations']=await (await conn.execute('SELECT * FROM public.bot_payload_derivations ORDER BY payload_id,parent_payload_id')).fetchall()
            result['calls']=await (await conn.execute('SELECT * FROM public.bot_calls ORDER BY call_id')).fetchall()
        result['template']={name:row['input_payload_id'] for name,row in calls.items()}
        result['template']['result']=calls['template']['result_payload_id']
        result['template']['descendant']=descendant.payload_id
        result['template']['chat']=view['payload_ids']
        result['template']['other_chat']=other_view['payload_ids']
        result['template']['conversation_id']=receipt['conversation_id']
"""
LEGACY_SEED = (
    BINDING_SEED.replace(
        "        for number in (1,2):", SEED_FUNCTIONS + "\n        for number in (1,2):"
    )
    .replace(
        "        if PARAMETERS['erased']:",
        SEED_CALLS + SEED_AFTER + "\n        if PARAMETERS['erased']:",
    )
    .replace(
        "        await store.release_host(lease)",
        SEED_CAPTURE + "\n        await store.release_host(lease)",
    )
)


def upgrade_resources(tmp_path, monkeypatch):
    # Freeze the complete release chain, including the actual 048 component
    # migration and every original migration/helper checksum.
    path = resources(tmp_path / "new-schema", cutoff="049")
    monkeypatch.setattr(bot_module, "files", lambda _: path)


async def rows(store, statement, parameters=()):
    async with store.pool.connection() as conn:
        return await (await conn.execute(statement, parameters)).fetchall()


@pytest.mark.parametrize("mode", ["alive", "predeleted", "missing_body"])
async def test_real_041_template_prerequisite_is_unknown_not_current_binding_revision(
    database,
    tmp_path,
    monkeypatch,
    mode,
):
    old = await run_legacy_041(
        tmp_path, LEGACY_SEED, {"dsn": database, "erased": False, "mode": mode}
    )
    upgrade_resources(tmp_path, monkeypatch)
    store = BotStore(database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        await store.migrate()
        lease = await store.acquire_host("new-host", "template-upgrade", ttl_seconds=120)
        p = PayloadStore(store)
        identity = old["template"]
        dependencies = await rows(
            store,
            "SELECT d.*,h.manifest,b.content FROM public.bot_payload_legacy_dependencies d "
            "JOIN public.bot_payload_headers h ON h.bot_id=d.bot_id AND h.payload_id=d.parent_payload_id "
            "LEFT JOIN public.bot_payload_bodies b ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id "
            "WHERE d.proof_kind='binding.unknown' ORDER BY d.payload_id",
        )
        assert len(dependencies) == 1
        bridge = dependencies[0]
        assert bridge["payload_id"] == identity["template"] and bridge["proof_id"] == "old-template"
        assert legacy_source_ref("bot-a").source_unit_id in {
            s["source_unit_id"] for s in bridge["manifest"]["sources"]
        }
        revisions = await rows(
            store, "SELECT binding_id,revision,values_payload_id FROM public.bot_binding_revisions"
        )
        revision_ids = {row["values_payload_id"] for row in revisions}
        assert bridge["parent_payload_id"] not in revision_ids
        expected_proof = {
            "call_id": "old-template",
            "binding_id": "binding-old",
            "conversation_ids": [identity["conversation_id"]],
            "proof": "CONTEXT_BOUND_INPUT_BODY_UNAVAILABLE"
            if mode == "missing_body"
            else "EXACT_TEMPLATE_CALL_WITH_CONTEXT",
        }
        anchor_header = (
            await rows(
                store,
                "SELECT digest FROM public.bot_payload_headers WHERE payload_id=%s",
                (bridge["parent_payload_id"],),
            )
        )[0]
        assert anchor_header["digest"] == digest_json(expected_proof)
        if mode == "alive":
            assert json.loads(bytes(bridge["content"])) == expected_proof
        else:
            assert bridge["content"] is None
        # Immutable call facts and original body/source/parent proofs survive.
        actual_calls = json.loads(
            json.dumps(
                await rows(store, "SELECT * FROM public.bot_calls ORDER BY call_id"), default=str
            )
        )
        assert actual_calls == old["calls"]
        ledger = json.loads(
            json.dumps(
                await rows(
                    store, "SELECT * FROM public.forest_bot_schema_migrations ORDER BY version"
                ),
                default=str,
            )
        )
        assert ledger[: len(old["ledger"])] == old["ledger"]
        for table, ordering in (
            ("bot_payload_sources", "payload_id,source_unit_id,source_revision,part_id"),
            ("bot_payload_derivations", "payload_id,parent_payload_id"),
        ):
            actual = await rows(store, "SELECT * FROM public." + table + " ORDER BY " + ordering)
            before = old["sources" if table.endswith("sources") else "derivations"]
            old_ids = {h["payload_id"] for h in old["headers"]}
            assert [r for r in actual if r["payload_id"] in old_ids] == before
        headers = {
            r["payload_id"]: r
            for r in await rows(store, "SELECT * FROM public.bot_payload_headers")
        }
        for before in old["headers"]:
            after = headers[before["payload_id"]]
            assert {
                key: after[key]
                for key in ("manifest", "digest", "input_digest", "owner_kind", "owner_id")
            } == {
                key: before[key]
                for key in ("manifest", "digest", "input_digest", "owner_kind", "owner_id")
            }
        clock = (await rows(store, "SELECT deletion_epoch FROM public.bot_memory_clock"))[0][
            "deletion_epoch"
        ]
        copies = [identity["template"], identity["result"], identity["descendant"]]
        if mode == "alive":
            assert (await p.read_json(lease, identity["template"], expected_deletion_epoch=clock))[
                "template"
            ] == "OLD-TEMPLATE-1"
            future = await p.put_json(
                lease,
                owner_kind="model.snapshot",
                owner_id="future-template-copy",
                value={"copied": "OLD-TEMPLATE-1"},
                parents=(identity["descendant"],),
                expected_deletion_epoch=clock,
            )
            copies.append(future.payload_id)
            inherited = await rows(
                store,
                "SELECT source_unit_id FROM public.bot_payload_sources WHERE payload_id=%s",
                (future.payload_id,),
            )
            assert legacy_source_ref("bot-a").source_unit_id in {
                r["source_unit_id"] for r in inherited
            }
            result = await p.forget_sources(
                lease,
                "after-upgrade",
                (SourceRef("unrelated-source"),),
                expected_deletion_epoch=clock,
            )
            assert result["legacy_conservative"]
            clock = result["deletion_epoch"]
        for payload_id in copies:
            with pytest.raises(ForestError) as exc:
                await p.read_json(lease, payload_id, expected_deletion_epoch=clock)
            assert exc.value.code in {"PAYLOAD_ERASED", "SOURCE_SUPPRESSED"}
        while (await p.scrub(lease, limit=256))["remaining"]:
            pass
        assert not await rows(
            store, "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=ANY(%s)", (copies,)
        )
        for payload_id in identity["chat"] + identity["other_chat"]:
            original = await p.read_json(lease, payload_id, expected_deletion_epoch=clock)
            assert original["segments"][0]["text"].startswith("original-chat-")
        # Neither current V nor another Binding gets an invented dependency.
        async with store.pool.connection() as conn, conn.transaction():
            await store._host(conn, lease)
            for revision in await (
                await conn.execute("SELECT * FROM public.bot_binding_revisions")
            ).fetchall():
                values = await BindingStore(store)._values_in(conn, revision)
                assert values["values"]["response_template"] in {"OLD-TEMPLATE-1", "OLD-TEMPLATE-2"}
    finally:
        await store.close()
