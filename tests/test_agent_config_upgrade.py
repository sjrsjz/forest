"""The exact published f86ba34 package writes all pre-045 facts."""

import base64

import psycopg
import pytest
from legacy_041 import run_legacy_041
from test_agent_config_payloads import erase_payload, rows
from test_messages import owner, receive
from test_upgrade import upgrade_database as upgrade_database

from forest.agent_settings import normalize_stored_agent_values
from forest.storage.agent import AgentStore
from forest.storage.agent_config import AgentConfig
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.messages import MessageStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres

SEED = r'''
import asyncio, base64, hashlib, json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.models import ModelCapabilities, ModelResult, prepare_model_request
from forest.packages import digest_json
from forest.security import SecretVault
from forest.storage.agent import AgentStore
from forest.storage.agent_config import AgentConfig
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.children import ChildStore
from forest.storage.control import BotControl
from forest.storage.messages import MessageStore
from forest.storage.payloads import PayloadStore
from forest.memory_types import SourceRef

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    store = BotStore(PARAMETERS['dsn'], 'bot-a')
    await store.open()
    await store.migrate()
    host = await store.acquire_host('old', 'old-041', ttl_seconds=300)
    key = Ed25519PrivateKey.generate()
    await BotControl(store).trust(host,issuer_id='real-old-issuer',key_revision=1,
        public_key=base64.b64encode(key.public_key().public_bytes_raw()).decode())
    def ticket(action, operation, payload, digest=''):
        now=datetime.now(UTC)
        claims=dict(ticket_id=str(uuid4()),issuer_id='real-old-issuer',subject_id='old-admin',
            session_ref='old-session',bot_id='bot-a',operation_id=operation,action=action,
            input_digest=digest_json(payload),plan_digest=digest,issued_at=now.isoformat(),
            expires_at=(now+timedelta(seconds=25)).isoformat(),key_revision=1,account_revision=1,
            session_revision=1,policy_revision=1)
        return dict(claims=claims,signature=base64.b64encode(key.sign(digest_json(claims).encode())).decode())
    vault=SecretVault(Path(PARAMETERS['vault']))
    configs=AgentConfig(store,vault)
    values={**DEFAULT_AGENT_SETTINGS,'enabled':True,'model':'old-real-model',
        'system_prompt':'OLD-SPEC-CANARY','children_enabled':True,'max_output_tokens':64,
        'child_max_output_tokens':64}
    payload=dict(expected_revision=0,values=values,credentials_changed=True,secret_digest=digest_json({'api_key':'OLD-KEY'}))
    plan=await configs.plan(ticket('agent.config.plan','old-config-plan',payload),'old-config-plan',payload,
        secret_ciphertext=vault.encrypt('OLD-KEY'))
    payload={k:plan[k] for k in ('expected_revision','plan_id','plan_digest')}
    await configs.apply(ticket('agent.config.apply','old-config-apply',payload,payload['plan_digest']),
        'old-config-apply',payload)
    bindings=BindingStore(store)
    payload=dict(binding_id='binding-a',self_id='1000',display_name='old binding')
    await bindings.create(ticket('binding.create','old-create',payload),'old-create',payload)
    bindingvalues=dict(enabled=True,api_base_url='http://127.0.0.1:5700',response_mode='agent',
        response_template='',trigger_mentions=True,trigger_prefix='',private_trigger=True,admin_user_ids=[])
    cipher=vault.encrypt('{}')
    payload=dict(binding_id='binding-a',expected_revision=0,values=bindingvalues,
        credentials_changed=True,secret_digest=hashlib.sha256(cipher.encode()).hexdigest())
    proposed=await bindings.plan(ticket('binding.plan','old-binding-plan',payload),'old-binding-plan',payload,cipher)
    payload={k:proposed[k] for k in ('expected_revision','plan_id','plan_digest')}
    payload['binding_id']='binding-a'
    await bindings.apply(ticket('binding.apply','old-binding-apply',payload,payload['plan_digest']),
        'old-binding-apply',payload)
    await bindings.activate(host,'binding-a',1,enabled=True,client_incarnation='old-client',expected_binding_epoch=1)
    messages=MessageStore(store,bindings)
    agent=AgentStore(messages)
    facts=[]
    for number in (1,2):
        event=dict(platform='onebot-v11',self_id='1000',channel_kind='group',channel_id='group-'+str(number),
            thread_id='',platform_message_id=str(number),sent_at='2026-09-13T00:00:00Z',
            author=dict(external_user_id='user-a',display_name='Actual Author',is_self=False),
            segments=[dict(type='text',text='OLD-INPUT-'+str(number))])
        accepted=await messages.ingest(host,'binding-a',event,decision='TRIGGER')
        actor=await messages.acquire_actor(host,accepted['conversation_id'],'old-actor-'+str(number),ttl_seconds=120)
        selected=await messages.select_input(actor,'select-'+str(number))
        spec=dict(config_revision=1,secret_revision=1,values=values,tools=[],system_prompt=values['system_prompt'])
        head=await agent.ensure_run(actor,selected['run_id'],spec)
        snapshot=await agent.snapshot(actor,head['run_id'])
        request=prepare_model_request(snapshot,ModelCapabilities('old','old','v1'),
            request_id='old-request-'+str(number),transport_attempt_id='old-attempt-'+str(number),
            output_tokens=64,stream=False)
        permit=await agent.prepare_model(actor,head['run_id'],'prepare-'+str(number),request)
        if number==1:
            await agent.check_model_permit(actor,permit,request)
            calls=[dict(id='child-call',name='forest_analyze',arguments=dict(
                authority_trigger_id=snapshot['trigger_ids'][0],arguments=dict(
                    task='OLD-CHILD-TASK',source_unit_ids=[snapshot['units'][0]['unit_id']])))]
            await agent.complete_model(actor,permit,ModelResult('RESPONDED',request.request_id,
                request.transport_attempt_id,request.payload_digest,text='Private analysis',
                tool_calls_json=json.dumps(calls),usage_json='{"total_tokens":4}'))
            await ChildStore(agent).spawn(actor,head['run_id'])
        facts.append(dict(run_id=head['run_id'],conversation_id=accepted['conversation_id'],
            spec_digest=digest_json(spec),spec=spec,request_id=request.request_id,
            wire=base64.b64encode(request.body).decode(),wire_digest=request.payload_digest))
        await messages.release_actor(actor)
    async with store.pool.connection() as conn:
        original=await (await conn.execute("SELECT input_payload_id FROM public.bot_web_operations WHERE operation_id='old-config-plan'")).fetchone()
        header=await (await conn.execute('SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s',
            (original['input_payload_id'],))).fetchone()
        facts[0]['source']=header['manifest']['sources'][0]
        immutable=await (await conn.execute('SELECT payload_id,digest,manifest FROM public.bot_payload_headers ORDER BY payload_id')).fetchall()
        ledger=await (await conn.execute('SELECT version,checksum FROM public.forest_bot_schema_migrations ORDER BY version')).fetchall()
    if PARAMETERS['delete_before']:
        await PayloadStore(store).forget_sources(host,'before-upgrade-delete',(SourceRef(**facts[0]['source']),),expected_deletion_epoch=0)
        await PayloadStore(store).scrub(host,limit=256)
    await store.release_host(host)
    await store.close()
    print(json.dumps(dict(facts=facts,immutable=immutable,ledger=ledger)))
asyncio.run(main())
'''


@pytest.mark.parametrize("delete_before", [False, True])
async def test_real_041_configuration_specs_and_existing_children_gain_dependencies(
    upgrade_database, tmp_path, delete_before,
):
    saved = await run_legacy_041(tmp_path, SEED, dict(
        dsn=upgrade_database, vault=str(tmp_path / "old-vault"), delete_before=delete_before,
    ))
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("new", "new-045", ttl_seconds=300)
        ledger = await rows(store, "SELECT version,checksum FROM public.forest_bot_schema_migrations ORDER BY version")
        assert ledger[:41] == saved["ledger"]
        current = {row["payload_id"]: row for row in await rows(
            store, "SELECT payload_id,digest,manifest FROM public.bot_payload_headers")}
        assert all(current[row["payload_id"]] == row for row in saved["immutable"])
        heads = await rows(store, "SELECT * FROM public.bot_agent_runs ORDER BY run_id")
        assert all(row["spec"] is None and row["spec_imported_legacy"] for row in heads)
        assert all(row["values"] is None for row in await rows(store, "SELECT * FROM public.bot_agent_config_revisions"))
        for fact in saved["facts"]:
            row = next(row for row in heads if row["run_id"] == fact["run_id"])
            assert row["spec_digest"] == row["spec_payload_digest"] == fact["spec_digest"]
            steps = await rows(store, "SELECT snapshot_payload_id FROM public.bot_agent_steps WHERE run_id=%s", (row["run_id"],))
            links = await rows(store, "SELECT * FROM public.bot_payload_legacy_dependencies WHERE parent_payload_id=%s",
                               (row["spec_payload_id"],))
            assert {s["snapshot_payload_id"] for s in steps} <= {edge["payload_id"] for edge in links}
        child = (await rows(store, "SELECT * FROM public.bot_agent_children"))[0]
        assert child["status"] == "CANCELLED" and child["error_code"] == "CHILD_FEATURE_RETIRED"
        childlinks = await rows(store, "SELECT * FROM public.bot_payload_legacy_dependencies WHERE proof_kind='agent.child'")
        assert {child["snapshot_payload_id"], child["spec_payload_id"]} == {link["payload_id"] for link in childlinks}
        assert {link["proof_id"] for link in childlinks} == {child["child_id"]}
        if not delete_before:
            bindings = BindingStore(store)
            before = (await rows(store, "SELECT binding_epoch FROM public.bot_bindings WHERE binding_id='binding-a'"))[0]
            await bindings.activate(lease, "binding-a", 1, enabled=True, client_incarnation="new-client",
                                    expected_binding_epoch=before["binding_epoch"])
            messages = MessageStore(store, bindings)
            fact = saved["facts"][1]
            actor = await owner(messages, lease, fact["conversation_id"], "new-actor")
            agent = AgentStore(messages)
            resumed = await agent.ensure_run(actor, fact["run_id"], None)
            assert resumed["phase"] == "RETRY_MODEL" and resumed["spec"] == fact["spec"]
            request = await agent.replay_prepared(actor, fact["run_id"])
            assert request.body == base64.b64decode(fact["wire"])
            assert request.payload_digest == fact["wire_digest"]
            current_values = (await AgentConfig(store).runtime(lease))["values"]
            assert current_values == normalize_stored_agent_values(fact["spec"]["values"])
            incoming = await receive(messages, lease, "new-after-upgrade", group="new-conversation", trigger=True)
            new_actor = await owner(messages, lease, incoming["conversation_id"], "new-run-actor")
            selected = await messages.select_input(new_actor, "new-after-cutover")
            # The old immutable values remain the configuration receipt. New
            # runtime code derives the current public projection from them.
            fresh_spec = {**fact["spec"]}
            fresh_spec.pop("system_prompt", None)
            fresh = await agent.ensure_run(new_actor, selected["run_id"], fresh_spec)
            assert fresh["spec_imported_legacy"] is False
            assert (await agent.snapshot(new_actor, fresh["run_id"]))["run_id"] == fresh["run_id"]
            await erase_payload(store, lease, resumed["spec_payload_id"])
        while (await PayloadStore(store).scrub(lease, limit=256))["remaining"]:
            pass
        bodies = await rows(store, "SELECT h.owner_kind,h.owner_id,b.content FROM public.bot_payload_headers h "
                             "JOIN public.bot_payload_bodies b USING(bot_id,payload_id)")
        assert all(b"OLD-SPEC-CANARY" not in bytes(row["content"]) for row in bodies)
        assert not await rows(store, "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id IN "
                             "(SELECT snapshot_payload_id FROM public.bot_agent_steps UNION ALL "
                             "SELECT snapshot_payload_id FROM public.bot_agent_children UNION ALL "
                             "SELECT spec_payload_id FROM public.bot_agent_children)")
        async with store.pool.connection() as conn, conn.transaction():
            with pytest.raises(psycopg.Error, match="Child Agent is retired"):
                await conn.execute(
                    "INSERT INTO public.bot_agent_children(bot_id,child_id,parent_run_id,parent_step_id,"
                    "tool_call_id,parent_input_revision,parent_snapshot_digest,authority_trigger_id,"
                    "principal_ref,source_unit_ids,snapshot_digest,snapshot_payload_id,spec_payload_id,"
                    "spec_metadata,deadline_at) VALUES ('bot-a','forbidden','x','x','x',0,'x','x','x',"
                    "'[]'::jsonb,'x','x','x','{}'::jsonb,clock_timestamp())"
                )
        await store.release_host(lease)
        await store.migrate()
        assert await rows(store, "SELECT payload_id,digest,manifest FROM public.bot_payload_headers ORDER BY payload_id")
    finally:
        await store.close()
