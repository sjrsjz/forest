"""Actual frozen 041 writer -> current Binding configuration families."""

import json

import pytest
from legacy_041 import run_legacy_041
from test_bot_store import database as database

from forest.errors import ForestError
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore

pytestmark = pytest.mark.postgres

LEGACY_SEED = r"""
import asyncio, base64, hashlib, json, sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.bindings import BindingStore
from forest.storage.control import BotControl
from forest.storage.payloads import PayloadStore
from forest.memory_types import SourceRef

async def main():
    store = BotStore(PARAMETERS['dsn'], 'bot-a')
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host('legacy', 'old-binding', ttl_seconds=120)
        control, binding = BotControl(store), BindingStore(store)
        key = Ed25519PrivateKey.generate()
        await control.trust(lease, issuer_id='old-web', key_revision=1,
            public_key=base64.b64encode(key.public_key().public_bytes_raw()).decode())
        def ticket(action, operation, payload):
            now = datetime.now(UTC)
            claims = dict(ticket_id=str(uuid4()),issuer_id='old-web',subject_id='old-admin',
                session_ref='old-session',bot_id='bot-a',operation_id=operation,action=action,
                input_digest=digest_json(payload),plan_digest=payload.get('plan_digest',''),
                key_revision=1,account_revision=1,session_revision=1,policy_revision=1,
                issued_at=now.isoformat(),expires_at=(now+timedelta(seconds=30)).isoformat())
            return {'claims':claims,'signature':base64.b64encode(key.sign(digest_json(claims).encode())).decode()}
        registration = dict(binding_id='binding-old',self_id='10001',display_name='OLD-NAME-CANARY')
        await binding.create(ticket('binding.create','old-create',registration),'old-create',registration)
        secret='original-encrypted-ciphertext'
        for number in (1,2):
            values=dict(enabled=True,api_base_url='http://127.0.0.1:5700',response_mode='component',
                response_template='OLD-TEMPLATE-'+str(number),trigger_mentions=True,trigger_prefix='',
                private_trigger=True,admin_user_ids=['old-admin-user'])
            payload=dict(binding_id='binding-old',expected_revision=number-1,values=values,
                credentials_changed=True,secret_digest=hashlib.sha256(secret.encode()).hexdigest())
            op='old-plan-'+str(number)
            plan=await binding.plan(ticket('binding.plan',op,payload),op,payload,secret)
            payload=dict(binding_id='binding-old',expected_revision=number-1,
                plan_id=plan['plan_id'],plan_digest=plan['plan_digest'])
            op='old-apply-'+str(number)
            await binding.apply(ticket('binding.apply',op,payload),op,payload)
            if number==1:
                await binding.activate(lease,'binding-old',1,enabled=True,
                    client_incarnation='old-client',expected_binding_epoch=1)
        if PARAMETERS['erased']:
            async with store.pool.connection() as conn:
                source=await (await conn.execute("SELECT source_unit_id FROM public.bot_payload_domains "
                    "WHERE kind='CONTROL_INPUT' AND owner_id=%s",('input:'+digest_json(['old-plan-1']),))).fetchone()
            p=PayloadStore(store)
            await p.forget_sources(lease,'old-forget',(SourceRef(source['source_unit_id']),),expected_deletion_epoch=0)
            await p.scrub(lease,limit=256)
        result={}
        async with store.pool.connection() as conn:
            for name,table,ordering in (
                ('bindings','bot_bindings','binding_id'),('plans','bot_binding_plans','plan_id'),
                ('revisions','bot_binding_revisions','revision'),('clients','bot_binding_clients','client_incarnation'),
                ('operations','bot_web_operations','operation_id'),('ledger','forest_bot_schema_migrations','version'),
                ('forgets','bot_payload_forget_requests','operation_id')):
                result[name]=await (await conn.execute('SELECT * FROM public.'+table+' ORDER BY '+ordering)).fetchall()
        await store.release_host(lease)
        print(json.dumps(result,default=str))
    finally:
        await store.close()
if sys.platform=='win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
"""


@pytest.mark.parametrize("erased", [False, True])
async def test_actual_041_binding_upgrade_preserves_effective_desired_and_erased_content(
    database, tmp_path, erased
):
    old = await run_legacy_041(tmp_path, LEGACY_SEED, {"dsn": database, "erased": erased})
    store = BotStore(database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        await store.migrate()
        lease = await store.acquire_host("new-host", "new-binding", ttl_seconds=120)
        bindings = BindingStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            actual = {}
            for name, table, ordering in (
                ("bindings", "bot_bindings", "binding_id"),
                ("plans", "bot_binding_plans", "plan_id"),
                ("revisions", "bot_binding_revisions", "revision"),
                ("clients", "bot_binding_clients", "client_incarnation"),
                ("operations", "bot_web_operations", "operation_id"),
                ("ledger", "forest_bot_schema_migrations", "version"),
                ("forgets", "bot_payload_forget_requests", "operation_id"),
            ):
                actual[name] = json.loads(
                    json.dumps(
                        await (
                            await conn.execute(
                                "SELECT * FROM public." + table + " ORDER BY " + ordering,
                            )
                        ).fetchall(),
                        default=str,
                    )
                )
            for name in ("clients", "operations", "forgets"):
                assert actual[name] == old[name]
            for name in ("bindings", "plans", "revisions"):
                removed = {"display_name", "config"} if name == "bindings" else {"values"}
                for before, after in zip(old[name], actual[name], strict=True):
                    assert {key: after[key] for key in before if key not in removed} == {
                        key: value for key, value in before.items() if key not in removed
                    }
                    assert all(after[key] is None for key in removed)
            assert actual["ledger"][: len(old["ledger"])] == old["ledger"]
            assert (
                actual["bindings"][0]["config_payload_id"]
                == actual["revisions"][0]["values_payload_id"]
            )
            await store._host(conn, lease)
            for row in actual["revisions"]:
                hydrated = await bindings._values_in(conn, row, required=False)
                if erased and row["revision"] == 1:
                    assert (
                        hydrated["values"] is None
                        and hydrated["values_payload_state"] == "UNAVAILABLE"
                    )
                    assert not await (
                        await conn.execute(
                            "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
                            (row["values_payload_id"],),
                        )
                    ).fetchone()
                else:
                    before = next(
                        item for item in old["revisions"] if item["revision"] == row["revision"]
                    )
                    assert hydrated["values"] == before["values"]
        desired = (await bindings.desired(lease))[0]
        assert desired["revision"] == 2 and desired["effective_revision"] == 1
        assert desired["display_name"] == "OLD-NAME-CANARY"
        assert desired["desired_values"]["response_template"] == "OLD-TEMPLATE-2"
        if erased:
            assert desired["config"] is None and desired["config_payload_state"] == "UNAVAILABLE"
            with pytest.raises(ForestError) as caught:
                await bindings.activate(
                    lease,
                    "binding-old",
                    1,
                    enabled=True,
                    client_incarnation="bad-restore",
                    expected_binding_epoch=2,
                    recover_effective=True,
                    expected_desired_revision=2,
                )
            assert caught.value.code == "CONTROL_CONTENT_REVOKED"
        current = await bindings.activate(
            lease,
            "binding-old",
            2,
            enabled=True,
            client_incarnation="new-client",
            expected_binding_epoch=2,
        )
        assert (
            current["effective_revision"] == 2
            and current["config"]["response_template"] == "OLD-TEMPLATE-2"
        )
        assert await bindings.credentials(lease, "binding-old") == "original-encrypted-ciphertext"
    finally:
        await store.close()
