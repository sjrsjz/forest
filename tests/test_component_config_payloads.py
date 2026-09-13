"""Real component/Watch payload deletion, independent replacement and publication."""

import asyncio
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from legacy_041 import run_legacy_041
from psycopg.types.json import Jsonb
from test_memory_config_payloads import forget_input, rows
from test_upgrade import upgrade_database as upgrade_database
from test_web_authority import COMPONENT, wait_for_lock
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.management.fleet import BotFleet
from forest.packages import PackageCatalog, digest_json
from forest.storage.bot import BotStore
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control import BotControl
from forest.storage.watch import WatchStore
from forest.watch import DevWatcher

pytestmark = pytest.mark.postgres


def package(tmp_path):
    source = tmp_path / "component"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples" / "counter", source)
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text()
        .replace("example.counter", COMPONENT)
        .replace('["state.read", "state.write"]', "[]"),
        encoding="utf-8",
    )
    (source / "schemas/config.json").write_text(
        json.dumps(
            {
                "type": "object",
                "required": ["label"],
                "properties": {"label": {"type": "string"}},
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    (source / "schemas/output.json").write_text(
        json.dumps(
            {
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "string"}},
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    (source / "python/counter.py").write_text(
        """from forest.sdk import Plugin, service
class Counter(Plugin):
    async def prepare(self, ctx, config, restored_state):
        self.label = config["label"]
    @service("counter.increment")
    async def increment(self, payload, ctx):
        return {"value": self.label}
""",
        encoding="utf-8",
    )
    catalog = PackageCatalog(tmp_path / "packages")
    return source, catalog, catalog.build(source)


def values(snapshot, label):
    return {"package_digest": snapshot.digest, "config": {"label": label}, "grants": []}


async def activate(env, kernel, snapshot, revision, label, *, recovery=False):
    operation = "component-activation:" + uuid4().hex
    await env.control.bind_activation(
        env.lease, COMPONENT, revision, operation, recover_effective=recovery
    )
    return await kernel.install(snapshot, config={"label": label}, operation_id=operation)


async def invoke(kernel):
    return await kernel.invoke(
        COMPONENT, "counter.increment", {"increment": 1}, operation_id=uuid4().hex
    )


async def test_independent_full_plan_survives_erased_mixed_preview_and_publishes_real_worker(
    environment, tmp_path
):
    env = environment
    _, _, snapshot = package(tmp_path)
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    try:
        await env.apply(await env.plan(values=values(snapshot, "A-private"), operation="a"))
        await activate(env, kernel, snapshot, 1, "A-private")
        assert await invoke(kernel) == {"value": "A-private"}
        b = await env.plan(expected=1, values=values(snapshot, "B-independent"), operation="b")
        assert b["before_values"]["config"] == {"label": "A-private"}
        await forget_input(env, "a")
        replay = await env.plan(expected=1, values=values(snapshot, "B-independent"), operation="b")
        assert BotControl.is_unavailable(replay)
        await env.apply(b, operation="apply-b")
        await activate(env, kernel, snapshot, 2, "B-independent")
        assert await invoke(kernel) == {"value": "B-independent"}
        current = await env.read()
        assert (
            current["desired_values_payload_state"]
            == current["effective_values_payload_state"]
            == "ACTIVE"
        )
        for table in ("bot_config_plans", "bot_config_revisions"):
            saved = await rows(env, f"SELECT * FROM public.{table}")
            assert all(
                row["values"] is None
                and set(row["values_metadata"]) == {"package_digest", "grants"}
                for row in saved
            )
        assert await rows(
            env,
            "SELECT 1 FROM public.bot_payload_derivations d JOIN public.bot_generations g "
            "ON g.bot_id=d.bot_id AND g.config_payload_id=d.payload_id JOIN public.bot_config_revisions r "
            "ON r.bot_id=d.bot_id AND r.values_payload_id=d.parent_payload_id WHERE r.revision=2",
        )
        history_payload = {"component_id": COMPONENT, "before": 3}
        history = await env.control.history(
            await env.ticket("config.history", "history", history_payload),
            "history",
            history_payload,
        )
        assert [(row["revision"], row["values_payload_state"]) for row in history["revisions"]] == [
            (2, "ACTIVE"),
            (1, "UNAVAILABLE"),
        ]
        assert history["revisions"][1]["values"] is None
    finally:
        await kernel.close()


async def test_unavailable_desired_does_not_block_independent_effective_fleet_recovery(
    environment, tmp_path
):
    env = environment
    _, _, snapshot = package(tmp_path)
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    await env.apply(await env.plan(values=values(snapshot, "effective-A"), operation="a"))
    try:
        await activate(env, kernel, snapshot, 1, "effective-A")
        await env.apply(
            await env.plan(expected=1, values=values(snapshot, "pending-B"), operation="b")
        )
        await forget_input(env, "b")
        status = (await env.control.desired(env.lease))[0]
        assert status["values"] is None and status["values_payload_state"] == "UNAVAILABLE"
        assert status["effective_values"]["config"] == {"label": "effective-A"}
    finally:
        await kernel.close()
    restored = Kernel(env.store, lease_seconds=120)
    await restored.start()
    env.lease = restored.lease
    fleet = BotFleet.__new__(BotFleet)
    fleet.packages, fleet.errors = {snapshot.digest: snapshot}, {}
    fleet.package_metadata = {snapshot.digest: BotFleet.describe_package(snapshot)}
    bot = SimpleNamespace(
        store=env.store, control=env.control, kernel=restored, applied={}, errors={}, retries={}
    )
    try:
        await fleet._reconcile_once(bot)
        assert await invoke(restored) == {"value": "effective-A"}
        assert bot.errors[COMPONENT] == "CONTROL_CONTENT_REVOKED"
        assert (await env.read())["effective_revision"] == 1
        c = await env.plan(expected=2, values=values(snapshot, "repair-C"), operation="c")
        assert c["before_values"] is None and c["before_values_payload_state"] == "UNAVAILABLE"
        await env.apply(c)
        await fleet._reconcile_once(bot)
        assert await invoke(restored) == {"value": "repair-C"}
    finally:
        await restored.close()


@pytest.mark.parametrize("boundary", ["prepare", "publish"])
async def test_configuration_deleted_at_real_worker_boundary_cannot_publish(
    environment, tmp_path, monkeypatch, boundary
):
    env = environment
    _, _, snapshot = package(tmp_path)
    await env.apply(await env.plan(values=values(snapshot, "guarded-private"), operation="guarded"))
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    if boundary == "prepare":
        original = env.store.snapshot_state

        async def revoke(*args, **kwargs):
            result = await original(*args, **kwargs)
            await forget_input(env, "guarded")
            return result

        monkeypatch.setattr(env.store, "snapshot_state", revoke)
    else:
        original = env.store.publish

        async def revoke(*args, **kwargs):
            await forget_input(env, "guarded")
            return await original(*args, **kwargs)

        monkeypatch.setattr(env.store, "publish", revoke)
    try:
        with pytest.raises(ForestError) as rejected:
            await activate(env, kernel, snapshot, 1, "guarded-private")
        assert rejected.value.code in {
            "GENERATION_EXPOSURE_REVOKED",
            "KERNEL_CONTENT_REVOKED",
            "CONTROL_CONTENT_REVOKED",
        }
        assert (await env.read())["effective_revision"] == 0
        assert not kernel.components and not kernel._workers
    finally:
        await kernel.close()


async def test_watch_spec_is_removable_and_cli_cache_cannot_recreate_it(environment, tmp_path):
    env = environment
    source, catalog, _ = package(tmp_path)
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    watcher = DevWatcher(kernel, catalog, source, config={"label": "watch-private"})
    try:
        await watcher.start()
        async with asyncio.timeout(15):
            while watcher.published_count == 0 and not watcher.failure:
                await asyncio.sleep(0.03)
        assert watcher.failure is None
        head = (await rows(env, "SELECT * FROM public.bot_dev_watch_heads"))[0]
        assert head["source_identity"] is head["config"] is head["grants"] is None
        assert head["spec_payload_id"] and head["spec_payload_digest"]
        # Host initialize has a real independent control input, but does not
        # fabricate a Web session, issuer or completed Web operation.
        assert not await rows(
            env, "SELECT * FROM public.bot_web_operations WHERE action='watch.initialize'"
        )
        input_header = (
            await rows(
                env,
                "SELECT h.payload_id FROM public.bot_payload_headers h "
                "JOIN public.bot_payload_derivations d ON d.bot_id=h.bot_id AND d.parent_payload_id=h.payload_id "
                "WHERE d.payload_id=%s AND h.owner_kind='control.input'",
                (head["spec_payload_id"],),
            )
        )[0]
        domain = (
            await rows(
                env,
                "SELECT source_unit_id FROM public.bot_payload_sources WHERE payload_id=%s",
                (input_header["payload_id"],),
            )
        )[0]["source_unit_id"]
        from forest.memory_types import SourceRef
        from forest.storage.payloads import PayloadStore

        repo = PayloadStore(env.store)
        await repo.forget_sources(
            env.lease, "erase-watch", (SourceRef(domain),), expected_deletion_epoch=0
        )
        await repo.scrub(env.lease, limit=256)
        # Even an unchanged source tree cannot keep a deleted spec alive in the
        # observer's cached path/config; no filesystem edit is needed to stop it.
        async with asyncio.timeout(10):
            while watcher.failure is None:
                await asyncio.sleep(0.02)
        assert watcher.failure.code == "CONTROL_CONTENT_REVOKED"
        await watcher.close()
        with pytest.raises(ForestError) as denied:
            await WatchStore(env.store).initialize(
                env.lease, watcher.source_identity, watcher.config, watcher.grants
            )
        assert denied.value.code == "CONTROL_CONTENT_REVOKED"
        await WatchStore(env.store).failed(
            env.lease, head["watch_id"], head["desired_request_id"], "WATCH_CONTENT_REVOKED"
        )
        assert not await rows(
            env,
            "SELECT * FROM public.bot_payload_bodies WHERE payload_id=%s",
            (head["spec_payload_id"],),
        )
        assert await rows(
            env,
            "SELECT 1 FROM public.bot_payload_derivations d JOIN public.bot_generations g "
            "ON g.bot_id=d.bot_id AND g.config_payload_id=d.payload_id WHERE d.parent_payload_id=%s",
            (head["spec_payload_id"],),
        )
    finally:
        await watcher.close()
        await kernel.close()


async def test_unbridged_plan_rolls_back_all_payload_writes(environment):
    env = environment
    content = ConfigPayloadStore(env.store)
    before = len(await rows(env, "SELECT * FROM public.bot_payload_headers"))
    payload = {
        "component_id": COMPONENT,
        "expected_revision": 0,
        "values": {"package_digest": "fixture-package", "config": {}, "grants": []},
    }
    ticket = await env.ticket("config.plan", "standalone", payload)
    with pytest.raises(psycopg.errors.CheckViolation, match="CONFIGURATION_PLAN_RECEIPT_REQUIRED"):
        async with env.store.pool.connection() as conn, conn.transaction():
            claims = await env.control._guard(conn, ticket, "config.plan", "standalone", payload)
            await env.control._head(conn, COMPONENT)
            original = await env.control.admitted_input_in(conn, claims)
            ref = await content.put_values_in(
                conn,
                family="config",
                kind="PLAN",
                resource=COMPONENT,
                identity="standalone-plan",
                values=payload["values"],
                parents=(original.payload_id,),
                epoch=0,
            )
            await conn.execute(
                "INSERT INTO public.bot_config_plans(bot_id,plan_id,component_id,subject_id,issuer_id,expected_revision,values_payload_id,values_payload_digest,values_metadata,plan_digest,expires_at) "
                "VALUES(%s,'standalone-plan',%s,%s,%s,0,%s,%s,%s,%s,clock_timestamp()+interval '1 minute')",
                (
                    env.store.bot_id,
                    COMPONENT,
                    claims["subject_id"],
                    claims["issuer_id"],
                    ref.payload_id,
                    ref.digest,
                    Jsonb({k: payload["values"][k] for k in ("package_digest", "grants")}),
                    digest_json(payload),
                ),
            )
    assert len(await rows(env, "SELECT * FROM public.bot_payload_headers")) == before
    assert not await rows(env, "SELECT * FROM public.bot_config_plans")


async def historical_plan(env, *, source_revision=1, operation="history-copy"):
    payload = {
        "component_id": COMPONENT,
        "expected_revision": 1,
        "source_revision": source_revision,
        "values": {
            "package_digest": "fixture-package",
            "config": {"label": "edited-history"},
            "grants": [],
        },
    }
    return await env.control.plan(
        await env.ticket("config.plan", operation, payload), operation, payload
    )


async def test_history_copy_remains_derived_after_editing_and_applying(environment):
    env = environment
    await env.apply(await env.plan(operation="history-original"))
    copied = await historical_plan(env)
    await env.apply(copied, "history-apply")
    assert (await env.read())["desired_values"]["config"] == {"label": "edited-history"}
    assert await rows(
        env,
        "SELECT 1 FROM public.bot_payload_derivations d JOIN public.bot_web_operations w "
        "ON w.bot_id=d.bot_id AND w.input_payload_id=d.payload_id JOIN public.bot_config_revisions r "
        "ON r.bot_id=d.bot_id AND r.values_payload_id=d.parent_payload_id "
        "WHERE w.operation_id='history-copy' AND r.component_id=%s AND r.revision=1",
        (COMPONENT,),
    )
    await forget_input(env, "history-original")
    assert (await env.read())["desired_values_payload_state"] == "UNAVAILABLE"
    assert BotControl.is_unavailable(await env.apply(copied, "history-apply"))


async def test_history_copy_cannot_apply_after_original_source_is_erased(environment):
    env = environment
    await env.apply(await env.plan(operation="history-original"))
    copied = await historical_plan(env)
    await forget_input(env, "history-original")
    with pytest.raises(ForestError) as denied:
        await env.apply(copied, "history-apply")
    assert denied.value.code == "CONTROL_CONTENT_REVOKED"
    assert not await rows(
        env, "SELECT 1 FROM public.bot_web_operations WHERE operation_id='history-apply'"
    )
    assert (await env.read())["desired_revision"] == 1


async def test_history_request_queued_behind_real_forget_transaction_cannot_reimport_body(
    environment, monkeypatch
):
    from forest.memory_types import SourceRef
    from forest.storage.payloads import PayloadStore

    env = environment
    await env.apply(await env.plan(operation="history-original"))
    target = (
        await rows(
            env,
            "SELECT s.source_unit_id FROM public.bot_payload_sources s "
            "JOIN public.bot_web_operations w ON w.bot_id=s.bot_id AND w.input_payload_id=s.payload_id "
            "WHERE w.operation_id='history-original'",
        )
    )[0]["source_unit_id"]
    repo = PayloadStore(env.store)
    entered, release = asyncio.Event(), asyncio.Event()
    original = repo.lock_in

    async def held(conn):
        result = await original(conn)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(repo, "lock_in", held)
    deletion = asyncio.create_task(
        repo.forget_sources(
            env.lease, "erase-history", (SourceRef(target),), expected_deletion_epoch=0
        )
    )
    copying = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        copying = asyncio.create_task(historical_plan(env))
        await wait_for_lock(env.store, "SELECT * FROM public.bot_runtime_state%FOR UPDATE")
        release.set()
        await deletion
        with pytest.raises(ForestError) as denied:
            await copying
        assert denied.value.code == "CONTROL_CONTENT_REVOKED"
        assert not await rows(
            env, "SELECT 1 FROM public.bot_web_operations WHERE operation_id='history-copy'"
        )
        assert (await env.read())["desired_revision"] == 1
    finally:
        release.set()
        await asyncio.gather(deletion, *([copying] if copying else []), return_exceptions=True)


async def test_history_identity_is_strict_and_database_rejects_dropped_source_parent(
    environment, monkeypatch
):
    from pydantic import ValidationError

    from forest.management.bots_api import ConfigPlan

    env = environment
    await env.apply(await env.plan(operation="history-original"))
    for revision in (True, False, 0, -1, "1", 1.0, 2**63):
        with pytest.raises(ValidationError):
            ConfigPlan(expected_revision=1, values={}, source_revision=revision)
        with pytest.raises(ForestError) as invalid:
            await historical_plan(env, source_revision=revision)
        assert invalid.value.code == "CONFIGURATION_SOURCE_INVALID"
    with pytest.raises(ForestError) as missing:
        await historical_plan(env, source_revision=2)
    assert missing.value.code == "CONFIGURATION_SOURCE_NOT_FOUND"
    original = env.control.admitted_input_in

    async def faulty_writer(conn, claims):
        claims.parents = ()
        return await original(conn, claims)

    monkeypatch.setattr(env.control, "admitted_input_in", faulty_writer)
    before = len(await rows(env, "SELECT * FROM public.bot_payload_headers"))
    with pytest.raises(psycopg.errors.CheckViolation, match="CONFIGURATION_SOURCE_CONFLICT"):
        await historical_plan(env)
    assert len(await rows(env, "SELECT * FROM public.bot_payload_headers")) == before
    assert not await rows(
        env, "SELECT 1 FROM public.bot_web_operations WHERE operation_id='history-copy'"
    )


LEGACY_COMPONENTS = r"""
import asyncio, base64, json, sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from forest.packages import digest_json
from forest.memory_types import SourceRef
from forest.storage.bot import BotStore
from forest.storage.control import BotControl
from forest.storage.watch import WatchStore
from forest.storage.payloads import PayloadStore

async def main():
    store=BotStore(PARAMETERS['dsn'],'bot-a'); await store.open()
    try:
        await store.migrate(); lease=await store.acquire_host('old-host','old-incarnation',ttl_seconds=120)
        control=BotControl(store); key=Ed25519PrivateKey.generate()
        await control.trust(lease,issuer_id='old-issuer',key_revision=1,
            public_key=base64.b64encode(key.public_key().public_bytes_raw()).decode())
        async def ticket(action,operation,payload):
            now=datetime.now(UTC)
            claims=dict(ticket_id=str(uuid4()),issuer_id='old-issuer',subject_id='old-admin',session_ref='old-session',
                bot_id='bot-a',operation_id=operation,action=action,input_digest=digest_json(payload),
                plan_digest=payload.get('plan_digest',''),issued_at=(now-timedelta(seconds=1)).isoformat(),
                expires_at=(now+timedelta(seconds=29)).isoformat(),key_revision=1,account_revision=1,
                session_revision=1,policy_revision=1)
            return dict(claims=claims,signature=base64.b64encode(key.sign(digest_json(claims).encode('ascii'))).decode())
        values={'package_digest':'old-component-package','config':{'label':'OLD_COMPONENT_BODY'},'grants':[]}
        payload={'component_id':'old.component','expected_revision':0,'values':values}
        plan=await control.plan(await ticket('config.plan','old-plan',payload),'old-plan',payload)
        payload={'component_id':'old.component',**{k:plan[k] for k in ('expected_revision','plan_id','plan_digest')}}
        await control.apply(await ticket('config.apply','old-apply',payload),'old-apply',payload)
        await control.bind_activation(lease,'old.component',1,'old-component-activate')
        async def generation(component,generation,operation,package,config):
            await store.register_generation(lease,component,generation,package,digest_json(config),generation+'-worker',
                config=config,configuration_operation_id=operation)
            await store.mark_ready(lease,generation)
            route=await store.get_route(component)
            op=await store.begin_operation(lease,component,operation,digest_json([component,config]),generation,route['route_epoch'])
            await store.freeze(lease,operation,op['op_epoch']); await store.quiesce_operation(lease,operation,op['op_epoch'])
            await store.publish(lease,operation,op['op_epoch'])
        await generation('old.component','old-component-generation','old-component-activate',values['package_digest'],values['config'])
        watch=WatchStore(store)
        spec={'source_identity':'D:/private old source/原始目录','config':{'label':'OLD_WATCH_BODY'},'grants':[]}
        head=await watch.initialize(lease,spec['source_identity'],spec['config'],frozenset())
        request=await watch.accept(lease,head['watch_id'],'old-watch-request',digest_json(['old-source']))
        await watch.built(lease,head['watch_id'],request['request_id'],source_digest=request['source_digest'],
            package_digest='old-watch-package',artifact='/managed-immutable-cache/old-watch',component_id='old.watch')
        await watch.activate(lease,head['watch_id'],request['request_id'],'old-watch-activate')
        await generation('old.watch','old-watch-generation','old-watch-activate','old-watch-package',spec['config'])
        tables=('bot_config_plans','bot_config_revisions','bot_config_heads','bot_config_activations',
            'bot_dev_watch_heads','bot_dev_watch_requests','bot_dev_watch_builds','bot_dev_watch_activations',
            'bot_dev_watch_results','bot_lifecycle_operations','bot_component_routes','bot_generations','bot_web_operations')
        async with store.pool.connection() as conn:
            facts={table:await (await conn.execute('SELECT * FROM public.'+table+' ORDER BY to_jsonb('+table+')::text')).fetchall() for table in tables}
            if PARAMETERS['erased']:
                source=await (await conn.execute("SELECT s.source_unit_id FROM public.bot_web_operations w "
                    "JOIN public.bot_payload_sources s ON s.bot_id=w.bot_id AND s.payload_id=w.input_payload_id "
                    "WHERE w.operation_id='old-plan'")).fetchone()
        if PARAMETERS['erased']:
            repo=PayloadStore(store)
            await repo.forget_sources(lease,'old-delete',(SourceRef(source['source_unit_id']),),expected_deletion_epoch=0)
            await repo.scrub(lease,limit=256)
            async with store.pool.connection() as conn:
                assert await (await conn.execute("SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_generations g "
                    "ON g.bot_id=b.bot_id AND g.config_payload_id=b.payload_id WHERE g.generation_id='old-watch-generation'")).fetchone()
        async with store.pool.connection() as conn:
            ledger=await (await conn.execute('SELECT * FROM public.forest_bot_schema_migrations ORDER BY version')).fetchall()
        await store.release_host(lease)
        print(json.dumps({'facts':facts,'ledger':ledger,'watch_id':head['watch_id'],'spec':spec},default=str))
    finally:
        await store.close()
if sys.platform=='win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
"""


@pytest.mark.parametrize("erased", [False, True], ids=["active", "previously-erased"])
async def test_true_041_component_watch_upgrade_preserves_facts_and_actual_generation_dependencies(
    upgrade_database, tmp_path, erased
):
    dsn = upgrade_database
    old = await run_legacy_041(tmp_path, LEGACY_COMPONENTS, {"dsn": dsn, "erased": erased})
    store = BotStore(dsn, "bot-a")
    await store.open()
    try:
        await store.migrate()
        async with store.pool.connection() as conn:
            ledger = await (
                await conn.execute(
                    "SELECT * FROM public.forest_bot_schema_migrations ORDER BY version"
                )
            ).fetchall()
            assert (
                json.loads(json.dumps(ledger[: len(old["ledger"])], default=str)) == old["ledger"]
            )
            for table, originals in old["facts"].items():
                current = await (
                    await conn.execute(
                        f"SELECT * FROM public.{table} ORDER BY to_jsonb({table})::text"
                    )
                ).fetchall()
                current = json.loads(json.dumps(current, default=str))
                assert len(current) == len(originals)
                for previous in originals:
                    # The fixed identity fields match exactly even though adding
                    # body refs can change a generic JSON row's ordering.
                    key = next(
                        k
                        for k in (
                            "generation_id",
                            "operation_id",
                            "request_id",
                            "plan_id",
                            "watch_id",
                            "component_id",
                        )
                        if k in previous
                    )
                    found = next(row for row in current if row[key] == previous[key])
                    for field, value in previous.items():
                        removed = field == "values" and table in {
                            "bot_config_plans",
                            "bot_config_revisions",
                        }
                        removed |= table == "bot_dev_watch_heads" and field in {
                            "source_identity",
                            "config",
                            "grants",
                        }
                        assert found[field] == (None if removed else value), (table, field)
            proofs = await (
                await conn.execute(
                    "SELECT proof_kind,proof_id FROM public.bot_payload_legacy_dependencies ORDER BY proof_kind"
                )
            ).fetchall()
            assert proofs == [
                {"proof_kind": "generation.config", "proof_id": "old-component-generation"},
                {"proof_kind": "watch.config", "proof_id": "old-watch-generation"},
            ]
        lease = await store.acquire_host("upgraded-host", "upgraded-incarnation", ttl_seconds=120)
        watch = WatchStore(store)
        if erased:
            with pytest.raises(ForestError) as unavailable:
                await watch.current(lease, old["watch_id"])
            assert unavailable.value.code == "CONTROL_CONTENT_REVOKED"
            from forest.storage.payloads import PayloadStore

            await PayloadStore(store).scrub(lease, limit=256)
            async with store.pool.connection() as conn:
                assert not await (
                    await conn.execute(
                        "SELECT 1 FROM public.bot_payload_bodies b JOIN public.bot_generations g "
                        "ON g.bot_id=b.bot_id AND g.config_payload_id=b.payload_id"
                    )
                ).fetchone()
        else:
            recovered = await watch.current(lease, old["watch_id"])
            assert {k: recovered[k] for k in old["spec"]} == old["spec"]
            assert recovered["last_good"]["artifact"] == "/managed-immutable-cache/old-watch"
        await store.migrate()
        await store.release_host(lease)
    finally:
        await store.close()
