"""Real component/Watch payload deletion, independent replacement and publication."""

import asyncio
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_memory_config_payloads import forget_input, rows
from test_upgrade import upgrade_database as upgrade_database
from test_web_authority import COMPONENT, wait_for_lock
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.management.fleet import BotFleet
from forest.packages import PackageCatalog, digest_json
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


def takeover_package(tmp_path):
    source, _, _ = package(tmp_path)
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'config_schema = "schemas/config.json"',
            'config_schema = "schemas/config.json"\nmessage_handler_takeover = "compatible"',
        )
        + '''

[[components.message_handlers]]
handler_id = "ordinary"
handler = "ordinary"
priority = 10
''',
        encoding="utf-8",
    )
    code = source / "python" / "counter.py"
    code.write_text(
        code.read_text(encoding="utf-8")
        .replace(
            "from forest.sdk import Plugin, service",
            "from forest.sdk import Pass, Plugin, message_handler, service",
        )
        .replace(
            "class Counter(Plugin):",
            '''class Counter(Plugin):
    @message_handler("ordinary", priority=10)
    async def ordinary(self, event, ctx):
        return Pass()
''',
        ),
        encoding="utf-8",
    )
    return PackageCatalog(tmp_path / "takeover-packages").build(source)


def values(snapshot, label):
    return {"package_digest": snapshot.digest, "config": {"label": label}, "grants": []}


async def test_handler_takeover_opt_in_is_bounded_component_metadata(environment):
    env = environment
    selected = {
        "package_digest": "fixture-package",
        "config": {"label": "takeover-enabled"},
        "grants": [],
        "message_handler_takeover": True,
    }
    await env.apply(await env.plan(values=selected, operation="takeover-metadata"))
    current = await env.read()
    assert current["desired_values"] == selected
    saved = await rows(
        env,
        "SELECT values_metadata FROM public.bot_config_revisions WHERE revision=1",
    )
    assert saved[0]["values_metadata"] == {
        "package_digest": "fixture-package",
        "grants": [],
        "message_handler_takeover": True,
    }

    malformed = {**selected, "message_handler_takeover": "yes"}
    with pytest.raises(ForestError) as caught:
        await env.plan(values=malformed, operation="takeover-metadata-invalid")
    assert caught.value.code == "CONFIGURATION_VALUES_INVALID"


async def activate(
    env, kernel, snapshot, revision, label, *, recovery=False, handler_takeover=False
):
    operation = "component-activation:" + uuid4().hex
    await env.control.bind_activation(
        env.lease, COMPONENT, revision, operation, recover_effective=recovery
    )
    return await kernel.install(
        snapshot,
        config={"label": label},
        operation_id=operation,
        message_handler_takeover=handler_takeover,
    )


async def invoke(kernel):
    return await kernel.invoke(
        COMPONENT, "counter.increment", {"increment": 1}, operation_id=uuid4().hex
    )


async def test_config_activation_is_the_management_proof_for_takeover(
    environment, tmp_path
):
    env = environment
    snapshot = takeover_package(tmp_path)
    selected = {
        **values(snapshot, "takeover-authorized"),
        "message_handler_takeover": True,
    }
    await env.apply(await env.plan(values=selected, operation="takeover-config"))
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    try:
        route = await activate(
            env,
            kernel,
            snapshot,
            1,
            "takeover-authorized",
            handler_takeover=True,
        )
        async with env.store.pool.connection() as conn:
            registered = await (
                await conn.execute(
                    "SELECT takeover_mode FROM public.bot_message_handler_generations "
                    "WHERE bot_id=%s AND generation_id=%s",
                    (env.store.bot_id, route["generation_id"]),
                )
            ).fetchone()
        assert registered["takeover_mode"] == "compatible"
    finally:
        await kernel.close()


async def test_kernel_cannot_invent_takeover_for_a_config_without_opt_in(
    environment, tmp_path
):
    env = environment
    snapshot = takeover_package(tmp_path)
    await env.apply(
        await env.plan(
            values=values(snapshot, "takeover-denied"), operation="takeover-denied-config"
        )
    )
    kernel = Kernel(env.store)
    kernel.lease = env.lease
    try:
        with pytest.raises(ForestError) as caught:
            await activate(
                env,
                kernel,
                snapshot,
                1,
                "takeover-denied",
                handler_takeover=True,
            )
        assert caught.value.code == "CONFIGURATION_SUPERSEDED"
    finally:
        await kernel.close()


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
