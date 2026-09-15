"""Actual 041 writer pools cannot regain authority after the atomic v2 cutover."""

import asyncio
import gzip
import hashlib
import json
import sys
from importlib.resources import files
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.storage import bot as bot_module
from forest.storage.bot import BotStore, _migration_checksum

FENCE = "042_schema_writer_v2.sql"
CURRENT_FENCE = "082_message_handler_chains.sql"
OLD_CHAIN = "addb66245fdf6113bf0067bd0c4a5be8fc114f88155ac55545bc93691f8be5ba"
OLD_WRITER_SHA = "8960f47437997394d2b07aa9f9bdbc2616da4d6a34b741f029ff170d072934dc"
CANARY = {"config": {"label": "旧配置 CANARY", "optional": None}, "grants": []}


def resources(path, *, cutoff="042"):
    source = files("forest.storage")
    (path / "bot_migrations").mkdir(parents=True)
    (path / "data_migrations").mkdir()
    for item in source.joinpath("bot_migrations").iterdir():
        if item.name.endswith(".sql") and item.name[:3] <= cutoff:
            (path / "bot_migrations" / item.name).write_text(item.read_text(encoding="utf-8"), encoding="utf-8")
    for name in bot_module.MIGRATION_HANDLERS.values():
        item = source.joinpath("data_migrations", name + ".py")
        (path / "data_migrations" / item.name).write_text(item.read_text(encoding="utf-8"), encoding="utf-8")
    for name, introduced in (
        ("migration_v1.py", "001"), ("migration_v2.py", "042"), ("migration_v3.py", "060"),
        ("migration_v4.py", "062"), ("migration_v5.py", "064"),
        ("migration_v6.py", "066"),
        ("migration_v7.py", "067"),
        ("migration_v8.py", "068"),
        ("migration_v9.py", "069"),
        ("migration_v10.py", "070"),
        ("migration_v11.py", "071"),
        ("migration_v12.py", "072"),
        ("migration_v13.py", "074"),
        ("migration_v14.py", "075"),
        ("migration_v15.py", "076"),
        ("migration_v16.py", "077"),
        ("migration_v17.py", "078"),
        ("migration_v18.py", "079"),
        ("migration_v19.py", "080"),
        ("migration_v20.py", "081"),
        ("migration_v21.py", "082"),
    ):
        if cutoff >= introduced:
            (path / name).write_text(source.joinpath(name).read_text(encoding="utf-8"), encoding="utf-8")
    if cutoff >= "062":
        (path / "shell_payloads.py").write_text(
            source.joinpath("shell_payloads.py").read_text(encoding="utf-8"), encoding="utf-8"
        )
        (path / "shell_registry.py").write_text(
            source.parent.joinpath("shell_registry.py").read_text(encoding="utf-8"), encoding="utf-8"
        )
    if cutoff >= "064":
        (path / "shell_payloads_v7.py").write_text(
            source.joinpath("shell_payloads_v7.py").read_text(encoding="utf-8"), encoding="utf-8"
        )
        (path / "shell_registry_v7.py").write_text(
            source.parent.joinpath("shell_registry_v7.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "066":
        (path / "shell_payloads_v8.py").write_text(
            source.joinpath("shell_payloads_v8.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v8.py").write_text(
            source.parent.joinpath("shell_registry_v8.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "067":
        (path / "shell_payloads_v9.py").write_text(
            source.joinpath("shell_payloads_v9.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v9.py").write_text(
            source.parent.joinpath("shell_registry_v9.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "068":
        (path / "shell_payloads_v10.py").write_text(
            source.joinpath("shell_payloads_v10.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        for name in ("shell_registry_v10.py", "shell_authority.py"):
            (path / name).write_text(
                source.parent.joinpath(name).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
    if cutoff >= "069":
        (path / "shell_payloads_v11.py").write_text(
            source.joinpath("shell_payloads_v11.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v11.py").write_text(
            source.parent.joinpath("shell_registry_v11.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "071":
        (path / "shell_payloads_v12.py").write_text(
            source.joinpath("shell_payloads_v12.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v12.py").write_text(
            source.parent.joinpath("shell_registry_v12.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    return path


def old_module(path):
    packed = Path(__file__).parent / "fixtures" / "legacy_041_writer" / "bot.py.gz"
    body = gzip.decompress(packed.read_bytes())
    assert len(body) == 123694 and hashlib.sha256(body).hexdigest() == OLD_WRITER_SHA
    module = ModuleType("frozen_writer_041_" + uuid4().hex)
    sys.modules[module.__name__] = module
    exec(compile(body, str(packed), "exec"), module.__dict__)
    module.files = lambda namespace: path
    assert module.WRITER_GENERATION == 1
    chain = [(p.name, module._migration_checksum(p.name, p.read_text(encoding="utf-8")))
             for p in sorted((path / "bot_migrations").glob("*.sql"))]
    assert len(chain) == 41
    assert hashlib.sha256(json.dumps(chain, separators=(",", ":")).encode()).hexdigest() == OLD_CHAIN
    return module


async def state(store):
    async with store.pool.connection() as conn:
        gate = await (await conn.execute("SELECT minimum_generation FROM public.bot_schema_writer_gate")).fetchone()
        host = await (await conn.execute("SELECT * FROM public.bot_host_lease")).fetchone()
        runtime = await (await conn.execute("SELECT * FROM public.bot_runtime_state")).fetchone()
        ledger = await (await conn.execute("SELECT * FROM public.forest_bot_schema_migrations ORDER BY version")).fetchall()
        plans = await (await conn.execute("SELECT * FROM public.bot_config_plans ORDER BY plan_id")).fetchall()
    return {"gate": gate["minimum_generation"], "host": host, "runtime": runtime, "ledger": ledger, "plans": plans}


async def error(code, awaitable):
    with pytest.raises(ForestError) as caught:
        await awaitable
    assert caught.value.code == code


@pytest_asyncio.fixture
async def old_041(upgrade_database, tmp_path, monkeypatch):
    module = old_module(resources(tmp_path / "old-041", cutoff="041"))
    # Fence tests isolate the 041 -> 042 boundary. Later body-family migrations
    # have independent real configuration writers and preservation assertions.
    new_resources = resources(tmp_path / "new-042")
    monkeypatch.setattr(bot_module, "files", lambda namespace: new_resources)
    old = module.BotStore(upgrade_database, "bot-a")
    new = BotStore(upgrade_database, "bot-a")
    await old.open()
    await new.open()
    try:
        await old.migrate()
        async with old.pool.connection() as conn:
            # A real immutable 041 config fact. 042 itself must not migrate or
            # reinterpret its body; the following configuration family owns it.
            await conn.execute(
                "INSERT INTO public.bot_config_plans(bot_id,plan_id,component_id,subject_id,issuer_id,"
                "expected_revision,values,plan_digest,expires_at) VALUES "
                "('bot-a','existing-plan','counter','admin','web',0,%s,'existing-digest',"
                "clock_timestamp()+interval '1 day')", (Jsonb(CANARY),),
            )
            assert (await (await conn.execute("SHOW forest.writer_generation")).fetchone())["forest.writer_generation"] == "1"
        yield SimpleNamespace(old=old, new=new, dsn=upgrade_database, module=module)
    finally:
        await new.close()
        await old.close()
        sys.modules.pop(module.__name__, None)


def test_v2_checksum_dependencies_leave_every_001_041_checksum_unchanged(tmp_path, monkeypatch):
    root = resources(tmp_path / "all")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    chain = [(p.name, _migration_checksum(p.name, p.read_text(encoding="utf-8")))
             for p in sorted((root / "bot_migrations").glob("*.sql")) if p.name[:3] <= "041"]
    assert len(chain) == 41
    assert hashlib.sha256(json.dumps(chain, separators=(",", ":")).encode()).hexdigest() == OLD_CHAIN
    source = (root / "bot_migrations" / FENCE).read_text(encoding="utf-8")
    before = _migration_checksum(FENCE, source)
    helper = root / "migration_v2.py"
    helper.write_text(helper.read_text(encoding="utf-8") + "\n# changed helper\n", encoding="utf-8")
    assert _migration_checksum(FENCE, source) != before
    assert [(name, _migration_checksum(name, (root / "bot_migrations" / name).read_text(encoding="utf-8")))
            for name, _ in chain] == chain
    helper.unlink()
    with pytest.raises(ForestError) as caught:
        _migration_checksum(FENCE, source)
    assert caught.value.code == "MIGRATION_HANDLER_MISSING"


@pytest.mark.postgres
async def test_fresh_database_crosses_all_gates_and_noop_preserves_live_host(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("new", "fresh", ttl_seconds=60)
        before = await state(store)
        assert before["gate"] == 20
        assert {FENCE, CURRENT_FENCE} <= {row["version"] for row in before["ledger"]}
        assert before["runtime"]["storage_generation"] == 1
        await store.migrate()
        assert await state(store) == before
        assert (await store.renew_host(lease)).owner_epoch == lease.owner_epoch
    finally:
        await store.close()


@pytest.mark.postgres
async def test_actual_041_live_host_then_upgrade_blocks_existing_pool_and_reconnect(old_041):
    old, new = old_041.old, old_041.new
    lease = await old.acquire_host("old", "before-cutover", ttl_seconds=60)
    before = await state(old)
    await error("MIGRATION_HOST_ACTIVE", new.migrate())
    assert await state(old) == before
    assert (await old.renew_host(lease)).owner_epoch == lease.owner_epoch
    await old.release_host(lease)
    before = await state(old)
    # Hold a genuine preexisting pool connection across the migration commit.
    async with old.pool.connection() as old_conn:
        await new.migrate()
        after = await state(new)
        assert after["gate"] == 2
        assert after["host"]["owner_epoch"] == before["host"]["owner_epoch"] + 1
        assert after["runtime"] == before["runtime"]
        assert after["plans"] == before["plans"] and after["plans"][0]["values"] == CANARY
        assert after["ledger"][:41] == before["ledger"]
        await error("STALE_HOST", old.renew_host(lease))
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"):
            await old.acquire_host("old", "old-reacquire", ttl_seconds=60)
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"):
            await old_conn.execute("UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '1 minute'")
        await error("SCHEMA_TOO_NEW", old.migrate())
    # A newly constructed actual old pool also remains writer 1.
    reopened_old = old_041.module.BotStore(old_041.dsn, "bot-a")
    reopened_new = BotStore(old_041.dsn, "bot-a")
    await reopened_old.open()
    await reopened_new.open()
    try:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"):
            await reopened_old.acquire_host("old", "reconnected-old", ttl_seconds=60)
        fresh = await reopened_new.acquire_host("new", "reconnected-new", ttl_seconds=60)
        assert fresh.owner_epoch > lease.owner_epoch
        async with reopened_new.pool.connection() as conn:
            assert (await (await conn.execute("SHOW forest.writer_generation")).fetchone())["forest.writer_generation"] == "20"
        await reopened_new.migrate()
        assert (await reopened_new.renew_host(fresh)).owner_epoch == fresh.owner_epoch
    finally:
        await reopened_new.close()
        await reopened_old.close()


@pytest.mark.postgres
@pytest.mark.parametrize("failure", ["sql", "handler"])
async def test_failed_cutover_rolls_back_gate_host_generation_ddl_and_ledger(old_041, tmp_path, monkeypatch, failure):
    old, new = old_041.old, old_041.new
    lease = await old.acquire_host("old", "released-before-failure", ttl_seconds=60)
    await old.release_host(lease)
    before = await state(old)
    root = resources(tmp_path / "failure")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    migration = root / "bot_migrations" / FENCE
    source = migration.read_text(encoding="utf-8") + "\nCREATE TABLE public.fence_probe(value integer);\n"
    if failure == "sql":
        source += "INSERT INTO public.fence_probe VALUES (1); DO $$ BEGIN RAISE EXCEPTION 'TEST_FENCE_FAILURE'; END $$;"
    else:
        body = "async def migrate_in(conn,store):\n    await conn.execute('INSERT INTO public.fence_probe VALUES (1)')\n    raise RuntimeError('TEST_FENCE_FAILURE')\n"
        (root / "data_migrations" / "fence_failure.py").write_text(body, encoding="utf-8")
        monkeypatch.setattr(bot_module, "MIGRATION_HANDLERS", {**bot_module.MIGRATION_HANDLERS, FENCE: "fence_failure"})
        actual_import = bot_module.import_module

        def load(name):
            if name != "forest.storage.data_migrations.fence_failure":
                return actual_import(name)
            module = ModuleType(name)
            exec(compile(body, "fence_failure.py", "exec"), module.__dict__)
            return module

        monkeypatch.setattr(bot_module, "import_module", load)
    migration.write_text(source, encoding="utf-8")
    with pytest.raises((psycopg.errors.RaiseException, RuntimeError), match="TEST_FENCE_FAILURE"):
        await new.migrate()
    assert await state(old) == before
    async with old.pool.connection() as conn:
        assert (await (await conn.execute("SELECT to_regclass('public.fence_probe') AS probe")).fetchone())["probe"] is None
    fresh = await old.acquire_host("old", "new-incarnation-after-rollback", ttl_seconds=60)
    assert (await old.renew_host(fresh)).owner_epoch == fresh.owner_epoch


@pytest.mark.postgres
async def test_checksum_rejections_never_retire_owner_or_raise_gate(old_041, tmp_path, monkeypatch):
    old, new = old_041.old, old_041.new
    lease = await old.acquire_host("old", "live-during-checksum-check", ttl_seconds=60)
    before = await state(old)
    root = resources(tmp_path / "tamper")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    old_sql = root / "bot_migrations" / "039_control_domains.sql"
    original = old_sql.read_text(encoding="utf-8")
    old_sql.write_text(original + "\n-- changed old SQL\n", encoding="utf-8")
    await error("MIGRATION_CHANGED", new.migrate())
    assert await state(old) == before
    old_sql.write_text(original, encoding="utf-8")
    await old.release_host(lease)
    await new.migrate()
    fresh = await new.acquire_host("new", "after-cutover", ttl_seconds=60)
    before = await state(new)
    helper = root / "migration_v2.py"
    helper.write_text(helper.read_text(encoding="utf-8") + "\n# changed released v2 helper\n", encoding="utf-8")
    await error("MIGRATION_CHANGED", new.migrate())
    assert await state(new) == before
    assert (await new.renew_host(fresh)).owner_epoch == fresh.owner_epoch


@pytest.mark.postgres
async def test_real_ddl_lock_timeout_rolls_back_the_writer_cutover(old_041, tmp_path, monkeypatch):
    old, new = old_041.old, old_041.new
    lease = await old.acquire_host("old", "before-ddl-timeout", ttl_seconds=60)
    await old.release_host(lease)
    before = await state(old)
    root = resources(tmp_path / "ddl-timeout")
    migration = root / "bot_migrations" / FENCE
    migration.write_text(migration.read_text(encoding="utf-8") +
                         "\nALTER TABLE public.bot_config_plans ADD COLUMN fence_probe integer;\n", encoding="utf-8")
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)
    async with old.pool.connection() as blocker, blocker.transaction():
        await blocker.execute("SELECT values FROM public.bot_config_plans")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            await new.migrate()
        assert await state(old) == before
        row = await (await blocker.execute(
            "SELECT count(*) AS n FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='bot_config_plans' AND column_name='fence_probe'"
        )).fetchone()
        assert row["n"] == 0
    fresh = await old.acquire_host("old", "after-ddl-timeout", ttl_seconds=60)
    assert (await old.renew_host(fresh)).owner_epoch == fresh.owner_epoch


async def wait_until_blocked(store):
    async with asyncio.timeout(2):
        while True:
            async with store.pool.connection() as conn:
                row = await (await conn.execute(
                    "SELECT count(*) AS n FROM pg_stat_activity WHERE datname=current_database() "
                    "AND wait_event_type='Lock' AND pid<>pg_backend_pid()"
                )).fetchone()
            if row["n"]:
                return
            await asyncio.sleep(0.01)


@pytest.mark.postgres
async def test_migration_winning_host_lock_fences_queued_old_acquisition(old_041, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    install = bot_module.install_writer_gate_v2_in

    async def hold_after_gate(conn):
        await install(conn)
        entered.set()
        await release.wait()

    monkeypatch.setattr(bot_module, "install_writer_gate_v2_in", hold_after_gate)
    migration = asyncio.create_task(old_041.new.migrate())
    acquisition = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        acquisition = asyncio.create_task(old_041.old.acquire_host("old", "queued-old", ttl_seconds=60))
        await wait_until_blocked(old_041.new)
        assert not acquisition.done()
        release.set()
        await migration
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"):
            await acquisition
        assert (await state(old_041.new))["gate"] == 2
    finally:
        release.set()
        await asyncio.gather(migration, *([acquisition] if acquisition else []), return_exceptions=True)


@pytest.mark.postgres
async def test_old_acquisition_winning_host_lock_prevents_cutover(old_041, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    old_now = old_041.old._now

    async def hold_old_now(conn):
        entered.set()
        await release.wait()
        return await old_now(conn)

    monkeypatch.setattr(old_041.old, "_now", hold_old_now)
    acquisition = asyncio.create_task(old_041.old.acquire_host("old", "first-old", ttl_seconds=60))
    migration = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        migration = asyncio.create_task(old_041.new.migrate())
        await wait_until_blocked(old_041.new)
        assert not migration.done()
        release.set()
        lease = await acquisition
        await error("MIGRATION_HOST_ACTIVE", migration)
        assert (await state(old_041.old))["gate"] == 1
        assert (await old_041.old.renew_host(lease)).owner_epoch == lease.owner_epoch
    finally:
        release.set()
        await asyncio.gather(acquisition, *([migration] if migration else []), return_exceptions=True)
