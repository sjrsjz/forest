"""Real PostgreSQL migration transactions, old writers, and erased legacy bodies.

The tiny 029 handler isolates the dispatcher/import protocol from the independent
input/context/Agent data conversions, which have their own upgrade fixtures.
"""

import hashlib
from importlib.resources import files
from types import ModuleType, SimpleNamespace

import psycopg
import pytest
import pytest_asyncio
from legacy_022 import FrozenPayloadStore
from psycopg.types.json import Jsonb
from test_bot_store import database as database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage import bot as bot_module
from forest.storage.bot import BotStore
from forest.storage.migration_v1 import legacy_source_ref
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres

PREFIX = """
from forest.storage.payloads import PayloadStore
from forest.memory_types import SourceRef
from forest.errors import ForestError
async def migrate_in(conn, store):
    payloads = PayloadStore(store)
"""


@pytest_asyncio.fixture
async def upgrade(database, tmp_path, monkeypatch, request):
    released = files("forest.storage")
    root = tmp_path / "resources"
    sql = root / "bot_migrations"
    sql.mkdir(parents=True)
    handlers = root / "data_migrations"
    handlers.mkdir()
    cutoff = getattr(request, "param", "028")
    for path in released.joinpath("bot_migrations").iterdir():
        if path.name.endswith(".sql") and path.name[:3] <= cutoff:
            (sql / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    helpers = root / "migration_v1.py"
    helpers.write_text(
        released.joinpath("migration_v1.py").read_text(encoding="utf-8"), encoding="utf-8"
    )
    monkeypatch.setattr(bot_module, "files", lambda namespace: root)

    def load(name):
        assert name == "forest.storage.data_migrations.input_payloads_v1"
        module = ModuleType(name)
        path = handlers / "input_payloads_v1.py"
        exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), module.__dict__)
        return module

    actual_import = bot_module.import_module
    monkeypatch.setattr(bot_module, "import_module", load)
    store = BotStore(database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        async with store.pool.connection() as conn:
            await conn.execute(
                "CREATE TABLE public.legacy_content_fixture(id text PRIMARY KEY, body jsonb)"
            )
            await conn.execute(
                "INSERT INTO public.legacy_content_fixture VALUES ('old',%s)",
                (Jsonb({"text": "原文 CANARY", "numeric": 1.25}),),
            )

        def install(body="    pass\n"):
            (sql / "029_input_payloads.sql").write_text(
                "CREATE TABLE public.migration_probe(id text PRIMARY KEY, payload_id text);",
                encoding="utf-8",
            )
            (handlers / "input_payloads_v1.py").write_text(PREFIX + body, encoding="utf-8")

        def include_current():
            for path in released.joinpath("bot_migrations").iterdir():
                if path.name.endswith(".sql"):
                    (sql / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            for path in released.joinpath("data_migrations").iterdir():
                if path.name.endswith(".py"):
                    (handlers / path.name).write_text(
                        path.read_text(encoding="utf-8"), encoding="utf-8"
                    )
            (root / "migration_v2.py").write_text(
                released.joinpath("migration_v2.py").read_text(encoding="utf-8"), encoding="utf-8"
            )
            monkeypatch.setattr(bot_module, "import_module", actual_import)

        yield SimpleNamespace(
            store=store,
            install=install,
            root=root,
            helpers=helpers,
            database=database,
            include_current=include_current,
        )
    finally:
        await store.close()


async def code(expected, operation):
    with pytest.raises(ForestError) as exc:
        await operation
    assert exc.value.code == expected


async def test_live_host_refused_then_drained_upgrade_fences_preexisting_old_connection(upgrade):
    store = upgrade.store
    lease = await store.acquire_host("old-host", "old-incarnation", ttl_seconds=60)
    # A connection created before the upgrade lacks the new static startup GUC.
    old = await psycopg.AsyncConnection.connect(upgrade.database, autocommit=True)
    try:
        upgrade.install()
        await code("MIGRATION_HOST_ACTIVE", store.migrate())
        async with store.pool.connection() as conn:
            assert (
                await (
                    await conn.execute("SELECT to_regclass('public.migration_probe') AS t")
                ).fetchone()
            )["t"] is None
            current = await (await conn.execute("SELECT * FROM public.bot_host_lease")).fetchone()
            assert current["owner_epoch"] == lease.owner_epoch
        # Migrations are an explicit stop/upgrade/start boundary.
        await store.release_host(lease)
        await store.migrate()
        for statement in (
            "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()+interval '1 minute'",
            "UPDATE public.bot_host_lease SET host_id='old-host',incarnation='old-return',owner_epoch=owner_epoch+1",
        ):
            with pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState, match="WRITER_SCHEMA_TOO_OLD"
            ):
                await old.execute(statement)
        fresh = await store.acquire_host("new-host", "new-incarnation", ttl_seconds=60)
        assert fresh.owner_epoch > lease.owner_epoch
        await code("STALE_HOST", store.renew_host(lease))
        # Idempotent migrate with no pending SQL must not retire a live new host.
        await store.migrate()
        assert (await store.renew_host(fresh)).owner_epoch == fresh.owner_epoch
    finally:
        await old.close()


async def test_hook_failure_rolls_back_ddl_import_retirement_and_ledger(upgrade):
    store = upgrade.store
    upgrade.install("""    row = await (await conn.execute('SELECT body FROM public.legacy_content_fixture')).fetchone()
    ref = await payloads.import_legacy_json_in(conn, owner_kind='context.parts', owner_id='old',
        value=row['body'], sources=(SourceRef('s'),))
    await conn.execute('INSERT INTO public.migration_probe VALUES (%s,%s)', ('old',ref.payload_id))
    await conn.execute('DELETE FROM public.legacy_content_fixture')
    raise ForestError('TEST_HOOK_FAILURE', 'abort conversion')
""")
    await code("TEST_HOOK_FAILURE", store.migrate())
    async with store.pool.connection() as conn:
        row = await (await conn.execute("SELECT owner_epoch FROM public.bot_host_lease")).fetchone()
        assert row["owner_epoch"] == 0
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.legacy_content_fixture")
            ).fetchone()
        )["n"] == 1
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_payload_headers")
            ).fetchone()
        )["n"] == 0
        assert (
            await (
                await conn.execute("SELECT to_regclass('public.bot_schema_writer_gate') AS t")
            ).fetchone()
        )["t"] is None
        assert (
            await (
                await conn.execute("SELECT to_regclass('public.migration_probe') AS t")
            ).fetchone()
        )["t"] is None
    handler = upgrade.root / "data_migrations" / "input_payloads_v1.py"
    handler.write_text(
        handler.read_text(encoding="utf-8").replace(
            "    raise ForestError('TEST_HOOK_FAILURE', 'abort conversion')", ""
        ),
        encoding="utf-8",
    )
    await store.migrate()
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT h.*,b.content FROM public.bot_payload_headers h JOIN public.bot_payload_bodies b USING(bot_id,payload_id)"
            )
        ).fetchone()
    assert bytes(row["content"]) == '{"numeric":1.25,"text":"原文 CANARY"}'.encode()
    assert row["payload_id"] == "payload:" + digest_json(["bot-a", "context.parts", "old"])


async def test_suppressed_imports_keep_stable_headers_and_never_recreate_bytes(upgrade):
    store = upgrade.store
    repo = FrozenPayloadStore(store)
    lease = await store.acquire_host("h", "i", ttl_seconds=60)
    original = await repo.put_json(
        lease,
        owner_kind="context.parts",
        owner_id="same",
        value={"text": "old"},
        sources=(SourceRef("s"),),
        expected_deletion_epoch=0,
    )
    await repo.forget_sources(lease, "erased", (SourceRef("s"),), expected_deletion_epoch=0)
    await repo.scrub(lease)
    await store.release_host(lease)
    upgrade.install("""    same = await payloads.import_legacy_json_in(conn, owner_kind='context.parts', owner_id='same',
        value={'text':'old'}, sources=(SourceRef('s'),))
    assert same.state == 'SCRUBBED'
    child = await payloads.import_legacy_json_in(conn, owner_kind='context.parts', owner_id='child',
        value={'text':'SUPPRESSED-CANARY'}, parents=(same.payload_id,))
    image = await payloads.import_legacy_bytes_in(conn, owner_kind='media.capture', owner_id='image',
        value=b'NEVER-RESTORE-PIXELS', sources=(SourceRef('s',99,'part:1'),))
    assert child.state == image.state == 'REVOKED'
    for ref in (same,child,image):
        await conn.execute('INSERT INTO public.migration_probe VALUES (%s,%s)', (ref.digest,ref.payload_id))
""")
    await store.migrate()
    await store.migrate()
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute("SELECT * FROM public.bot_payload_headers ORDER BY owner_id")
        ).fetchall()
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_payload_bodies")
            ).fetchone()
        )["n"] == 0
        assert {r["forget_operation_id"] for r in rows} == {"erased"}
        assert {r["payload_id"] for r in rows} >= {original.payload_id}
    fresh = await store.acquire_host("h2", "i2", ttl_seconds=60)
    # This synthetic import-only fixture stops at 029, before source-link SQL.
    assert (await FrozenPayloadStore(store).scrub(fresh))["removed"] == 2


async def test_migration_import_scope_rejects_normal_other_connection_and_inherited_task(upgrade):
    store = upgrade.store
    async with store.pool.connection() as conn, conn.transaction():
        await code(
            "MIGRATION_REQUIRED",
            PayloadStore(store).import_legacy_json_in(
                conn, owner_kind="context.parts", owner_id="illegal", value="secret"
            ),
        )
    upgrade.install("""    import asyncio
    async def denied(connection):
        try:
            await payloads.import_legacy_json_in(connection, owner_kind='context.parts',owner_id='forbidden',value='x')
        except ForestError as exc:
            assert exc.code == 'MIGRATION_REQUIRED'
        else:
            raise AssertionError('migration scope escaped')
    await asyncio.create_task(denied(conn))
    async with store.pool.connection() as other, other.transaction():
        await denied(other)
""")
    await store.migrate()
    async with store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_payload_headers")
            ).fetchone()
        )["n"] == 0


async def test_full_ledger_preflight_and_handler_helper_checksums_are_enforced(upgrade):
    store = upgrade.store
    upgrade.install()
    async with store.pool.connection() as conn:
        ledger = await (
            await conn.execute("SELECT * FROM public.forest_bot_schema_migrations")
        ).fetchall()
        for row in ledger:
            source = (upgrade.root / "bot_migrations" / row["version"]).read_text(encoding="utf-8")
            assert row["checksum"] == hashlib.sha256(source.encode()).hexdigest()
        await conn.execute(
            "UPDATE public.forest_bot_schema_migrations SET checksum='changed' WHERE version='028_agent_memory.sql'"
        )
    await code("MIGRATION_CHANGED", store.migrate())
    async with store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT to_regclass('public.migration_probe') AS t")
            ).fetchone()
        )["t"] is None
        await conn.execute(
            "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES ('999_future.sql','unknown')"
        )
    await code("SCHEMA_TOO_NEW", store.migrate())
    async with store.pool.connection() as conn:
        await conn.execute(
            "DELETE FROM public.forest_bot_schema_migrations WHERE version='999_future.sql'"
        )
        checksum = next(r["checksum"] for r in ledger if r["version"] == "028_agent_memory.sql")
        await conn.execute(
            "UPDATE public.forest_bot_schema_migrations SET checksum=%s WHERE version='028_agent_memory.sql'",
            (checksum,),
        )
    await store.migrate()
    original = upgrade.helpers.read_text(encoding="utf-8")
    upgrade.helpers.write_text(original + "\n# changed helper\n", encoding="utf-8")
    await code("MIGRATION_CHANGED", store.migrate())
    upgrade.helpers.write_text(original, encoding="utf-8")
    handler = upgrade.root / "data_migrations" / "input_payloads_v1.py"
    handler.write_text(
        handler.read_text(encoding="utf-8") + "\n# changed handler\n", encoding="utf-8"
    )
    await code("MIGRATION_CHANGED", store.migrate())


async def test_legacy_closure_is_explicit_fixed_on_first_decision_and_does_not_expand_old_replay(
    upgrade,
):
    store = upgrade.store
    # This case exercises the current ordinary-forget policy, not an old seed.
    upgrade.include_current()
    await store.migrate()
    repo = PayloadStore(store)
    lease = await store.acquire_host("h", "i", ttl_seconds=60)
    prior = await repo.forget_sources(
        lease, "pre-migration", (SourceRef("old-empty"),), expected_deletion_epoch=0
    )
    assert prior["legacy_conservative"] is False
    marker = legacy_source_ref(store.bot_id)
    derived = await repo.put_json(
        lease,
        owner_kind="context.parts",
        owner_id="unknown-old",
        value="unproved",
        sources=(marker,),
        expected_deletion_epoch=1,
    )
    descendant = await repo.put_json(
        lease,
        owner_kind="model.snapshot",
        owner_id="new-child",
        value="child",
        parents=(derived.payload_id,),
        expected_deletion_epoch=1,
    )
    independent = await repo.put_json(
        lease,
        owner_kind="memory.source",
        owner_id="independent",
        value="safe",
        sources=(SourceRef("another-group"),),
        expected_deletion_epoch=1,
    )
    # A receipt first decided before the marker existed retains its exact scope.
    assert prior == await repo.forget_sources(
        lease, "pre-migration", (SourceRef("old-empty"),), expected_deletion_epoch=0
    )
    assert await repo.read_json(lease, derived.payload_id, expected_deletion_epoch=1) == "unproved"
    result = await repo.forget_sources(
        lease, "after-migration", (SourceRef("requested"),), expected_deletion_epoch=1
    )
    assert result["legacy_conservative"] is True
    assert result == await repo.forget_sources(
        lease, "after-migration", (SourceRef("requested"),), expected_deletion_epoch=1
    )
    for ref in (derived, descendant):
        await code(
            "PAYLOAD_ERASED", repo.read_json(lease, ref.payload_id, expected_deletion_epoch=2)
        )
    assert await repo.read_json(lease, independent.payload_id, expected_deletion_epoch=2) == "safe"
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='after-migration'"
            )
        ).fetchone()
    assert row["targets"] == sorted(
        [
            {"source_unit_id": "requested", "part_id": "*"},
            {"source_unit_id": marker.source_unit_id, "part_id": "*"},
        ],
        key=lambda v: v["source_unit_id"],
    )
    assert row["input_digest"] == digest_json(
        [1, [{"source_unit_id": "requested", "part_id": "*"}]]
    )


async def test_expired_host_upgrade_and_ddl_lock_timeout_are_atomic(upgrade):
    store = upgrade.store
    lease = await store.acquire_host("expired", "expired-incarnation", ttl_seconds=0.01)
    upgrade.install()
    sql = upgrade.root / "bot_migrations" / "029_input_payloads.sql"
    sql.write_text(
        sql.read_text(encoding="utf-8")
        + "\nALTER TABLE public.legacy_content_fixture ADD COLUMN migrated boolean;",
        encoding="utf-8",
    )
    async with store.pool.connection() as blocker, blocker.transaction():
        await blocker.execute("SELECT * FROM public.legacy_content_fixture")
        await blocker.execute("SELECT pg_sleep(0.02)")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            await store.migrate()
        head = await (await blocker.execute("SELECT * FROM public.bot_host_lease")).fetchone()
        assert head["owner_epoch"] == lease.owner_epoch
        assert (
            await (
                await blocker.execute("SELECT to_regclass('public.bot_schema_writer_gate') AS t")
            ).fetchone()
        )["t"] is None
    await store.migrate()
    fresh = await store.acquire_host("new", "new-after-expiry", ttl_seconds=60)
    assert fresh.owner_epoch > lease.owner_epoch
    await code("STALE_HOST", store.renew_host(lease))


async def test_existing_incomplete_source_is_explicitly_revoked_and_scope_limit_is_not_truncated(
    upgrade,
):
    store = upgrade.store
    # Scope expansion is new application behavior and requires the full schema.
    upgrade.include_current()
    await store.migrate()
    repo = PayloadStore(store)
    lease = await store.acquire_host("h", "i", ttl_seconds=60)
    reply = await repo.put_json(
        lease,
        owner_kind="memory.source",
        owner_id="old-reply",
        value="legacy answer",
        sources=(SourceRef("reply-source"),),
        expected_deletion_epoch=0,
    )
    original = await repo.put_json(
        lease,
        owner_kind="memory.source",
        owner_id="independent-input",
        value="input",
        sources=(SourceRef("input-source"),),
        expected_deletion_epoch=0,
    )
    async with store.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO public.bot_memory_sources(bot_id,source_id,source_kind,visibility,platform,realm_id,channel_kind,channel_id,target_group_source_id) "
            "VALUES ('bot-a','group','GROUP','bot_shared','onebot','realm','group','123','group')"
        )
        for sequence, ref, unit, kind, complete in (
            (1, reply, "reply-source", "REPLY", False),
            (2, original, "input-source", "INPUT", True),
        ):
            await conn.execute(
                "INSERT INTO public.bot_memory_source_units(bot_id,source_unit_id,source_id,source_event_id,source_order_seq,order_origin,kind,principal_ref,identity_origin,payload_id,content_digest,lineage_complete,occurred_at) "
                "VALUES ('bot-a',%s,'group',%s,%s,'RECONSTRUCTED',%s,'principal','test',%s,%s,%s,clock_timestamp())",
                (unit, unit, sequence, kind, ref.payload_id, ref.digest, complete),
            )
    too_many = tuple(SourceRef(f"target:{n}") for n in range(4096))
    await code(
        "FORGET_SCOPE_LIMIT",
        repo.forget_sources(lease, "too-wide", too_many, expected_deletion_epoch=0),
    )
    assert await repo.epoch(lease) == 0
    result = await repo.forget_sources(
        lease, "forget-specific", (SourceRef("requested"),), expected_deletion_epoch=0
    )
    assert result["legacy_conservative"] is True
    assert result == await repo.forget_sources(
        lease, "forget-specific", (SourceRef("requested"),), expected_deletion_epoch=0
    )
    await code("PAYLOAD_ERASED", repo.read_json(lease, reply.payload_id, expected_deletion_epoch=1))
    assert await repo.read_json(lease, original.payload_id, expected_deletion_epoch=1) == "input"
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT targets FROM public.bot_payload_forget_requests WHERE operation_id='forget-specific'"
            )
        ).fetchone()
    assert {t["source_unit_id"] for t in row["targets"]} == {
        "requested",
        "reply-source",
        legacy_source_ref(store.bot_id).source_unit_id,
    }


async def test_forget_before_first_legacy_marker_never_imports_unknown_derivation_body(upgrade):
    store = upgrade.store
    raw = FrozenPayloadStore(store)
    lease = await store.acquire_host("old", "old-host", ttl_seconds=60)
    before = await raw.forget_sources(
        lease,
        "pre-cutover-delete",
        (SourceRef("forgotten-before-cutover", part_id="part:1"),),
        expected_deletion_epoch=0,
    )
    assert not before.get("legacy_conservative", False)
    async with store.pool.connection() as conn:
        original_request = await (
            await conn.execute("SELECT * FROM public.bot_payload_forget_requests")
        ).fetchone()
    await store.release_host(lease)
    upgrade.install("""    from forest.storage.migration_v1 import legacy_source_ref
    for kind, owner, value in (('reply.parts','reply','UNPROVED-REPLY-CANARY'),('model.snapshot','agent','UNPROVED-AGENT-CANARY')):
        ref = await payloads.import_legacy_json_in(conn,owner_kind=kind,owner_id=owner,value=value,sources=(legacy_source_ref(store.bot_id),))
        again = await payloads.import_legacy_json_in(conn,owner_kind=kind,owner_id=owner,value=value,sources=(legacy_source_ref(store.bot_id),))
        assert ref == again and ref.state == 'REVOKED'
        await conn.execute('INSERT INTO public.migration_probe VALUES (%s,%s)', (owner,ref.payload_id))
""")
    await store.migrate()
    async with store.pool.connection() as conn:
        rows = await (await conn.execute("SELECT * FROM public.bot_payload_headers")).fetchall()
        assert len(rows) == 2 and {r["forget_operation_id"] for r in rows} == {"pre-cutover-delete"}
        assert not await (await conn.execute("SELECT 1 FROM public.bot_payload_bodies")).fetchall()
        original = await (
            await conn.execute("SELECT targets FROM public.bot_payload_forget_requests")
        ).fetchone()
        assert original["targets"] == [
            {"source_unit_id": "forgotten-before-cutover", "part_id": "part:1"}
        ]
        for row in rows:
            assert {r["source_unit_id"] for r in row["manifest"]["sources"]} == {
                "forgotten-before-cutover",
                legacy_source_ref(store.bot_id).source_unit_id,
            }
    # This synthetic dispatcher fixture stops at 029; inspect immutable old
    # facts directly instead of invoking a current-schema writer on that subset.
    async with store.pool.connection() as conn:
        assert (
            original_request
            == await (
                await conn.execute("SELECT * FROM public.bot_payload_forget_requests")
            ).fetchone()
        )


@pytest.mark.parametrize("upgrade", ["022"], indirect=True)
@pytest.mark.parametrize("already_forgotten,incomplete_count", [(False, 1), (True, 1), (True, 257)])
async def test_actual_022_incomplete_source_reconciliation_is_explicit_and_preserves_other_input(
    upgrade, already_forgotten, incomplete_count
):
    store = upgrade.store
    raw = FrozenPayloadStore(store)
    lease = await store.acquire_host("old", "old-source-host", ttl_seconds=300)
    refs = {}
    names = ["a", "b", "reply", *(f"reply:{i:04}" for i in range(1, incomplete_count))]
    for identifier in names:
        refs[identifier] = await raw.put_json(
            lease,
            owner_kind="memory.source",
            owner_id=identifier,
            value={"text": "OLD-" + identifier},
            sources=(SourceRef(identifier),),
            expected_deletion_epoch=0,
        )
    artifact = await raw.put_json(
        lease,
        owner_kind="memory.artifact",
        owner_id="derived",
        value="OLD-INCOMPLETE-DERIVED",
        parents=(refs["reply"].payload_id,),
        expected_deletion_epoch=0,
    )
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO public.bot_memory_sources(bot_id,source_id,source_kind,visibility,platform,realm_id,channel_kind,channel_id,target_group_source_id) "
            "VALUES ('bot-a','legacy-group','GROUP','bot_shared','onebot','realm','group','123','legacy-group')"
        )
        for sequence, identifier in enumerate(names, 1):
            ref = refs[identifier]
            await conn.execute(
                "INSERT INTO public.bot_memory_source_units(bot_id,source_unit_id,source_id,source_event_id,source_order_seq,order_origin,kind,principal_ref,identity_origin,payload_id,content_digest,lineage_complete,occurred_at) "
                "VALUES ('bot-a',%s,'legacy-group',%s,%s,'RECONSTRUCTED',%s,'old-principal','test',%s,%s,%s,clock_timestamp())",
                (
                    identifier,
                    identifier,
                    sequence,
                    "REPLY" if identifier.startswith("reply") else "INPUT",
                    ref.payload_id,
                    ref.digest,
                    not identifier.startswith("reply"),
                ),
            )
    if already_forgotten:
        # The released implementation has no conservative derived-source
        # expansion; this is the actual old API, not a hand-built approximation.
        old_receipt = await raw.forget_sources(
            lease, "old-original", (SourceRef("a"),), expected_deletion_epoch=0
        )
    async with store.pool.connection() as conn:
        original = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
            )
        ).fetchall()
        assert (
            await (
                await conn.execute(
                    "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
                    (refs["reply"].payload_id,),
                )
            ).fetchone()
        )["state"] == "ACTIVE"
    await store.release_host(lease)
    upgrade.include_current()
    await store.migrate()
    raw = PayloadStore(store)
    new = await store.acquire_host("new", "new-source-host", ttl_seconds=60)
    epoch = await raw.epoch(new)
    async with store.pool.connection() as conn:
        before_repeat = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
            )
        ).fetchall()
        audit = await (
            await conn.execute(
                "SELECT * FROM public.bot_audit_records WHERE action='memory.legacy_suppression_reconciled'"
            )
        ).fetchall()
        current_original = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='old-original'"
            )
        ).fetchall()
    assert current_original == original
    if already_forgotten:
        replayed = await raw.forget_sources(
            new, "old-original", (SourceRef("a"),), expected_deletion_epoch=0
        )
        assert {key: replayed[key] for key in old_receipt} == old_receipt
        assert replayed["legacy_conservative"] is False
        batches = (incomplete_count + 255) // 256
        assert epoch == 1 + batches and len(audit) == batches
        assert audit[0]["details"]["preexisting_operation_id"] == "old-original"
        assert sum(row["details"]["source_count"] for row in audit) == incomplete_count
        assert len(before_repeat) == 1 + batches
        for ref in (refs["reply"], artifact):
            await code(
                "PAYLOAD_ERASED", raw.read_json(new, ref.payload_id, expected_deletion_epoch=epoch)
            )
        while (await raw.scrub(new))["remaining"]:
            pass
        async with store.pool.connection() as conn:
            states = await (
                await conn.execute(
                    "SELECT h.state FROM public.bot_memory_source_units u JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                    "WHERE NOT u.lineage_complete"
                )
            ).fetchall()
            assert len(states) == incomplete_count and {r["state"] for r in states} == {"SCRUBBED"}
        # A later ordinary deletion does not keep expanding already-revoked
        # incomplete sources. No payload in this fixture uses the marker.
        later = await raw.forget_sources(
            new, "later", (SourceRef("unrelated"),), expected_deletion_epoch=epoch
        )
        assert later["legacy_conservative"] is False
        async with store.pool.connection() as conn:
            decision = await (
                await conn.execute(
                    "SELECT targets FROM public.bot_payload_forget_requests WHERE operation_id='later'"
                )
            ).fetchone()
            assert {v["source_unit_id"] for v in decision["targets"]} == {"unrelated"}
        epoch += 1
    else:
        assert epoch == 0 and audit == [] and before_repeat == []
        assert await raw.read_json(new, refs["reply"].payload_id, expected_deletion_epoch=0) == {
            "text": "OLD-reply"
        }
    assert await raw.read_json(new, refs["b"].payload_id, expected_deletion_epoch=epoch) == {
        "text": "OLD-b"
    }
    async with store.pool.connection() as conn:
        decisions = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
            )
        ).fetchall()
    await store.migrate()
    async with store.pool.connection() as conn:
        assert (
            decisions
            == await (
                await conn.execute(
                    "SELECT * FROM public.bot_payload_forget_requests ORDER BY operation_id"
                )
            ).fetchall()
        )
