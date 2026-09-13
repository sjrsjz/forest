"""Real nonempty released 038 databases upgrade without invented request inputs."""

import hashlib
import json
from importlib import import_module
from importlib.resources import files

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_bot_store import database as database
from test_bot_store import store as store
from test_control_payloads import content as content
from test_control_payloads import input_body, result_body
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import MIGRATION_HANDLERS, BotStore, _migration_checksum
from forest.storage.control_payloads import ControlPayloadStore
from forest.storage.migration_v1 import _migration_context, install_writer_gate_in
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres

# Computed once from every actual 1976e01 SQL/handler/helper blob through 038.
# Test execution needs no Git or network and never synthesizes an OldWriter.
RELEASED_038_CHAIN = "f977e9b46c7e8ffc243ee38856cb38c6ccc39185f9866ddd81328b3c1b381192"
LONG = "legacy-" + "长" * 1800
CANARY = {"values": {"label": "CONTROL-OLD-CANARY", "multiplier": 7}}


async def schema_038(store):
    root = files("forest.storage").joinpath("bot_migrations")
    plan = []
    for path in sorted(root.iterdir(), key=lambda p: p.name):
        if path.name.endswith(".sql") and path.name[:3] <= "038":
            source = path.read_text(encoding="utf-8")
            plan.append((path.name, source, _migration_checksum(path.name, source)))
    assert len(plan) == 38
    identity = json.dumps([(p[0], p[2]) for p in plan], separators=(",", ":"))
    assert hashlib.sha256(identity.encode()).hexdigest() == RELEASED_038_CHAIN
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute("CREATE TABLE public.forest_bot_schema_migrations(version text PRIMARY KEY,"
                           "checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
        gated = False
        for version, source, checksum in plan:
            handler = MIGRATION_HANDLERS.get(version)
            if handler and not gated:
                await install_writer_gate_in(conn)
                gated = True
            await conn.execute(source)
            if handler:
                module = import_module("forest.storage.data_migrations." + handler)
                with _migration_context(conn, store, version):
                    await module.migrate_in(conn, store)
            await conn.execute("INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                               (version, checksum))
        await conn.execute("INSERT INTO public.forest_bot_identity(bot_id) VALUES ('bot-a')")
        await conn.execute("INSERT INTO public.bot_runtime_state(bot_id) VALUES ('bot-a')")
        await conn.execute("INSERT INTO public.bot_host_lease(bot_id) VALUES ('bot-a')")


async def read_rows(store, query, args=()):
    async with store.pool.connection() as conn:
        return await (await conn.execute(query, args)).fetchall()


async def seed(store):
    values = {f"legacy-{index:03}": {"index": index} for index in range(66)}
    values.update({"legacy-null": None, "legacy-false": False, "legacy-empty": "", LONG: CANARY})
    async with store.pool.connection() as conn, conn.transaction():
        for operation, value in values.items():
            await conn.execute(
                "INSERT INTO public.bot_web_operations(bot_id,operation_id,action,input_digest,subject_id,"
                "issuer_id,ticket_basis,result,created_at) VALUES ('bot-a',%s,'config.plan',%s,'admin',"
                "'issuer',%s,%s,'2025-01-02T03:04:05Z')",
                (operation, digest_json(["unknown-original-input", operation]),
                 Jsonb({"operation_id": operation, "key_revision": 7, "policy_revision": 4}), Jsonb(value)),
            )
        await conn.execute(
            "INSERT INTO public.bot_config_plans(bot_id,plan_id,component_id,subject_id,issuer_id,"
            "expected_revision,values,plan_digest,expires_at) VALUES ('bot-a','old-plan','counter','admin',"
            "'issuer',0,%s,'old-plan-digest','2025-01-03T00:00:00Z')", (Jsonb(CANARY),),
        )
        await conn.execute(
            "INSERT INTO public.bot_config_revisions(bot_id,component_id,revision,previous_revision,"
            "values,policy_revision,plan_id,operation_id) VALUES ('bot-a','counter',1,0,%s,4,'old-plan',%s)",
            (Jsonb(CANARY), LONG),
        )
        await conn.execute("INSERT INTO public.bot_audit_records(bot_id,audit_id,action,operation_id,details) "
                           "VALUES ('bot-a','old-audit','config.plan',%s,%s)", (LONG, Jsonb(CANARY)))
        await conn.execute("INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload) "
                           "VALUES ('bot-a','old-outbox','config.plan',%s,%s)", (LONG, Jsonb(CANARY)))
    return values, await read_rows(store, "SELECT * FROM public.bot_web_operations ORDER BY operation_id")


async def hydrate(store, row, *, epoch, required=True):
    async with store.pool.connection() as conn, conn.transaction():
        return await ControlPayloadStore(store).hydrate_result_in(conn, row, epoch=epoch, required=required)


@pytest.mark.parametrize("forgotten_before", [False, True])
async def test_nonempty_038_preserves_facts_json_null_and_legacy_scope(upgrade_database, forgotten_before):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await schema_038(store)
        values, originals = await seed(store)
        if forgotten_before:
            lease = await store.acquire_host("old-host", "old-incarnation", ttl_seconds=120)
            payloads = PayloadStore(store)
            await payloads.put_json(lease, owner_kind="memory.source", owner_id="old-source", value="gone",
                                    sources=(SourceRef("forgotten-first"),), expected_deletion_epoch=0)
            await payloads.forget_sources(lease, "old-forget", (SourceRef("forgotten-first"),), expected_deletion_epoch=0)
            await store.release_host(lease)
        await store.migrate()
        migrated = await read_rows(store, "SELECT * FROM public.bot_web_operations ORDER BY operation_id")
        assert len(migrated) == 70
        for before, after in zip(originals, migrated, strict=True):
            assert {key: after[key] for key in before if key != "result"} == {
                key: value for key, value in before.items() if key != "result"
            }
            assert after["result"] is None and after["input_legacy_unknown"]
            assert after["input_payload_id"] is None and after["input_payload_digest"] is None
            assert after["result_payload_id"] and after["result_payload_digest"]
            hydrated = await hydrate(store, after, epoch=int(forgotten_before), required=False)
            assert hydrated["result_payload_state"] == ("UNAVAILABLE" if forgotten_before else "ACTIVE")
            assert hydrated["result"] == (None if forgotten_before else values[after["operation_id"]])
        target = migrated[0]
        for changes in ({"action": "config.apply"}, {"operation_id": "not-a-migrated-operation"},
                        {"result_payload_digest": "sha256:" + "0" * 64}):
            with pytest.raises(ForestError) as exc:
                await hydrate(store, {**target, **changes}, epoch=int(forgotten_before), required=False)
            assert exc.value.code == "CONTROL_PAYLOAD_IDENTITY_CONFLICT"
        with pytest.raises(ForestError) as exc:
            async with store.pool.connection() as conn, conn.transaction():
                await ControlPayloadStore(store).put_result_in(
                    conn, target["operation_id"], values[target["operation_id"]],
                    action=target["action"], input_payload_id=None, epoch=int(forgotten_before),
                )
        assert exc.value.code == "CONTROL_PAYLOAD_MISSING"
        with pytest.raises(psycopg.Error):
            async with store.pool.connection() as conn:
                await conn.execute("UPDATE public.bot_web_operations SET input_legacy_unknown=false "
                                   "WHERE operation_id=%s", (target["operation_id"],))
        assert not await read_rows(store, "SELECT * FROM public.bot_payload_domains WHERE kind='CONTROL_INPUT'")
        # Audit/outbox/config remain explicit later-family work, not silently
        # claimed erased by this one operation-result migration.
        assert (await read_rows(store, "SELECT details FROM public.bot_audit_records WHERE audit_id='old-audit'"))[0]["details"] == CANARY
        assert (await read_rows(store, "SELECT payload FROM public.bot_outbox_events WHERE event_id='old-outbox'"))[0]["payload"] == CANARY
        assert (await read_rows(store, "SELECT values FROM public.bot_config_revisions"))[0]["values"] == CANARY
        headers_before = await read_rows(store, "SELECT * FROM public.bot_payload_headers ORDER BY payload_id")
        ledger_before = await read_rows(store, "SELECT * FROM public.forest_bot_schema_migrations ORDER BY version")
        await store.close()
        store = BotStore(upgrade_database, "bot-a")
        await store.open()
        await store.migrate()
        assert await read_rows(store, "SELECT * FROM public.bot_payload_headers ORDER BY payload_id") == headers_before
        assert await read_rows(store, "SELECT * FROM public.forest_bot_schema_migrations ORDER BY version") == ledger_before
        if not forgotten_before:
            lease = await store.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
            payloads = PayloadStore(store)
            forgotten = await payloads.forget_sources(lease, "forget-after", (SourceRef("some-chat"),), expected_deletion_epoch=0)
            assert forgotten["legacy_conservative"]
            removed = 0
            while True:
                batch = await payloads.scrub(lease, limit=32)
                removed += batch["removed"]
                if not batch["removed"]:
                    break
            # 041 adds two explicit conservative configuration bridges to
            # the 70 immutable operation results from this 038 fixture.
            assert removed == len(headers_before)
            assert sum(row["owner_kind"] == "control.result" for row in headers_before) == 70
            await store.release_host(lease)
        target = migrated[0]
        with pytest.raises(ForestError) as exc:
            await hydrate(store, target, epoch=1)
        assert exc.value.code == "CONTROL_CONTENT_REVOKED"
        assert (await hydrate(store, target, epoch=1, required=False))["result_payload_digest"] == target["result_payload_digest"]
        bodies = await read_rows(store, "SELECT b.content FROM public.bot_payload_headers h "
                                 "JOIN public.bot_payload_bodies b USING(bot_id,payload_id) WHERE h.owner_kind='control.result'")
        assert not bodies
    finally:
        await store.close()


async def test_040_payload_conflict_rolls_back_both_schema_and_earlier_batch(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await schema_038(store)
        _, originals = await seed(store)
        lease = await store.acquire_host("host", "incarnation", ttl_seconds=120)
        collision = await PayloadStore(store).put_json(
            lease, owner_kind="control.result", owner_id="result:" + digest_json([LONG]),
            value={"conflicting": "existing-owner"}, sources=(SourceRef("independent"),), expected_deletion_epoch=0,
        )
        await store.release_host(lease)
        with pytest.raises(ForestError) as exc:
            await store.migrate()
        assert exc.value.code == "PAYLOAD_CONFLICT"
        assert await read_rows(store, "SELECT * FROM public.bot_web_operations ORDER BY operation_id") == originals
        assert len(await read_rows(store, "SELECT * FROM public.forest_bot_schema_migrations")) == 38
        assert (await read_rows(store, "SELECT payload_id FROM public.bot_payload_headers")) == [{"payload_id": collision.payload_id}]
        assert not await read_rows(store, "SELECT 1 FROM information_schema.columns WHERE table_name='bot_web_operations' "
                                   "AND column_name='result_payload_id'")
    finally:
        await store.close()


async def insert_current(content, original, result, *, operation="preview", action="config.plan", legacy=False):
    repo, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        return await (await conn.execute(
            "INSERT INTO public.bot_web_operations(bot_id,operation_id,action,input_digest,subject_id,issuer_id,"
            "ticket_basis,result,input_payload_id,input_payload_digest,input_legacy_unknown,"
            "result_payload_id,result_payload_digest) VALUES (%s,%s,%s,'unchanged-semantic-digest','admin',"
            "'issuer','{}',NULL,%s,%s,%s,%s,%s) RETURNING *",
            (repo.store.bot_id, operation, action, original.payload_id if original else None,
             original.digest if original else None, legacy, result.payload_id, result.digest),
        )).fetchone()


@pytest.mark.parametrize("operation", ["preview", '控制/引号"反斜\\用户🙂'])
async def test_new_sql_operation_accepts_real_input_but_cannot_claim_legacy(content, operation):
    original = await input_body(content, operation, value={"normalized": True})
    result = await result_body(content, original, operation=operation)
    row = await insert_current(content, original, result, operation=operation)
    assert row["result"] is None and row["input_legacy_unknown"] is False
    loaded = await hydrate(content[0].store, row, epoch=0)
    assert loaded["result"] is None and loaded["result_payload_state"] == "ACTIVE"
    with pytest.raises(psycopg.errors.CheckViolation, match="CONTROL_INPUT_LEGACY_FORBIDDEN"):
        await insert_current(content, None, result, operation="invented-legacy", legacy=True)
    for changes in ({"input_legacy_unknown": True}, {"operation_id": "other", "input_legacy_unknown": True}):
        with pytest.raises(ForestError) as exc:
            await hydrate(content[0].store, {**row, **changes}, epoch=0)
        assert exc.value.code == "CONTROL_PAYLOAD_IDENTITY_CONFLICT"
    for statement in (
        "UPDATE public.bot_web_operations SET input_legacy_unknown=true,input_payload_id=NULL,input_payload_digest=NULL",
        "UPDATE public.bot_web_operations SET result_payload_digest='sha256:" + "0" * 64 + "'",
        "DELETE FROM public.bot_web_operations",
    ):
        with pytest.raises(psycopg.Error):
            async with content[0].store.pool.connection() as conn:
                await conn.execute(statement)


@pytest.mark.parametrize("case", ["wrong_action", "wrong_input", "missing_parent", "erased"])
async def test_new_sql_result_reference_and_source_guard(content, case):
    repo, lease = content
    original = await input_body(content)
    result = await result_body(content, original)
    if case == "wrong_action":
        attempt = insert_current(content, original, result, action="config.apply")
    elif case == "wrong_input":
        other = await input_body(content, "other")
        attempt = insert_current(content, other, result)
    elif case == "missing_parent":
        isolated_input = await input_body(content, "isolated")
        orphan = await repo.payloads.put_json(
            lease, owner_kind="control.result", owner_id=repo._owner("result", "isolated"),
            value={"action": "config.plan", "operation_id": "isolated", "value": None},
            expected_deletion_epoch=0,
        )
        attempt = insert_current(content, isolated_input, orphan, operation="isolated")
    else:
        source = (await read_rows(repo.store, "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='CONTROL_INPUT'"))[0]
        await repo.payloads.forget_sources(lease, "erase-input", (SourceRef(source["source_unit_id"]),), expected_deletion_epoch=0)
        attempt = insert_current(content, original, result)
    with pytest.raises(psycopg.Error, match="CONTROL_(PAYLOAD_IDENTITY_CONFLICT|CONTENT_REVOKED)"):
        await attempt
    assert not await read_rows(repo.store, "SELECT * FROM public.bot_web_operations")
