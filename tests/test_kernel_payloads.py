"""Real 001--035 data upgrades and Kernel content reference/deletion invariants."""

import hashlib
import json
from importlib import import_module
from importlib.resources import files

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import MIGRATION_HANDLERS, BotStore, _digest, _migration_checksum
from forest.storage.kernel_payloads import KernelPayloadStore
from forest.storage.migration_v1 import (
    _migration_context,
    install_writer_gate_in,
    legacy_source_ref,
)

pytestmark = pytest.mark.postgres

# Hash of the sorted (filename, migration checksum) pairs from actual HEAD
# released 001--035 Git blobs. No test-time Git dependency or synthetic OldWriter.
RELEASED_CHAIN = "139d893c1867b5d4ae83d0962cba9822ac87ad1af8e8e14d46c9857f75404982"
CALL = "legacy-call-" + "c" * 300
OP = "legacy-operation-" + "o" * 300
MIGRATION = "legacy-migration-" + "m" * 300
VALUE = {"message": "KERNEL-LEGACY-CANARY", "image": {"description": "old pixels"}}


async def old_schema(store):
    root = files("forest.storage").joinpath("bot_migrations")
    plan = []
    for path in sorted(root.iterdir(), key=lambda p: p.name):
        if path.name.endswith(".sql") and path.name[:3] <= "035":
            source = path.read_text(encoding="utf8")
            plan.append((path.name, source, _migration_checksum(path.name, source)))
    assert len(plan) == 35
    identity = json.dumps([(p[0], p[2]) for p in plan], separators=(",", ":"))
    assert hashlib.sha256(identity.encode()).hexdigest() == RELEASED_CHAIN
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "CREATE TABLE public.forest_bot_schema_migrations(version text PRIMARY KEY,"
            "checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
        )
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
            await conn.execute(
                "INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                (version, checksum),
            )
        await conn.execute("INSERT INTO public.forest_bot_identity(bot_id) VALUES ('bot-a')")
        await conn.execute("INSERT INTO public.bot_runtime_state(bot_id) VALUES ('bot-a')")
        await conn.execute("INSERT INTO public.bot_host_lease(bot_id) VALUES ('bot-a')")


async def seed(store, *, conflict=False):
    """Use the real old SQL contract, including JSONB null from Jsonb(None)."""
    source_rows = {"data": {"revision": 1, "value": VALUE, "deleted": False}}
    result_values = {"data": VALUE, "null": None}
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO public.bot_generations(bot_id,generation_id,component_id,package_digest,"
            "config_digest,config,instance_id,worker_incarnation,host_incarnation,host_owner_epoch,status) "
            "VALUES ('bot-a','old-gen','counter','package',%s,'{}','instance','worker','host',1,'ACTIVE')",
            (_digest({}),),
        )
        await conn.execute(
            "INSERT INTO public.bot_component_routes(bot_id,component_id,generation_id,route_epoch,admission) "
            "VALUES ('bot-a','counter','old-gen',1,true)"
        )
        for call, op, status, value in (
            (CALL, OP, "COMPLETED", VALUE),
            ("null-call", "null-op", "COMPLETED", None),
            ("pending", "pending-op", "ADMITTED", None),
        ):
            await conn.execute(
                "INSERT INTO public.bot_calls(bot_id,call_id,operation_id,component_id,generation_id,route_epoch,"
                "worker_incarnation,host_id,host_incarnation,owner_epoch,storage_generation,maintenance_epoch,"
                "policy_revision,deadline_at,input_digest,status,result,root_call_id) "
                "VALUES ('bot-a',%s,%s,'counter','old-gen',1,'worker','host-id','host',1,1,1,1,"
                "clock_timestamp()+interval '1 hour',%s,%s,%s,%s)",
                (
                    call,
                    op,
                    _digest({"input": call}),
                    status,
                    Jsonb(value) if status == "COMPLETED" else None,
                    call,
                ),
            )
        for key, value in (("data", VALUE), ("null", None)):
            operation = "state-" + key
            receipt = {
                "operation_id": operation,
                "call_operation_id": OP,
                "component_id": "counter",
                "revision": 1,
                "value": value,
            }
            await conn.execute(
                "INSERT INTO public.bot_state_steps(bot_id,call_id,operation_id,input_digest,payload_digest,receipt) "
                "VALUES ('bot-a',%s,%s,%s,%s,%s)",
                (CALL, operation, "input-" + key, _digest([key, 0, value]), Jsonb(receipt)),
            )
            await conn.execute(
                "INSERT INTO public.bot_state_revisions(bot_id,component_id,key,revision,previous_revision,value,call_id,operation_id) "
                "VALUES ('bot-a','counter',%s,1,0,%s,%s,%s)",
                (key, Jsonb(value), CALL, operation),
            )
            await conn.execute(
                "INSERT INTO public.bot_component_state(bot_id,component_id,key,revision,value) "
                "VALUES ('bot-a','counter',%s,1,%s)",
                (key, Jsonb("conflicting" if conflict and key == "data" else value)),
            )
        await conn.execute(
            "INSERT INTO public.bot_component_state(bot_id,component_id,key,revision,value,deleted) VALUES "
            "('bot-a','counter','absent',0,NULL,false),('bot-a','counter','tombstone',2,'null',true)"
        )
        await conn.execute(
            "INSERT INTO public.bot_lifecycle_operations(bot_id,operation_id,component_id,input_digest,candidate_generation_id,"
            "old_generation_id,expected_route_epoch,phase,host_incarnation,host_owner_epoch,op_epoch,lease_until) "
            "VALUES ('bot-a',%s,'counter','plan','old-gen','old-gen',1,'FROZEN','host',1,1,clock_timestamp())",
            (MIGRATION,),
        )
        await conn.execute(
            "INSERT INTO public.bot_state_migration_sources(bot_id,operation_id,component_id,source_generation_id,"
            "source_route_epoch,source_schema,source_revision,source_digest,source_rows,target_schema) "
            "VALUES ('bot-a',%s,'counter','old-gen',1,1,2,%s,%s,2)",
            (MIGRATION, _digest(source_rows), Jsonb(source_rows)),
        )
        await conn.execute(
            "INSERT INTO public.bot_state_migration_results(bot_id,operation_id,source_digest,result_digest,result_values) "
            "VALUES ('bot-a',%s,%s,%s,%s)",
            (MIGRATION, _digest(source_rows), _digest(result_values), Jsonb(result_values)),
        )
    return source_rows, result_values


async def select(conn, table, where="", args=()):
    # Only fixed test-owned identifiers are accepted here.
    assert table in {
        "bot_calls",
        "bot_component_state",
        "bot_state_revisions",
        "bot_state_steps",
        "bot_state_migration_sources",
        "bot_state_migration_results",
    }
    return await (
        await conn.execute("SELECT * FROM public." + table + " WHERE bot_id='bot-a' " + where, args)
    ).fetchall()


async def test_nonempty_035_upgrade_preserves_json_null_receipts_and_digests(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await old_schema(store)
        source_rows, result_values = await seed(store)
        await store.migrate()
        helper = KernelPayloadStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            clock = await helper.payloads.lock_in(conn)
            epoch = clock["deletion_epoch"]
            calls = {r["call_id"]: r for r in await select(conn, "bot_calls")}
            assert all(
                row["result"] is None
                and row["input_legacy_unknown"]
                and row["input_payload_id"] is None
                for row in calls.values()
            )
            for call, value in ((CALL, VALUE), ("null-call", None)):
                hydrated = await helper.hydrate_call_in(conn, calls[call], epoch=epoch)
                assert hydrated["status"] == "COMPLETED" and hydrated["payload_state"] == "ACTIVE"
                assert hydrated["result"] == value
                assert hydrated["result_digest"] == digest_json(value)
                assert hydrated["input_digest"] == _digest({"input": call})
                assert hydrated["root_call_id"] == call and hydrated["result_payload_id"]
            pending = await helper.hydrate_call_in(conn, calls["pending"], epoch=epoch)
            assert pending["payload_state"] == "ABSENT" and pending["result_digest"] is None
            current = {r["key"]: r for r in await select(conn, "bot_component_state")}
            history = {r["key"]: r for r in await select(conn, "bot_state_revisions")}
            for key, value in (("data", VALUE), ("null", None)):
                assert current[key]["value"] is None and history[key]["value"] is None
                assert current[key]["value_payload_id"] == history[key]["value_payload_id"]
                assert (await helper.hydrate_state_in(conn, current[key], epoch=epoch))[
                    "value"
                ] == value
                step = (
                    await select(conn, "bot_state_steps", "AND operation_id=%s", ("state-" + key,))
                )[0]
                assert (
                    step["receipt"] is None
                    and step["value_payload_id"] == current[key]["value_payload_id"]
                )
                receipt = (await helper.hydrate_step_in(conn, step, epoch=epoch))["receipt"]
                assert receipt == {
                    "operation_id": "state-" + key,
                    "call_operation_id": OP,
                    "component_id": "counter",
                    "revision": 1,
                    "value": value,
                }
                assert step["payload_digest"] == _digest([key, 0, value])
            assert (await helper.hydrate_state_in(conn, current["absent"], epoch=epoch))[
                "payload_state"
            ] == "ABSENT"
            assert (await helper.hydrate_state_in(conn, current["tombstone"], epoch=epoch))[
                "payload_state"
            ] == "TOMBSTONE"
            source = (await select(conn, "bot_state_migration_sources"))[0]
            result = (await select(conn, "bot_state_migration_results"))[0]
            assert source["source_rows"] is None and result["result_values"] is None
            assert source["source_digest"] == result["source_digest"] == _digest(source_rows)
            assert result["result_digest"] == _digest(result_values)
            assert (await helper.hydrate_migration_source_in(conn, source, epoch=epoch))[
                "source_rows"
            ] == source_rows
            assert (await helper.hydrate_migration_result_in(conn, result, epoch=epoch))[
                "result_values"
            ] == result_values
            owners = await (
                await conn.execute(
                    "SELECT owner_id FROM public.bot_payload_headers WHERE owner_kind IN ('kernel.result','state.value','state.migration')"
                )
            ).fetchall()
            assert max(len(r["owner_id"]) for r in owners) < 128
            before = await (
                await conn.execute(
                    "SELECT payload_id,digest,manifest FROM public.bot_payload_headers ORDER BY payload_id"
                )
            ).fetchall()
        await store.migrate()
        async with store.pool.connection() as conn:
            assert (
                before
                == await (
                    await conn.execute(
                        "SELECT payload_id,digest,manifest FROM public.bot_payload_headers ORDER BY payload_id"
                    )
                ).fetchall()
            )
    finally:
        await store.close()


@pytest.mark.parametrize("forget_before_upgrade", [False, True])
async def test_legacy_forget_erases_all_kernel_copies_without_reviving_on_upgrade(
    upgrade_database, forget_before_upgrade
):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await old_schema(store)
        await seed(store)
        helper = KernelPayloadStore(store)
        if not forget_before_upgrade:
            await store.migrate()
        lease = await store.acquire_host("host", "forget-host", ttl_seconds=120)
        await helper.payloads.forget_sources(
            lease, "forget", (SourceRef("newer-input"),), expected_deletion_epoch=0
        )
        await store.release_host(lease)
        if forget_before_upgrade:
            await store.migrate()
        lease = await store.acquire_host("host", "scrub-host", ttl_seconds=120)
        while (await helper.payloads.scrub(lease))["remaining"]:
            pass
        async with store.pool.connection() as conn, conn.transaction():
            await helper.payloads.lock_in(conn)
            call = (await select(conn, "bot_calls", "AND call_id=%s", (CALL,)))[0]
            hydrated = await helper.hydrate_call_in(conn, call, epoch=1, required=False)
            assert hydrated["status"] == "COMPLETED" and hydrated["result"] is None
            assert hydrated["payload_state"] == "UNAVAILABLE" and hydrated[
                "result_digest"
            ] == digest_json(VALUE)
            for method, row in (
                (helper.hydrate_call_in, call),
                (
                    helper.hydrate_state_in,
                    (await select(conn, "bot_component_state", "AND key='data'"))[0],
                ),
                (
                    helper.hydrate_step_in,
                    (await select(conn, "bot_state_steps", "AND operation_id='state-data'"))[0],
                ),
                (
                    helper.hydrate_migration_source_in,
                    (await select(conn, "bot_state_migration_sources"))[0],
                ),
                (
                    helper.hydrate_migration_result_in,
                    (await select(conn, "bot_state_migration_results"))[0],
                ),
            ):
                with pytest.raises(ForestError, match="unavailable") as error:
                    await method(conn, row, epoch=1)
                assert error.value.code == "KERNEL_CONTENT_REVOKED"
            bodies = await (
                await conn.execute(
                    "SELECT b.content FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) WHERE h.owner_kind IN ('kernel.result','state.value','state.migration')"
                )
            ).fetchall()
            assert bodies == []
            suppressed = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_suppression WHERE source_unit_id=%s",
                    (legacy_source_ref(store.bot_id).source_unit_id,),
                )
            ).fetchone()
            # A prior forget may predate the first legacy-derived owner. The
            # frozen importer also rejects that body from its durable request.
            assert suppressed or forget_before_upgrade
        await store.migrate()
    finally:
        await store.close()


async def test_conflicting_old_current_and_history_roll_back_entire_migration(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await old_schema(store)
        await seed(store, conflict=True)
        with pytest.raises(ForestError) as error:
            await store.migrate()
        assert error.value.code == "PAYLOAD_CONFLICT"
        async with store.pool.connection() as conn:
            assert (await select(conn, "bot_calls", "AND call_id=%s", (CALL,)))[0][
                "result"
            ] == VALUE
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM public.forest_bot_schema_migrations WHERE version='037_kernel_payloads.sql'"
                )
            ).fetchone()
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='bot_calls' AND column_name='result_payload_id'"
                )
            ).fetchone()
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_headers WHERE owner_kind IN ('kernel.result','state.value','state.migration')"
                )
            ).fetchone()
    finally:
        await store.close()


async def test_new_payload_closure_null_and_duplicate_receipts_never_revive(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("host", "incarnation", ttl_seconds=120)
        helper = KernelPayloadStore(store)
        async with store.pool.connection() as conn, conn.transaction():
            await store._host(conn, lease)
            await helper.payloads.lock_in(conn)
            input_ref = await helper.put_call_input_in(
                conn, "call", VALUE, sources=(SourceRef("input-a"),), epoch=0
            )
            state_ref = await helper.put_state_in(
                conn, "counter", "key", 1, None, parents=(input_ref.payload_id,), epoch=0
            )
            output_ref = await helper.put_call_result_in(
                conn, "call", None, parents=(state_ref.payload_id,), epoch=0
            )
            source_ref = await helper.put_migration_source_in(
                conn, "migration", {"key": None}, parents=(state_ref.payload_id,), epoch=0
            )
            candidate = await helper.put_migration_result_in(
                conn, "migration", {"key": None}, parents=(source_ref.payload_id,), epoch=0
            )
            assert await helper.read_in(conn, output_ref.payload_id, epoch=0) is None
            assert (
                await helper.put_call_result_in(
                    conn, "call", None, parents=(state_ref.payload_id,), epoch=0
                )
            ).payload_id == output_ref.payload_id
            row = {
                "status": "COMPLETED",
                "result_payload_id": output_ref.payload_id,
                "result_digest": output_ref.digest,
            }
            assert (await helper.hydrate_call_in(conn, row, epoch=0))["payload_state"] == "ACTIVE"
            with pytest.raises(ForestError) as error:
                await helper.hydrate_call_in(
                    conn, {**row, "result_digest": digest_json("wrong")}, epoch=0
                )
            assert error.value.code == "KERNEL_PAYLOAD_IDENTITY_CONFLICT"
        await helper.payloads.forget_sources(
            lease, "forget", (SourceRef("input-a"),), expected_deletion_epoch=0
        )
        await helper.payloads.scrub(lease)
        async with store.pool.connection() as conn, conn.transaction():
            await store._host(conn, lease)
            await helper.payloads.lock_in(conn)
            assert (await helper.hydrate_call_in(conn, row, epoch=1, required=False))[
                "payload_state"
            ] == "UNAVAILABLE"
            with pytest.raises(ForestError) as error:
                await helper.put_call_result_in(
                    conn, "call", None, parents=(state_ref.payload_id,), epoch=1
                )
            assert error.value.code == "KERNEL_CONTENT_REVOKED"
            assert await helper.read_in(conn, candidate.payload_id, epoch=1, required=False) is None
            assert not await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=ANY(%s)",
                    (
                        [
                            input_ref.payload_id,
                            state_ref.payload_id,
                            output_ref.payload_id,
                            source_ref.payload_id,
                            candidate.payload_id,
                        ],
                    ),
                )
            ).fetchone()
    finally:
        await store.close()


async def test_database_pins_terminal_result_and_state_receipt_references(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await old_schema(store)
        await seed(store)
        await store.migrate()
        async with store.pool.connection() as conn:
            null_call = (await select(conn, "bot_calls", "AND call_id='null-call'"))[0]
            for query, args in (
                ("UPDATE public.bot_calls SET result='{}' WHERE call_id=%s", (CALL,)),
                (
                    "UPDATE public.bot_calls SET result_payload_id=%s,result_digest=%s WHERE call_id=%s",
                    (null_call["result_payload_id"], null_call["result_digest"], CALL),
                ),
                ("UPDATE public.bot_calls SET status='FAILED' WHERE call_id=%s", (CALL,)),
                ("UPDATE public.bot_calls SET result_digest='forged' WHERE call_id=%s", (CALL,)),
                (
                    "UPDATE public.bot_calls SET input_payload_id=%s,input_legacy_unknown=false WHERE call_id=%s",
                    (null_call["result_payload_id"], CALL),
                ),
                (
                    "UPDATE public.bot_component_state SET value_payload_id=%s,value_digest=%s WHERE key='data'",
                    (null_call["result_payload_id"], null_call["result_digest"]),
                ),
                ("UPDATE public.bot_component_state SET revision=revision+2 WHERE key='data'", ()),
                (
                    'UPDATE public.bot_state_steps SET receipt_metadata=receipt_metadata || \'{"canary":"text"}\'',
                    (),
                ),
                (
                    "UPDATE public.bot_state_revisions SET value_payload_id=%s",
                    (null_call["result_payload_id"],),
                ),
                ("UPDATE public.bot_state_migration_results SET result_digest='forged'", ()),
                ("DELETE FROM public.bot_calls WHERE call_id='pending'", ()),
            ):
                with pytest.raises(psycopg.Error):
                    await conn.execute(query, args)
            assert (await select(conn, "bot_calls", "AND call_id=%s", (CALL,)))[0][
                "result_digest"
            ] == digest_json(VALUE)
            # A new Call cannot opt into the migration-only unknown input flag.
            with pytest.raises(psycopg.Error):
                await conn.execute(
                    "INSERT INTO public.bot_calls SELECT (jsonb_populate_record(NULL::public.bot_calls,to_jsonb(c)||"
                    '\'{"call_id":"forged-legacy","operation_id":"forged-op","root_call_id":"forged-legacy"}\')).* '
                    "FROM public.bot_calls c WHERE call_id='pending'"
                )
    finally:
        await store.close()
