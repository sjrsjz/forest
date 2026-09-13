"""Durable schema transitions against PostgreSQL and real isolated Workers."""

import asyncio
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from test_kernel import GRANTS, increment
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.memory_types import SourceRef
from forest.packages import PackageCatalog
from forest.storage.kernel_payloads import KernelPayloadStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


def state_package(
    tmp_path,
    schema,
    *,
    source=None,
    migration=None,
    suffix="",
    value_schema=None,
    delay=0,
    recover=False,
):
    directory = tmp_path / f"source-{schema}-{suffix}"
    shutil.copytree(EXAMPLE, directory)
    manifest = (directory / "module.toml").read_text()
    manifest = manifest.replace('version = "0.1.0"', f'version = "0.{schema}.{1 if suffix else 0}"')
    fields = f'state_schema = {schema}\nstate_value_schema = "schemas/state.json"'
    if source is not None:
        fields += f"\nstate_migrations = [{{from_schema = {source}, to_schema = {schema}}}]"
    (directory / "module.toml").write_text(manifest.replace("state_schema = 1", fields))
    key = "total" if schema >= 3 else "counter"
    value = "amount" if schema == 2 else None
    access = "(current['value'] or {}).get('amount', 0)" if value else "(current['value'] or 0)"
    write = "{'amount': value}" if value else "value"
    code = f"""import asyncio
from forest.sdk import Plugin, service
from forest.errors import ForestError
class Counter(Plugin):
    @service("counter.increment")
    async def increment(self, payload, ctx):
        await asyncio.sleep({delay})
        current = await ctx.state.get({key!r})
        value = {access} + payload["increment"]
        receipt = await ctx.state.commit({key!r}, {write}, current["revision"], ctx.operations.stable("add"))
        return {{"value": value, "revision": receipt["revision"]}}
"""
    if recover:
        code = code.replace(
            f"        current = await ctx.state.get({key!r})",
            f"""        try:
            current = await ctx.state.get({key!r})
        except ForestError as exc:
            if exc.code != 'KERNEL_CONTENT_REVOKED' or exc.details.get('payload_state') != 'UNAVAILABLE':
                raise
            current = {{'revision': exc.details['current_revision'], 'value': None}}""",
        )
    if migration is not None:
        code += "    async def migrate(self, state, *, from_schema, to_schema):\n"
        code += "\n".join("        " + line for line in migration.splitlines()) + "\n"
    (directory / "python" / "counter.py").write_text(code)
    default = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            key: {
                "type": "object",
                "required": ["amount"],
                "properties": {"amount": {"type": "integer"}},
                "additionalProperties": False,
            }
            if value
            else {"type": "integer"}
        },
    }
    (directory / "schemas" / "state.json").write_text(
        json.dumps(default if value_schema is None else value_schema)
    )
    return PackageCatalog(tmp_path / "catalog").build(directory)


async def state_rows(store):
    helper = KernelPayloadStore(store)
    async with store.pool.connection() as conn, conn.transaction():
        epoch = (await helper.payloads.lock_in(conn))["deletion_epoch"]
        rows = await (
            await conn.execute("SELECT * FROM bot_component_state ORDER BY key")
        ).fetchall()
        assert all(row["value"] is None for row in rows)
        hydrated = [await helper.hydrate_state_in(conn, row, epoch=epoch) for row in rows]
        return [
            {key: row[key] for key in ("key", "revision", "value", "deleted")} for row in hydrated
        ]


async def test_v1_v2_v3_preserves_revisions_receipts_and_removed_key_fence(tmp_path, bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        first = await increment(kernel, "original", 4)
        assert first == {"value": 4, "revision": 1}
        await kernel.install(
            state_package(
                tmp_path,
                2,
                source=1,
                migration="return {'counter': {'amount': state.get('counter', 0)}}",
            ),
            grants=GRANTS,
            operation_id="to-v2",
        )
        assert await store.state_schema(kernel.lease, "example.counter") == 2
        assert await increment(kernel, "v2", 3) == {"value": 7, "revision": 3}
        await kernel.install(
            state_package(
                tmp_path, 3, source=2, migration="return {'total': state['counter']['amount']}"
            ),
            grants=GRANTS,
            operation_id="to-v3",
        )
        assert await increment(kernel, "v3", 2) == {"value": 9, "revision": 2}
        assert await state_rows(store) == [
            {"key": "counter", "revision": 4, "value": None, "deleted": True},
            {"key": "total", "revision": 2, "value": 9, "deleted": False},
        ]
        # A historical operation is a historical receipt, never a state write.
        assert await increment(kernel, "original", 4) == first
        assert (await state_rows(store))[1]["value"] == 9
        # An explicit reverse migration reads CURRENT v3 state, not a v1 snapshot.
        await kernel.install(
            state_package(
                tmp_path,
                1,
                source=3,
                suffix="reverse",
                migration="return {'counter': state['total']}",
            ),
            grants=GRANTS,
        )
        assert await increment(kernel, "reverse", 1) == {"value": 10, "revision": 6}
        async with store.pool.connection() as conn, conn.transaction():
            helper = KernelPayloadStore(store)
            epoch = (await helper.payloads.lock_in(conn))["deletion_epoch"]
            receipts = await (
                await conn.execute("SELECT * FROM bot_state_steps ORDER BY created_at")
            ).fetchall()
            assert len(receipts) == 4
            assert all(receipt["receipt"] is None for receipt in receipts)
            assert (await helper.hydrate_step_in(conn, receipts[0], epoch=epoch))["receipt"][
                "value"
            ] == 4
    finally:
        await kernel.close()
    assert not kernel._workers and not kernel.cleanup_errors


@pytest.mark.parametrize(
    "migration,code",
    [
        (
            "self.n = getattr(self, 'n', 0) + 1\nreturn {'counter': {'amount': self.n}}",
            "STATE_MIGRATION_NONDETERMINISTIC",
        ),
        ("return {'counter': 'wrong'}", "STATE_MIGRATION_INVALID"),
        ("return {'counter': {'amount': float('nan')}}", "IPC_FRAME_INVALID"),
        (
            "await self.ctx.state.commit('counter', 99, 1, 'illegal')\nreturn {}",
            "CALL_CONTEXT_REQUIRED",
        ),
        ("raise RuntimeError('migration failed')", "PLUGIN_FAILED"),
    ],
)
async def test_candidate_migration_failure_preserves_live_source(
    tmp_path, bot_database, migration, code
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        original = await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        await increment(kernel, "before", 5)
        worker = kernel.components["example.counter"].worker
        with pytest.raises(ForestError) as error:
            await kernel.install(
                state_package(tmp_path, 2, source=1, migration=migration), grants=GRANTS
            )
        assert error.value.code == code
        assert (await store.get_route("example.counter"))["generation_id"] == original[
            "generation_id"
        ]
        assert worker.alive and worker.state == "ACTIVE"
        assert await store.state_schema(kernel.lease, "example.counter") == 1
        assert await increment(kernel, "after", 1) == {"value": 6, "revision": 2}
        assert len(kernel._workers) == 1
    finally:
        await kernel.close()


async def test_undeclared_upgrade_and_downgrade_rejected_before_freeze(tmp_path, bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(state_package(tmp_path, 2), grants=GRANTS)
        await increment(kernel, "v2", 8)
        for schema in (1, 3):
            with pytest.raises(ForestError, match="declare migration") as error:
                await kernel.install(state_package(tmp_path, schema), grants=GRANTS)
            assert error.value.code == "STATE_MIGRATION_MISSING"
        assert await increment(kernel, "still-v2", 2) == {"value": 10, "revision": 2}
    finally:
        await kernel.close()


async def test_failed_commit_rolls_back_state_and_route_under_latest_pause(
    tmp_path, bot_database, monkeypatch
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    original_event = store._event

    async def fail_publication(conn, topic, operation_id, payload):
        if topic == "lifecycle.committed":
            raise RuntimeError("transaction failed after state writes")
        await original_event(conn, topic, operation_id, payload)

    staged = store.stage_state_migration

    async def pause_after_staging(*args, **kwargs):
        result = await staged(*args, **kwargs)
        runtime = await store.runtime()
        await store.set_maintenance(kernel.lease, runtime["maintenance_epoch"], False)
        return result

    try:
        route = await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        await increment(kernel, "before", 4)
        monkeypatch.setattr(store, "_event", fail_publication)
        monkeypatch.setattr(store, "stage_state_migration", pause_after_staging)
        with pytest.raises(RuntimeError):
            await kernel.install(
                state_package(
                    tmp_path,
                    2,
                    source=1,
                    migration="return {'counter': {'amount': state['counter']}}",
                ),
                grants=GRANTS,
                operation_id="failed-publication",
            )
        current = await store.get_route("example.counter")
        assert current["generation_id"] == route["generation_id"]
        assert not current["admission"]
        assert await state_rows(store) == [
            {"key": "counter", "revision": 1, "value": 4, "deleted": False}
        ]
        assert await store.state_schema(kernel.lease, "example.counter") == 1
        assert (await store.read_operation("failed-publication"))["phase"] == "FAILED"
    finally:
        await kernel.close()


async def test_lost_publication_ack_restart_reads_new_schema_and_current_values(
    tmp_path, bot_database, monkeypatch
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    candidate = state_package(
        tmp_path, 2, source=1, migration="return {'counter': {'amount': state['counter']}}"
    )
    publish = store.publish

    async def lost_ack(*args, **kwargs):
        await publish(*args, **kwargs)
        raise ConnectionError("committed acknowledgement lost")

    try:
        await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        await increment(kernel, "before", 7)
        monkeypatch.setattr(store, "publish", lost_ack)
        with pytest.raises(ConnectionError):
            await kernel.install(candidate, grants=GRANTS, operation_id="lost-ack")
        assert (await store.read_operation("lost-ack"))["phase"] == "COMMITTED"
        assert await store.state_schema(kernel.lease, "example.counter") == 2
        assert (await state_rows(store))[0]["value"] == {"amount": 7}
        assert not kernel._workers
    finally:
        await kernel.close()
    monkeypatch.setattr(store, "publish", publish)
    successor = Kernel(store)
    await successor.start()
    try:
        await successor.install(candidate, grants=GRANTS)
        assert await increment(successor, "after-restart", 1) == {"value": 8, "revision": 3}
    finally:
        await successor.close()


async def test_512_state_values_reload_through_four_pages_without_losing_deletion_parents(
    tmp_path, bot_database
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    payloads = PayloadStore(store)
    try:
        await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        await increment(kernel, "large-state-input", 17)
        large = state_package(
            tmp_path,
            3,
            source=1,
            value_schema={},
            migration="return {'total': state['counter'], **{f'key-{i:03}': i for i in range(511)}}",
        )
        await kernel.install(large, grants=GRANTS, operation_id="build-512")
        assert len(await store.snapshot_state(kernel.lease, "example.counter")) == 512
        route = await kernel.install(large, grants=GRANTS, operation_id="restore-512")
        assert await increment(kernel, "after-512", 1) == {"value": 18, "revision": 2}
        async with store.pool.connection() as conn, conn.transaction():
            epoch = (await payloads.lock_in(conn))["deletion_epoch"]
            pages = await (
                await conn.execute(
                    "SELECT h.* FROM public.bot_generation_exposures e JOIN public.bot_payload_headers h "
                    "ON h.bot_id=e.bot_id AND h.payload_id=ANY(e.payload_refs) "
                    "WHERE e.generation_id=%s AND e.kind='PREPARE_STATE' ORDER BY h.owner_id",
                    (route["generation_id"],),
                )
            ).fetchall()
            assert len(pages) == 4
            values = {}
            for page in pages:
                contents = await payloads.read_json_in(
                    conn, page["payload_id"], expected_deletion_epoch=epoch
                )
                assert len(contents) == 128
                assert not set(values) & set(contents)
                values.update(contents)
                parents = await (
                    await conn.execute(
                        "SELECT parent_payload_id FROM public.bot_payload_derivations WHERE payload_id=%s",
                        (page["payload_id"],),
                    )
                ).fetchall()
                assert len(parents) == 128
            assert len(values) == 512 and values["total"]["value"] == 17
            target = await (
                await conn.execute(
                    "SELECT d.source_unit_id FROM public.bot_payload_domains d JOIN public.bot_calls c "
                    "ON d.bot_id=c.bot_id AND d.owner_id=c.call_id WHERE d.kind='SERVICE_INPUT' AND c.operation_id='large-state-input'"
                )
            ).fetchone()
        await payloads.forget_sources(
            kernel.lease,
            "forget-large-origin",
            (SourceRef(target["source_unit_id"]),),
            expected_deletion_epoch=0,
        )
        with pytest.raises(ForestError) as error:
            await store.authorize_generation_dispatch(kernel.lease, route["generation_id"])
        assert error.value.code == "GENERATION_EXPOSURE_REVOKED"
        async with store.pool.connection() as conn, conn.transaction():
            helper = KernelPayloadStore(store)
            await payloads.lock_in(conn)
            for page in pages:
                assert (
                    await helper.read_in(conn, page["payload_id"], epoch=1, required=False) is None
                )
            rows = await (
                await conn.execute("SELECT * FROM public.bot_component_state WHERE NOT deleted")
            ).fetchall()
            assert len(rows) == 512
            for row in rows:
                assert (await helper.hydrate_state_in(conn, row, epoch=1, required=False))[
                    "payload_state"
                ] == "UNAVAILABLE"
    finally:
        await kernel.close()
    assert not kernel._workers and not kernel.cleanup_errors


@pytest.mark.parametrize("corruption", ["head_revision", "source_digest"])
async def test_source_revision_and_digest_rechecked_at_atomic_publication(
    tmp_path, bot_database, monkeypatch, corruption
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    publish = store.publish

    async def changed_source(*args, **kwargs):
        # Fault injection represents an unguarded writer/storage corruption. The
        # normal state API is already fenced by quiescence and old call grants.
        async with store.pool.connection() as conn, conn.transaction():
            if corruption == "head_revision":
                await conn.execute("UPDATE bot_component_state_heads SET revision=revision+1")
            else:
                helper = KernelPayloadStore(store)
                epoch = (await helper.payloads.lock_in(conn))["deletion_epoch"]
                row = await (
                    await conn.execute("SELECT * FROM bot_component_state WHERE key='counter'")
                ).fetchone()
                ref = await helper.put_state_in(
                    conn,
                    row["component_id"],
                    row["key"],
                    row["revision"] + 1,
                    123,
                    parents=(row["value_payload_id"],),
                    epoch=epoch,
                )
                await conn.execute(
                    "UPDATE bot_component_state SET revision=revision+1,value_payload_id=%s,value_digest=%s WHERE key='counter'",
                    (ref.payload_id, ref.digest),
                )
        return await publish(*args, **kwargs)

    try:
        route = await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        await increment(kernel, "before", 5)
        monkeypatch.setattr(store, "publish", changed_source)
        with pytest.raises(ForestError) as error:
            await kernel.install(
                state_package(
                    tmp_path,
                    2,
                    source=1,
                    migration="return {'counter': {'amount': state['counter']}}",
                ),
                grants=GRANTS,
            )
        assert error.value.code == "STATE_SOURCE_CONFLICT"
        assert (await store.get_route("example.counter"))["generation_id"] == route["generation_id"]
        assert await store.state_schema(kernel.lease, "example.counter") == 1
    finally:
        await kernel.close()


async def test_one_hundred_reload_cycles_have_stable_owned_resource_baseline(
    tmp_path, bot_database
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    snapshot = state_package(tmp_path, 1)
    retired = []
    try:
        await kernel.install(snapshot, grants=GRANTS)
        initial_tasks = asyncio.all_tasks()
        for index in range(100):
            retired.append(kernel.components["example.counter"].worker)
            await kernel.install(snapshot, grants=GRANTS, operation_id=f"reload-{index}")
            assert len(kernel._workers) == 1
            assert not any(worker.alive for worker in retired)
            assert not kernel._operations and not kernel._calls
            assert await increment(kernel, f"count-{index}") == {
                "value": index + 1,
                "revision": index + 1,
            }
        # Pool/IPC completion and the supervised periodic scan can still be
        # finishing after the last call. Require the original task-count bound
        # throughout an idle interval, without excluding or allowing extra tasks.
        loop = asyncio.get_running_loop()
        settle_deadline = loop.time() + 5
        quiet_since = None
        while True:
            current_tasks = asyncio.all_tasks()
            now = loop.time()
            if len(current_tasks) <= len(initial_tasks):
                quiet_since = now if quiet_since is None else quiet_since
                if now - quiet_since >= 0.2:
                    break
            else:
                quiet_since = None
            if now >= settle_deadline:
                extras = sorted(
                    (task.get_name(), repr(task.get_coro()))
                    for task in current_tasks - initial_tasks
                )
                pytest.fail(
                    f"Task count did not settle to baseline {len(initial_tasks)}; "
                    f"current={len(current_tasks)}, extra tasks={extras}"
                )
            await asyncio.sleep(0.05)
        assert len(current_tasks) <= len(initial_tasks)
    finally:
        await kernel.close()
    assert not kernel.cleanup_errors and not kernel._workers


async def test_old_state_step_cannot_replay_or_write_after_migration(
    tmp_path, bot_database, monkeypatch
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    commit = store.commit_state
    captured = []

    async def remember(*args, **kwargs):
        captured.append((args, kwargs))
        return await commit(*args, **kwargs)

    try:
        await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        monkeypatch.setattr(store, "commit_state", remember)
        await increment(kernel, "old-step", 6)
        await kernel.install(
            state_package(
                tmp_path, 2, source=1, migration="return {'counter': {'amount': state['counter']}}"
            ),
            grants=GRANTS,
        )
        args, kwargs = captured[0]
        with pytest.raises(ForestError) as error:
            await commit(*args, **kwargs)
        assert error.value.code == "STALE_ROUTE"
        assert (await state_rows(store))[0]["value"] == {"amount": 6}
    finally:
        await kernel.close()


async def test_process_death_after_shadow_write_preserves_source_and_recovery(
    tmp_path, bot_database
):
    store, uri = bot_database
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(state_package(tmp_path, 1), grants=GRANTS)
        await increment(kernel, "before-crash", 12)
    finally:
        await kernel.close()
    candidate = state_package(
        tmp_path, 2, source=1, migration="return {'counter': {'amount': state['counter']}}"
    )
    script = """import asyncio, json, os, sys
from pathlib import Path
from forest.kernel import Kernel
from forest.packages import PackageSnapshot
from forest.storage.bot import BotStore
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
data = json.loads(sys.stdin.read())
async def main():
    store = BotStore(data['dsn'], 'test-bot')
    await store.open()
    kernel = Kernel(store, lease_seconds=3)
    await kernel.start()
    stage = store.stage_state_migration
    async def crash(*args, **kwargs):
        await stage(*args, **kwargs)
        os._exit(23)
    store.stage_state_migration = crash
    await kernel.install(PackageSnapshot(Path(data['root']), data['digest'], data['manifest']),
        grants=frozenset({'state.read', 'state.write'}), operation_id='crash-after-shadow')
asyncio.run(main())
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.to_thread(
            process.communicate,
            json.dumps(
                {
                    "dsn": uri,
                    "root": str(candidate.root),
                    "digest": candidate.digest,
                    "manifest": candidate.manifest,
                }
            ).encode(),
            timeout=30,
        )
        assert process.returncode == 23, stderr.decode(errors="replace")
    finally:
        if process.poll() is None:
            process.kill()
            await asyncio.to_thread(process.wait)
    assert (await state_rows(store))[0]["value"] == 12
    assert (await store.read_operation("crash-after-shadow"))["phase"] == "QUIESCENT"
    await asyncio.sleep(3.1)
    successor = Kernel(store)
    await successor.start()
    try:
        await successor.install(candidate, grants=GRANTS)
        assert (await store.read_operation("crash-after-shadow"))["phase"] == "ABANDONED"
        assert await increment(successor, "after-crash", 1) == {"value": 13, "revision": 3}
    finally:
        await successor.close()


async def test_migration_captures_the_last_inflight_write_after_drain(tmp_path, bot_database):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    pending = None
    try:
        await kernel.install(state_package(tmp_path, 1, delay=0.25), grants=GRANTS)
        await increment(kernel, "before", 4)
        pending = asyncio.create_task(increment(kernel, "inflight", 2))
        async with asyncio.timeout(5):
            while not kernel._calls:
                await asyncio.sleep(0.01)
        await kernel.install(
            state_package(
                tmp_path, 2, source=1, migration="return {'counter': {'amount': state['counter']}}"
            ),
            grants=GRANTS,
        )
        assert await pending == {"value": 6, "revision": 2}
        assert (await state_rows(store))[0] == {
            "key": "counter",
            "revision": 3,
            "value": {"amount": 6},
            "deleted": False,
        }
        assert await increment(kernel, "after", 1) == {"value": 7, "revision": 4}
    finally:
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
        await kernel.close()


@pytest.mark.parametrize("boundary", ["capture", "stage"])
async def test_deleted_state_cannot_restore_candidate_or_replay_then_clean_restart_keeps_fence(
    tmp_path,
    bot_database,
    monkeypatch,
    boundary,
):
    store, _ = bot_database
    kernel = Kernel(store)
    await kernel.start()
    payloads = PayloadStore(store)
    original = state_package(tmp_path, 1)
    method_name = "capture_state_migration" if boundary == "capture" else "stage_state_migration"
    method = getattr(store, method_name)
    captured = []
    try:
        await kernel.install(original, grants=GRANTS)
        assert await increment(kernel, "private-input", 41) == {"value": 41, "revision": 1}
        async with store.pool.connection() as conn:
            target = await (
                await conn.execute(
                    "SELECT d.source_unit_id FROM public.bot_payload_domains d JOIN public.bot_calls c "
                    "ON d.bot_id=c.bot_id AND d.owner_id=c.call_id WHERE d.kind='SERVICE_INPUT' AND c.operation_id='private-input'"
                )
            ).fetchone()
        assert target

        async def forget_after_receipt(*args, **kwargs):
            result = await method(*args, **kwargs)
            captured.append(result)
            await payloads.forget_sources(
                kernel.lease,
                "forget-state-input",
                (SourceRef(target["source_unit_id"]),),
                expected_deletion_epoch=0,
            )
            return result

        monkeypatch.setattr(store, method_name, forget_after_receipt)
        with pytest.raises(ForestError) as error:
            await kernel.install(
                state_package(
                    tmp_path,
                    2,
                    source=1,
                    migration="return {'counter': {'amount': state['counter']}}",
                ),
                grants=GRANTS,
                operation_id="revoked-candidate",
            )
        assert error.value.code == "GENERATION_EXPOSURE_REVOKED"
        assert len(captured) == 1
        assert await store.state_schema(kernel.lease, "example.counter") == 1
        assert (await store.read_operation("revoked-candidate"))["phase"] == "FAILED"
        async with store.pool.connection() as conn, conn.transaction():
            helper = KernelPayloadStore(store)
            epoch = (await payloads.lock_in(conn))["deletion_epoch"]
            row = await (
                await conn.execute("SELECT * FROM public.bot_component_state WHERE key='counter'")
            ).fetchone()
            assert row["revision"] == 1 and row["value"] is None
            assert (await helper.hydrate_state_in(conn, row, epoch=epoch, required=False))[
                "payload_state"
            ] == "UNAVAILABLE"
            captured_id = captured[0][
                "source_payload_id" if boundary == "capture" else "result_payload_id"
            ]
            assert await helper.read_in(conn, captured_id, epoch=epoch, required=False) is None
    finally:
        await kernel.close()
        monkeypatch.setattr(store, method_name, method)
    assert not kernel._workers and not kernel.cleanup_errors

    successor = Kernel(store)
    await successor.start()
    try:
        await successor.install(original, grants=GRANTS)
        assert await store.snapshot_state(successor.lease, "example.counter") == {}
        # Existing plugins must not silently reinterpret erased state as null.
        with pytest.raises(ForestError) as error:
            await increment(successor, "unhandled-read", 1)
        assert error.value.code == "KERNEL_CONTENT_REVOKED"
        assert error.value.details["current_revision"] == 1
        assert error.value.details["payload_state"] == "UNAVAILABLE"
        await successor.install(
            state_package(tmp_path, 1, suffix="recover", recover=True), grants=GRANTS
        )
        # The new plugin explicitly handles the typed error and uses its CAS fence.
        assert await increment(successor, "new-independent", 1) == {"value": 1, "revision": 2}
        with pytest.raises(ForestError) as error:
            await increment(successor, "private-input", 41)
        assert error.value.code == "KERNEL_CONTENT_REVOKED"
        assert await state_rows(store) == [
            {"key": "counter", "revision": 2, "value": 1, "deleted": False}
        ]
        await payloads.scrub(successor.lease)
        async with store.pool.connection() as conn:
            old = await (
                await conn.execute(
                    "SELECT status,result,result_digest FROM public.bot_calls WHERE operation_id='private-input'"
                )
            ).fetchone()
            assert old["status"] == "COMPLETED" and old["result"] is None and old["result_digest"]
    finally:
        await successor.close()
