"""Real PostgreSQL and Worker coverage for durable component events."""

import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.bot import BotStore
from forest.storage.events import EventStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def event_store():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a dedicated test PostgreSQL")
    name = "forest_events_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "event-bot")
    try:
        await store.open()
        await store.migrate()
        yield store
    finally:
        await store.close()
        assert name.startswith("forest_events_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def event_package(
    tmp_path: Path,
    name: str,
    *,
    publisher: bool = False,
    subscriber_id: str | None = None,
    fail: bool = False,
    max_attempts: int = 3,
    version: str = "0.1.0",
    decorate_subscription: bool = True,
    event_value_type: str = "integer",
):
    source = tmp_path / f"{name}-{version}-{uuid4().hex}"
    (source / "python").mkdir(parents=True)
    (source / "schemas").mkdir()
    capabilities = ["state.read", "state.write"] if subscriber_id else []
    if publisher:
        capabilities.append("event.publish:profile.changed")
    manifest = f'''manifest_version = 1
package_id = "test.{name}"
version = "{version}"
kernel_api = ">=1.0,<2.0"
runtime = "python-worker"
entrypoint = "implementation:Component"
[[components]]
id = "{name}"
scope = "bot"
state_schema = 1
reload_mode = "drain"
config_schema = "schemas/config.json"
[components.capabilities]
requests = {json.dumps(capabilities)}
[[components.provides]]
name = "{name}.run"
contract = "1.0"
input_schema = "schemas/input.json"
output_schema = "schemas/output.json"
'''
    if publisher:
        manifest += '''[[components.publishes]]
topic = "profile.changed"
contract = "1.0"
schema = "schemas/event.json"
'''
    if subscriber_id and decorate_subscription:
        manifest += f'''[[components.subscribes]]
subscriber_id = "{subscriber_id}"
topic = "profile.changed"
contract = "1.0"
schema = "schemas/event.json"
max_attempts = {max_attempts}
'''
    (source / "module.toml").write_text(manifest, encoding="utf-8")
    schemas = {
        "config": {"type": "object"},
        "input": {
            "type": "object", "required": ["value"],
            "properties": {
                "value": {"type": "integer"},
                "event_operation": {"type": "string"},
            },
            "additionalProperties": False,
        },
        "output": {},
        "event": {
            "type": "object", "required": ["value"],
            "properties": {"value": {"type": event_value_type}}, "additionalProperties": False,
        },
    }
    for key, value in schemas.items():
        (source / "schemas" / f"{key}.json").write_text(json.dumps(value), encoding="utf-8")
    methods = []
    if publisher:
        methods.append('''    @service("%s.run")
    async def run(self, payload, ctx):
        return await ctx.events.publish(
            "profile.changed", {"value": payload["value"]}, contract="1.0",
            operation_id=payload.get("event_operation") or
                ctx.operations.stable("publish:" + str(payload["value"])),
        )
''' % name)
    else:
        methods.append('''    @service("%s.run")
    async def run(self, payload, ctx):
        return payload
''' % name)
    if subscriber_id:
        body = 'raise RuntimeError("private subscriber failure")' if fail else '''current = await ctx.state.get("events")
        value = current["value"] or []
        if event["event_id"] not in value:
            value.append(event["event_id"])
            await ctx.state.commit(
                "events", value, current["revision"],
                ctx.operations.stable("consume:" + event["event_id"]),
            )
        return {"consumed": event["event_id"]}'''
        methods.append(f'''    @subscribe("{subscriber_id}", "profile.changed", "1.0")
    async def changed(self, event, ctx):
        {body}
''')
    imports = "from forest.sdk import Plugin, service" + (", subscribe" if subscriber_id else "")
    code = imports + "\nclass Component(Plugin):\n" + "\n".join(methods)
    (source / "python" / "implementation.py").write_text(code, encoding="utf-8")
    return PackageCatalog(tmp_path / "catalog").build(source)


def install_spec(snapshot):
    return {
        "snapshot": snapshot,
        "grants": frozenset(snapshot.component.get("capabilities", {}).get("requests", [])),
    }


async def wait_deliveries(store, expected, *, timeout=15):
    async with asyncio.timeout(timeout):
        while True:
            rows = await EventStore(store).deliveries()
            if expected(rows):
                return rows
            await asyncio.sleep(0.05)


async def test_event_publish_fans_out_and_lost_ack_does_not_repeat_handler(
    tmp_path, event_store, monkeypatch
):
    kernel = Kernel(event_store)
    await kernel.start()
    acknowledged = EventStore.acknowledge
    lost = False

    async def lose_first_ack(self, *args, **kwargs):
        nonlocal lost
        await acknowledged(self, *args, **kwargs)
        if not lost:
            lost = True
            raise RuntimeError("synthetic lost acknowledgement")

    monkeypatch.setattr(EventStore, "acknowledge", lose_first_ack)
    try:
        left = event_package(tmp_path, "left", subscriber_id="profile.left")
        right = event_package(tmp_path, "right", subscriber_id="profile.right")
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(left), install_spec(right), install_spec(publisher)])
        receipt = await kernel.invoke(
            "publisher", "publisher.run", {"value": 7}, operation_id="publish-seven"
        )
        assert receipt["delivery_count"] == 2
        assert await kernel.invoke(
            "publisher", "publisher.run", {"value": 7}, operation_id="publish-seven"
        ) == receipt
        rows = await wait_deliveries(
            event_store, lambda items: len(items) == 2 and all(r["status"] == "ACKED" for r in items)
        )
        assert {row["event_id"] for row in rows} == {receipt["event_id"]}
        assert all(row["attempt_count"] == 1 for row in rows)
        for component in ("left", "right"):
            state = await event_store.snapshot_state(kernel.lease, component)
            assert state["events"]["value"] == [receipt["event_id"]]
        async with event_store.pool.connection() as conn:
            count = await (await conn.execute(
                "SELECT count(*) AS n FROM bot_component_events"
            )).fetchone()
        assert count["n"] == 1
    finally:
        await kernel.close()


async def test_failing_subscriber_is_independent_and_reaches_dead_letter(tmp_path, event_store):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        good = event_package(tmp_path, "good", subscriber_id="profile.good")
        bad = event_package(
            tmp_path, "bad", subscriber_id="profile.bad", fail=True, max_attempts=2
        )
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(good), install_spec(bad), install_spec(publisher)])
        await kernel.invoke(
            "publisher", "publisher.run", {"value": 9}, operation_id="publish-nine"
        )
        rows = await wait_deliveries(
            event_store,
            lambda items: {row["status"] for row in items} == {"ACKED", "DEAD"},
        )
        failed = next(row for row in rows if row["status"] == "DEAD")
        assert failed["subscriber_id"] == "profile.bad"
        assert failed["attempt_count"] == 2
        assert failed["last_error_code"] == "PLUGIN_FAILED"
        assert "private" not in json.dumps(failed, default=str)
        state = await event_store.snapshot_state(kernel.lease, "good")
        assert len(state["events"]["value"]) == 1
    finally:
        await kernel.close()


async def test_pending_delivery_moves_to_compatible_reloaded_subscriber(tmp_path, event_store):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        failing = event_package(
            tmp_path, "consumer", subscriber_id="profile.consumer", fail=True,
            max_attempts=8, version="0.1.0",
        )
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(failing), install_spec(publisher)])
        receipt = await kernel.invoke(
            "publisher", "publisher.run", {"value": 11}, operation_id="publish-eleven"
        )
        await wait_deliveries(
            event_store,
            lambda items: len(items) == 1 and items[0]["status"] == "RETRY_WAIT",
        )
        replacement = event_package(
            tmp_path, "consumer", subscriber_id="profile.consumer",
            max_attempts=8, version="0.2.0",
        )
        await kernel.install(replacement, grants=frozenset({"state.read", "state.write"}))
        rows = await wait_deliveries(
            event_store,
            lambda items: len(items) == 1 and items[0]["status"] == "ACKED",
        )
        assert rows[0]["attempt_count"] >= 2
        state = await event_store.snapshot_state(kernel.lease, "consumer")
        assert state["events"]["value"] == [receipt["event_id"]]
    finally:
        await kernel.close()


async def test_claimed_delivery_reroutes_without_spending_handler_attempt(tmp_path, event_store):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        original = event_package(
            tmp_path,
            "consumer",
            subscriber_id="profile.reroute",
            max_attempts=1,
            version="0.1.0",
        )
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(original), install_spec(publisher)])
        kernel._event_dispatcher.cancel()
        await asyncio.gather(kernel._event_dispatcher, return_exceptions=True)
        receipt = await kernel.invoke(
            "publisher", "publisher.run", {"value": 12}, operation_id="publish-reroute"
        )
        events = EventStore(event_store)
        claimed = await events.claim(kernel.lease, lease_seconds=3)
        assert claimed is not None and claimed["attempt_count"] == 1

        replacement = event_package(
            tmp_path,
            "consumer",
            subscriber_id="profile.reroute",
            max_attempts=1,
            version="0.2.0",
        )
        await kernel.install(
            replacement, grants=frozenset({"state.read", "state.write"})
        )
        await kernel._deliver_event(claimed)
        rows = await events.deliveries()
        assert rows[0]["status"] == "RETRY_WAIT"
        assert rows[0]["attempt_count"] == 0

        rerouted = None
        async with asyncio.timeout(2):
            while rerouted is None:
                rerouted = await events.claim(kernel.lease, lease_seconds=3)
                if rerouted is None:
                    await asyncio.sleep(0.05)
        assert rerouted is not None and rerouted["attempt_count"] == 1
        assert rerouted["target_generation_id"] != claimed["target_generation_id"]
        await kernel._deliver_event(rerouted)
        rows = await events.deliveries()
        assert rows[0]["status"] == "ACKED"
        state = await event_store.snapshot_state(kernel.lease, "consumer")
        assert state["events"]["value"] == [receipt["event_id"]]
    finally:
        await kernel.close()


async def test_concurrent_publications_converge_and_consume_in_transaction_sequence(
    tmp_path, event_store
):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        consumer = event_package(tmp_path, "consumer", subscriber_id="profile.ordered")
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(consumer), install_spec(publisher)])
        first, duplicate = await asyncio.gather(
            kernel.invoke(
                "publisher", "publisher.run",
                {"value": 1, "event_operation": "shared-event"}, operation_id="call-shared-a",
            ),
            kernel.invoke(
                "publisher", "publisher.run",
                {"value": 1, "event_operation": "shared-event"}, operation_id="call-shared-b",
            ),
        )
        assert first == duplicate
        await asyncio.gather(
            kernel.invoke(
                "publisher", "publisher.run",
                {"value": 2, "event_operation": "ordered-a"}, operation_id="call-ordered-a",
            ),
            kernel.invoke(
                "publisher", "publisher.run",
                {"value": 3, "event_operation": "ordered-b"}, operation_id="call-ordered-b",
            ),
        )
        await wait_deliveries(
            event_store,
            lambda items: len(items) == 3 and all(row["status"] == "ACKED" for row in items),
        )
        async with event_store.pool.connection() as conn:
            events = await (await conn.execute(
                "SELECT event_id,event_seq FROM bot_component_events ORDER BY event_seq"
            )).fetchall()
        assert [row["event_seq"] for row in events] == [1, 2, 3]
        state = await event_store.snapshot_state(kernel.lease, "consumer")
        assert state["events"]["value"] == [row["event_id"] for row in events]
    finally:
        await kernel.close()


async def test_expired_claim_keeps_attempt_operation_for_completion_reconciliation(
    tmp_path, event_store
):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        consumer = event_package(tmp_path, "consumer", subscriber_id="profile.recover")
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(consumer), install_spec(publisher)])
        kernel._event_dispatcher.cancel()
        await asyncio.gather(kernel._event_dispatcher, return_exceptions=True)
        await kernel.invoke(
            "publisher", "publisher.run", {"value": 4}, operation_id="publish-recovery"
        )
        events = EventStore(event_store)
        first = await events.claim(kernel.lease, lease_seconds=3)
        assert first is not None and first["attempt_count"] == 1
        async with event_store.pool.connection() as conn:
            await conn.execute(
                "UPDATE bot_event_deliveries SET claim_until=clock_timestamp()-interval '1 second' "
                "WHERE delivery_id=%s",
                (first["delivery_id"],),
            )
        recovered = await events.claim(kernel.lease, lease_seconds=3)
        assert recovered is not None
        assert recovered["attempt_count"] == 1
        assert recovered["call_operation_id"] == first["call_operation_id"]
        assert recovered["claim_token"] != first["claim_token"]
    finally:
        await kernel.close()


async def test_capacity_deferral_and_shutdown_abandon_preserve_attempt_identity(
    tmp_path, event_store
):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        consumer = event_package(tmp_path, "consumer", subscriber_id="profile.defer")
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(consumer), install_spec(publisher)])
        kernel._event_dispatcher.cancel()
        await asyncio.gather(kernel._event_dispatcher, return_exceptions=True)
        await kernel.invoke(
            "publisher", "publisher.run", {"value": 5}, operation_id="publish-defer"
        )
        events = EventStore(event_store)
        claimed = await events.claim(kernel.lease, lease_seconds=3)
        assert claimed is not None and claimed["attempt_count"] == 1

        await events.defer(kernel.lease, claimed, delay_seconds=0)
        rows = await events.deliveries()
        assert rows[0]["status"] == "RETRY_WAIT"
        assert rows[0]["attempt_count"] == 0
        assert rows[0]["claim_token"] is None

        reclaimed = await events.claim(kernel.lease, lease_seconds=3)
        assert reclaimed is not None and reclaimed["attempt_count"] == 1
        with pytest.raises(ForestError) as forged_ack:
            await events.acknowledge(
                kernel.lease,
                reclaimed,
                result_payload_id=reclaimed["event_payload_id"],
            )
        assert forged_ack.value.code == "EVENT_RESULT_MISMATCH"
        await events.abandon(kernel.lease, reclaimed)
        rows = await events.deliveries()
        assert rows[0]["status"] == "INFLIGHT"
        assert rows[0]["attempt_count"] == 1
        assert rows[0]["claim_token"] == reclaimed["claim_token"]

        recovered = await events.claim(kernel.lease, lease_seconds=3)
        assert recovered is not None
        assert recovered["attempt_count"] == 1
        assert recovered["call_operation_id"] == reclaimed["call_operation_id"]
        await events.defer(kernel.lease, recovered, delay_seconds=0)
        rows = await events.deliveries()
        assert rows[0]["status"] == "INFLIGHT"
        assert rows[0]["attempt_count"] == 1
        continued = await events.claim(kernel.lease, lease_seconds=3)
        assert continued is not None
        assert continued["attempt_count"] == 1
        assert continued["call_operation_id"] == recovered["call_operation_id"]
    finally:
        await kernel.close()


async def test_dispatcher_shutdown_expires_claim_even_if_delivery_body_never_cleans_up(
    tmp_path, event_store, monkeypatch
):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        consumer = event_package(tmp_path, "consumer", subscriber_id="profile.shutdown")
        publisher = event_package(tmp_path, "publisher", publisher=True)
        await kernel.install_many([install_spec(consumer), install_spec(publisher)])
        kernel._event_dispatcher.cancel()
        await asyncio.gather(kernel._event_dispatcher, return_exceptions=True)
        await kernel.invoke(
            "publisher", "publisher.run", {"value": 6}, operation_id="publish-shutdown"
        )

        entered = asyncio.Event()

        async def blocked_delivery(_delivery):
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(kernel, "_deliver_event", blocked_delivery)
        kernel._event_dispatcher = asyncio.create_task(kernel._supervise_events())
        kernel._event_wake.set()
        async with asyncio.timeout(5):
            await entered.wait()
        await kernel.close()

        assert kernel._call_count == 0
        async with event_store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT status,claim_until<=clock_timestamp() AS expired "
                    "FROM bot_event_deliveries"
                )
            ).fetchone()
        assert row == {"status": "INFLIGHT", "expired": True}
    finally:
        await kernel.close()


async def test_subscription_code_mismatch_and_implicit_removal_are_rejected(tmp_path, event_store):
    kernel = Kernel(event_store)
    await kernel.start()
    try:
        mismatched = event_package(
            tmp_path, "mismatch", subscriber_id="profile.mismatch",
            decorate_subscription=False,
        )
        with pytest.raises(ForestError) as mismatch:
            await kernel.install(
                mismatched, grants=frozenset({"state.read", "state.write"})
            )
        assert mismatch.value.code == "SUBSCRIPTION_MANIFEST_MISMATCH"

        original = event_package(tmp_path, "consumer", subscriber_id="profile.stable")
        route = await kernel.install(
            original, grants=frozenset({"state.read", "state.write"})
        )
        worker = kernel.components["consumer"].worker
        removed = event_package(tmp_path, "consumer", version="0.2.0")
        with pytest.raises(ForestError) as removal:
            await kernel.install(removed)
        assert removal.value.code == "SUBSCRIPTION_REMOVAL_REQUIRES_RETIREMENT"
        assert kernel.components["consumer"].worker is worker
        assert worker.alive
        assert (await event_store.get_route("consumer"))["generation_id"] == route["generation_id"]

        incompatible = event_package(
            tmp_path, "incompatible", publisher=True, event_value_type="string"
        )
        with pytest.raises(ForestError) as schema:
            await kernel.install(
                incompatible, grants=frozenset({"event.publish:profile.changed"})
            )
        assert schema.value.code == "EVENT_CONTRACT_SCHEMA_CONFLICT"
    finally:
        await kernel.close()
