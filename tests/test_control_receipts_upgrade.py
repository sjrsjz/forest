"""Actual 038 event bodies upgrade to exact 040 receipts, including already forgotten results."""

import pytest
from psycopg.types.json import Jsonb
from test_control_results_upgrade import read_rows, schema_038
from test_upgrade import upgrade_database as upgrade_database

from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.control_receipts import ControlReceiptStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("forgotten_before", [False, True])
async def test_nonempty_exact_event_upgrade_keeps_unrelated_events_and_restarts_idempotently(
    upgrade_database, forgotten_before
):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await schema_038(store)
        values = {
            "package_digest": "old-package",
            "config": {"label": "OLD-PLAN-CANARY"},
            "grants": [],
        }
        plan = {
            "plan_id": "old-plan",
            "plan_digest": "old-digest",
            "values": values,
            "expected_revision": 0,
        }
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO public.bot_config_plans(bot_id,plan_id,component_id,subject_id,issuer_id,expected_revision,values,plan_digest,expires_at) VALUES ('bot-a','old-plan','counter','admin','issuer',0,%s,'old-digest','2030-01-01Z')",
                (Jsonb(values),),
            )
            for op, action, result in [
                ("preview", "config.plan", plan),
                ("apply", "config.apply", {"desired_revision": 1, "status": "ACCEPTED"}),
            ]:
                await conn.execute(
                    "INSERT INTO public.bot_web_operations(bot_id,operation_id,action,input_digest,subject_id,issuer_id,ticket_basis,result) VALUES ('bot-a',%s,%s,%s,'admin','issuer','{}',%s)",
                    (op, action, digest_json(["unrecorded", op]), Jsonb(result)),
                )
            await conn.execute(
                "INSERT INTO public.bot_config_revisions(bot_id,component_id,revision,previous_revision,values,policy_revision,plan_id,operation_id) VALUES ('bot-a','counter',1,0,%s,1,'old-plan','apply')",
                (Jsonb(values),),
            )
            for table, key, label, body in [
                ("bot_audit_records", "audit_id", "action", "details"),
                ("bot_outbox_events", "event_id", "topic", "payload"),
            ]:
                for n in range(66):
                    await conn.execute(
                        f"INSERT INTO public.{table}(bot_id,{key},{label},operation_id,{body}) VALUES ('bot-a',%s,'config.plan','preview',%s)",
                        (f"exact-{n:03}", Jsonb({"subject_id": "admin", **plan})),
                    )
                await conn.execute(
                    f"INSERT INTO public.{table}(bot_id,{key},{label},operation_id,{body}) VALUES ('bot-a','different','config.plan','preview',%s)",
                    (Jsonb({"subject_id": "admin", "unrelated": "keep-exact-family-boundary"}),),
                )
        if forgotten_before:
            lease = await store.acquire_host("host", "before", ttl_seconds=120)
            await PayloadStore(store).forget_sources(
                lease, "old-forget", (SourceRef("some-old-chat"),), expected_deletion_epoch=0
            )
            await store.release_host(lease)
        await store.migrate()
        operation = (
            await read_rows(
                store, "SELECT * FROM public.bot_web_operations WHERE operation_id='preview'"
            )
        )[0]
        assert (
            operation["input_legacy_unknown"]
            and operation["input_payload_id"] is None
            and operation["result"] is None
        )
        for table, key, body, kind in [
            ("bot_audit_records", "audit_id", "details", "audit"),
            ("bot_outbox_events", "event_id", "payload", "outbox"),
        ]:
            events = await read_rows(
                store, f"SELECT * FROM public.{table} WHERE {key} LIKE 'exact-%%' ORDER BY {key}"
            )
            assert len(events) == 66
            assert all(
                e[body] is None and e["control_result_payload_id"] == operation["result_payload_id"]
                for e in events
            )
            async with store.pool.connection() as conn, conn.transaction():
                value = await ControlReceiptStore(store).hydrate_event_in(
                    conn, events[0], kind=kind, required=False
                )
            assert value["control_payload_state"] == (
                "UNAVAILABLE" if forgotten_before else "ACTIVE"
            )
            assert value[body] == (None if forgotten_before else {"subject_id": "admin", **plan})
            outsider = (
                await read_rows(store, f"SELECT * FROM public.{table} WHERE {key}='different'")
            )[0]
            assert (
                outsider["control_result_payload_id"] is None
                and outsider[body]["unrelated"] == "keep-exact-family-boundary"
            )
        bridges = await read_rows(
            store, "SELECT * FROM public.bot_control_value_sources ORDER BY source_kind"
        )
        assert len(bridges) == 2
        assert (
            bridges[0]["source_kind"] == "PLAN"
            and bridges[0]["legacy_untracked"] == forgotten_before
        )
        assert bridges[1]["source_kind"] == "REVISION" and not bridges[1]["legacy_untracked"]
        before = await read_rows(
            store, "SELECT * FROM public.bot_payload_headers ORDER BY payload_id"
        )
        await store.close()
        store = BotStore(upgrade_database, "bot-a")
        await store.open()
        await store.migrate()
        assert (
            await read_rows(store, "SELECT * FROM public.bot_payload_headers ORDER BY payload_id")
            == before
        )
        assert (
            await read_rows(
                store, "SELECT * FROM public.bot_control_value_sources ORDER BY source_kind"
            )
            == bridges
        )
    finally:
        await store.close()
