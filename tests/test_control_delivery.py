"""Real OneBot UNKNOWN receipt and Web reconciliation retain only permitted content."""

import pytest
from test_web_bindings import configure, get, onebot_server, receive, wait_for
from test_web_bots import ORIGIN, post, ready, running, setup
from test_web_bots import web_bots_database as web_bots_database
from test_web_control_receipts import erase

from forest.bootstrap import BootstrapState
from forest.errors import ForestError
from forest.management.app import WebSettings
from forest.memory_types import SourceRef

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "erase_original,decision", [(False, "SKIP"), (True, "SKIP"), (True, "SENT")]
)
async def test_web_unknown_resolution_inherits_administrative_source_and_replays_without_sending(
    tmp_path, web_bots_database, erase_original, decision
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    async with (
        onebot_server() as server,
        running(WebSettings(data, ORIGIN, managed_dsn=web_bots_database)) as (app, client),
    ):
        await setup(client, token)
        created = await post(
            client, "/management/v1/bots", {"display_name": "Resolve"}, "create", status=202
        )
        await ready(client, created)
        root = "/management/v1/bots/" + created["bot_id"]
        binding = await post(
            client,
            root + "/bindings",
            {"self_id": "10001", "display_name": "Account"},
            "binding",
            status=201,
        )
        await configure(client, root + "/bindings/" + binding["binding_id"], server["origin"])
        server["unknown_next"] = True
        assert (
            await receive(client, binding["binding_id"], 2, "/chat one uncertain message")
        ).status_code == 200
        pending = await wait_for(
            lambda: get(client, root + "/delivery"),
            lambda value: any(p["state"] == "UNKNOWN" for p in value["parts"]),
        )
        part = next(p for p in pending["parts"] if p["state"] == "UNKNOWN")
        bot = app.state.runtime.fleet.bots[created["bot_id"]]
        content = bot.platform.delivery.contents
        async with bot.store.pool.connection() as conn:
            original_part = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_parts WHERE intent_id=%s", (part["intent_id"],)
                )
            ).fetchone()
            source = await (
                await conn.execute(
                    "SELECT o.source_unit_id FROM public.bot_memory_source_occurrences o JOIN public.bot_message_inbox i USING(bot_id,event_id) WHERE i.platform_message_id='2'"
                )
            ).fetchone()
        assert len(server["sends"]) == 1
        if erase_original:
            await content.payloads.forget_sources(
                bot.kernel._authority(),
                "erase-message",
                (SourceRef(source["source_unit_id"]),),
                expected_deletion_epoch=0,
            )
            await content.payloads.scrub(bot.kernel._authority())
        payload = {
            "expected_revision": part["revision"],
            "decision": decision,
            "reason": "ADMIN-REASON-CANARY",
        }
        endpoint = root + "/delivery/" + part["intent_id"] + "/0/resolution"
        resolved = await post(client, endpoint, payload, "resolve")
        assert resolved["state"] == ("SKIPPED" if decision == "SKIP" else "SENT")
        assert await post(client, endpoint, payload, "resolve") == resolved
        async with bot.store.pool.connection() as conn:
            receipt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_web_operations WHERE action='delivery.reconcile'"
                )
            ).fetchone()
            resolution = await (
                await conn.execute(
                    "SELECT * FROM public.bot_delivery_resolutions WHERE operation_id=%s",
                    (receipt["operation_id"],),
                )
            ).fetchone()
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_attempts WHERE intent_id=%s",
                    (part["intent_id"],),
                )
            ).fetchone()
        assert (
            attempt["state"] == "UNKNOWN"
            and resolution["result_metadata"]["state"] == resolved["state"]
        )
        assert resolution["reason"] is None and resolution["result"] is None
        if erase_original:
            assert resolution["payload_id"] is None
        else:
            async with bot.store.pool.connection() as conn, conn.transaction():
                epoch = (await content.payloads.lock_in(conn))["deletion_epoch"]
                body = await content.read_receipt_in(
                    conn, resolution["payload_id"], expected_deletion_epoch=epoch
                )
                header = await content.payloads._header_in(conn, resolution["payload_id"])
            assert body["reason"] == "ADMIN-REASON-CANARY"
            assert set(header["manifest"]["parents"]) == {
                receipt["input_payload_id"],
                original_part["payload_id"],
            }
        await erase(bot, receipt["operation_id"])
        replay = await post(client, endpoint, payload, "resolve")
        assert replay == {
            "operation_id": receipt["operation_id"],
            "action": "delivery.reconcile",
            "operation_status": "RECORDED",
            "result_payload_state": "UNAVAILABLE",
        }
        assert len(server["sends"]) == 1
        async with bot.store.pool.connection() as conn:
            after = await (
                await conn.execute(
                    "SELECT * FROM public.bot_delivery_resolutions WHERE operation_id=%s",
                    (receipt["operation_id"],),
                )
            ).fetchone()
            attempts = await (
                await conn.execute(
                    "SELECT * FROM public.bot_send_attempts WHERE intent_id=%s",
                    (part["intent_id"],),
                )
            ).fetchall()
        assert after == resolution and attempts == [attempt]
        if resolution["payload_id"]:
            async with bot.store.pool.connection() as conn, conn.transaction():
                epoch = (await content.payloads.lock_in(conn))["deletion_epoch"]
                with pytest.raises(ForestError) as caught:
                    await content.read_receipt_in(
                        conn, resolution["payload_id"], expected_deletion_epoch=epoch
                    )
                assert caught.value.code in {"PAYLOAD_ERASED", "SOURCE_SUPPRESSED"}
