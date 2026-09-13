"""Real HTTP and PG: erased admissions cannot restart cross-boundary effects."""

import pytest
from test_web_bots import (
    ORIGIN,
    BootstrapState,
    WebSettings,
    apply,
    effective,
    invoke,
    make_plan,
    post,
    ready,
    running,
    setup,
)
from test_web_bots import web_bots_database as web_bots_database

from forest.errors import ForestError
from forest.management.binding_catalog import BindingCatalog
from forest.memory_types import SourceRef

pytestmark = pytest.mark.postgres


async def erase(bot, operation):
    content = bot.control.content
    async with bot.store.pool.connection() as conn:
        domain = await (
            await conn.execute(
                "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='CONTROL_INPUT' AND owner_id=%s",
                (content._owner("input", operation),),
            )
        ).fetchone()
        clock = await (
            await conn.execute("SELECT deletion_epoch FROM public.bot_memory_clock")
        ).fetchone()
    await content.payloads.forget_sources(
        bot.kernel._authority(),
        "erase:" + operation,
        (SourceRef(domain["source_unit_id"]),),
        expected_deletion_epoch=clock["deletion_epoch"],
    )
    await content.payloads.scrub(bot.kernel._authority())


async def test_erased_web_service_admission_and_binding_reservation_do_not_finish_effects(
    tmp_path, web_bots_database, monkeypatch
):
    data = tmp_path / "state"
    token = BootstrapState(data).initialize()
    settings = WebSettings(data, ORIGIN, managed_dsn=web_bots_database)
    async with running(settings) as (app, client):
        await setup(client, token)
        created = await post(
            client,
            "/management/v1/bots",
            {"display_name": "Receipt boundaries"},
            "create",
            status=202,
        )
        await ready(client, created)
        root = "/management/v1/bots/" + created["bot_id"]
        base = root + "/components/forest.counter"
        package = next(
            item
            for item in (await client.get("/management/v1/packages")).json()["packages"]
            if item["component_id"] == "forest.counter"
        )
        plan = await make_plan(client, base, package, "Counter", 1, 0, "plan")
        await apply(client, base, plan, "apply")
        await effective(client, base, 1)
        actual = await invoke(client, base, 2, "normal")
        bot = app.state.runtime.fleet.bots[created["bot_id"]]
        async with bot.store.pool.connection() as conn:
            receipt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_web_operations WHERE operation_id=%s",
                    (actual["operation_id"],),
                )
            ).fetchone()
            call = await (
                await conn.execute(
                    "SELECT h.manifest FROM public.bot_calls c JOIN public.bot_payload_headers h ON h.bot_id=c.bot_id AND h.payload_id=c.input_payload_id WHERE c.operation_id=%s",
                    (actual["operation_id"],),
                )
            ).fetchone()
            assert receipt["result_payload_id"] in call["manifest"]["parents"]
        count = 0

        async def unreachable(*args, **kwargs):
            nonlocal count
            count += 1
            raise ForestError("TEST_GAP", "Stopped before Kernel admission", 503)

        monkeypatch.setattr(bot.kernel, "invoke", unreachable)
        await invoke(client, base, 3, "gap", status=503)
        assert count == 1
        async with bot.store.pool.connection() as conn:
            receipt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_web_operations WHERE action='service.invoke' AND operation_id<>%s",
                    (actual["operation_id"],),
                )
            ).fetchone()
            assert (
                await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM public.bot_calls WHERE operation_id=%s",
                        (receipt["operation_id"],),
                    )
                ).fetchone()
            )["n"] == 0
        await erase(bot, receipt["operation_id"])
        replay = await invoke(client, base, 3, "gap")
        assert replay == {
            "operation_id": receipt["operation_id"],
            "action": "service.invoke",
            "operation_status": "RECORDED",
            "result_payload_state": "UNAVAILABLE",
        }
        assert count == 1

        registered_calls = 0

        async def registration_gap(*args, **kwargs):
            nonlocal registered_calls
            registered_calls += 1
            raise ForestError("TEST_REGISTRATION_GAP", "Stopped before directory registration", 503)

        monkeypatch.setattr(BindingCatalog, "registered", registration_gap)
        body = {"self_id": "991100", "display_name": "Account"}
        await post(client, root + "/bindings", body, "binding", status=503)
        async with bot.store.pool.connection() as conn:
            creation = await (
                await conn.execute(
                    "SELECT * FROM public.bot_web_operations WHERE action='binding.create'"
                )
            ).fetchone()
        async with app.state.runtime.catalog.store.pool.connection() as conn:
            before = await (
                await conn.execute(
                    "SELECT * FROM platform_bindings WHERE operation_id=%s",
                    (creation["operation_id"],),
                )
            ).fetchone()
        assert before["stage"] == "RESERVED" and registered_calls == 1
        await erase(bot, creation["operation_id"])
        replay = await post(client, root + "/bindings", body, "binding", status=201)
        assert (
            replay["result_payload_state"] == "UNAVAILABLE"
            and replay["operation_status"] == "RECORDED"
        )
        assert registered_calls == 1
        async with app.state.runtime.catalog.store.pool.connection() as conn:
            after = await (
                await conn.execute(
                    "SELECT * FROM platform_bindings WHERE operation_id=%s",
                    (creation["operation_id"],),
                )
            ).fetchone()
        assert after == before
