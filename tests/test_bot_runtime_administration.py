"""Bot maintenance uses one reviewed CAS path for Web and group Shell."""

import asyncio

import pytest
from binding_seed import _ticket_factory
from psycopg.types.json import Jsonb
from test_config_writer_fence import resources
from test_group_scheduler import row, until
from test_messages import fails, owner, receive
from test_messages import messages as messages
from test_shell import allow, completed, dispatcher
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.storage import bot as bot_module
from forest.storage.bot import BotStore
from forest.storage.bot_runtime import BotRuntimeAdministration

pytestmark = pytest.mark.postgres


async def request(admin, ticket, operation, payload, lease):
    signed = await ticket("bot.maintenance", operation, payload)
    return await admin.set_maintenance(signed, operation, payload, lease=lease)


async def test_web_authority_pause_resume_is_atomic_replayable_and_fenced(messages):
    repository, lease, _ = messages
    ticket = await _ticket_factory(repository.store, lease)
    admin = BotRuntimeAdministration(repository.store)
    read_operation = "runtime-read"
    before = await admin.read(
        await ticket("bot.runtime.read", read_operation, {}), read_operation, {}
    )
    assert before["maintenance_epoch"] == 1 and before["data_admission"]
    first_input = await receive(repository, lease, "before-pause", trigger=True)
    actor = await owner(repository, lease, first_input["conversation_id"], "paused-actor")

    pause = {"expected_maintenance_epoch": 1, "data_admission": False, "reason": "provider maintenance"}
    first = await request(admin, ticket, "pause", pause, lease)
    assert first["maintenance_epoch"] == 2 and not first["data_admission"]
    assert await request(admin, ticket, "pause", pause, lease) == first

    await receive(repository, lease, "queued-paused", trigger=True)
    await fails("DATA_ADMISSION_CLOSED", repository.select_input(actor, "paused-input"))

    stale = {"expected_maintenance_epoch": 1, "data_admission": True, "reason": "stale restore"}
    with pytest.raises(ForestError) as conflict:
        await request(admin, ticket, "stale-resume", stale, lease)
    assert conflict.value.code == "MAINTENANCE_CONFLICT"

    resume = {"expected_maintenance_epoch": 2, "data_admission": True, "reason": "provider recovered"}
    restored = await request(admin, ticket, "resume", resume, lease)
    assert restored["maintenance_epoch"] == 3 and restored["data_admission"]
    await fails("STALE_ACTOR", repository.select_input(actor, "old-resumed-input"))
    actor = await owner(repository, lease, first_input["conversation_id"], "paused-actor")
    selected = await repository.select_input(actor, "resumed-input")
    assert len(selected["trigger_ids"]) == 2
    assert (await row(repository, "SELECT count(*) AS n FROM public.bot_web_operations "
                      "WHERE action='bot.maintenance'"))["n"] == 2


async def test_concurrent_maintenance_commands_converge_by_epoch(messages):
    repository, lease, _ = messages
    ticket = await _ticket_factory(repository.store, lease)
    admin = BotRuntimeAdministration(repository.store)
    payload = {"expected_maintenance_epoch": 1, "data_admission": False, "reason": "incident"}

    results = await asyncio.gather(
        request(admin, ticket, "pause-a", payload, lease),
        request(admin, ticket, "pause-b", payload, lease),
        return_exceptions=True,
    )
    successes = [value for value in results if isinstance(value, dict)]
    failures = [value for value in results if isinstance(value, ForestError)]
    assert len(successes) == len(failures) == 1
    assert successes[0]["maintenance_epoch"] == 2
    assert failures[0].code == "MAINTENANCE_CONFLICT"


async def test_group_shell_pause_and_resume_keep_intake_and_control_available(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    controls = dispatcher(repository, lease)
    try:
        pause = await receive(
            repository, lease, "shell-pause",
            text='#forest bot pause --expect-maintenance 1 --reason "provider maintenance"',
        )
        await controls.start()
        await until(lambda: completed(repository, 1))
        paused = await row(
            repository, "SELECT outcome FROM public.bot_shell_requests WHERE command_id=%s",
            (pause["command_id"],),
        )
        assert paused["outcome"] == {
            "code": "BOT_MAINTENANCE", "maintenance_epoch": 2, "data_admission": False,
        }

        queued = await receive(repository, lease, "while-paused", trigger=True)
        resume = await receive(
            repository, lease, "shell-resume",
            text='#forest bot resume --expect-maintenance 2 --reason "provider recovered"',
        )
        controls.notify()
        await until(lambda: completed(repository, 2))
        restored = await row(
            repository, "SELECT outcome FROM public.bot_shell_requests WHERE command_id=%s",
            (resume["command_id"],),
        )
        assert restored["outcome"] == {
            "code": "BOT_MAINTENANCE", "maintenance_epoch": 3, "data_admission": True,
        }
        actor = await owner(repository, lease, queued["conversation_id"], "shell-resumed-actor")
        selected = await repository.select_input(actor, "shell-resumed-input")
        assert len(selected["trigger_ids"]) == 1
        context = await repository.context(actor, run_id=selected["run_id"])
        rendered = str(context["units"])
        # The resume command arrived after the last Agent trigger, so this model
        # input stops at that trigger. It remains durable tail data for the next
        # safe point or the post-run silent drain.
        assert len(context["units"]) == 2
        assert "provider maintenance" not in rendered and "provider recovered" not in rendered
        assert rendered.count("管理命令") == 1
        assert (await repository.store.runtime())["data_admission"]
    finally:
        await controls.close()


async def test_p0_pause_uses_reserved_slot_while_p1_query_is_waiting(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    controls = dispatcher(repository, lease)
    controls.max_concurrency = 2
    entered, release = asyncio.Event(), asyncio.Event()
    original = controls._typed

    async def held(current_lease, claim):
        if claim["command_name"] == "run.read":
            entered.set()
            await release.wait()
        return await original(current_lease, claim)

    controls._typed = held
    try:
        await receive(repository, lease, "held-query", text="#forest run list")
        await controls.start()
        await asyncio.wait_for(entered.wait(), 3)
        await receive(
            repository, lease, "urgent-pause",
            text='#forest bot pause --expect-maintenance 1 --reason "urgent incident"',
        )
        controls.notify()
        await until(lambda: completed(repository, 1))
        assert not release.is_set()
        state = await repository.store.runtime()
        assert not state["data_admission"] and state["maintenance_epoch"] == 2
    finally:
        release.set()
        await controls.close()


async def test_053_upgrade_adds_only_implied_runtime_rights_and_keeps_disabled_issuer(
    upgrade_database, tmp_path, monkeypatch
):
    current_files = bot_module.files
    old = resources(tmp_path / "schema-052", cutoff="052")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old)
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        issuers = {
            "reader": (["message.read"], True),
            "cancel-only": (["message.cancel"], True),
            "apply-only": (["config.apply"], True),
            "operator": (["message.read", "message.cancel", "config.apply"], True),
            "disabled": (["message.read", "message.cancel", "config.apply"], False),
        }
        async with store.pool.connection() as conn:
            for issuer, (actions, enabled) in issuers.items():
                await conn.execute(
                    "INSERT INTO public.bot_web_issuers"
                    "(bot_id,issuer_id,key_revision,public_key,enabled,actions) "
                    "VALUES (%s,%s,1,'unused-test-key',%s,%s)",
                    (store.bot_id, issuer, enabled, Jsonb(actions)),
                )
        monkeypatch.setattr(bot_module, "files", current_files)
        await store.migrate()
        async with store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT issuer_id,enabled,actions FROM public.bot_web_issuers ORDER BY issuer_id"
                )
            ).fetchall()
        upgraded = {item["issuer_id"]: set(item["actions"]) for item in rows}
        assert upgraded["reader"] == {
            "message.read", "bot.runtime.read", "event.read", "component_job.read",
        }
        assert upgraded["cancel-only"] == {"message.cancel"}
        assert upgraded["apply-only"] == {"config.apply"}
        assert upgraded["operator"] == {
            "message.read", "message.cancel", "config.apply", "bot.runtime.read", "bot.maintenance",
            "event.read", "event.retire.apply", "component_job.read",
            "component_job.retire.apply",
        }
        assert upgraded["disabled"] == upgraded["operator"]
        assert not next(item for item in rows if item["issuer_id"] == "disabled")["enabled"]
    finally:
        await store.close()
