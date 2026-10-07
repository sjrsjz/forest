import asyncio
from types import SimpleNamespace

from forest.errors import ForestError
from forest.multi_bot_supervisor import MultiBotSupervisor, _BotRunner


async def test_failed_bot_drains_admitted_management_before_closing_host():
    online = asyncio.Event()
    failed = asyncio.Event()
    close_called = asyncio.Event()
    trigger_failure = asyncio.Event()
    observed = []

    class Catalog:
        async def observe(self, _lease, *, observed_state, **_kwargs):
            observed.append(observed_state)
            if observed_state == "ONLINE":
                online.set()
            elif observed_state == "FAILED":
                failed.set()

    class StorageProfiles:
        async def resolve(self, **_kwargs):
            return SimpleNamespace(dsn="postgresql://test")

    class Host:
        def __init__(self, settings):
            self.settings = settings
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/test")
            )
            self._supervision_task = None

        async def start(self):
            async def fail():
                await trigger_failure.wait()
                raise ForestError("BOT_TEST_FAILURE", "Test runtime failed", 503)

            self._supervision_task = asyncio.create_task(fail())

        async def close(self):
            close_called.set()

    supervisor = MultiBotSupervisor(
        Catalog(),
        storage_profiles=StorageProfiles(),
        settings_resolver=lambda bot, storage: SimpleNamespace(
            bot_id=bot["bot_id"], dsn=storage.dsn
        ),
        host_factory=Host,
        host_id="host/lifecycle-test",
    )
    supervisor.lease = object()
    runner = _BotRunner(asyncio.Event())
    bot = {
        "bot_id": "bot/test",
        "storage_profile_ref": "storage/test",
        "storage_generation": 1,
        "intent_revision": 2,
    }
    supervisor._runners[bot["bot_id"]] = runner
    supervisor._scope_bots[supervisor.management_scope_id(bot["bot_id"])] = bot["bot_id"]
    task = asyncio.create_task(supervisor._run_bot(bot, runner))
    try:
        await asyncio.wait_for(online.wait(), timeout=2)
        async with supervisor.management_host(supervisor.management_scope_id(bot["bot_id"])):
            trigger_failure.set()
            async with asyncio.timeout(2):
                while runner.management_accepting:
                    await asyncio.sleep(0)
            assert not close_called.is_set()
            assert not failed.is_set()
        await asyncio.wait_for(task, timeout=2)
        assert close_called.is_set()
        assert failed.is_set()
        assert observed == ["STARTING", "ONLINE", "FAILED"]
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_stop_during_start_never_publishes_online_observation():
    starting = asyncio.Event()
    finish_start = asyncio.Event()
    observed = []
    closed = asyncio.Event()

    class Catalog:
        async def observe(self, _lease, *, observed_state, **_kwargs):
            observed.append(observed_state)

    class StorageProfiles:
        async def resolve(self, **_kwargs):
            return SimpleNamespace(dsn="postgresql://test")

    class Host:
        def __init__(self, _settings):
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/test")
            )
            self._supervision_task = None

        async def start(self):
            starting.set()
            await finish_start.wait()

        async def close(self):
            closed.set()

    supervisor = MultiBotSupervisor(
        Catalog(), storage_profiles=StorageProfiles(),
        settings_resolver=lambda bot, storage: SimpleNamespace(
            bot_id=bot["bot_id"], dsn=storage.dsn,
        ), host_factory=Host, host_id="host/lifecycle-test",
    )
    supervisor.lease = object()
    runner = _BotRunner(asyncio.Event())
    bot = {
        "bot_id": "bot/test",
        "storage_profile_ref": "storage/test",
        "storage_generation": 1,
        "intent_revision": 2,
    }
    task = asyncio.create_task(supervisor._run_bot(bot, runner))
    try:
        await asyncio.wait_for(starting.wait(), timeout=2)
        runner.stop.set()
        finish_start.set()
        await asyncio.wait_for(task, timeout=2)
        assert closed.is_set()
        assert observed == ["STARTING", "STOPPING", "OFFLINE"]
        assert runner.host is None
        assert not runner.management_accepting
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_retirement_does_not_claim_offline_when_host_close_fails():
    observed = []

    class Catalog:
        async def observe(self, _lease, *, observed_state, error_code=None, **_kwargs):
            observed.append((observed_state, error_code))
            if observed_state == "ONLINE":
                raise ForestError("BOT_INTENT_CHANGED", "Bot is retiring", 409)

    class StorageProfiles:
        async def resolve(self, **_kwargs):
            return SimpleNamespace(dsn="postgresql://test")

    class Host:
        def __init__(self, _settings):
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/test")
            )
            self._supervision_task = None

        async def start(self):
            pass

        async def close(self):
            raise RuntimeError("close failed")

    supervisor = MultiBotSupervisor(
        Catalog(), storage_profiles=StorageProfiles(),
        settings_resolver=lambda bot, storage: SimpleNamespace(
            bot_id=bot["bot_id"], dsn=storage.dsn,
        ), host_factory=Host, host_id="host/lifecycle-test",
    )
    supervisor.lease = object()
    runner = _BotRunner(asyncio.Event())
    bot = {
        "bot_id": "bot/test", "storage_profile_ref": "storage/test",
        "storage_generation": 1, "intent_revision": 2,
    }
    await asyncio.wait_for(supervisor._run_bot(bot, runner), timeout=2)
    assert observed == [
        ("STARTING", None), ("ONLINE", None),
        ("FAILED", "BOT_RUNTIME_CLOSE_FAILED"),
    ]
