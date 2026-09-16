"""Current clean baseline and writer-generation gate helpers."""

from importlib.resources import files

import pytest
from test_upgrade import upgrade_database as upgrade_database

from forest.storage import bot as bot_module
from forest.storage.bot import BotStore

FENCE = "042_schema_writer_v2.sql"
CURRENT_FENCE = "087_memory_profile_lifecycle.sql"


def resources(path, *, cutoff="042"):
    source = files("forest.storage")
    (path / "bot_migrations").mkdir(parents=True)
    (path / "data_migrations").mkdir()
    for item in source.joinpath("bot_migrations").iterdir():
        if item.name.endswith(".sql") and item.name[:3] <= cutoff:
            (path / "bot_migrations" / item.name).write_text(item.read_text(encoding="utf-8"), encoding="utf-8")
    for name in bot_module.MIGRATION_HANDLERS.values():
        item = source.joinpath("data_migrations", name + ".py")
        (path / "data_migrations" / item.name).write_text(item.read_text(encoding="utf-8"), encoding="utf-8")
    for name, introduced in (
        ("migration_v1.py", "001"), ("migration_v2.py", "042"), ("migration_v3.py", "060"),
        ("migration_v4.py", "062"), ("migration_v5.py", "064"),
        ("migration_v6.py", "066"),
        ("migration_v7.py", "067"),
        ("migration_v8.py", "068"),
        ("migration_v9.py", "069"),
        ("migration_v10.py", "070"),
        ("migration_v11.py", "071"),
        ("migration_v12.py", "072"),
        ("migration_v13.py", "074"),
        ("migration_v14.py", "075"),
        ("migration_v15.py", "076"),
        ("migration_v16.py", "077"),
        ("migration_v17.py", "078"),
        ("migration_v18.py", "079"),
        ("migration_v19.py", "080"),
        ("migration_v20.py", "081"),
        ("migration_v21.py", "082"),
        ("migration_v22.py", "083"),
        ("migration_v23.py", "084"),
        ("migration_v24.py", "085"),
        ("migration_v25.py", "086"),
        ("migration_v26.py", "087"),
    ):
        if cutoff >= introduced:
            (path / name).write_text(source.joinpath(name).read_text(encoding="utf-8"), encoding="utf-8")
    if cutoff >= "062":
        (path / "shell_payloads.py").write_text(
            source.joinpath("shell_payloads.py").read_text(encoding="utf-8"), encoding="utf-8"
        )
        (path / "shell_registry.py").write_text(
            source.parent.joinpath("shell_registry.py").read_text(encoding="utf-8"), encoding="utf-8"
        )
    if cutoff >= "064":
        (path / "shell_payloads_v7.py").write_text(
            source.joinpath("shell_payloads_v7.py").read_text(encoding="utf-8"), encoding="utf-8"
        )
        (path / "shell_registry_v7.py").write_text(
            source.parent.joinpath("shell_registry_v7.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "066":
        (path / "shell_payloads_v8.py").write_text(
            source.joinpath("shell_payloads_v8.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v8.py").write_text(
            source.parent.joinpath("shell_registry_v8.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "067":
        (path / "shell_payloads_v9.py").write_text(
            source.joinpath("shell_payloads_v9.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v9.py").write_text(
            source.parent.joinpath("shell_registry_v9.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "068":
        (path / "shell_payloads_v10.py").write_text(
            source.joinpath("shell_payloads_v10.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        for name in ("shell_registry_v10.py", "shell_authority.py"):
            (path / name).write_text(
                source.parent.joinpath(name).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
    if cutoff >= "069":
        (path / "shell_payloads_v11.py").write_text(
            source.joinpath("shell_payloads_v11.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v11.py").write_text(
            source.parent.joinpath("shell_registry_v11.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    if cutoff >= "071":
        (path / "shell_payloads_v12.py").write_text(
            source.joinpath("shell_payloads_v12.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (path / "shell_registry_v12.py").write_text(
            source.parent.joinpath("shell_registry_v12.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    return path


async def state(store):
    async with store.pool.connection() as conn:
        gate = await (await conn.execute("SELECT minimum_generation FROM public.bot_schema_writer_gate")).fetchone()
        host = await (await conn.execute("SELECT * FROM public.bot_host_lease")).fetchone()
        runtime = await (await conn.execute("SELECT * FROM public.bot_runtime_state")).fetchone()
        ledger = await (await conn.execute("SELECT * FROM public.forest_bot_schema_migrations ORDER BY version")).fetchall()
        plans = await (await conn.execute("SELECT * FROM public.bot_config_plans ORDER BY plan_id")).fetchall()
    return {"gate": gate["minimum_generation"], "host": host, "runtime": runtime, "ledger": ledger, "plans": plans}


@pytest.mark.postgres
async def test_fresh_database_crosses_all_gates_and_noop_preserves_live_host(upgrade_database):
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("new", "fresh", ttl_seconds=60)
        before = await state(store)
        assert before["gate"] == 26
        assert {FENCE, CURRENT_FENCE} <= {row["version"] for row in before["ledger"]}
        assert {
            "019_children.sql", "032_child_payloads.sql", "050_single_agent_protocol.sql"
        }.isdisjoint(row["version"] for row in before["ledger"])
        assert before["runtime"]["storage_generation"] == 1
        async with store.pool.connection() as conn:
            retired = await (
                await conn.execute(
                    "SELECT to_regclass('public.bot_agent_children') AS children,"
                    "to_regclass('public.bot_child_model_attempts') AS attempts,"
                    "to_regclass('public.bot_agent_run_budgets') AS run_budgets"
                )
            ).fetchone()
        assert retired == {"children": None, "attempts": None, "run_budgets": "bot_agent_run_budgets"}
        await store.migrate()
        assert await state(store) == before
        assert (await store.renew_host(lease)).owner_epoch == lease.owner_epoch
    finally:
        await store.close()
