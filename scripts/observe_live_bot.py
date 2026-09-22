"""Read-only soak observer for one running production Bot.

Run this inside the candidate image with the Supervisor's read-only Secret
mount and deployment network. Output contains identities, states and counts;
it never emits credentials, payload bodies, message text or author IDs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from urllib.request import urlopen

from psycopg import sql

from forest.clean_supervisor_host import CleanSupervisorSettings
from forest.schema_bundles import module_schema_namespace
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority

RUNTIME = module_schema_namespace("forest.kernel.runtime")
ONEBOT = module_schema_namespace("forest.adapter.onebot")
MEMORY = module_schema_namespace("forest.memory.graph")
AGENT = module_schema_namespace("forest.agent.runtime")
DELIVERY = module_schema_namespace("forest.delivery")
JOBS = module_schema_namespace("forest.job.scheduler")


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Observe one live Bot without reading content.")
    value.add_argument("--bot-id", required=True)
    value.add_argument("--duration-seconds", type=int, default=3600)
    value.add_argument("--interval-seconds", type=float, default=30.0)
    value.add_argument("--stale-seconds", type=int, default=300)
    value.add_argument("--health-url", default="http://supervisor:8081/")
    return value


def _validate(arguments: argparse.Namespace) -> None:
    if not 1 <= arguments.duration_seconds <= 604800:
        raise SystemExit("--duration-seconds must be between 1 and 604800")
    if not 0.25 <= arguments.interval_seconds <= 3600:
        raise SystemExit("--interval-seconds must be between 0.25 and 3600")
    if not 1 <= arguments.stale_seconds <= 86400:
        raise SystemExit("--stale-seconds must be between 1 and 86400")


async def _deployment_state(store, bot_id: str) -> dict:
    async with store.pool.connection() as connection:
        row = await (
            await connection.execute(
                "SELECT b.supply_state,i.desired_state,o.observed_state,o.error_code "
                "FROM forest_deployment.bots b "
                "JOIN forest_deployment.runtime_intents i USING(bot_id) "
                "LEFT JOIN forest_deployment.runtime_observations o USING(bot_id) "
                "WHERE b.bot_id=%s",
                (bot_id,),
            )
        ).fetchone()
    if row is None:
        raise RuntimeError("BOT_NOT_FOUND")
    return dict(row)


async def _bot_state(store: CleanBotStore, bot_id: str, stale_seconds: int) -> dict:
    async with store.pool.connection() as connection:
        async with connection.transaction():
            await connection.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            bundles = await (
                await connection.execute("SELECT count(*) AS count FROM forest_kernel.schema_bundles")
            ).fetchone()
            graph = await (
                await connection.execute(
                    sql.SQL("SELECT graph_id,graph_revision FROM {}.runtime_graph_head WHERE bot_id=%s").format(
                        sql.Identifier(RUNTIME)
                    ),
                    (bot_id,),
                )
            ).fetchone()
            if graph is None:
                raise RuntimeError("RUNTIME_GRAPH_HEAD_MISSING")
            members = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT count(*) AS members FROM {}.runtime_graph_modules "
                        "WHERE bot_id=%s AND graph_id=%s"
                    ).format(sql.Identifier(RUNTIME)),
                    (bot_id, graph["graph_id"]),
                )
            ).fetchone()
            lifecycle = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT count(*) AS recorded,count(*) FILTER (WHERE state='ACTIVE') AS active "
                        "FROM (SELECT DISTINCT ON (component_id) component_id,state FROM {}.runtime_graph_lifecycle_facts "
                        "WHERE bot_id=%s AND graph_id=%s ORDER BY component_id,sequence DESC) latest"
                    ).format(sql.Identifier(RUNTIME)),
                    (bot_id, graph["graph_id"]),
                )
            ).fetchone()
            binding = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT state,desired_revision,effective_revision,receive_enabled,error_code,"
                        "current_client_incarnation FROM {}.binding_heads "
                        "WHERE bot_id=%s AND binding_id='binding/main'"
                    ).format(sql.Identifier(ONEBOT)),
                    (bot_id,),
                )
            ).fetchone()
            client = None
            if binding is not None and binding["current_client_incarnation"] is not None:
                client = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT state,error_code FROM {}.binding_clients "
                            "WHERE bot_id=%s AND client_incarnation=%s"
                        ).format(sql.Identifier(ONEBOT)),
                        (bot_id, binding["current_client_incarnation"]),
                    )
                ).fetchone()
            memory = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT "
                        "(SELECT count(*) FROM {m}.memory_trunks WHERE bot_id=%s) AS trunks,"
                        "(SELECT count(*) FROM {m}.source_units WHERE bot_id=%s) AS sources,"
                        "(SELECT count(*) FROM {m}.embedding_facets WHERE bot_id=%s) AS facets,"
                        "(SELECT count(*) FROM {m}.embedding_facets WHERE bot_id=%s AND state='READY') AS ready_facets,"
                        "(SELECT count(*) FROM {m}.embedding_attempts WHERE bot_id=%s AND state IN ('PREPARED','DISPATCHED') "
                        " AND COALESCE(dispatched_at,prepared_at)<clock_timestamp()-(%s * interval '1 second')) AS stale_attempts,"
                        "(SELECT count(*) FROM {m}.embedding_budget_reservations WHERE bot_id=%s AND state='RESERVED' "
                        " AND created_at<clock_timestamp()-(%s * interval '1 second')) AS stale_reservations"
                    ).format(m=sql.Identifier(MEMORY)),
                    (bot_id, bot_id, bot_id, bot_id, bot_id, stale_seconds, bot_id, stale_seconds),
                )
            ).fetchone()
            concurrency = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT count(*) AS overlapping_sessions FROM ("
                        "SELECT session_id FROM {}.agent_runs WHERE bot_id=%s AND terminal_at IS NULL "
                        "GROUP BY session_id HAVING count(*)>1) overlapping"
                    ).format(sql.Identifier(AGENT)),
                    (bot_id,),
                )
            ).fetchone()
            work = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT "
                        "(SELECT count(*) FROM {a}.agent_runs WHERE bot_id=%s AND terminal_at IS NULL) AS active_runs,"
                        "(SELECT count(*) FROM {d}.send_intents WHERE bot_id=%s AND state NOT IN ('SENT','FAILED','CANCELLED')) AS pending_sends,"
                        "(SELECT count(*) FROM {j}.jobs WHERE bot_id=%s AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN')) AS active_jobs"
                    ).format(a=sql.Identifier(AGENT), d=sql.Identifier(DELIVERY), j=sql.Identifier(JOBS)),
                    (bot_id, bot_id, bot_id),
                )
            ).fetchone()
    return {
        "schema_bundles": bundles["count"],
        "graph_id": graph["graph_id"],
        "graph_revision": graph["graph_revision"],
        "members": members["members"],
        "lifecycle_recorded": lifecycle["recorded"],
        "active_members": lifecycle["active"],
        "binding": None
        if binding is None
        else {
            "state": binding["state"],
            "desired_revision": binding["desired_revision"],
            "effective_revision": binding["effective_revision"],
            "receive_enabled": binding["receive_enabled"],
            "error_code": binding["error_code"],
        },
        "client": None if client is None else dict(client),
        "memory": dict(memory),
        "concurrency": dict(concurrency),
        "work": dict(work),
    }


def _health(url: str) -> int:
    with urlopen(url, timeout=5) as response:
        return response.status


def _violations(deployment: dict, bot: dict, health_status: int) -> list[str]:
    failures = []
    if deployment["supply_state"] != "READY" or deployment["desired_state"] != "ONLINE":
        failures.append("DEPLOYMENT_INTENT_INVALID")
    if deployment["observed_state"] != "ONLINE" or deployment["error_code"] is not None:
        failures.append("DEPLOYMENT_OBSERVATION_INVALID")
    if bot["schema_bundles"] != 19 or bot["members"] != 19:
        failures.append("RUNTIME_GRAPH_CARDINALITY_INVALID")
    if bot["lifecycle_recorded"] != 19 or bot["active_members"] != 19:
        failures.append("RUNTIME_LIFECYCLE_INVALID")
    binding = bot["binding"] or {}
    if (
        binding.get("state") != "ONLINE"
        or not binding.get("receive_enabled")
        or binding.get("error_code") is not None
        or binding.get("desired_revision") != binding.get("effective_revision")
    ):
        failures.append("ONEBOT_BINDING_INVALID")
    client = bot["client"] or {}
    if client.get("state") != "ACTIVE" or client.get("error_code") is not None:
        failures.append("ONEBOT_CLIENT_INVALID")
    if bot["memory"]["stale_attempts"] or bot["memory"]["stale_reservations"]:
        failures.append("EMBEDDING_CONVERGENCE_STALE")
    if bot["concurrency"]["overlapping_sessions"]:
        failures.append("AGENT_SESSION_CONCURRENCY_INVALID")
    if health_status != 200:
        failures.append("MANAGEMENT_HTTP_INVALID")
    return failures


async def observe(arguments: argparse.Namespace) -> int:
    settings = CleanSupervisorSettings.from_environment()
    deployment = CleanDeploymentStore(settings.dsn, settings.deployment_id)
    await deployment.open()
    bot_store = None
    all_failures: set[str] = set()
    sample_count = 0
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        secrets = DeploymentSecretAuthority(
            deployment,
            active_key_id=settings.master_keyring.active_key_id,
            key_resolver=settings.master_keyring.resolve,
        )
        catalog = DeploymentCatalogRepository(deployment)
        profiles = DeploymentStorageProfileAuthority(deployment, secrets=secrets)
        bot = await catalog.inspect(arguments.bot_id)
        storage = await profiles.resolve(
            profile_ref=bot["storage_profile_ref"],
            expected_bot_id=arguments.bot_id,
            expected_storage_generation=bot["storage_generation"],
        )
        bot_store = CleanBotStore(storage.dsn, arguments.bot_id)
        await bot_store.open()
        while True:
            deployment_state, bot_state, health_status = await asyncio.gather(
                _deployment_state(deployment, arguments.bot_id),
                _bot_state(bot_store, arguments.bot_id, arguments.stale_seconds),
                asyncio.to_thread(_health, arguments.health_url),
            )
            failures = _violations(deployment_state, bot_state, health_status)
            all_failures.update(failures)
            sample_count += 1
            print(
                json.dumps(
                    {
                        "protocol": "forest.live-bot-observation/1",
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                        "sample": sample_count,
                        "deployment": deployment_state,
                        "bot": bot_state,
                        "management_http_status": health_status,
                        "violations": failures,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )
            elapsed = time.monotonic() - started
            if elapsed >= arguments.duration_seconds:
                break
            await asyncio.sleep(min(arguments.interval_seconds, arguments.duration_seconds - elapsed))
    finally:
        if bot_store is not None:
            await bot_store.close()
        await deployment.close()
    print(
        json.dumps(
            {
                "protocol": "forest.live-bot-observation-summary/1",
                "bot_id": arguments.bot_id,
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": round(time.monotonic() - started, 3),
                "samples": sample_count,
                "violations": sorted(all_failures),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    return 1 if all_failures else 0


def main() -> None:
    arguments = parser().parse_args()
    _validate(arguments)
    raise SystemExit(asyncio.run(observe(arguments)))


if __name__ == "__main__":
    main()
