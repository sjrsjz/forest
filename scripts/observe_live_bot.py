"""Read-only soak observer for one running production Bot.

Run this inside the candidate image with the Supervisor's read-only Secret
mount and deployment network. Output contains identities, states and counts;
it never emits credentials, payload bodies, message text or author IDs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from datetime import datetime, timezone
from urllib.request import urlopen

from psycopg import sql
from psycopg.errors import UndefinedTable

from forest.deployment_access_settings import DeploymentAccessSettings
from forest.schema_bundles import module_schema_namespace
from forest.storage.bot_store import BotStore
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_store import DeploymentStore

RUNTIME = module_schema_namespace("forest.kernel.runtime")
CONVERSATION = module_schema_namespace("forest.conversation.scheduler")
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
    value.add_argument("--group-id", help="Inspect one OneBot group lane without reading message content.")
    value.add_argument(
        "--embedding-diagnostics", action="store_true",
        help="Include the last hour of embedding work failure codes (costlier read).",
    )
    value.add_argument(
        "--component-id", action="append", default=[],
        help="Include one current Graph component's Generation identity (at most eight).",
    )
    value.add_argument(
        "--deployment-component-id", action="append", default=[],
        help="Require one active deployment Graph component (at most eight).",
    )
    return value


def _validate(arguments: argparse.Namespace) -> None:
    if not 1 <= arguments.duration_seconds <= 604800:
        raise SystemExit("--duration-seconds must be between 1 and 604800")
    if not 0.25 <= arguments.interval_seconds <= 3600:
        raise SystemExit("--interval-seconds must be between 0.25 and 3600")
    if not 1 <= arguments.stale_seconds <= 86400:
        raise SystemExit("--stale-seconds must be between 1 and 86400")
    if arguments.group_id is not None and re.fullmatch(r"[1-9][0-9]{0,19}", arguments.group_id) is None:
        raise SystemExit("--group-id must be a positive decimal OneBot group ID")
    if len(arguments.component_id) > 8 or len(set(arguments.component_id)) != len(arguments.component_id):
        raise SystemExit("--component-id accepts at most eight distinct components")
    if any(re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", item) is None
           for item in arguments.component_id):
        raise SystemExit("--component-id must be a valid component ID")
    if (len(arguments.deployment_component_id) > 8 or
            len(set(arguments.deployment_component_id)) != len(arguments.deployment_component_id)):
        raise SystemExit("--deployment-component-id accepts at most eight distinct components")
    if any(re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", item) is None
           for item in arguments.deployment_component_id):
        raise SystemExit("--deployment-component-id must be a valid component ID")


async def _deployment_state(
    store, bot_id: str, component_ids: tuple[str, ...] = (),
) -> dict:
    async with store.pool.connection() as connection:
        async with connection.transaction():
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
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
            graph = None
            if component_ids:
                schema = await (
                    await connection.execute(
                        "SELECT to_regclass('forest_deployment.runtime_graph_clock') "
                        "AS graph_clock"
                    )
                ).fetchone()
                if schema["graph_clock"] is None:
                    raise RuntimeError(
                        "DEPLOYMENT_SCHEMA_INCOMPATIBLE: runtime_graph_clock is absent; "
                        "install the candidate deployment baseline before checking packages"
                    )
                graph = await (
                    await connection.execute(
                        "SELECT c.active_graph_id,g.graph_revision,g.state "
                        "FROM forest_deployment.runtime_graph_clock c "
                        "LEFT JOIN forest_deployment.runtime_graphs g "
                        "ON g.deployment_id=c.deployment_id AND g.graph_id=c.active_graph_id "
                        "WHERE c.deployment_id=%s",
                        (store.deployment_id,),
                    )
                ).fetchone()
                members = await (
                    await connection.execute(
                        "SELECT m.component_id,m.generation_id,m.control_admission,"
                        "m.data_admission,g.state AS generation_state "
                        "FROM forest_deployment.runtime_graph_clock c "
                        "JOIN forest_deployment.runtime_graph_members m "
                        "ON m.deployment_id=c.deployment_id AND m.graph_id=c.active_graph_id "
                        "JOIN forest_deployment.runtime_generations g "
                        "ON g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                        "WHERE c.deployment_id=%s AND m.component_id=ANY(%s) "
                        "ORDER BY m.component_id",
                        (store.deployment_id, list(component_ids)),
                    )
                ).fetchall()
    if row is None:
        raise RuntimeError("BOT_NOT_FOUND")
    result = dict(row)
    if component_ids:
        result["graph"] = {
            "graph_id": graph["active_graph_id"] if graph else None,
            "graph_revision": graph["graph_revision"] if graph else None,
            "state": graph["state"] if graph else None,
            "components": {component_id: None for component_id in component_ids},
        }
        for member in members:
            result["graph"]["components"][member["component_id"]] = {
                "generation_id": member["generation_id"],
                "state": member["generation_state"],
                "control_admission": member["control_admission"],
                "data_admission": member["data_admission"],
            }
    return result


async def _bot_state(
    store: BotStore, bot_id: str, stale_seconds: int, group_id: str | None = None,
    component_ids: tuple[str, ...] = (), *, embedding_diagnostics: bool = False,
) -> dict:
    async with store.pool.connection() as connection:
        async with connection.transaction():
            await connection.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            bundles = await (
                await connection.execute(
                    "SELECT count(*) AS count,"
                    "count(*) FILTER (WHERE state='READY') AS ready "
                    "FROM forest_kernel.schema_bundles WHERE bot_id=%s",
                    (bot_id,),
                )
            ).fetchone()
            memory_attempt_table = await (
                await connection.execute(
                    "SELECT to_regclass(%s) AS table_name",
                    (f"{MEMORY}.embedding_batch_attempts",),
                )
            ).fetchone()
            if memory_attempt_table["table_name"] is None:
                raise RuntimeError(
                    "BOT_SCHEMA_INCOMPATIBLE: embedding_batch_attempts is absent; "
                    "run this observer against a Bot installed from the candidate baseline"
                )
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
            components = None
            if component_ids:
                rows = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT component_id,generation_id,package_version,package_digest "
                            "FROM {}.runtime_graph_modules WHERE bot_id=%s AND graph_id=%s "
                            "AND component_id=ANY(%s) ORDER BY component_id"
                        ).format(sql.Identifier(RUNTIME)),
                        (bot_id, graph["graph_id"], list(component_ids)),
                    )
                ).fetchall()
                components = {component_id: None for component_id in component_ids}
                for row in rows:
                    components[row["component_id"]] = {
                        "generation_id": row["generation_id"],
                        "package_version": row["package_version"],
                        "package_digest": row["package_digest"],
                    }
            members = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT count(*) AS members FROM {}.runtime_graph_modules "
                        "WHERE bot_id=%s AND graph_id=%s"
                    ).format(sql.Identifier(RUNTIME)),
                    (bot_id, graph["graph_id"]),
                )
            ).fetchone()
            unmatched_bundles = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT count(*) AS count FROM forest_kernel.schema_bundles b "
                        "WHERE b.bot_id=%s AND b.state='READY' AND NOT EXISTS ("
                        "SELECT 1 FROM {}.runtime_graph_modules m "
                        "WHERE m.bot_id=b.bot_id AND m.graph_id=%s "
                        "AND m.component_id=b.component_id)"
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
            memory = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT "
                        "(SELECT count(*) FROM {m}.memory_trunks WHERE bot_id=%s) AS trunks,"
                        "(SELECT count(*) FROM {m}.source_units WHERE bot_id=%s) AS sources,"
                        "(SELECT count(*) FROM {m}.embedding_facets WHERE bot_id=%s) AS facets,"
                        "(SELECT count(*) FROM {m}.embedding_facets WHERE bot_id=%s AND state='READY') AS ready_facets,"
                        "(SELECT count(*) FROM {m}.embedding_facets WHERE bot_id=%s "
                        " AND purpose='DOCUMENT' AND state='PENDING' "
                        " AND next_attempt_at<=clock_timestamp()) AS pending_embedding_due,"
                        "(SELECT count(*) FROM {m}.embedding_facets WHERE bot_id=%s "
                        " AND purpose='DOCUMENT' AND state='PENDING' "
                        " AND next_attempt_at<clock_timestamp()-interval '5 minutes') "
                        "AS pending_embedding_overdue,"
                        "(SELECT extract(epoch FROM min(next_attempt_at)-clock_timestamp())::bigint "
                        " FROM {m}.embedding_facets WHERE bot_id=%s "
                        " AND purpose='DOCUMENT' AND state='PENDING') "
                        "AS next_pending_embedding_seconds,"
                        "(SELECT extract(epoch FROM clock_timestamp()-min(updated_at))::bigint "
                        " FROM {m}.embedding_facets WHERE bot_id=%s "
                        " AND purpose='DOCUMENT' AND state='UNKNOWN') "
                        "AS oldest_unknown_embedding_seconds,"
                        "(SELECT count(*) FROM {m}.embedding_batch_attempts WHERE bot_id=%s "
                        " AND state IN ('PREPARED','DISPATCHED') "
                        " AND deadline_at<clock_timestamp()-(%s * interval '1 second')) AS stale_attempts,"
                        "(SELECT count(*) FROM {m}.embedding_budget_reservations WHERE bot_id=%s AND state='RESERVED' "
                        " AND deadline_at<clock_timestamp()-(%s * interval '1 second')) AS stale_reservations"
                    ).format(m=sql.Identifier(MEMORY)),
                    (bot_id, bot_id, bot_id, bot_id, bot_id, bot_id, bot_id,
                     bot_id, bot_id,
                     stale_seconds, bot_id, stale_seconds),
                )
            ).fetchone()
            facet_states = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT purpose,state,count(*) AS count FROM {}.embedding_facets "
                        "WHERE bot_id=%s GROUP BY purpose,state ORDER BY purpose,state"
                    ).format(sql.Identifier(MEMORY)),
                    (bot_id,),
                )
            ).fetchall()
            attempt_failures = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT state,error_code,count(*) AS count "
                        "FROM {}.embedding_batch_attempts WHERE bot_id=%s "
                        "AND state IN ('UNKNOWN','FAILED') "
                        "GROUP BY state,error_code ORDER BY state,error_code"
                    ).format(sql.Identifier(MEMORY)),
                    (bot_id,),
                )
            ).fetchall()
            embedding_budget = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT reserved_requests,settled_requests,unknown_requests,"
                        "reserved_tokens,charged_tokens,unknown_tokens "
                        "FROM {}.embedding_budget_days WHERE bot_id=%s "
                        "AND budget_day=(clock_timestamp() AT TIME ZONE 'UTC')::date"
                    ).format(sql.Identifier(MEMORY)),
                    (bot_id,),
                )
            ).fetchone()
            recent_embedding_work_errors = []
            if embedding_diagnostics:
                recent_embedding_work_errors = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT s.input_payload->'outcome'->'error'->>'code' AS code,"
                            "count(*) AS count "
                            "FROM {m}.embedding_claims c "
                            "JOIN {m}.embedding_work_settlements s "
                            "ON s.bot_id=c.bot_id AND s.claim_token=c.claim_id "
                            "WHERE c.bot_id=%s AND c.created_at>=clock_timestamp()-interval '1 hour' "
                            "AND s.input_payload->'outcome'->>'status'='FAILED' "
                            "GROUP BY s.input_payload->'outcome'->'error'->>'code'"
                        ).format(m=sql.Identifier(MEMORY)),
                        (bot_id,),
                    )
                ).fetchall()
            latest_decisions = await (
                await connection.execute(
                    sql.SQL(
                        "SELECT latest.trigger_decision,latest.reason_code,count(*) AS conversations "
                        "FROM {}.conversation_lanes lane "
                        "JOIN LATERAL ("
                        "SELECT trigger_decision,reason_code FROM {}.inbox_messages "
                        "WHERE bot_id=lane.bot_id AND conversation_id=lane.conversation_id "
                        "ORDER BY ingress_seq DESC LIMIT 1"
                        ") latest ON true "
                        "WHERE lane.bot_id=%s "
                        "GROUP BY latest.trigger_decision,latest.reason_code "
                        "ORDER BY latest.trigger_decision,latest.reason_code"
                    ).format(sql.Identifier(CONVERSATION), sql.Identifier(CONVERSATION)),
                    (bot_id,),
                )
            ).fetchall()
            group_state = None
            if group_id is not None:
                group = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT lane.binding_ref,lane.next_ingress_seq,lane.classified_through_seq,"
                            "lane.context_applied_seq,lane.active_run_ref IS NOT NULL AS active_run,"
                            "latest.ingress_seq,latest.received_at,latest.classification_state,"
                            "latest.trigger_decision,latest.reason_code,latest.classified_at,"
                            "trigger.status AS trigger_status "
                            "FROM {c}.conversation_lanes lane "
                            "LEFT JOIN LATERAL ("
                            "SELECT event_id,ingress_seq,received_at,classification_state,"
                            "trigger_decision,reason_code,classified_at FROM {c}.inbox_messages "
                            "WHERE bot_id=lane.bot_id AND conversation_id=lane.conversation_id "
                            "ORDER BY ingress_seq DESC LIMIT 1"
                            ") latest ON true "
                            "LEFT JOIN LATERAL ("
                            "SELECT status FROM {c}.conversation_triggers "
                            "WHERE bot_id=lane.bot_id AND conversation_id=lane.conversation_id "
                            "AND event_id=latest.event_id"
                            ") trigger ON true "
                            "WHERE lane.bot_id=%s AND lane.conversation_id=%s"
                        ).format(c=sql.Identifier(CONVERSATION)),
                        (bot_id, f"onebot:group:{group_id}"),
                    )
                ).fetchone()
                group_state = {
                    "group_id": group_id,
                    "observed": group is not None,
                }
                if group is not None:
                    latest_send = await (
                        await connection.execute(
                            sql.SQL(
                                "SELECT intent_id,state,error_code,lane_sequence,completed_at "
                                "FROM {}.send_intents WHERE bot_id=%s AND binding_ref=%s "
                                "AND lane_kind='CHAT' AND ordering_key=%s "
                                "ORDER BY lane_sequence DESC LIMIT 1"
                            ).format(sql.Identifier(DELIVERY)),
                            (bot_id, group["binding_ref"], f"onebot:group:{group_id}"),
                        )
                    ).fetchone()
                    group_state.update(
                        next_ingress_seq=group["next_ingress_seq"],
                        classified_through_seq=group["classified_through_seq"],
                        context_applied_seq=group["context_applied_seq"],
                        active_run=group["active_run"],
                        latest_ingress_seq=group["ingress_seq"],
                        latest_received_at=(
                            group["received_at"].isoformat()
                            if group["received_at"] is not None
                            else None
                        ),
                        latest_classification_state=group["classification_state"],
                        latest_trigger_decision=group["trigger_decision"],
                        latest_reason_code=group["reason_code"],
                        latest_classified_at=(
                            group["classified_at"].isoformat()
                            if group["classified_at"] is not None
                            else None
                        ),
                        latest_message_trigger_status=group["trigger_status"],
                        latest_chat_send=(
                            {
                                "intent_id": latest_send["intent_id"],
                                "state": latest_send["state"],
                                "error_code": latest_send["error_code"],
                                "lane_sequence": latest_send["lane_sequence"],
                                "completed_at": (
                                    latest_send["completed_at"].isoformat()
                                    if latest_send["completed_at"] is not None else None
                                ),
                            }
                            if latest_send is not None else None
                        ),
                    )
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
            claims = await (
                await connection.execute(
                    sql.SQL(
                        "WITH active AS ("
                        "SELECT lane.conversation_id,lane.active_session_ref,lane.active_run_ref,"
                        "count(*) AS group_count,"
                        "bool_or(g.session_ref=lane.active_session_ref "
                        "AND g.run_ref=lane.active_run_ref) AS run_match "
                        "FROM {c}.trigger_claim_groups g "
                        "JOIN {c}.conversation_lanes lane "
                        "ON lane.bot_id=g.bot_id AND lane.conversation_id=g.conversation_id "
                        "WHERE g.bot_id=%s AND g.claim_state='ACTIVE' "
                        "GROUP BY lane.conversation_id,lane.active_session_ref,lane.active_run_ref"
                        ") SELECT count(*) AS active_conversations,"
                        "count(*) FILTER (WHERE group_count=1 AND active_session_ref IS NULL "
                        "AND active_run_ref IS NULL) AS valid_prefill_conversations,"
                        "count(*) FILTER (WHERE group_count<>1 OR "
                        "((active_session_ref IS NULL AND active_run_ref IS NULL) "
                        "OR run_match) IS NOT TRUE) AS misaligned_conversations "
                        "FROM active"
                    ).format(c=sql.Identifier(CONVERSATION)),
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
    result = {
        "schema_bundles": bundles["count"],
        "ready_schema_bundles": bundles["ready"],
        "unmatched_ready_schema_bundles": unmatched_bundles["count"],
        "graph_id": graph["graph_id"],
        "graph_revision": graph["graph_revision"],
        "members": members["members"],
        "lifecycle_recorded": lifecycle["recorded"],
        "active_members": lifecycle["active"],
        "memory": {
            **dict(memory),
            "facet_states": {
                f"{row['purpose']}.{row['state']}": row["count"]
                for row in facet_states
            },
            "attempt_failures": {
                f"{row['state']}.{row['error_code'] or 'NONE'}": row["count"]
                for row in attempt_failures
            },
            "embedding_budget_today": dict(embedding_budget) if embedding_budget else None,
        },
        "concurrency": dict(concurrency),
        "conversation_claims": dict(claims),
        "latest_ingress_decisions": [
            {
                "decision": row["trigger_decision"],
                "reason_code": row["reason_code"],
                "conversations": row["conversations"],
            }
            for row in latest_decisions
        ],
        "work": dict(work),
    }
    if embedding_diagnostics:
        result["memory"]["embedding_work_errors_last_hour"] = {
            row["code"] or "NONE": row["count"] for row in recent_embedding_work_errors
        }
    if group_state is not None:
        result["target_group"] = group_state
    if components is not None:
        result["components"] = components
    return result


def _health(url: str) -> int:
    with urlopen(url, timeout=5) as response:
        return response.status


def _violations(deployment: dict, bot: dict, health_status: int) -> list[str]:
    failures = []
    if deployment["supply_state"] != "READY" or deployment["desired_state"] != "ONLINE":
        failures.append("DEPLOYMENT_INTENT_INVALID")
    if deployment["observed_state"] != "ONLINE" or deployment["error_code"] is not None:
        failures.append("DEPLOYMENT_OBSERVATION_INVALID")
    if "graph" in deployment:
        graph = deployment["graph"]
        if graph["state"] != "ACTIVE":
            failures.append("DEPLOYMENT_GRAPH_INACTIVE")
        if any(member is None or member["state"] != "ACTIVE" or
               not member["control_admission"]
               for member in graph["components"].values()):
            failures.append("DEPLOYMENT_COMPONENT_INACTIVE")
    if (
        not bot["ready_schema_bundles"]
        or bot["ready_schema_bundles"] > bot["members"]
        or bot["unmatched_ready_schema_bundles"]
    ):
        failures.append("RUNTIME_GRAPH_CARDINALITY_INVALID")
    if (
        not bot["members"]
        or bot["lifecycle_recorded"] != bot["members"]
        or bot["active_members"] != bot["members"]
    ):
        failures.append("RUNTIME_LIFECYCLE_INVALID")
    if bot["memory"]["stale_attempts"] or bot["memory"]["stale_reservations"]:
        failures.append("EMBEDDING_CONVERGENCE_STALE")
    if bot["memory"]["facet_states"].get("DOCUMENT.UNKNOWN", 0):
        failures.append("DOCUMENT_EMBEDDING_UNKNOWN")
    if bot["memory"]["facet_states"].get("DOCUMENT.FAILED", 0):
        failures.append("DOCUMENT_EMBEDDING_FAILED")
    if bot["concurrency"]["overlapping_sessions"]:
        failures.append("AGENT_SESSION_CONCURRENCY_INVALID")
    if bot["conversation_claims"]["misaligned_conversations"]:
        failures.append("CONVERSATION_CLAIM_ALIGNMENT_INVALID")
    if health_status != 200:
        failures.append("MANAGEMENT_HTTP_INVALID")
    return failures


async def observe(arguments: argparse.Namespace) -> int:
    settings = DeploymentAccessSettings.from_environment()
    deployment = DeploymentStore(settings.dsn, settings.deployment_id)
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
        bot_store = BotStore(storage.dsn, arguments.bot_id)
        await bot_store.open()
        while True:
            deployment_state, bot_state, health_status = await asyncio.gather(
                _deployment_state(
                    deployment, arguments.bot_id, tuple(arguments.deployment_component_id),
                ),
                _bot_state(
                    bot_store, arguments.bot_id, arguments.stale_seconds,
                    arguments.group_id, tuple(arguments.component_id),
                    embedding_diagnostics=arguments.embedding_diagnostics,
                ),
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
    try:
        result = asyncio.run(observe(arguments))
    except UndefinedTable:
        raise SystemExit(
            "OBSERVATION_SCHEMA_INCOMPATIBLE: a required deployment or Bot table is absent; "
            "observe a Bot installed from the candidate baseline"
        ) from None
    except RuntimeError as error:
        raise SystemExit(str(error)) from None
    raise SystemExit(result)


if __name__ == "__main__":
    main()
