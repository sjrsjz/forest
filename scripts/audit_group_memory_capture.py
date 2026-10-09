"""Audit group-message capture without reading message bodies or media bytes.

Run once with the Supervisor's environment and read-only Secret volume. This
scans the complete group Inbox and is not a high-frequency health check.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone

from psycopg import sql

from forest.deployment_access_settings import DeploymentAccessSettings
from forest.schema_bundles import module_schema_namespace
from forest.storage.bot_store import BotStore
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_store import DeploymentStore

CONVERSATION = module_schema_namespace("forest.conversation.scheduler")
MEMORY = module_schema_namespace("forest.memory.graph")


async def audit(bot_id: str, grace_seconds: int) -> dict:
    settings = DeploymentAccessSettings.from_environment()
    deployment = DeploymentStore(settings.dsn, settings.deployment_id)
    await deployment.open()
    bot_store = None
    try:
        secrets = DeploymentSecretAuthority(
            deployment,
            active_key_id=settings.master_keyring.active_key_id,
            key_resolver=settings.master_keyring.resolve,
        )
        catalog = DeploymentCatalogRepository(deployment)
        profiles = DeploymentStorageProfileAuthority(deployment, secrets=secrets)
        bot = await catalog.inspect(bot_id)
        storage = await profiles.resolve(
            profile_ref=bot["storage_profile_ref"],
            expected_bot_id=bot_id,
            expected_storage_generation=bot["storage_generation"],
        )
        bot_store = BotStore(storage.dsn, bot_id)
        await bot_store.open()
        async with bot_store.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                coverage = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT count(*) AS inbox_messages,"
                            "count(o.occurrence_id) AS source_occurrences,"
                            "count(u.event_ref) AS unavailable,"
                            "count(*) FILTER (WHERE o.occurrence_id IS NOT NULL "
                            "AND u.event_ref IS NOT NULL) AS overlap,"
                            "count(*) FILTER (WHERE o.occurrence_id IS NULL "
                            "AND u.event_ref IS NULL AND i.received_at > "
                            "transaction_timestamp()-(%s * interval '1 second')) "
                            "AS pending_within_grace,"
                            "count(*) FILTER (WHERE o.occurrence_id IS NULL "
                            "AND u.event_ref IS NULL AND i.received_at <= "
                            "transaction_timestamp()-(%s * interval '1 second')) "
                            "AS unexplained_older_than_grace,"
                            "min(i.received_at) FILTER (WHERE o.occurrence_id IS NULL "
                            "AND u.event_ref IS NULL AND i.received_at <= "
                            "transaction_timestamp()-(%s * interval '1 second')) "
                            "AS oldest_unexplained_received_at "
                            "FROM {c}.inbox_messages i "
                            "LEFT JOIN {m}.source_occurrences o "
                            "ON o.bot_id=i.bot_id AND o.conversation_ref=i.conversation_id "
                            "AND o.event_ref=i.event_id "
                            "LEFT JOIN {m}.unavailable_source_events u "
                            "ON u.bot_id=i.bot_id AND u.conversation_ref=i.conversation_id "
                            "AND u.event_ref=i.event_id "
                            "WHERE i.bot_id=%s AND i.event_kind='group.message'"
                        ).format(c=sql.Identifier(CONVERSATION), m=sql.Identifier(MEMORY)),
                        (grace_seconds, grace_seconds, grace_seconds, bot_id),
                    )
                ).fetchone()
                reasons = await (
                    await connection.execute(
                        sql.SQL(
                            "SELECT u.error_code,count(*) AS events "
                            "FROM {m}.unavailable_source_events u "
                            "JOIN {c}.inbox_messages i ON i.bot_id=u.bot_id "
                            "AND i.conversation_id=u.conversation_ref "
                            "AND i.event_id=u.event_ref "
                            "WHERE i.bot_id=%s AND i.event_kind='group.message' "
                            "GROUP BY u.error_code ORDER BY u.error_code"
                        ).format(c=sql.Identifier(CONVERSATION), m=sql.Identifier(MEMORY)),
                        (bot_id,),
                    )
                ).fetchall()
        violations = []
        if coverage["overlap"]:
            violations.append("MEMORY_CAPTURE_RESULT_OVERLAP")
        if coverage["unexplained_older_than_grace"]:
            violations.append("GROUP_MEMORY_CAPTURE_MISSING")
        return {
            "protocol": "forest.group-memory-capture-audit/1",
            "bot_id": bot_id,
            "audited_at": datetime.now(timezone.utc).isoformat(),
            "grace_seconds": grace_seconds,
            **{
                key: coverage[key]
                for key in (
                    "inbox_messages", "source_occurrences", "unavailable", "overlap",
                    "pending_within_grace", "unexplained_older_than_grace",
                )
            },
            "oldest_unexplained_received_at": (
                coverage["oldest_unexplained_received_at"].isoformat()
                if coverage["oldest_unexplained_received_at"] else None
            ),
            "unavailable_reasons": {row["error_code"]: row["events"] for row in reasons},
            "violations": violations,
        }
    finally:
        if bot_store is not None:
            await bot_store.close()
        await deployment.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bot-id", required=True)
    parser.add_argument("--grace-seconds", type=int, default=300)
    arguments = parser.parse_args()
    if not 1 <= arguments.grace_seconds <= 86400:
        parser.error("--grace-seconds must be between 1 and 86400")
    result = asyncio.run(audit(arguments.bot_id, arguments.grace_seconds))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    raise SystemExit(1 if result["violations"] else 0)


if __name__ == "__main__":
    main()
