"""Pre-release destructive Bot database rebuild from frozen deployment authorities.

This script deliberately does not mutate the deployment catalog or provisioning
Job. It is only for replacing an unpublished Bot database after a baseline
change. Run it in the candidate image with the Supervisor's read-only Secret
mount and deployment network. The default mode is read-only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from forest.clean_supervisor_host import CleanSupervisorSettings
from forest.errors import ForestError
from forest.official_bot_provisioning import OfficialBotProvisioner, OfficialBotSettingsResolver
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.schema_bundles import module_schema_namespace
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Drop one unpublished Bot database and reinstall current baselines."
    )
    value.add_argument("--bot-id", required=True)
    value.add_argument("--execute", action="store_true")
    value.add_argument(
        "--confirm-database",
        help="Required with --execute; must exactly match the derived database name.",
    )
    return value


def maintenance_dsn(dsn: str) -> str:
    parsed = urlsplit(dsn)
    return urlunsplit(parsed._replace(path="/postgres"))


async def rebuild(*, bot_id: str, execute: bool, confirmation: str | None) -> dict:
    settings = CleanSupervisorSettings.from_environment()
    store = CleanDeploymentStore(settings.dsn, settings.deployment_id)
    await store.open()
    try:
        secrets = DeploymentSecretAuthority(
            store,
            active_key_id=settings.master_keyring.active_key_id,
            key_resolver=settings.master_keyring.resolve,
        )
        catalog = DeploymentCatalogRepository(store)
        storage_profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)
        bootstrap_profiles = DeploymentBootstrapProfileAuthority(store, secrets=secrets)
        bot = await catalog.inspect(bot_id)
        if bot["supply_state"] != "READY":
            raise ForestError("BOT_REBUILD_STATE_INVALID", "Bot is not READY", 409)
        storage = await storage_profiles.resolve(
            profile_ref=bot["storage_profile_ref"],
            expected_bot_id=bot_id,
            expected_storage_generation=bot["storage_generation"],
        )
        profile = await bootstrap_profiles.load(bot["bootstrap_profile_ref"])
        if profile.manifest_digest != bot["bootstrap_manifest_digest"]:
            raise ForestError(
                "BOT_REBUILD_PROFILE_MISMATCH",
                "Bot catalog and Bootstrap profile manifests differ",
                409,
            )
        resolver = OfficialBotSettingsResolver(secrets=secrets)
        await resolver.resolve(profile=profile, storage=storage)
        allocator = LocalPostgresDatabaseAllocator(settings.dsn)
        allocator.validate_target(bot_id, storage.dsn)
        database_name = allocator.database_name(bot_id)
        role_name = allocator.role_name(bot_id)
        async with store.pool.connection() as connection:
            lease = await (
                await connection.execute(
                    "SELECT host_id,incarnation,owner_epoch,expires_at,"
                    "expires_at<=clock_timestamp() AS expired "
                    "FROM forest_deployment.supervisor_lease WHERE singleton"
                )
            ).fetchone()
        result = {
            "mode": "execute" if execute else "dry-run",
            "bot_id": bot_id,
            "database_name": database_name,
            "database_role": role_name,
            "storage_generation": storage.storage_generation,
            "bootstrap_profile_ref": profile.profile_ref,
            "bootstrap_manifest_digest": profile.manifest_digest,
            "supervisor_lease_expired": bool(lease["expired"]),
        }
        if not execute:
            return result
        if confirmation != database_name:
            raise ForestError(
                "BOT_REBUILD_CONFIRMATION_REQUIRED",
                "--confirm-database must exactly match the derived Bot database",
                422,
            )
        guard = await catalog.claim_supervisor(
            "development-rebuild/" + bot_id,
            "incarnation/rebuild/" + str(uuid4()),
            lease_seconds=300,
        )
        guard_ref = [guard]
        guard_stop = asyncio.Event()

        async def renew_guard() -> None:
            while not guard_stop.is_set():
                try:
                    await asyncio.wait_for(guard_stop.wait(), timeout=30)
                except TimeoutError:
                    guard_ref[0] = await catalog.renew_supervisor(
                        guard_ref[0], lease_seconds=300
                    )

        guard_task = asyncio.create_task(renew_guard(), name="bot-database-rebuild-lease")
        try:
            connection = await psycopg.AsyncConnection.connect(
                maintenance_dsn(settings.dsn), autocommit=True, row_factory=dict_row
            )
            try:
                existing = await (
                    await connection.execute(
                        "SELECT pg_get_userbyid(datdba) AS owner "
                        "FROM pg_database WHERE datname=%s",
                        (database_name,),
                    )
                ).fetchone()
                if existing is not None and existing["owner"] != role_name:
                    raise ForestError(
                        "BOT_REBUILD_OWNER_MISMATCH",
                        "Derived Bot database owner differs",
                        409,
                    )
                if existing is not None:
                    await connection.execute(
                        sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                            sql.Identifier(database_name)
                        )
                    )
            finally:
                await connection.close()

            await OfficialBotProvisioner(
                settings_resolver=resolver,
                database_allocator=allocator,
            )(
                claim=SimpleNamespace(bot_id=bot_id),
                storage=storage,
                profile=profile,
            )

            target = CleanBotStore(storage.dsn, bot_id)
            await target.open()
            try:
                runtime_namespace = module_schema_namespace("forest.kernel.runtime")
                async with target.pool.connection() as connection:
                    bundles = await (
                        await connection.execute(
                            "SELECT count(*) AS count FROM forest_kernel.schema_bundles"
                        )
                    ).fetchone()
                    graph_heads = await (
                        await connection.execute(
                            sql.SQL(
                                "SELECT count(*) AS count FROM {}.runtime_graph_head"
                            ).format(sql.Identifier(runtime_namespace))
                        )
                    ).fetchone()
            finally:
                await target.close()
            if bundles["count"] != 17 or graph_heads["count"] != 0:
                raise ForestError(
                    "BOT_REBUILD_VERIFICATION_FAILED",
                    "Rebuilt Bot database does not contain the expected clean baseline",
                    500,
                )
            result.update(schema_bundles=bundles["count"], runtime_graph_heads=0)
            return result
        finally:
            guard_stop.set()
            try:
                await guard_task
            finally:
                await catalog.release_supervisor(guard_ref[0])
    finally:
        await store.close()


def main() -> None:
    arguments = parser().parse_args()
    try:
        result = asyncio.run(
            rebuild(
                bot_id=arguments.bot_id,
                execute=arguments.execute,
                confirmation=arguments.confirm_database,
            )
        )
    except ForestError as exc:
        raise SystemExit(f"{exc.code}: {exc.message}") from None
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
