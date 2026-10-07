"""Accept Host storage and Bot supply through the actual Supervisor composition."""

from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.bootstrap_contributions import BootstrapConfigurationRegistry
from forest.builtin_packages import official_builtin_configuration
from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.deployment_web_bootstrap import DeploymentWebBootstrap
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.model_contract import ModelCapabilities
from forest.official_bot_provisioning import (
    official_bootstrap_contributions,
    official_bootstrap_schema_digests,
)
from forest.packages import PackageCatalog
from forest.schema_bundles import module_schema_namespace
from forest.storage.bot_store import BotStore
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_storage_requests import DeploymentStorageRequestAuthority
from forest.storage.deployment_store import DeploymentStore
from forest.supervisor_host import SupervisorSettings, run_supervisor


async def _until(predicate, supervisor_task: asyncio.Task, *, seconds: float):
    deadline = monotonic() + seconds
    while monotonic() < deadline:
        if supervisor_task.done():
            await supervisor_task
            raise AssertionError("Supervisor stopped before the acceptance condition")
        result = await predicate()
        if result is not None:
            return result
        await asyncio.sleep(0.2)
    raise AssertionError("Supervisor did not reach the acceptance condition")


@pytest.mark.postgres
async def test_supervisor_settles_host_storage_then_provisions_offline_bot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    root_dsn = os.environ.get("FOREST_TEST_DSN")
    if not root_dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    suffix = uuid4().hex
    deployment_name = "forest_deployment_" + suffix
    host_name = "forest_host_" + suffix
    host_id = "host/" + suffix
    deployment_id = "deployment/supervisor-acceptance"
    parsed = urlsplit(root_dsn)

    def uri(database_name: str) -> str:
        return urlunsplit(parsed._replace(path="/" + database_name))

    admin = await psycopg.AsyncConnection.connect(root_dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(deployment_name)))
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(host_name)))
    deployment_dsn, host_dsn = uri(deployment_name), uri(host_name)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        management_port = listener.getsockname()[1]
    now = datetime.now(timezone.utc)
    key_id = "master/supervisor-acceptance/1"
    keyring = DeploymentMasterKeyring(key_id, {key_id: bytes(range(32))})
    settings = SupervisorSettings(
        deployment_id=deployment_id,
        host_id=host_id,
        dsn=deployment_dsn,
        host_dsn=host_dsn,
        master_keyring=keyring,
        web_bootstrap=DeploymentWebBootstrap(
            "setup/supervisoracceptance0001", "p" * 48,
            now, now + timedelta(hours=1),
        ),
        management_port=management_port,
        management_origin=f"http://127.0.0.1:{management_port}",
        poll_seconds=0.1,
    )
    package_root = tmp_path / "packages"
    monkeypatch.setenv("FOREST_PACKAGE_CATALOG_ROOT", str(package_root))
    supervisor_task = asyncio.create_task(run_supervisor(settings))
    store = DeploymentStore(deployment_dsn, deployment_id)
    physical: list[tuple[str, str]] = []
    try:
        async def management_ready():
            try:
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1", management_port,
                )
            except OSError:
                return None
            writer.close()
            await writer.wait_closed()
            return True

        await _until(management_ready, supervisor_task, seconds=90)
        await store.open()
        secrets = DeploymentSecretAuthority(
            store, active_key_id=key_id, key_resolver=keyring.resolve,
        )
        password_ref = "secret://supervisor-acceptance/password"
        dsn_ref = "secret://supervisor-acceptance/dsn"
        await secrets.put(
            operation_id="supervisor-acceptance/password",
            secret_ref=password_ref,
            purpose="storage.postgresql.password",
            plaintext=b"forest-supervisor-acceptance-role-password-1",
            expected_revision=0,
        )
        request = await DeploymentStorageRequestAuthority(
            store, secrets=secrets,
        ).request(
            operation_id="supervisor-acceptance/storage-request",
            host_id=host_id,
            bot_id="bot/supervisor-acceptance",
            storage_generation=1,
            profile_ref="storage-profile/supervisor-acceptance/1",
            dsn_secret_ref=dsn_ref,
            credential_ref=password_ref,
        )
        profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)

        async def settled_profile():
            try:
                return await profiles.metadata(request.profile_ref)
            except ForestError as error:
                if error.code == "STORAGE_PROFILE_NOT_FOUND":
                    return None
                raise

        storage = await _until(settled_profile, supervisor_task, seconds=120)
        target_dsn = await secrets.resolve_text(dsn_ref)
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        try:
            row = await (await target.execute(
                "SELECT current_user, current_database()"
            )).fetchone()
            assert row == (urlsplit(target_dsn).username, urlsplit(target_dsn).path[1:])
        finally:
            await target.close()

        for name, purpose in (
            ("model", "provider.chat-completions.credential"),
            ("embedding", "embedding.multimodal.credential"),
            ("onebot", "adapter.onebot.access-token"),
        ):
            await secrets.put(
                operation_id=f"supervisor-acceptance/{name}",
                secret_ref=f"secret://supervisor-acceptance/{name}",
                purpose=purpose,
                plaintext=(name + "-test-key").encode(),
                expected_revision=0,
            )
        embedding = EmbeddingProfile(
            deployment_id="embedding.multimodal/supervisor-acceptance",
            model_revision="test/1", model="Qwen/Qwen3-VL-Embedding-8B",
            dimensions=4096,
        )
        components = {
            "provider.chat-completions": {
                "protocol": "forest.provider.chat-completions/1",
                "base_url": "https://model.example/v1",
                "credential_ref": "secret://supervisor-acceptance/model",
                "timeout_seconds": 30,
                "max_connections": 3,
                "capabilities": ModelCapabilities(
                    deployment_id="provider.chat-completions/supervisor-acceptance",
                    model="test-model", capability_revision="test/1",
                    input_modalities=("text", "image"), max_output_tokens=2048,
                ).as_dict(),
            },
            "embedding.multimodal": {
                "protocol": "forest.embedding.multimodal/1",
                "endpoint": "https://embedding.example/v1/embeddings",
                "credential_ref": "secret://supervisor-acceptance/embedding",
                "timeout_seconds": 20,
                "max_connections": 2,
                "profile": {
                    "deployment_id": embedding.deployment_id,
                    "model_revision": embedding.model_revision,
                    "model": embedding.model,
                    "dimensions": embedding.dimensions,
                },
            },
            "vector.index": {
                **official_builtin_configuration("vector_index_milvus", overrides={
                    "space_id": embedding.space_id, "dimensions": embedding.dimensions,
                }),
                "endpoint": "http://127.0.0.1:19530",
            },
            "conversation.trigger-policy": {
                "protocol": "forest.conversation.trigger-policy/1",
                "allowed_group_ids": ["933179609"],
                "group_trigger_mode": "MENTION", "private_enabled": True,
            },
            "shell.command": {
                "protocol": "forest.shell.command.bootstrap/1",
                "administrator_principals": ["qq:10001"],
            },
            "agent.runtime": {
                "protocol": "forest.agent.runtime.bootstrap/1",
                "preset_system": ["Test Bot"],
            },
            "adapter.onebot": {
                "protocol": "forest.adapter.onebot.bootstrap/1",
                "binding_id": "binding/main", "self_id": "123456789",
                "websocket_url": "ws://127.0.0.1:3001/",
                "access_token_ref": "secret://supervisor-acceptance/onebot",
            },
        }
        package_catalog = PackageCatalog(package_root)
        registry = BootstrapConfigurationRegistry(
            official_bootstrap_contributions(package_catalog)
        )
        bootstrap = await DeploymentBootstrapProfileAuthority(
            store, secrets=secrets,
        ).create(
            operation_id="supervisor-acceptance/bootstrap",
            profile_ref="bootstrap-profile/supervisor-acceptance/1",
            bot_id=request.bot_id,
            storage_profile_ref=storage.profile_ref,
            storage_generation=1,
            components=components,
            schema_digests=official_bootstrap_schema_digests(registry),
            package_digests=registry.package_digests(),
        )
        jobs = DeploymentProvisioningJobAuthority(store)
        await jobs.enqueue(
            operation_id="supervisor-acceptance/provisioning",
            job_id="provisioning/supervisor-acceptance/1",
            bot_id=request.bot_id,
            display_name="Supervisor Acceptance",
            storage_profile_ref=storage.profile_ref,
            storage_generation=1,
            bootstrap_profile_ref=bootstrap.profile_ref,
        )

        async def ready_bot():
            job = await jobs.inspect("provisioning/supervisor-acceptance/1")
            if job["state"] in {"FAILED", "BLOCKED"}:
                raise AssertionError(job)
            if job["state"] != "SUCCEEDED":
                return None
            bot = await DeploymentCatalogRepository(store).inspect(request.bot_id)
            return bot if bot["supply_state"] == "READY" else None

        bot = await _until(ready_bot, supervisor_task, seconds=120)
        assert bot["desired_state"] == "OFFLINE"
        target_store = BotStore(target_dsn, request.bot_id)
        await target_store.open()
        try:
            async with target_store.pool.connection() as conn:
                row = await (await conn.execute(
                    "SELECT count(*) AS count FROM forest_kernel.schema_bundles"
                )).fetchone()
            assert row["count"] == 22
        finally:
            await target_store.close()
    finally:
        supervisor_task.cancel()
        await asyncio.gather(supervisor_task, return_exceptions=True)
        await store.close()
        host_admin = await psycopg.AsyncConnection.connect(host_dsn, autocommit=True)
        try:
            async with host_admin.cursor() as cursor:
                try:
                    await cursor.execute(sql.SQL(
                        "SELECT physical_name,role_name FROM {}.physical_generations"
                    ).format(sql.Identifier(module_schema_namespace("forest.host.storage"))))
                    physical = await cursor.fetchall()
                except psycopg.errors.UndefinedTable:
                    pass
        finally:
            await host_admin.close()
        for database_name, role_name in physical:
            assert database_name.startswith("forest_bot_")
            assert role_name.startswith("forest_role_")
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(database_name),
            ))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(
                sql.Identifier(role_name),
            ))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(host_name),
        ))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(deployment_name),
        ))
        await admin.close()
