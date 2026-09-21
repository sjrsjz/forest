from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from scripts.rebuild_bot_database import rebuild

from forest.errors import ForestError
from forest.official_bot_provisioning import (
    official_bootstrap_contributions,
    official_bootstrap_schema_digests,
)
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority


@pytest.mark.postgres
async def test_pre_release_rebuild_replaces_only_the_derived_bot_database(
    tmp_path, monkeypatch
):
    root_dsn = os.environ.get("FOREST_TEST_DSN")
    if not root_dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    suffix = uuid4().hex
    deployment_name = "forest_deployment_" + suffix
    bot_id = "bot/rebuild-" + suffix
    parsed = urlsplit(root_dsn)
    deployment_dsn = urlunsplit(parsed._replace(path="/" + deployment_name))
    admin = await psycopg.AsyncConnection.connect(root_dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(deployment_name)))
    allocator = LocalPostgresDatabaseAllocator(deployment_dsn)
    target_name = allocator.database_name(bot_id)
    target_role = allocator.role_name(bot_id)
    key_id = "master/rebuild-test/1"
    key = bytes(range(32))
    keyring_path = tmp_path / "keyring.json"
    keyring_path.write_text(
        json.dumps(
            {
                "protocol": "forest.deployment-master-keyring/1",
                "active_key_id": key_id,
                "keys": {key_id: base64.b64encode(key).decode("ascii")},
            },
            separators=(",", ":"),
        ),
        encoding="ascii",
    )
    now = datetime.now(timezone.utc)
    web_bootstrap_path = tmp_path / "web-bootstrap.json"
    web_bootstrap_path.write_text(
        json.dumps(
            {
                "protocol": "forest.deployment-web-bootstrap/2",
                "setup_id": "setup/rebuildtest00001",
                "pairing_code": "p" * 48,
                "issued_at": now.isoformat(),
                "expires_at": (now + timedelta(hours=1)).isoformat(),
            },
            separators=(",", ":"),
        ),
        encoding="ascii",
    )
    store = CleanDeploymentStore(deployment_dsn, "deployment/rebuild-test")
    await store.open()
    try:
        catalog = DeploymentCatalogRepository(store)
        await catalog.initialize()
        secrets = DeploymentSecretAuthority(
            store,
            active_key_id=key_id,
            key_resolver=lambda requested: key if requested == key_id else None,
        )
        target_dsn = allocator.new_target_dsn(bot_id)
        values = {
            "secret://rebuild/storage": ("storage.postgresql.dsn", target_dsn),
            "secret://provider.chat-completions/default": (
                "provider.chat-completions.credential",
                "model-probe-key",
            ),
            "secret://embedding.multimodal/default": (
                "embedding.multimodal.credential",
                "embedding-probe-key",
            ),
            "secret://adapter.onebot/default": (
                "adapter.onebot.access-token",
                "onebot-probe-token",
            ),
        }
        for index, (secret_ref, (purpose, plaintext)) in enumerate(values.items(), 1):
            await secrets.put(
                operation_id=f"secret/rebuild/{index}",
                secret_ref=secret_ref,
                purpose=purpose,
                plaintext=plaintext.encode(),
                expected_revision=0,
            )
        storage_profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)
        storage = await storage_profiles.create(
            operation_id="storage/rebuild/create",
            profile_ref="storage-profile/rebuild/1",
            bot_id=bot_id,
            storage_generation=1,
            dsn_secret_ref="secret://rebuild/storage",
        )
        components = {
            contribution.component_id: contribution.initial_value
            for contribution in official_bootstrap_contributions()
        }
        bootstrap_profiles = DeploymentBootstrapProfileAuthority(store, secrets=secrets)
        bootstrap = await bootstrap_profiles.create(
            operation_id="bootstrap/rebuild/create",
            profile_ref="bootstrap-profile/rebuild/1",
            bot_id=bot_id,
            storage_profile_ref=storage.profile_ref,
            storage_generation=storage.storage_generation,
            components=components,
            schema_digests=official_bootstrap_schema_digests(),
        )
        await catalog.register_ready(
            operation_id="catalog/rebuild/ready",
            bot_id=bot_id,
            display_name="Rebuild Probe",
            storage_profile_ref=storage.profile_ref,
            bootstrap_profile_ref=bootstrap.profile_ref,
            bootstrap_manifest_digest=bootstrap.manifest_digest,
            storage_generation=storage.storage_generation,
        )
        await allocator.ensure_database(bot_id=bot_id, target_dsn=target_dsn)
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        await target.execute("CREATE TABLE public.pre_rebuild_marker(value integer)")
        await target.close()
        active_lease = await catalog.claim_supervisor(
            "host/rebuild-blocker",
            "incarnation/rebuild-blocker",
            lease_seconds=300,
        )
    finally:
        await store.close()

    monkeypatch.setenv("FOREST_DEPLOYMENT_DSN", deployment_dsn)
    monkeypatch.setenv("FOREST_DEPLOYMENT_ID", "deployment/rebuild-test")
    monkeypatch.setenv("FOREST_DEPLOYMENT_MASTER_KEYRING_FILE", str(keyring_path))
    monkeypatch.setenv("FOREST_DEPLOYMENT_WEB_BOOTSTRAP_FILE", str(web_bootstrap_path))
    monkeypatch.setenv("FOREST_DEPLOYMENT_MANAGEMENT_ORIGIN", "http://127.0.0.1:8081")
    try:
        with pytest.raises(ForestError) as blocked:
            await rebuild(
                bot_id=bot_id,
                execute=True,
                confirmation=target_name,
            )
        assert blocked.value.code == "DEPLOYMENT_SUPERVISOR_BUSY"
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        marker = await (
            await target.execute("SELECT to_regclass('public.pre_rebuild_marker') AS marker")
        ).fetchone()
        assert marker[0] == "pre_rebuild_marker"
        await target.close()
        release_store = CleanDeploymentStore(deployment_dsn, "deployment/rebuild-test")
        await release_store.open()
        try:
            await DeploymentCatalogRepository(release_store).release_supervisor(active_lease)
        finally:
            await release_store.close()

        result = await rebuild(
            bot_id=bot_id,
            execute=True,
            confirmation=target_name,
        )
        assert result["schema_bundles"] == 18
        assert result["runtime_graph_heads"] == 0
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        marker = await (
            await target.execute("SELECT to_regclass('public.pre_rebuild_marker') AS marker")
        ).fetchone()
        assert marker[0] is None
        await target.close()
        verify_store = CleanDeploymentStore(deployment_dsn, "deployment/rebuild-test")
        await verify_store.open()
        try:
            bot = await DeploymentCatalogRepository(verify_store).inspect(bot_id)
            assert bot["supply_state"] == "READY"
            assert bot["bootstrap_manifest_digest"] == bootstrap.manifest_digest
            async with verify_store.pool.connection() as connection:
                lease = await (
                    await connection.execute(
                        "SELECT expires_at<=clock_timestamp() AS expired "
                        "FROM forest_deployment.supervisor_lease WHERE singleton"
                    )
                ).fetchone()
            assert lease["expired"] is True
        finally:
            await verify_store.close()
    finally:
        await admin.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(target_name)
            )
        )
        await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(target_role)))
        await admin.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                sql.Identifier(deployment_name)
            )
        )
        await admin.close()
