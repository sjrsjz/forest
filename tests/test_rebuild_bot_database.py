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

import scripts.rebuild_bot_database as rebuild_module
from forest.errors import ForestError
from forest.official_bot_provisioning import (
    OFFICIAL_BOT_COMPONENT_IDS,
    official_bootstrap_contributions,
    official_bootstrap_schema_digests,
)
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_store import DeploymentStore
from forest.vector_index_effect_driver import vector_collection_name
from scripts.rebuild_bot_database import rebuild


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
    store = DeploymentStore(deployment_dsn, "deployment/rebuild-test")
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

    monkeypatch.setenv("FOREST_PACKAGE_CATALOG_ROOT", str(tmp_path / "packages"))
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
        release_store = DeploymentStore(deployment_dsn, "deployment/rebuild-test")
        await release_store.open()
        try:
            await DeploymentCatalogRepository(release_store).release_supervisor(active_lease)
        finally:
            await release_store.close()

        monkeypatch.delenv("FOREST_PACKAGE_CATALOG_ROOT")
        with pytest.raises(ForestError) as missing_catalog:
            await rebuild(bot_id=bot_id, execute=True, confirmation=target_name)
        assert missing_catalog.value.code == "BOT_REBUILD_PACKAGE_CATALOG_REQUIRED"
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        marker = await (
            await target.execute("SELECT to_regclass('public.pre_rebuild_marker') AS marker")
        ).fetchone()
        assert marker[0] == "pre_rebuild_marker"
        await target.close()
        monkeypatch.setenv("FOREST_PACKAGE_CATALOG_ROOT", str(tmp_path / "packages"))

        expected_collection = vector_collection_name(
            bot_id, components["vector.index"]["space_id"]
        )
        vector_calls = []
        vector_ready = False

        class FakeVectorClient:
            def __init__(self, *, uri, token, timeout):
                vector_calls.append(("connect", uri, token, timeout))

            async def has_collection(self, *, collection_name):
                vector_calls.append(("has", collection_name))
                if not vector_ready:
                    raise OSError("isolated vector preflight failure")
                return True

            async def drop_collection(self, *, collection_name):
                vector_calls.append(("drop", collection_name))

            async def close(self):
                vector_calls.append(("close",))

        monkeypatch.setattr(rebuild_module, "AsyncMilvusClient", FakeVectorClient)

        with pytest.raises(ForestError) as unavailable:
            await rebuild(bot_id=bot_id, execute=True, confirmation=target_name)
        assert unavailable.value.code == "BOT_REBUILD_VECTOR_PREFLIGHT_FAILED"
        assert unavailable.value.details["database_rebuilt"] is False
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        marker = await (
            await target.execute("SELECT to_regclass('public.pre_rebuild_marker') AS marker")
        ).fetchone()
        assert marker[0] == "pre_rebuild_marker"
        await target.close()
        vector_ready = True

        result = await rebuild(
            bot_id=bot_id,
            execute=True,
            confirmation=target_name,
        )
        assert result["schema_bundles"] == len(OFFICIAL_BOT_COMPONENT_IDS)
        assert result["runtime_graph_heads"] == 0
        assert result["vector_collection_name"] == expected_collection
        assert result["vector_collection_reset"] is True
        assert ("has", expected_collection) in vector_calls
        assert ("drop", expected_collection) in vector_calls
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        marker = await (
            await target.execute("SELECT to_regclass('public.pre_rebuild_marker') AS marker")
        ).fetchone()
        assert marker[0] is None
        await target.close()
        verify_store = DeploymentStore(deployment_dsn, "deployment/rebuild-test")
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
