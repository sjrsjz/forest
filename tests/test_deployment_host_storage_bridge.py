"""A lost deployment settlement reuses the same Host reservation after reload."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.deployment_host_storage_bridge import DeploymentHostStorageBridge
from forest.deployment_storage_settlement import DeploymentStorageSettlementAuthority
from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_storage_postgres_effect import HostPostgresMaterializationEffect
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog
from forest.schema_bundles import module_schema_namespace
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_storage_requests import DeploymentStorageRequestAuthority
from forest.storage.deployment_store import DeploymentStore
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore


@pytest.mark.postgres
async def test_host_reserve_replays_after_deployment_lease_loss_and_hot_reload(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    host_name = "forest_host_" + uuid4().hex
    deployment_name = "forest_deployment_" + uuid4().hex
    host_id = "host/" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(host_name)))
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(deployment_name)))
    parsed = urlsplit(dsn)

    def database_uri(name):
        return urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))

    host_store = HostStore(database_uri(host_name), host_id)
    deployment_store = DeploymentStore(database_uri(deployment_name), "deployment/bridge")
    await host_store.open()
    await deployment_store.open()
    host_supervisor = HostSupervisor(HostIdentityAuthority(host_store))
    controller = None
    created_physical = None
    try:
        await host_supervisor.start()
        catalog_authority = DeploymentCatalogRepository(deployment_store)
        await catalog_authority.initialize()
        lease = await catalog_authority.claim_supervisor(
            host_id, "incarnation/storage-bridge",
        )
        requests = DeploymentStorageRequestAuthority(deployment_store)
        frozen = await requests.request(
            operation_id="bridge/request",
            host_id=host_id, bot_id="bot/bridge", storage_generation=1,
            profile_ref="storage-profile/bridge/1",
            dsn_secret_ref="secret://bridge/dsn",
            credential_ref="secret://bridge/password",
        )
        source = tmp_path / "host-storage"
        shutil.copytree(
            Path(__file__).parents[1] / "src/forest/builtin/host_storage", source,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        catalog = PackageCatalog(tmp_path / "catalog")

        async def credential(ref, purpose):
            assert ref == "secret://bridge/password"
            assert purpose == "storage.postgresql.password"
            return "forest-host-storage-bridge-password-for-tests"

        def physical_factory(configuration, revision, identity, declaration):
            return HostPostgresMaterializationEffect(
                configuration, revision, identity, declaration,
                store=host_store, supervisor=host_supervisor,
                maintenance_dsn=dsn, credential_resolver=credential,
            )

        controller = HostPackageController(
            HostRuntimeScope(
                host_store, host_supervisor,
                effect_driver_factories={
                    "host.postgresql-materializer": physical_factory,
                },
            ), catalog, official_sources=(source,),
        )
        await controller.start()
        bridge = DeploymentHostStorageBridge(
            requests=requests,
            deployment_supervisor=SimpleNamespace(
                authority_lease=lambda: replace(lease, owner_epoch=lease.owner_epoch + 1),
            ),
            host_supervisor=host_supervisor,
            host_controller_provider=lambda: controller,
        )
        with pytest.raises(ForestError) as stale:
            await bridge.reserve(frozen.request_id)
        assert stale.value.code == "STALE_DEPLOYMENT_SUPERVISOR"
        assert await requests.resource_binding(frozen.request_id) is None
        page = await controller.registry.get("host.storage").management_contract.query(
            "generations", {"bot_id": frozen.bot_id},
        )
        assert len(page["items"]) == 1
        reserved_id = page["items"][0]["resource_id"]
        created_physical = (
            page["items"][0]["physical_name"], page["items"][0]["role_name"],
        )

        old_graph = controller.registry.graph_identity
        manifest = source / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "0.7.0"', 'version = "0.7.1"', 1,
            ), encoding="utf-8",
        )
        replacement = catalog.build_official(source, scope="host")
        await controller.replace(
            "host.storage", replacement.digest,
            expected_graph_revision=old_graph.graph_revision,
        )
        bridge.deployment_supervisor = SimpleNamespace(authority_lease=lambda: lease)
        binding = await bridge.reserve(frozen.request_id)
        assert binding.resource_id == reserved_id
        assert await bridge.reserve(frozen.request_id) == binding
        foreign = await requests.request(
            operation_id="bridge/foreign-request",
            host_id="host/" + "f" * 32,
            bot_id="bot/foreign-host", storage_generation=1,
            profile_ref="storage-profile/foreign-host/1",
            dsn_secret_ref="secret://foreign-host/dsn",
            credential_ref="secret://foreign-host/password",
        )
        with pytest.raises(ForestError) as wrong_host:
            await bridge.reserve(foreign.request_id)
        assert wrong_host.value.code == "STORAGE_REQUEST_HOST_UNAVAILABLE"
        page = await controller.registry.get("host.storage").management_contract.query(
            "generations", {"bot_id": frozen.bot_id},
        )
        assert [item["resource_id"] for item in page["items"]] == [reserved_id]
        queued = await bridge.enqueue_materialization(frozen.request_id)
        assert queued["resource_id"] == reserved_id
        assert await bridge.enqueue_materialization(frozen.request_id) == queued
        async with asyncio.timeout(30):
            while True:
                status = await bridge.materialization_status(frozen.request_id)
                if status["state"] == "SUCCEEDED":
                    break
                if status["state"] == "FAILED":
                    pytest.fail(f"Host materialization failed: {status['last_error_code']}")
                await asyncio.sleep(0.1)
        assert status["resource_id"] == reserved_id
        resource = await controller.registry.get("host.storage").management_contract.query(
            "resource", {
                "resource_id": reserved_id,
                "deployment_id": frozen.deployment_id,
                "bot_id": frozen.bot_id,
                "storage_generation": frozen.storage_generation,
            },
        )
        assert resource["state"] == "ACTIVE"
        assert (resource["physical_name"], resource["role_name"]) == created_physical
        bridge.deployment_supervisor = SimpleNamespace(
            authority_lease=lambda: replace(lease, owner_epoch=lease.owner_epoch + 1),
        )
        with pytest.raises(ForestError) as stale_pin:
            await bridge.pin_reference(frozen.request_id)
        assert stale_pin.value.code == "STALE_DEPLOYMENT_SUPERVISOR"
        assert await requests.pinned_resource(frozen.request_id) is None
        async with host_store.pool.connection() as conn:
            pinned_count = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS count FROM {}.resource_reference_pins "
                            "WHERE resource_id=%s").format(
                        sql.Identifier(module_schema_namespace("forest.host.storage")),
                    ),
                    (reserved_id,),
                )
            ).fetchone()
        assert pinned_count["count"] == 1
        previous_graph = controller.registry.graph_identity
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "0.7.1"', 'version = "0.7.2"', 1,
            ), encoding="utf-8",
        )
        replacement = catalog.build_official(source, scope="host")
        await controller.replace(
            "host.storage", replacement.digest,
            expected_graph_revision=previous_graph.graph_revision,
        )
        bridge.deployment_supervisor = SimpleNamespace(authority_lease=lambda: lease)
        pinned, concurrent_pin = await asyncio.gather(
            bridge.pin_reference(frozen.request_id),
            bridge.pin_reference(frozen.request_id),
        )
        assert concurrent_pin == pinned
        assert pinned.request_id == frozen.request_id
        assert pinned.resource_id == reserved_id
        assert pinned.host_id == host_id
        assert pinned.resource_revision == resource["revision"]
        assert pinned.credential_digest == "sha256:" + hashlib.sha256(
            b"forest-host-storage-bridge-password-for-tests"
        ).hexdigest()
        assert (pinned.physical_name, pinned.role_name) == created_physical
        assert await bridge.pin_reference(frozen.request_id) == pinned
        assert await requests.pinned_resource(frozen.request_id) == pinned
        async with host_store.pool.connection() as conn:
            pinned_count = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS count FROM {}.resource_reference_pins "
                            "WHERE resource_id=%s").format(
                        sql.Identifier(module_schema_namespace("forest.host.storage")),
                    ),
                    (reserved_id,),
                )
            ).fetchone()
        assert pinned_count["count"] == 1
        secrets = DeploymentSecretAuthority(
            deployment_store, active_key_id="bridge-test-key",
            key_resolver=lambda _key_id: b"b" * 32,
        )
        profiles = DeploymentStorageProfileAuthority(deployment_store, secrets=secrets)
        settlement = DeploymentStorageSettlementAuthority(
            deployment_store, secrets=secrets, profiles=profiles,
            host_id=host_id, maintenance_dsn=dsn,
        )
        async with deployment_store.transaction() as conn:
            with pytest.raises(ForestError) as wrong_pinned_dsn:
                await secrets.put_in(
                    conn, operation_id=frozen.request_id + "/dsn",
                    secret_ref=frozen.dsn_secret_ref,
                    purpose="storage.postgresql.dsn",
                    plaintext=urlunsplit(urlsplit(settlement._target_dsn(
                        created_physical[1], created_physical[0],
                        "forest-host-storage-bridge-password-for-tests",
                    ))._replace(path="/wrong_database")).encode(),
                    expected_revision=0,
                    pinned_request_id=frozen.request_id,
                )
        assert wrong_pinned_dsn.value.code == "STORAGE_SETTLEMENT_MISMATCH"
        with pytest.raises(ForestError) as wrong_password:
            await settlement.finalize(
                lease, request_id=frozen.request_id,
                password="another-password-with-sufficient-length-for-test",
            )
        assert wrong_password.value.code == "STORAGE_SETTLEMENT_MISMATCH"
        with pytest.raises(ForestError) as stale_settlement:
            await settlement.finalize(
                replace(lease, owner_epoch=lease.owner_epoch + 1),
                request_id=frozen.request_id,
                password="forest-host-storage-bridge-password-for-tests",
            )
        assert stale_settlement.value.code == "STALE_DEPLOYMENT_SUPERVISOR"
        with pytest.raises(ForestError) as unpublished:
            await profiles.metadata(frozen.profile_ref)
        assert unpublished.value.code == "STORAGE_PROFILE_NOT_FOUND"

        settled, concurrent_settlement = await asyncio.gather(
            settlement.finalize(
                lease, request_id=frozen.request_id,
                password="forest-host-storage-bridge-password-for-tests",
            ),
            settlement.finalize(
                lease, request_id=frozen.request_id,
                password="forest-host-storage-bridge-password-for-tests",
            ),
        )
        assert concurrent_settlement == settled
        assert settled.profile_ref == frozen.profile_ref
        assert settled.dsn_secret_ref == frozen.dsn_secret_ref
        assert await profiles.metadata(frozen.profile_ref) == settled
        secret_metadata = await secrets.metadata(frozen.dsn_secret_ref)
        assert secret_metadata.purpose == "storage.postgresql.dsn"
        assert secret_metadata.version == 1
        target_dsn = await secrets.resolve_text(frozen.dsn_secret_ref)
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        try:
            actual = await (await target.execute(
                "SELECT current_user AS role_name,current_database() AS database_name",
            )).fetchone()
            assert actual == created_physical[::-1]
        finally:
            await target.close()
        with pytest.raises(ForestError) as ordinary_rewrite:
            await secrets.put(
                operation_id="bridge/rewrite-dsn",
                secret_ref=frozen.dsn_secret_ref,
                purpose="storage.postgresql.dsn",
                plaintext=target_dsn.encode(), expected_revision=0,
            )
        assert ordinary_rewrite.value.code == "STORAGE_REQUEST_DSN_RESERVED"
        async with deployment_store.transaction() as conn:
            with pytest.raises(ForestError) as privileged_rewrite:
                await secrets.put_in(
                    conn, operation_id="bridge/rewrite-pinned-dsn",
                    secret_ref=frozen.dsn_secret_ref,
                    purpose="storage.postgresql.dsn",
                    plaintext=target_dsn.encode(), expected_revision=0,
                    pinned_request_id=frozen.request_id,
                )
        assert privileged_rewrite.value.code == "STORAGE_REQUEST_DSN_SETTLED"
        alternate_endpoint = DeploymentStorageSettlementAuthority(
            deployment_store, secrets=secrets, profiles=profiles,
            host_id=host_id,
            maintenance_dsn=dsn.replace("@127.0.0.1:", "@localhost:"),
        )
        with pytest.raises(ForestError) as changed_endpoint:
            await alternate_endpoint.finalize(
                lease, request_id=frozen.request_id,
                password="forest-host-storage-bridge-password-for-tests",
            )
        assert changed_endpoint.value.code == "STORAGE_SETTLEMENT_CONFLICT"

    finally:
        if controller is not None:
            await controller.close()
        await host_supervisor.close()
        await deployment_store.close()
        await host_store.close()
        if created_physical is not None:
            assert created_physical[0].startswith("forest_bot_")
            assert created_physical[1].startswith("forest_role_")
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(created_physical[0]),
            ))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(
                sql.Identifier(created_physical[1]),
            ))
        assert host_name.startswith("forest_host_")
        assert deployment_name.startswith("forest_deployment_")
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(host_name),
        ))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(deployment_name),
        ))
        await admin.close()
