"""A lost deployment settlement reuses the same Host reservation after reload."""

from __future__ import annotations

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

from forest.deployment_host_storage_bridge import DeploymentHostStorageReservationBridge
from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_storage_postgres_effect import HostPostgresMaterializationEffect
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog
from forest.storage.deployment_catalog import DeploymentCatalogRepository
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

        async def credential(_ref, _purpose):
            return "bridge-test-password"

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
        bridge = DeploymentHostStorageReservationBridge(
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

        old_graph = controller.registry.graph_identity
        manifest = source / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "0.6.0"', 'version = "0.6.1"', 1,
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
        assert page["items"][0]["state"] == "RESERVED"
    finally:
        if controller is not None:
            await controller.close()
        await host_supervisor.close()
        await deployment_store.close()
        await host_store.close()
        assert host_name.startswith("forest_host_")
        assert deployment_name.startswith("forest_deployment_")
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(host_name),
        ))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(deployment_name),
        ))
        await admin.close()
