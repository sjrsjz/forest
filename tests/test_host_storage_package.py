"""Host storage reservations stay module-owned across Generation replacement."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_storage_postgres_effect import HostPostgresMaterializationEffect
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore


def test_host_storage_internal_write_binding_rejects_another_command(tmp_path):
    source = tmp_path / "altered"
    shutil.copytree(
        Path(__file__).parents[1] / "src/forest/builtin/host_storage", source,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'internal_command = "host.storage.reserve-record"',
            'internal_command = "host.storage.other-command"',
        ), encoding="utf-8",
    )
    with pytest.raises(ForestError) as rejected:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert rejected.value.code == "INVALID_CONTROL_INTERNAL_COMMAND"


@pytest.mark.postgres
async def test_host_storage_reservation_is_fenced_idempotent_and_survives_reload(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    database = "forest_host_" + uuid4().hex
    host_id = "host/" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + database, "", ""))
    store = HostStore(uri, host_id)
    await store.open()
    supervisor = HostSupervisor(HostIdentityAuthority(store))
    controller = None
    created_physical = None
    try:
        await supervisor.start()
        source = tmp_path / "host-storage"
        shutil.copytree(
            Path(__file__).parents[1] / "src/forest/builtin/host_storage", source,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        catalog = PackageCatalog(tmp_path / "catalog")

        async def credential(ref: str, purpose: str) -> str:
            assert ref == "secret/storage/test"
            assert purpose == "storage.postgresql.password"
            return "forest-host-storage-password-for-tests"

        def physical_factory(configuration, revision, identity, declaration):
            return HostPostgresMaterializationEffect(
                configuration, revision, identity, declaration,
                store=store, supervisor=supervisor, maintenance_dsn=dsn,
                credential_resolver=credential,
            )

        controller = HostPackageController(
            HostRuntimeScope(
                store, supervisor,
                effect_driver_factories={
                    "host.postgresql-materializer": physical_factory,
                },
            ), catalog,
            official_sources=(source,),
        )
        registry = await controller.start()
        member = registry.get("host.storage")
        assert await member.management_contract.query("generations", {}) == {
            "items": [], "next_after": None,
        }
        graph = registry.graph_identity
        route = FrozenServiceRoute(
            service_id="host.storage.reserve", version=1,
            caller_component_id="host.test-bridge",
            provider_component_id="host.storage",
            provider_generation_id=graph.generations["host.storage"],
            graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        endpoint = member.runtime_service_handlers()[("host.storage.reserve", 1)]
        payload = {
            "deployment_id": "deployment/test", "bot_id": "bot/storage",
            "storage_generation": 1,
        }

        async def reserve(operation: str, value: dict):
            return await endpoint.invoke_routed(
                route,
                RuntimeServiceInvocation(operation, 10, "test.host-storage", operation),
                (value,), {},
            )

        first = await reserve("reserve/storage/1", payload)
        assert first["state"] == "RESERVED"
        assert first["host_id"] == host_id
        assert first["resource_id"].startswith("resource/")
        assert await reserve("reserve/storage/1", payload) == first
        with pytest.raises(ForestError) as altered:
            await reserve("reserve/storage/1", {**payload, "storage_generation": 2})
        assert altered.value.code == "HOST_STORAGE_OPERATION_CONFLICT"
        with pytest.raises(ForestError) as duplicate:
            await reserve("reserve/storage/2", payload)
        assert duplicate.value.code == "HOST_STORAGE_RESOURCE_CONFLICT"
        page = await member.management_contract.query(
            "generations", {"bot_id": "bot/storage", "limit": 1},
        )
        assert len(page["items"]) == 1
        assert page["items"][0]["resource_id"] == first["resource_id"]
        assert page["items"][0]["physical_name"].startswith("forest_bot_")
        assert "owner_marker" not in page["items"][0]
        created_physical = (
            page["items"][0]["physical_name"],
            page["items"][0]["role_name"],
        )
        physical_route = FrozenServiceRoute(
            service_id="host.storage.postgres.materialize", version=1,
            caller_component_id="host.storage",
            provider_component_id="host.storage",
            provider_generation_id=graph.generations["host.storage"],
            graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        materialize = member.runtime_service_handlers()[
            ("host.storage.postgres.materialize", 1)
        ]
        receipt = await materialize.invoke_routed(
            physical_route,
            RuntimeServiceInvocation(
                "materialize/storage/1", 10, "test.host-storage", "bot/storage",
            ),
            ({"resource_id": first["resource_id"],
              "credential_ref": "secret/storage/test"},), {},
        )
        assert receipt["physical_name"] == created_physical[0]
        assert receipt["role_name"] == created_physical[1]
        assert receipt["owner_marker_digest"].startswith("sha256:")
        realize_route = FrozenServiceRoute(
            service_id="host.storage.realize", version=1,
            caller_component_id="host.test-bridge",
            provider_component_id="host.storage",
            provider_generation_id=graph.generations["host.storage"],
            graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        realize = member.runtime_service_handlers()[("host.storage.realize", 1)]
        with pytest.raises(ForestError) as forged:
            await realize.invoke_routed(
                realize_route,
                RuntimeServiceInvocation(
                    "realize/storage/forged", 10, "test.host-storage", "bot/storage",
                ),
                ({**receipt, "owner_marker_digest": "sha256:" + "0" * 64},), {},
            )
        assert forged.value.code == "HOST_STORAGE_RECEIPT_MISMATCH"
        database_row = await (
            await admin.execute(
                "SELECT pg_get_userbyid(datdba) AS owner,"
                "shobj_description(oid,'pg_database') AS marker "
                "FROM pg_database WHERE datname=%s", (created_physical[0],),
            )
        ).fetchone()
        assert database_row[0] == created_physical[1]
        assert first["resource_id"] in database_row[1]
        await admin.execute(sql.SQL("COMMENT ON DATABASE {} IS NULL").format(
            sql.Identifier(created_physical[0])
        ))
        assert await materialize.invoke_routed(
            physical_route,
            RuntimeServiceInvocation(
                "materialize/storage/reconcile", 10, "test.host-storage", "bot/storage",
            ),
            ({"resource_id": first["resource_id"],
              "credential_ref": "secret/storage/test"},), {},
        ) == receipt
        database_row = await (
            await admin.execute(
                "SELECT shobj_description(oid,'pg_database') "
                "FROM pg_database WHERE datname=%s", (created_physical[0],),
            )
        ).fetchone()
        assert first["resource_id"] in database_row[0]
        await admin.execute(sql.SQL("COMMENT ON DATABASE {} IS 'another owner'").format(
            sql.Identifier(created_physical[0])
        ))
        with pytest.raises(ForestError) as mismatched:
            await materialize.invoke_routed(
                physical_route,
                RuntimeServiceInvocation(
                    "materialize/storage/mismatch", 10, "test.host-storage", "bot/storage",
                ),
                ({"resource_id": first["resource_id"],
                  "credential_ref": "secret/storage/test"},), {},
            )
        assert mismatched.value.code == "HOST_STORAGE_DATABASE_MISMATCH"

        manifest = source / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "0.3.0"', 'version = "0.3.1"', 1,
            ), encoding="utf-8",
        )
        replacement = catalog.build_official(source, scope="host")
        current = await controller.replace(
            "host.storage", replacement.digest,
            expected_graph_revision=graph.graph_revision,
        )
        assert current.graph_identity.graph_revision == graph.graph_revision + 1
        assert (await current.get("host.storage").management_contract.query(
            "generations", {"bot_id": "bot/storage"},
        ))["items"][0]["resource_id"] == first["resource_id"]
        new_graph = current.graph_identity
        new_route = FrozenServiceRoute(
            service_id="host.storage.reserve", version=1,
            caller_component_id="host.test-bridge",
            provider_component_id="host.storage",
            provider_generation_id=new_graph.generations["host.storage"],
            graph_id=new_graph.graph_id, graph_revision=new_graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        new_endpoint = current.get("host.storage").runtime_service_handlers()[
            ("host.storage.reserve", 1)
        ]
        assert await new_endpoint.invoke_routed(
            new_route,
            RuntimeServiceInvocation(
                "reserve/storage/1", 10, "test.host-storage", "reserve/storage/1",
            ),
            (payload,), {},
        ) == first
        new_physical_route = FrozenServiceRoute(
            service_id="host.storage.postgres.materialize", version=1,
            caller_component_id="host.storage",
            provider_component_id="host.storage",
            provider_generation_id=new_graph.generations["host.storage"],
            graph_id=new_graph.graph_id, graph_revision=new_graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        new_materialize = current.get("host.storage").runtime_service_handlers()[
            ("host.storage.postgres.materialize", 1)
        ]
        with pytest.raises(ForestError) as mismatch_after_reload:
            await new_materialize.invoke_routed(
                new_physical_route,
                RuntimeServiceInvocation(
                    "materialize/storage/new-generation", 10,
                    "test.host-storage", "bot/storage",
                ),
                ({"resource_id": first["resource_id"],
                  "credential_ref": "secret/storage/test"},), {},
            )
        assert mismatch_after_reload.value.code == "HOST_STORAGE_DATABASE_MISMATCH"
        await admin.execute(sql.SQL("COMMENT ON DATABASE {} IS NULL").format(
            sql.Identifier(created_physical[0])
        ))
        assert await new_materialize.invoke_routed(
            new_physical_route,
            RuntimeServiceInvocation(
                "materialize/storage/new-generation-reconcile", 10,
                "test.host-storage", "bot/storage",
            ),
            ({"resource_id": first["resource_id"],
              "credential_ref": "secret/storage/test"},), {},
        ) == receipt
        new_realize_route = FrozenServiceRoute(
            service_id="host.storage.realize", version=1,
            caller_component_id="host.test-bridge",
            provider_component_id="host.storage",
            provider_generation_id=new_graph.generations["host.storage"],
            graph_id=new_graph.graph_id, graph_revision=new_graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        new_realize = current.get("host.storage").runtime_service_handlers()[
            ("host.storage.realize", 1)
        ]
        realized = await new_realize.invoke_routed(
            new_realize_route,
            RuntimeServiceInvocation(
                "realize/storage/1", 10, "test.host-storage", "bot/storage",
            ),
            (receipt,), {},
        )
        assert realized == {
            "protocol": "forest.host-storage-realized/1",
            "resource_id": first["resource_id"],
            "host_id": host_id, "state": "ACTIVE", "revision": 2,
        }
        assert await new_realize.invoke_routed(
            new_realize_route,
            RuntimeServiceInvocation(
                "realize/storage/1", 10, "test.host-storage", "bot/storage",
            ),
            (receipt,), {},
        ) == realized
        assert (await current.get("host.storage").management_contract.query(
            "generations", {"bot_id": "bot/storage", "state": "ACTIVE"},
        ))["items"][0]["revision"] == 2
        await admin.execute(sql.SQL("COMMENT ON DATABASE {} IS NULL").format(
            sql.Identifier(created_physical[0])
        ))
        with pytest.raises(ForestError) as unmarked_active:
            await new_materialize.invoke_routed(
                new_physical_route,
                RuntimeServiceInvocation(
                    "materialize/storage/active-unmarked", 10,
                    "test.host-storage", "bot/storage",
                ),
                ({"resource_id": first["resource_id"],
                  "credential_ref": "secret/storage/test"},), {},
            )
        assert unmarked_active.value.code == "HOST_STORAGE_DATABASE_MISMATCH"
    finally:
        if controller is not None:
            await controller.close()
        await supervisor.close()
        await store.close()
        if created_physical is not None:
            assert created_physical[0].startswith("forest_bot_")
            assert created_physical[1].startswith("forest_role_")
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(created_physical[0])
            ))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(
                sql.Identifier(created_physical[1])
            ))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(database)
        ))
        await admin.close()
