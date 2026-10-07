"""Host storage reservations stay module-owned across Generation replacement."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_storage_postgres_effect import HostPostgresMaterializationEffect
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.schema_bundles import module_schema_namespace
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore


async def _realize_private(store, host_id, operation_id, receipt):
    """Exercise the package-private Command without publishing it as a service."""
    context = {
        "scope_kind": "host", "scope_id": host_id,
        "component_id": "host.storage", "port_kind": "command",
        "port_name": "host.storage.realize-record",
        "operation_id": operation_id, "input_digest": digest_json(receipt),
    }
    async with store.pool.connection() as conn, conn.transaction():
        row = await (
            await conn.execute(
                sql.SQL("SELECT {}.host_storage_realize(%s::jsonb,%s::jsonb) AS result")
                .format(sql.Identifier(module_schema_namespace("forest.host.storage"))),
                (Jsonb(context), Jsonb(receipt)),
            )
        ).fetchone()
    outcome = row["result"]
    if not outcome["ok"]:
        error = outcome["error"]
        raise ForestError(error["code"], error["message"], error["status"])
    return outcome["value"]


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


def test_host_storage_private_work_command_must_be_declared(tmp_path):
    source = tmp_path / "altered"
    shutil.copytree(
        Path(__file__).parents[1] / "src/forest/builtin/host_storage", source,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    manifest = source / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'private_commands = ["host.storage.realize-record"]',
            'private_commands = ["host.storage.missing-command"]',
        ), encoding="utf-8",
    )
    with pytest.raises(ForestError) as rejected:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert rejected.value.code == "INVALID_OWNED_WORK"


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
    queued_physical = None
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
        assert ("host.storage.realize", 1) not in member.runtime_service_handlers()
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
        assert receipt["credential_digest"] == "sha256:" + hashlib.sha256(
            b"forest-host-storage-password-for-tests"
        ).hexdigest()
        with pytest.raises(ForestError) as forged:
            await _realize_private(
                store, host_id, "realize/storage/forged",
                {**receipt, "owner_marker_digest": "sha256:" + "0" * 64},
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
                'version = "0.7.0"', 'version = "0.7.1"', 1,
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
        realized = await _realize_private(store, host_id, "realize/storage/1", receipt)
        assert realized == {
            "protocol": "forest.host-storage-realized/1",
            "resource_id": first["resource_id"],
            "host_id": host_id, "state": "ACTIVE", "revision": 2,
            "credential_digest": receipt["credential_digest"],
        }
        assert await _realize_private(store, host_id, "realize/storage/1", receipt) == realized
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

        queued_payload = {**payload, "storage_generation": 2}
        queued_reservation = await new_endpoint.invoke_routed(
            new_route,
            RuntimeServiceInvocation(
                "reserve/storage/queued", 10, "test.host-storage", "bot/storage",
            ),
            (queued_payload,), {},
        )
        queued_page = await current.get("host.storage").management_contract.query(
            "generations", {"bot_id": "bot/storage", "state": "RESERVED"},
        )
        queued_row = next(item for item in queued_page["items"]
                          if item["resource_id"] == queued_reservation["resource_id"])
        queued_physical = (queued_row["physical_name"], queued_row["role_name"])
        enqueue_route = FrozenServiceRoute(
            service_id="host.storage.materialization.enqueue", version=1,
            caller_component_id="host.test-bridge",
            provider_component_id="host.storage",
            provider_generation_id=new_graph.generations["host.storage"],
            graph_id=new_graph.graph_id, graph_revision=new_graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        enqueue = current.get("host.storage").runtime_service_handlers()[
            ("host.storage.materialization.enqueue", 1)
        ]
        current.get("host.storage").freeze_owned_admission()
        await current.get("host.storage").drain_owned_work()
        request = {
            "resource_id": queued_reservation["resource_id"],
            "credential_ref": "secret/storage/test",
        }
        lookup = {
            "resource_id": queued_reservation["resource_id"],
            "deployment_id": queued_reservation["deployment_id"],
            "bot_id": queued_reservation["bot_id"],
            "storage_generation": queued_reservation["storage_generation"],
        }
        reserved_resource = await current.get("host.storage").management_contract.query(
            "resource", lookup,
        )
        assert reserved_resource["physical_name"] == queued_physical[0]
        assert reserved_resource["role_name"] == queued_physical[1]
        assert reserved_resource["state"] == "RESERVED"
        with pytest.raises(ForestError) as other_owner:
            await current.get("host.storage").management_contract.query(
                "resource", {**lookup, "deployment_id": "another-deployment"},
            )
        assert other_owner.value.code == "HOST_STORAGE_RESOURCE_NOT_FOUND"
        async def enqueue_request(operation_id, value):
            return await enqueue.invoke_routed(
                enqueue_route,
                RuntimeServiceInvocation(
                    operation_id, 10, "test.host-storage", "bot/storage",
                ),
                (value,), {},
            )

        accepted, replay = await asyncio.gather(
            enqueue_request("materialize/queued", request),
            enqueue_request("materialize/queued", request),
        )
        assert replay == accepted
        assert accepted["resource_id"] == queued_reservation["resource_id"]
        assert accepted["state"] == "PENDING"
        with pytest.raises(ForestError) as conflicting_request:
            await enqueue_request(
                "materialize/another-request",
                {**request, "credential_ref": "secret/storage/other"},
            )
        assert conflicting_request.value.code == "HOST_STORAGE_MATERIALIZATION_CONFLICT"
        assert await current.get("host.storage").management_contract.query(
            "materialization.status", lookup,
        ) == accepted
        with pytest.raises(ForestError) as other_status:
            await current.get("host.storage").management_contract.query(
                "materialization.status", {**lookup, "bot_id": "another-bot"},
            )
        assert other_status.value.code == "HOST_STORAGE_MATERIALIZATION_NOT_FOUND"
        async with store.pool.connection() as conn, conn.transaction():
            abandoned = (await (await conn.execute(
                sql.SQL("SELECT {}.host_storage_claim_materialization(%s::jsonb,%s::jsonb) "
                        "AS result").format(sql.Identifier(
                    module_schema_namespace("forest.host.storage")
                )),
                (Jsonb({"scope_kind": "host", "scope_id": host_id,
                        "component_id": "host.storage"}),
                 Jsonb({"protocol": "forest.owned-work-claim-request/1",
                        "claim_token": "abandoned/queued", "lease_seconds": 3})),
            )).fetchone())["result"]
        assert abandoned["ok"] and abandoned["value"]["work_id"] == queued_reservation["resource_id"]
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "0.7.1"', 'version = "0.7.2"', 1,
            ), encoding="utf-8",
        )
        resumed_package = catalog.build_official(source, scope="host")
        current = await controller.replace(
            "host.storage", resumed_package.digest,
            expected_graph_revision=new_graph.graph_revision,
        )
        async with asyncio.timeout(20):
            while True:
                async with store.pool.connection() as conn:
                    work = (await (await conn.execute(
                        sql.SQL("SELECT state,attempt_count,result_payload,last_error_code "
                                "FROM {}.materialization_work WHERE resource_id=%s").format(
                            sql.Identifier(module_schema_namespace("forest.host.storage"))
                        ), (queued_reservation["resource_id"],),
                    )).fetchone())
                if work["state"] == "SUCCEEDED":
                    break
                if work["state"] == "FAILED":
                    pytest.fail(f"Host materialization failed: {work['last_error_code']}")
                await asyncio.sleep(0.1)
        assert work["attempt_count"] == 2
        assert work["result_payload"]["state"] == "ACTIVE"
        assert await current.get("host.storage").management_contract.query(
            "materialization.status", lookup,
        ) == {
            "resource_id": queued_reservation["resource_id"],
            "state": "SUCCEEDED", "attempt_count": 2, "last_error_code": None,
        }
        active_page = await current.get("host.storage").management_contract.query(
            "generations", {"bot_id": "bot/storage", "state": "ACTIVE"},
        )
        active_resource = await current.get("host.storage").management_contract.query(
            "resource", lookup,
        )
        assert active_resource["state"] == "ACTIVE"
        assert active_resource["physical_name"] == queued_physical[0]
        final_graph = current.graph_identity
        pin_route = FrozenServiceRoute(
            service_id="host.storage.reference.pin", version=1,
            caller_component_id="host.test-bridge",
            provider_component_id="host.storage",
            provider_generation_id=final_graph.generations["host.storage"],
            graph_id=final_graph.graph_id, graph_revision=final_graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        pin = current.get("host.storage").runtime_service_handlers()[
            ("host.storage.reference.pin", 1)
        ]

        async def pin_request(operation_id, value):
            return await pin.invoke_routed(
                pin_route,
                RuntimeServiceInvocation(
                    operation_id, 10, "test.host-storage", "bot/storage",
                ),
                (value,), {},
            )

        pin_input = {**lookup, "profile_ref": "storage-profile/queued"}
        pinned, pin_replay = await asyncio.gather(
            pin_request("pin/queued", pin_input),
            pin_request("pin/queued", pin_input),
        )
        assert pinned == pin_replay
        assert pinned["resource_id"] == queued_reservation["resource_id"]
        assert pinned["state"] == "PINNED"
        assert pinned["resource_revision"] == active_resource["revision"]
        assert pinned["credential_digest"] == receipt["credential_digest"]
        with pytest.raises(ForestError) as other_pin:
            await pin_request(
                "pin/other-profile", {**pin_input, "profile_ref": "storage-profile/other"},
            )
        assert other_pin.value.code == "HOST_STORAGE_REFERENCE_CONFLICT"
        with pytest.raises(ForestError) as other_owner_pin:
            await pin_request(
                "pin/other-owner", {**pin_input, "deployment_id": "another-deployment"},
            )
        assert other_owner_pin.value.code == "HOST_STORAGE_RESOURCE_UNAVAILABLE"
        with pytest.raises(ForestError) as manual_pin:
            await pin_request("pin/manual", {
                "resource_id": first["resource_id"],
                "deployment_id": payload["deployment_id"],
                "bot_id": payload["bot_id"],
                "storage_generation": payload["storage_generation"],
                "profile_ref": "storage-profile/manual",
            })
        assert manual_pin.value.code == "HOST_STORAGE_RESOURCE_UNAVAILABLE"
        assert queued_reservation["resource_id"] in {
            item["resource_id"] for item in active_page["items"]
        }
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
        if queued_physical is not None:
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(queued_physical[0])
            ))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(
                sql.Identifier(queued_physical[1])
            ))
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
            sql.Identifier(database)
        ))
        await admin.close()
