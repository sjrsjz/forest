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
from forest.deployment_storage_ports import DeploymentStoragePortEffectDriver
from forest.deployment_storage_settlement import DeploymentStorageSettlementAuthority
from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_storage_postgres_effect import HostPostgresMaterializationEffect
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog
from forest.schema_bundles import module_schema_namespace
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_dispatch import DeploymentStorageDispatchAuthority
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
    additional_physical = None
    try:
        await host_supervisor.start()
        catalog_authority = DeploymentCatalogRepository(deployment_store)
        await catalog_authority.initialize()
        lease = await catalog_authority.claim_supervisor(
            host_id, "incarnation/storage-bridge",
        )
        secrets = DeploymentSecretAuthority(
            deployment_store, active_key_id="bridge-test-key",
            key_resolver=lambda _key_id: b"b" * 32,
        )
        await secrets.put(
            operation_id="bridge/password-secret",
            secret_ref="secret://bridge/password",
            purpose="storage.postgresql.password",
            plaintext=b"forest-host-storage-bridge-password-for-tests",
            expected_revision=0,
        )
        requests = DeploymentStorageRequestAuthority(deployment_store, secrets=secrets)
        frozen = await requests.request(
            operation_id="bridge/request",
            host_id=host_id, bot_id="bot/bridge", storage_generation=1,
            profile_ref="storage-profile/bridge/1",
            dsn_secret_ref="secret://bridge/dsn",
            credential_ref="secret://bridge/password",
        )
        dispatch = DeploymentStorageDispatchAuthority(deployment_store)
        claim = await dispatch.claim(lease)
        assert claim.request_id == frozen.request_id
        await secrets.put(
            operation_id="bridge/automatic-password",
            secret_ref="secret://bridge/automatic-password",
            purpose="storage.postgresql.password",
            plaintext=b"forest-automatic-storage-password-for-tests",
            expected_revision=0,
        )
        automatic = await requests.request(
            operation_id="bridge/automatic-request",
            host_id=host_id, bot_id="bot/bridge-automatic", storage_generation=1,
            profile_ref="storage-profile/bridge-automatic/1",
            dsn_secret_ref="secret://bridge/automatic-dsn",
            credential_ref="secret://bridge/automatic-password",
        )
        source = tmp_path / "host-storage"
        shutil.copytree(
            Path(__file__).parents[1] / "src/forest/builtin/host_storage", source,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        catalog = PackageCatalog(tmp_path / "catalog")

        def physical_factory(configuration, revision, identity, declaration):
            return HostPostgresMaterializationEffect(
                configuration, revision, identity, declaration,
                store=host_store, supervisor=host_supervisor,
                maintenance_dsn=dsn,
                credential_resolver=secrets.resolve_text_for_purpose,
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
            profiles=DeploymentStorageProfileAuthority(
                deployment_store, secrets=secrets,
            ),
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
                'version = "0.8.0"', 'version = "0.8.1"', 1,
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
        await secrets.put(
            operation_id="bridge/foreign-password-secret",
            secret_ref="secret://foreign-host/password",
            purpose="storage.postgresql.password",
            plaintext=b"forest-foreign-host-password-for-tests",
            expected_revision=0,
        )
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
                'version = "0.8.1"', 'version = "0.8.2"', 1,
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
        assert (await dispatch.settle(lease, claim, outcome="SETTLED"))["state"] == "SETTLED"
        assert (await dispatch.read(frozen.request_id))["state"] == "SETTLED"
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

        bootstrap = await DeploymentBootstrapProfileAuthority(
            deployment_store, secrets=secrets,
        ).create(
            operation_id="bridge/retirement-bootstrap",
            profile_ref="bootstrap-profile/bridge/retirement",
            bot_id=frozen.bot_id, storage_profile_ref=frozen.profile_ref,
            storage_generation=frozen.storage_generation,
            components={"conversation.trigger-policy": {
                "allowed_group_ids": ["933179609"],
            }},
            schema_digests={"conversation.trigger-policy": "sha256:" + "a" * 64},
            package_digests={"conversation.trigger-policy": "sha256:" + "c" * 64},
        )
        bot = await catalog_authority.register_ready(
            operation_id="bridge/retirement-ready", bot_id=frozen.bot_id,
            display_name="Bridge Retirement",
            storage_profile_ref=frozen.profile_ref,
            bootstrap_profile_ref=bootstrap.profile_ref,
            bootstrap_manifest_digest=bootstrap.manifest_digest,
        )
        with pytest.raises(ForestError) as not_revoked:
            await bridge.release_reference(frozen.request_id)
        assert not_revoked.value.code == "STORAGE_PROFILE_NOT_REVOKED"
        with pytest.raises(ForestError) as not_retired:
            await profiles.revoke(
                lease, operation_id="bridge/revoke-too-early",
                profile_ref=frozen.profile_ref, bot_id=frozen.bot_id,
                expected_bot_revision=bot["revision"],
            )
        assert not_retired.value.code == "BOT_RETIREMENT_NOT_FENCED"
        retired = await catalog_authority.begin_retirement(
            operation_id="bridge/retirement-begin", bot_id=bot["bot_id"],
            expected_revision=bot["revision"],
        )
        assert retired["supply_state"] == "DELETING"
        lease = await catalog_authority.renew_supervisor(lease, lease_seconds=120)
        revoked, replayed = await asyncio.gather(
            profiles.revoke(
                lease, operation_id="bridge/profile-revoke",
                profile_ref=frozen.profile_ref, bot_id=frozen.bot_id,
                expected_bot_revision=retired["revision"],
            ),
            profiles.revoke(
                lease, operation_id="bridge/profile-revoke",
                profile_ref=frozen.profile_ref, bot_id=frozen.bot_id,
                expected_bot_revision=retired["revision"],
            ),
        )
        assert replayed == revoked
        assert revoked["resource_id"] == reserved_id
        assert revoked["retirement_operation_id"] == "bridge/retirement-begin"
        assert revoked["observation_revision"] is None
        assert await profiles.metadata(frozen.profile_ref) == settled
        with pytest.raises(ForestError) as denied_resolve:
            await profiles.resolve(
                profile_ref=frozen.profile_ref, expected_bot_id=frozen.bot_id,
                expected_storage_generation=frozen.storage_generation,
            )
        assert denied_resolve.value.code == "STORAGE_PROFILE_REVOKED"
        with pytest.raises(ForestError) as duplicate_revoke:
            await profiles.revoke(
                lease, operation_id="bridge/profile-revoke-another",
                profile_ref=frozen.profile_ref, bot_id=frozen.bot_id,
                expected_bot_revision=retired["revision"],
            )
        assert duplicate_revoke.value.code == "STORAGE_PROFILE_REVOCATION_CONFLICT"
        with pytest.raises(ForestError) as bootstrap_after_revoke:
            await DeploymentBootstrapProfileAuthority(
                deployment_store, secrets=secrets,
            ).create(
                operation_id="bridge/bootstrap-after-revoke",
                profile_ref="bootstrap-profile/bridge/after-revoke",
                bot_id=frozen.bot_id, storage_profile_ref=frozen.profile_ref,
                storage_generation=frozen.storage_generation,
                components={"conversation.trigger-policy": {
                    "allowed_group_ids": ["933179609"],
                }},
                schema_digests={"conversation.trigger-policy": "sha256:" + "a" * 64},
                package_digests={"conversation.trigger-policy": "sha256:" + "c" * 64},
            )
        assert bootstrap_after_revoke.value.code == "STORAGE_PROFILE_REVOKED"
        with pytest.raises(ForestError) as job_after_revoke:
            await DeploymentProvisioningJobAuthority(deployment_store).enqueue(
                operation_id="bridge/job-after-revoke",
                job_id="provisioning/bridge/after-revoke",
                bot_id=frozen.bot_id, display_name="Retired Bridge",
                storage_profile_ref=frozen.profile_ref,
                storage_generation=frozen.storage_generation,
                bootstrap_profile_ref=bootstrap.profile_ref,
            )
        assert job_after_revoke.value.code == "STORAGE_PROFILE_REVOKED"

        released, release_replay = await asyncio.gather(
            bridge.release_reference(frozen.request_id),
            bridge.release_reference(frozen.request_id),
        )
        assert released == release_replay
        assert released["resource_id"] == reserved_id
        assert released["state"] == "RETIRED"
        assert released["resource_revision"] == pinned.resource_revision + 1
        assert released["revocation_operation_id"] == revoked["operation_id"]
        assert await bridge.release_reference(frozen.request_id) == released
        resource = await controller.registry.get("host.storage").management_contract.query(
            "resource", {
                "resource_id": reserved_id,
                "deployment_id": frozen.deployment_id,
                "bot_id": frozen.bot_id,
                "storage_generation": frozen.storage_generation,
            },
        )
        assert resource["state"] == "RETIRED"
        async with host_store.pool.connection() as conn:
            release_history = await (
                await conn.execute(
                    sql.SQL("SELECT (SELECT count(*) FROM {}.resource_reference_pins "
                            "WHERE resource_id=%s) AS pins, "
                            "(SELECT count(*) FROM {}.resource_reference_releases "
                            "WHERE resource_id=%s) AS releases").format(
                        sql.Identifier(module_schema_namespace("forest.host.storage")),
                        sql.Identifier(module_schema_namespace("forest.host.storage")),
                    ),
                    (reserved_id, reserved_id),
                )
            ).fetchone()
        assert release_history == {"pins": 1, "releases": 1}

        automatic_claim = await dispatch.claim(lease)
        assert automatic_claim.request_id == automatic.request_id

        async def accept_generation(_conn, _lease, **_options):
            return None

        identity = SimpleNamespace(
            component_id="deployment.storage-orchestrator",
            generation_id="generation/bridge-test",
        )
        runtime = SimpleNamespace(
            scope_id=deployment_store.deployment_id,
            store=deployment_store,
            authority_lease=lambda: lease,
            generation_authority=SimpleNamespace(
                verify_owned_work_in=accept_generation,
            ),
        )
        ports = {}
        for name, semantics in (
            ("inspect", "read"), ("enqueue", "idempotent"),
            ("status", "read"), ("pin", "idempotent"),
            ("finalize", "idempotent"),
        ):
            port = DeploymentStoragePortEffectDriver(
                {}, 1, identity,
                {"name": f"deployment.storage-orchestrator.{name}",
                 "kind": "deployment.host-storage-port", "semantics": semantics},
                runtime=runtime, dispatch=dispatch, requests=requests,
                profiles=profiles, secrets=secrets, bridge=bridge,
                settlement=settlement,
            )
            await port.start()
            ports[name] = port
        scope = SimpleNamespace(
            scope_kind="deployment", scope_id=deployment_store.deployment_id,
            component_id=identity.component_id, generation_id=identity.generation_id,
        )
        payload = {
            "request_id": automatic.request_id,
            "claim_token": automatic_claim.claim_token,
            "attempt_no": automatic_claim.attempt_no,
        }
        async def call_port(name):
            return await ports[name].invoke(payload, scope=scope, invocation=None)

        assert (await call_port("inspect"))["state"] == "PENDING"
        async with asyncio.timeout(30):
            while True:
                assert (await call_port("enqueue"))["state"] == "ENQUEUED"
                status = await call_port("status")
                if status["state"] == "SUCCEEDED":
                    break
                assert status["state"] in {"PENDING", "CLAIMED"}
                await asyncio.sleep(0.1)
        assert (await call_port("pin"))["state"] == "PINNED"
        assert (await call_port("finalize"))["state"] == "SETTLED"
        assert (await call_port("inspect"))["state"] == "SETTLED"
        await dispatch.settle(lease, automatic_claim, outcome="SETTLED")
        assert (await profiles.metadata(automatic.profile_ref)).bot_id == automatic.bot_id
        resource = await controller.registry.get("host.storage").management_contract.query(
            "generations", {"bot_id": automatic.bot_id},
        )
        additional_physical = (
            resource["items"][0]["physical_name"],
            resource["items"][0]["role_name"],
        )
        target_dsn = await secrets.resolve_text(automatic.dsn_secret_ref)
        target = await psycopg.AsyncConnection.connect(target_dsn, autocommit=True)
        try:
            actual = await (await target.execute(
                "SELECT current_user AS role_name,current_database() AS database_name",
            )).fetchone()
            assert actual == additional_physical[::-1]
        finally:
            await target.close()

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
        if additional_physical is not None:
            assert additional_physical[0].startswith("forest_bot_")
            assert additional_physical[1].startswith("forest_role_")
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(additional_physical[0]),
            ))
            await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(
                sql.Identifier(additional_physical[1]),
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
