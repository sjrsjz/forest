"""Host scope identity survives process restarts and fences stale incarnations."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.errors import ForestError
from forest.packages import storage_bundle_digest
from forest.runtime_generation import PreparedGenerationBinding, RuntimeStorageBinding
from forest.schema_bundles import StorageBundleSpec
from forest.storage.host_generation import HostGenerationAuthority
from forest.storage.host_identity import HostIdentityAuthority, HostLease
from forest.storage.host_schema_bundles import HostSchemaBundleInstaller
from forest.storage.host_store import HostStore


@pytest.mark.postgres
async def test_host_identity_is_stable_and_incarnation_lease_is_fenced():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_host_" + uuid4().hex
    host_id = "host/" + uuid4().hex
    first_incarnation = "incarnation/" + str(uuid4())
    second_incarnation = "incarnation/" + str(uuid4())
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    first_store = HostStore(uri, host_id)
    await first_store.open()
    try:
        first = HostIdentityAuthority(first_store)
        identity = await first.initialize()
        assert identity["host_id"] == host_id
        assert (await first.initialize())["baseline_sha256"] == identity["baseline_sha256"]
        baseline = b"CREATE TABLE host_state (value text NOT NULL);"
        spec = StorageBundleSpec(
            component_id="host.sample", schema_id="host.sample.state",
            schema_version="1.0.0", baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract="host.sample/1", scope="host",
        )
        installer = HostSchemaBundleInstaller(first_store)
        digest = storage_bundle_digest(spec, "declarative")
        installed = await installer.install(spec, baseline, digest)
        assert installed["host_id"] == host_id
        assert installed["namespace"] == spec.namespace
        assert await installer.install(spec, baseline, digest) == installed
        assert (await first.initialize())["host_id"] == host_id
        wrong_spec = StorageBundleSpec(
            component_id="host.sample", schema_id="host.sample.wrong-scope",
            schema_version="1.0.0", baseline_sha256=hashlib.sha256(baseline).hexdigest(),
            repository_contract="host.sample/1", scope="deployment",
        )
        with pytest.raises(ForestError) as wrong_bundle:
            await installer.install(wrong_spec, baseline, digest)
        assert wrong_bundle.value.code == "INVALID_STORAGE_BUNDLE"
        lease = await first.claim(first_incarnation)
        assert (await first.claim(first_incarnation)).owner_epoch == lease.owner_epoch

        package_digest = "sha256:" + "1" * 64
        binding = PreparedGenerationBinding(
            component_id="host.sample", requires=(), package_id="host.sample",
            package_version="1.0.0", package_digest=package_digest,
            driver="python-worker", trust_level="official",
            descriptor_digest=package_digest, route_contract_digest=package_digest,
            management_contract_digest=package_digest, management_ui_digest=package_digest,
            config_revision=1, config_payload_id="config/host.sample/1",
            config_payload_digest=package_digest, instance_id="instance/host.sample",
            worker_incarnation="worker/host.sample", state_schema=1,
            state_contract_digest=package_digest, capability_grants_digest=package_digest,
            data_admission=False, control_admission=True,
            storage=RuntimeStorageBinding(
                schema_id=spec.schema_id, schema_version=spec.schema_version,
                namespace=spec.namespace, baseline_sha256=spec.baseline_sha256,
                package_digest=digest, repository_contract=spec.repository_contract,
                execution_profile="declarative",
            ),
        )

        class Configuration:
            async def activate_in(self, conn, **_kwargs):
                await conn.execute("SELECT 1")

        generations = HostGenerationAuthority(first_store)
        graph = await generations.prepare_graph(lease, [binding], str(uuid4()))
        assert graph == await generations.prepare_graph(lease, [binding], graph.activation_id)
        assert await generations.current_graph() is None
        assert await generations.publish_graph(
            lease, graph, configuration_activators={"host.sample": Configuration()},
        ) == graph.graph_revision
        assert await generations.current_graph() == graph
        assert (await generations.verify_active_generation(
            component_id="host.sample", generation_id=graph.generations["host.sample"],
            admission_lane="CONTROL",
        ))["route_epoch"] == 1
        newer_binding = replace(
            binding, config_revision=2, config_payload_id="config/host.sample/2",
        )
        newer = await generations.prepare_graph(lease, [newer_binding], str(uuid4()))
        assert await generations.publish_graph(
            lease, newer, configuration_activators={"host.sample": Configuration()},
        ) == newer.graph_revision
        assert await generations.current_graph() == newer
        selection_graph, selection = await generations.active_package_selection()
        assert selection_graph == newer
        assert selection["host.sample"]["package_digest"] == package_digest
        assert selection["host.sample"]["configuration"]["revision"] == 2
        assert (await generations.verify_active_generation(
            component_id="host.sample", generation_id=newer.generations["host.sample"],
            admission_lane="CONTROL",
        ))["route_epoch"] == 2
        with pytest.raises(ForestError) as old_generation:
            await generations.verify_active_generation(
                component_id="host.sample", generation_id=graph.generations["host.sample"],
                admission_lane="CONTROL",
            )
        assert old_generation.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
        with pytest.raises(ForestError) as busy:
            await first.claim(second_incarnation)
        assert busy.value.code == "HOST_SUPERVISOR_BUSY"

        second_store = HostStore(uri, host_id)
        await second_store.open()
        try:
            second = HostIdentityAuthority(second_store)
            assert (await second.initialize())["host_id"] == host_id
            wrong_store = HostStore(uri, "host/" + uuid4().hex)
            await wrong_store.open()
            try:
                with pytest.raises(ForestError) as mismatch:
                    await HostIdentityAuthority(wrong_store).initialize()
                assert mismatch.value.code == "HOST_SCOPE_MISMATCH"
            finally:
                await wrong_store.close()
            await first.release(lease)
            successor = await second.claim(second_incarnation)
            assert successor.owner_epoch == lease.owner_epoch + 1
            with pytest.raises(ForestError) as stale_graph:
                await generations.prepare_graph(lease, [binding], str(uuid4()))
            assert stale_graph.value.code == "STALE_HOST_SUPERVISOR"
            with pytest.raises(ForestError) as old_owner:
                await HostGenerationAuthority(second_store).verify_active_generation(
                    component_id="host.sample", generation_id=newer.generations["host.sample"],
                    admission_lane="CONTROL",
                )
            assert old_owner.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
            with pytest.raises(ForestError) as stale:
                await first.renew(lease)
            assert stale.value.code == "STALE_HOST_LEASE"
            async with second_store.transaction() as conn:
                await second.verify_in(conn, successor)
                with pytest.raises(ForestError) as fenced:
                    await first.verify_in(conn, lease)
                assert fenced.value.code == "STALE_HOST_LEASE"
                with pytest.raises(psycopg.errors.LockNotAvailable):
                    async with first_store.transaction() as blocked:
                        await blocked.execute("SET LOCAL lock_timeout='100ms'")
                        await blocked.execute(
                            "UPDATE forest_host.supervisor_lease SET expires_at=expires_at "
                            "WHERE host_id=%s", (host_id,),
                        )
            await second.release(successor)
            contenders = await asyncio.gather(
                first.claim("incarnation/" + str(uuid4())),
                second.claim("incarnation/" + str(uuid4())),
                return_exceptions=True,
            )
            winners = [item for item in contenders if isinstance(item, HostLease)]
            losers = [item for item in contenders if isinstance(item, ForestError)]
            assert len(winners) == len(losers) == 1
            assert winners[0].owner_epoch == successor.owner_epoch + 1
            assert losers[0].code == "HOST_SUPERVISOR_BUSY"
            await first.release(winners[0])
        finally:
            await second_store.close()
    finally:
        await first_store.close()
        assert name.startswith("forest_host_") and len(name) == 44
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()
