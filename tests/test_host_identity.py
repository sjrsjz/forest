"""Host scope identity survives process restarts and fences stale incarnations."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import replace
from importlib.resources import files
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.errors import ForestError
from forest.host_kernel_management import HostKernelManagementAuthority, HostKernelQueryAuthority
from forest.host_runtime import HostRuntimeScope
from forest.package_capabilities import PackageCallScope, PackageServiceRoute
from forest.package_domain_authority import PostgresPackageDomainAuthority
from forest.packages import digest_json, storage_bundle_digest
from forest.runtime_generation import PreparedGenerationBinding, RuntimeStorageBinding
from forest.schema_bundles import StorageBundleSpec
from forest.storage.host_generation import HostGenerationAuthority
from forest.storage.host_graph_changes import HostGraphChangeAuthority
from forest.storage.host_identity import HostIdentityAuthority, HostLease
from forest.storage.host_module_configuration import HostModuleConfigurationRepository
from forest.storage.host_schema_bundles import HostSchemaBundleInstaller
from forest.storage.host_store import HostStore
from forest.storage.host_web_authority import HostWebAuthorityRepository
from forest.web_authority_ticket import WebAuthorityTicketAuthority


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
        config_sql = files("forest.storage.official").joinpath(
            "control_module_configuration.sql"
        ).read_bytes() + b"""
CREATE FUNCTION host_sample_probe(call_context jsonb, request jsonb) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object(
        'protocol', 'forest.domain-result/1', 'ok', true,
        'value', jsonb_build_object('host_id', call_context->>'scope_id',
                                    'value', request->>'value')
    )
$$;
CREATE TABLE host_sample_native (value text NOT NULL);
"""
        config_spec = StorageBundleSpec(
            component_id="host.sample", schema_id="host.sample.configuration",
            schema_version="1.0.0", baseline_sha256=hashlib.sha256(config_sql).hexdigest(),
            repository_contract="forest.control-configuration.repository/1", scope="host",
        )
        config_storage_digest = storage_bundle_digest(config_spec, "official")
        await installer.install(
            config_spec, config_sql, config_storage_digest, execution_profile="official",
        )
        configuration = HostModuleConfigurationRepository(
            first_store, component_id="host.sample", namespace=config_spec.namespace,
        )
        validation_digest = digest_json({"protocol": "host.sample-configuration/1"})
        first_config = await configuration.ensure_initialized(
            operation_id="bootstrap/host.sample", value={"value": "first"},
            validation_digest=validation_digest,
        )
        assert (await configuration.desired_checked()).value == {"value": "first"}
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
            config_revision=first_config.revision, config_payload_id=first_config.payload_id,
            config_payload_digest=first_config.payload_digest,
            instance_id="instance/host.sample",
            worker_incarnation="worker/host.sample", state_schema=1,
            state_contract_digest=package_digest, capability_grants_digest=package_digest,
            data_admission=True, control_admission=True,
            storage=RuntimeStorageBinding(
                schema_id=config_spec.schema_id, schema_version=config_spec.schema_version,
                namespace=config_spec.namespace, baseline_sha256=config_spec.baseline_sha256,
                package_digest=config_storage_digest,
                repository_contract=config_spec.repository_contract,
                execution_profile="official",
            ),
        )

        runtime = HostRuntimeScope(
            first_store, SimpleNamespace(authority_lease=lambda **_values: lease),
        )
        assert runtime.authority_lease() == lease
        generations = runtime.generation_authority
        graph = await generations.prepare_graph(lease, [binding], str(uuid4()))
        assert graph == await generations.prepare_graph(lease, [binding], graph.activation_id)
        assert await generations.current_graph() is None
        assert await generations.publish_graph(
            lease, graph, configuration_activators={"host.sample": configuration},
        ) == graph.graph_revision
        assert await configuration.effective() == first_config
        assert await generations.current_graph() == graph
        assert (await generations.verify_active_generation(
            component_id="host.sample", generation_id=graph.generations["host.sample"],
            admission_lane="CONTROL",
        ))["route_epoch"] == 1
        ledger = runtime.service_operation_ledger
        route = PackageServiceRoute(
            service_id="owned-work.host.sample.probe", graph_id=graph.graph_id,
            graph_revision=graph.graph_revision, admission_lane="DATA", owned_work=True,
            parent_call_id=None, timeout_seconds=10,
        )
        call = PackageCallScope(
            bot_id=None, scope_kind="host", scope_id=host_id,
            component_id="host.sample", generation_id=graph.generations["host.sample"],
            instance_id="instance/host.sample", worker_incarnation="worker/host.sample",
            call_id="call:host-ledger", call_operation_id="owned:host-ledger",
            causal_root_kind="package.owned-work", causal_root_id="owned:host-ledger",
        )
        domain = PostgresPackageDomainAuthority(
            first_store, generations, component_id="host.sample",
            runtime_package_digest=package_digest, storage_spec=config_spec,
            storage_package_digest=config_storage_digest,
            storage_execution_profile="official",
        )
        assert await domain.invoke_query(
            call, {"name": "host.sample.probe", "function": "host_sample_probe"},
            {"value": "visible"}, parent_call_id="call:host-ledger",
            timeout_seconds=5,
        ) == {"host_id": host_id, "value": "visible"}
        class NativeRead:
            async def query(self, payload):
                return {"host_id": host_id, "value": payload["value"]}

        query_decl = {"name": "host.sample.native-read", "kind": "native-read"}
        query_authority = HostKernelQueryAuthority(
            first_store, generations, component_id="host.sample",
            ports=(query_decl,), handlers={("host.sample", "native-read"): NativeRead()},
        )
        native_scope = SimpleNamespace(
            scope_kind="host", scope_id=host_id, component_id="host.sample",
            bot_id=None, generation_id=graph.generations["host.sample"],
            call_operation_id="native/host.sample/1",
        )
        assert await query_authority.invoke_query(
            native_scope, query_decl, {"value": "visible"},
            parent_call_id="call:native-read", timeout_seconds=5,
        ) == {"host_id": host_id, "value": "visible"}
        with pytest.raises(ForestError) as wrong_native_scope:
            await query_authority.invoke_query(
                SimpleNamespace(**{**vars(native_scope), "scope_id": "host/" + uuid4().hex}),
                query_decl, {}, parent_call_id="call:wrong-scope", timeout_seconds=5,
            )
        assert wrong_native_scope.value.code == "KERNEL_MANAGEMENT_BINDING_INVALID"

        class NativeWrite:
            async def prepare(self, payload, *, operation_id):
                return payload["value"]

            async def execute_in(self, conn, prepared, *, operation_id):
                await conn.execute(
                    sql.SQL("INSERT INTO {}.host_sample_native(value) VALUES (%s)").format(
                        sql.Identifier(config_spec.namespace)
                    ), (prepared,),
                )
                return {"value": prepared}

        command_decl = {"name": "host.sample.native-write", "kind": "native-write"}
        command_authority = HostKernelManagementAuthority(
            first_store, generations, component_id="host.sample",
            ports=(command_decl,), handlers={("host.sample", "native-write"): NativeWrite()},
        )

        async def before_native_write(_conn):
            return None

        native_args = {
            "parent_call_id": "call:native-write", "timeout_seconds": 5,
            "before_write": before_native_write, "management_authorization": object(),
        }
        with pytest.raises(ForestError) as rejected_native:
            await command_authority.invoke_management_action(
                native_scope, command_decl, {"value": "rolled-back"},
                result_validator=lambda _result: False, **native_args,
            )
        assert rejected_native.value.code == "MODULE_MANAGEMENT_OUTPUT_INVALID"
        assert await command_authority.invoke_management_action(
            native_scope, command_decl, {"value": "committed"},
            result_validator=lambda result: result == {"value": "committed"},
            **native_args,
        ) == {"value": "committed"}
        async with first_store.pool.connection() as conn:
            rows = await conn.execute(
                sql.SQL("SELECT value FROM {}.host_sample_native").format(
                    sql.Identifier(config_spec.namespace)
                )
            )
            assert [row["value"] for row in await rows.fetchall()] == ["committed"]
        with pytest.raises(ForestError) as foreign_scope:
            await domain.invoke_query(
                replace(call, scope_id="host/" + uuid4().hex),
                {"name": "host.sample.probe", "function": "host_sample_probe"},
                {}, parent_call_id="call:host-ledger", timeout_seconds=5,
            )
        assert foreign_scope.value.code == "DOMAIN_STORAGE_FENCE_MISMATCH"
        assert (await ledger.begin(call, route, {"value": 1})).execute
        competing = replace(call, call_id="call:host-ledger-competing")
        in_progress = await ledger.begin(competing, route, {"value": 1})
        assert in_progress.error.code == "SERVICE_OPERATION_IN_PROGRESS"
        await ledger.succeed(call, route, {"value": "done"})
        assert (await ledger.begin(competing, route, {"value": 1})).result == {
            "value": "done",
        }
        with pytest.raises(ForestError) as conflict:
            await ledger.begin(competing, route, {"value": 2})
        assert conflict.value.code == "SERVICE_OPERATION_CONFLICT"
        tickets = WebAuthorityTicketAuthority(
            issuer_id="deployment/test", active_key_id="key/host-test",
            key_resolver=lambda _key_id: bytes(range(32)),
        )
        receipts = HostWebAuthorityRepository(first_store, ticket_authority=tickets)
        session = SimpleNamespace(
            subject=SimpleNamespace(subject_id="web/admin", role="ADMIN"),
            session_ref="session/host-test", session_revision=1,
        )

        async def issue(action_key, operation_id, input_digest, plan_digest=None):
            return await tickets.issue(
                session=session, target_scope_kind="host", target_scope_id=host_id,
                target_component_id="host.sample",
                target_generation_id=graph.generations["host.sample"],
                target_graph_revision=graph.graph_revision,
                target_operation_ref=operation_id, action_key=action_key,
                input_digest=input_digest, plan_digest=plan_digest,
            )

        async def authorize_in(conn, *, ticket, component_id, operation_id, action_key,
                               input_digest, plan_digest=None):
            await tickets.verify(
                ticket, target_scope_kind="host", target_scope_id=host_id,
                target_component_id=component_id,
                target_generation_id=graph.generations["host.sample"],
                target_graph_revision=graph.graph_revision,
                target_operation_ref=operation_id, action_key=action_key,
                input_digest=input_digest, plan_digest=plan_digest,
                required_role="ADMIN",
            )
            return await receipts.accept_in(conn, ticket=ticket)

        plan_body = {"expected_desired_revision": 1, "value": {"value": "second"}}
        plan_ticket = await issue(
            "configuration.plan", "plan/host.sample/2", digest_json(plan_body),
        )
        plan = await configuration.plan(
            operation_id=plan_ticket.target_operation_ref, expected_desired_revision=1,
            value=plan_body["value"], validation_digest=validation_digest,
            authorization=plan_ticket, authorization_guard=authorize_in,
        )
        apply_body = {"plan_id": plan["plan_id"], "expected_desired_revision": 1}
        apply_ticket = await issue(
            "configuration.apply", "apply/host.sample/2", digest_json(apply_body),
            digest_json({"plan_id": plan["plan_id"]}),
        )

        async def authorize_apply(conn):
            return await authorize_in(
                conn, ticket=apply_ticket, component_id="host.sample",
                operation_id=apply_ticket.target_operation_ref,
                action_key="configuration.apply", input_digest=apply_ticket.input_digest,
                plan_digest=apply_ticket.plan_digest,
            )

        changes = HostGraphChangeAuthority(first_store, ticket_authority=tickets)
        with pytest.raises(ForestError) as forged:
            await changes.enqueue_configuration(
                lease=lease,
                ticket=replace(apply_ticket, signature="hmac-sha256:" + "0" * 64),
                configuration=configuration, plan_id=plan["plan_id"],
                expected_desired_revision=1, validation_digest=validation_digest,
                authorize_in=authorize_apply,
            )
        assert forged.value.code == "WEB_AUTHORITY_TICKET_SIGNATURE_INVALID"
        assert (await configuration.desired()).revision == 1
        configuration_runtime = HostRuntimeScope(
            first_store, SimpleNamespace(authority_lease=lambda **_values: lease),
            graph_change_authority=changes,
        )
        committed = await configuration_runtime.apply_configuration(
            configuration, operation_id=apply_ticket.target_operation_ref,
            plan_id=plan["plan_id"], expected_desired_revision=1,
            authorization=apply_ticket, authorization_guard=authorize_in,
            validation_digest=validation_digest,
        )
        second_config = committed["configuration"]
        change_request = await changes.claim(lease)
        assert change_request["operation_id"] == apply_ticket.target_operation_ref
        assert committed["rebuild"] == {
            "request_id": apply_ticket.target_operation_ref, "state": "QUEUED",
        }
        assert change_request["state"] == "RUNNING"
        assert (await configuration.effective()).revision == 1
        newer_binding = replace(
            binding, config_revision=second_config.revision,
            config_payload_id=second_config.payload_id,
            config_payload_digest=second_config.payload_digest,
        )
        newer = await generations.prepare_graph(
            lease, [newer_binding], change_request["activation_id"],
        )
        assert await generations.publish_graph(
            lease, newer, configuration_activators={"host.sample": configuration},
        ) == newer.graph_revision
        proof = await changes.published_graph(change_request)
        assert proof["graph_id"] == newer.graph_id
        assert (await changes.settle(
            lease, operation_id=change_request["operation_id"], graph=proof,
        ))["state"] == "SUCCEEDED"
        replace_body = {
            "component_id": "host.sample", "package_digest": package_digest,
            "expected_graph_revision": newer.graph_revision,
            "plan_digest": digest_json({"replace": "host.sample"}),
        }
        replace_ticket = await tickets.issue(
            session=session, target_scope_kind="host", target_scope_id=host_id,
            target_component_id="$runtime", target_generation_id=newer.graph_id,
            target_graph_revision=newer.graph_revision,
            target_operation_ref="replace/host.sample/probe", action_key="module.replace",
            input_digest=digest_json(replace_body),
            plan_digest=replace_body["plan_digest"],
        )

        async def authorize_replace(conn):
            await tickets.verify(
                replace_ticket, target_scope_kind="host", target_scope_id=host_id,
                target_component_id="$runtime", target_generation_id=newer.graph_id,
                target_graph_revision=newer.graph_revision,
                target_operation_ref=replace_ticket.target_operation_ref,
                action_key="module.replace", input_digest=replace_ticket.input_digest,
                plan_digest=replace_body["plan_digest"],
                required_role="ADMIN",
            )
            return await receipts.accept_in(conn, ticket=replace_ticket)

        queued = await changes.enqueue_web(
            lease=lease, ticket=replace_ticket, body=replace_body,
            authorize_in=authorize_replace,
        )
        assert queued["state"] == "QUEUED"
        replacement_claim = await changes.claim(lease)
        assert replacement_claim["operation_id"] == queued["operation_id"]
        assert (await changes.settle(
            lease, operation_id=queued["operation_id"], error_code="TEST_PROBE_REJECTED",
        ))["state"] == "FAILED"
        assert (await configuration.effective(include_value=True)).value == {"value": "second"}
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
        with pytest.raises(ForestError) as stale_operation:
            await ledger.begin(
                replace(call, call_id="call:host-ledger-stale",
                        call_operation_id="owned:host-ledger-stale",
                        causal_root_id="owned:host-ledger-stale"),
                route, {},
            )
        assert stale_operation.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
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
