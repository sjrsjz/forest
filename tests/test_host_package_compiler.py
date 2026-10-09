"""Host packages use the control-scope compiler and durable Host RuntimeGraph."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from forest.deployment_packages import bootstrap_builtin_deployment_package
from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_packages import (
    bootstrap_builtin_host_package,
    bootstrap_installed_host_package,
)
from forest.host_runtime import HostRuntimeScope
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog, digest_json
from forest.storage.host_graph_changes import HostGraphChangeAuthority
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore
from forest.storage.host_web_authority import HostWebAuthorityRepository
from forest.web_authority_ticket import WebAuthorityTicketAuthority


def _host_package(source: Path) -> None:
    (source / "python").mkdir(parents=True)
    (source / "schemas").mkdir()
    (source / "module.toml").write_text(
        '''manifest_version = 1
package_id = "forest.host-sample"
version = "1.0.0"
kernel_api = ">=1.0,<2.0"
runtime = "python-worker"
entrypoint = "host_sample:HostSample"

[[components]]
id = "host.sample"
scope = "host"
state_schema = 1
reload_mode = "drain"
config_schema = "schemas/config.json"
bootstrap_config = "bootstrap.json"

[components.resources]
max_frame_bytes = 262144
max_inflight = 4
max_queue_items = 16
max_state_inflight = 2
max_owned_tasks = 1
max_owned_resources = 1

[components.lifecycle]
prepare_timeout_s = 10
start_timeout_s = 10
drain_timeout_s = 30
stop_timeout_s = 10

[components.capabilities]
requests = []

[[components.provides]]
name = "host.sample.echo"
contract = "1.0"
semantics = "read"
visibility = "management"
input_schema = "schemas/echo.json"
output_schema = "schemas/echo.json"

[[components.management_queries]]
id = "echo"
port = "host.sample.echo"
timeout_s = 5
''',
        encoding="utf-8",
    )
    (source / "bootstrap.json").write_text('{"value":"host"}', encoding="utf-8")
    (source / "schemas/config.json").write_text(
        '{"$schema":"https://json-schema.org/draft/2020-12/schema",'
        '"type":"object","properties":{"value":{"type":"string"}},'
        '"required":["value"],"additionalProperties":false}',
        encoding="utf-8",
    )
    (source / "schemas/echo.json").write_text(
        '{"$schema":"https://json-schema.org/draft/2020-12/schema",'
        '"type":"object","properties":{"value":{"type":"string"}},'
        '"required":["value"],"additionalProperties":false}',
        encoding="utf-8",
    )
    (source / "python/host_sample.py").write_text(
        "from forest.sdk import Plugin, service\n\n"
        "class HostSample(Plugin):\n"
        "    @service('host.sample.echo', semantics='read')\n"
        "    async def echo(self, payload, ctx):\n"
        "        return payload\n",
        encoding="utf-8",
    )


@pytest.mark.postgres
async def test_host_package_compiles_into_real_generation(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_host_" + uuid4().hex
    host_id = "host/" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = HostStore(uri, host_id)
    await store.open()
    controller = None
    change_task = None
    supervisor = None
    try:
        identity = HostIdentityAuthority(store)
        supervisor = HostSupervisor(identity)
        await supervisor.start()
        lease = supervisor.authority_lease()
        source = tmp_path / "source"
        _host_package(source)
        catalog = PackageCatalog(tmp_path / "catalog")
        snapshot = catalog.build_official(source, scope="host")
        runtime = HostRuntimeScope(
            store, supervisor,
        )
        with pytest.raises(ForestError) as wrong_compiler:
            await bootstrap_builtin_deployment_package(snapshot, runtime)
        assert wrong_compiler.value.code == "DEPLOYMENT_PACKAGE_SCOPE_INVALID"
        preview = await bootstrap_installed_host_package(
            snapshot, runtime, plan_only=True,
        )
        assert preview.artifact.scope == "host"
        descriptor = await bootstrap_builtin_host_package(snapshot, runtime)
        assert descriptor.artifact.scope == "host"
        controller = HostPackageController(
            runtime, catalog, official_sources=(source,),
        )
        registry = await controller.start()
        graph = registry.graph_identity
        assert graph == await runtime.generation_authority.current_graph()
        member = registry.get("host.sample")
        assert member.generation_identity.generation_id == graph.generations["host.sample"]
        assert (await member.management_contract.read())["desired"]["value"] == {
            "value": "host",
        }
        assert await member.management_contract.query("echo", {"value": "live"}) == {
            "value": "live",
        }
        manifest = source / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.0"', 'version = "1.0.1"', 1,
            ),
            encoding="utf-8",
        )
        replacement = catalog.build_official(source, scope="host")
        replaced = await controller.replace(
            "host.sample", replacement.digest,
            expected_graph_revision=graph.graph_revision,
        )
        assert replaced.graph_identity.graph_revision == graph.graph_revision + 1
        assert replaced.get("host.sample").snapshot.digest == replacement.digest
        assert await replaced.get("host.sample").management_contract.query(
            "echo", {"value": "reloaded"},
        ) == {"value": "reloaded"}
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.1"', 'version = "1.0.2"', 1,
            ),
            encoding="utf-8",
        )
        queued_package = catalog.build_official(source, scope="host")
        preview = await controller.preview_change(
            operation_kind="REPLACE", component_id="host.sample",
            package_digest=queued_package.digest,
            expected_graph_revision=replaced.graph_identity.graph_revision,
        )
        body = {
            "component_id": "host.sample",
            "package_digest": queued_package.digest,
            "expected_graph_revision": replaced.graph_identity.graph_revision,
            "plan_digest": preview["plan_digest"],
        }
        tickets = WebAuthorityTicketAuthority(
            issuer_id="deployment/test", active_key_id="key/host-test",
            key_resolver=lambda _key_id: bytes(range(32)),
        )
        session = SimpleNamespace(
            subject=SimpleNamespace(subject_id="web/admin", role="ADMIN"),
            session_ref="session/host-controller", session_revision=1,
        )
        ticket = await tickets.issue(
            session=session, target_scope_kind="host", target_scope_id=host_id,
            target_component_id="$runtime",
            target_generation_id=replaced.graph_identity.graph_id,
            target_graph_revision=replaced.graph_identity.graph_revision,
            target_operation_ref="replace/host.sample/3", action_key="module.replace",
            input_digest=digest_json(body), plan_digest=body["plan_digest"],
        )
        receipts = HostWebAuthorityRepository(store, ticket_authority=tickets)
        changes = HostGraphChangeAuthority(store, ticket_authority=tickets)

        async def authorize_in(conn):
            return await receipts.accept_in(conn, ticket=ticket)

        mismatched_plan = await tickets.issue(
            session=session, target_scope_kind="host", target_scope_id=host_id,
            target_component_id="$runtime",
            target_generation_id=replaced.graph_identity.graph_id,
            target_graph_revision=replaced.graph_identity.graph_revision,
            target_operation_ref="replace/host.sample/3", action_key="module.replace",
            input_digest=digest_json(body), plan_digest=digest_json({"wrong": True}),
        )
        with pytest.raises(ForestError) as invalid_plan:
            await changes.enqueue_web(
                lease=lease, ticket=mismatched_plan, body=body, authorize_in=authorize_in,
            )
        assert invalid_plan.value.code == "HOST_GRAPH_CHANGE_INPUT_INVALID"
        request = await changes.enqueue_web(
            lease=lease, ticket=ticket, body=body, authorize_in=authorize_in,
        )
        assert request["state"] == "QUEUED"
        change_task = asyncio.create_task(controller.serve_graph_changes(changes))
        async with asyncio.timeout(15):
            while (await changes.get(ticket.target_operation_ref))["state"] == "QUEUED":
                await asyncio.sleep(0.05)
            while (await changes.get(ticket.target_operation_ref))["state"] == "RUNNING":
                await asyncio.sleep(0.05)
        settled = await changes.get(ticket.target_operation_ref)
        assert settled["state"] == "SUCCEEDED"
        assert controller.registry.graph_identity.graph_revision == graph.graph_revision + 2
        assert controller.registry.get("host.sample").snapshot.digest == queued_package.digest

        # The installed artifact has no official provenance, yet its read
        # service must survive a real Generation replacement.
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.2"', 'version = "1.0.3"', 1,
            ),
            encoding="utf-8",
        )
        ordinary = catalog.build(source)
        assert not catalog.is_official_host(ordinary.digest)
        previous = controller.registry
        ordinary_registry = await controller.replace(
            "host.sample", ordinary.digest,
            expected_graph_revision=previous.graph_identity.graph_revision,
        )
        assert ordinary_registry.graph_identity.graph_revision == (
            previous.graph_identity.graph_revision + 1
        )
        assert ordinary_registry.get("host.sample").generation_identity != (
            previous.get("host.sample").generation_identity
        )
        assert await ordinary_registry.get("host.sample").management_contract.query(
            "echo", {"value": "ordinary-reloaded"},
        ) == {"value": "ordinary-reloaded"}
    finally:
        if change_task is not None:
            change_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await change_task
        if controller is not None:
            await controller.close()
        if supervisor is not None:
            await supervisor.close()
        await store.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()
