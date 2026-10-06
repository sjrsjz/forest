"""Host packages use the control-scope compiler and durable Host RuntimeGraph."""

from __future__ import annotations

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
from forest.host_packages import (
    bootstrap_builtin_host_package,
    bootstrap_installed_host_package,
)
from forest.host_runtime import HostRuntimeScope
from forest.packages import PackageCatalog
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore


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
    registry = None
    try:
        identity = HostIdentityAuthority(store)
        await identity.initialize()
        lease = await identity.claim("incarnation/" + str(uuid4()))
        source = tmp_path / "source"
        _host_package(source)
        catalog = PackageCatalog(tmp_path / "catalog")
        snapshot = catalog.build_official(source, scope="host")
        runtime = HostRuntimeScope(
            store, SimpleNamespace(authority_lease=lambda **_values: lease),
        )
        with pytest.raises(ForestError) as wrong_compiler:
            await bootstrap_builtin_deployment_package(snapshot, runtime)
        assert wrong_compiler.value.code == "DEPLOYMENT_PACKAGE_SCOPE_INVALID"
        preview = await bootstrap_installed_host_package(
            snapshot, runtime, plan_only=True,
        )
        assert preview.artifact.scope == "host"
        descriptor = await bootstrap_builtin_host_package(snapshot, runtime)
        registry = RuntimeModuleRegistry(runtime, (descriptor,))
        await registry.start()
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
    finally:
        if registry is not None:
            await registry.close()
        await store.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()
