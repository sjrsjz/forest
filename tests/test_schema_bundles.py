from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from shutil import copytree, ignore_patterns
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.types.json import Jsonb

from forest.bot_host import BotGraphCandidate, BotHost
from forest.builtin_packages import (
    _configuration_storage,
    bootstrap_builtin_package,
    bootstrap_installed_package,
    compile_installed_candidate_descriptor,
    inspect_installed_package,
)
from forest.component_graph_preview import preview_selected_graph
from forest.component_selection import (
    ComponentSelectionEntry,
    ComponentSelectionReader,
    ComponentSelectionSnapshot,
)
from forest.component_selection_commit import resolve_selection_commit_preflight
from forest.core_runtime import (
    bootstrap_kernel,
    runtime_graph_replacement_request_service,
)
from forest.errors import ForestError
from forest.kernel_payload_effect_driver import kernel_payload_json_effect_driver_factory
from forest.kernel_payload_participant import KernelPayloadUnitOfWorkParticipant
from forest.package_capabilities import PackageCallScope, PackageServiceRoute
from forest.package_effects import PackageEffectDriverRegistry
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RoutedServiceEndpoint,
    RuntimeContractRouter,
    RuntimeKernelServiceContext,
    RuntimeServiceInvocation,
)
from forest.runtime_generation import (
    PreparedGenerationBinding,
    RuntimeStorageBinding,
)
from forest.runtime_modules import (
    ModuleLifecycleState,
    OwnedWorkLifecycle,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
    official_artifact,
    official_storage_bundle,
)
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)
from forest.storage.authorization_authority import (
    CapabilityGrant,
    KernelAuthorizationRepository,
)
from forest.storage.bot_store import BotStore
from forest.storage.component_state import ComponentStateCommands, ComponentStateQueries
from forest.storage.effect_authority import KernelEffectRepository
from forest.storage.kernel_audit import AuditEnvelope, KernelAuditRepository
from forest.storage.kernel_runtime import KernelRuntimeRepository
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.payload_authority import (
    KernelPayloadRepository,
    PayloadSource,
    SourceTarget,
)
from forest.storage.schema_bundles import SchemaBundleInstaller
from forest.storage.service_operations import KernelServiceOperationLedger
from forest.web_authority_ticket import WebAuthorityTicketAuthority


def bundle(
    baseline: bytes,
    *,
    component_id: str = "example.records",
    schema_id: str = "example.records",
) -> StorageBundleSpec:
    return StorageBundleSpec(
        component_id=component_id,
        schema_id=schema_id,
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract="example.records/1",
    )


@pytest.mark.parametrize(
    "sql_source",
    [
        b"CREATE/**/SCHEMA escaped;",
        b"CREATE TABLE records(id int); INSERT INTO records VALUES (1);",
        b"CREATE TABLE records AS SELECT 1 AS id;",
        b"CREATE TABLE forest_kernel.stolen(id int);",
        b"DO $body$ BEGIN NULL; END $body$;",
        b'CREATE TABLE "quoted"(id int);',
    ],
)
def test_storage_baseline_rejects_escape_forms(sql_source):
    with pytest.raises(ForestError) as caught:
        validate_storage_baseline(sql_source, hashlib.sha256(sql_source).hexdigest())
    assert caught.value.code == "INVALID_STORAGE_BUNDLE"


def test_storage_baseline_accepts_declarative_owned_objects():
    source = (
        b"CREATE TYPE record_state AS ENUM ('READY', 'DONE');\n"
        b"CREATE TABLE records(record_id text PRIMARY KEY, state record_state NOT NULL);\n"
        b"CREATE INDEX records_state_idx ON records(state);\n"
        b"COMMENT ON TABLE records IS 'module-owned';\n"
    )
    assert validate_storage_baseline(source, hashlib.sha256(source).hexdigest()) == source.decode()


def test_only_official_profile_accepts_invariant_functions_and_triggers():
    source = (
        b"CREATE TABLE records(record_id text PRIMARY KEY);\n"
        b"CREATE FUNCTION reject_change() RETURNS trigger LANGUAGE plpgsql AS $$\n"
        b"BEGIN RAISE EXCEPTION 'immutable'; END $$;\n"
        b"CREATE TRIGGER records_immutable BEFORE UPDATE ON records "
        b"FOR EACH ROW EXECUTE FUNCTION reject_change();\n"
    )
    digest = hashlib.sha256(source).hexdigest()
    with pytest.raises(ForestError) as caught:
        validate_storage_baseline(source, digest)
    assert caught.value.code == "INVALID_STORAGE_BUNDLE"
    assert validate_official_storage_baseline(source, digest) == source.decode()


@pytest_asyncio.fixture
async def clean_authority():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_bundle_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-bundle")
    await store.open()
    installer = SchemaBundleInstaller(store)
    try:
        yield store, installer
    finally:
        await store.close()
        assert name.startswith("forest_bundle_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def _isolated_runtime_descriptors(*descriptors):
    """Omit the absent Worker Host edge in focused lower-layer fixtures."""

    return tuple(
        replace(descriptor, retention_requires=tuple(
            component_id for component_id in descriptor.retention_requires
            if component_id != "component.worker-host"
        ))
        for descriptor in descriptors
    )


async def test_kernel_replacement_service_preserves_frozen_selection_credentials():
    calls = []

    class Runtime:
        async def request_graph_replacement(self, lease, **values):
            calls.append((lease, values))
            return {**values, "activation_id": "activation/one", "state": "QUEUED"}

    context = SimpleNamespace(
        participant=lambda _name: SimpleNamespace(service=Runtime()),
        host_lease="lease/one",
    )
    request = runtime_graph_replacement_request_service(context)
    payload = {
        "protocol": "forest.runtime-graph-replacement-request/3",
        "operation_id": "selection/one",
        "expected_graph_revision": 7,
        "requested_by": "web:admin",
        "target_component_id": "example.counter",
        "selection_revision": 2,
        "selection_digest": "sha256:" + "a" * 64,
    }
    receipt = await request(payload)
    assert receipt["protocol"] == "forest.runtime-graph-replacement-receipt/3"
    assert receipt["selection_revision"] == 2
    assert receipt["selection_digest"] == payload["selection_digest"]
    assert calls == [
        ("lease/one", {key: value for key, value in payload.items() if key != "protocol"})
    ]
    with pytest.raises(ForestError) as malformed:
        await request({**payload, "selection_digest": "sha256:bad"})
    assert malformed.value.code == "RUNTIME_REPLACEMENT_INPUT_INVALID"
    assert len(calls) == 1


@pytest.mark.postgres
async def test_worker_host_selection_revision_cas_and_operation_replay(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    snapshot = PackageCatalog(tmp_path / "selection-packages").build(
        Path(__file__).parents[1] / "src/forest/builtin/component_worker_host"
    )

    storage = _configuration_storage(snapshot)
    await installer.install(
        storage.spec, storage.baseline, storage.package_digest,
        execution_profile="official",
    )

    def command_payload(function, payload):
        return {
            **payload,
            "desired_enabled": function == "enable",
            "plan_digest": "sha256:" + "0" * 64,
            "expected_configuration_revision": (
                1 if payload["source_kind"] == "installed" and function == "enable"
                else None
            ),
        }

    async def call(
        function, operation_id, payload, *, port_kind="management_action",
        desired_enabled=None,
    ):
        payload = command_payload(function, payload)
        if desired_enabled is not None:
            payload["desired_enabled"] = desired_enabled
        name = "component.selection." + function
        context = {
            "bot_id": store.bot_id, "port_kind": port_kind, "port_name": name,
            "operation_id": operation_id, "input_digest": digest_json(payload),
        }
        async with store.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS value").format(
                    sql.Identifier(storage.spec.namespace, "component_selection_" + function)
                ),
                (Jsonb(context), Jsonb(payload)),
            )
            return (await result.fetchone())["value"]

    source = {
        "component_id": "example.optional", "source_kind": "builtin",
        "package_digest": None, "manifest_digest": None,
        "approved_capabilities": [], "expected_selection_revision": 0,
        "expected_graph_revision": 1, "requested_by": "subject/admin",
    }
    bypass = await call("enable", "selection/bypass", source, port_kind="command")
    assert bypass["error"]["code"] == "COMPONENT_SELECTION_INVALID"
    misbound = await call(
        "enable", "selection/misbound", source, desired_enabled=False
    )
    assert misbound["error"]["code"] == "COMPONENT_SELECTION_INVALID"
    first, rival = await asyncio.gather(
        call("enable", "selection/first", source),
        call("disable", "selection/rival", source),
    )
    assert sorted((first["ok"], rival["ok"])) == [False, True]
    winner = first if first["ok"] else rival
    loser = rival if first["ok"] else first
    assert winner["value"]["selection_revision"] == 1
    winning_operation = "selection/first" if first["ok"] else "selection/rival"
    assert winner["value"]["request_id"] == "selection:" + hashlib.md5(
        (store.bot_id + "\x1f" + winning_operation).encode(), usedforsecurity=False
    ).hexdigest()
    assert loser["error"]["code"] == "COMPONENT_SELECTION_REVISION_CONFLICT"
    replay = await call(
        "enable" if first["ok"] else "disable",
        "selection/first" if first["ok"] else "selection/rival",
        source,
    )
    assert replay == winner
    changed = {**source, "expected_selection_revision": 1}
    conflict = await call(
        "enable" if first["ok"] else "disable",
        "selection/first" if first["ok"] else "selection/rival",
        changed,
    )
    assert conflict["error"]["code"] == "COMPONENT_SELECTION_OPERATION_CONFLICT"
    async with store.pool.connection() as conn:
        result = await conn.execute(
            sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS value").format(
                sql.Identifier(storage.spec.namespace, "component_selection_snapshot")
            ),
            (Jsonb({"bot_id": store.bot_id}), Jsonb({})),
        )
        frozen = (await result.fetchone())["value"]["value"]
    assert frozen["revision"] == 1
    assert frozen["entries"] == [{
        "component_id": "example.optional", "source_kind": "builtin",
        "package_digest": None, "manifest_digest": None,
        "approved_capabilities": [], "desired_enabled": winner["value"]["desired_enabled"],
        "desired_revision": 1,
    }]
    reader = ComponentSelectionReader(store, snapshot)
    current = await reader.snapshot()
    assert current.revision == frozen["revision"]
    assert [entry.as_dict() for entry in current.entries] == frozen["entries"]
    winning_function = "enable" if first["ok"] else "disable"
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("forest.component.selection/" + store.bot_id,),
        )
        assert await reader.operation_replay_in(
            conn, operation_id="selection/missing", input_digest=digest_json(source),
            desired_enabled=True,
        ) is None
        assert await reader.operation_replay_in(
            conn, operation_id=winning_operation,
            input_digest=digest_json(command_payload(winning_function, source)),
            desired_enabled=winner["value"]["desired_enabled"],
        ) == winner["value"]
        with pytest.raises(ForestError) as replay_conflict:
            await reader.operation_replay_in(
                conn, operation_id=winning_operation,
                input_digest=digest_json(command_payload(winning_function, changed)),
                desired_enabled=winner["value"]["desired_enabled"],
            )
        assert replay_conflict.value.code == "COMPONENT_SELECTION_OPERATION_CONFLICT"
    next_action = "disable" if winner["value"]["desired_enabled"] else "enable"
    async with reader.publication_fence(
        expected_revision=current.revision, expected_digest=current.digest
    ):
        async with store.pool.connection() as conn, conn.transaction():
            projected = await reader.project_in(
                conn,
                component_id=source["component_id"],
                source_kind=source["source_kind"],
                package_digest=None,
                manifest_digest=None,
                approved_capabilities=(),
                desired_enabled=next_action == "enable",
                expected_selection_revision=current.revision,
            )
        pending = asyncio.create_task(
            call(next_action, "selection/next", changed)
        )
        await asyncio.sleep(0.05)
        assert not pending.done()
    next_result = await asyncio.wait_for(pending, timeout=5)
    assert next_result["value"]["selection_revision"] == 2
    assert projected == await reader.snapshot()
    replay_host = SimpleNamespace(
        _replacement_lock=asyncio.Lock(), registry=object(), _closing=False,
        selection_reader=reader,
        _official_descriptors=(SimpleNamespace(
            component_id="component.worker-host",
            artifact=SimpleNamespace(package_digest=snapshot.digest),
        ),),
    )
    preflight = resolve_selection_commit_preflight(
        replay_host, "runtime.selection-commit",
        component_id="component.worker-host", package_digest=snapshot.digest,
    )
    assert preflight is not None
    async with preflight(
        command_payload(winning_function, source), operation_id=winning_operation
    ) as verify_in:
        async with store.pool.connection() as conn, conn.transaction():
            await verify_in(conn)
    async with preflight(
        command_payload(winning_function, changed), operation_id=winning_operation
    ) as verify_in:
        async with store.pool.connection() as conn, conn.transaction():
            with pytest.raises(ForestError) as changed_replay:
                await verify_in(conn)
    assert changed_replay.value.code == "COMPONENT_SELECTION_OPERATION_CONFLICT"
    with pytest.raises(psycopg.Error) as immutable_operation:
        async with store.pool.connection() as conn:
            await conn.execute(
                sql.SQL("UPDATE {} SET snapshot_payload='{{}}'::jsonb WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace, "selection_operations")
                ),
                (store.bot_id,),
            )
    assert immutable_operation.value.sqlstate == "55000"

    async def work_call(function, payload):
        async with store.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS value").format(
                    sql.Identifier(
                        storage.spec.namespace,
                        "component_selection_application_" + function,
                    )
                ),
                (Jsonb({"bot_id": store.bot_id}), Jsonb(payload)),
            )
            return (await result.fetchone())["value"]

    first_claim = (await work_call(
        "claim", {"claim_token": str(uuid4()), "lease_seconds": 45}
    ))["value"]
    assert first_claim["payload"]["snapshot"] == frozen
    assert first_claim["payload"]["selection_revision"] == 1
    settled = await work_call("settle", {
        "work_id": first_claim["work_id"],
        "claim_token": first_claim["claim_token"],
        "outcome": {"status": "SUCCEEDED", "result": {
            "disposition": "SUPERSEDED", "receipt": None,
            "error_code": "COMPONENT_SELECTION_CHANGED",
        }},
    })
    assert settled["ok"]
    assert (await work_call("read", {"request_id": first_claim["work_id"]}))[
        "value"
    ]["state"] == "SUPERSEDED"
    second_claim = (await work_call(
        "claim", {"claim_token": str(uuid4()), "lease_seconds": 45}
    ))["value"]
    assert second_claim["payload"]["selection_revision"] == 2
    assert second_claim["payload"]["snapshot"]["revision"] == 2
    assert second_claim["payload"]["snapshot"] != frozen
    assert (await work_call("claim", {
        "claim_token": str(uuid4()), "lease_seconds": 45,
    }))["value"] is None
    with pytest.raises(ForestError) as stale:
        async with reader.publication_fence(
            expected_revision=current.revision, expected_digest=current.digest
        ):
            pass
    assert stale.value.code == "COMPONENT_SELECTION_CHANGED"

    installed = {
        "component_id": "example.installed", "source_kind": "installed",
        "package_digest": "sha256:" + "a" * 64,
        "manifest_digest": "sha256:" + "b" * 64,
        "approved_capabilities": ["state.read"],
        "expected_selection_revision": 2, "expected_graph_revision": 1,
        "requested_by": "subject/admin",
    }
    async with reader.publication_fence(expected_revision=2):
        async with store.pool.connection() as conn, conn.transaction():
            projected_installed = await reader.project_in(
                conn, component_id=installed["component_id"],
                source_kind="installed",
                package_digest=installed["package_digest"],
                manifest_digest=installed["manifest_digest"],
                approved_capabilities=("state.read",), desired_enabled=True,
                expected_selection_revision=2,
            )
    assert (await call("enable", "selection/installed", installed))["ok"]
    assert projected_installed == await reader.snapshot()

    official = {
        "component_id": "routing.graph", "source_kind": "official",
        "package_digest": "sha256:" + "c" * 64,
        "manifest_digest": "sha256:" + "d" * 64,
        "approved_capabilities": [],
        "expected_selection_revision": 3, "expected_graph_revision": 1,
        "requested_by": "subject/admin",
    }
    async with reader.publication_fence(expected_revision=3):
        async with store.pool.connection() as conn, conn.transaction():
            projected_official = await reader.project_in(
                conn, component_id=official["component_id"],
                source_kind="official",
                package_digest=official["package_digest"],
                manifest_digest=official["manifest_digest"],
                approved_capabilities=(), desired_enabled=True,
                expected_selection_revision=3,
            )
    assert (await call("enable", "selection/official", official))["ok"]
    assert projected_official == await reader.snapshot()
    bad_grants = {**official, "approved_capabilities": ["state.read"],
                  "expected_selection_revision": 4}
    assert (await call("enable", "selection/official-grants", bad_grants))[
        "error"
    ]["code"] == "COMPONENT_SELECTION_INVALID"
    bad_self = {**official, "component_id": "component.worker-host",
                "expected_selection_revision": 4}
    assert (await call("enable", "selection/official-self", bad_self))[
        "error"
    ]["code"] == "COMPONENT_SELECTION_INVALID"


@pytest.mark.postgres
async def test_kernel_baseline_is_minimal_and_idempotent(clean_authority):
    store, installer = clean_authority
    first = await installer.initialize_kernel()
    second = await installer.initialize_kernel()
    assert first["bot_id"] == second["bot_id"] == store.bot_id
    async with store.pool.connection() as conn:
        relations = await (
            await conn.execute(
                "SELECT n.nspname,c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname IN ('public','forest_kernel') AND c.relkind IN ('r','p') "
                "ORDER BY n.nspname,c.relname"
            )
        ).fetchall()
    assert [(row["nspname"], row["relname"]) for row in relations] == [
        ("forest_kernel", "bot_identity"),
        ("forest_kernel", "schema_bundles"),
    ]


@pytest.mark.postgres
async def test_bundle_install_is_exact_idempotent_and_concurrent(clean_authority):
    _store, installer = clean_authority
    await installer.initialize_kernel()
    baseline = b"CREATE TABLE records(record_id text PRIMARY KEY);"
    spec = bundle(baseline)
    package_digest = "sha256:" + "1" * 64
    left, right = await asyncio.gather(
        installer.install(spec, baseline, package_digest),
        installer.install(spec, baseline, package_digest),
    )
    assert left["namespace"] == right["namespace"] == spec.namespace
    assert left["state"] == right["state"] == "READY"
    with pytest.raises(ForestError) as caught:
        await installer.install(spec, baseline, "sha256:" + "2" * 64)
    assert caught.value.code == "SCHEMA_BUNDLE_MISMATCH"


@pytest.mark.postgres
async def test_official_bundle_profile_is_persisted_and_part_of_identity(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    baseline = b"CREATE TABLE records(record_id text PRIMARY KEY);"
    spec = bundle(baseline, schema_id="example.official")
    package_digest = "sha256:" + "5" * 64
    row = await installer.install(spec, baseline, package_digest, execution_profile="official")
    assert row["execution_profile"] == "official"
    with pytest.raises(ForestError) as caught:
        await installer.install(spec, baseline, package_digest)
    assert caught.value.code == "SCHEMA_BUNDLE_MISMATCH"

    invariant_baseline = (
        b"CREATE TABLE records(record_id text PRIMARY KEY);\n"
        b"CREATE FUNCTION reject_change() RETURNS trigger LANGUAGE plpgsql AS $$\n"
        b"BEGIN RAISE EXCEPTION 'immutable'; END $$;\n"
        b"CREATE TRIGGER records_immutable BEFORE UPDATE ON records "
        b"FOR EACH ROW EXECUTE FUNCTION reject_change();\n"
    )
    invariant_spec = bundle(invariant_baseline, schema_id="example.invariants")
    await installer.install(
        invariant_spec,
        invariant_baseline,
        "sha256:" + "6" * 64,
        execution_profile="official",
    )
    async with store.pool.connection() as conn:
        await conn.execute(
            sql.SQL("INSERT INTO {}.records(record_id) VALUES ('one')").format(
                sql.Identifier(invariant_spec.namespace)
            )
        )
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.records SET record_id='two'").format(
                    sql.Identifier(invariant_spec.namespace)
                )
            )


def test_official_storage_identity_is_independent_from_runtime_distribution():
    runtime_artifact = official_artifact("bot")
    first = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    repeated = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    another = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )

    assert first.package_digest == repeated.package_digest
    assert first.package_digest != runtime_artifact.package_digest
    assert first.package_digest != another.package_digest
    assert first.as_fact()["package_digest"] == first.package_digest


def test_builtin_storage_identity_survives_code_only_hot_reload(tmp_path):
    source = tmp_path / "counter"
    copytree(
        Path(__file__).resolve().parents[1] / "src/forest/builtin/counter",
        source,
        ignore=ignore_patterns("__pycache__", "*.pyc"),
    )
    catalog = PackageCatalog(tmp_path / "catalog")
    before = catalog.build(source, development=True)
    worker = source / "python/counter.py"
    worker.write_bytes(worker.read_bytes() + b"\n# code-only package revision\n")
    after = catalog.build(source, development=True)

    assert before.digest != after.digest
    assert before.manifest["version"] != after.manifest["version"]
    assert _configuration_storage(before).package_digest == _configuration_storage(
        after
    ).package_digest


@pytest.mark.postgres
async def test_clean_kernel_runtime_bundle_owns_only_generic_runtime_facts(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    row = await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    assert row["component_id"] == "kernel.runtime"
    assert row["execution_profile"] == "official"
    namespace = storage.spec.namespace
    async with store.pool.connection() as conn:
        tables = await (
            await conn.execute(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n "
                "ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind IN ('r','p') "
                "ORDER BY c.relname",
                (namespace,),
            )
        ).fetchall()
        names = {item["relname"] for item in tables}
        assert {
            "runtime_state",
            "host_lease",
            "generations",
            "routes",
            "lifecycle_operations",
            "service_operations",
            "service_call_attempts",
            "state_heads",
            "runtime_graphs",
            "runtime_graph_modules",
            "runtime_graph_retention_dependencies",
        } <= names
        assert not any(
            token in name
            for name in names
            for token in ("agent", "memory", "onebot", "binding", "job")
        )
        seeded = await (
            await conn.execute(
                sql.SQL("SELECT bot_id FROM {}.runtime_state").format(sql.Identifier(namespace))
            )
        ).fetchone()
        assert seeded["bot_id"] == store.bot_id
        columns = await (
            await conn.execute(
                "SELECT table_name,column_name,is_nullable FROM information_schema.columns "
                "WHERE table_schema=%s AND ((table_name='generations' AND "
                "column_name='config_revision') OR (table_name='runtime_graph_modules' "
                "AND column_name='generation_id')) ORDER BY table_name",
                (namespace,),
            )
        ).fetchall()
        assert columns == [
            {
                "table_name": "generations",
                "column_name": "config_revision",
                "is_nullable": "NO",
            },
            {
                "table_name": "runtime_graph_modules",
                "column_name": "generation_id",
                "is_nullable": "NO",
            },
        ]
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.host_incarnations"
                "(bot_id,incarnation,host_id,owner_epoch) VALUES (%s,'inc','host',1)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id,),
        )
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("DELETE FROM {}.host_incarnations").format(sql.Identifier(namespace))
            )
        digest = "sha256:" + "1" * 64
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.generations"
                "(bot_id,generation_id,component_id,package_digest,config_revision,"
                "config_payload_id,config_payload_digest,instance_id,worker_incarnation,"
                "host_incarnation,host_owner_epoch,state_schema,state_contract_digest,"
                "capability_grants_digest,status) VALUES "
                "(%s,'gen-1','component.correct',%s,7,'payload-7',%s,'instance-1',"
                "'worker-1','inc',1,1,%s,%s,'READY')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, digest, digest, digest, digest),
        )
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.generations SET config_revision=8").format(
                    sql.Identifier(namespace)
                )
            )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.runtime_graphs"
                "(bot_id,graph_id,graph_revision,graph_digest,activation_id,state) "
                "VALUES (%s,'graph-1',1,%s,'activation-1','PREPARED')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, digest),
        )
        member_sql = sql.SQL(
            "INSERT INTO {}.runtime_graph_modules"
            "(bot_id,graph_id,component_id,generation_id,package_id,package_version,"
            "package_digest,driver,trust_level,descriptor_digest,route_contract_digest,"
            "management_contract_digest,management_ui_digest,data_admission,control_admission) "
            "VALUES (%s,'graph-1',%s,'gen-1','pkg','1.0.0',%s,'native','official',"
            "%s,%s,%s,%s,true,true)"
        ).format(sql.Identifier(namespace))
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await conn.execute(
                member_sql,
                (
                    store.bot_id,
                    "component.wrong",
                    digest,
                    digest,
                    digest,
                    digest,
                    digest,
                ),
            )
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await conn.execute(
                member_sql,
                (
                    store.bot_id,
                    "component.correct",
                    "sha256:" + "2" * 64,
                    digest,
                    digest,
                    digest,
                    digest,
                ),
            )
        await conn.execute(
            member_sql,
            (
                store.bot_id,
                "component.correct",
                digest,
                digest,
                digest,
                digest,
                digest,
            ),
        )


def runtime_binding(
    component_id: str,
    *,
    marker: str,
    requires: tuple[str, ...] = (),
    storage: RuntimeStorageBinding | None = None,
) -> PreparedGenerationBinding:
    digest = "sha256:" + marker * 64
    return PreparedGenerationBinding(
        component_id=component_id,
        requires=requires,
        package_id="forest.tests",
        package_version="1.0.0",
        package_digest=digest,
        driver="forest.tests/1",
        trust_level="official",
        descriptor_digest=digest,
        route_contract_digest=digest,
        management_contract_digest=digest,
        management_ui_digest=digest,
        config_revision=1,
        config_payload_id=f"config:{component_id}:1",
        config_payload_digest=digest,
        instance_id=f"instance:{component_id}:{marker}",
        worker_incarnation=f"worker:{component_id}:{marker}",
        state_schema=1,
        state_contract_digest=digest,
        capability_grants_digest=digest,
        data_admission=True,
        control_admission=True,
        storage=storage,
    )


@pytest.mark.postgres
async def test_clean_runtime_repository_publishes_exact_generation_graph(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    runtime_storage = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    await installer.install(
        payload_storage.spec,
        payload_storage.baseline,
        payload_storage.package_digest,
        execution_profile=payload_storage.execution_profile,
    )
    await installer.install(
        runtime_storage.spec,
        runtime_storage.baseline,
        runtime_storage.package_digest,
        execution_profile=runtime_storage.execution_profile,
    )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    configuration = ModuleConfigurationRepository(
        store,
        component_id="kernel.runtime",
        namespace=runtime_storage.spec.namespace,
        payloads=payloads,
    )
    validation_digest = "sha256:" + "9" * 64
    await configuration.initialize(
        operation_id="initialize:kernel.runtime",
        value={},
        validation_digest=validation_digest,
    )
    repository = KernelRuntimeRepository(store, runtime_storage.spec.namespace)
    lease, renewed = await asyncio.gather(
        repository.claim_host("host-a", "inc-a"),
        repository.claim_host("host-a", "inc-a"),
    )
    assert renewed.owner_epoch == lease.owner_epoch == 1
    with pytest.raises(ForestError) as caught:
        await repository.claim_host("host-b", "inc-b")
    assert caught.value.code == "HOST_LEASE_HELD"

    storage = RuntimeStorageBinding(
        runtime_storage.spec.schema_id,
        runtime_storage.spec.schema_version,
        runtime_storage.spec.namespace,
        runtime_storage.spec.baseline_sha256,
        runtime_storage.package_digest,
        runtime_storage.spec.repository_contract,
        runtime_storage.execution_profile,
    )
    kernel = runtime_binding("kernel.runtime", marker="1", storage=storage)
    kernel = replace(kernel, package_digest=runtime_storage.package_digest)
    consumer = runtime_binding("example.consumer", marker="2", requires=("kernel.runtime",))
    activation_id = str(uuid4())
    graph = await repository.prepare_graph(renewed, [consumer, kernel], activation_id)
    assert await repository.prepare_graph(renewed, [kernel, consumer], activation_id) == graph
    assert await repository.publish_graph(renewed, graph) == graph.graph_revision
    assert await repository.publish_graph(renewed, graph) == graph.graph_revision

    namespace = runtime_storage.spec.namespace
    async with store.pool.connection() as conn:
        head = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graph_head").format(sql.Identifier(namespace))
            )
        ).fetchone()
        routes = await (
            await conn.execute(
                sql.SQL(
                    "SELECT component_id,generation_id,route_epoch,data_admission,"
                    "control_admission FROM {}.routes ORDER BY component_id"
                ).format(sql.Identifier(namespace))
            )
        ).fetchall()
        generations = await (
            await conn.execute(
                sql.SQL(
                    "SELECT component_id,config_revision,status FROM {}.generations "
                    "ORDER BY component_id"
                ).format(sql.Identifier(namespace))
            )
        ).fetchall()
    assert head["graph_id"] == graph.graph_id
    assert {item["component_id"] for item in routes} == {
        "example.consumer",
        "kernel.runtime",
    }
    assert all(item["generation_id"] and item["route_epoch"] == 1 for item in routes)
    assert all(item["data_admission"] and item["control_admission"] for item in routes)
    assert [(item["config_revision"], item["status"]) for item in generations] == [
        (1, "ACTIVE"),
        (1, "ACTIVE"),
    ]

    with pytest.raises(ForestError) as caught:
        await repository.prepare_graph(
            renewed,
            [kernel, replace(consumer, config_revision=2)],
            activation_id,
        )
    assert caught.value.code == "RUNTIME_ACTIVATION_CONFLICT"

    kernel_v2 = replace(
        runtime_binding("kernel.runtime", marker="5", storage=storage),
        package_digest=runtime_storage.package_digest,
    )
    consumer_v2 = runtime_binding("example.consumer", marker="6", requires=("kernel.runtime",))
    graph_v2 = await repository.prepare_graph(renewed, [kernel_v2, consumer_v2], str(uuid4()))
    assert graph_v2.graph_revision > graph.graph_revision
    assert await repository.publish_graph(renewed, graph_v2) == graph_v2.graph_revision
    with pytest.raises(ForestError) as caught:
        await repository.publish_graph(renewed, graph)
    assert caught.value.code == "RUNTIME_GRAPH_STALE"
    assert await repository.retire_graph(renewed, graph) == graph_v2.graph_revision

    async with store.pool.connection() as conn:
        routes = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.routes ORDER BY component_id").format(
                    sql.Identifier(namespace)
                )
            )
        ).fetchall()
        statuses = await (
            await conn.execute(
                sql.SQL(
                    "SELECT generation_id,status FROM {}.generations ORDER BY generation_id"
                ).format(sql.Identifier(namespace))
            )
        ).fetchall()
    assert all(item["route_epoch"] == 2 for item in routes)
    assert {
        item["status"] for item in statuses if item["generation_id"] in graph.generations.values()
    } == {"QUIESCING"}
    assert {
        item["status"]
        for item in statuses
        if item["generation_id"] in graph_v2.generations.values()
    } == {"ACTIVE"}

    active_route = await repository.verify_active_generation(
        component_id="example.consumer",
        generation_id=graph_v2.generations["example.consumer"],
    )
    assert active_route.graph_id == graph_v2.graph_id
    kernel_route = await repository.verify_active_generation(
        component_id="kernel.runtime",
        generation_id=graph_v2.generations["kernel.runtime"],
    )
    with pytest.raises(ForestError) as unsafe:
        await repository.freeze_graph_admission(
            renewed, graph_v2, components=("kernel.runtime",)
        )
    assert unsafe.value.code == "RUNTIME_GRAPH_MEMBER_SELECTION_UNSAFE"
    with pytest.raises(ForestError) as unknown:
        await repository.freeze_graph_admission(
            renewed, graph_v2, components=("example.absent",)
        )
    assert unknown.value.code == "RUNTIME_GRAPH_MEMBER_SELECTION_INVALID"
    await repository.freeze_graph_admission(
        renewed, graph_v2, components=("example.consumer",)
    )
    kept_route = await repository.verify_active_generation(
        component_id="kernel.runtime",
        generation_id=graph_v2.generations["kernel.runtime"],
    )
    assert kept_route.route_epoch == kernel_route.route_epoch
    with pytest.raises(ForestError) as frozen:
        await repository.verify_active_generation(
            component_id="example.consumer",
            generation_id=graph_v2.generations["example.consumer"],
        )
    assert frozen.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    await repository.resume_graph_admission(
        renewed, graph_v2, components=("example.consumer",)
    )
    active_route = await repository.verify_active_generation(
        component_id="example.consumer",
        generation_id=graph_v2.generations["example.consumer"],
    )
    assert active_route.route_epoch == 4
    assert (
        await repository.verify_active_generation(
            component_id="kernel.runtime",
            generation_id=graph_v2.generations["kernel.runtime"],
        )
    ).route_epoch == kernel_route.route_epoch
    await repository.freeze_graph_admission(renewed, graph_v2)
    with pytest.raises(ForestError) as caught:
        await repository.verify_active_generation(
            component_id="example.consumer",
            generation_id=graph_v2.generations["example.consumer"],
        )
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    async with store.pool.connection() as conn, conn.transaction():
        draining = await repository.verify_generation_member_in(
            conn,
            component_id="example.consumer",
            generation_id=graph_v2.generations["example.consumer"],
        )
    assert draining == graph_v2
    await repository.resume_graph_admission(renewed, graph_v2)
    resumed_route = await repository.verify_active_generation(
        component_id="example.consumer",
        generation_id=graph_v2.generations["example.consumer"],
    )
    assert resumed_route.route_epoch == active_route.route_epoch + 2
    await repository.freeze_graph_admission(renewed, graph_v2)

    retired_revision = await repository.retire_graph(renewed, graph_v2)
    assert retired_revision > graph_v2.graph_revision
    async with store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as caught:
            await repository.verify_generation_member_in(
                conn,
                component_id="example.consumer",
                generation_id=graph_v2.generations["example.consumer"],
            )
    assert caught.value.code == "RUNTIME_GENERATION_NOT_CURRENT"
    await repository.record_states(
        renewed,
        graph_v2,
        {component_id: ("STOPPED", None) for component_id in graph_v2.generations},
    )
    with pytest.raises(ForestError) as caught:
        await repository.record_states(
            renewed,
            graph_v2,
            {"kernel.runtime": ("ACTIVE", None)},
        )
    assert caught.value.code == "RUNTIME_LIFECYCLE_INVALID"
    async with store.pool.connection() as conn:
        head = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graph_head").format(sql.Identifier(namespace))
            )
        ).fetchone()
        routes = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.routes ORDER BY component_id").format(
                    sql.Identifier(namespace)
                )
            )
        ).fetchall()
        saved_graph = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graphs WHERE graph_id=%s").format(
                    sql.Identifier(namespace)
                ),
                (graph_v2.graph_id,),
            )
        ).fetchone()
        lifecycle = await (
            await conn.execute(
                sql.SQL(
                    "SELECT component_id,state FROM {}.runtime_graph_lifecycle_facts "
                    "WHERE graph_id=%s ORDER BY component_id,sequence"
                ).format(sql.Identifier(namespace)),
                (graph_v2.graph_id,),
            )
        ).fetchall()
    assert head["graph_id"] is None and head["graph_revision"] == retired_revision
    assert saved_graph["state"] == "STOPPED"
    assert {
        item["component_id"]: [
            fact["state"] for fact in lifecycle if fact["component_id"] == item["component_id"]
        ]
        for item in routes
    } == {
        "example.consumer": ["PREPARED", "ACTIVE", "QUIESCING", "QUIESCENT", "STOPPED"],
        "kernel.runtime": ["PREPARED", "ACTIVE", "QUIESCING", "QUIESCENT", "STOPPED"],
    }
    assert all(
        item["generation_id"] is None
        and not item["data_admission"]
        and not item["control_admission"]
        for item in routes
    )


@pytest.mark.postgres
async def test_runtime_graph_retains_unchanged_generations_without_republishing_them(
    clean_authority,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    runtime = kernel.runtime
    lease = await runtime.claim_host("retain-host", "retain-incarnation")
    provider = runtime_binding("example.provider", marker="a")
    caller = replace(
        runtime_binding("example.caller", marker="d"),
        retention_requires=("example.provider",),
    )
    consumer = runtime_binding(
        "example.consumer", marker="b", requires=("example.provider",)
    )
    first = await runtime.prepare_graph(lease, [provider, caller, consumer], str(uuid4()))
    await runtime.publish_graph(lease, first)
    retained_id = first.generations["example.provider"]
    provider_instance = SimpleNamespace(
        runtime_contract={"services": [{"service_id": "example.effect", "version": 1}]},
        runtime_service_handlers=lambda: {
            ("example.effect", 1): RoutedServiceEndpoint(
                lambda route, _invocation, payload: {
                    "graph_id": route.graph_id,
                    "graph_revision": route.graph_revision,
                    "value": payload["value"],
                }
            )
        },
        generation_identity=SimpleNamespace(generation_id=retained_id),
    )
    caller_instance = SimpleNamespace(
        runtime_contract={"requires": [{"service_id": "example.effect", "version": 1}]}
    )
    registry = SimpleNamespace(
        graph_identity=first,
        instances={
            "example.provider": provider_instance,
            "example.caller": caller_instance,
        },
        states={
            "example.provider": ModuleLifecycleState.ACTIVE,
            "example.caller": ModuleLifecycleState.ACTIVE,
        },
        generation_authority=runtime,
    )
    router = RuntimeContractRouter(registry)
    handle = router.request(
        caller_component_id="example.caller",
        service_id="example.effect",
        version=1,
        admission_lane="DATA",
        owned_work=False,
    )
    router.register("example.provider", provider_instance)
    router.seal()
    router.activate()
    before = await runtime.verify_active_generation(
        component_id="example.provider", generation_id=retained_id
    )
    ledger = KernelServiceOperationLedger(runtime, kernel.payload)
    scope = PackageCallScope(
        bot_id=store.bot_id,
        component_id="example.provider",
        generation_id=retained_id,
        instance_id="instance:example.provider:a",
        worker_incarnation="worker:example.provider:a",
        call_id="call:retained-before",
        call_operation_id="operation:retained-before",
        causal_root_kind="test.runtime",
        causal_root_id="source:retained",
    )
    origin_route = PackageServiceRoute(
        service_id="example.effect",
        graph_id=first.graph_id,
        graph_revision=first.graph_revision,
        admission_lane="DATA",
        owned_work=True,
        parent_call_id=None,
        timeout_seconds=30,
    )
    assert (await ledger.begin(scope, origin_route, {"value": 1})).execute

    second = await runtime.prepare_graph(
        lease,
        [runtime_binding("example.consumer", marker="c", requires=("example.provider",))],
        str(uuid4()),
        retained_generations={
            "example.provider": retained_id,
            "example.caller": first.generations["example.caller"],
        },
        source_graph=first,
    )
    assert second.generations["example.provider"] == retained_id
    assert await runtime.prepare_graph(
        lease,
        [runtime_binding("example.consumer", marker="c", requires=("example.provider",))],
        second.activation_id,
        retained_generations={
            "example.provider": retained_id,
            "example.caller": first.generations["example.caller"],
        },
        source_graph=first,
    ) == second
    with pytest.raises(ForestError) as conflict:
        await runtime.publish_graph(
            lease, second, configuration_activators={"example.provider": object()}
        )
    assert conflict.value.code == "RUNTIME_CONFIGURATION_PARTICIPANTS_INVALID"
    await runtime.publish_graph(lease, second)
    after = await runtime.verify_active_generation(
        component_id="example.provider", generation_id=retained_id
    )
    assert after.route_epoch == before.route_epoch
    assert after.graph_id == second.graph_id
    async with store.pool.connection() as conn, conn.transaction():
        assert await runtime.verify_generation_graph_fence_in(
            conn,
            component_id="example.provider",
            generation_id=retained_id,
            graph_id=first.graph_id,
            graph_revision=first.graph_revision,
        ) == second
        with pytest.raises(ForestError) as conflict:
            await runtime.verify_generation_graph_fence_in(
                conn,
                component_id="example.provider",
                generation_id=retained_id,
                graph_id=first.graph_id,
                graph_revision=second.graph_revision,
            )
        assert conflict.value.code == "RUNTIME_GENERATION_GRAPH_FENCE_STALE"
    assert await handle.invoke(
        {"value": 7},
        operation_id="call:retained-router",
        causal_root_kind="test.runtime",
        causal_root_id="source:retained",
    ) == {
        "graph_id": second.graph_id,
        "graph_revision": second.graph_revision,
        "value": 7,
    }
    await ledger.succeed(scope, origin_route, {"value": 1})
    later_scope = replace(
        scope,
        call_id="call:retained-after",
        call_operation_id="operation:retained-after",
    )
    with pytest.raises(ForestError) as conflict:
        await ledger.begin(
            later_scope,
            replace(origin_route, graph_revision=second.graph_revision),
            {"value": 2},
        )
    assert conflict.value.code == "RUNTIME_GENERATION_GRAPH_FENCE_STALE"
    assert (await ledger.begin(later_scope, origin_route, {"value": 2})).execute
    await ledger.succeed(later_scope, origin_route, {"value": 2})
    async with store.pool.connection() as conn:
        statuses = await (
            await conn.execute(
                sql.SQL(
                    "SELECT generation_id,status FROM {}.generations "
                    "WHERE generation_id=ANY(%s)"
                ).format(sql.Identifier(runtime.namespace)),
                (list(first.generations.values()),),
            )
        ).fetchall()
        attempts = await (
            await conn.execute(
                sql.SQL(
                    "SELECT call_id,graph_id,graph_revision,status FROM {}.service_call_attempts "
                    "WHERE call_id IN (%s,%s) ORDER BY call_id"
                ).format(sql.Identifier(runtime.namespace)),
                (scope.call_id, later_scope.call_id),
            )
        ).fetchall()
    assert {item["generation_id"]: item["status"] for item in statuses} == {
        retained_id: "ACTIVE",
        first.generations["example.caller"]: "ACTIVE",
        first.generations["example.consumer"]: "QUIESCING",
    }
    assert {
        item["call_id"]: (item["graph_id"], item["graph_revision"], item["status"])
        for item in attempts
    } == {
        scope.call_id: (first.graph_id, first.graph_revision, "SUCCEEDED"),
        later_scope.call_id: (second.graph_id, second.graph_revision, "SUCCEEDED"),
    }
    with pytest.raises(ForestError) as conflict:
        await runtime.record_states(
            lease, first, {"example.provider": ("STOPPED", None)}
        )
    assert conflict.value.code == "RUNTIME_RETAINED_GENERATION_ACTIVE"
    await runtime.record_states(
        lease, first, {"example.consumer": ("STOPPED", None)}
    )
    assert (
        await runtime.verify_active_generation(
            component_id="example.provider", generation_id=retained_id
        )
    ).route_epoch == before.route_epoch

    with pytest.raises(ForestError) as conflict:
        await runtime.prepare_graph(
            lease,
            [runtime_binding("example.provider", marker="d")],
            str(uuid4()),
            retained_generations={
                "example.consumer": second.generations["example.consumer"]
            },
            source_graph=second,
        )
    assert conflict.value.code == "RUNTIME_RETAINED_DEPENDENCY_CHANGED"
    with pytest.raises(ForestError) as conflict:
        await runtime.prepare_graph(
            lease,
            [
                runtime_binding("example.provider", marker="d"),
                runtime_binding(
                    "example.consumer", marker="e", requires=("example.provider",)
                ),
            ],
            str(uuid4()),
            retained_generations={
                "example.caller": second.generations["example.caller"]
            },
            source_graph=second,
        )
    assert conflict.value.code == "RUNTIME_RETAINED_DEPENDENCY_CHANGED"

    newer = await runtime.prepare_graph(
        lease,
        [
            runtime_binding("example.provider", marker="f"),
            runtime_binding(
                "example.consumer", marker="1", requires=("example.provider",)
            ),
        ],
        str(uuid4()),
    )
    stale = await runtime.prepare_graph(
        lease,
        [runtime_binding("example.consumer", marker="e", requires=("example.provider",))],
        str(uuid4()),
        retained_generations={"example.provider": retained_id},
        source_graph=second,
    )
    await runtime.publish_graph(lease, newer)
    with pytest.raises(ForestError) as conflict:
        await runtime.publish_graph(lease, stale)
    assert conflict.value.code == "RUNTIME_RETAINED_GENERATION_STALE"
    with pytest.raises(ForestError) as conflict:
        await handle.invoke(
            {"value": 8},
            operation_id="call:stale-router",
            causal_root_kind="test.runtime",
            causal_root_id="source:retained",
        )
    assert conflict.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    with pytest.raises(ForestError) as conflict:
        await ledger.begin(
            replace(
                scope,
                call_id="call:retired-provider",
                call_operation_id="operation:retired-provider",
            ),
            origin_route,
            {"value": 3},
        )
    assert conflict.value.code == "RUNTIME_GENERATION_NOT_CURRENT"


@pytest.mark.postgres
async def test_component_state_separates_queries_from_revision_checked_commands(
    clean_authority,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    lease = await kernel.runtime.claim_host("state-host", "state-incarnation")
    binding = runtime_binding("example.state", marker="a")
    graph = await kernel.runtime.prepare_graph(lease, [binding], str(uuid4()))
    await kernel.runtime.publish_graph(lease, graph)
    generation_id = graph.generations["example.state"]
    queries = ComponentStateQueries(kernel.runtime, kernel.payload)
    commands = ComponentStateCommands(kernel.runtime, kernel.payload)

    assert await queries.get(
        component_id="example.state",
        generation_id=generation_id,
        state_key="counter",
    ) == {"value": None, "revision": 0}
    first = await commands.commit(
        component_id="example.state",
        generation_id=generation_id,
        call_id="call:first",
        state_key="counter",
        value={"count": 1},
        expected_revision=0,
        operation_id="state:counter:1",
    )
    replay = await commands.commit(
        component_id="example.state",
        generation_id=generation_id,
        call_id="call:retry",
        state_key="counter",
        value={"count": 1},
        expected_revision=0,
        operation_id="state:counter:1",
    )
    assert replay == first
    assert await queries.get(
        component_id="example.state",
        generation_id=generation_id,
        state_key="counter",
    ) == {"value": {"count": 1}, "revision": 1}

    attempts = await asyncio.gather(
        *(
            commands.commit(
                component_id="example.state",
                generation_id=generation_id,
                call_id=f"call:concurrent:{index}",
                state_key="counter",
                value={"count": index},
                expected_revision=1,
                operation_id=f"state:counter:concurrent:{index}",
            )
            for index in (2, 3)
        ),
        return_exceptions=True,
    )
    assert len([item for item in attempts if isinstance(item, dict)]) == 1
    conflicts = [item for item in attempts if isinstance(item, ForestError)]
    assert len(conflicts) == 1
    assert conflicts[0].code == "COMPONENT_STATE_REVISION_CONFLICT"

    same_operation = await asyncio.gather(*(
        commands.commit(
            component_id="example.state", generation_id=generation_id,
            call_id=f"call:shared:{index}", state_key="shared",
            value={"count": 1}, expected_revision=0,
            operation_id="state:shared:1",
        ) for index in (1, 2)
    ))
    assert same_operation[0] == same_operation[1]
    assert await queries.get(
        component_id="example.state", generation_id=generation_id,
        state_key="shared",
    ) == {"value": {"count": 1}, "revision": 1}

    cross_key = await asyncio.gather(*(
        commands.commit(
            component_id="example.state", generation_id=generation_id,
            call_id=f"call:cross-key:{key}", state_key=key,
            value={"count": 1}, expected_revision=0,
            operation_id="state:cross-key:1",
        ) for key in ("other-a", "other-b")
    ), return_exceptions=True)
    assert len([item for item in cross_key if isinstance(item, dict)]) == 1
    cross_key_conflicts = [
        item for item in cross_key if isinstance(item, ForestError)
    ]
    assert len(cross_key_conflicts) == 1
    assert cross_key_conflicts[0].code == "COMPONENT_STATE_OPERATION_CONFLICT"


@pytest.mark.postgres
async def test_builtin_package_domain_state_runs_only_through_declared_authority(
    clean_authority,
    tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    counter = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/counter"))),
        PackageCatalog(tmp_path / "packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={"label": "计数", "multiplier": 2},
    )
    lease = await kernel.runtime.claim_host("counter-host", "counter-incarnation")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(
        host,
        _isolated_runtime_descriptors(*kernel.descriptors, counter.descriptor),
    )
    await registry.start()
    try:
        instance = registry.get("forest.counter")
        endpoint = instance.runtime_service_handlers()[("counter.increment", 1)]
        route = FrozenServiceRoute(
            service_id="counter.increment",
            version=1,
            caller_component_id="kernel.runtime",
            provider_component_id="forest.counter",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        first = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "counter:service:1", 10, "test.counter", "source:counter:1"
            ),
            ({"increment": 1},),
            {},
        )
        second = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "counter:service:2", 10, "test.counter", "source:counter:2"
            ),
            ({"increment": 1},),
            {},
        )
        replay = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "counter:service:1", 10, "test.counter", "source:counter:1"
            ),
            ({"increment": 1},),
            {},
        )
        third = await endpoint.invoke_routed(
            route,
            RuntimeServiceInvocation(
                "counter:service:3", 10, "test.counter", "source:counter:3"
            ),
            ({"increment": 1},),
            {},
        )
        assert first == {"value": 2, "revision": 1, "label": "计数"}
        assert second == {"value": 4, "revision": 2, "label": "计数"}
        assert replay == first
        assert third == {"value": 6, "revision": 3, "label": "计数"}
        participant = RuntimeKernelServiceContext(
            registry, ("forest.counter",)
        ).participant("forest.counter")
        assert participant.component_id == "forest.counter"
        assert participant.generation_id == instance.generation_identity.generation_id
        with pytest.raises(RuntimeError, match="rollback participant"):
            async with store.pool.connection() as conn, conn.transaction():
                rolled_back = await participant.invoke_in(
                    conn,
                    "counter.set-in-transaction",
                    {"value": 8, "expected_revision": 3},
                    operation_id="counter:participant:rollback",
                    causal_root_kind="test.counter",
                    causal_root_id="source:counter:rollback",
                    parent_call_id="call:counter:rollback",
                    timeout_seconds=10,
                )
                assert rolled_back == {"value": 8, "revision": 4}
                raise RuntimeError("rollback participant")
        async with store.pool.connection() as conn, conn.transaction():
            committed = await participant.invoke_in(
                conn,
                "counter.set-in-transaction",
                {"value": 8, "expected_revision": 3},
                operation_id="counter:participant:commit",
                causal_root_kind="test.counter",
                causal_root_id="source:counter:commit",
                parent_call_id="call:counter:commit",
                timeout_seconds=10,
            )
        assert committed == {"value": 8, "revision": 4}
        deferred_endpoint = instance.runtime_service_handlers()[("counter.defer", 1)]
        deferred_route = FrozenServiceRoute(
            service_id="counter.defer",
            version=1,
            caller_component_id="kernel.runtime",
            provider_component_id="forest.counter",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        deferred = await deferred_endpoint.invoke_routed(
            deferred_route,
            RuntimeServiceInvocation(
                "counter:defer:1", 10, "test.counter", "source:counter:defer"
            ),
            ({"work_id": "work-1", "value": 10, "expected_revision": 4},),
            {},
        )
        assert deferred == {"work_id": "work-1", "state": "PENDING"}
        work_row = None
        for _attempt in range(100):
            async with store.pool.connection() as conn:
                work_row = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT state,attempt_count,result_payload "
                            "FROM {}.counter_work WHERE work_id=%s"
                        ).format(
                            sql.Identifier(counter.descriptor.storage.spec.namespace)
                        ),
                        ("work-1",),
                    )
                ).fetchone()
            if work_row is not None and work_row["state"] == "SUCCEEDED":
                break
            await asyncio.sleep(0.05)
        assert dict(work_row) == {
            "state": "SUCCEEDED",
            "attempt_count": 1,
            "result_payload": {"value": 10, "revision": 5},
        }
        contract = instance.management_contract
        description = contract.describe()
        assert [item["query_id"] for item in description["queries"]] == [
            "counter.current"
        ]
        assert [item["action_id"] for item in description["actions"]] == [
            "counter.set"
        ]
        assert await contract.query("counter.current", {}) == {
            "value": 10,
            "revision": 5,
        }

        async def reject_management_write(_conn, **_kwargs):
            raise ForestError("WEB_POLICY_DENIED", "denied", 403)

        authorization = SimpleNamespace(
            actor_ref="web:test",
            web_subject_ref="web:test",
            target_generation_id=instance.generation_identity.generation_id,
        )
        with pytest.raises(ForestError, match="denied"):
            await contract.action(
                "counter.set",
                operation_id="management:counter:set:denied",
                payload={"value": 11, "expected_revision": 5},
                authorization=authorization,
                authorization_guard=reject_management_write,
            )
        assert await contract.query("counter.current", {}) == {
            "value": 10,
            "revision": 5,
        }
        async def accept_management_write(_conn, **_kwargs):
            return None

        changed = await contract.action(
            "counter.set",
            operation_id="management:counter:set:accepted",
            payload={"value": 12, "expected_revision": 5},
            authorization=authorization,
            authorization_guard=accept_management_write,
        )
        assert changed == {"value": 12, "revision": 6}
        transaction_endpoint = instance.runtime_service_handlers()[
            ("counter.transaction", 1)
        ]
        transaction_route = FrozenServiceRoute(
            service_id="counter.transaction",
            version=1,
            caller_component_id="kernel.runtime",
            provider_component_id="forest.counter",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        transaction_result = await transaction_endpoint.invoke_routed(
            transaction_route,
            RuntimeServiceInvocation(
                "counter:uow:1", 10, "test.counter", "source:counter:uow"
            ),
            ({"value": 14, "expected_revision": 6},),
            {},
        )
        assert transaction_result == {"value": 14, "revision": 7}
        reject_endpoint = instance.runtime_service_handlers()[
            ("counter.transaction-reject", 1)
        ]
        reject_route = FrozenServiceRoute(
            service_id="counter.transaction-reject",
            version=1,
            caller_component_id="kernel.runtime",
            provider_component_id="forest.counter",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        with pytest.raises(ForestError) as rejected:
            await reject_endpoint.invoke_routed(
                reject_route,
                RuntimeServiceInvocation(
                    "counter:uow:reject:1",
                    10,
                    "test.counter",
                    "source:counter:uow:reject",
                ),
                ({"value": 16, "expected_revision": 7},),
                {},
            )
        assert rejected.value.code == "COUNTER_UOW_REJECTED"
        assert await contract.query("counter.current", {}) == {
            "value": 14,
            "revision": 7,
        }
        cross_endpoint = instance.runtime_service_handlers()[
            ("counter.cross-component-probe", 1)
        ]
        cross_route = FrozenServiceRoute(
            service_id="counter.cross-component-probe",
            version=1,
            caller_component_id="kernel.runtime",
            provider_component_id="forest.counter",
            provider_generation_id=instance.generation_identity.generation_id,
            graph_id=registry.graph_identity.graph_id,
            graph_revision=registry.graph_identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
        )
        with pytest.raises(ForestError) as cross_rejected:
            await cross_endpoint.invoke_routed(
                cross_route,
                RuntimeServiceInvocation(
                    "counter:uow:cross:1",
                    10,
                    "test.counter",
                    "source:counter:uow:cross",
                ),
                (
                    {
                        "counter": {"value": 18, "expected_revision": 7},
                        "effect": {
                            "protocol": "forest.effect-observe/1",
                            "effect_operation_id": "effect:absent",
                            "attempt_id": "attempt:absent",
                            "observation_operation_id": "observation:absent",
                            "status": "UNKNOWN",
                            "evidence_kind": "executor",
                            "evidence_payload": None,
                        },
                    },
                ),
                {},
            )
        assert cross_rejected.value.code == "EFFECT_ATTEMPT_NOT_FOUND"
        assert await contract.query("counter.current", {}) == {
            "value": 14,
            "revision": 7,
        }
        async with store.pool.connection() as conn:
            domain_row = await (
                await conn.execute(
                    sql.SQL("SELECT value,revision FROM {}.counter_values").format(
                        sql.Identifier(counter.descriptor.storage.spec.namespace)
                    )
                )
            ).fetchone()
            generic_state = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS count FROM {}.state_heads "
                        "WHERE bot_id=%s AND component_id=%s"
                    ).format(sql.Identifier(kernel.runtime.namespace)),
                    (store.bot_id, "forest.counter"),
                )
            ).fetchone()
        assert dict(domain_row) == {"value": 14, "revision": 7}
        assert generic_state["count"] == 0
    finally:
        assert await registry.close() == []


@pytest.mark.postgres
async def test_delivery_builtin_owns_transactional_intent_creation(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    delivery = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/delivery"))),
        PackageCatalog(tmp_path / "delivery-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration={"protocol": "forest.delivery/1"},
        effect_driver_registry=PackageEffectDriverRegistry(
            {
                "kernel.payload-json": kernel_payload_json_effect_driver_factory(
                    runtime=kernel.runtime,
                    payloads=kernel.payload,
                )
            }
        ),
        resolved_contracts={},
    )
    reply_ref = "reply/1"
    epoch = await kernel.payload.epoch()
    saved = await kernel.payload.put_bytes(
        owner_component_id="agent.runtime",
        owner_kind="final.reply",
        owner_id=reply_ref,
        media_type="application/json",
        content=b'{"text":"A"}',
        expected_deletion_epoch=epoch,
    )
    request = {
        "protocol": "forest.delivery-intent/1",
        "intent_id": "intent/1",
        "binding_ref": "binding/main",
        "lane_kind": "CHAT",
        "ordering_key": "conversation/1",
        "conversation_ref": "conversation/1",
        "channel_kind": "GROUP",
        "channel_ref": "933179609",
        "cause_component_id": "conversation.scheduler",
        "cause_kind": "context.reply",
        "cause_id": reply_ref,
        "delivery_generation_id": "generation/delivery-test",
        "reply_ref": reply_ref,
        "parts": [
            {
                "part_operation_id": "part/1",
                "effect_operation_ref": "effect/1",
                "payload": {
                    "payload_id": saved.payload_id,
                    "digest": saved.content_digest,
                    "owner_component_id": "agent.runtime",
                    "owner_kind": "final.reply",
                    "owner_id": reply_ref,
                },
            }
        ],
    }
    context = {
        "bot_id": store.bot_id,
        "generation_id": "generation/delivery-test",
        "operation_id": "delivery:create:1",
        "input_digest": digest_json(request),
    }
    namespace = delivery.descriptor.storage.spec.namespace
    async with store.pool.connection() as conn, conn.transaction():
        envelope = (
            await (
                await conn.execute(
                    sql.SQL(
                        "SELECT {}.delivery_create_chat_intent(%s::jsonb,%s::jsonb) AS value"
                    ).format(sql.Identifier(namespace)),
                    (psycopg.types.json.Jsonb(context), psycopg.types.json.Jsonb(request)),
                )
            ).fetchone()
        )["value"]
    assert envelope == {
        "protocol": "forest.domain-result/1",
        "ok": True,
        "value": {
            "intent_id": "intent/1",
            "state": "QUEUED",
            "lane_sequence": 1,
            "part_count": 1,
        },
    }
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                sql.SQL(
                    "SELECT i.state,p.state AS part_state,p.payload_id "
                    "FROM {}.send_intents i JOIN {}.send_parts p USING(bot_id,intent_id) "
                    "WHERE i.bot_id=%s AND i.intent_id=%s"
                ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                (store.bot_id, "intent/1"),
            )
        ).fetchone()
    assert dict(row) == {
        "state": "QUEUED",
        "part_state": "QUEUED",
        "payload_id": saved.payload_id,
    }
    claim_request = {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "claim:delivery-test",
        "lease_seconds": 60,
    }
    async with store.pool.connection() as conn, conn.transaction():
        claimed = (
            await (
                await conn.execute(
                    sql.SQL("SELECT {}.delivery_claim_work(%s::jsonb,%s::jsonb) AS value").format(
                        sql.Identifier(namespace)
                    ),
                    (
                        psycopg.types.json.Jsonb(context),
                        psycopg.types.json.Jsonb(claim_request),
                    ),
                )
            ).fetchone()
        )["value"]
        work = claimed["value"]["payload"]
        assert work["mode"] == "DISPATCH"
        assert work["attempt_id"] == "claim:delivery-test"
        released = (
            await (
                await conn.execute(
                    sql.SQL("SELECT {}.delivery_release_claim(%s::jsonb,%s::jsonb) AS value").format(
                        sql.Identifier(namespace)
                    ),
                    (
                        psycopg.types.json.Jsonb(context),
                        psycopg.types.json.Jsonb(
                            {
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "delivery",
                                    "generation_id": "generation/delivery-test",
                                },
                                "input": {"work": work, "reason_code": "test-release"},
                                "prior": {},
                            }
                        ),
                    ),
                )
            ).fetchone()
        )["value"]
    assert released["value"] == {"intent_id": "intent/1", "state": "QUEUED"}


@pytest.mark.postgres
async def test_clean_runtime_rejects_uninstalled_storage_and_stale_host(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    runtime_storage = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    await installer.install(
        runtime_storage.spec,
        runtime_storage.baseline,
        runtime_storage.package_digest,
        execution_profile=runtime_storage.execution_profile,
    )
    repository = KernelRuntimeRepository(store, runtime_storage.spec.namespace)
    old_lease = await repository.claim_host("host-a", "inc-a")
    missing = RuntimeStorageBinding(
        "missing.schema",
        "1.0.0",
        "forest_m_missing",
        "a" * 64,
        "sha256:" + "a" * 64,
        "missing/1",
        "official",
    )
    with pytest.raises(ForestError) as caught:
        await repository.prepare_graph(
            old_lease,
            [runtime_binding("example.missing", marker="3", storage=missing)],
            str(uuid4()),
        )
    assert caught.value.code == "RUNTIME_STORAGE_BINDING_INVALID"

    async with store.pool.connection() as conn:
        await conn.execute(
            sql.SQL("UPDATE {}.host_lease SET expires_at='1970-01-01 UTC' WHERE bot_id=%s").format(
                sql.Identifier(runtime_storage.spec.namespace)
            ),
            (store.bot_id,),
        )
    claims = await asyncio.gather(
        repository.claim_host("host-b", "inc-b"),
        repository.claim_host("host-c", "inc-c"),
        return_exceptions=True,
    )
    leases = [item for item in claims if not isinstance(item, BaseException)]
    failures = [item for item in claims if isinstance(item, BaseException)]
    assert len(leases) == len(failures) == 1
    new_lease = leases[0]
    assert isinstance(failures[0], ForestError)
    assert failures[0].code == "HOST_LEASE_HELD"
    assert new_lease.owner_epoch == old_lease.owner_epoch + 1
    with pytest.raises(ForestError) as caught:
        await repository.prepare_graph(
            old_lease,
            [runtime_binding("example.module", marker="4")],
            str(uuid4()),
        )
    assert caught.value.code == "STALE_HOST"
    async with store.pool.connection() as conn:
        await conn.execute(
            sql.SQL("UPDATE {}.host_lease SET expires_at='1970-01-01 UTC' WHERE bot_id=%s").format(
                sql.Identifier(runtime_storage.spec.namespace)
            ),
            (store.bot_id,),
        )
    with pytest.raises(ForestError) as caught:
        await repository.claim_host("host-a", "inc-a")
    assert caught.value.code == "STALE_HOST"


@pytest.mark.postgres
async def test_runtime_registry_requires_and_publishes_prepared_generation_binding(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    runtime_storage = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    await installer.install(
        payload_storage.spec,
        payload_storage.baseline,
        payload_storage.package_digest,
        execution_profile=payload_storage.execution_profile,
    )
    await installer.install(
        runtime_storage.spec,
        runtime_storage.baseline,
        runtime_storage.package_digest,
        execution_profile=runtime_storage.execution_profile,
    )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    configuration = ModuleConfigurationRepository(
        store,
        component_id="kernel.runtime",
        namespace=runtime_storage.spec.namespace,
        payloads=payloads,
    )
    await configuration.initialize(
        operation_id="initialize:kernel.runtime",
        value={},
        validation_digest="sha256:" + "9" * 64,
    )
    repository = KernelRuntimeRepository(store, runtime_storage.spec.namespace)
    lease = await repository.claim_host("host-a", "inc-a")
    events = []

    class CleanModule:
        runtime_contract = {"services": []}
        state_schema = 1
        state_contract = {"type": "object", "additionalProperties": False}

        def __init__(self, *, mismatch=False, fail_bind=False, fail_activate=False):
            self.mismatch = mismatch
            self.fail_bind = fail_bind
            self.fail_activate = fail_activate
            self.module_configuration = configuration
            self.identity = None
            self.task = None

        async def prepare_generation(self, context):
            events.append(("prepare", context.component_id))
            effective = await self.module_configuration.desired()
            return PreparedGenerationBinding(
                component_id=("wrong.component" if self.mismatch else context.component_id),
                requires=context.requires,
                package_id=context.package_id,
                package_version=context.package_version,
                package_digest=context.package_digest,
                driver=context.driver,
                trust_level=context.trust_level,
                descriptor_digest=context.descriptor_digest,
                route_contract_digest=context.route_contract_digest,
                management_contract_digest=context.management_contract_digest,
                management_ui_digest=context.management_ui_digest,
                config_revision=effective.revision,
                config_payload_id=effective.payload_id,
                config_payload_digest=effective.payload_digest,
                instance_id=f"instance:kernel.runtime:{context.activation_id}",
                worker_incarnation=f"worker:kernel.runtime:{context.activation_id}",
                state_schema=context.state_schema,
                state_contract_digest=context.state_contract_digest,
                capability_grants_digest=context.package_digest,
                data_admission=True,
                control_admission=True,
                storage=context.storage,
            )

        async def start(self):
            assert self.identity is not None
            events.append(("start", "kernel.runtime"))
            self.task = asyncio.create_task(asyncio.Event().wait())

        async def bind_generation(self, identity):
            self.identity = identity
            events.append(("bind", identity.component_id, identity.generation_id))
            if self.fail_bind:
                raise RuntimeError("bind failed")

        def activate_generation(self):
            events.append(("activate", "kernel.runtime"))
            if self.fail_activate:
                raise RuntimeError("activation failed")

        async def quiesce(self):
            events.append(("quiesce", "kernel.runtime"))

        def freeze_owned_admission(self):
            events.append(("freeze-owned", "kernel.runtime"))

        async def drain_owned_work(self):
            events.append(("drain-owned", "kernel.runtime"))

        def open_owned_admission(self):
            events.append(("open-owned", "kernel.runtime"))

        async def close(self):
            events.append(("close", "kernel.runtime"))
            if self.task is not None:
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

        def owned_tasks(self):
            return (self.task,) if self.task is not None else ()

    kernel = SimpleNamespace(_authority=lambda **_kwargs: lease)
    runtime = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=kernel,
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=repository,
    )
    descriptor = RuntimeModuleDescriptor(
        "kernel.runtime",
        (),
        lambda *_: CleanModule(),
        official_artifact("bot"),
        storage=runtime_storage,
        owned_work_lifecycle=OwnedWorkLifecycle.FREEZE_DRAIN,
    )
    registry = RuntimeModuleRegistry(runtime, (descriptor,))
    await registry.prewarm()
    assert registry.states["kernel.runtime"] == ModuleLifecycleState.PREPARED
    with pytest.raises(ForestError) as caught:
        await repository.verify_active_generation(
            component_id="kernel.runtime",
            generation_id=registry.graph_identity.generations["kernel.runtime"],
        )
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    await registry.start()
    route = await repository.verify_active_generation(
        component_id="kernel.runtime",
        generation_id=registry.graph_identity.generations["kernel.runtime"],
    )
    assert route.graph_id == registry.graph_identity.graph_id
    assert registry.graph_identity is not None
    assert registry.generation_bindings["kernel.runtime"].config_revision == 1
    await registry.drain_owned_work_for_replacement("replacement/test")
    assert registry.states["kernel.runtime"] == ModuleLifecycleState.QUIESCENT
    with pytest.raises(ForestError) as caught:
        await repository.verify_active_generation(
            component_id="kernel.runtime",
            generation_id=registry.graph_identity.generations["kernel.runtime"],
        )
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    await registry.resume_owned_work_after_failed_replacement()
    assert registry.states["kernel.runtime"] == ModuleLifecycleState.ACTIVE
    assert await registry.close() == []
    assert events == [
        ("prepare", "kernel.runtime"),
        (
            "bind",
            "kernel.runtime",
            registry.graph_identity.generations["kernel.runtime"],
        ),
        ("start", "kernel.runtime"),
        ("activate", "kernel.runtime"),
        ("open-owned", "kernel.runtime"),
        ("freeze-owned", "kernel.runtime"),
        ("drain-owned", "kernel.runtime"),
        ("quiesce", "kernel.runtime"),
        ("activate", "kernel.runtime"),
        ("open-owned", "kernel.runtime"),
        ("freeze-owned", "kernel.runtime"),
        ("drain-owned", "kernel.runtime"),
        ("quiesce", "kernel.runtime"),
        ("close", "kernel.runtime"),
    ]
    async with store.pool.connection() as conn:
        generation = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.generations").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                )
            )
        ).fetchone()
        graph = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graphs").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                )
            )
        ).fetchone()
    assert generation["config_revision"] == 1 and generation["status"] == "DISPOSED"
    assert graph["state"] == "STOPPED"

    bind_failure_descriptor = replace(
        descriptor,
        factory=lambda *_: CleanModule(fail_bind=True),
    )
    bind_failure_registry = RuntimeModuleRegistry(runtime, (bind_failure_descriptor,))
    with pytest.raises(RuntimeError, match="bind failed"):
        await bind_failure_registry.prepare()
    assert bind_failure_registry.states["kernel.runtime"] == ModuleLifecycleState.STOPPED
    async with store.pool.connection() as conn:
        bind_failure_graph = await (
            await conn.execute(
                sql.SQL("SELECT state FROM {}.runtime_graphs WHERE graph_id=%s").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                ),
                (bind_failure_registry.graph_identity.graph_id,),
            )
        ).fetchone()
    assert bind_failure_graph["state"] == "STOPPED"

    failed_descriptor = replace(
        descriptor,
        factory=lambda *_: CleanModule(fail_activate=True),
    )
    failed_registry = RuntimeModuleRegistry(runtime, (failed_descriptor,))
    with pytest.raises(RuntimeError, match="activation failed"):
        await failed_registry.start()
    assert failed_registry.states["kernel.runtime"] == ModuleLifecycleState.STOPPED
    with pytest.raises(ForestError) as caught:
        await repository.verify_active_generation(
            component_id="kernel.runtime",
            generation_id=failed_registry.graph_identity.generations["kernel.runtime"],
        )
    assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    async with store.pool.connection() as conn:
        failed_graph = await (
            await conn.execute(
                sql.SQL("SELECT state FROM {}.runtime_graphs WHERE graph_id=%s").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                ),
                (failed_registry.graph_identity.graph_id,),
            )
        ).fetchone()
        head = await (
            await conn.execute(
                sql.SQL("SELECT graph_id FROM {}.runtime_graph_head").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                )
            )
        ).fetchone()
    assert failed_graph["state"] == "STOPPED"
    assert head["graph_id"] is None

    mismatch_descriptor = replace(
        descriptor,
        factory=lambda *_: CleanModule(mismatch=True),
    )
    mismatch_registry = RuntimeModuleRegistry(runtime, (mismatch_descriptor,))
    with pytest.raises(ForestError) as caught:
        await mismatch_registry.prepare()
    assert caught.value.code == "RUNTIME_GENERATION_BINDING_MISMATCH"

    first = RuntimeModuleRegistry(runtime, (descriptor,))
    await first.start()
    second = RuntimeModuleRegistry(runtime, (descriptor,))
    await second.start()
    first_graph_id = first.graph_identity.graph_id
    first_generation_id = first.graph_identity.generations["kernel.runtime"]
    second_graph_id = second.graph_identity.graph_id
    second_generation_id = second.graph_identity.generations["kernel.runtime"]

    assert first_graph_id != second_graph_id
    assert await first.close() == []
    assert await repository.verify_active_generation(
        component_id="kernel.runtime",
        generation_id=second_generation_id,
    )
    async with store.pool.connection() as conn:
        graphs = await (
            await conn.execute(
                sql.SQL(
                    "SELECT graph_id,state FROM {}.runtime_graphs "
                    "WHERE graph_id IN (%s,%s) ORDER BY graph_id"
                ).format(sql.Identifier(runtime_storage.spec.namespace)),
                (first_graph_id, second_graph_id),
            )
        ).fetchall()
        generations = await (
            await conn.execute(
                sql.SQL(
                    "SELECT generation_id,status FROM {}.generations "
                    "WHERE generation_id IN (%s,%s) ORDER BY generation_id"
                ).format(sql.Identifier(runtime_storage.spec.namespace)),
                (first_generation_id, second_generation_id),
            )
        ).fetchall()
        head = await (
            await conn.execute(
                sql.SQL("SELECT graph_id FROM {}.runtime_graph_head").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                )
            )
        ).fetchone()
    assert {row["graph_id"]: row["state"] for row in graphs} == {
        first_graph_id: "STOPPED",
        second_graph_id: "ACTIVE",
    }
    assert {row["generation_id"]: row["status"] for row in generations} == {
        first_generation_id: "DISPOSED",
        second_generation_id: "ACTIVE",
    }
    assert head["graph_id"] == second_graph_id
    assert await second.close() == []


@pytest.mark.postgres
async def test_configuration_preview_snapshot_rejects_concurrent_revision_mix(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload", "forest.kernel.payload",
        "forest.kernel-payload.repository/1", "storage/official/kernel_payload.sql",
    )
    runtime_storage = official_storage_bundle(
        "kernel.runtime", "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1", "storage/official/kernel_runtime.sql",
    )
    authorization_storage = official_storage_bundle(
        "kernel.authorization", "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    for bundle in (payload_storage, runtime_storage, authorization_storage):
        await installer.install(
            bundle.spec, bundle.baseline, bundle.package_digest,
            execution_profile=bundle.execution_profile,
        )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    configs = [
        ModuleConfigurationRepository(
            store, component_id=bundle.spec.component_id,
            namespace=bundle.spec.namespace, payloads=payloads,
        )
        for bundle in (runtime_storage, authorization_storage)
    ]
    validation_digest = "sha256:" + "7" * 64
    for index, config in enumerate(configs):
        await config.initialize(
            operation_id=f"preview.init.{index}", value={"version": 1},
            validation_digest=validation_digest,
        )
    plan = await configs[1].plan(
        operation_id="preview.plan.second", expected_desired_revision=1,
        value={"version": 2}, validation_digest=validation_digest,
    )
    with pytest.raises(psycopg.errors.SerializationFailure):
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            assert (await configs[0].desired_checked_in(conn)).revision == 1
            await configs[1].apply(
                operation_id="preview.apply.second", plan_id=plan["plan_id"],
                expected_desired_revision=1,
            )
            await configs[1].desired_checked_in(conn)
    assert (await configs[1].desired_checked()).revision == 2


@pytest.mark.postgres
async def test_module_configuration_and_runtime_publication_are_one_transaction(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    runtime_storage = official_storage_bundle(
        "kernel.runtime",
        "forest.kernel.runtime",
        "forest.kernel-runtime.repository/1",
        "storage/official/kernel_runtime.sql",
    )
    authorization_storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    for storage_bundle in (payload_storage, runtime_storage, authorization_storage):
        await installer.install(
            storage_bundle.spec,
            storage_bundle.baseline,
            storage_bundle.package_digest,
            execution_profile=storage_bundle.execution_profile,
        )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    web_receipts = KernelAuthorizationRepository(
        store, namespace=authorization_storage.spec.namespace
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="kernel.runtime",
        namespace=runtime_storage.spec.namespace,
        payloads=payloads,
    )
    validation_digest = "sha256:" + "7" * 64
    initial = await asyncio.gather(
        configuration.initialize(
            operation_id="configuration.initialize",
            value={"label": "one"},
            validation_digest=validation_digest,
        ),
        configuration.initialize(
            operation_id="configuration.initialize",
            value={"label": "one"},
            validation_digest=validation_digest,
        ),
    )
    assert initial[0] == initial[1]

    plans = await asyncio.gather(
        configuration.plan(
            operation_id="configuration.plan.2",
            expected_desired_revision=1,
            value={"label": "two"},
            validation_digest=validation_digest,
        ),
        configuration.plan(
            operation_id="configuration.plan.2",
            expected_desired_revision=1,
            value={"label": "two"},
            validation_digest=validation_digest,
        ),
    )
    assert plans[0]["plan_id"] == plans[1]["plan_id"]
    with pytest.raises(RuntimeError, match="abort sibling participant"):
        async with store.pool.connection() as conn, conn.transaction():
            staged = await configuration.apply_in(
                conn,
                operation_id="configuration.apply.2",
                plan_id=plans[0]["plan_id"],
                expected_desired_revision=1,
            )
            assert staged.revision == 2
            raise RuntimeError("abort sibling participant")
    assert (await configuration.desired()).revision == 1
    applied = await asyncio.gather(
        configuration.apply(
            operation_id="configuration.apply.2",
            plan_id=plans[0]["plan_id"],
            expected_desired_revision=1,
        ),
        configuration.apply(
            operation_id="configuration.apply.2",
            plan_id=plans[0]["plan_id"],
            expected_desired_revision=1,
        ),
    )
    assert applied[0] == applied[1]
    assert (await configuration.effective(include_value=True)).value == {"label": "one"}
    desired = await configuration.desired(include_value=True)
    assert desired.revision == 2 and desired.value == {"label": "two"}
    pending_plan = await configuration.plan(
        operation_id="configuration.plan.while-pending",
        expected_desired_revision=2,
        value={"label": "three"},
        validation_digest=validation_digest,
    )
    assert pending_plan["expected_desired_revision"] == 2

    runtime = KernelRuntimeRepository(store, runtime_storage.spec.namespace)
    lease = await runtime.claim_host("host-a", "inc-a")
    storage_binding = RuntimeStorageBinding(
        runtime_storage.spec.schema_id,
        runtime_storage.spec.schema_version,
        runtime_storage.spec.namespace,
        runtime_storage.spec.baseline_sha256,
        runtime_storage.package_digest,
        runtime_storage.spec.repository_contract,
        runtime_storage.execution_profile,
    )
    candidate = replace(
        runtime_binding("kernel.runtime", marker="7", storage=storage_binding),
        package_digest=runtime_storage.package_digest,
        config_revision=desired.revision,
        config_payload_id=desired.payload_id,
        config_payload_digest=desired.payload_digest,
    )
    graph = await runtime.prepare_graph(lease, [candidate], str(uuid4()))
    await runtime.publish_graph(
        lease,
        graph,
        configuration_activators={"kernel.runtime": configuration},
    )
    effective = await configuration.effective(include_value=True)
    assert effective.revision == 2 and effective.value == {"label": "two"}

    signer = WebAuthorityTicketAuthority(
        issuer_id="deployment/test",
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: b"w" * 32,
    )
    web_session = SimpleNamespace(
        session_ref="web-session/configuration",
        session_revision=1,
        subject=SimpleNamespace(subject_id="web-subject/admin", role="ADMIN"),
    )
    generation_id = graph.generations["kernel.runtime"]

    async def ticket(operation_id, action_key, body, plan_digest=None):
        return await signer.issue(
            session=web_session,
            target_scope_kind="bot",
            target_scope_id=store.bot_id,
            target_component_id="kernel.runtime",
            target_generation_id=generation_id,
            target_graph_revision=graph.graph_revision,
            target_operation_ref=operation_id,
            action_key=action_key,
            input_digest=digest_json(body),
            plan_digest=plan_digest,
        )

    async def authorize_configuration(
        conn,
        *,
        ticket,
        component_id,
        operation_id,
        action_key,
        input_digest,
        plan_digest,
    ):
        await signer.verify(
            ticket,
            target_scope_kind="bot",
            target_scope_id=store.bot_id,
            target_component_id=component_id,
            target_generation_id=generation_id,
            target_graph_revision=graph.graph_revision,
            target_operation_ref=operation_id,
            action_key=action_key,
            input_digest=input_digest,
            plan_digest=plan_digest,
            required_role="ADMIN",
        )
        route = await runtime.verify_active_generation_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
            admission_lane="CONTROL",
        )
        assert route.graph_revision == graph.graph_revision
        return await web_receipts.accept_web_authority_ticket_in(conn, ticket=ticket)

    plan_v3_body = {"expected_desired_revision": 2, "value": {"label": "three"}}
    plan_v3 = await configuration.plan(
        operation_id="configuration.plan.3",
        expected_desired_revision=2,
        value={"label": "three"},
        validation_digest=validation_digest,
        authorization=await ticket(
            "configuration.plan.3", "configuration.plan", plan_v3_body
        ),
        authorization_guard=authorize_configuration,
    )
    apply_v3_body = {
        "plan_id": plan_v3["plan_id"],
        "expected_desired_revision": 2,
    }
    desired_v3 = await configuration.apply(
        operation_id="configuration.apply.3",
        plan_id=plan_v3["plan_id"],
        expected_desired_revision=2,
        authorization=await ticket(
            "configuration.apply.3",
            "configuration.apply",
            apply_v3_body,
            digest_json({"plan_id": plan_v3["plan_id"]}),
        ),
        authorization_guard=authorize_configuration,
    )
    recovery_body = {"expected_desired_revision": 3, "value": {"label": "two"}}
    recovery_plan = await configuration.plan(
        operation_id="configuration.plan.recovery",
        expected_desired_revision=3,
        value={"label": "two"},
        validation_digest=validation_digest,
        authorization=await ticket(
            "configuration.plan.recovery", "configuration.plan", recovery_body
        ),
        authorization_guard=authorize_configuration,
    )
    async with store.pool.connection() as conn:
        receipt_operations = await (
            await conn.execute(
                sql.SQL(
                    "SELECT target_operation_ref FROM {}.web_authority_receipts "
                    "ORDER BY target_operation_ref"
                ).format(sql.Identifier(authorization_storage.spec.namespace))
            )
        ).fetchall()
    assert [row["target_operation_ref"] for row in receipt_operations] == [
        "configuration.apply.3",
        "configuration.plan.3",
        "configuration.plan.recovery",
    ]
    candidate_v3 = replace(
        runtime_binding("kernel.runtime", marker="8", storage=storage_binding),
        package_digest=runtime_storage.package_digest,
        config_revision=desired_v3.revision,
        config_payload_id=desired_v3.payload_id,
        config_payload_digest=desired_v3.payload_digest,
    )
    graph_v3 = await runtime.prepare_graph(lease, [candidate_v3], str(uuid4()))
    epoch = await payloads.epoch()
    await payloads.forget(
        operation_id="forget.configuration.3",
        payload_ids=(desired_v3.payload_id,),
        expected_deletion_epoch=epoch,
    )
    with pytest.raises(ForestError) as caught:
        await runtime.publish_graph(
            lease,
            graph_v3,
            configuration_activators={"kernel.runtime": configuration},
        )
    assert caught.value.code == "PAYLOAD_REVOKED"
    assert (await configuration.effective()).revision == 2
    async with store.pool.connection() as conn:
        head = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graph_head").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                )
            )
        ).fetchone()
        failed_candidate = await (
            await conn.execute(
                sql.SQL("SELECT state FROM {}.runtime_graphs WHERE graph_id=%s").format(
                    sql.Identifier(runtime_storage.spec.namespace)
                ),
                (graph_v3.graph_id,),
            )
        ).fetchone()
    assert head["graph_id"] == graph.graph_id
    assert failed_candidate["state"] == "PREPARED"
    recovery_apply = {
        "plan_id": recovery_plan["plan_id"],
        "expected_desired_revision": 3,
    }
    recovered = await configuration.apply(
        operation_id="configuration.apply.recovery",
        plan_id=recovery_plan["plan_id"],
        expected_desired_revision=3,
        authorization=await ticket(
            "configuration.apply.recovery", "configuration.apply", recovery_apply,
            digest_json({"plan_id": recovery_plan["plan_id"]}),
        ),
        authorization_guard=authorize_configuration,
    )
    assert recovered.revision == 4
    assert (await configuration.effective()).revision == 2
    candidate_v4 = replace(
        runtime_binding("kernel.runtime", marker="9", storage=storage_binding),
        package_digest=runtime_storage.package_digest,
        config_revision=recovered.revision,
        config_payload_id=recovered.payload_id,
        config_payload_digest=recovered.payload_digest,
    )
    graph_v4 = await runtime.prepare_graph(lease, [candidate_v4], str(uuid4()))
    await runtime.publish_graph(
        lease, graph_v4,
        configuration_activators={"kernel.runtime": configuration},
    )
    assert (await configuration.desired()).revision == 4
    assert (await configuration.effective(include_value=True)).value == {"label": "two"}
    assert (await configuration.effective()).revision == 4


@pytest.mark.postgres
async def test_clean_kernel_bootstrap_publishes_all_required_authorities(clean_authority):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    replay = await bootstrap_kernel(store, installer)
    assert {
        component_id: grant.grant_set_digest for component_id, grant in core.grants.items()
    } == {component_id: grant.grant_set_digest for component_id, grant in replay.grants.items()}
    assert {item.component_id for item in core.descriptors} == {
        "kernel.runtime",
        "kernel.payload",
        "kernel.authorization",
        "kernel.effect",
        "kernel.audit",
    }
    lease = await core.runtime.claim_host("host-a", "inc-a")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    with pytest.raises(ForestError) as missing_control:
        RuntimeModuleRegistry(host, core.descriptors)
    assert missing_control.value.code == "RUNTIME_MODULE_RETENTION_DEPENDENCY_MISSING"
    registry = RuntimeModuleRegistry(host, _isolated_runtime_descriptors(*core.descriptors))
    await registry.start()
    assert set(registry.states.values()) == {ModuleLifecycleState.ACTIVE}
    authorization_module = registry.get("kernel.authorization")
    authorization_configuration = await authorization_module.management_contract.read()
    assert authorization_configuration["desired"]["value"] == {
        "protocol": "forest.web-operation-policy/1",
        "rules": [],
    }
    authorization_descriptor = next(
        item for item in core.descriptors if item.component_id == "kernel.authorization"
    )
    assert [item.contribution_id for item in authorization_descriptor.management] == [
        "web-operation-policy"
    ]
    graph = registry.graph_identity
    assert graph is not None and set(graph.generations) == {
        "kernel.runtime",
        "kernel.payload",
        "kernel.authorization",
        "kernel.effect",
        "kernel.audit",
    }
    namespace = next(
        descriptor.storage.spec.namespace
        for descriptor in core.descriptors
        if descriptor.component_id == "kernel.runtime"
    )
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT component_id,capability_grants_digest,status "
                    "FROM {}.generations ORDER BY component_id"
                ).format(sql.Identifier(namespace))
            )
        ).fetchall()
        head = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graph_head").format(sql.Identifier(namespace))
            )
        ).fetchone()
    assert head["graph_id"] == graph.graph_id
    assert {row["component_id"]: row["capability_grants_digest"] for row in rows} == {
        component_id: grant.grant_set_digest for component_id, grant in core.grants.items()
    }
    assert all(row["status"] == "ACTIVE" for row in rows)
    assert await registry.close() == []
    async with store.pool.connection() as conn:
        statuses = await (
            await conn.execute(
                sql.SQL("SELECT status FROM {}.generations").format(sql.Identifier(namespace))
            )
        ).fetchall()
    assert {row["status"] for row in statuses} == {"DISPOSED"}


@pytest.mark.postgres
async def test_runtime_registry_transfers_unaffected_generations_without_restarting_them(
    clean_authority,
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    lease = await core.runtime.claim_host("host-local", "inc-local")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    descriptors = _isolated_runtime_descriptors(*core.descriptors)
    previous = RuntimeModuleRegistry(host, descriptors)
    await previous.start()
    original_graph = previous.graph_identity
    original_instances = dict(previous.instances)

    runtime_descriptor = next(
        item for item in core.descriptors if item.component_id == "kernel.runtime"
    )

    def fail_target(runtime, dependencies):
        instance = runtime_descriptor.factory(runtime, dependencies)

        async def fail_start():
            raise RuntimeError("candidate prewarm failed")

        instance.start = fail_start
        return instance

    failed = RuntimeModuleRegistry(
        host,
        tuple(
            replace(item, factory=fail_target)
            if item.component_id == "kernel.runtime"
            else item
            for item in descriptors
        ),
    )
    assert failed.plan_retained_generations(previous, "kernel.runtime") == (
        "kernel.runtime",
    )
    with pytest.raises(RuntimeError, match="candidate prewarm failed"):
        await failed.prewarm()
    assert await failed.close() == []
    assert (await core.runtime.status())["graph_id"] == original_graph.graph_id
    assert previous.instances == original_instances

    candidate = RuntimeModuleRegistry(host, descriptors)
    preview = candidate.preview_graph_transition(previous, target_component_id="kernel.runtime")
    assert preview.replace == ("kernel.runtime",)
    assert preview.retain == tuple(
        component_id for component_id in previous.order if component_id != "kernel.runtime"
    )
    assert preview.remove == preview.add == ()
    assert preview.drain == preview.prepare == ("kernel.runtime",)
    assert candidate.instances == {}
    assert candidate._retained_from is None
    affected = candidate.plan_retained_generations(previous, "kernel.runtime")
    assert affected == ("kernel.runtime",)
    await candidate.prewarm()
    await previous.drain_owned_work_for_replacement(
        candidate.activation_id, components=affected
    )
    await candidate.start()
    candidate.accept_retained_ownership()
    assert await previous.close() == []

    assert candidate.graph_identity.graph_revision > original_graph.graph_revision
    assert candidate.get("kernel.runtime") is not original_instances["kernel.runtime"]
    assert candidate.graph_identity.generations["kernel.runtime"] != (
        original_graph.generations["kernel.runtime"]
    )
    for component_id in original_graph.generations:
        if component_id == "kernel.runtime":
            continue
        assert candidate.get(component_id) is original_instances[component_id]
        assert candidate.graph_identity.generations[component_id] == (
            original_graph.generations[component_id]
        )
    assert await candidate.close() == []


@pytest.mark.postgres
async def test_runtime_graph_adds_and_removes_a_package_without_restarting_kernel(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    lease = await core.runtime.claim_host("host-member-change", "inc-member-change")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    descriptors = _isolated_runtime_descriptors(*core.descriptors)
    first = RuntimeModuleRegistry(host, descriptors)
    await first.start()
    original_instances = dict(first.instances)
    original_generations = dict(first.graph_identity.generations)
    counter = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/counter"))),
        PackageCatalog(tmp_path / "member-change-packages"),
        store,
        installer,
        kernel=core,
        initial_configuration={"label": "optional", "multiplier": 1},
    )

    added = RuntimeModuleRegistry(
        host, descriptors + _isolated_runtime_descriptors(counter.descriptor)
    )
    add_plan = added.preview_graph_transition(first, target_component_id="forest.counter")
    assert add_plan.replace == add_plan.remove == add_plan.drain == ()
    assert add_plan.retain == tuple(first.order)
    assert add_plan.add == add_plan.prepare == ("forest.counter",)
    controller = object.__new__(BotHost)
    controller._replacement_lock = asyncio.Lock()
    controller._closing = False
    controller.registry = first
    async def admit_capacity(_capacity):
        return None
    controller._admit_worker_capacity = admit_capacity
    selection = ComponentSelectionSnapshot(0, (), digest_json({
        "protocol": "forest.component-selection-snapshot/1",
        "revision": 0,
        "entries": [],
    }))

    @asynccontextmanager
    async def publication_fence(**_kwargs):
        yield selection

    controller.selection_reader = SimpleNamespace(publication_fence=publication_fence)

    async def assemble_added(_activation_id=None):
        return BotGraphCandidate(added, selection, (), controller.selection_reader)

    controller._assemble_candidate = assemble_added
    assert await controller.replace_runtime_graph(
        target_component_id="forest.counter"
    ) is added
    assert (await core.runtime.status())["graph_id"] == added.graph_identity.graph_id
    for component_id, generation_id in original_generations.items():
        assert added.get(component_id) is original_instances[component_id]
        assert added.graph_identity.generations[component_id] == generation_id
    counter_instance = added.get("forest.counter")

    removed = RuntimeModuleRegistry(host, descriptors)
    remove_plan = removed.preview_graph_transition(added, target_component_id="forest.counter")
    assert remove_plan.replace == remove_plan.add == remove_plan.prepare == ()
    assert remove_plan.retain == tuple(removed.order)
    assert remove_plan.remove == remove_plan.drain == ("forest.counter",)

    async def assemble_removed(_activation_id=None):
        return BotGraphCandidate(removed, selection, (), controller.selection_reader)

    controller._assemble_candidate = assemble_removed
    assert await controller.replace_runtime_graph(
        target_component_id="forest.counter"
    ) is removed
    assert counter_instance.state.value == "STOPPED"
    assert (await core.runtime.status())["graph_id"] == removed.graph_identity.graph_id
    for component_id, generation_id in original_generations.items():
        assert removed.get(component_id) is original_instances[component_id]
        assert removed.graph_identity.generations[component_id] == generation_id
    with pytest.raises(ForestError) as missing:
        await core.runtime.verify_active_generation(
            component_id="forest.counter",
            generation_id=added.graph_identity.generations["forest.counter"],
        )
    assert missing.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    assert await removed.close() == []


@pytest.mark.postgres
async def test_installed_archive_uses_separate_configuration_schema_and_hot_reloads(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    package_root = tmp_path / "installed-source"
    copytree(Path(__file__).parent / "fixtures" / "routing_component", package_root)
    baseline = b"CREATE TABLE plugin_records (record_id text PRIMARY KEY);"
    (package_root / "schema").mkdir()
    (package_root / "schema" / "records.sql").write_bytes(baseline)
    with (package_root / "module.toml").open("a", encoding="utf-8") as manifest:
        manifest.write(
            "\n[components.storage]\n"
            'schema_id = "tests.routing-component.records"\n'
            'schema_version = "1.0.0"\n'
            'baseline = "schema/records.sql"\n'
            f'baseline_sha256 = "{hashlib.sha256(baseline).hexdigest()}"\n'
            'scope = "bot"\n'
            'repository_contract = "tests.routing-component.repository/1"\n'
            'execution_profile = "declarative"\n'
        )
    source_catalog = PackageCatalog(tmp_path / "source-packages")
    source = source_catalog.build(package_root)
    catalog = PackageCatalog(tmp_path / "installed-packages")
    installed_snapshot = catalog.import_archive(source_catalog.export_archive(source.digest))
    approved = ("event.publish:tests.ready", "job.schedule:tests.echo")
    async with store.pool.connection() as conn:
        before = await (
            await conn.execute(
                "SELECT count(*) AS n FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND component_id=%s",
                (store.bot_id, "tests.routing-component"),
            )
        ).fetchone()
    with pytest.raises(ForestError) as preview_unconfigured:
        await inspect_installed_package(
            installed_snapshot.digest, catalog, store, kernel=core,
            approved_capabilities=approved,
            expected_manifest_digest=digest_json(installed_snapshot.manifest),
            expected_configuration_revision=1,
        )
    assert preview_unconfigured.value.code == "PACKAGE_CONFIGURATION_REQUIRED"
    async with store.pool.connection() as conn:
        after = await (
            await conn.execute(
                "SELECT count(*) AS n FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND component_id=%s",
                (store.bot_id, "tests.routing-component"),
            )
        ).fetchone()
    assert after == before
    with pytest.raises(ForestError) as not_configured:
        await bootstrap_installed_package(
            installed_snapshot.digest, catalog, store, installer,
            kernel=core, initial_configuration=None,
            approved_capabilities=approved,
        )
    assert not_configured.value.code == "PACKAGE_CONFIGURATION_REQUIRED"
    async with store.pool.connection() as conn:
        after_failed_bootstrap = await (
            await conn.execute(
                "SELECT count(*) AS n FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND component_id=%s",
                (store.bot_id, "tests.routing-component"),
            )
        ).fetchone()
    assert after_failed_bootstrap == before
    installed = await bootstrap_installed_package(
        installed_snapshot.digest,
        catalog,
        store,
        installer,
        kernel=core,
        initial_configuration={},
        approved_capabilities=approved,
    )
    inspected = await inspect_installed_package(
        installed_snapshot.digest, catalog, store, kernel=core,
        approved_capabilities=approved,
        expected_manifest_digest=digest_json(installed_snapshot.manifest),
        expected_configuration_revision=1,
    )
    assert inspected.component_id == "tests.routing-component"
    assert inspected.package_digest == installed_snapshot.digest
    assert inspected.manifest_digest == digest_json(installed_snapshot.manifest)
    desired_fact = await installed.configuration.desired()
    assert inspected.configuration_revision == desired_fact.revision == 1
    assert inspected.configuration_payload_id == desired_fact.payload_id
    assert inspected.configuration_payload_digest == desired_fact.payload_digest
    assert inspected.configuration_schema_digest == digest_json(
        installed_snapshot.schema(installed_snapshot.component["config_schema"])
    )
    assert inspected.storage_digest == installed.descriptor.storage.package_digest
    with pytest.raises(ForestError) as stale_configuration:
        await inspect_installed_package(
            installed_snapshot.digest, catalog, store, kernel=core,
            approved_capabilities=approved,
            expected_manifest_digest=digest_json(installed_snapshot.manifest),
            expected_configuration_revision=2,
        )
    assert stale_configuration.value.code == "MODULE_CONFIGURATION_REVISION_CONFLICT"
    assert installed.descriptor.artifact.trust_level == "installed"
    assert installed.descriptor.storage is not None
    assert installed.descriptor.storage.execution_profile == "declarative"
    assert installed.descriptor.storage.package_digest != installed_snapshot.digest
    assert installed.configuration.namespace != installed.descriptor.storage.spec.namespace
    async with store.pool.connection() as conn:
        config_bundle = await (
            await conn.execute(
                "SELECT component_id,execution_profile FROM forest_kernel.schema_bundles "
                "WHERE bot_id=%s AND namespace=%s",
                (store.bot_id, installed.configuration.namespace),
            )
        ).fetchone()
    assert config_bundle["component_id"] == "tests.routing-component"
    assert config_bundle["execution_profile"] == "official"
    routing = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/routing_graph"))),
        catalog,
        store,
        installer,
        kernel=core,
        initial_configuration={"protocol": "forest.routing.graph/2"},
    )
    lease = await core.runtime.claim_host("host-installed-package", "inc-installed-package")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    isolated_core = replace(
        core, descriptors=_isolated_runtime_descriptors(*core.descriptors)
    )
    routing_descriptor = _isolated_runtime_descriptors(routing.descriptor)[0]
    descriptors = isolated_core.descriptors + (routing_descriptor, installed.descriptor)
    first = RuntimeModuleRegistry(host, descriptors)
    await first.start()
    first_graph = first.graph_identity
    core_instances = {item: first.get(item) for item in first_graph.generations if item.startswith("kernel.")}

    updated_root = tmp_path / "updated-source"
    copytree(package_root, updated_root)
    updated_manifest = updated_root / "module.toml"
    updated_manifest.write_text(
        updated_manifest.read_text(encoding="utf-8").replace(
            'version = "1.0.0"', 'version = "1.0.1"', 1
        ),
        encoding="utf-8",
    )
    updated_code = updated_root / "python" / "routing_component.py"
    updated_code.write_text(
        updated_code.read_text(encoding="utf-8") + "\nGENERATION_MARKER = 'v2'\n",
        encoding="utf-8",
    )
    updated_source = source_catalog.build(updated_root)
    updated_snapshot = catalog.import_archive(
        source_catalog.export_archive(updated_source.digest)
    )
    planned_artifact, planned_descriptor = await compile_installed_candidate_descriptor(
        updated_snapshot.digest, catalog, store, kernel=core,
        approved_capabilities=approved,
        expected_manifest_digest=digest_json(updated_snapshot.manifest),
        expected_configuration_revision=1,
    )
    assert planned_artifact.package_digest == updated_snapshot.digest
    assert planned_descriptor.factory is None
    planned_registry = RuntimeModuleRegistry(
        host, isolated_core.descriptors + (routing_descriptor, planned_descriptor)
    )
    planned_impact = planned_registry.preview_graph_transition(
        first, target_component_id="tests.routing-component"
    )
    def pinned_selection(snapshot):
        entry = ComponentSelectionEntry(
            "tests.routing-component", "installed", snapshot.digest,
            digest_json(snapshot.manifest), approved, True, 1,
        )
        frozen = {
            "protocol": "forest.component-selection-snapshot/1",
            "revision": 1,
            "entries": [entry.as_dict()],
        }
        return ComponentSelectionSnapshot(1, (entry,), digest_json(frozen))

    selection = pinned_selection(updated_snapshot)
    selected_preview = await preview_selected_graph(
        first, selection, (routing_descriptor,), catalog, store,
        kernel=isolated_core,
        target_component_id="tests.routing-component",
        expected_graph_revision=first_graph.graph_revision,
        expected_configuration_revisions={"tests.routing-component": 1},
    )
    assert selected_preview.impact == planned_impact
    assert planned_artifact in selected_preview.packages
    assert selected_preview.plan_digest.startswith("sha256:")
    repeated_preview = await preview_selected_graph(
        first, selection, (routing_descriptor,), catalog, store,
        kernel=isolated_core,
        target_component_id="tests.routing-component",
        expected_graph_revision=first_graph.graph_revision,
        expected_configuration_revisions={"tests.routing-component": 1},
    )
    assert repeated_preview.plan_digest == selected_preview.plan_digest
    schema_changed_root = tmp_path / "schema-changed-source"
    copytree(updated_root, schema_changed_root)
    changed_baseline = (
        baseline + b"\nCREATE TABLE extra_records (record_id text PRIMARY KEY);"
    )
    (schema_changed_root / "schema" / "records.sql").write_bytes(changed_baseline)
    changed_manifest = schema_changed_root / "module.toml"
    changed_manifest.write_text(
        changed_manifest.read_text(encoding="utf-8")
        .replace('version = "1.0.1"', 'version = "1.0.2"', 1)
        .replace('schema_version = "1.0.0"', 'schema_version = "1.0.1"', 1)
        .replace(
            f'baseline_sha256 = "{hashlib.sha256(baseline).hexdigest()}"',
            f'baseline_sha256 = "{hashlib.sha256(changed_baseline).hexdigest()}"',
            1,
        ),
        encoding="utf-8",
    )
    changed_source = source_catalog.build(schema_changed_root)
    changed_snapshot = catalog.import_archive(
        source_catalog.export_archive(changed_source.digest)
    )
    with pytest.raises(ForestError) as schema_change:
        await preview_selected_graph(
            first, pinned_selection(changed_snapshot), (routing_descriptor,),
            catalog, store, kernel=isolated_core,
            target_component_id="tests.routing-component",
            expected_graph_revision=first_graph.graph_revision,
            expected_configuration_revisions={"tests.routing-component": 1},
        )
    assert schema_change.value.code == "SCHEMA_BUNDLE_UPGRADE_REQUIRED"
    assert (await core.runtime.status())["graph_revision"] == first_graph.graph_revision
    prior_selection = pinned_selection(installed_snapshot)
    prior_preview = await preview_selected_graph(
        first, prior_selection, (routing_descriptor,), catalog, store,
        kernel=isolated_core,
        target_component_id="tests.routing-component",
        expected_graph_revision=first_graph.graph_revision,
        expected_configuration_revisions={"tests.routing-component": 1},
    )
    assert prior_preview.plan_digest != selected_preview.plan_digest
    with pytest.raises(ForestError) as not_executable:
        await planned_registry.prewarm()
    assert not_executable.value.code == "RUNTIME_MODULE_PLAN_NOT_EXECUTABLE"
    assert (await core.runtime.status())["graph_revision"] == first_graph.graph_revision
    updated = await bootstrap_installed_package(
        updated_snapshot.digest,
        catalog,
        store,
        installer,
        kernel=core,
        initial_configuration=None,
        approved_capabilities=(
            "event.publish:tests.ready",
            "job.schedule:tests.echo",
        ),
    )
    assert updated.snapshot.digest != installed.snapshot.digest
    assert (await updated.configuration.desired(include_value=True)).value == {}
    assert updated.descriptor.storage.package_digest == installed.descriptor.storage.package_digest
    candidate = RuntimeModuleRegistry(
        host, isolated_core.descriptors + (routing_descriptor, updated.descriptor)
    )
    assert candidate.preview_graph_transition(
        first, target_component_id="tests.routing-component"
    ) == planned_impact
    affected = candidate.plan_retained_generations(first, "tests.routing-component")
    await candidate.prewarm()
    await first.drain_owned_work_for_replacement(
        candidate.activation_id,
        components=affected if len(affected) < len(first.order) else None,
    )
    await candidate.start()
    candidate.accept_retained_ownership()
    assert await first.close() == []
    assert candidate.graph_identity.generations["tests.routing-component"] != (
        first_graph.generations["tests.routing-component"]
    )
    assert candidate.get("tests.routing-component").snapshot.digest == updated_snapshot.digest
    for component_id, instance in core_instances.items():
        assert candidate.get(component_id) is instance
        assert candidate.graph_identity.generations[component_id] == first_graph.generations[component_id]
    assert await candidate.close() == []


@pytest.mark.postgres
async def test_post_publication_activation_failure_retires_retained_generations(
    clean_authority,
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    lease = await core.runtime.claim_host("host-activation-failure", "inc-activation-failure")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    descriptors = _isolated_runtime_descriptors(*core.descriptors)
    previous = RuntimeModuleRegistry(host, descriptors)
    await previous.start()
    source_graph = previous.graph_identity
    runtime_descriptor = next(
        item for item in core.descriptors if item.component_id == "kernel.runtime"
    )

    def fail_activation(runtime, dependencies):
        instance = runtime_descriptor.factory(runtime, dependencies)

        def activate_generation():
            raise RuntimeError("local activation failed")

        instance.activate_generation = activate_generation
        return instance

    candidate = RuntimeModuleRegistry(
        host,
        tuple(
            replace(item, factory=fail_activation)
            if item.component_id == "kernel.runtime"
            else item
            for item in descriptors
        ),
    )
    affected = candidate.plan_retained_generations(previous, "kernel.runtime")
    await candidate.prewarm()
    await previous.drain_owned_work_for_replacement(
        candidate.activation_id, components=affected
    )
    with pytest.raises(RuntimeError, match="local activation failed"):
        await candidate.start()

    assert candidate.publication_committed
    assert (await core.runtime.status())["graph_id"] is None
    assert (await core.runtime.graph_for_activation(source_graph.activation_id))[
        "state"
    ] == "RETIRED"
    with pytest.raises(ForestError) as rejected:
        await previous.resume_owned_work_after_failed_replacement()
    assert rejected.value.code == "RUNTIME_GRAPH_NOT_CURRENT"
    assert await candidate.close() == []
    assert await previous.close() == []


@pytest.mark.postgres
async def test_graph_replacement_queue_is_revision_fenced_claimed_and_idempotent(
    clean_authority,
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    lease = await core.runtime.claim_host("host-reload", "inc-reload")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    descriptors = _isolated_runtime_descriptors(*core.descriptors)
    current = RuntimeModuleRegistry(host, descriptors)
    await current.start()
    graph = current.graph_identity

    with pytest.raises(ForestError) as missing_target:
        await core.runtime.request_graph_replacement(
            lease,
            operation_id="reload/missing-target",
            expected_graph_revision=graph.graph_revision,
            requested_by="qq:42",
            target_component_id="example.absent",
        )
    assert missing_target.value.code == "RUNTIME_MODULE_NOT_FOUND"
    with pytest.raises(ForestError) as not_queued:
        await core.runtime.graph_replacement("reload/missing-target")
    assert not_queued.value.code == "RUNTIME_REPLACEMENT_NOT_FOUND"

    requested = await core.runtime.request_graph_replacement(
        lease,
        operation_id="reload/one",
        expected_graph_revision=graph.graph_revision,
        requested_by="qq:42",
    )
    assert requested["state"] == "QUEUED"
    assert await core.runtime.request_graph_replacement(
        lease,
        operation_id="reload/one",
        expected_graph_revision=graph.graph_revision,
        requested_by="qq:42",
    ) == requested
    with pytest.raises(ForestError) as caught:
        await core.runtime.request_graph_replacement(
            lease,
            operation_id="reload/one",
            expected_graph_revision=graph.graph_revision,
            requested_by="qq:other",
        )
    assert caught.value.code == "RUNTIME_REPLACEMENT_CONFLICT"

    claim = await core.runtime.claim_graph_replacement(lease)
    assert claim["operation_id"] == "reload/one"
    assert claim["state"] == "RUNNING"
    assert claim["attempt_count"] == 1
    assert await core.runtime.claim_graph_replacement(lease) is None

    candidate = RuntimeModuleRegistry(
        host,
        descriptors,
        activation_id=claim["activation_id"],
    )
    await candidate.start()
    settled = await core.runtime.finish_graph_replacement(
        lease,
        operation_id="reload/one",
        graph_id=candidate.graph_identity.graph_id,
        graph_revision=candidate.graph_identity.graph_revision,
    )
    assert settled["state"] == "SUCCEEDED"
    assert await core.runtime.finish_graph_replacement(
        lease,
        operation_id="reload/one",
        graph_id=candidate.graph_identity.graph_id,
        graph_revision=candidate.graph_identity.graph_revision,
    ) == settled
    assert (await core.runtime.graph_replacement("reload/one"))["state"] == "SUCCEEDED"
    assert await core.runtime.request_graph_replacement(
        lease,
        operation_id="reload/one",
        expected_graph_revision=graph.graph_revision,
        requested_by="qq:42",
    ) == settled

    frozen = await core.runtime.request_graph_replacement(
        lease,
        operation_id="selection/add-counter",
        expected_graph_revision=candidate.graph_identity.graph_revision,
        requested_by="web:admin",
        target_component_id="example.counter",
        selection_revision=1,
        selection_digest="sha256:" + "a" * 64,
    )
    assert frozen["state"] == "QUEUED"
    assert frozen["selection_revision"] == 1
    assert frozen["selection_digest"] == "sha256:" + "a" * 64
    assert await core.runtime.request_graph_replacement(
        lease,
        operation_id="selection/add-counter",
        expected_graph_revision=candidate.graph_identity.graph_revision,
        requested_by="web:admin",
        target_component_id="example.counter",
        selection_revision=1,
        selection_digest="sha256:" + "a" * 64,
    ) == frozen
    with pytest.raises(ForestError) as changed_selection:
        await core.runtime.request_graph_replacement(
            lease,
            operation_id="selection/add-counter",
            expected_graph_revision=candidate.graph_identity.graph_revision,
            requested_by="web:admin",
            target_component_id="example.counter",
            selection_revision=2,
            selection_digest="sha256:" + "b" * 64,
        )
    assert changed_selection.value.code == "RUNTIME_REPLACEMENT_CONFLICT"
    frozen_claim = await core.runtime.claim_graph_replacement(lease)
    assert frozen_claim["operation_id"] == "selection/add-counter"
    await core.runtime.fail_graph_replacement(
        lease, operation_id="selection/add-counter", error_code="COMPONENT_SELECTION_CHANGED"
    )

    targeted = await core.runtime.request_graph_replacement(
        lease,
        operation_id="reload/two",
        expected_graph_revision=candidate.graph_identity.graph_revision,
        requested_by="qq:42",
        target_component_id="kernel.runtime",
    )
    assert targeted["target_component_id"] == "kernel.runtime"
    assert (await core.runtime.status())["latest_replacement"]["target_component_id"] == (
        "kernel.runtime"
    )
    with pytest.raises(ForestError) as retargeted:
        await core.runtime.request_graph_replacement(
            lease,
            operation_id="reload/two",
            expected_graph_revision=candidate.graph_identity.graph_revision,
            requested_by="qq:42",
            target_component_id=None,
        )
    assert retargeted.value.code == "RUNTIME_REPLACEMENT_CONFLICT"
    left, right = await asyncio.gather(
        core.runtime.claim_graph_replacement(lease),
        core.runtime.claim_graph_replacement(lease),
    )
    claims = [item for item in (left, right) if item is not None]
    assert len(claims) == 1
    failed = await core.runtime.fail_graph_replacement(
        lease,
        operation_id="reload/two",
        error_code="TEST_REPLACEMENT_FAILED",
    )
    assert failed["state"] == "FAILED"
    assert await core.runtime.fail_graph_replacement(
        lease,
        operation_id="reload/two",
        error_code="TEST_REPLACEMENT_FAILED",
    ) == failed
    assert await candidate.close() == []
    assert await current.close() == []


@pytest.mark.postgres
async def test_clean_kernel_authorization_fences_exact_tickets_and_revocation(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    row = await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    assert row["component_id"] == "kernel.authorization"
    lowered = storage.baseline.lower()
    for forbidden in (b"agent", b"onebot", b"memory", b"binding", b"job", b"web_session"):
        assert forbidden not in lowered

    authority = KernelAuthorizationRepository(store, namespace=storage.spec.namespace)
    grant_args = {
        "grantee_kind": "platform.user",
        "grantee_id": "qq/1234",
        "entries": (CapabilityGrant("module.reload", "component", "example.worker"),),
        "expected_policy_revision": 1,
        "audience_component_id": "component.worker-host",
        "audience_generation_id": "generation/one",
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=2),
    }
    left, right = await asyncio.gather(
        authority.create_grant_set(**grant_args),
        authority.create_grant_set(**grant_args),
    )
    assert left == right

    input_digest = "sha256:" + "1" * 64
    ticket_args = {
        "issuer_kind": "deployment.gateway",
        "issuer_id": "gateway/one",
        "subject_kind": "platform.user",
        "subject_id": "qq/1234",
        "action": "module.reload",
        "resource_kind": "component",
        "resource_id": "example.worker",
        "input_digest": input_digest,
        "plan_digest": None,
        "grant_set": left,
    }
    consumed_expiry = datetime.now(timezone.utc) + timedelta(seconds=20)
    pending_expiry = datetime.now(timezone.utc) + timedelta(seconds=20)
    consumed_ticket = await authority.issue_ticket(
        ticket_id="ticket/consumed",
        operation_id="operation/consumed",
        expires_at=consumed_expiry,
        **ticket_args,
    )
    pending_ticket = await authority.issue_ticket(
        ticket_id="ticket/pending",
        operation_id="operation/pending",
        expires_at=pending_expiry,
        **ticket_args,
    )
    assert consumed_ticket["grant_set_digest"] == left.grant_set_digest
    assert pending_ticket["policy_revision"] == 1

    consume_args = {
        "ticket_id": "ticket/consumed",
        "operation_id": "operation/consumed",
        "action": "module.reload",
        "resource_kind": "component",
        "resource_id": "example.worker",
        "input_digest": input_digest,
        "plan_digest": None,
        "audience_component_id": "component.worker-host",
        "audience_generation_id": "generation/one",
    }
    receipt = await authority.consume_ticket(**consume_args)
    assert await authority.consume_ticket(**consume_args) == receipt
    revocation = await authority.revoke(
        operation_id="revoke/one",
        selector_kind="principal",
        selector_id="qq/1234",
        expected_policy_revision=1,
    )
    assert revocation["policy_revision"] == 2
    assert (
        await authority.revoke(
            operation_id="revoke/one",
            selector_kind="principal",
            selector_id="qq/1234",
            expected_policy_revision=1,
        )
        == revocation
    )
    assert await authority.consume_ticket(**consume_args) == receipt
    assert (
        await authority.issue_ticket(
            ticket_id="ticket/pending",
            operation_id="operation/pending",
            expires_at=pending_expiry,
            **ticket_args,
        )
        == pending_ticket
    )
    with pytest.raises(ForestError) as caught:
        await authority.consume_ticket(
            **{**consume_args, "ticket_id": "ticket/pending", "operation_id": "operation/pending"}
        )
    assert caught.value.code == "POLICY_CONFLICT"

    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("DELETE FROM {}.tickets WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )


@pytest.mark.postgres
async def test_web_authority_receipt_is_immutable_idempotent_and_rolls_back(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    authority = KernelAuthorizationRepository(store, namespace=storage.spec.namespace)
    signer = WebAuthorityTicketAuthority(
        issuer_id="deployment/test",
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: b"w" * 32,
    )
    session = SimpleNamespace(
        session_ref="web-session/test",
        session_revision=1,
        subject=SimpleNamespace(subject_id="web-subject/admin", role="ADMIN"),
    )

    async def issue():
        return await signer.issue(
            session=session,
            target_scope_kind="bot",
            target_scope_id="bot-bundle",
            target_component_id="agent.runtime",
            target_generation_id="generation/agent/1",
            target_graph_revision=1,
            target_operation_ref="web:operation/configuration",
            action_key="configuration.apply",
            input_digest="sha256:" + "a" * 64,
            plan_digest="sha256:" + "b" * 64,
        )

    first = await issue()
    with pytest.raises(RuntimeError):
        async with store.pool.connection() as conn, conn.transaction():
            await authority.accept_web_authority_ticket_in(conn, ticket=first)
            raise RuntimeError("rollback")
    async with store.pool.connection() as conn:
        count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS value FROM {}.web_authority_receipts").format(
                    sql.Identifier(storage.spec.namespace)
                )
            )
        ).fetchone()
    assert count["value"] == 0

    receipt = await authority.accept_web_authority_ticket(ticket=first)
    assert receipt["policy_revision"] == 1
    assert receipt["target_operation_ref"] == "web:operation/configuration"
    renewed = await issue()
    assert renewed.ticket_id != first.ticket_id
    assert await authority.accept_web_authority_ticket(ticket=renewed) == receipt

    with pytest.raises(ForestError) as conflict:
        await authority.accept_web_authority_ticket(
            ticket=replace(renewed, input_digest="sha256:" + "c" * 64)
        )
    assert conflict.value.code == "WEB_AUTHORITY_RECEIPT_CONFLICT"

    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("DELETE FROM {}.web_authority_receipts WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )


@pytest.mark.postgres
async def test_kernel_authorization_delegates_only_explicit_capabilities_to_exact_generation(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    authority = KernelAuthorizationRepository(store, namespace=storage.spec.namespace)
    delegated = await authority.create_grant_set(
        grantee_kind="component",
        grantee_id="agent.runtime",
        entries=(CapabilityGrant("tool.invoke", "bot", store.bot_id, delegable=True),),
        expected_policy_revision=1,
    )
    digest = "sha256:" + "a" * 64
    expiry = datetime.now(timezone.utc) + timedelta(seconds=20)
    ticket = await authority.issue_ticket(
        ticket_id="ticket/tool/one",
        issuer_kind="kernel",
        issuer_id="kernel.authorization",
        subject_kind="component",
        subject_id="agent.runtime",
        action="tool.invoke",
        resource_kind="bot",
        resource_id=store.bot_id,
        operation_id="effect/tool/one",
        input_digest=digest,
        plan_digest=None,
        grant_set=delegated,
        expires_at=expiry,
        audience_component_id="tool.weather",
        audience_generation_id="generation/weather-one",
    )
    assert ticket["audience_component_id"] == "tool.weather"
    assert ticket["audience_generation_id"] == "generation/weather-one"
    receipt = await authority.consume_ticket(
        ticket_id=ticket["ticket_id"],
        operation_id="effect/tool/one",
        action="tool.invoke",
        resource_kind="bot",
        resource_id=store.bot_id,
        input_digest=digest,
        plan_digest=None,
        audience_component_id="tool.weather",
        audience_generation_id="generation/weather-one",
    )
    assert receipt["policy_revision"] == 1

    fixed = await authority.create_grant_set(
        grantee_kind="component",
        grantee_id="agent.runtime",
        entries=(CapabilityGrant("provider.invoke", "bot", store.bot_id),),
        expected_policy_revision=1,
    )
    with pytest.raises(ForestError) as caught:
        await authority.issue_ticket(
            ticket_id="ticket/provider/wrong-delegation",
            issuer_kind="kernel",
            issuer_id="kernel.authorization",
            subject_kind="component",
            subject_id="agent.runtime",
            action="provider.invoke",
            resource_kind="bot",
            resource_id=store.bot_id,
            operation_id="provider/wrong-delegation",
            input_digest=digest,
            plan_digest=None,
            grant_set=fixed,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=20),
            audience_component_id="provider.chat-completions",
            audience_generation_id="generation/provider-one",
        )
    assert caught.value.code == "CAPABILITY_DENIED"

    concurrent_expiry = datetime.now(timezone.utc) + timedelta(seconds=20)
    concurrent_values = dict(
        issuer_kind="agent.policy",
        issuer_id="agent.runtime/tool-authorization",
        subject_kind="component",
        subject_id="agent.runtime",
        action="tool.invoke",
        resource_kind="bot",
        resource_id=store.bot_id,
        operation_id="effect/tool/concurrent-tickets",
        input_digest="sha256:" + "9" * 64,
        plan_digest="sha256:" + "8" * 64,
        grant_set=delegated,
        expires_at=concurrent_expiry,
        audience_component_id="tool.weather",
        audience_generation_id="generation/weather-one",
    )
    await asyncio.gather(
        authority.issue_ticket(
            ticket_id="ticket/tool/concurrent/one",
            issuance_operation_id="ticket-issue/tool/concurrent/one",
            **concurrent_values,
        ),
        authority.issue_ticket(
            ticket_id="ticket/tool/concurrent/two",
            issuance_operation_id="ticket-issue/tool/concurrent/two",
            **concurrent_values,
        ),
    )

    async def consume_concurrent(ticket_id):
        try:
            await authority.consume_ticket(
                ticket_id=ticket_id,
                operation_id=concurrent_values["operation_id"],
                action="tool.invoke",
                resource_kind="bot",
                resource_id=store.bot_id,
                input_digest="sha256:" + "9" * 64,
                plan_digest="sha256:" + "8" * 64,
                audience_component_id="tool.weather",
                audience_generation_id="generation/weather-one",
            )
        except ForestError as exc:
            return exc.code
        return "accepted"

    assert sorted(
        await asyncio.gather(
            consume_concurrent("ticket/tool/concurrent/one"),
            consume_concurrent("ticket/tool/concurrent/two"),
        )
    ) == ["TICKET_CONSUMPTION_CONFLICT", "accepted"]


@pytest.mark.postgres
async def test_kernel_authorization_approval_requires_ticketed_decision_and_single_consumption(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    authority = KernelAuthorizationRepository(store, namespace=storage.spec.namespace)
    input_digest = "sha256:" + "b" * 64
    plan_digest = "sha256:" + "c" * 64
    expiry = datetime.now(timezone.utc) + timedelta(minutes=5)
    approval_args = dict(
        approval_id="approval/tool/one",
        request_kind="agent.tool",
        request_id="tool-call/one",
        action="tool.invoke",
        input_digest=input_digest,
        plan_digest=plan_digest,
        requester_kind="component",
        requester_id="agent.runtime",
        approver_scope_kind="group",
        approver_scope_id="933179609",
        expected_policy_revision=1,
        expires_at=expiry,
    )
    pending, pending_replay = await asyncio.gather(
        authority.request_approval(**approval_args),
        authority.request_approval(**approval_args),
    )
    assert pending["state"] == "PENDING"
    assert pending_replay == pending
    assert await authority.request_approval(**approval_args) == pending

    approver = await authority.create_grant_set(
        grantee_kind="platform.user",
        grantee_id="qq/1234",
        entries=(CapabilityGrant("approval.decide", "group", "933179609"),),
        expected_policy_revision=1,
        audience_component_id="kernel.authorization",
        audience_generation_id="generation/authorization-one",
    )
    decision_digest = authority.approval_decision_digest(
        approval_id="approval/tool/one", decision="APPROVED"
    )
    decision_expiry = datetime.now(timezone.utc) + timedelta(seconds=20)
    await authority.issue_ticket(
        ticket_id="ticket/approval/tool-one",
        issuer_kind="group.shell",
        issuer_id="shell/main",
        subject_kind="platform.user",
        subject_id="qq/1234",
        action="approval.decide",
        resource_kind="group",
        resource_id="933179609",
        operation_id="approval-decision/tool-one",
        input_digest=decision_digest,
        plan_digest=plan_digest,
        grant_set=approver,
        expires_at=decision_expiry,
    )
    decision_args = dict(
        approval_id="approval/tool/one",
        decision="APPROVED",
        decided_by_kind="platform.user",
        decided_by_id="qq/1234",
        decision_operation_id="approval-decision/tool-one",
        authorization_ticket_id="ticket/approval/tool-one",
        audience_component_id="kernel.authorization",
        audience_generation_id="generation/authorization-one",
    )
    approved = await authority.decide_approval(**decision_args)
    assert approved["state"] == "APPROVED"
    assert await authority.decide_approval(**decision_args) == approved

    consume_args = dict(
        approval_id="approval/tool/one",
        request_kind="agent.tool",
        request_id="tool-call/one",
        action="tool.invoke",
        input_digest=input_digest,
        plan_digest=plan_digest,
        requester_kind="component",
        requester_id="agent.runtime",
        consume_operation_id="effect/tool/one",
    )
    consumed = await authority.consume_approval(**consume_args)
    assert consumed["state"] == "CONSUMED"
    assert await authority.consume_approval(**consume_args) == consumed
    assert (await authority.decide_approval(**decision_args))["state"] == "CONSUMED"
    with pytest.raises(ForestError) as caught:
        await authority.consume_approval(
            **{**consume_args, "consume_operation_id": "effect/tool/two"}
        )
    assert caught.value.code == "APPROVAL_CONSUMPTION_CONFLICT"


@pytest.mark.postgres
async def test_kernel_authorization_consumption_and_revocation_converge_in_both_orders(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    authority = KernelAuthorizationRepository(store, namespace=storage.spec.namespace)
    digest = "sha256:" + "2" * 64

    async def issue(revision: int, suffix: str):
        grant = await authority.create_grant_set(
            grantee_kind="component",
            grantee_id="shell.command",
            entries=(CapabilityGrant("runtime.inspect", "bot", store.bot_id),),
            expected_policy_revision=revision,
        )
        await authority.issue_ticket(
            ticket_id=f"ticket/{suffix}",
            issuer_kind="kernel",
            issuer_id="kernel.authorization",
            subject_kind="component",
            subject_id="shell.command",
            action="runtime.inspect",
            resource_kind="bot",
            resource_id=store.bot_id,
            operation_id=f"operation/{suffix}",
            input_digest=digest,
            plan_digest=None,
            grant_set=grant,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=20),
        )

    def consume_values(suffix: str):
        return {
            "ticket_id": f"ticket/{suffix}",
            "operation_id": f"operation/{suffix}",
            "action": "runtime.inspect",
            "resource_kind": "bot",
            "resource_id": store.bot_id,
            "input_digest": digest,
            "plan_digest": None,
        }

    await issue(1, "consume-first")
    consumed = asyncio.Event()
    release_consumption = asyncio.Event()
    revoke_started = asyncio.Event()

    async def consume_first():
        async with store.pool.connection() as conn, conn.transaction():
            receipt = await authority.consume_ticket_in(conn, **consume_values("consume-first"))
            consumed.set()
            await release_consumption.wait()
            return receipt

    async def revoke_after_consumption():
        await consumed.wait()
        revoke_started.set()
        return await authority.revoke(
            operation_id="revoke/after-consumption",
            selector_kind="grant",
            selector_id="consume-first",
            expected_policy_revision=1,
        )

    consumption_task = asyncio.create_task(consume_first())
    revocation_task = asyncio.create_task(revoke_after_consumption())
    await revoke_started.wait()
    release_consumption.set()
    receipt, revocation = await asyncio.gather(consumption_task, revocation_task)
    assert receipt["policy_revision"] == 1
    assert revocation["policy_revision"] == 2
    assert await authority.consume_ticket(**consume_values("consume-first")) == receipt

    await issue(2, "revoke-first")
    revoked = asyncio.Event()
    release_revocation = asyncio.Event()

    async def revoke_first():
        async with store.pool.connection() as conn, conn.transaction():
            result = await authority.revoke_in(
                conn,
                operation_id="revoke/before-consumption",
                selector_kind="grant",
                selector_id="revoke-first",
                expected_policy_revision=2,
            )
            revoked.set()
            await release_revocation.wait()
            return result

    async def consume_after_revocation():
        await revoked.wait()
        try:
            await authority.consume_ticket(**consume_values("revoke-first"))
        except ForestError as exc:
            return exc.code
        return "accepted"

    revocation_task = asyncio.create_task(revoke_first())
    consumption_task = asyncio.create_task(consume_after_revocation())
    await revoked.wait()
    release_revocation.set()
    revocation, result = await asyncio.gather(revocation_task, consumption_task)
    assert revocation["policy_revision"] == 3
    assert result == "POLICY_CONFLICT"


@pytest.mark.postgres
async def test_clean_kernel_effect_preserves_unknown_and_reconciles_explicitly(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    authorization_storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    effect_storage = official_storage_bundle(
        "kernel.effect",
        "forest.kernel.effect",
        "forest.kernel-effect.repository/1",
        "storage/official/kernel_effect.sql",
    )
    for storage in (payload_storage, authorization_storage, effect_storage):
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    lowered = effect_storage.baseline.lower()
    for forbidden in (
        b"agent",
        b"onebot",
        b"memory",
        b"binding",
        b"job",
        b"tool_id",
        b"intent_id",
        b"provider",
    ):
        assert forbidden not in lowered

    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    authorization = KernelAuthorizationRepository(
        store, namespace=authorization_storage.spec.namespace
    )
    checked_fences = []
    synchronize_first_permits = True
    first_permit_arrivals = 0
    both_first_permits_ready = asyncio.Event()

    async def verify_fence(_conn, **identity):
        nonlocal first_permit_arrivals
        checked_fences.append(identity)
        if synchronize_first_permits:
            first_permit_arrivals += 1
            if first_permit_arrivals == 2:
                both_first_permits_ready.set()
            await both_first_permits_ready.wait()

    effects = KernelEffectRepository(
        store,
        namespace=effect_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
        authorization_consumer=authorization.consume_ticket_in,
        fence_verifier=verify_fence,
        fence_kind="connection",
    )
    effect_operation_id = "effect/send-one"
    attempt_id = "attempt/send-one/1"
    semantic_digest = "sha256:" + "3" * 64
    input_payload = await payloads.put_bytes(
        owner_component_id="kernel.effect",
        owner_kind="effect.input",
        owner_id=effect_operation_id,
        media_type="application/json",
        content=b'{"semantic":"send"}',
        expected_deletion_epoch=0,
    )
    wire_payload = await payloads.put_bytes(
        owner_component_id="kernel.effect",
        owner_kind="effect.wire",
        owner_id=attempt_id,
        media_type="application/octet-stream",
        content=b"exact wire bytes",
        expected_deletion_epoch=0,
    )
    effect = await effects.admit(
        effect_operation_id=effect_operation_id,
        input_digest=semantic_digest,
        effect_kind="external.dispatch",
        target_kind="channel",
        target_id="group/933179609",
        producer_component_id="delivery",
        producer_generation_id="generation/delivery-one",
        root_operation_kind="conversation.reply",
        root_operation_id="reply/one",
        input_payload_id=input_payload.payload_id,
        input_payload_digest=input_payload.content_digest,
    )
    assert (
        await effects.admit(
            effect_operation_id=effect_operation_id,
            input_digest=semantic_digest,
            effect_kind="external.dispatch",
            target_kind="channel",
            target_id="group/933179609",
            producer_component_id="delivery",
            producer_generation_id="generation/delivery-one",
            root_operation_kind="conversation.reply",
            root_operation_id="reply/one",
            input_payload_id=input_payload.payload_id,
            input_payload_digest=input_payload.content_digest,
        )
        == effect
    )

    grant = await authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="delivery",
        entries=(CapabilityGrant("message.send", "channel", "group/933179609"),),
        expected_policy_revision=1,
        audience_component_id="adapter.onebot",
        audience_generation_id="generation/adapter-one",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=2),
    )
    permit_operation_id = "permit/send-one/1"
    deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
    await authorization.issue_ticket(
        ticket_id="ticket/send-one/1",
        issuer_kind="kernel",
        issuer_id="kernel.authorization",
        subject_kind="component",
        subject_id="delivery",
        action="message.send",
        resource_kind="channel",
        resource_id="group/933179609",
        operation_id=permit_operation_id,
        input_digest=semantic_digest,
        plan_digest=None,
        grant_set=grant,
        expires_at=deadline,
    )
    permit_values = dict(
        effect_operation_id=effect_operation_id,
        attempt_id=attempt_id,
        permit_operation_id=permit_operation_id,
        wire_payload_id=wire_payload.payload_id,
        wire_payload_digest=wire_payload.content_digest,
        authorization_ticket_id="ticket/send-one/1",
        authorization_action="message.send",
        authorization_resource_kind="channel",
        authorization_resource_id="group/933179609",
        authorization_plan_digest=None,
        executor_component_id="adapter.onebot",
        executor_generation_id="generation/adapter-one",
        fence_kind="connection",
        fence_id="connection/main",
        fence_epoch=7,
        deadline_at=deadline,
    )
    permit, permit_replay = await asyncio.gather(
        effects.permit(**permit_values), effects.permit(**permit_values)
    )
    synchronize_first_permits = False
    assert permit_replay == permit
    assert permit.attempt_no == 1
    handoff = await effects.mark_handoff(permit=permit, handoff_operation_id="handoff/send-one/1")
    assert handoff["state"] == "DISPATCHING"
    assert (
        await effects.mark_handoff(permit=permit, handoff_operation_id="handoff/send-one/1")
        == handoff
    )

    unknown = await effects.observe(
        effect_operation_id=effect_operation_id,
        attempt_id=attempt_id,
        observation_operation_id="observe/send-one/unknown",
        status="UNKNOWN",
        evidence_kind="executor",
    )
    assert unknown["accepted"] is True
    premature = await effects.observe(
        effect_operation_id=effect_operation_id,
        attempt_id=attempt_id,
        observation_operation_id="observe/send-one/premature-success",
        status="SUCCEEDED",
        evidence_kind="executor",
    )
    assert premature["accepted"] is False
    evidence_payload = await payloads.put_bytes(
        owner_component_id="kernel.effect",
        owner_kind="effect.evidence",
        owner_id="reconcile/send-one/success",
        media_type="application/json",
        content=b'{"platform_receipt":"confirmed"}',
        expected_deletion_epoch=0,
    )
    reconciled = await effects.observe(
        effect_operation_id=effect_operation_id,
        attempt_id=attempt_id,
        observation_operation_id="reconcile/send-one/success",
        status="SUCCEEDED",
        evidence_kind="reconciliation",
        evidence_payload_id=evidence_payload.payload_id,
        evidence_payload_digest=evidence_payload.content_digest,
    )
    assert reconciled["accepted"] is True
    conflict = await effects.observe(
        effect_operation_id=effect_operation_id,
        attempt_id=attempt_id,
        observation_operation_id="reconcile/send-one/not-applied",
        status="NOT_APPLIED",
        evidence_kind="reconciliation",
    )
    assert conflict["accepted"] is False
    async with store.pool.connection() as conn:
        state = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state FROM {}.effect_operations "
                    "WHERE bot_id=%s AND effect_operation_id=%s"
                ).format(sql.Identifier(effect_storage.spec.namespace)),
                (store.bot_id, effect_operation_id),
            )
        ).fetchone()
        assert state["state"] == "SUCCEEDED"
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("DELETE FROM {}.effect_observations").format(
                    sql.Identifier(effect_storage.spec.namespace)
                )
            )
    assert len(checked_fences) >= 2

    concurrent_effect_id = "effect/concurrent-reconciliation"
    concurrent_attempt_id = "attempt/concurrent-reconciliation/1"
    concurrent_digest = "sha256:" + "4" * 64
    await effects.admit(
        effect_operation_id=concurrent_effect_id,
        input_digest=concurrent_digest,
        effect_kind="external.dispatch",
        target_kind="channel",
        target_id="group/933179609",
        producer_component_id="delivery",
        producer_generation_id="generation/delivery-one",
        root_operation_kind="conversation.reply",
        root_operation_id="reply/concurrent",
    )
    concurrent_wire = await payloads.put_bytes(
        owner_component_id="kernel.effect",
        owner_kind="effect.wire",
        owner_id=concurrent_attempt_id,
        media_type="application/octet-stream",
        content=b"concurrent wire bytes",
        expected_deletion_epoch=0,
    )
    concurrent_permit_operation = "permit/concurrent-reconciliation/1"
    concurrent_deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
    await authorization.issue_ticket(
        ticket_id="ticket/concurrent-reconciliation/1",
        issuer_kind="kernel",
        issuer_id="kernel.authorization",
        subject_kind="component",
        subject_id="delivery",
        action="message.send",
        resource_kind="channel",
        resource_id="group/933179609",
        operation_id=concurrent_permit_operation,
        input_digest=concurrent_digest,
        plan_digest=None,
        grant_set=grant,
        expires_at=concurrent_deadline,
    )
    concurrent_permit = await effects.permit(
        effect_operation_id=concurrent_effect_id,
        attempt_id=concurrent_attempt_id,
        permit_operation_id=concurrent_permit_operation,
        wire_payload_id=concurrent_wire.payload_id,
        wire_payload_digest=concurrent_wire.content_digest,
        authorization_ticket_id="ticket/concurrent-reconciliation/1",
        authorization_action="message.send",
        authorization_resource_kind="channel",
        authorization_resource_id="group/933179609",
        authorization_plan_digest=None,
        executor_component_id="adapter.onebot",
        executor_generation_id="generation/adapter-one",
        fence_kind="connection",
        fence_id="connection/main",
        fence_epoch=7,
        deadline_at=concurrent_deadline,
    )
    await effects.mark_handoff(
        permit=concurrent_permit,
        handoff_operation_id="handoff/concurrent-reconciliation/1",
    )
    await effects.observe(
        effect_operation_id=concurrent_effect_id,
        attempt_id=concurrent_attempt_id,
        observation_operation_id="observe/concurrent-reconciliation/unknown",
        status="UNKNOWN",
        evidence_kind="executor",
    )

    async def reconcile(operation_id: str, status: str):
        return await effects.observe(
            effect_operation_id=concurrent_effect_id,
            attempt_id=concurrent_attempt_id,
            observation_operation_id=operation_id,
            status=status,
            evidence_kind="reconciliation",
        )

    competing = await asyncio.gather(
        reconcile("reconcile/concurrent/succeeded", "SUCCEEDED"),
        reconcile("reconcile/concurrent/not-applied", "NOT_APPLIED"),
    )
    assert sorted(item["accepted"] for item in competing) == [False, True]
    accepted = next(item for item in competing if item["accepted"])
    async with store.pool.connection() as conn:
        current = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state FROM {}.effect_operations "
                    "WHERE bot_id=%s AND effect_operation_id=%s"
                ).format(sql.Identifier(effect_storage.spec.namespace)),
                (store.bot_id, concurrent_effect_id),
            )
        ).fetchone()
    assert current["state"] == accepted["observed_status"]
    assert len(checked_fences) >= 4

    abandoned_effect_id = "effect/abandoned-retry"
    abandoned_digest = "sha256:" + "5" * 64
    await effects.admit(
        effect_operation_id=abandoned_effect_id,
        input_digest=abandoned_digest,
        effect_kind="external.dispatch",
        target_kind="channel",
        target_id="group/933179609",
        producer_component_id="delivery",
        producer_generation_id="generation/delivery-one",
        root_operation_kind="conversation.reply",
        root_operation_id="reply/abandoned",
    )

    async def prepare_abandon_attempt(number: int):
        attempt = f"attempt/abandoned-retry/{number}"
        operation = f"permit/abandoned-retry/{number}"
        ticket = f"ticket/abandoned-retry/{number}"
        wire = await payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind="effect.wire",
            owner_id=attempt,
            media_type="application/octet-stream",
            content=f"abandon wire {number}".encode(),
            expected_deletion_epoch=0,
        )
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=20)
        await authorization.issue_ticket(
            ticket_id=ticket,
            issuer_kind="kernel",
            issuer_id="kernel.authorization",
            subject_kind="component",
            subject_id="delivery",
            action="message.send",
            resource_kind="channel",
            resource_id="group/933179609",
            operation_id=operation,
            input_digest=abandoned_digest,
            plan_digest=None,
            grant_set=grant,
            expires_at=expires_at,
        )
        return await effects.permit(
            effect_operation_id=abandoned_effect_id,
            attempt_id=attempt,
            permit_operation_id=operation,
            wire_payload_id=wire.payload_id,
            wire_payload_digest=wire.content_digest,
            authorization_ticket_id=ticket,
            authorization_action="message.send",
            authorization_resource_kind="channel",
            authorization_resource_id="group/933179609",
            authorization_plan_digest=None,
            executor_component_id="adapter.onebot",
            executor_generation_id="generation/adapter-one",
            fence_kind="connection",
            fence_id="connection/main",
            fence_epoch=7,
            deadline_at=expires_at,
        )

    first_abandoned = await prepare_abandon_attempt(1)
    abandonment = await effects.abandon_prepared(
        effect_operation_id=abandoned_effect_id,
        attempt_id=first_abandoned.attempt_id,
        abandonment_operation_id="abandon/abandoned-retry/1",
        reason_code="PREPARE_EXPIRED",
    )
    assert (
        await effects.abandon_prepared(
            effect_operation_id=abandoned_effect_id,
            attempt_id=first_abandoned.attempt_id,
            abandonment_operation_id="abandon/abandoned-retry/1",
            reason_code="PREPARE_EXPIRED",
        )
        == abandonment
    )
    with pytest.raises(ForestError) as mismatched_effect:
        await effects.abandon_prepared(
            effect_operation_id="effect/other",
            attempt_id=first_abandoned.attempt_id,
            abandonment_operation_id="abandon/abandoned-retry/1",
            reason_code="PREPARE_EXPIRED",
        )
    assert mismatched_effect.value.code == "EFFECT_ABANDONMENT_CONFLICT"
    second_abandoned = await prepare_abandon_attempt(2)
    assert second_abandoned.attempt_no == 2
    await effects.abandon_prepared(
        effect_operation_id=abandoned_effect_id,
        attempt_id=second_abandoned.attempt_id,
        abandonment_operation_id="abandon/abandoned-retry/2",
        reason_code="EXPLICIT_CANCEL",
        cancel_effect=True,
    )
    async with store.pool.connection() as conn:
        abandoned_state = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state,attempt_count FROM {}.effect_operations "
                    "WHERE bot_id=%s AND effect_operation_id=%s"
                ).format(sql.Identifier(effect_storage.spec.namespace)),
                (store.bot_id, abandoned_effect_id),
            )
        ).fetchone()
    assert abandoned_state == {"state": "CANCELLED", "attempt_count": 2}
    assert len(checked_fences) >= 6


@pytest.mark.postgres
async def test_clean_kernel_audit_bundle_is_filterable_immutable_and_payload_only(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.audit",
        "forest.kernel.audit",
        "forest.kernel-audit.repository/1",
        "storage/official/kernel_audit.sql",
    )
    row = await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    assert row["component_id"] == "kernel.audit"
    lowered = storage.baseline.lower()
    assert b" jsonb" not in lowered
    for forbidden in (b"agent", b"onebot", b"memory", b"binding", b"job"):
        assert forbidden not in lowered

    verified_payloads = []

    async def verify_payload(_conn, **identity):
        verified_payloads.append(identity)

    repository = KernelAuditRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=verify_payload,
    )
    envelope = AuditEnvelope(
        audit_id="audit-1",
        action="component.reload",
        severity="SECURITY",
        producer_component_id="component.worker-host",
        producer_generation_id="generation/one",
        origin_kind="shell",
        actor_kind="platform.user",
        actor_id="qq/1234",
        resource_kind="component",
        resource_id="example.worker",
        operation_kind="lifecycle",
        operation_id="operation/1",
        call_id="call/1",
        details_payload_id="payload/1",
        details_payload_digest="sha256:" + "1" * 64,
    )
    with pytest.raises(ForestError) as caught:
        await KernelAuditRepository(store, namespace=storage.spec.namespace).append(envelope)
    assert caught.value.code == "AUDIT_PAYLOAD_PORT_REQUIRED"
    first = await repository.append(envelope)
    assert await repository.append(envelope) == first
    assert verified_payloads[-1] == {
        "payload_id": "payload/1",
        "digest": "sha256:" + "1" * 64,
        "owner_component_id": "kernel.audit",
        "owner_kind": "audit.details",
        "owner_id": "audit-1",
    }
    with pytest.raises(ForestError) as caught:
        await repository.append(
            AuditEnvelope(**{**envelope.__dict__, "action": "component.disable"})
        )
    assert caught.value.code == "AUDIT_ID_CONFLICT"
    await repository.append(
        AuditEnvelope(
            audit_id="audit-2",
            action="component.reload",
            producer_component_id="component.worker-host",
            origin_kind="web",
            resource_kind="component",
            resource_id="other.worker",
        )
    )
    page = await repository.scan(
        producer_component_id="component.worker-host",
        resource_kind="component",
        resource_id="example.worker",
    )
    assert [item["audit_id"] for item in page["records"]] == ["audit-1"]
    assert page["records"][0]["details_payload_id"] == "payload/1"

    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("DELETE FROM {}.records WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )


@pytest.mark.postgres
async def test_clean_kernel_payload_bundle_owns_content_lineage_and_deletion_clock(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    row = await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    assert row["component_id"] == "kernel.payload"
    lowered = storage.baseline.lower()
    for forbidden in (b"agent", b"onebot", b"memory", b"job"):
        assert forbidden not in lowered

    namespace = storage.spec.namespace
    body = b"payload body"
    body_digest = "sha256:" + hashlib.sha256(body).hexdigest()
    manifest_digest = "sha256:" + hashlib.sha256(b"[]").hexdigest()
    manifest_id = "manifest:" + manifest_digest.removeprefix("sha256:")
    payload_id = "payload:" + "1" * 64
    async with store.pool.connection() as conn:
        clock = await (
            await conn.execute(
                sql.SQL("SELECT deletion_epoch FROM {}.deletion_clock").format(
                    sql.Identifier(namespace)
                )
            )
        ).fetchone()
        assert clock["deletion_epoch"] == 0
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.source_manifests"
                "(bot_id,manifest_id,manifest_digest,item_count) VALUES (%s,%s,%s,0)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, manifest_id, manifest_digest),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.payload_headers("
                "bot_id,payload_id,owner_component_id,owner_kind,owner_id,media_type,"
                "byte_length,content_digest,input_digest,source_manifest_id,"
                "source_manifest_digest,parent_count,accepted_deletion_epoch) "
                "VALUES (%s,%s,'kernel.audit','audit.details','audit-1',"
                "'application/json',%s,%s,%s,%s,%s,0,0)"
            ).format(sql.Identifier(namespace)),
            (
                store.bot_id,
                payload_id,
                len(body),
                body_digest,
                "sha256:" + "2" * 64,
                manifest_id,
                manifest_digest,
            ),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.payload_bodies(bot_id,payload_id,content) VALUES (%s,%s,%s)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, payload_id, body),
        )
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.payload_bodies SET content='changed'").format(
                    sql.Identifier(namespace)
                )
            )


@pytest.mark.postgres
async def test_kernel_payload_repository_closes_sources_and_forget_descendants(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    repository = KernelPayloadRepository(store, namespace=storage.spec.namespace)
    source = PayloadSource(
        source_component_id="conversation.scheduler",
        source_kind="group.message",
        source_id="message/1",
        source_revision=1,
        part_id="text/0",
        source_trunk_id="group/933179609",
        observation_conversation_id="conversation/1",
        observation_binding_id="binding/1",
        author_subject_id="qq/1234",
    )
    parent = await repository.put_bytes(
        owner_component_id="conversation.scheduler",
        owner_kind="context.input",
        owner_id="unit/1",
        media_type="text/plain",
        content=b"parent",
        sources=(source,),
        expected_deletion_epoch=0,
    )
    child = await repository.put_bytes(
        owner_component_id="agent.runtime",
        owner_kind="model.request",
        owner_id="request/1",
        media_type="application/json",
        content=b'{"request":1}',
        parents=(parent.payload_id,),
        expected_deletion_epoch=0,
    )
    assert child.source_manifest_digest == parent.source_manifest_digest
    admission_locks = 0
    both_admissions_ready = asyncio.Event()

    async def parallel_put():
        nonlocal admission_locks
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "SELECT deletion_epoch FROM {}.deletion_clock WHERE bot_id=%s FOR SHARE"
                ).format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id,),
            )
            admission_locks += 1
            if admission_locks == 2:
                both_admissions_ready.set()
            await both_admissions_ready.wait()
            return await repository.put_bytes_in(
                conn,
                owner_component_id="memory.graph",
                owner_kind="retrieval.evidence",
                owner_id="evidence/parallel",
                media_type="application/json",
                content=b'{"evidence":true}',
                parents=(parent.payload_id,),
                expected_deletion_epoch=0,
            )

    parallel = await asyncio.gather(parallel_put(), parallel_put())
    assert parallel[0] == parallel[1]
    receipt = await repository.forget(
        operation_id="forget/1",
        source_targets=(
            SourceTarget(
                source_component_id=source.source_component_id,
                source_kind=source.source_kind,
                source_id=source.source_id,
                source_revision=source.source_revision,
                part_id=source.part_id,
            ),
        ),
        expected_deletion_epoch=0,
    )
    assert receipt["selected_count"] == 3
    assert (
        await repository.forget(
            operation_id="forget/1",
            source_targets=(
                SourceTarget(
                    source_component_id=source.source_component_id,
                    source_kind=source.source_kind,
                    source_id=source.source_id,
                    source_revision=source.source_revision,
                    part_id=source.part_id,
                ),
            ),
            expected_deletion_epoch=0,
        )
        == receipt
    )
    async with store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as caught:
            await repository.read_bytes_in(conn, child.payload_id, expected_deletion_epoch=1)
        assert caught.value.code == "PAYLOAD_REVOKED"
    for content in (b'{"request":1}', b'{"request":2}'):
        with pytest.raises(ForestError) as caught:
            await repository.put_bytes(
                owner_component_id="agent.runtime",
                owner_kind="model.request",
                owner_id="request/1",
                media_type="application/json",
                content=content,
                parents=(parent.payload_id,),
                expected_deletion_epoch=1,
            )
        assert caught.value.code == "PAYLOAD_REVOKED"
    with pytest.raises(ForestError) as caught:
        await repository.put_bytes(
            owner_component_id="memory.graph",
            owner_kind="index.document",
            owner_id="document/late",
            media_type="text/plain",
            content=b"late",
            sources=(source,),
            expected_deletion_epoch=1,
        )
    assert caught.value.code == "SOURCE_SUPPRESSED"
    assert await repository.scrub(limit=10) == {"scrubbed": 3}


@pytest.mark.postgres
async def test_kernel_payload_forget_selects_multiple_sources_as_one_closure(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.payload", "forest.kernel.payload",
        "forest.kernel-payload.repository/1", "storage/official/kernel_payload.sql",
    )
    await installer.install(
        storage.spec, storage.baseline, storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    repository = KernelPayloadRepository(store, namespace=storage.spec.namespace)
    roots = []
    for index in range(3):
        source = PayloadSource("conversation.scheduler", "group.message", f"message/{index}", 1)
        roots.append(await repository.put_bytes(
            owner_component_id="conversation.scheduler", owner_kind="context.input",
            owner_id=f"input/{index}", media_type="text/plain",
            content=f"source {index}".encode(), sources=(source,),
            expected_deletion_epoch=0,
        ))
    derived = await repository.put_bytes(
        owner_component_id="memory.graph", owner_kind="retrieval.evidence",
        owner_id="evidence/shared", media_type="application/json",
        content=b'{"evidence":true}',
        parents=(roots[0].payload_id, roots[1].payload_id),
        expected_deletion_epoch=0,
    )
    participant = KernelPayloadUnitOfWorkParticipant(
        SimpleNamespace(generation_identity=SimpleNamespace(generation_id="payload/test")),
        repository,
    )
    envelope = {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "memory.graph", "generation_id": "memory/test"},
        "prior": {"prepare": {
            "protocol": "forest.payload-forget-batch-plan/1",
            "anchor_payload_ids": [roots[0].payload_id, roots[1].payload_id],
        }},
    }
    async with store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as ambiguous:
            await participant.invoke_in(conn, "payload.preview-forget-anchors-from-prior", {
                **envelope,
                "prior": {"prepare": {
                    "protocol": "forest.payload-forget-batch-plan/1",
                    "anchor_payload_ids": [roots[0].payload_id, derived.payload_id],
                }},
                "input": {"protocol": "forest.payload-forget-anchors-preview/1",
                          "prior_step_id": "prepare"},
            })
        assert ambiguous.value.code == "PAYLOAD_FORGET_SOURCE_AMBIGUOUS"
        preview = await participant.invoke_in(
            conn, "payload.preview-forget-anchors-from-prior", {
                **envelope,
                "input": {"protocol": "forest.payload-forget-anchors-preview/1",
                          "prior_step_id": "prepare"},
            },
        )
        receipt = await participant.invoke_in(
            conn, "payload.forget-anchors-from-prior", {
                **envelope,
                "input": {
                    "protocol": "forest.payload-forget-anchors/1",
                    "prior_step_id": "prepare",
                    "operation_id": "forget/two-sources",
                    "expected_deletion_epoch": preview["deletion_epoch"],
                    "expected_selection_digest": preview["selection_digest"],
                },
            },
        )
        selected = await repository.forget_selected_payload_ids_in(
            conn, "forget/two-sources"
        )
    assert receipt["protocol"] == "forest.payload-forgotten/1"
    assert selected == sorted([roots[0].payload_id, roots[1].payload_id, derived.payload_id])
    assert receipt["selected_payload_ids"] == selected
    assert roots[2].payload_id not in selected


@pytest.mark.postgres
async def test_kernel_payload_admission_and_forget_converge_in_both_lock_orders(
    clean_authority,
):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    repository = KernelPayloadRepository(store, namespace=storage.spec.namespace)
    first_source = PayloadSource("conversation.scheduler", "group.message", "message/first", 1)
    first_target = SourceTarget(
        first_source.source_component_id,
        first_source.source_kind,
        first_source.source_id,
        first_source.source_revision,
    )
    admitted = asyncio.Event()
    forget_started = asyncio.Event()
    release_admission = asyncio.Event()

    async def admit_first():
        async with store.pool.connection() as conn, conn.transaction():
            ref = await repository.put_bytes_in(
                conn,
                owner_component_id="conversation.scheduler",
                owner_kind="context.input",
                owner_id="race/first",
                media_type="text/plain",
                content=b"first",
                sources=(first_source,),
                expected_deletion_epoch=0,
            )
            admitted.set()
            await release_admission.wait()
            return ref

    async def forget_first():
        await admitted.wait()
        forget_started.set()
        return await repository.forget(
            operation_id="forget/race-first",
            source_targets=(first_target,),
            expected_deletion_epoch=0,
        )

    admission_task = asyncio.create_task(admit_first())
    forget_task = asyncio.create_task(forget_first())
    await forget_started.wait()
    release_admission.set()
    first_ref, first_receipt = await asyncio.gather(admission_task, forget_task)
    assert first_receipt["selected_count"] == 1

    second_source = PayloadSource("conversation.scheduler", "group.message", "message/second", 1)
    second_target = SourceTarget(
        second_source.source_component_id,
        second_source.source_kind,
        second_source.source_id,
        second_source.source_revision,
    )
    forget_committed = asyncio.Event()
    release_forget = asyncio.Event()

    async def forget_second():
        async with store.pool.connection() as conn, conn.transaction():
            receipt = await repository.forget_in(
                conn,
                operation_id="forget/race-second",
                source_targets=(second_target,),
                expected_deletion_epoch=1,
            )
            forget_committed.set()
            await release_forget.wait()
            return receipt

    async def admit_second():
        await forget_committed.wait()
        with pytest.raises(ForestError) as caught:
            await repository.put_bytes(
                owner_component_id="conversation.scheduler",
                owner_kind="context.input",
                owner_id="race/second",
                media_type="text/plain",
                content=b"second",
                sources=(second_source,),
                expected_deletion_epoch=2,
            )
        return caught.value.code

    second_forget_task = asyncio.create_task(forget_second())
    second_admission_task = asyncio.create_task(admit_second())
    await forget_committed.wait()
    release_forget.set()
    second_receipt, rejected_code = await asyncio.gather(second_forget_task, second_admission_task)
    assert second_receipt["selected_count"] == 0
    assert rejected_code == "SOURCE_SUPPRESSED"

    async with store.pool.connection() as conn:
        state = await (
            await conn.execute(
                sql.SQL(
                    "SELECT state FROM {}.payload_headers WHERE bot_id=%s AND payload_id=%s"
                ).format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id, first_ref.payload_id),
            )
        ).fetchone()
    assert state["state"] == "REVOKED"

    concurrent_source = PayloadSource(
        "conversation.scheduler", "group.message", "message/concurrent-forget", 1
    )
    concurrent_ref = await repository.put_bytes(
        owner_component_id="conversation.scheduler",
        owner_kind="context.input",
        owner_id="race/concurrent-forget",
        media_type="text/plain",
        content=b"concurrent",
        sources=(concurrent_source,),
        expected_deletion_epoch=2,
    )
    concurrent_receipts = await asyncio.gather(*(
        repository.forget(
            operation_id="forget/race-concurrent",
            payload_ids=(concurrent_ref.payload_id,),
        )
        for _ in range(2)
    ))
    assert concurrent_receipts[0] == concurrent_receipts[1]
    assert concurrent_receipts[0]["deletion_epoch"] == 3
    assert await repository.epoch() == 3


@pytest.mark.postgres
async def test_kernel_payload_forget_preview_fences_a_changed_derivation(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    repository = KernelPayloadRepository(store, namespace=storage.spec.namespace)
    source = PayloadSource("conversation.scheduler", "group.message", "message/plan", 1)
    target = SourceTarget(
        source.source_component_id,
        source.source_kind,
        source.source_id,
        source.source_revision,
    )
    root = await repository.put_bytes(
        owner_component_id="conversation.scheduler",
        owner_kind="context.input",
        owner_id="plan/root",
        media_type="text/plain",
        content=b"root",
        sources=(source,),
        expected_deletion_epoch=0,
    )
    async with store.pool.connection() as conn, conn.transaction():
        first_plan = await repository.preview_forget_in(conn, source_targets=(target,))
    assert first_plan["deletion_epoch"] == 0
    assert first_plan["selected_payload_ids"] == [root.payload_id]

    child = await repository.put_bytes(
        owner_component_id="memory.graph",
        owner_kind="embedding.result",
        owner_id="plan/child",
        media_type="application/json",
        content=b"{}",
        parents=(root.payload_id,),
        expected_deletion_epoch=0,
    )
    with pytest.raises(ForestError) as changed:
        await repository.forget(
            operation_id="forget/plan",
            source_targets=(target,),
            expected_deletion_epoch=first_plan["deletion_epoch"],
            expected_selection_digest=first_plan["selection_digest"],
        )
    assert changed.value.code == "FORGET_SELECTION_CONFLICT"
    assert await repository.epoch() == 0

    async with store.pool.connection() as conn, conn.transaction():
        plan = await repository.preview_forget_in(conn, source_targets=(target,))
    assert plan["selected_payload_ids"] == sorted([root.payload_id, child.payload_id])
    receipt = await repository.forget(
        operation_id="forget/plan",
        source_targets=(target,),
        expected_deletion_epoch=plan["deletion_epoch"],
        expected_selection_digest=plan["selection_digest"],
    )
    assert receipt["selected_count"] == 2
    assert receipt["selection_digest"] == plan["selection_digest"]
    assert await repository.epoch() == 1
    assert await repository.forget(
        operation_id="forget/plan",
        source_targets=(target,),
        expected_deletion_epoch=plan["deletion_epoch"],
        expected_selection_digest=plan["selection_digest"],
    ) == receipt


@pytest.mark.postgres
async def test_kernel_audit_requires_payload_authority_identity(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    audit_storage = official_storage_bundle(
        "kernel.audit",
        "forest.kernel.audit",
        "forest.kernel-audit.repository/1",
        "storage/official/kernel_audit.sql",
    )
    for storage in (payload_storage, audit_storage):
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    async with store.pool.connection() as conn, conn.transaction():
        details = await payloads.put_json_in(
            conn,
            owner_component_id="kernel.audit",
            owner_kind="audit.details",
            owner_id="audit-joined",
            value={"safe": True},
            expected_deletion_epoch=0,
        )
    audits = KernelAuditRepository(
        store,
        namespace=audit_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    record = await audits.append(
        AuditEnvelope(
            audit_id="audit-joined",
            action="payload.verified",
            producer_component_id="kernel.payload",
            origin_kind="runtime",
            details_payload_id=details.payload_id,
            details_payload_digest=details.content_digest,
        )
    )
    assert record["details_payload_id"] == details.payload_id


@pytest.mark.postgres
async def test_failed_bundle_rolls_back_namespace_and_ledger(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    baseline = b"CREATE TABLE duplicate(id int); CREATE TABLE duplicate(id int);"
    spec = bundle(baseline, schema_id="example.failure")
    with pytest.raises(ForestError) as caught:
        await installer.install(spec, baseline, "sha256:" + "3" * 64)
    assert caught.value.code == "SCHEMA_BUNDLE_INSTALL_FAILED"
    async with store.pool.connection() as conn:
        namespace = await (
            await conn.execute("SELECT 1 FROM pg_namespace WHERE nspname=%s", (spec.namespace,))
        ).fetchone()
        ledger = await (
            await conn.execute(
                "SELECT 1 FROM forest_kernel.schema_bundles WHERE schema_id=%s",
                (spec.schema_id,),
            )
        ).fetchone()
    assert namespace is None and ledger is None


@pytest.mark.postgres
async def test_bundle_rejects_unregistered_occupied_namespace(clean_authority):
    store, installer = clean_authority
    await installer.initialize_kernel()
    baseline = b"CREATE TABLE records(record_id text PRIMARY KEY);"
    spec = bundle(baseline, schema_id="example.occupied")
    async with store.pool.connection() as conn:
        await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(spec.namespace)))
    with pytest.raises(ForestError) as caught:
        await installer.install(spec, baseline, "sha256:" + "4" * 64)
    assert caught.value.code == "SCHEMA_NAMESPACE_OCCUPIED"


@pytest.mark.postgres
async def test_routing_builtin_owns_worker_storage_and_lifecycle(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    lease = await core.runtime.claim_host("host-routing-package", "inc-routing-package")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    routing = await bootstrap_builtin_package(
        Path(str(files("forest").joinpath("builtin/routing_graph"))),
        PackageCatalog(tmp_path / "routing-packages"),
        store,
        installer,
        kernel=core,
        initial_configuration={"protocol": "forest.routing.graph/2"},
    )
    component = await bootstrap_builtin_package(
        Path(__file__).parent / "fixtures" / "routing_component",
        PackageCatalog(tmp_path / "component-packages"),
        store,
        installer,
        kernel=core,
        initial_configuration={},
    )
    registry = RuntimeModuleRegistry(
        host, _isolated_runtime_descriptors(
            *core.descriptors, routing.descriptor, component.descriptor
        )
    )
    await registry.start()
    instance = registry.get("routing.graph")
    assert instance.snapshot.module_root.name == "python"
    assert instance.state.value == "ACTIVE"
    assert len(instance.owned_tasks()) == 16
    component_instance = registry.get("tests.routing-component")
    component_generation = component_instance.generation_identity
    event_service = component_instance._event_publisher
    job_service = component_instance._component_job_scheduler
    published = await event_service.invoke(
        {
            "protocol": "forest.event-publication/1",
            "publisher_component_id": "tests.routing-component",
            "publisher_generation_id": component_generation.generation_id,
            "topic": "tests.ready",
            "contract": "1.0",
            "payload": {"value": 7},
        },
        operation_id="routing-test:event",
        causal_root_kind="test",
        causal_root_id="routing-test",
    )
    scheduled = await job_service.invoke(
        {
            "protocol": "forest.component-job-schedule/1",
            "owner_component_id": "tests.routing-component",
            "owner_generation_id": component_generation.generation_id,
            "handler_id": "tests.echo",
            "payload": {"value": 9},
            "delay_seconds": 0,
        },
        operation_id="routing-test:job",
        causal_root_kind="test",
        causal_root_id="routing-test",
    )
    assert published["delivery_count"] == 1
    assert scheduled["state"] == "WAITING"
    registry.notify_work()
    namespace = routing.descriptor.storage.spec.namespace
    states = None
    for _ in range(100):
        async with store.pool.connection() as conn:
            states = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT (SELECT state FROM {}.event_deliveries LIMIT 1) AS event_state,"
                        "(SELECT state FROM {}.component_jobs LIMIT 1) AS job_state,"
                        "(SELECT result_payload FROM {}.component_jobs LIMIT 1) AS job_result"
                    ).format(
                        sql.Identifier(namespace),
                        sql.Identifier(namespace),
                        sql.Identifier(namespace),
                    )
                )
            ).fetchone()
        if states["event_state"] == states["job_state"] == "SUCCEEDED":
            break
        await asyncio.sleep(0.05)
    assert states == {
        "event_state": "SUCCEEDED",
        "job_state": "SUCCEEDED",
        "job_result": {"echo": 9},
    }
    await registry.close()
    assert instance.state.value == "STOPPED"


@pytest.mark.postgres
async def test_reviewed_bot_archive_binds_private_sql_only_with_exact_catalog_provenance(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    staging = PackageCatalog(tmp_path / "reviewed-staging")
    source = staging.build(Path(__file__).parents[1] / "src/forest/builtin/counter")
    archive = staging.export_archive(source.digest)
    approved = tuple(source.component["capabilities"]["requests"])
    configuration = {"label": "reviewed", "multiplier": 1}

    ordinary = PackageCatalog(tmp_path / "ordinary")
    ordinary.import_archive(archive)
    with pytest.raises(ForestError) as denied:
        await bootstrap_installed_package(
            source.digest, ordinary, store, installer, kernel=core,
            initial_configuration=configuration, approved_capabilities=approved,
        )
    assert denied.value.code == "PACKAGE_STORAGE_TRUST_REQUIRED"

    reviewed = PackageCatalog(tmp_path / "reviewed")
    reviewed.publish_reviewed_bot_archive(
        archive,
        expected_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
        expected_package_digest=source.digest,
        expected_manifest_digest=digest_json(source.manifest),
        expected_package_id=source.manifest["package_id"],
        expected_component_id=source.component["id"],
        expected_runtime="python-worker",
    )
    provisioned = await bootstrap_installed_package(
        source.digest, reviewed, store, installer, kernel=core,
        initial_configuration=configuration, approved_capabilities=approved,
    )
    assert provisioned.descriptor.artifact.trust_level == "reviewed"
    assert provisioned.descriptor.storage.execution_profile == "official"
    inspection, candidate = await compile_installed_candidate_descriptor(
        source.digest, reviewed, store, kernel=core,
        approved_capabilities=approved,
        expected_manifest_digest=digest_json(source.manifest),
        expected_configuration_revision=1,
    )
    assert inspection.trust_level == candidate.artifact.trust_level == "reviewed"
    namespace = provisioned.descriptor.storage.spec.namespace
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                sql.SQL("SELECT {}.counter_current(%s::jsonb, %s::jsonb) AS result").format(
                    sql.Identifier(namespace)
                ),
                (Jsonb({}), Jsonb({})),
            )
        ).fetchone()
    assert row["result"] == {
        "protocol": "forest.domain-result/1",
        "ok": True,
        "value": {"value": None, "revision": 0},
    }
    lease = await core.runtime.claim_host("host-reviewed-sql", "inc-reviewed-sql")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=core.runtime,
    )
    registry = RuntimeModuleRegistry(
        host, _isolated_runtime_descriptors(*core.descriptors, provisioned.descriptor)
    )
    await registry.start()
    first_graph = registry.graph_identity
    assert "forest.counter" in first_graph.generations

    updated_root = tmp_path / "reviewed-counter-v2"
    copytree(Path(__file__).parents[1] / "src/forest/builtin/counter", updated_root)
    manifest = updated_root / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'version = "0.7.2"', 'version = "0.7.3"', 1
        ),
        encoding="utf-8",
    )
    worker_source = updated_root / "python" / "counter.py"
    worker_source.write_text(
        worker_source.read_text(encoding="utf-8") + "\nREVIEWED_GENERATION = 2\n",
        encoding="utf-8",
    )
    updated_source = staging.build(updated_root)
    updated_archive = staging.export_archive(updated_source.digest)
    reviewed.publish_reviewed_bot_archive(
        updated_archive,
        expected_archive_sha256="sha256:" + hashlib.sha256(updated_archive).hexdigest(),
        expected_package_digest=updated_source.digest,
        expected_manifest_digest=digest_json(updated_source.manifest),
        expected_package_id=updated_source.manifest["package_id"],
        expected_component_id=updated_source.component["id"],
        expected_runtime="python-worker",
    )
    planned, descriptor = await compile_installed_candidate_descriptor(
        updated_source.digest, reviewed, store, kernel=core,
        approved_capabilities=approved,
        expected_manifest_digest=digest_json(updated_source.manifest),
        expected_configuration_revision=1,
    )
    assert planned.trust_level == descriptor.artifact.trust_level == "reviewed"
    assert descriptor.factory is None
    assert descriptor.storage.package_digest == provisioned.descriptor.storage.package_digest

    updated = await bootstrap_installed_package(
        updated_source.digest, reviewed, store, installer, kernel=core,
        initial_configuration=None, approved_capabilities=approved,
    )
    candidate = RuntimeModuleRegistry(
        host, _isolated_runtime_descriptors(*core.descriptors, updated.descriptor)
    )
    plan = candidate.preview_graph_transition(
        registry, target_component_id="forest.counter"
    )
    assert plan.replace == ("forest.counter",)
    assert all(item.startswith("kernel.") for item in plan.retain)
    affected = candidate.plan_retained_generations(registry, "forest.counter")
    await candidate.prewarm()
    await registry.drain_owned_work_for_replacement(
        candidate.activation_id,
        components=affected if len(affected) < len(registry.order) else None,
    )
    await candidate.start()
    candidate.accept_retained_ownership()
    assert await registry.close() == []
    assert candidate.graph_identity.generations["forest.counter"] != (
        first_graph.generations["forest.counter"]
    )
    for component_id in first_graph.generations:
        if component_id.startswith("kernel."):
            assert candidate.graph_identity.generations[component_id] == (
                first_graph.generations[component_id]
            )
    assert await candidate.close() == []


@pytest.mark.postgres
async def test_reviewed_bot_effect_requires_owner_bound_host_driver(clean_authority, tmp_path):
    store, installer = clean_authority
    core = await bootstrap_kernel(store, installer)
    package_root = tmp_path / "effect-source"
    copytree(Path(__file__).parent / "fixtures" / "routing_component", package_root)
    with (package_root / "module.toml").open("a", encoding="utf-8") as manifest:
        manifest.write(
            "\n[[components.effect_services]]\n"
            'name = "tests.reviewed.echo"\n'
            'contract = "1.0"\n'
            'kind = "tests.reviewed.echo"\n'
            'semantics = "read"\n'
            'input_schema = "schemas/value.json"\n'
            'output_schema = "schemas/value.json"\n'
        )
    staging = PackageCatalog(tmp_path / "effect-staging")
    source = staging.build(package_root)
    archive = staging.export_archive(source.digest)
    approved = tuple(source.component["capabilities"]["requests"])
    ordinary = PackageCatalog(tmp_path / "effect-ordinary")
    ordinary.import_archive(archive)
    with pytest.raises(ForestError) as denied:
        await bootstrap_installed_package(
            source.digest, ordinary, store, installer, kernel=core,
            initial_configuration={}, approved_capabilities=approved,
        )
    assert denied.value.code == "PACKAGE_EFFECT_DRIVER_BINDING_INVALID"

    catalog = PackageCatalog(tmp_path / "effect-reviewed")
    catalog.publish_reviewed_bot_archive(
        archive,
        expected_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
        expected_package_digest=source.digest,
        expected_manifest_digest=digest_json(source.manifest),
        expected_package_id=source.manifest["package_id"],
        expected_component_id=source.component["id"],
        expected_runtime="python-worker",
    )
    source_ref = "sha256:" + "a" * 64
    contract_fact = {
        "kind": "tests.reviewed.echo",
        "source_ref": source_ref,
        "contract": "1.0",
        "semantics": "read",
        "input_schema_digest": digest_json(source.schema("schemas/value.json")),
        "output_schema_digest": digest_json(source.schema("schemas/value.json")),
    }
    def factory(_settings, _revision, _generation, _deps):
        return None
    registry = PackageEffectDriverRegistry(
        {"tests.reviewed.echo": factory},
        source_refs={"tests.reviewed.echo": source_ref},
        contract_facts={"tests.reviewed.echo": contract_fact},
    )
    provisioned = await bootstrap_installed_package(
        source.digest, catalog, store, installer, kernel=core,
        initial_configuration={}, approved_capabilities=approved,
        effect_driver_registry=registry,
    )
    assert provisioned.descriptor.artifact.trust_level == "reviewed"
    with pytest.raises(ForestError) as missing:
        await compile_installed_candidate_descriptor(
            source.digest, catalog, store, kernel=core,
            approved_capabilities=approved,
            expected_manifest_digest=digest_json(source.manifest),
            expected_configuration_revision=1,
        )
    assert missing.value.code == "PACKAGE_EFFECT_DRIVER_UNAVAILABLE"
    inspection, candidate = await compile_installed_candidate_descriptor(
        source.digest, catalog, store, kernel=core,
        approved_capabilities=approved,
        expected_manifest_digest=digest_json(source.manifest),
        expected_configuration_revision=1,
        effect_driver_registry=registry,
    )
    assert inspection.trust_level == candidate.artifact.trust_level == "reviewed"
    assert candidate.effect_driver_bindings == (
        ("tests.reviewed.echo", "tests.reviewed.echo", source_ref),
    )
    invalid_registry = PackageEffectDriverRegistry(
        {"tests.reviewed.echo": factory},
        source_refs={"tests.reviewed.echo": source_ref},
        contract_facts={
            "tests.reviewed.echo": {
                **contract_fact,
                "output_schema_digest": "sha256:" + "0" * 64,
            },
        },
    )
    with pytest.raises(ForestError) as incompatible:
        await compile_installed_candidate_descriptor(
            source.digest, catalog, store, kernel=core,
            approved_capabilities=approved,
            expected_manifest_digest=digest_json(source.manifest),
            expected_configuration_revision=1,
            effect_driver_registry=invalid_registry,
        )
    assert incompatible.value.code == "PACKAGE_EFFECT_CONTRACT_MISMATCH"
