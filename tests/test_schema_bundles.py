from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.runtime_generation import (
    PreparedGenerationBinding,
    RuntimeStorageBinding,
)
from forest.runtime_modules import official_storage_bundle
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)
from forest.storage.authorization_authority import (
    CapabilityGrant,
    KernelAuthorizationRepository,
)
from forest.storage.bot import BotStore
from forest.storage.effect_authority import KernelEffectRepository
from forest.storage.kernel_audit import AuditEnvelope, KernelAuditRepository
from forest.storage.kernel_runtime import KernelRuntimeRepository
from forest.storage.payload_authority import (
    KernelPayloadRepository,
    PayloadSource,
    SourceTarget,
)
from forest.storage.schema_bundles import SchemaBundleInstaller


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
    row = await installer.install(
        spec, baseline, package_digest, execution_profile="official"
    )
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
            "calls",
            "state_heads",
            "runtime_graphs",
            "runtime_graph_modules",
        } <= names
        assert not any(
            token in name
            for name in names
            for token in ("agent", "memory", "onebot", "binding", "job")
        )
        seeded = await (
            await conn.execute(
                sql.SQL("SELECT bot_id FROM {}.runtime_state").format(
                    sql.Identifier(namespace)
                )
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
                sql.SQL("DELETE FROM {}.host_incarnations").format(
                    sql.Identifier(namespace)
                )
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
    await installer.install(
        runtime_storage.spec,
        runtime_storage.baseline,
        runtime_storage.package_digest,
        execution_profile=runtime_storage.execution_profile,
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
        runtime_storage.spec.repository_contract,
        runtime_storage.execution_profile,
    )
    kernel = runtime_binding("kernel.runtime", marker="1", storage=storage)
    kernel = replace(kernel, package_digest=runtime_storage.package_digest)
    consumer = runtime_binding(
        "example.consumer", marker="2", requires=("kernel.runtime",)
    )
    activation_id = str(uuid4())
    graph = await repository.prepare_graph(renewed, [consumer, kernel], activation_id)
    assert await repository.prepare_graph(renewed, [kernel, consumer], activation_id) == graph
    assert await repository.publish_graph(renewed, graph) == graph.graph_revision
    assert await repository.publish_graph(renewed, graph) == graph.graph_revision

    namespace = runtime_storage.spec.namespace
    async with store.pool.connection() as conn:
        head = await (
            await conn.execute(
                sql.SQL("SELECT * FROM {}.runtime_graph_head").format(
                    sql.Identifier(namespace)
                )
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
    consumer_v2 = runtime_binding(
        "example.consumer", marker="6", requires=("kernel.runtime",)
    )
    graph_v2 = await repository.prepare_graph(
        renewed, [kernel_v2, consumer_v2], str(uuid4())
    )
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
    assert {item["status"] for item in statuses if item["generation_id"] in graph.generations.values()} == {
        "QUIESCING"
    }
    assert {
        item["status"]
        for item in statuses
        if item["generation_id"] in graph_v2.generations.values()
    } == {"ACTIVE"}

    retired_revision = await repository.retire_graph(renewed, graph_v2)
    assert retired_revision > graph_v2.graph_revision
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
                sql.SQL("SELECT * FROM {}.runtime_graph_head").format(
                    sql.Identifier(namespace)
                )
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
            fact["state"]
            for fact in lifecycle
            if fact["component_id"] == item["component_id"]
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
            sql.SQL(
                "UPDATE {}.host_lease SET expires_at='1970-01-01 UTC' WHERE bot_id=%s"
            ).format(sql.Identifier(runtime_storage.spec.namespace)),
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
            sql.SQL(
                "UPDATE {}.host_lease SET expires_at='1970-01-01 UTC' WHERE bot_id=%s"
            ).format(sql.Identifier(runtime_storage.spec.namespace)),
            (store.bot_id,),
        )
    with pytest.raises(ForestError) as caught:
        await repository.claim_host("host-a", "inc-a")
    assert caught.value.code == "STALE_HOST"


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
        "entries": (
            CapabilityGrant("module.reload", "component", "example.worker"),
        ),
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
    assert await authority.revoke(
        operation_id="revoke/one",
        selector_kind="principal",
        selector_id="qq/1234",
        expected_policy_revision=1,
    ) == revocation
    assert await authority.consume_ticket(**consume_args) == receipt
    assert await authority.issue_ticket(
        ticket_id="ticket/pending",
        operation_id="operation/pending",
        expires_at=pending_expiry,
        **ticket_args,
    ) == pending_ticket
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
    assert await effects.admit(
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
    ) == effect

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
    handoff = await effects.mark_handoff(
        permit=permit, handoff_operation_id="handoff/send-one/1"
    )
    assert handoff["state"] == "DISPATCHING"
    assert await effects.mark_handoff(
        permit=permit, handoff_operation_id="handoff/send-one/1"
    ) == handoff

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
    assert await effects.abandon_prepared(
        effect_operation_id=abandoned_effect_id,
        attempt_id=first_abandoned.attempt_id,
        abandonment_operation_id="abandon/abandoned-retry/1",
        reason_code="PREPARE_EXPIRED",
    ) == abandonment
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
        await KernelAuditRepository(
            store, namespace=storage.spec.namespace
        ).append(envelope)
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
            AuditEnvelope(
                **{**envelope.__dict__, "action": "component.disable"}
            )
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
                    "SELECT deletion_epoch FROM {}.deletion_clock "
                    "WHERE bot_id=%s FOR SHARE"
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
    assert await repository.forget(
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
    ) == receipt
    async with store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as caught:
            await repository.read_bytes_in(
                conn, child.payload_id, expected_deletion_epoch=1
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
    first_source = PayloadSource(
        "conversation.scheduler", "group.message", "message/first", 1
    )
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

    second_source = PayloadSource(
        "conversation.scheduler", "group.message", "message/second", 1
    )
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
    second_receipt, rejected_code = await asyncio.gather(
        second_forget_task, second_admission_task
    )
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
