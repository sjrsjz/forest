from __future__ import annotations

import asyncio
import hashlib
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.runtime_modules import official_storage_bundle
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)
from forest.storage.bot import BotStore
from forest.storage.kernel_audit import AuditEnvelope, KernelAuditRepository
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
    parallel = await asyncio.gather(
        *(
            repository.put_bytes(
                owner_component_id="memory.graph",
                owner_kind="retrieval.evidence",
                owner_id="evidence/parallel",
                media_type="application/json",
                content=b'{"evidence":true}',
                parents=(parent.payload_id,),
                expected_deletion_epoch=0,
            )
            for _ in range(2)
        )
    )
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
