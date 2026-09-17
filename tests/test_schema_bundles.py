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
from forest.schema_bundles import (
    StorageBundleSpec,
    validate_official_storage_baseline,
    validate_storage_baseline,
)
from forest.storage.bot import BotStore
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
