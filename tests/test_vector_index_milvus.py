from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from psycopg import sql
from psycopg.types.json import Jsonb
from pymilvus import DataType
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.vector_index_milvus.python.vector_index import MilvusVectorIndex
from forest.builtin_packages import (
    bootstrap_builtin_package,
    official_builtin_configuration,
    official_builtin_configuration_schema,
)
from forest.core_runtime import bootstrap_kernel
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.package_effects import PackageEffectDriverRegistry
from forest.packages import PackageCatalog, digest_json
from forest.schema_bundles import module_schema_namespace
from forest.vector_index_effect_driver import (
    MilvusVectorIndexEffectDriver,
    milvus_vector_index_effect_driver_factory,
    vector_collection_name,
)


def _with_replay(row):
    vector = row["vector"]
    return {
        **row,
        "replay": {
            "result": {
                "payload_id": "replay/" + row["ref_id"],
                "digest": "sha256:" + "a" * 64,
                "owner_component_id": "embedding.multimodal",
                "owner_kind": "embedding.result",
                "owner_id": "attempt/" + row["ref_id"],
            },
            "item_index": 0,
            "vector_digest": digest_json(vector),
        },
    }


def _bind_package_startup(driver, store):
    namespace = module_schema_namespace("forest.vector.index")

    async def call(name, payload, *, operation_id):
        function = {
            "vector.collection.plan": "vector_collection_plan",
            "vector.collection.confirm": "vector_collection_confirm",
        }[name]
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace), sql.Identifier(function)
                    ),
                    (Jsonb({"bot_id": store.bot_id, "operation_id": operation_id}),
                     Jsonb(payload)),
                )
            ).fetchone()
        envelope = row["result"]
        if not envelope["ok"]:
            error = envelope["error"]
            raise ForestError(error["code"], error["message"], error["status"])
        return envelope["value"]

    driver.bind_startup_commands(call)
    return driver


@pytest.mark.asyncio
async def test_vector_package_validates_replay_payload_before_milvus_effect():
    component = MilvusVectorIndex()
    component.space_id = "space/test"
    component.dimensions = 4
    request = _with_replay({
        "ref_id": "facet/one", "space_id": "space/test",
        "purpose": "DOCUMENT", "vector": [1.0, 0.0, 0.0, 0.0],
    })
    calls = []

    async def read_json(reference):
        assert reference == request["replay"]["result"]
        return {
            "protocol": "forest.embedding-result/1", "status": "RESPONDED",
            "space_id": "space/test",
            "vectors": [{"index": 0, "facet_id": "facet/one",
                         "values": [1.0, 0.0, 0.0, 0.0]}],
        }

    async def call(service, payload, **options):
        calls.append((service, payload, options))
        return {"ref_id": "facet/one", "space_id": "space/test"}

    command_calls = []

    async def command(name, payload, **_options):
        command_calls.append(name)
        if name == "vector.ref.reserve":
            return {
                "ref_id": payload["ref_id"], "space_id": payload["space_id"],
                "input_digest": digest_json(payload), "state": "PENDING",
            }
        assert name == "vector.ref.activate"
        return {"ref_id": payload["ref_id"], "space_id": "space/test",
                "state": "ACTIVE"}

    ctx = SimpleNamespace(
        payloads=SimpleNamespace(read_json=read_json),
        commands=SimpleNamespace(call=command),
        services=SimpleNamespace(call=call),
        operations=SimpleNamespace(stable=lambda value: value),
    )
    assert await component.upsert(request, ctx) == {
        "ref_id": "facet/one", "space_id": "space/test"
    }
    assert command_calls == ["vector.ref.reserve", "vector.ref.activate"]
    assert len(calls) == 1 and calls[0][0] == "vector.transport.upsert"
    assert calls[0][1]["input_digest"] == digest_json(request)
    changed = {**request, "vector": [0.0, 1.0, 0.0, 0.0]}
    with pytest.raises(ForestError) as mismatch:
        await component.upsert(changed, ctx)
    assert mismatch.value.code == "VECTOR_REPLAY_VECTOR_INVALID"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_vector_index_configuration_rejects_mutable_space_shape():
    assert not Draft202012Validator(
        official_builtin_configuration_schema("vector_index_milvus")
    ).is_valid(
            {
                "protocol": "forest.vector-index.milvus/1",
                "endpoint": "http://milvus:19530",
                "credential_ref": "",
                "timeout_seconds": 10,
                "space_id": "space/test",
                "dimensions": 0,
                "metric": "COSINE",
            }
        )


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_embedding_and_vector_identity_are_package_declared_immutable_paths(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    catalog = PackageCatalog(tmp_path / "packages")
    embedding_value = official_builtin_configuration("embedding_multimodal")
    embedding = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/embedding_multimodal",
        catalog, store, installer, kernel=kernel,
        initial_configuration=embedding_value,
        effect_driver_registry=PackageEffectDriverRegistry({
            "embedding.multimodal.http": lambda *_args: None,
        }),
    )
    embedding_schema = embedding.snapshot.schema(
        embedding.snapshot.component["config_schema"]
    )
    embedding_digest = digest_json(embedding_schema)
    endpoint_only = {**embedding_value, "endpoint": "https://example.org/embeddings"}
    await embedding.configuration.plan(
        operation_id="embedding.endpoint.change", expected_desired_revision=1,
        value=endpoint_only, validation_digest=embedding_digest,
    )
    with pytest.raises(ForestError) as embedding_error:
        await embedding.configuration.plan(
            operation_id="embedding.profile.change", expected_desired_revision=1,
            value={
                **embedding_value,
                "profile": {**embedding_value["profile"], "model_revision": "other"},
            },
            validation_digest=embedding_digest,
        )
    assert embedding_error.value.code == "MODULE_CONFIGURATION_IMMUTABLE"

    profile = EmbeddingProfile(**embedding_value["profile"])
    vector_value = official_builtin_configuration(
        "vector_index_milvus",
        overrides={"space_id": profile.space_id, "dimensions": profile.dimensions},
    )
    vector = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        catalog, store, installer, kernel=kernel,
        initial_configuration=vector_value,
        effect_driver_registry=PackageEffectDriverRegistry({
            "vector-index.milvus": lambda *_args: None,
        }),
    )
    vector_digest = digest_json(vector.snapshot.schema(vector.snapshot.component["config_schema"]))
    await vector.configuration.plan(
        operation_id="vector.endpoint.change", expected_desired_revision=1,
        value={**vector_value, "endpoint": "http://127.0.0.1:19530"},
        validation_digest=vector_digest,
    )
    with pytest.raises(ForestError) as vector_error:
        await vector.configuration.plan(
            operation_id="vector.space.change", expected_desired_revision=1,
            value={**vector_value, "dimensions": vector_value["dimensions"] - 1},
            validation_digest=vector_digest,
        )
    assert vector_error.value.code == "MODULE_CONFIGURATION_IMMUTABLE"


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_vector_ref_domain_fences_late_publication(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    space_id = "space/ref/" + uuid4().hex
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "ref-packages"),
        store, installer, kernel=kernel,
        initial_configuration=official_builtin_configuration(
            "vector_index_milvus",
            overrides={"space_id": space_id, "dimensions": 4},
        ),
        effect_driver_registry=PackageEffectDriverRegistry({
            "vector-index.milvus": lambda *_args: None,
        }),
    )
    namespace = module_schema_namespace("forest.vector.index")

    async def invoke(function, payload, *, digest=None, operation_id=None):
        context = {"bot_id": store.bot_id}
        if digest is not None:
            context["input_digest"] = digest
        if operation_id is not None:
            context["operation_id"] = operation_id
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace), sql.Identifier(function)
                    ),
                    (Jsonb(context), Jsonb(payload)),
                )
            ).fetchone()
        return row["result"]

    document = _with_replay({
        "ref_id": "ref/document", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [1.0, 0.0, 0.0, 0.0],
    })
    shape = {
        "space_id": space_id,
        "collection_name": vector_collection_name(store.bot_id, space_id),
        "dimensions": 4, "metric": "COSINE",
    }
    absent = {**shape, "exists": False, "collection_id": None}
    plan = "vector_collection_plan"
    assert (await invoke(plan, absent, operation_id="startup/a"))["value"] == {
        "state": "CREATE", "reason": None, "claim_token": "startup/a",
    }
    assert (await invoke(plan, absent, operation_id="startup/b"))["value"][
        "state"
    ] == "WAIT"
    assert (await invoke(
        "vector_ref_reserve", document, digest=digest_json(document)
    ))["error"]["code"] == "VECTOR_INDEX_COLLECTION_STARTING"
    confirmation = {**shape, "collection_id": 42, "claim_token": "startup/a"}
    assert (await invoke(
        "vector_collection_confirm",
        {**confirmation, "claim_token": "startup/b"},
    ))["error"]["code"] == "VECTOR_INDEX_COLLECTION_CLAIM_STALE"
    assert (await invoke(
        "vector_collection_confirm", confirmation
    ))["value"]["state"] == "READY"
    assert (await invoke(
        plan, {**shape, "exists": True, "collection_id": 42},
        operation_id="startup/b",
    ))["value"]["state"] == "READY"
    assert (await invoke(
        plan, {**shape, "exists": True, "collection_id": 43},
        operation_id="startup/b",
    ))["value"]["state"] == "REBUILD_REQUIRED"
    digest = digest_json(document)
    reserve = "vector_ref_reserve"
    activate = "vector_ref_activate"
    tombstone = "vector_ref_tombstone"
    assert (await invoke(reserve, document, digest=digest))["value"]["state"] == "PENDING"
    assert (await invoke(reserve, document, digest=digest))["value"]["state"] == "PENDING"
    assert (await invoke(reserve, document, digest="sha256:" + "0" * 64))[
        "error"
    ]["code"] == "VECTOR_INDEX_REF_CONFLICT"
    assert (await invoke(activate, {
        "ref_id": document["ref_id"], "input_digest": digest,
    }))["value"]["state"] == "ACTIVE"
    assert (await invoke(reserve, document, digest=digest))["value"]["state"] == "ACTIVE"
    assert (await invoke(activate, {
        "ref_id": document["ref_id"], "input_digest": digest,
    }))["value"]["state"] == "ACTIVE"

    pending = _with_replay({
        "ref_id": "ref/pending", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [0.0, 1.0, 0.0, 0.0],
    })
    pending_digest = digest_json(pending)
    assert (await invoke(reserve, pending, digest=pending_digest))["ok"]
    assert (await invoke(tombstone, {
        "space_id": space_id, "ref_ids": ["ref/pending", "ref/document", "ref/first"],
    }))["ok"]
    assert (await invoke(activate, {
        "ref_id": "ref/pending", "input_digest": pending_digest,
    }))["error"]["code"] == "VECTOR_INDEX_REF_DELETED"
    assert (await invoke(reserve, pending, digest=pending_digest))[
        "error"
    ]["code"] == "VECTOR_INDEX_REF_DELETED"
    first = _with_replay({
        "ref_id": "ref/first", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [0.0, 0.0, 1.0, 0.0],
    })
    assert (await invoke(reserve, first, digest=digest_json(first)))[
        "error"
    ]["code"] == "VECTOR_INDEX_REF_DELETED"

    query = _with_replay({
        "ref_id": "ref/query", "space_id": space_id,
        "purpose": "QUERY", "vector": [1.0, 0.0, 0.0, 0.0],
    })
    query_digest = digest_json(query)
    assert (await invoke(reserve, query, digest=query_digest))["ok"]
    assert (await invoke("vector_ref_require_query", {
        "query_ref_id": "ref/query", "space_id": space_id, "limit": 1,
    }))["error"]["code"] == "VECTOR_INDEX_QUERY_REF_MISSING"
    assert (await invoke(activate, {
        "ref_id": "ref/query", "input_digest": query_digest,
    }))["ok"]
    assert (await invoke("vector_ref_require_query", {
        "query_ref_id": "ref/query", "space_id": space_id, "limit": 1,
    }))["ok"]
    live = _with_replay({
        "ref_id": "ref/live", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [1.0, 0.0, 0.0, 0.0],
    })
    assert (await invoke(reserve, live, digest=digest_json(live)))["ok"]
    assert (await invoke(activate, {
        "ref_id": live["ref_id"], "input_digest": digest_json(live),
    }))["ok"]
    hits = {
        "query_ref_id": "ref/query", "space_id": space_id,
        "hits": [
            {"ref_id": "ref/document", "score": 0.9},
            {"ref_id": "ref/live", "score": 0.8},
            {"ref_id": "ref/first", "score": 0.7},
        ],
    }
    filtered = await invoke("vector_ref_filter_active", hits)
    assert filtered["value"]["hits"] == [hits["hits"][1]]
    assert (await invoke(tombstone, {
        "space_id": space_id, "ref_ids": ["ref/live"],
    }))["ok"]
    assert (await invoke("vector_ref_filter_active", hits))["value"]["hits"] == []
    conflict = await invoke(tombstone, {
        "space_id": "space/other", "ref_ids": ["ref/atomic-new", "ref/query"],
    })
    assert conflict["error"]["code"] == "VECTOR_INDEX_SPACE_MISMATCH"
    assert (await invoke(reserve, _with_replay({
        "ref_id": "ref/atomic-new", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [0.0, 1.0, 0.0, 0.0],
    }), digest=digest_json(_with_replay({
        "ref_id": "ref/atomic-new", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [0.0, 1.0, 0.0, 0.0],
    }))))["ok"]
    assert (await invoke(tombstone, {
        "space_id": space_id, "ref_ids": ["ref/query"],
    }))["ok"]
    assert (await invoke("vector_ref_filter_active", hits))["error"]["code"] == (
        "VECTOR_INDEX_QUERY_REF_MISSING"
    )

    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT ref_id,state,input_digest,replay_locator,next_sweep_at "
                    "FROM {}.vector_index_refs WHERE bot_id=%s ORDER BY ref_id"
                ).format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
        ).fetchall()
    assert [(row["ref_id"], row["state"]) for row in rows] == [
        ("ref/atomic-new", "PENDING"), ("ref/document", "TOMBSTONED"),
        ("ref/first", "TOMBSTONED"), ("ref/live", "TOMBSTONED"),
        ("ref/pending", "TOMBSTONED"), ("ref/query", "TOMBSTONED"),
    ]
    assert all(row["input_digest"] is None and row["replay_locator"] is None
               and row["next_sweep_at"] is not None for row in rows[1:])


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_missing_collection_with_active_refs_requires_rebuild(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    configuration = official_builtin_configuration(
        "vector_index_milvus",
        overrides={"space_id": f"space/test/{uuid4().hex}", "dimensions": 4},
    )
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "vector-packages"),
        store, installer, kernel=kernel, initial_configuration=configuration,
        effect_driver_registry=PackageEffectDriverRegistry({
            "vector-index.milvus": lambda *_args: None,
        }),
    )

    class MissingCollection:
        closed = False

        async def has_collection(self, *, collection_name):
            return False

        def create_schema(self, **_values):
            raise AssertionError("An empty collection must not replace published refs")

        async def close(self):
            self.closed = True

    clients = []

    def client_factory(**_values):
        client = MissingCollection()
        clients.append(client)
        return client

    async def verify_generation(**_values):
        return None

    async def resolve_token(_ref, _revision):
        return ""

    identity = SimpleNamespace(
        component_id="vector.index", generation_id="generation/test"
    )
    runtime = SimpleNamespace(verify_generation_member=verify_generation)

    def driver_for(service):
        return _bind_package_startup(MilvusVectorIndexEffectDriver(
            configuration, 1, identity,
            {"name": service, "kind": "vector-index.milvus"},
            bot_id=store.bot_id, runtime=runtime,
            token_resolver=resolve_token, client_factory=client_factory,
        ), store)

    search = driver_for("vector.transport.search")
    prune = driver_for("vector.transport.prune")
    namespace = module_schema_namespace("forest.vector.index")
    replay = _with_replay({
        "ref_id": "ref/document/existing", "space_id": configuration["space_id"],
        "purpose": "DOCUMENT", "vector": [1.0, 0.0, 0.0, 0.0],
    })
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.vector_index_refs "
                "(bot_id,ref_id,space_id,state,purpose,input_digest,"
                "replay_locator,vector_digest) "
                "VALUES (%s,%s,%s,'PENDING','DOCUMENT',%s,%s,%s)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, replay["ref_id"], configuration["space_id"],
             digest_json(replay), Jsonb(replay["replay"]),
             replay["replay"]["vector_digest"]),
        )
    await search.start()
    await prune.start()
    assert search.accepting and search.rebuild_required
    assert prune.accepting and prune.rebuild_required
    for driver, payload in (
        (search, {"query_ref_id": "ref/query",
                  "space_id": configuration["space_id"], "limit": 1}),
        (prune, {"ref_ids": ["ref/document/existing"],
                 "space_id": configuration["space_id"]}),
    ):
        with pytest.raises(ForestError) as missing:
            await driver.invoke(
                payload, scope=identity,
                invocation=SimpleNamespace(operation_id="missing"),
            )
        assert missing.value.code == "VECTOR_INDEX_REBUILD_REQUIRED"
    await search.close()
    await prune.close()
    assert all(client.closed for client in clients)



@pytest.mark.asyncio
@pytest.mark.postgres
async def test_tombstone_sweep_claim_and_settle_are_package_owned(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    space_id = "space/sweep/" + uuid4().hex
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "sweep-packages"),
        store, installer, kernel=kernel,
        initial_configuration=official_builtin_configuration(
            "vector_index_milvus",
            overrides={"space_id": space_id, "dimensions": 4},
        ),
        effect_driver_registry=PackageEffectDriverRegistry({
            "vector-index.milvus": lambda *_args: None,
        }),
    )
    namespace = module_schema_namespace("forest.vector.index")
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.vector_index_refs"
                "(bot_id,ref_id,space_id,state,tombstoned_at,next_sweep_at) "
                "VALUES (%s,'ref/a',%s,'TOMBSTONED',clock_timestamp(),"
                "clock_timestamp()-interval '1 second'),"
                "(%s,'ref/b',%s,'TOMBSTONED',clock_timestamp(),"
                "clock_timestamp()-interval '1 second')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, space_id, store.bot_id, space_id),
        )

    async def invoke(function, payload):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace), sql.Identifier(function)
                    ),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(payload)),
                )
            ).fetchone()
        return row["result"]

    claim = {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "claim/vector-sweep/one", "lease_seconds": 60,
    }
    first = await invoke("vector_claim_tombstone_sweep", claim)
    assert first["ok"]
    work = first["value"]
    assert work["payload"] == {"space_id": space_id, "ref_ids": ["ref/a", "ref/b"]}
    assert (await invoke("vector_claim_tombstone_sweep", claim))["value"] == work
    assert (await invoke("vector_claim_tombstone_sweep", {
        **claim, "claim_token": "claim/vector-sweep/two",
    }))["value"] is None

    settlement = {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": work["work_id"], "claim_token": claim["claim_token"],
        "attempt": work["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": work["payload"]},
    }
    settled = await invoke("vector_settle_tombstone_sweep", settlement)
    assert settled["ok"]
    assert (await invoke("vector_settle_tombstone_sweep", settlement)) == settled
    conflict = await invoke("vector_settle_tombstone_sweep", {
        **settlement, "outcome": {"status": "FAILED", "error": {"code": "test"}},
    })
    assert conflict["error"]["code"] == "VECTOR_SWEEP_SETTLE_CONFLICT"
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT ref_id,sweep_count,sweep_claim_token,"
                    "next_sweep_at>clock_timestamp() AS rescheduled "
                    "FROM {}.vector_index_refs WHERE bot_id=%s ORDER BY ref_id"
                ).format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
        ).fetchall()
    assert [(row["ref_id"], row["sweep_count"], row["sweep_claim_token"],
             row["rescheduled"]) for row in rows] == [
        ("ref/a", 1, None, True), ("ref/b", 1, None, True),
    ]

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.vector_sweep_settlements"
                "(bot_id,claim_token,input_payload,result_payload,settled_at) "
                "SELECT %s,'old/'||g,'{{}}'::jsonb,'{{}}'::jsonb,"
                "clock_timestamp()-interval '49 hours' "
                "FROM generate_series(1,130) AS g"
            ).format(sql.Identifier(namespace)),
            (store.bot_id,),
        )
    for token, expected_left in (("gc/first", 2), ("gc/second", 0)):
        assert (await invoke("vector_claim_tombstone_sweep", {
            **claim, "claim_token": token,
        }))["value"] is None
        async with store.pool.connection() as conn:
            counts = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) FILTER (WHERE claim_token LIKE 'old/%%') "
                        "AS expired, count(*) FILTER "
                        "(WHERE claim_token=%s) AS fresh "
                        "FROM {}.vector_sweep_settlements WHERE bot_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (claim["claim_token"], store.bot_id),
                )
            ).fetchone()
        assert counts == {"expired": expected_left, "fresh": 1}

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.vector_index_refs"
                "(bot_id,ref_id,space_id,state,tombstoned_at,next_sweep_at) "
                "VALUES (%s,'ref/c',%s,'TOMBSTONED',clock_timestamp(),"
                "clock_timestamp()-interval '1 second')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, space_id),
        )
    short_claim = {**claim, "claim_token": "claim/vector-sweep/expired",
                   "lease_seconds": 3}
    expired = (await invoke("vector_claim_tombstone_sweep", short_claim))["value"]
    assert expired["payload"]["ref_ids"] == ["ref/c"]
    await asyncio.sleep(3.2)
    successor_claim = {**claim, "claim_token": "claim/vector-sweep/successor"}
    successor = (await invoke("vector_claim_tombstone_sweep", successor_claim))["value"]
    assert successor["payload"]["ref_ids"] == ["ref/c"]
    stale = await invoke("vector_settle_tombstone_sweep", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": expired["work_id"], "claim_token": short_claim["claim_token"],
        "attempt": 1, "outcome": {"status": "SUCCEEDED", "result": expired["payload"]},
    })
    assert stale["error"]["code"] == "VECTOR_SWEEP_CLAIM_STALE"
    assert (await invoke("vector_settle_tombstone_sweep", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": successor["work_id"], "claim_token": successor_claim["claim_token"],
        "attempt": 1, "outcome": {"status": "FAILED", "error": {"code": "NETWORK"}},
    }))["ok"]


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_milvus_vector_index_round_trip_by_reference(clean_authority, tmp_path):
    uri = os.environ.get("FOREST_TEST_MILVUS_URI")
    if not uri:
        pytest.skip("FOREST_TEST_MILVUS_URI is not configured")

    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    bot_id = store.bot_id
    space_id = f"space/test/{uuid4().hex}"
    configuration = official_builtin_configuration(
        "vector_index_milvus",
        overrides={"endpoint": uri, "space_id": space_id, "dimensions": 4},
    )

    async def verify_generation(**_values):
        return None

    async def resolve_token(_ref, _revision):
        return ""

    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "vector-packages"),
        store, installer, kernel=kernel, initial_configuration=configuration,
        effect_driver_registry=PackageEffectDriverRegistry({
            "vector-index.milvus": milvus_vector_index_effect_driver_factory(
                bot_id=bot_id, runtime=kernel.runtime,
                token_resolver=resolve_token,
            )
        }),
    )
    identity = SimpleNamespace(
        component_id="vector.index", generation_id="generation/vector-test"
    )
    runtime = SimpleNamespace(verify_generation_member=verify_generation)

    def driver_for(service):
        return _bind_package_startup(MilvusVectorIndexEffectDriver(
            configuration, 1, identity,
            {"name": service, "kind": "vector-index.milvus"},
            bot_id=bot_id, runtime=runtime,
            token_resolver=resolve_token,
        ), store)

    drivers = {
        name: driver_for(name) for name in (
            "vector.transport.upsert", "vector.transport.search",
            "vector.transport.prune",
        )
    }
    scope = identity
    invocation = SimpleNamespace(operation_id="test")
    collection_name = drivers["vector.transport.upsert"].collection_name

    def transport_row(ref_id, purpose, vector):
        public = _with_replay({
            "ref_id": ref_id, "space_id": space_id,
            "purpose": purpose, "vector": vector,
        })
        return {**public, "input_digest": digest_json(public)}

    try:
        await asyncio.gather(*(driver.start() for driver in drivers.values()))
        assert not any(driver.rebuild_required for driver in drivers.values())
        upsert = drivers["vector.transport.upsert"]
        collection = await upsert.client.describe_collection(
            collection_name=collection_name
        )
        assert {field["name"] for field in collection["fields"]} == {
            "ref_id", "bot_id", "space_id", "purpose", "input_digest", "embedding",
        }

        class LostAcknowledgement:
            def __init__(self, client):
                self.client = client
                self.fail_once = True

            def __getattr__(self, name):
                return getattr(self.client, name)

            async def upsert(self, **values):
                result = await self.client.upsert(**values)
                if self.fail_once:
                    self.fail_once = False
                    raise OSError("injected lost Milvus acknowledgement")
                return result

        upsert.client = LostAcknowledgement(upsert.client)
        near = transport_row(
            "ref/document/near", "DOCUMENT", [1.0, 0.0, 0.0, 0.0]
        )
        with pytest.raises(ForestError) as uncertain:
            await upsert.invoke(near, scope=scope, invocation=invocation)
        assert uncertain.value.code == "VECTOR_INDEX_UPSERT_UNAVAILABLE"
        assert await upsert.client.get(
            collection_name=collection_name, ids=["ref/document/near"],
            output_fields=["ref_id"], consistency_level="Strong",
        ) == [{"ref_id": "ref/document/near"}]
        namespace = module_schema_namespace("forest.vector.index")
        async with store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    sql.SQL("SELECT ref_id FROM {}.vector_index_refs WHERE bot_id=%s")
                    .format(sql.Identifier(namespace)), (bot_id,),
                )
            ).fetchall()
        assert rows == []

        await upsert.close()
        upsert = driver_for("vector.transport.upsert")
        drivers["vector.transport.upsert"] = upsert
        await upsert.start()
        for row in (
            near,
            transport_row("ref/document/far", "DOCUMENT", [0.0, 1.0, 0.0, 0.0]),
            transport_row("ref/query", "QUERY", [1.0, 0.0, 0.0, 0.0]),
        ):
            assert await upsert.invoke(
                row, scope=scope, invocation=invocation,
            ) == {"ref_id": row["ref_id"], "space_id": space_id}
        result = await drivers["vector.transport.search"].invoke(
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
            scope=scope, invocation=invocation,
        )
        assert [hit["ref_id"] for hit in result["hits"]] == [
            "ref/document/near", "ref/document/far",
        ]
        assert all(set(hit) == {"ref_id", "score"} for hit in result["hits"])
        await drivers["vector.transport.prune"].invoke(
            {"ref_ids": ["ref/document/near"], "space_id": space_id},
            scope=scope, invocation=invocation,
        )
        result = await drivers["vector.transport.search"].invoke(
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
            scope=scope, invocation=invocation,
        )
        assert [hit["ref_id"] for hit in result["hits"]] == ["ref/document/far"]

        replay = near["replay"]
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.vector_index_refs "
                    "(bot_id,ref_id,space_id,state,purpose,input_digest,"
                    "replay_locator,vector_digest,published_at) "
                    "VALUES (%s,%s,%s,'ACTIVE','DOCUMENT',%s,%s,%s,"
                    "clock_timestamp())"
                ).format(sql.Identifier(namespace)),
                (bot_id, near["ref_id"], space_id, near["input_digest"],
                 Jsonb(replay), replay["vector_digest"]),
            )
            binding = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT * FROM {}.vector_collection_bindings "
                        "WHERE bot_id=%s ORDER BY binding_revision DESC LIMIT 1"
                    ).format(sql.Identifier(namespace)),
                    (bot_id,),
                )
            ).fetchone()
        assert binding["binding_revision"] == 1
        original_collection_id = binding["collection_id"]
        client = upsert.client
        await client.drop_collection(collection_name=collection_name)
        replacement_schema = client.create_schema(
            auto_id=False, enable_dynamic_field=False
        )
        replacement_schema.add_field(
            "ref_id", DataType.VARCHAR, is_primary=True, max_length=256
        )
        replacement_schema.add_field("bot_id", DataType.VARCHAR, max_length=256)
        replacement_schema.add_field("space_id", DataType.VARCHAR, max_length=256)
        replacement_schema.add_field("purpose", DataType.VARCHAR, max_length=16)
        replacement_schema.add_field(
            "input_digest", DataType.VARCHAR, max_length=71
        )
        replacement_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4)
        indexes = client.prepare_index_params()
        indexes.add_index(
            field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE"
        )
        await client.create_collection(
            collection_name=collection_name, schema=replacement_schema,
            index_params=indexes, consistency_level="Strong",
        )
        description = await client.describe_collection(
            collection_name=collection_name
        )
        assert description["collection_id"] != original_collection_id
        await drivers["vector.transport.search"].close()
        replacement = driver_for("vector.transport.search")
        drivers["vector.transport.search"] = replacement
        await replacement.start()
        assert replacement.rebuild_required
        with pytest.raises(ForestError) as replaced:
            await replacement.invoke(
                {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
                scope=scope, invocation=invocation,
            )
        assert replaced.value.code == "VECTOR_INDEX_REBUILD_REQUIRED"
    finally:
        client = drivers["vector.transport.upsert"].client
        if client is not None and await client.has_collection(
            collection_name=collection_name
        ):
            await client.drop_collection(collection_name=collection_name)
        for driver in drivers.values():
            await driver.close()
