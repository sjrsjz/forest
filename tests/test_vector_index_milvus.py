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

from forest.builtin.vector_index_milvus.python.vector_index import (
    MilvusVectorIndex,
    _physical_digest,
)
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
            "vector.collection.quarantine": "vector_collection_quarantine",
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

    async def query(name, payload):
        assert name == "vector.collection.current"
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.vector_collection_current(%s::jsonb,%s::jsonb) "
                            "AS result").format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(payload)),
                )
            ).fetchone()
        assert row["result"]["ok"]
        return row["result"]["value"]

    driver.bind_startup_queries(query)
    driver.bind_startup_commands(call)
    driver.bind_runtime_commands(call)
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
                "input_digest": payload["source_input_digest"], "state": "PENDING",
                "binding": {"binding_revision": 1, "collection_id": 42,
                            "collection_name": "test_collection"},
            }
        if name == "vector.io.enter":
            return {"ticket": "test-ticket", "binding": payload["binding"]}
        if name == "vector.io.exit":
            return {"ticket": payload["ticket"]}
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
    assert command_calls == ["vector.ref.reserve", "vector.io.enter",
                             "vector.ref.activate", "vector.io.exit"]
    assert calls[0][1]["input_digest"] == digest_json(request)
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
async def test_vector_startup_reobserves_binding_advanced_during_plan():
    bot_id = "bot/startup-race"
    space_id = "space/startup-race"
    base = vector_collection_name(bot_id, space_id)
    candidate = base + "_r2"
    bindings = (
        {"binding_revision": 1, "collection_id": 42,
         "collection_name": base},
        {"binding_revision": 2, "collection_id": 43,
         "collection_name": candidate},
    )

    class Physical:
        closed = False

        async def has_collection(self, *, collection_name):
            return collection_name in {base, candidate}

        async def describe_collection(self, *, collection_name):
            return {
                "collection_id": 42 if collection_name == base else 43,
                "fields": [
                    {"name": name, **({"params": {"dim": 4}} if name == "embedding"
                                    else {})}
                    for name in (
                        "ref_id", "bot_id", "space_id", "purpose",
                        "input_digest", "embedding",
                    )
                ],
            }

        async def close(self):
            self.closed = True

    physical = Physical()
    configuration = official_builtin_configuration(
        "vector_index_milvus", overrides={"space_id": space_id, "dimensions": 4}
    )

    async def token(_ref, _revision):
        return ""

    driver = MilvusVectorIndexEffectDriver(
        configuration, 1,
        SimpleNamespace(component_id="vector.index", generation_id="generation/race"),
        {"name": "vector.transport.search", "kind": "vector-index.milvus"},
        bot_id=bot_id, runtime=SimpleNamespace(), token_resolver=token,
        client_factory=lambda **_values: physical,
    )
    queried = 0
    observed = []

    async def current(name, payload):
        nonlocal queried
        assert name == "vector.collection.current" and payload == {}
        queried += 1
        return {"binding": bindings[0] if queried == 1 else bindings[1]}

    async def plan(name, payload, *, operation_id):
        assert name == "vector.collection.plan" and operation_id
        observed.append(payload["collection_name"])
        if len(observed) == 1:
            raise ForestError("VECTOR_INDEX_SPACE_IMMUTABLE", "binding advanced", 409)
        return {"state": "READY", "reason": None, "claim_token": None,
                "binding_revision": 2, "collection_id": 43}

    driver.bind_startup_queries(current)
    driver.bind_startup_commands(plan)
    await driver.start()
    assert observed == [base, candidate]
    assert driver.collection_name == candidate and driver.binding_revision == 2
    await driver.close()
    assert physical.closed


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
        if function in ("vector_ref_activate", "vector_ref_filter_active"):
            payload = {**payload, "binding": {
                "binding_revision": 1, "collection_id": 42,
                "collection_name": vector_collection_name(store.bot_id, space_id),
            }}
        if function == "vector_ref_reserve":
            payload = {
                **payload, "source_input_digest": digest or digest_json(payload),
                "physical_digest": _physical_digest(payload["vector"]),
            }
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
        "binding_revision": None, "collection_id": None,
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
    concurrent = [
        _with_replay({
            "ref_id": f"ref/concurrent/{index}", "space_id": space_id,
            "purpose": "DOCUMENT", "vector": [0.0, 1.0, 0.0, 0.0],
        }) for index in (1, 2)
    ]
    for item in concurrent:
        assert (await invoke(reserve, item, digest=digest_json(item)))["ok"]
    activated = await asyncio.gather(*(
        invoke(activate, {"ref_id": item["ref_id"],
                          "input_digest": digest_json(item)})
        for item in (*concurrent, *concurrent)
    ))
    assert all(result["ok"] for result in activated)
    deleted = await asyncio.gather(*(
        invoke(tombstone, {"space_id": space_id, "ref_ids": [item["ref_id"]]})
        for item in concurrent
    ))
    assert all(result["ok"] for result in deleted)
    assert (await invoke(tombstone, {
        "space_id": space_id, "ref_ids": [item["ref_id"] for item in concurrent],
    }))["ok"]
    head = await invoke("vector_ref_change_head", {})
    assert head["value"] == {"through_seq": 12}
    cursor = 0
    changes = []
    while cursor < 12:
        page = (await invoke("vector_ref_change_page", {
            "after_seq": cursor, "through_seq": 12, "limit": 5,
        }))["value"]
        assert page["next_seq"] > cursor
        changes.extend(page["changes"])
        cursor = page["next_seq"]
    assert [item["mutation_seq"] for item in changes] == list(range(1, 13))
    assert changes[0]["ref_id"] == document["ref_id"]
    assert changes[0]["state"] == "ACTIVE"
    assert changes[0]["replay_locator"] == document["replay"]
    assert changes[0]["input_digest"] == digest
    assert changes[0]["physical_digest"] == _physical_digest(document["vector"])
    assert {(item["ref_id"], item["state"]) for item in changes[8:10]} == {
        (item["ref_id"], "ACTIVE") for item in concurrent
    }
    assert {(item["ref_id"], item["state"]) for item in changes[10:]} == {
        (item["ref_id"], "TOMBSTONED") for item in concurrent
    }
    assert all(item["replay_locator"] is None for item in changes if
               item["state"] == "TOMBSTONED")
    assert (await invoke("vector_ref_change_page", {
        "after_seq": 12, "through_seq": 12, "limit": 5,
    }))["value"]["changes"] == []
    late = _with_replay({
        "ref_id": "ref/late", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [0.0, 0.0, 1.0, 0.0],
    })
    late_digest = digest_json(late)
    late_binding = (await invoke(reserve, late, digest=late_digest))["value"]["binding"]
    quarantined = await invoke("vector_collection_quarantine", {
        "binding_revision": 1, "collection_id": 42,
        "reason": "collection identity changed",
    })
    assert (await invoke(activate, {
        "ref_id": late["ref_id"], "input_digest": late_digest,
        "binding": late_binding,
    }))["error"]["code"] == "VECTOR_INDEX_BINDING_STALE"
    assert (await invoke("vector_ref_filter_active", hits))["error"]["code"] == (
        "VECTOR_INDEX_BINDING_STALE"
    )
    assert quarantined["value"]["state"] == "REBUILD_REQUIRED"
    assert (await invoke(plan, {**shape, "exists": True, "collection_id": 42},
                         operation_id="startup/c"))["value"]["state"] == "REBUILD_REQUIRED"
    assert (await invoke(reserve, document, digest=digest))["error"]["code"] == (
        "VECTOR_INDEX_REBUILD_REQUIRED"
    )
    assert (await invoke("vector_ref_require_query", {
        "query_ref_id": "ref/query", "space_id": space_id, "limit": 1,
    }))["error"]["code"] == "VECTOR_INDEX_REBUILD_REQUIRED"
    assert (await invoke(tombstone, {
        "space_id": space_id, "ref_ids": ["ref/live"],
    }))["ok"]

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
        ("ref/atomic-new", "PENDING"),
        ("ref/concurrent/1", "TOMBSTONED"),
        ("ref/concurrent/2", "TOMBSTONED"),
        ("ref/document", "TOMBSTONED"),
        ("ref/first", "TOMBSTONED"), ("ref/late", "PENDING"),
        ("ref/live", "TOMBSTONED"),
        ("ref/pending", "TOMBSTONED"), ("ref/query", "TOMBSTONED"),
    ]
    assert rows[5]["input_digest"] == late_digest
    assert rows[5]["replay_locator"] == late["replay"]
    assert all(row["input_digest"] is None and row["replay_locator"] is None
               and row["next_sweep_at"] is not None
               for row in rows[1:] if row["ref_id"] != "ref/late")


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_rebuild_replay_lost_lease_abandons_candidate(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    space_id = "space/replay-lease/" + uuid4().hex
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "replay-lease-packages"),
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
    ref = _with_replay({
        "ref_id": "ref/lease/live", "space_id": space_id,
        "purpose": "DOCUMENT", "vector": [1.0, 0.0, 0.0, 0.0],
    })
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_collection_bindings "
                    "(bot_id,binding_revision,space_id,collection_name,"
                    "collection_id,dimensions,metric) "
                    "VALUES (%s,1,%s,%s,41,4,'COSINE')")
            .format(sql.Identifier(namespace)),
            (store.bot_id, space_id,
             vector_collection_name(store.bot_id, space_id)),
        )
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_collection_health "
                    "(bot_id,binding_revision,state) VALUES (%s,1,'READY')")
            .format(sql.Identifier(namespace)), (store.bot_id,),
        )
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_index_refs "
                    "(bot_id,ref_id,space_id,state,purpose,input_digest,"
                    "replay_locator,vector_digest,physical_digest,published_at) "
                    "VALUES (%s,%s,%s,'ACTIVE','DOCUMENT',%s,%s,%s,%s,"
                    "clock_timestamp())")
            .format(sql.Identifier(namespace)),
            (store.bot_id, ref["ref_id"], space_id, digest_json(ref),
             Jsonb(ref["replay"]), ref["replay"]["vector_digest"],
             _physical_digest(ref["vector"])),
        )
        await conn.execute(
            sql.SQL("SELECT {}.vector_append_ref_change("
                    "%s,%s,%s,'ACTIVE','DOCUMENT',%s,%s,%s,%s)")
            .format(sql.Identifier(namespace)),
            (store.bot_id, ref["ref_id"], space_id, digest_json(ref),
             Jsonb(ref["replay"]), ref["replay"]["vector_digest"],
             _physical_digest(ref["vector"])),
        )

    async def domain(function, payload, operation_id="rebuild/lease"):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result")
                .format(sql.Identifier(namespace), sql.Identifier(function)),
                (Jsonb({"bot_id": store.bot_id,
                        "operation_id": operation_id}), Jsonb(payload)),
            )).fetchone()
        return row["result"]

    enqueued = (await domain(
        "vector_enqueue_rebuild", {"requested_by": "admin"},
    ))["value"]
    claim = {"protocol": "forest.owned-work-claim-request/1",
             "claim_token": "create/first", "lease_seconds": 120}
    creation = (await domain("vector_claim_rebuild_create", claim))["value"]
    candidate = {
        "request_id": enqueued["request_id"],
        "claim_token": creation["claim_token"],
        "candidate_revision": creation["payload"]["candidate_revision"],
        "collection_name": creation["payload"]["collection_name"],
        "collection_id": 42,
    }
    assert candidate["candidate_revision"] == 2
    confirmed = (await domain(
        "vector_confirm_rebuild_candidate", candidate,
    ))["value"]
    assert (await domain("vector_settle_rebuild_create", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": creation["work_id"],
        "claim_token": creation["claim_token"],
        "attempt": creation["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": confirmed},
    }))["ok"]
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL("UPDATE {}.vector_collection_health "
                    "SET state='REBUILD_REQUIRED',reason='base index unavailable' "
                    "WHERE bot_id=%s")
            .format(sql.Identifier(namespace)), (store.bot_id,),
        )
    replay = (await domain("vector_claim_rebuild_replay", {
        **claim, "claim_token": "replay/expired", "lease_seconds": 660,
    }))["value"]
    assert replay["payload"]["candidate_revision"] == 2
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL("UPDATE {}.vector_rebuild_work "
                    "SET lease_deadline=clock_timestamp()-interval '1 second' "
                    "WHERE bot_id=%s AND request_id=%s")
            .format(sql.Identifier(namespace)),
            (store.bot_id, enqueued["request_id"]),
        )
    assert (await domain("vector_claim_rebuild_replay", {
        **claim, "claim_token": "replay/successor", "lease_seconds": 660,
    }))["value"] is None
    replacement = (await domain("vector_claim_rebuild_create", {
        **claim, "claim_token": "create/replacement",
    }))["value"]
    assert replacement["payload"]["candidate_revision"] == 3
    assert replacement["payload"]["collection_name"] != candidate["collection_name"]
    late = await domain("vector_settle_rebuild_replay", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": replay["work_id"],
        "claim_token": replay["claim_token"],
        "attempt": replay["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": {
            "request_id": enqueued["request_id"],
            "candidate_revision": 2, "collection_id": 42, "next_seq": 1,
        }},
    })
    assert late["error"]["code"] == "VECTOR_REPLAY_CLAIM_STALE"


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_rebuild_candidate_audit_paginates_frozen_refs(
    clean_authority, tmp_path,
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    space_id = "space/audit-page/" + uuid4().hex
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "audit-page-packages"),
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
    bot_id = store.bot_id
    name = vector_collection_name(bot_id, space_id)
    digest = "sha256:" + "a" * 64
    physical = "f32-sha256:" + "b" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_collection_bindings "
                    "(bot_id,binding_revision,space_id,collection_name,"
                    "collection_id,dimensions,metric) "
                    "VALUES (%s,1,%s,%s,41,4,'COSINE')")
            .format(sql.Identifier(namespace)), (bot_id, space_id, name),
        )
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_collection_health "
                    "(bot_id,binding_revision,state) VALUES (%s,1,'READY')")
            .format(sql.Identifier(namespace)), (bot_id,),
        )
        for index in range(65):
            ref_id = f"ref/audit/{index:03d}"
            await conn.execute(
                sql.SQL("INSERT INTO {}.vector_index_refs "
                        "(bot_id,ref_id,space_id,state,purpose,input_digest,"
                        "replay_locator,vector_digest,physical_digest,published_at) "
                        "VALUES (%s,%s,%s,'ACTIVE','DOCUMENT',%s,%s,%s,%s,"
                        "clock_timestamp())")
                .format(sql.Identifier(namespace)),
                (bot_id, ref_id, space_id, digest, Jsonb({"test": index}),
                 digest, physical),
            )
            await conn.execute(
                sql.SQL("SELECT {}.vector_append_ref_change("
                        "%s,%s,%s,'ACTIVE','DOCUMENT',%s,%s,%s,%s)")
                .format(sql.Identifier(namespace)),
                (bot_id, ref_id, space_id, digest, Jsonb({"test": index}),
                 digest, physical),
            )
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_rebuild_work "
                    "(bot_id,request_id,input_digest,requested_by,space_id,"
                    "base_revision,dimensions,state,candidate_revision,"
                    "candidate_name,candidate_id,replay_waterline,replay_cursor) "
                    "VALUES (%s,'audit/page',%s,'admin',%s,1,4,"
                    "'CANDIDATE_READY',2,%s,42,65,65)")
            .format(sql.Identifier(namespace)),
            (bot_id, digest, space_id, name + "_r2"),
        )

    async def domain(function, payload, *, operation_id=None, allow_error=False):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result")
                .format(sql.Identifier(namespace), sql.Identifier(function)),
                (Jsonb({"bot_id": bot_id, "operation_id": operation_id}),
                 Jsonb(payload)),
            )).fetchone()
        if allow_error:
            return row["result"]
        assert row["result"]["ok"], row["result"]
        return row["result"]["value"]

    claim = {"protocol": "forest.owned-work-claim-request/1",
             "lease_seconds": 90}
    first = await domain("vector_claim_rebuild_audit", {
        **claim, "claim_token": "audit/page/first",
    })
    assert len(first["payload"]["expected"]) == 64
    assert first["payload"]["expected"][0]["ref_id"] == "ref/audit/000"
    assert first["payload"]["expected"][-1]["ref_id"] == "ref/audit/063"
    await domain("vector_settle_rebuild_audit", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": first["work_id"],
        "claim_token": first["claim_token"],
        "attempt": first["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": {
            "request_id": "audit/page", "candidate_revision": 2,
            "collection_id": 42, "audit_seq": 65,
            "next_ref": "ref/audit/063", "done": False,
        }},
    })
    second = await domain("vector_claim_rebuild_audit", {
        **claim, "claim_token": "audit/page/second",
    })
    assert second["payload"]["after_ref"] == "ref/audit/063"
    assert [row["ref_id"] for row in second["payload"]["expected"]] == [
        "ref/audit/064"
    ]
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL("UPDATE {}.vector_rebuild_work "
                    "SET lease_deadline=clock_timestamp()-interval '1 second' "
                    "WHERE bot_id=%s AND request_id='audit/page'")
            .format(sql.Identifier(namespace)), (bot_id,),
        )
    resumed = await domain("vector_claim_rebuild_audit", {
        **claim, "claim_token": "audit/page/resumed",
    })
    assert resumed["payload"]["after_ref"] == second["payload"]["after_ref"]
    assert resumed["payload"]["expected"] == second["payload"]["expected"]
    await domain("vector_settle_rebuild_audit", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": resumed["work_id"],
        "claim_token": resumed["claim_token"],
        "attempt": resumed["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": {
            "request_id": "audit/page", "candidate_revision": 2,
            "collection_id": 42, "audit_seq": 65,
            "next_ref": "ref/audit/064", "done": True,
        }},
    })
    status = await domain("vector_query_rebuild", {
        "request_id": "audit/page", "requested_by": "admin",
    })
    assert status["state"] == "AUDITED"
    assert status["audit_after_ref"] == "ref/audit/064"
    assert status["audited_at"] is not None

    old_binding = {"binding_revision": 1, "collection_id": 41,
                   "collection_name": name}
    old_ticket = await domain("vector_io_enter", {
        "space_id": space_id, "binding": old_binding, "purpose": "UPSERT",
    }, operation_id="audit/page/old-io")
    assert old_ticket["ticket"] == "audit/page/old-io"
    publish_claim = await domain("vector_claim_rebuild_publish", {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "audit/page/publish", "lease_seconds": 180,
    })
    assert publish_claim["payload"]["audit_seq"] == 65
    assert (await domain("vector_query_rebuild", {
        "request_id": "audit/page", "requested_by": "admin",
    }))["state"] == "PUBLISHING"
    refused = await domain("vector_io_enter", {
        "space_id": space_id, "binding": old_binding, "purpose": "UPSERT",
    }, operation_id="audit/page/new-io", allow_error=True)
    assert refused["error"]["code"] == "VECTOR_INDEX_SWITCHING"
    assert (await domain("vector_query_publish_drain", {
        "request_id": "audit/page", "claim_token": publish_claim["claim_token"],
    }))["active_count"] == 1
    pending = await domain("vector_publish_rebuild", publish_claim["payload"],
                           allow_error=True)
    assert pending["error"]["code"] == "VECTOR_REBUILD_DRAIN_PENDING"
    await domain("vector_io_exit", {"ticket": old_ticket["ticket"]})
    assert (await domain("vector_query_publish_drain", {
        "request_id": "audit/page", "claim_token": publish_claim["claim_token"],
    }))["active_count"] == 0
    async def publish_query(name, payload, **_options):
        assert name == "vector.io.pending"
        return await domain("vector_query_publish_drain", payload)

    async def publish_service(service_name, payload, **_options):
        assert service_name == "vector.transport.audit"
        assert payload["binding"] == {
            "binding_revision": 2, "collection_id": 42,
            "collection_name": name + "_r2",
        }
        return {"space_id": space_id, "mode": "SCAN",
                "rows": [], "next_ref": None, "done": True}

    async def publish_command(name, payload, **_options):
        assert name == "vector.rebuild.commit"
        return await domain("vector_publish_rebuild", payload)

    component = MilvusVectorIndex()
    component.space_id = space_id
    publish_ctx = SimpleNamespace(
        queries=SimpleNamespace(call=publish_query),
        services=SimpleNamespace(call=publish_service),
        commands=SimpleNamespace(call=publish_command),
        operations=SimpleNamespace(stable=lambda value: value),
    )
    publication = await component.publish_rebuild_candidate(
        {"payload": publish_claim["payload"]}, publish_ctx,
    )
    assert publication["binding_revision"] == 2
    assert publication["published_seq"] == 65
    assert await domain("vector_publish_rebuild", publish_claim["payload"]) == publication
    await domain("vector_settle_rebuild_publish", {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": publish_claim["work_id"],
        "claim_token": publish_claim["claim_token"],
        "attempt": publish_claim["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": publication},
    })
    current = await domain("vector_collection_current", {"space_id": space_id})
    assert current["binding"]["binding_revision"] == 2
    assert current["binding"]["collection_id"] == 42
    assert (await domain("vector_query_rebuild", {
        "request_id": "audit/page", "requested_by": "admin",
    }))["state"] == "PUBLISHED"


@pytest.mark.asyncio
@pytest.mark.postgres
@pytest.mark.parametrize("case", ("missing-active", "unbound-physical"))
async def test_package_owned_ref_audit_quarantines_stable_physical_mismatch(
    clean_authority, tmp_path, case
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    space_id = "space/audit/" + uuid4().hex
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "audit-packages"),
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

    async def invoke(function, payload, *, operation_id=None):
        context = {"bot_id": store.bot_id}
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
        envelope = row["result"]
        if not envelope["ok"]:
            error = envelope["error"]
            raise ForestError(error["code"], error["message"], error["status"])
        return envelope["value"]

    shape = {
        "space_id": space_id,
        "collection_name": vector_collection_name(store.bot_id, space_id),
        "dimensions": 4, "metric": "COSINE",
    }
    plan = await invoke(
        "vector_collection_plan", {**shape, "exists": False, "collection_id": None},
        operation_id="audit/start",
    )
    assert plan["state"] == "CREATE"
    await invoke(
        "vector_collection_confirm",
        {**shape, "collection_id": 42, "claim_token": plan["claim_token"]},
    )

    async def publish(ref_id):
        public = _with_replay({
            "ref_id": ref_id, "space_id": space_id,
            "purpose": "DOCUMENT", "vector": [0.1, 0.2, 0.3, 0.4],
        })
        digest = digest_json(public)
        reserved = await invoke("vector_ref_reserve", {
            **public, "source_input_digest": digest,
            "physical_digest": _physical_digest(public["vector"]),
        })
        await invoke("vector_ref_activate", {
            "ref_id": ref_id, "input_digest": digest,
            "binding": reserved["binding"],
        })
        return public

    if case == "missing-active":
        await publish("ref/a")
    claim_input = {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "audit/first", "lease_seconds": 90,
    }
    work = await invoke("vector_claim_ref_audit", claim_input)
    assert work == await invoke("vector_claim_ref_audit", claim_input)
    assert work["payload"]["phase"] == (
        "REF" if case == "missing-active" else "PHYSICAL"
    )
    if case == "missing-active":
        assert work["payload"]["refs"] == ["ref/a"]
        await publish("ref/b")
        assert (await invoke("vector_adjudicate_ref_audit", {
            "space_id": space_id, "binding_revision": 1,
            "collection_id": 42, "cycle_seq": work["payload"]["cycle_seq"],
            "claim_token": work["claim_token"],
            "observations": [{"ref_id": "ref/b", "physical": None}],
        })) == {"state": "READY", "ref_id": None}
    else:
        assert work["payload"]["refs"] == []

    async def service_call(name, payload, **_options):
        assert name == "vector.transport.audit"
        if case == "missing-active":
            assert payload["mode"] == "GET" and payload["ref_ids"] == ["ref/a"]
            return {
                "space_id": space_id, "mode": "GET", "rows": [],
                "next_ref": None, "done": True,
            }
        assert payload["mode"] == "SCAN"
        return {
            "space_id": space_id, "mode": "SCAN",
            "rows": [{
                "ref_id": "ref/unbound", "bot_id": store.bot_id,
                "space_id": space_id, "purpose": "DOCUMENT",
                "input_digest": "sha256:" + "a" * 64,
                "physical_digest": "f32-sha256:" + "b" * 64,
            }],
            "next_ref": "ref/unbound", "done": True,
        }

    async def command_call(name, payload, **_options):
        assert name == "vector.audit.adjudicate"
        return await invoke("vector_adjudicate_ref_audit", payload)

    component = MilvusVectorIndex()
    component.space_id = space_id
    ctx = SimpleNamespace(
        services=SimpleNamespace(call=service_call),
        commands=SimpleNamespace(call=command_call),
        operations=SimpleNamespace(stable=lambda value: value),
    )
    result = await component.audit_refs(work, ctx)
    assert result["quarantined"]
    settlement = {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": work["work_id"], "claim_token": work["claim_token"],
        "attempt": work["attempt"],
        "outcome": {"status": "SUCCEEDED", "result": result},
    }
    settled, replayed = await asyncio.gather(
        invoke("vector_settle_ref_audit", settlement),
        invoke("vector_settle_ref_audit", settlement),
    )
    assert settled == {"protocol": "forest.owned-work-settled/1",
                       "work_id": work["work_id"]}
    assert replayed == settled
    with pytest.raises(ForestError) as stale:
        await invoke("vector_adjudicate_ref_audit", {
            "space_id": space_id, "binding_revision": 1,
            "collection_id": 42, "cycle_seq": work["payload"]["cycle_seq"],
            "claim_token": work["claim_token"],
            "observations": [{"ref_id": "ref/unbound", "physical": None}],
        })
    assert stale.value.code == "VECTOR_AUDIT_CLAIM_STALE"
    async with store.pool.connection() as conn:
        health = await (
            await conn.execute(
                sql.SQL("SELECT state,reason FROM {}.vector_collection_health "
                        "WHERE bot_id=%s").format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
        ).fetchone()
    assert health == {"state": "REBUILD_REQUIRED", "reason": "row mismatch"}
    assert await invoke("vector_claim_ref_audit", {
        **claim_input, "claim_token": "audit/second",
    }) is None


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_ref_audit_resumes_into_physical_scan_and_records_waterline(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    space_id = "space/audit-ok/" + uuid4().hex
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "audit-ok-packages"),
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

    async def invoke(function, payload, *, operation_id=None):
        context = {"bot_id": store.bot_id}
        if operation_id is not None:
            context["operation_id"] = operation_id
        async with store.pool.connection() as conn, conn.transaction():
            envelope = (
                await (
                    await conn.execute(
                        sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                            sql.Identifier(namespace), sql.Identifier(function)
                        ),
                        (Jsonb(context), Jsonb(payload)),
                    )
                ).fetchone()
            )["result"]
        assert envelope["ok"], envelope
        return envelope["value"]

    shape = {
        "space_id": space_id,
        "collection_name": vector_collection_name(store.bot_id, space_id),
        "dimensions": 4, "metric": "COSINE",
    }
    plan = await invoke(
        "vector_collection_plan", {**shape, "exists": False, "collection_id": None},
        operation_id="audit-ok/start",
    )
    await invoke("vector_collection_confirm", {
        **shape, "collection_id": 42, "claim_token": plan["claim_token"],
    })

    async def publish(ref_id):
        public = _with_replay({
            "ref_id": ref_id, "space_id": space_id,
            "purpose": "DOCUMENT", "vector": [0.1, 0.2, 0.3, 0.4],
        })
        digest = digest_json(public)
        reserved = await invoke("vector_ref_reserve", {
            **public, "source_input_digest": digest,
            "physical_digest": _physical_digest(public["vector"]),
        })
        await invoke("vector_ref_activate", {
            "ref_id": ref_id, "input_digest": digest,
            "binding": reserved["binding"],
        })
        return {
            "ref_id": ref_id, "bot_id": store.bot_id,
            "space_id": space_id, "purpose": "DOCUMENT",
            "input_digest": digest,
            "physical_digest": _physical_digest(public["vector"]),
        }

    first_row = await publish("ref/a")
    claim = {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "audit-ok/ref", "lease_seconds": 90,
    }
    first = await invoke("vector_claim_ref_audit", claim)
    assert first["payload"]["phase"] == "REF"
    assert first["payload"]["refs"] == ["ref/a"]
    assert first["payload"]["cycle_seq"] == 1
    late_row = await publish("ref/b")

    async def service_call(name, payload, **_options):
        assert name == "vector.transport.audit"
        if payload["mode"] == "GET":
            return {
                "space_id": space_id, "mode": "GET", "rows": [first_row],
                "next_ref": None, "done": True,
            }
        return {
            "space_id": space_id, "mode": "SCAN",
            "rows": [first_row, late_row],
            "next_ref": "ref/b", "done": True,
        }

    async def command_call(name, payload, **_options):
        assert name == "vector.audit.adjudicate"
        return await invoke("vector_adjudicate_ref_audit", payload)

    component = MilvusVectorIndex()
    component.space_id = space_id
    ctx = SimpleNamespace(
        services=SimpleNamespace(call=service_call),
        commands=SimpleNamespace(call=command_call),
        operations=SimpleNamespace(stable=lambda value: value),
    )

    async def run_and_settle(work):
        result = await component.audit_refs(work, ctx)
        assert not result["quarantined"]
        settled = await invoke("vector_settle_ref_audit", {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": work["work_id"], "claim_token": work["claim_token"],
            "attempt": work["attempt"],
            "outcome": {"status": "SUCCEEDED", "result": result},
        })
        assert settled == {"protocol": "forest.owned-work-settled/1",
                           "work_id": work["work_id"]}

    await run_and_settle(first)
    second = await invoke("vector_claim_ref_audit", {
        **claim, "claim_token": "audit-ok/physical",
    })
    assert second["payload"]["phase"] == "PHYSICAL"
    assert second["payload"]["cycle_seq"] == 1
    await run_and_settle(second)
    async with store.pool.connection() as conn:
        state = await (
            await conn.execute(
                sql.SQL(
                    "SELECT phase,cycle_seq,last_checked_seq,"
                    "last_checked_at IS NOT NULL AS checked,"
                    "next_at>clock_timestamp() AS scheduled "
                    "FROM {}.vector_audit_work WHERE bot_id=%s"
                ).format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
        ).fetchone()
    assert state == {
        "phase": "REF", "cycle_seq": None, "last_checked_seq": 1,
        "checked": True, "scheduled": True,
    }


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
                "replay_locator,vector_digest,physical_digest) "
                "VALUES (%s,%s,%s,'PENDING','DOCUMENT',%s,%s,%s,%s)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, replay["ref_id"], configuration["space_id"],
             digest_json(replay), Jsonb(replay["replay"]),
             replay["replay"]["vector_digest"], _physical_digest(replay["vector"])),
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
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_collection_bindings "
                    "(bot_id,binding_revision,space_id,collection_name,"
                    "collection_id,dimensions,metric) "
                    "VALUES (%s,1,%s,%s,42,4,'COSINE')")
            .format(sql.Identifier(namespace)),
            (store.bot_id, space_id, vector_collection_name(store.bot_id, space_id)),
        )
        await conn.execute(
            sql.SQL("INSERT INTO {}.vector_collection_health "
                    "(bot_id,binding_revision,state) VALUES (%s,1,'READY')")
            .format(sql.Identifier(namespace)), (store.bot_id,),
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
    assert work["payload"] == {
        "space_id": space_id, "ref_ids": ["ref/a", "ref/b"],
        "binding": {"binding_revision": 1, "collection_id": 42,
                    "collection_name": vector_collection_name(store.bot_id, space_id)},
    }
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
            "vector.transport.prune", "vector.transport.audit",
            "vector.transport.candidate-create",
        )
    }
    scope = identity
    invocation = SimpleNamespace(operation_id="test")
    collection_name = drivers["vector.transport.upsert"].collection_name
    candidate_name = None

    def transport_row(ref_id, purpose, vector):
        public = _with_replay({
            "ref_id": ref_id, "space_id": space_id,
            "purpose": purpose, "vector": vector,
        })
        return {**public, "input_digest": digest_json(public),
                "binding": binding_snapshot}

    try:
        await asyncio.gather(*(driver.start() for driver in drivers.values()))
        assert not any(driver.rebuild_required for driver in drivers.values())
        upsert = drivers["vector.transport.upsert"]
        collection = await upsert.client.describe_collection(
            collection_name=collection_name
        )
        binding_snapshot = {
            "binding_revision": 1, "collection_id": collection["collection_id"],
            "collection_name": collection_name,
        }
        with pytest.raises(ForestError) as stale_binding:
            await upsert.invoke(
                {**transport_row("ref/stale", "DOCUMENT", [1.0, 0.0, 0.0, 0.0]),
                 "binding": {**binding_snapshot, "binding_revision": 2}},
                scope=scope, invocation=invocation,
            )
        assert stale_binding.value.code == "VECTOR_INDEX_BINDING_STALE"
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
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2,
             "binding": binding_snapshot},
            scope=scope, invocation=invocation,
        )
        assert [hit["ref_id"] for hit in result["hits"]] == [
            "ref/document/near", "ref/document/far",
        ]
        assert all(set(hit) == {"ref_id", "score"} for hit in result["hits"])
        await drivers["vector.transport.prune"].invoke(
            {"ref_ids": ["ref/document/near"], "space_id": space_id,
             "binding": binding_snapshot},
            scope=scope, invocation=invocation,
        )
        result = await drivers["vector.transport.search"].invoke(
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2,
             "binding": binding_snapshot},
            scope=scope, invocation=invocation,
        )
        assert [hit["ref_id"] for hit in result["hits"]] == ["ref/document/far"]

        precision = transport_row(
            "ref/document/precision", "DOCUMENT", [0.1, 0.2, 0.3, 0.4]
        )
        await upsert.invoke(precision, scope=scope, invocation=invocation)
        audit = drivers["vector.transport.audit"]
        observed = await audit.invoke(
            {"space_id": space_id, "mode": "GET",
             "ref_ids": ["ref/document/precision", "ref/document/near"],
             "binding": binding_snapshot},
            scope=scope, invocation=invocation,
        )
        assert observed["done"] and observed["next_ref"] is None
        assert observed["rows"] == [{
            "ref_id": precision["ref_id"], "bot_id": bot_id,
            "space_id": space_id, "purpose": "DOCUMENT",
            "input_digest": precision["input_digest"],
            "physical_digest": _physical_digest(precision["vector"]),
        }]
        cursor = None
        scanned = []
        while True:
            page = await audit.invoke(
                {"space_id": space_id, "mode": "SCAN",
                 "after_ref": cursor, "limit": 1,
                  "binding": binding_snapshot},
                scope=scope, invocation=invocation,
            )
            scanned.extend(page["rows"])
            if page["done"]:
                break
            assert page["next_ref"] != cursor
            cursor = page["next_ref"]
        assert [row["ref_id"] for row in scanned] == [
            "ref/document/far", "ref/document/precision", "ref/query",
        ]
        assert next(row for row in scanned if
                    row["ref_id"] == precision["ref_id"]) == observed["rows"][0]

        # An expired unconfirmed attempt receives a new revision and cannot
        # confirm the successor's candidate after its lease is lost.
        async def rebuild_domain(function, payload, operation_id="rebuild/test"):
            async with store.pool.connection() as conn, conn.transaction():
                row = await (
                    await conn.execute(
                        sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                            sql.Identifier(namespace), sql.Identifier(function)
                        ),
                        (Jsonb({"bot_id": bot_id, "operation_id": operation_id}),
                         Jsonb(payload)),
                    )
                ).fetchone()
            envelope = row["result"]
            if not envelope["ok"]:
                error = envelope["error"]
                raise ForestError(error["code"], error["message"], error["status"])
            return envelope["value"]

        enqueued = await rebuild_domain(
            "vector_enqueue_rebuild", {"requested_by": "admin"},
            operation_id="rebuild/request",
        )
        assert enqueued["state"] == "QUEUED"
        claim_input = {
            "protocol": "forest.owned-work-claim-request/1",
            "claim_token": "rebuild/claim/first", "lease_seconds": 120,
        }
        first_claim = await rebuild_domain(
            "vector_claim_rebuild_create", claim_input
        )
        assert first_claim["payload"]["candidate_revision"] == 2
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("UPDATE {}.vector_rebuild_work "
                        "SET lease_deadline=clock_timestamp()-interval '1 second' "
                        "WHERE bot_id=%s AND request_id=%s")
                .format(sql.Identifier(namespace)),
                (bot_id, enqueued["request_id"]),
            )
        next_claim = await rebuild_domain(
            "vector_claim_rebuild_create",
            {**claim_input, "claim_token": "rebuild/claim/second"},
        )
        assert next_claim["payload"]["candidate_revision"] == 3
        with pytest.raises(ForestError) as late_confirm:
            await rebuild_domain("vector_confirm_rebuild_candidate", {
                "request_id": enqueued["request_id"],
                "claim_token": first_claim["claim_token"],
                "candidate_revision": 2,
                "collection_name": first_claim["payload"]["collection_name"],
                "collection_id": 42,
            })
        assert late_confirm.value.code == "VECTOR_REBUILD_CLAIM_STALE"

        async def candidate_service(name, payload, **_options):
            assert name == "vector.transport.candidate-create"
            return await drivers[name].invoke(
                payload, scope=scope, invocation=invocation
            )

        async def candidate_command(name, payload, **options):
            assert name == "vector.rebuild.confirm"
            return await rebuild_domain(
                "vector_confirm_rebuild_candidate", payload,
                operation_id=options["operation_id"],
            )

        component = MilvusVectorIndex()
        component.space_id = space_id
        component.dimensions = 4
        candidate_receipt = await component.create_rebuild_candidate(
            {"payload": next_claim["payload"]},
            SimpleNamespace(
                services=SimpleNamespace(call=candidate_service),
                commands=SimpleNamespace(call=candidate_command),
                operations=SimpleNamespace(stable=lambda value: value),
            ),
        )
        candidate_name = candidate_receipt["collection_name"]
        # A crash after durable confirmation but before owned-work settlement
        # must reuse the confirmed physical identity without creating again.
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("UPDATE {}.vector_rebuild_work "
                        "SET lease_deadline=clock_timestamp()-interval '1 second' "
                        "WHERE bot_id=%s AND request_id=%s")
                .format(sql.Identifier(namespace)),
                (bot_id, enqueued["request_id"]),
            )
        confirmed_claim = await rebuild_domain(
            "vector_claim_rebuild_create",
            {**claim_input, "claim_token": "rebuild/claim/confirmed"},
        )
        assert confirmed_claim["payload"]["phase"] == "CONFIRMED"
        assert confirmed_claim["payload"]["candidate_revision"] == 3
        assert confirmed_claim["payload"]["collection_id"] == (
            candidate_receipt["collection_id"]
        )

        async def no_second_physical_create(*_args, **_kwargs):
            raise AssertionError("confirmed candidate must not be recreated")

        confirmed_receipt = await component.create_rebuild_candidate(
            {"payload": confirmed_claim["payload"]},
            SimpleNamespace(
                services=SimpleNamespace(call=no_second_physical_create),
                commands=SimpleNamespace(call=candidate_command),
                operations=SimpleNamespace(stable=lambda value: value),
            ),
        )
        assert confirmed_receipt == candidate_receipt
        with pytest.raises(ForestError) as late_settlement:
            await rebuild_domain("vector_settle_rebuild_create", {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": next_claim["work_id"],
                "claim_token": next_claim["claim_token"],
                "attempt": next_claim["attempt"],
                "outcome": {"status": "SUCCEEDED", "result": candidate_receipt},
            })
        assert late_settlement.value.code == "VECTOR_REBUILD_CLAIM_STALE"
        await rebuild_domain("vector_settle_rebuild_create", {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": confirmed_claim["work_id"],
            "claim_token": confirmed_claim["claim_token"],
            "attempt": confirmed_claim["attempt"],
            "outcome": {"status": "SUCCEEDED", "result": confirmed_receipt},
        })
        status = await rebuild_domain("vector_query_rebuild", {
            "request_id": enqueued["request_id"], "requested_by": "admin",
        })
        assert status["state"] == "CANDIDATE_READY"
        assert status["candidate_revision"] == 3
        assert status["candidate_id"] == candidate_receipt["collection_id"]
        assert status["replay_waterline"] is not None
        candidate_binding = {
            "binding_revision": status["candidate_revision"],
            "collection_id": status["candidate_id"],
            "collection_name": candidate_name,
        }
        live_replay = transport_row(
            "ref/replay/query", "QUERY", [0.0, 1.0, 0.0, 0.0]
        )
        deleted_replay = transport_row(
            "ref/replay/deleted", "DOCUMENT", [0.0, 0.0, 1.0, 0.0]
        )
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("INSERT INTO {}.vector_index_refs "
                        "(bot_id,ref_id,space_id,state,purpose,input_digest,"
                        "replay_locator,vector_digest,physical_digest,published_at) "
                        "VALUES (%s,%s,%s,'ACTIVE','QUERY',%s,%s,%s,%s,"
                        "clock_timestamp())")
                .format(sql.Identifier(namespace)),
                (bot_id, live_replay["ref_id"], space_id,
                 live_replay["input_digest"], Jsonb(live_replay["replay"]),
                 live_replay["replay"]["vector_digest"],
                 _physical_digest(live_replay["vector"])),
            )
            await conn.execute(
                sql.SQL("INSERT INTO {}.vector_index_refs "
                        "(bot_id,ref_id,space_id,state,tombstoned_at,next_sweep_at) "
                        "VALUES (%s,%s,%s,'TOMBSTONED',clock_timestamp(),"
                        "clock_timestamp())")
                .format(sql.Identifier(namespace)),
                (bot_id, deleted_replay["ref_id"], space_id),
            )
            for row, state in (
                (live_replay, "ACTIVE"),
                (deleted_replay, "ACTIVE"),
                (deleted_replay, "TOMBSTONED"),
            ):
                active = state == "ACTIVE"
                await conn.execute(
                    sql.SQL("SELECT {}.vector_append_ref_change("
                            "%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                    .format(sql.Identifier(namespace)),
                    (bot_id, row["ref_id"], space_id, state,
                     row["purpose"] if active else None,
                     row["input_digest"] if active else None,
                     Jsonb(row["replay"]) if active else None,
                     row["replay"]["vector_digest"] if active else None,
                     _physical_digest(row["vector"]) if active else None),
                )
        replay_claim = await rebuild_domain(
            "vector_claim_rebuild_replay",
            {**claim_input, "claim_token": "rebuild/replay/first",
             "lease_seconds": 660},
        )
        actions = replay_claim["payload"]["changes"]
        assert [item["mutation_seq"] for item in actions] == [1, 2, 3]
        assert [item["state"] for item in actions] == [
            "ACTIVE", "TOMBSTONED", "TOMBSTONED",
        ]
        assert all(item["replay_locator"] is None for item in actions[1:])

        async def read_replay_json(reference):
            assert reference == live_replay["replay"]["result"]
            return {
                "protocol": "forest.embedding-result/1",
                "status": "RESPONDED", "space_id": space_id,
                "vectors": [{"index": 0, "facet_id": live_replay["ref_id"],
                             "values": live_replay["vector"]}],
            }

        async def replay_service(name, payload, **_options):
            assert name in {
                "vector.transport.upsert", "vector.transport.prune",
                "vector.transport.audit",
            }
            return await drivers[name].invoke(
                payload, scope=scope, invocation=invocation
            )

        replay_context = SimpleNamespace(
            payloads=SimpleNamespace(read_json=read_replay_json),
            services=SimpleNamespace(call=replay_service),
            operations=SimpleNamespace(stable=lambda value: value),
        )
        replay_receipt = await component.replay_rebuild_page(
            {"payload": replay_claim["payload"]}, replay_context,
        )
        await rebuild_domain("vector_settle_rebuild_replay", {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": replay_claim["work_id"],
            "claim_token": replay_claim["claim_token"],
            "attempt": replay_claim["attempt"],
            "outcome": {"status": "SUCCEEDED", "result": replay_receipt},
        })
        assert (await rebuild_domain("vector_query_rebuild", {
            "request_id": enqueued["request_id"], "requested_by": "admin",
        }))["replay_cursor"] == 3
        assert await upsert.client.get(
            collection_name=candidate_name, ids=[live_replay["ref_id"]],
            output_fields=["ref_id"], consistency_level="Strong",
        ) == [{"ref_id": live_replay["ref_id"]}]
        assert await upsert.client.get(
            collection_name=candidate_name, ids=[deleted_replay["ref_id"]],
            output_fields=["ref_id"], consistency_level="Strong",
        ) == []
        audit_claim = await rebuild_domain(
            "vector_claim_rebuild_audit",
            {**claim_input, "claim_token": "rebuild/audit/active",
             "lease_seconds": 90},
        )
        assert [row["ref_id"] for row in audit_claim["payload"]["expected"]] == [
            live_replay["ref_id"]
        ]
        # A tombstone committed after the audit claim belongs to the next
        # replay waterline; it must not rewrite this frozen expected page.
        await rebuild_domain("vector_ref_tombstone", {
            "space_id": space_id, "ref_ids": [live_replay["ref_id"]],
        })
        audit_row = {
            "ref_id": live_replay["ref_id"], "bot_id": bot_id,
            "space_id": space_id, "purpose": "QUERY",
            "embedding": live_replay["vector"],
            "input_digest": live_replay["input_digest"],
        }
        await upsert.client.upsert(
            collection_name=candidate_name,
            data=[{**audit_row, "input_digest": "sha256:" + "f" * 64}],
        )
        with pytest.raises(ForestError) as wrong_digest:
            await component.audit_rebuild_candidate(
                {"payload": audit_claim["payload"]}, replay_context,
            )
        assert wrong_digest.value.code == "VECTOR_REBUILD_AUDIT_MISMATCH"
        await upsert.client.upsert(
            collection_name=candidate_name, data=[audit_row],
        )
        audit_receipt = await component.audit_rebuild_candidate(
            {"payload": audit_claim["payload"]}, replay_context,
        )
        await rebuild_domain("vector_settle_rebuild_audit", {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": audit_claim["work_id"],
            "claim_token": audit_claim["claim_token"],
            "attempt": audit_claim["attempt"],
            "outcome": {"status": "SUCCEEDED", "result": audit_receipt},
        })
        audited = await rebuild_domain("vector_query_rebuild", {
            "request_id": enqueued["request_id"], "requested_by": "admin",
        })
        assert audited["state"] == "AUDITED"
        assert audited["audit_seq"] == 3

        catchup_claim = await rebuild_domain(
            "vector_claim_rebuild_replay",
            {**claim_input, "claim_token": "rebuild/replay/catchup",
             "lease_seconds": 660},
        )
        assert [row["state"] for row in catchup_claim["payload"]["changes"]] == [
            "TOMBSTONED"
        ]
        catchup_receipt = await component.replay_rebuild_page(
            {"payload": catchup_claim["payload"]}, replay_context,
        )
        await rebuild_domain("vector_settle_rebuild_replay", {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": catchup_claim["work_id"],
            "claim_token": catchup_claim["claim_token"],
            "attempt": catchup_claim["attempt"],
            "outcome": {"status": "SUCCEEDED", "result": catchup_receipt},
        })
        assert (await rebuild_domain("vector_query_rebuild", {
            "request_id": enqueued["request_id"], "requested_by": "admin",
        }))["replay_cursor"] == 4
        assert await upsert.client.get(
            collection_name=candidate_name, ids=[live_replay["ref_id"]],
            output_fields=["ref_id"], consistency_level="Strong",
        ) == []
        second_audit = await rebuild_domain(
            "vector_claim_rebuild_audit",
            {**claim_input, "claim_token": "rebuild/audit/empty",
             "lease_seconds": 90},
        )
        assert second_audit["payload"]["audit_seq"] == 4
        assert second_audit["payload"]["expected"] == []
        second_receipt = await component.audit_rebuild_candidate(
            {"payload": second_audit["payload"]}, replay_context,
        )
        await rebuild_domain("vector_settle_rebuild_audit", {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": second_audit["work_id"],
            "claim_token": second_audit["claim_token"],
            "attempt": second_audit["attempt"],
            "outcome": {"status": "SUCCEEDED", "result": second_receipt},
        })
        audited = await rebuild_domain("vector_query_rebuild", {
            "request_id": enqueued["request_id"], "requested_by": "admin",
        })
        assert audited["state"] == "AUDITED"
        assert audited["audit_seq"] == 4
        assert await rebuild_domain(
            "vector_claim_rebuild_replay",
            {**claim_input, "claim_token": "rebuild/replay/empty",
             "lease_seconds": 660},
        ) is None

        candidate_query = {**transport_row(
            "ref/query/candidate", "QUERY", [1.0, 0.0, 0.0, 0.0]
        ), "binding": candidate_binding}
        candidate_document = {**transport_row(
            "ref/document/candidate", "DOCUMENT", [1.0, 0.0, 0.0, 0.0]
        ), "binding": candidate_binding}
        await upsert.invoke(candidate_query, scope=scope, invocation=invocation)
        await upsert.invoke(candidate_document, scope=scope, invocation=invocation)
        candidate_hits = await drivers["vector.transport.search"].invoke(
            {"query_ref_id": candidate_query["ref_id"], "space_id": space_id,
             "limit": 2, "binding": candidate_binding},
            scope=scope, invocation=invocation,
        )
        assert [hit["ref_id"] for hit in candidate_hits["hits"]] == [
            candidate_document["ref_id"]
        ]
        assert await upsert.client.get(
            collection_name=collection_name, ids=[candidate_document["ref_id"]],
            output_fields=["ref_id"], consistency_level="Strong",
        ) == []

        replay = near["replay"]
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.vector_index_refs "
                    "(bot_id,ref_id,space_id,state,purpose,input_digest,"
                    "replay_locator,vector_digest,physical_digest,published_at) "
                    "VALUES (%s,%s,%s,'ACTIVE','DOCUMENT',%s,%s,%s,%s,"
                    "clock_timestamp())"
                ).format(sql.Identifier(namespace)),
                (bot_id, near["ref_id"], space_id, near["input_digest"],
                 Jsonb(replay), replay["vector_digest"],
                 _physical_digest(near["vector"])),
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
        with pytest.raises(ForestError) as live_replaced:
            await drivers["vector.transport.search"].invoke(
                {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2,
             "binding": binding_snapshot},
                scope=scope, invocation=invocation,
            )
        assert live_replaced.value.code == "VECTOR_INDEX_REBUILD_REQUIRED"
        async with store.pool.connection() as conn:
            health = await (await conn.execute(
                sql.SQL("SELECT state,reason FROM {}.vector_collection_health "
                        "WHERE bot_id=%s").format(sql.Identifier(namespace)),
                (bot_id,),
            )).fetchone()
        assert health == {"state": "REBUILD_REQUIRED",
                          "reason": "collection identity changed"}
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

        # Simulate only the already verified SQL publication fact here; the
        # candidate replay and atomic switch authority are separate work.
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("INSERT INTO {}.vector_collection_bindings "
                        "(bot_id,binding_revision,space_id,collection_name,"
                        "collection_id,dimensions,metric) "
                        "VALUES (%s,%s,%s,%s,%s,4,'COSINE')")
                .format(sql.Identifier(namespace)),
                (bot_id, candidate_binding["binding_revision"], space_id,
                 candidate_name, candidate_binding["collection_id"]),
            )
            await conn.execute(
                sql.SQL("UPDATE {}.vector_collection_health "
                        "SET binding_revision=%s,state='READY',reason=NULL "
                        "WHERE bot_id=%s").format(sql.Identifier(namespace)),
                (candidate_binding["binding_revision"], bot_id),
            )
        await replacement.close()
        recovered = driver_for("vector.transport.search")
        drivers["vector.transport.search"] = recovered
        await recovered.start()
        assert recovered.collection_name == candidate_name
        assert recovered.binding_revision == candidate_binding["binding_revision"]
        recovered_hits = await recovered.invoke(
            {"query_ref_id": candidate_query["ref_id"], "space_id": space_id,
             "limit": 2, "binding": candidate_binding},
            scope=scope, invocation=invocation,
        )
        assert [hit["ref_id"] for hit in recovered_hits["hits"]] == [
            candidate_document["ref_id"]
        ]
    finally:
        client = drivers["vector.transport.upsert"].client
        if client is not None and await client.has_collection(
            collection_name=collection_name
        ):
            await client.drop_collection(collection_name=collection_name)
        if (client is not None and candidate_name is not None
                and await client.has_collection(collection_name=candidate_name)):
            await client.drop_collection(collection_name=candidate_name)
        for driver in drivers.values():
            await driver.close()
