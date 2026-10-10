from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
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
from forest.vector_index_effect_driver import (
    MilvusVectorIndexEffectDriver,
    milvus_vector_index_effect_driver_factory,
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

    ctx = SimpleNamespace(
        payloads=SimpleNamespace(read_json=read_json),
        services=SimpleNamespace(call=call),
        operations=SimpleNamespace(stable=lambda value: value),
    )
    assert await component.upsert(request, ctx) == {
        "ref_id": "facet/one", "space_id": "space/test"
    }
    assert len(calls) == 1 and calls[0][0] == "vector.transport.upsert"
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
        return MilvusVectorIndexEffectDriver(
            configuration, 1, identity,
            {"name": service, "kind": "vector-index.milvus"},
            bot_id=store.bot_id, store=store, runtime=runtime,
            token_resolver=resolve_token, client_factory=client_factory,
        )

    search = driver_for("vector.transport.search")
    delete = driver_for("vector.transport.delete")
    async with store.pool.connection() as conn, conn.transaction():
        await search.refs.record_active(
            conn, ref_id="ref/document/existing",
            input_digest="sha256:" + "0" * 64,
            replay_locator=_with_replay({
                "ref_id": "ref/document/existing", "vector": [1.0, 0.0, 0.0, 0.0],
            })["replay"],
            vector_digest=digest_json([1.0, 0.0, 0.0, 0.0]),
        )
    await search.start()
    await delete.start()
    assert search.accepting and search.rebuild_required
    assert delete.accepting and delete.rebuild_required
    with pytest.raises(ForestError) as missing:
        await search.invoke(
            {"query_ref_id": "ref/query", "space_id": configuration["space_id"], "limit": 1},
            scope=identity, invocation=SimpleNamespace(operation_id="search"),
        )
    assert missing.value.code == "VECTOR_INDEX_REBUILD_REQUIRED"
    assert await delete.invoke(
        {"ref_ids": ["ref/document/existing"], "space_id": configuration["space_id"]},
        scope=identity, invocation=SimpleNamespace(operation_id="delete"),
    ) == {
        "ref_ids": ["ref/document/existing"], "space_id": configuration["space_id"],
    }
    async with store.pool.connection() as conn, conn.transaction():
        assert (await delete.refs.lock(conn, "ref/document/existing"))["state"] == "TOMBSTONED"
    await search.close()
    await delete.close()
    assert all(client.closed for client in clients)


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_milvus_vector_index_round_trip_by_reference(clean_authority, tmp_path):
    uri = os.environ.get("FOREST_TEST_MILVUS_URI")
    if not uri:
        pytest.skip("FOREST_TEST_MILVUS_URI is not configured")

    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    suffix = uuid4().hex
    bot_id = store.bot_id
    space_id = f"space/test/{suffix}"
    async def verify_generation(**_values):
        return None

    async def resolve_token(_ref, _revision):
        return ""

    configuration = {
        "protocol": "forest.vector-index.milvus/1",
        "endpoint": uri,
        "credential_ref": "",
        "timeout_seconds": 30,
        "space_id": space_id,
        "dimensions": 4,
        "metric": "COSINE",
    }
    await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/vector_index_milvus",
        PackageCatalog(tmp_path / "vector-packages"),
        store, installer, kernel=kernel, initial_configuration=configuration,
        effect_driver_registry=PackageEffectDriverRegistry({
            "vector-index.milvus": milvus_vector_index_effect_driver_factory(
                bot_id=bot_id, store=store, runtime=kernel.runtime,
                token_resolver=resolve_token,
            )
        }),
    )
    identity = SimpleNamespace(
        component_id="vector.index", generation_id="generation/vector-test"
    )
    kernel_runtime = SimpleNamespace(verify_generation_member=verify_generation)
    drivers = {
        service: MilvusVectorIndexEffectDriver(
            configuration,
            1,
            identity,
            {"name": service, "kind": "vector-index.milvus"},
            bot_id=bot_id,
            store=store,
            runtime=kernel_runtime,
            token_resolver=resolve_token,
        )
        for service in (
            "vector.transport.upsert",
            "vector.transport.search",
            "vector.transport.delete",
        )
    }
    scope = identity
    invocation = SimpleNamespace(operation_id="test")
    collection_name = drivers["vector.transport.upsert"].collection_name
    try:
        await asyncio.gather(*(driver.start() for driver in drivers.values()))
        assert not any(driver.rebuild_required for driver in drivers.values())
        collection = await drivers["vector.transport.upsert"].client.describe_collection(
            collection_name=collection_name
        )
        assert {field["name"] for field in collection["fields"]} == {
            "ref_id", "bot_id", "space_id", "purpose", "input_digest", "embedding",
        }
        # Milvus may commit a write while its acknowledgement is lost. The
        # PostgreSQL ref fence must remain uncommitted until the retry confirms
        # the same immutable row.
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

        upsert_driver = drivers["vector.transport.upsert"]
        upsert_driver.client = LostAcknowledgement(upsert_driver.client)
        near_row = _with_replay({
            "ref_id": "ref/document/near",
            "space_id": space_id,
            "purpose": "DOCUMENT",
            "vector": [1.0, 0.0, 0.0, 0.0],
        })
        with pytest.raises(ForestError) as uncertain:
            await upsert_driver.invoke(
                near_row, scope=scope, invocation=invocation,
            )
        assert uncertain.value.code == "VECTOR_INDEX_UPSERT_UNAVAILABLE"
        assert await upsert_driver.client.get(
            collection_name=collection_name, ids=["ref/document/near"],
            output_fields=["ref_id"], consistency_level="Strong",
        ) == [{"ref_id": "ref/document/near"}]
        async with store.pool.connection() as conn, conn.transaction():
            assert await upsert_driver.refs.lock(conn, "ref/document/near") is None
        await upsert_driver.close()
        upsert_driver = MilvusVectorIndexEffectDriver(
            configuration, 1, identity,
            {"name": "vector.transport.upsert", "kind": "vector-index.milvus"},
            bot_id=bot_id, store=store, runtime=kernel_runtime,
            token_resolver=resolve_token,
        )
        drivers["vector.transport.upsert"] = upsert_driver
        await upsert_driver.start()
        await drivers["vector.transport.upsert"].invoke(
            near_row,
            scope=scope,
            invocation=invocation,
        )
        async with store.pool.connection() as conn, conn.transaction():
            assert (await upsert_driver.refs.lock(conn, "ref/document/near"))["state"] == "ACTIVE"
        await drivers["vector.transport.upsert"].invoke(
            _with_replay({
                "ref_id": "ref/document/far",
                "space_id": space_id,
                "purpose": "DOCUMENT",
                "vector": [0.0, 1.0, 0.0, 0.0],
            }),
            scope=scope,
            invocation=invocation,
        )
        await drivers["vector.transport.upsert"].invoke(
            _with_replay({
                "ref_id": "ref/query",
                "space_id": space_id,
                "purpose": "QUERY",
                "vector": [1.0, 0.0, 0.0, 0.0],
            }),
            scope=scope,
            invocation=invocation,
        )

        result = await drivers["vector.transport.search"].invoke(
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
            scope=scope,
            invocation=invocation,
        )
        hits = result["hits"]
        assert [hit["ref_id"] for hit in hits] == [
            "ref/document/near",
            "ref/document/far",
        ]
        assert set(hits[0]) == {"ref_id", "score"}
        assert all("embedding" not in hit for hit in hits)

        with pytest.raises(ForestError) as rebound:
            await drivers["vector.transport.upsert"].invoke(
                _with_replay({
                    "ref_id": "ref/document/near",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "vector": [0.0, 0.0, 1.0, 0.0],
                }),
                scope=scope,
                invocation=invocation,
            )
        assert rebound.value.code == "VECTOR_INDEX_REF_CONFLICT"

        await drivers["vector.transport.delete"].invoke(
            {"ref_ids": ["ref/document/near"], "space_id": space_id},
            scope=scope,
            invocation=invocation,
        )
        result = await drivers["vector.transport.search"].invoke(
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
            scope=scope,
            invocation=invocation,
        )
        hits = result["hits"]
        assert [hit["ref_id"] for hit in hits] == ["ref/document/far"]
        with pytest.raises(ForestError) as deleted:
            await drivers["vector.transport.upsert"].invoke(
                _with_replay({
                    "ref_id": "ref/document/near",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "vector": [1.0, 0.0, 0.0, 0.0],
                }),
                scope=scope,
                invocation=invocation,
            )
        assert deleted.value.code == "VECTOR_INDEX_REF_DELETED"
        await drivers["vector.transport.delete"].invoke(
            {"ref_ids": ["ref/never-published"], "space_id": space_id},
            scope=scope,
            invocation=invocation,
        )
        with pytest.raises(ForestError) as preempted:
            await drivers["vector.transport.upsert"].invoke(
                _with_replay({
                    "ref_id": "ref/never-published",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "vector": [0.0, 0.0, 1.0, 0.0],
                }),
                scope=scope,
                invocation=invocation,
            )
        assert preempted.value.code == "VECTOR_INDEX_REF_DELETED"

        async with store.pool.connection() as conn, conn.transaction():
            binding, active = await upsert_driver.refs.lock_collection_state(conn)
        assert active
        assert binding["binding_revision"] == 1
        original_collection_id = binding["collection_id"]
        client = upsert_driver.client
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
        replacement_schema.add_field("input_digest", DataType.VARCHAR, max_length=71)
        replacement_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4)
        replacement_indexes = client.prepare_index_params()
        replacement_indexes.add_index(
            field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE"
        )
        await client.create_collection(
            collection_name=collection_name, schema=replacement_schema,
            index_params=replacement_indexes, consistency_level="Strong",
        )
        replacement_description = await client.describe_collection(
            collection_name=collection_name
        )
        assert replacement_description["collection_id"] != original_collection_id
        await drivers["vector.transport.search"].close()
        replacement_search = MilvusVectorIndexEffectDriver(
            configuration, 1, identity,
            {"name": "vector.transport.search", "kind": "vector-index.milvus"},
            bot_id=bot_id, store=store, runtime=kernel_runtime,
            token_resolver=resolve_token,
        )
        drivers["vector.transport.search"] = replacement_search
        await replacement_search.start()
        assert replacement_search.rebuild_required
        with pytest.raises(ForestError) as replaced:
            await replacement_search.invoke(
                {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
                scope=scope, invocation=invocation,
            )
        assert replaced.value.code == "VECTOR_INDEX_REBUILD_REQUIRED"
    finally:
        client = drivers["vector.transport.upsert"].client
        if client is not None and await client.has_collection(collection_name=collection_name):
            await client.drop_collection(collection_name=collection_name)
        for driver in drivers.values():
            await driver.close()
