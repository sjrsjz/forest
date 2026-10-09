from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from test_schema_bundles import clean_authority as clean_authority

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
            "vector.index.upsert",
            "vector.index.search",
            "vector.index.delete",
        )
    }
    scope = identity
    invocation = SimpleNamespace(operation_id="test")
    collection_name = drivers["vector.index.upsert"].collection_name
    try:
        for driver in drivers.values():
            await driver.start()
        collection = await drivers["vector.index.upsert"].client.describe_collection(
            collection_name=collection_name
        )
        assert {field["name"] for field in collection["fields"]} == {
            "ref_id", "bot_id", "space_id", "purpose", "embedding",
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

        upsert_driver = drivers["vector.index.upsert"]
        upsert_driver.client = LostAcknowledgement(upsert_driver.client)
        near_row = {
            "ref_id": "ref/document/near",
            "space_id": space_id,
            "purpose": "DOCUMENT",
            "vector": [1.0, 0.0, 0.0, 0.0],
        }
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
            {"name": "vector.index.upsert", "kind": "vector-index.milvus"},
            bot_id=bot_id, store=store, runtime=kernel_runtime,
            token_resolver=resolve_token,
        )
        drivers["vector.index.upsert"] = upsert_driver
        await upsert_driver.start()
        await drivers["vector.index.upsert"].invoke(
            near_row,
            scope=scope,
            invocation=invocation,
        )
        async with store.pool.connection() as conn, conn.transaction():
            assert (await upsert_driver.refs.lock(conn, "ref/document/near"))["state"] == "ACTIVE"
        await drivers["vector.index.upsert"].invoke(
            {
                "ref_id": "ref/document/far",
                "space_id": space_id,
                "purpose": "DOCUMENT",
                "vector": [0.0, 1.0, 0.0, 0.0],
            },
            scope=scope,
            invocation=invocation,
        )
        await drivers["vector.index.upsert"].invoke(
            {
                "ref_id": "ref/query",
                "space_id": space_id,
                "purpose": "QUERY",
                "vector": [1.0, 0.0, 0.0, 0.0],
            },
            scope=scope,
            invocation=invocation,
        )

        result = await drivers["vector.index.search"].invoke(
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
            await drivers["vector.index.upsert"].invoke(
                {
                    "ref_id": "ref/document/near",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "vector": [0.0, 0.0, 1.0, 0.0],
                },
                scope=scope,
                invocation=invocation,
            )
        assert rebound.value.code == "VECTOR_INDEX_REF_CONFLICT"

        await drivers["vector.index.delete"].invoke(
            {"ref_ids": ["ref/document/near"], "space_id": space_id},
            scope=scope,
            invocation=invocation,
        )
        result = await drivers["vector.index.search"].invoke(
            {"query_ref_id": "ref/query", "space_id": space_id, "limit": 2},
            scope=scope,
            invocation=invocation,
        )
        hits = result["hits"]
        assert [hit["ref_id"] for hit in hits] == ["ref/document/far"]
        with pytest.raises(ForestError) as deleted:
            await drivers["vector.index.upsert"].invoke(
                {
                    "ref_id": "ref/document/near",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "vector": [1.0, 0.0, 0.0, 0.0],
                },
                scope=scope,
                invocation=invocation,
            )
        assert deleted.value.code == "VECTOR_INDEX_REF_DELETED"
        await drivers["vector.index.delete"].invoke(
            {"ref_ids": ["ref/never-published"], "space_id": space_id},
            scope=scope,
            invocation=invocation,
        )
        with pytest.raises(ForestError) as preempted:
            await drivers["vector.index.upsert"].invoke(
                {
                    "ref_id": "ref/never-published",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "vector": [0.0, 0.0, 1.0, 0.0],
                },
                scope=scope,
                invocation=invocation,
            )
        assert preempted.value.code == "VECTOR_INDEX_REF_DELETED"
    finally:
        client = drivers["vector.index.upsert"].client
        if client is not None:
            await client.drop_collection(collection_name=collection_name)
        for driver in drivers.values():
            await driver.close()
