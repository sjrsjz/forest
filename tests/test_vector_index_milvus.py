from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.package_effects import PackageEffectDriverRegistry
from forest.packages import PackageCatalog
from forest.vector_index_builtin import (
    validate_vector_index_configuration,
    vector_index_configuration_validator_factory,
)
from forest.vector_index_effect_driver import (
    MilvusVectorIndexEffectDriver,
    milvus_vector_index_effect_driver_factory,
)


@pytest.mark.asyncio
async def test_vector_index_configuration_rejects_mutable_space_shape():
    with pytest.raises(ForestError) as caught:
        validate_vector_index_configuration(
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
    assert caught.value.code == "VECTOR_INDEX_CONFIGURATION_INVALID"


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_milvus_vector_index_round_trip_by_reference(clean_authority, tmp_path):
    uri = os.environ.get("FOREST_TEST_MILVUS_URI")
    if not uri:
        pytest.skip("FOREST_TEST_MILVUS_URI is not configured")

    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
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
        configuration_validator_factory=vector_index_configuration_validator_factory(store),
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
        await drivers["vector.index.upsert"].invoke(
            {
                "ref_id": "ref/document/near",
                "space_id": space_id,
                "purpose": "DOCUMENT",
                "source_unit_id": "unit/near",
                "source_revision": 1,
                "modality": "TEXT",
                "vector": [1.0, 0.0, 0.0, 0.0],
            },
            scope=scope,
            invocation=invocation,
        )
        await drivers["vector.index.upsert"].invoke(
            {
                "ref_id": "ref/document/far",
                "space_id": space_id,
                "purpose": "DOCUMENT",
                "source_unit_id": "unit/far",
                "source_revision": 1,
                "modality": "IMAGE",
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
                "source_unit_id": "unit/query",
                "source_revision": 1,
                "modality": "TEXT",
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
        assert hits[0]["source_unit_id"] == "unit/near"
        assert all("embedding" not in hit for hit in hits)

        with pytest.raises(ForestError) as rebound:
            await drivers["vector.index.upsert"].invoke(
                {
                    "ref_id": "ref/document/near",
                    "space_id": space_id,
                    "purpose": "DOCUMENT",
                    "source_unit_id": "unit/near",
                    "source_revision": 1,
                    "modality": "TEXT",
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
                    "source_unit_id": "unit/near",
                    "source_revision": 1,
                    "modality": "TEXT",
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
                    "source_unit_id": "unit/new",
                    "source_revision": 1,
                    "modality": "TEXT",
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
