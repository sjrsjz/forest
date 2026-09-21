from __future__ import annotations

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from forest.clean_vector_index_runtime import CleanMilvusVectorIndexRuntime
from forest.errors import ForestError


@pytest.mark.asyncio
async def test_vector_index_configuration_rejects_mutable_space_shape():
    with pytest.raises(ForestError) as caught:
        CleanMilvusVectorIndexRuntime._parse_configuration(
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
async def test_milvus_vector_index_round_trip_by_reference():
    uri = os.environ.get("FOREST_TEST_MILVUS_URI")
    if not uri:
        pytest.skip("FOREST_TEST_MILVUS_URI is not configured")

    suffix = uuid4().hex
    bot_id = f"bot/vector-test/{suffix}"
    collection_name = f"forest_test_{suffix}"

    async def verify_generation(**_values):
        return None

    async def resolve_token(_ref, _revision):
        return ""

    runtime = CleanMilvusVectorIndexRuntime(
        repository=SimpleNamespace(store=SimpleNamespace(bot_id=bot_id)),
        module_configuration=None,
        grants=None,
        generation_verifier=verify_generation,
        token_resolver=resolve_token,
    )
    runtime.configuration = {
        "protocol": "forest.vector-index.milvus/1",
        "endpoint": uri,
        "credential_ref": "",
        "timeout_seconds": 30,
        "space_id": "space/test",
        "dimensions": 4,
        "metric": "COSINE",
    }
    runtime.configuration_revision = 1
    runtime.identity = {
        "collection_name": collection_name,
        "space_id": "space/test",
        "dimensions": 4,
        "metric": "COSINE",
    }
    runtime.generation_identity = SimpleNamespace(generation_id="generation/vector-test")
    try:
        await runtime.start()
        runtime.activate_generation()
        await runtime.upsert(
            ref_id="ref/document/near",
            space_id="space/test",
            purpose="DOCUMENT",
            source_unit_id="unit/near",
            source_revision=1,
            modality="TEXT",
            vector=(1.0, 0.0, 0.0, 0.0),
        )
        await runtime.upsert(
            ref_id="ref/document/far",
            space_id="space/test",
            purpose="DOCUMENT",
            source_unit_id="unit/far",
            source_revision=1,
            modality="IMAGE",
            vector=(0.0, 1.0, 0.0, 0.0),
        )
        await runtime.upsert(
            ref_id="ref/query",
            space_id="space/test",
            purpose="QUERY",
            source_unit_id="unit/query",
            source_revision=1,
            modality="TEXT",
            vector=(1.0, 0.0, 0.0, 0.0),
        )

        hits = await runtime.search(
            query_ref_id="ref/query", space_id="space/test", limit=2
        )
        assert [hit["ref_id"] for hit in hits] == [
            "ref/document/near",
            "ref/document/far",
        ]
        assert hits[0]["source_unit_id"] == "unit/near"
        assert all("embedding" not in hit for hit in hits)

        await runtime.delete(ref_ids=("ref/document/near",), space_id="space/test")
        hits = await runtime.search(
            query_ref_id="ref/query", space_id="space/test", limit=2
        )
        assert [hit["ref_id"] for hit in hits] == ["ref/document/far"]
    finally:
        if runtime.client is not None:
            await runtime.client.drop_collection(collection_name=collection_name)
        await runtime.close()
