"""Memory-owned publication of derived vectors and deletion outbox entries."""

import asyncio
import math

from embedding_work import _digest, _plain

from forest.errors import ForestError


def _validated_vectors(result, facets, space_id, dimensions):
    if (
        not isinstance(result, dict)
        or result.get("protocol") != "forest.embedding-result/1"
        or result.get("status") != "RESPONDED"
        or result.get("space_id") != space_id
        or not isinstance(result.get("vectors"), list)
        or len(result["vectors"]) != len(facets)
    ):
        raise ForestError(
            "MEMORY_VECTOR_RESULT_INVALID", "Embedding batch result differs from its facets", 502
        )
    vectors = result["vectors"]
    for index, facet in enumerate(facets):
        vector = vectors[index]
        values = vector.get("values") if isinstance(vector, dict) else None
        if (
            facet["item_index"] != index
            or not isinstance(vector, dict)
            or vector.get("index") != index
            or vector.get("facet_id") != facet["facet_id"]
            or not isinstance(values, list)
            or len(values) != dimensions
            or any(type(value) not in (int, float) or not math.isfinite(value)
                   for value in values)
            or (facet["state"] != "SUPPRESSED" and
                _digest(values) != facet["vector_digest"])
        ):
            raise ForestError(
                "MEMORY_VECTOR_RESULT_INVALID", "Embedding vector differs from its facet", 502
            )
    return vectors


async def publish(ctx, task, profile):
    work = _plain(task["payload"])
    facets = work["facets"]
    active = any(facet["state"] != "SUPPRESSED" for facet in facets)
    vectors = (
        _validated_vectors(
            _plain(await ctx.payloads.read_json(work["result"])),
            facets, work["space_id"], profile["embedding"]["dimensions"],
        ) if active else None
    )
    slots = asyncio.Semaphore(4)

    async def publish_one(facet):
        if facet["state"] == "SUPPRESSED":
            return {"facet_id": facet["facet_id"], "status": "SKIPPED"}
        try:
            async with slots:
                response = _plain(await ctx.services.call(
                    "vector.index.upsert",
                    {
                        "ref_id": facet["facet_id"],
                        "space_id": work["space_id"],
                        "purpose": work["purpose"],
                        "source_unit_id": facet["source_unit_id"],
                        "source_revision": facet["source_revision"],
                        "modality": facet["modality"],
                        "vector": vectors[facet["item_index"]]["values"],
                    },
                    operation_id=ctx.operations.stable("vector-upsert:" + facet["facet_id"]),
                    timeout=15,
                ))
        except ForestError as exc:
            if exc.code != "VECTOR_INDEX_REF_DELETED":
                raise
            return {"facet_id": facet["facet_id"], "status": "TOMBSTONED"}
        if response != {"ref_id": facet["facet_id"], "space_id": work["space_id"]}:
            raise ForestError(
                "MEMORY_VECTOR_UPSERT_INVALID", "Vector index confirmed a different ref", 502
            )
        return {"facet_id": facet["facet_id"], "status": "PUBLISHED"}

    results = await asyncio.gather(*(publish_one(facet) for facet in facets),
                                   return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result
    committed = _plain(await ctx.uow.call(
        "memory.publish-vector-batch",
        {
            "payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
            "memory": {
                "protocol": "forest.memory-vector-publish/1",
                "attempt_id": work["attempt_id"],
                "claim_token": work["claim_token"],
                "space_id": work["space_id"],
                "results": results,
            },
        },
        operation_id=ctx.operations.stable("vector-publish:" + work["claim_token"]),
        timeout=20,
    ))
    if committed.get("attempt_id") != work["attempt_id"]:
        raise ForestError(
            "MEMORY_VECTOR_PUBLISH_INVALID", "Memory committed another vector batch", 502
        )
    return {
        "attempt_id": work["attempt_id"],
        "ready_count": committed["ready_count"],
        "deletion_count": committed["deletion_count"],
    }


async def delete(ctx, task):
    work = _plain(task["payload"])
    response = _plain(await ctx.services.call(
        "vector.index.delete",
        {"ref_ids": [work["index_ref_id"]], "space_id": work["space_id"]},
        operation_id=ctx.operations.stable("vector-delete:" + work["index_ref_id"]),
        timeout=20,
    ))
    if response != {"ref_ids": [work["index_ref_id"]], "space_id": work["space_id"]}:
        raise ForestError(
            "MEMORY_VECTOR_DELETE_INVALID", "Vector index confirmed another deletion", 502
        )
    return {"index_ref_id": work["index_ref_id"], "deleted": True}
