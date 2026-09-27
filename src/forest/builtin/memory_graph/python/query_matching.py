"""Persist ANN match receipts before computing seeds or expanding the graph."""

import hashlib
import json

from forest.errors import ForestError


def _digest(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")).hexdigest()


async def prepare(ctx, query_id, policy):
    progress = await ctx.queries.call("memory.read-match-preparation", {"query_id": query_id}, timeout=10)
    if not progress["prepared"]:
        progress = await ctx.uow.call(
            "memory.begin-query-matching",
            {"payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
             "memory": {"protocol": "forest.memory-query-matching-begin/1",
                        "query_id": query_id, "policy": policy,
                        "deletion_epoch_step_id": "payload_epoch"}},
            operation_id=ctx.operations.stable("matching-begin:" + _digest(query_id)), timeout=20,
        )
    if not progress["complete"]:
        policy = progress["policy"]
        completed = set(progress["completed_facet_refs"])
        after_facet_id = None
        for _ in range(8):
            page = await ctx.queries.call(
                "memory.read-query-vectors",
                {"query_id": query_id, "after_facet_id": after_facet_id, "limit": 32}, timeout=10,
            )
            for facet in page["facets"]:
                if facet["ref_id"] in completed:
                    continue
                response = await ctx.services.call(
                    "vector.index.search",
                    {"query_ref_id": facet["ref_id"], "space_id": progress["space_id"],
                     "limit": policy["candidate_limit_per_facet"]},
                    operation_id=ctx.operations.stable("matching-search:" + _digest(facet["ref_id"])),
                    timeout=15,
                )
                if (response["query_ref_id"] != facet["ref_id"]
                        or response["space_id"] != progress["space_id"]):
                    raise ForestError("MEMORY_VECTOR_SEARCH_MISMATCH", "Index returned another query", 502)
                candidates = [
                    {"ref_id": hit["ref_id"], "candidate_rank": rank, "score": hit["score"]}
                    for rank, hit in enumerate(response["hits"], 1)
                ]
                progress = await ctx.uow.call(
                    "memory.record-query-matches",
                    {"payload_epoch": {"protocol": "forest.payload-epoch-capture/1"},
                     "memory": {"protocol": "forest.memory-query-matches/1", "query_id": query_id,
                                "query_ref_id": facet["ref_id"], "candidates": candidates,
                                "deletion_epoch_step_id": "payload_epoch"}},
                    operation_id=ctx.operations.stable(
                        "matching-receipt:" + _digest([facet["ref_id"], candidates])
                    ), timeout=20,
                )
                completed = set(progress["completed_facet_refs"])
            if not page["has_more"]:
                break
            after_facet_id = page["next_facet_id"]
        else:
            raise ForestError("MEMORY_QUERY_FACET_LIMIT", "Query exceeds 256 facets", 422)
    return {
        "protocol": "forest.memory-query-matched/1", "query_id": query_id,
        "facet_count": progress["facet_count"], "match_count": progress["match_count"],
    }
