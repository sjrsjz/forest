"""Memory-owned qualification and rank fusion of vector seed candidates."""

import hashlib

from forest.errors import ForestError


async def retrieve(ctx, query_id, policy):
    facets = []
    after_facet_id = None
    for _ in range(8):
        page = await ctx.queries.call(
            "memory.read-query-vectors",
            {"query_id": query_id, "after_facet_id": after_facet_id, "limit": 32},
            timeout=10,
        )
        facets.extend(page["facets"])
        if not page["has_more"]:
            break
        after_facet_id = page["next_facet_id"]
    else:
        raise ForestError("MEMORY_QUERY_FACET_LIMIT", "Query exceeds 256 facets", 422)
    ranked = {}
    for facet in facets:
        response = await ctx.services.call(
            "vector.index.search",
            {"query_ref_id": facet["ref_id"], "space_id": page["space_id"],
             "limit": policy["candidate_limit_per_facet"]},
            operation_id=ctx.operations.stable(
                "vector-search:" + hashlib.sha256(facet["ref_id"].encode("utf-8")).hexdigest()
            ),
            timeout=15,
        )
        if response["query_ref_id"] != facet["ref_id"] or response["space_id"] != page["space_id"]:
            raise ForestError("MEMORY_VECTOR_SEARCH_MISMATCH", "Index returned another query", 502)
        candidates = [
            {"ref_id": hit["ref_id"], "candidate_rank": rank, "score": hit["score"]}
            for rank, hit in enumerate(response["hits"], 1)
            if hit["score"] >= policy["min_similarity"]
        ]
        seen_units = set()
        for offset in range(0, len(candidates), 32):
            qualified = await ctx.queries.call(
                "memory.qualify-vector-candidates",
                {"query_id": query_id, "query_facet_id": facet["ref_id"],
                 "candidates": candidates[offset:offset + 32]},
                timeout=10,
            )
            for hit in qualified["candidates"]:
                unit_id = hit["source_unit_id"]
                if unit_id in seen_units:
                    continue
                seen_units.add(unit_id)
                entry = ranked.setdefault(unit_id, {
                    "source_unit_id": unit_id, "source_revision": hit["source_revision"],
                    "source_id": hit["source_id"], "trunk_id": hit["trunk_id"],
                    "source_kind": hit["source_kind"], "platform": hit["platform"],
                    "realm_id": hit["realm_id"], "external_group_id": hit["external_group_id"],
                    "author_subject_ref": hit["author_subject_ref"],
                    "rank_score": 0.0, "activation": 1.0, "query_modalities": set(),
                })
                entry["rank_score"] += 1.0 / (60 + hit["candidate_rank"])
                entry["query_modalities"].add(facet["modality"])
    seeds = sorted(ranked.values(), key=lambda item: (-item["rank_score"], item["source_unit_id"]))[
        :policy["seed_limit"]
    ]
    return {
        "protocol": "forest.memory-query-seeds/1", "query_id": query_id,
        "space_id": page["space_id"], "index_snapshot_at": page["index_snapshot_at"],
        "seeds": [{**seed, "query_modalities": sorted(seed["query_modalities"])} for seed in seeds],
    }
