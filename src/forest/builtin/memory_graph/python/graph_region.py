"""Lazy maximum-product expansion over Memory's bounded neighbour port."""

import asyncio
import heapq


async def expand(ctx, query_id, seeds, policy):
    """Read each visited node once; database sampling precedes Worker expansion.

    Scores cannot grow along a path because configured weights are in (0, 1].
    Therefore the maximum-score frontier settles each reachable node once.
    The read budget bounds work even when current revocations remove nodes.
    This read-only region still requires final evidence validation and freezing.
    """
    budget = policy["max_graph_nodes"]
    selected = seeds[:budget]
    seed_scores = {seed["source_unit_id"]: seed["rank_score"] for seed in selected}
    seed_order = {seed["source_unit_id"]: rank for rank, seed in enumerate(selected)}
    best = {node_id: 1.0 for node_id in seed_scores}
    parents = {}
    queue = [(-1.0, seed_order[node_id], node_id) for node_id in seed_scores]
    heapq.heapify(queue)
    visited = set()
    nodes = []
    association_sampled = False
    async with asyncio.timeout(20):
        while queue and len(visited) < budget:
            negative_score, _, node_id = heapq.heappop(queue)
            score = -negative_score
            if node_id in visited or score < best[node_id]:
                continue
            visited.add(node_id)
            page = await ctx.queries.call(
                "memory.read-graph-neighbours",
                {"query_id": query_id, "source_unit_id": node_id,
                 "association_limit": policy["max_association_edges_per_node"]},
                timeout=10,
            )
            if page["node"] is None:
                continue
            parent_id, edge_id = parents.get(node_id, (None, None))
            nodes.append({
                **page["node"], "activation": score,
                "rank_score": seed_scores.get(node_id, 0.0), "is_seed": node_id in seed_scores,
                "path_parent_source_unit_id": parent_id, "path_edge_id": edge_id,
            })
            association_sampled |= page["association_sampled"]
            for edge in page["edges"]:
                target_id = edge["target_id"]
                weight = policy[
                    "sequence_weight" if edge["relation"] == "SEQUENCE" else "association_weight"
                ]
                propagated = score * weight
                if (target_id in visited or propagated < policy["min_graph_relevance"]
                        or propagated <= best.get(target_id, 0.0)):
                    continue
                best[target_id] = propagated
                parents[target_id] = (node_id, edge["edge_id"])
                heapq.heappush(queue, (-propagated, len(seed_order), target_id))
    return {
        "nodes": nodes, "read_count": len(visited),
        "truncated": len(seeds) > budget or any(
            node_id not in visited and -score == best[node_id] for score, _, node_id in queue
        ),
        "association_sampled": association_sampled,
    }
