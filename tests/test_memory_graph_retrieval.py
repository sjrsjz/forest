from types import SimpleNamespace

import pytest

from forest.builtin.memory_graph.python.graph_region import expand


def _context(adjacency, reads, *, missing=()):
    async def neighbours(port, body, **_kwargs):
        assert port == "memory.read-graph-neighbours"
        assert body["query_id"] == "query/test"
        node_id = body["source_unit_id"]
        reads.append(node_id)
        return {
            "node": None if node_id in missing else {"source_unit_id": node_id},
            "edges": adjacency.get(node_id, []), "association_sampled": False,
        }

    return SimpleNamespace(queries=SimpleNamespace(call=neighbours))


def _edge(edge_id, target, relation="ASSOCIATED"):
    return {"edge_id": edge_id, "target_id": target, "relation": relation}


def _policy(budget=32):
    return {
        "max_graph_nodes": budget, "max_association_edges_per_node": 32,
        "sequence_weight": 0.55, "association_weight": 0.9, "min_graph_relevance": 0.05,
    }


@pytest.mark.asyncio
async def test_lazy_diffusion_settles_strongest_path_once_and_handles_cycles():
    reads = []
    ctx = _context({
        "a": [_edge("ab", "b", "SEQUENCE"), _edge("ad", "d")],
        "b": [_edge("bc", "c", "SEQUENCE")],
        "d": [_edge("dc", "c")], "c": [_edge("ca", "a")],
    }, reads)
    result = await expand(ctx, "query/test", [{"source_unit_id": "a", "rank_score": 0.1}], _policy())
    assert [(item["source_unit_id"], item["activation"]) for item in result["nodes"]] == [
        ("a", 1.0), ("d", 0.9), ("c", 0.81), ("b", 0.55),
    ]
    assert result["nodes"][2]["path_parent_source_unit_id"] == "d"
    assert result["nodes"][2]["path_edge_id"] == "dc"
    assert reads == ["a", "d", "c", "b"]
    assert result["read_count"] == 4 and not result["truncated"]
    assert result["truncation_reason"] is None


@pytest.mark.asyncio
async def test_node_budget_stops_neighbour_reads_before_traversing_whole_graph():
    reads = []
    ctx = _context({"seed": [_edge(f"e{i}", f"n{i}") for i in range(32)]}, reads)
    result = await expand(
        ctx, "query/test", [{"source_unit_id": "seed", "rank_score": 0.1}], _policy(3)
    )
    assert len(reads) == result["read_count"] == len(result["nodes"]) == 3
    assert result["truncated"] and result["truncation_reason"] == "NODE_LIMIT"


@pytest.mark.asyncio
async def test_weight_one_neighbours_cannot_displace_direct_seeds_at_node_budget():
    reads = []
    ctx = _context({"z": [_edge("za", "a")]}, reads)
    policy = {**_policy(2), "association_weight": 1.0}
    result = await expand(ctx, "query/test", [
        {"source_unit_id": "z", "rank_score": 0.1},
        {"source_unit_id": "y", "rank_score": 0.02},
    ], policy)
    assert reads == ["z", "y"]
    assert all(node["is_seed"] for node in result["nodes"])
    assert result["truncated"] and result["truncation_reason"] == "NODE_LIMIT"


@pytest.mark.asyncio
async def test_unavailable_nodes_consume_read_budget_and_do_not_expand():
    reads = []
    ctx = _context({"gone": [_edge("hidden", "secret")]}, reads, missing=("gone",))
    result = await expand(
        ctx, "query/test", [{"source_unit_id": "gone", "rank_score": 0.1}], _policy()
    )
    assert reads == ["gone"] and result["read_count"] == 1
    assert result["nodes"] == [] and not result["truncated"]
    assert result["truncation_reason"] is None


@pytest.mark.asyncio
async def test_seed_and_frontier_limits_have_distinct_reasons():
    seeds = [{"source_unit_id": name, "rank_score": 0.1} for name in ("a", "b", "c")]
    no_edges = await expand(_context({}, []), "query/test", seeds, _policy(2))
    assert no_edges["truncation_reason"] == "SEED_LIMIT"
    with_edges = await expand(
        _context({"a": [_edge("ad", "d")]}, []), "query/test", seeds, _policy(2)
    )
    assert with_edges["truncation_reason"] == "SEED_AND_NODE_LIMIT"
