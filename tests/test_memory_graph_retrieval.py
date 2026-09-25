from forest.builtin.memory_graph.python.graph_region import (
    RetrievalGraphEdge,
    strongest_path_activations,
)


def _edge(edge_id, source, target, relation, weight):
    return RetrievalGraphEdge(edge_id, source, target, relation, weight)


def test_strongest_path_uses_qiyu_seed_and_multiplicative_semantics():
    activations = strongest_path_activations(
        ordered_nodes=("a", "b", "c", "d"),
        edges=(
            _edge("ab", "a", "b", "SEQUENCE", 0.55),
            _edge("bc", "b", "c", "SEQUENCE", 0.55),
            _edge("ad", "a", "d", "ASSOCIATED", 0.90),
            _edge("dc", "d", "c", "ASSOCIATED", 0.90),
        ),
        seeds=("a",),
        sampling_seed="query-1",
        min_relevance=0.05,
        max_nodes=32,
        max_association_edges_per_node=32,
    )
    assert [(item.node_id, item.relevance) for item in activations] == [
        ("a", 1.0),
        ("d", 0.9),
        ("c", 0.81),
        ("b", 0.55),
    ]
    assert activations[2].parent_id == "d"
    assert activations[2].edge_id == "dc"


def test_high_degree_associations_are_query_seeded_sampled_before_expansion():
    nodes = ("seed", *(f"n{i}" for i in range(20)))
    edges = tuple(
        _edge(f"edge-{index}", "seed", node, "ASSOCIATED", 0.9)
        for index, node in enumerate(nodes[1:])
    )

    def selected(query):
        return {
            item.node_id
            for item in strongest_path_activations(
                ordered_nodes=nodes,
                edges=edges,
                seeds=("seed",),
                sampling_seed=query,
                max_nodes=32,
                max_association_edges_per_node=5,
            )
            if item.node_id != "seed"
        }

    first = selected("query-a")
    assert len(first) == 5
    assert first == selected("query-a")
    assert first != selected("query-b")


def test_sequence_edges_are_not_consumed_by_association_fanout_limit():
    activations = strongest_path_activations(
        ordered_nodes=("seed", "before", "after", "x", "y"),
        edges=(
            _edge("s-before", "seed", "before", "SEQUENCE", 0.55),
            _edge("s-after", "seed", "after", "SEQUENCE", 0.55),
            _edge("s-x", "seed", "x", "ASSOCIATED", 0.9),
            _edge("s-y", "seed", "y", "ASSOCIATED", 0.9),
        ),
        seeds=("seed",),
        sampling_seed="query",
        max_nodes=32,
        max_association_edges_per_node=1,
    )
    reached = {item.node_id for item in activations}
    assert {"seed", "before", "after"} <= reached
    assert len(reached & {"x", "y"}) == 1
