"""Deterministic bounded expansion for archive-backed memory graphs."""

from __future__ import annotations

import hashlib
import heapq
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetrievalGraphEdge:
    edge_id: str
    source_id: str
    target_id: str
    relation: str
    weight: float

    def __post_init__(self) -> None:
        if not self.edge_id or not self.source_id or not self.target_id:
            raise ValueError("graph edge identities cannot be empty")
        if self.source_id == self.target_id:
            raise ValueError("graph edge endpoints must differ")
        if self.relation not in {"SEQUENCE", "ASSOCIATED"}:
            raise ValueError("unsupported retrieval graph relation")
        if isinstance(self.weight, bool) or not 0.0 < self.weight <= 1.0:
            raise ValueError("graph edge weight must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class RetrievalGraphActivation:
    node_id: str
    relevance: float
    parent_id: str | None
    edge_id: str | None


def _sample_rank(*, sampling_seed: str, source_id: str, edge: RetrievalGraphEdge) -> bytes:
    return hashlib.sha256(
        "\0".join((sampling_seed, source_id, edge.edge_id, edge.target_id)).encode("utf-8")
    ).digest()


def strongest_path_activations(
    *,
    ordered_nodes: tuple[str, ...],
    edges: tuple[RetrievalGraphEdge, ...],
    seeds: tuple[str, ...],
    sampling_seed: str,
    min_relevance: float = 0.05,
    max_nodes: int = 32,
    max_association_edges_per_node: int = 32,
) -> tuple[RetrievalGraphActivation, ...]:
    """Return maximum-product activations with bounded association fan-out.

    Every qualified seed starts at 1.0. Sequence edges are never sampled.
    When an expanded node has more associative neighbours than its configured
    cap, a query-seeded hash ranking selects the subset. The subset therefore
    varies across frozen queries while replaying one query exactly.
    """

    if isinstance(min_relevance, bool) or not 0.0 < min_relevance <= 1.0:
        raise ValueError("min_relevance must be in (0, 1]")
    if type(max_nodes) is not int or max_nodes <= 0:
        raise ValueError("max_nodes must be positive")
    if (
        type(max_association_edges_per_node) is not int
        or max_association_edges_per_node <= 0
    ):
        raise ValueError("max_association_edges_per_node must be positive")
    if not sampling_seed:
        raise ValueError("sampling_seed cannot be empty")

    positions = {node_id: index for index, node_id in enumerate(ordered_nodes)}
    adjacency: dict[str, list[RetrievalGraphEdge]] = {}
    for edge in edges:
        if edge.source_id in positions and edge.target_id in positions:
            adjacency.setdefault(edge.source_id, []).append(edge)

    bounded_adjacency: dict[str, tuple[RetrievalGraphEdge, ...]] = {}
    for source_id, outgoing in adjacency.items():
        sequence = sorted(
            (edge for edge in outgoing if edge.relation == "SEQUENCE"),
            key=lambda edge: (positions[edge.target_id], edge.edge_id),
        )
        associations = [edge for edge in outgoing if edge.relation == "ASSOCIATED"]
        if len(associations) > max_association_edges_per_node:
            associations.sort(
                key=lambda edge: (
                    _sample_rank(
                        sampling_seed=sampling_seed,
                        source_id=source_id,
                        edge=edge,
                    ),
                    edge.edge_id,
                )
            )
            associations = associations[:max_association_edges_per_node]
        else:
            associations.sort(key=lambda edge: (positions[edge.target_id], edge.edge_id))
        bounded_adjacency[source_id] = tuple((*sequence, *associations))

    relevance = {
        node_id: 1.0 for node_id in dict.fromkeys(seeds) if node_id in positions
    }
    predecessor: dict[str, tuple[str, str]] = {}
    queue = [(-score, positions[node_id], node_id) for node_id, score in relevance.items()]
    heapq.heapify(queue)
    while queue:
        negative_score, _, source_id = heapq.heappop(queue)
        source_score = -negative_score
        if source_score < relevance.get(source_id, 0.0):
            continue
        for edge in bounded_adjacency.get(source_id, ()):
            propagated = source_score * edge.weight
            current = relevance.get(edge.target_id, 0.0)
            if propagated < min_relevance or propagated <= current:
                continue
            relevance[edge.target_id] = propagated
            predecessor[edge.target_id] = (source_id, edge.edge_id)
            heapq.heappush(
                queue,
                (-propagated, positions[edge.target_id], edge.target_id),
            )

    ranked = sorted(
        relevance.items(), key=lambda item: (-item[1], positions[item[0]], item[0])
    )[:max_nodes]
    return tuple(
        RetrievalGraphActivation(
            node_id=node_id,
            relevance=score,
            parent_id=predecessor.get(node_id, (None, None))[0],
            edge_id=predecessor.get(node_id, (None, None))[1],
        )
        for node_id, score in ranked
    )
