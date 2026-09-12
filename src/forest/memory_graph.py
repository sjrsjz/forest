"""Versioned, metadata-only graph reduction of an already admitted Bot fact set.

This module performs no I/O, extraction, authorization, clock allocation or head
publication. Its caller freezes eligibility, source heads and suppression under
the authoritative storage protocol, and rechecks those dependencies on publish
and retrieval. The resulting graph is not an authorization or retrieval response.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from typing import Literal

from forest.errors import ForestError
from forest.memory_types import SourceRef

REDUCER_VERSION = "forest-graph-v1"
AGGREGATION_VERSION = "source-manifest-max-sum-micros-v1"
IDENTITY_VERSION = "forest-graph-id-v1"
MAX_FACTS = 100_000
MAX_REFERENCES = 1024
MAX_WEIGHT_MICROS = 10**12
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SOURCE_KINDS = frozenset({"GROUP", "DIRECT", "DOCUMENT", "SYSTEM", "UNKNOWN"})
_NODE_KINDS = frozenset({"SOURCE_UNIT", "ASSERTION", "ENTITY", "TRUNK_ROOT"})
_RELATIONS = frozenset({"ASSOCIATES_WITH", "EVIDENCE", "CONTRADICTS", "SUBJECT_OF"})
_SYMMETRIC = frozenset({"ASSOCIATES_WITH", "CONTRADICTS"})


def _bad(code: str, message: str):
    raise ForestError(code, message, 422)


def _text(value, *, empty=False):
    try:
        size = len(value.encode("utf-8")) if isinstance(value, str) else 257
    except UnicodeError:
        size = 257
    if (
        not isinstance(value, str)
        or size > 256
        or (not empty and not value)
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        _bad("GRAPH_INVALID", "Graph identifiers must be bounded text without control characters")


def _positive(value):
    if type(value) is not int or not 1 <= value < 2**63:
        _bad("GRAPH_INVALID", "Graph revisions and positions must be positive integers")


def _digest(value):
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _bad("GRAPH_INVALID", "Graph content references require a SHA-256 digest")


def _version(value):
    if type(value) is not int or value != 1:
        _bad("GRAPH_VERSION_UNSUPPORTED", "Unsupported graph metadata schema version")


def _refs(values):
    result = tuple(values)
    if not 1 <= len(result) <= MAX_REFERENCES or any(
        not isinstance(ref, SourceRef) for ref in result
    ):
        _bad("GRAPH_INVALID", "Facts require a bounded, nonempty typed source reference set")
    for ref in result:
        _text(ref.source_unit_id)
        _positive(ref.source_revision)
        _text(ref.part_id)
    return tuple(
        sorted(set(result), key=lambda r: (r.source_unit_id, r.source_revision, r.part_id))
    )


def _strings(values, *, limit=MAX_REFERENCES):
    result = tuple(values)
    if len(result) > limit:
        _bad("GRAPH_INVALID", "Graph reference set is too large")
    for value in result:
        _text(value)
    return tuple(sorted(set(result)))


def _canonical(value) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("ascii")


@dataclass(frozen=True)
class SourcePart:
    part_id: str
    part_index: int
    kind: Literal["text", "image", "json", "control"]
    content_digest: str
    part_revision: int = 1

    def __post_init__(self):
        _text(self.part_id)
        if (
            self.part_id == "*"
            or type(self.part_index) is not int
            or not 0 <= self.part_index < MAX_REFERENCES
        ):
            _bad("GRAPH_INVALID", "Source part identity or position is invalid")
        if self.kind not in {"text", "image", "json", "control"}:
            _bad("GRAPH_INVALID", "Unknown source part kind")
        _digest(self.content_digest)
        _positive(self.part_revision)


@dataclass(frozen=True)
class SourceUnit:
    bot_id: str
    source_unit_id: str
    source_id: str
    source_kind: Literal["GROUP", "DIRECT", "DOCUMENT", "SYSTEM", "UNKNOWN"]
    source_order_seq: int
    source_revision: int
    content_digest: str
    payload_ref: str
    unit_kind: str
    author_id: str
    parts: tuple[SourcePart, ...] = ()
    available_parts: tuple[str, ...] | None = None
    group_source_id: str = ""
    platform: str = ""
    realm_id: str = ""
    external_group_id: str = ""
    identity_incarnation: int = 1
    source_conversation_id: str = ""
    source_ingress_seq: int | None = None
    occurrence_refs: tuple[str, ...] = ()
    eligible: bool = True
    available: bool = True
    schema_version: int = 1

    def __post_init__(self):
        _version(self.schema_version)
        for value in (
            self.bot_id,
            self.source_unit_id,
            self.source_id,
            self.payload_ref,
            self.unit_kind,
            self.author_id,
        ):
            _text(value)
        for value in (
            self.group_source_id,
            self.platform,
            self.realm_id,
            self.external_group_id,
            self.source_conversation_id,
        ):
            _text(value, empty=True)
        for value in (self.source_order_seq, self.source_revision, self.identity_incarnation):
            _positive(value)
        if self.source_ingress_seq is not None:
            _positive(self.source_ingress_seq)
            if not self.source_conversation_id:
                _bad("GRAPH_INVALID", "Ingress positions require their source conversation")
        if type(self.eligible) is not bool or type(self.available) is not bool:
            _bad("GRAPH_INVALID", "Source availability must be explicitly boolean")
        _digest(self.content_digest)
        if self.source_kind not in _SOURCE_KINDS:
            _bad("GRAPH_INVALID", "Unknown source kind")
        if self.source_kind == "GROUP":
            if (
                self.group_source_id != self.source_id
                or not self.platform
                or not self.realm_id
                or not self.external_group_id
            ):
                _bad("GRAPH_INVALID", "Group sources require a stable complete group identity")
        elif self.group_source_id or self.external_group_id:
            _bad("GRAPH_INVALID", "Non-group sources cannot claim a group identity")
        parts = tuple(self.parts)
        if len(parts) > MAX_REFERENCES or any(not isinstance(p, SourcePart) for p in parts):
            _bad("GRAPH_INVALID", "Source parts must be bounded typed metadata")
        if len({p.part_id for p in parts}) != len(parts) or len(
            {p.part_index for p in parts}
        ) != len(parts):
            _bad("GRAPH_INVALID", "Source part identities and positions must be unique")
        object.__setattr__(self, "parts", tuple(sorted(parts, key=lambda p: p.part_index)))
        available = (
            _strings(p.part_id for p in parts)
            if self.available_parts is None
            else _strings(self.available_parts)
        )
        if not set(available).issubset(p.part_id for p in parts):
            _bad("GRAPH_INVALID", "Available parts must belong to this source revision")
        object.__setattr__(self, "available_parts", available)
        object.__setattr__(self, "occurrence_refs", _strings(self.occurrence_refs))


@dataclass(frozen=True)
class LogicalKey:
    subject_id: str
    predicate: str
    valid_time_scope: str = "unspecified"
    key_version: str = "logical-key-v1"
    cardinality: Literal["one", "many"] = "one"

    def __post_init__(self):
        for value in (self.subject_id, self.predicate, self.valid_time_scope, self.key_version):
            _text(value)
        if self.cardinality not in {"one", "many"}:
            _bad("GRAPH_INVALID", "Logical keys require an explicit cardinality contract")


@dataclass(frozen=True)
class ValueRef:
    value_digest: str
    value_ref: str
    value_kind: Literal["text", "number", "boolean", "json", "entity"] = "text"

    def __post_init__(self):
        _digest(self.value_digest)
        _text(self.value_ref)
        if self.value_kind not in {"text", "number", "boolean", "json", "entity"}:
            _bad("GRAPH_INVALID", "Unknown assertion value kind")


@dataclass(frozen=True)
class Assertion:
    bot_id: str
    operation_id: str
    assertion_id: str
    logical_key: LogicalKey
    value: ValueRef
    source_refs: tuple[SourceRef, ...]
    schema_version: int = 1

    def __post_init__(self):
        _version(self.schema_version)
        for value in (self.bot_id, self.operation_id, self.assertion_id):
            _text(value)
        if not isinstance(self.logical_key, LogicalKey) or not isinstance(self.value, ValueRef):
            _bad("GRAPH_INVALID", "Assertions require typed logical keys and payload references")
        object.__setattr__(self, "source_refs", _refs(self.source_refs))


@dataclass(frozen=True)
class NodeRef:
    bot_id: str
    kind: Literal["SOURCE_UNIT", "ASSERTION", "ENTITY", "TRUNK_ROOT"]
    identity: str

    def __post_init__(self):
        _text(self.bot_id)
        _text(self.identity)
        if self.kind not in _NODE_KINDS:
            _bad("GRAPH_INVALID", "Unknown graph endpoint kind")


def node_id(ref: NodeRef) -> str:
    if not isinstance(ref, NodeRef):
        _bad("GRAPH_INVALID", "Node identity requires a typed Bot-scoped reference")
    return (
        "graph-node:sha256:"
        + hashlib.sha256(_canonical([IDENTITY_VERSION, asdict(ref)])).hexdigest()
    )


def relation_id(
    bot_id: str, kind: str, left: NodeRef, right: NodeRef, relation_key: str = ""
) -> str:
    _text(bot_id)
    _text(relation_key, empty=True)
    if kind not in _RELATIONS | {"MEMBER_OF", "TRUNK_NEXT"}:
        _bad("GRAPH_INVALID", "Unknown graph relation kind")
    if (
        not isinstance(left, NodeRef)
        or not isinstance(right, NodeRef)
        or left.bot_id != bot_id
        or right.bot_id != bot_id
    ):
        _bad("GRAPH_BOT_MISMATCH", "Graph relations cannot cross Bot domains")
    ends = [node_id(left), node_id(right)]
    if kind in _SYMMETRIC:
        ends.sort()
    return (
        "graph-edge:sha256:"
        + hashlib.sha256(
            _canonical([IDENTITY_VERSION, bot_id, kind, ends, relation_key])
        ).hexdigest()
    )


@dataclass(frozen=True)
class RelationContribution:
    bot_id: str
    operation_id: str
    contribution_id: str
    kind: Literal["ASSOCIATES_WITH", "EVIDENCE", "CONTRADICTS", "SUBJECT_OF"]
    left: NodeRef
    right: NodeRef
    source_refs: tuple[SourceRef, ...]
    relation_key: str = ""
    weight_micros: int = 1_000_000
    schema_version: int = 1

    def __post_init__(self):
        _version(self.schema_version)
        for value in (self.bot_id, self.operation_id, self.contribution_id):
            _text(value)
        relation_id(self.bot_id, self.kind, self.left, self.right, self.relation_key)
        if self.kind not in _RELATIONS or self.left == self.right:
            _bad("GRAPH_INVALID", "Explicit relations require two distinct, supported endpoints")
        if self.kind in _SYMMETRIC and node_id(self.left) > node_id(self.right):
            left, right = self.right, self.left
            object.__setattr__(self, "left", left)
            object.__setattr__(self, "right", right)
        if self.kind == "EVIDENCE" and (self.left.kind, self.right.kind) != (
            "ASSERTION",
            "SOURCE_UNIT",
        ):
            _bad("GRAPH_INVALID", "EVIDENCE points from an assertion to its source unit")
        if self.kind == "SUBJECT_OF" and (self.left.kind, self.right.kind) != (
            "ENTITY",
            "ASSERTION",
        ):
            _bad("GRAPH_INVALID", "SUBJECT_OF points from an entity to an assertion")
        if self.kind == "CONTRADICTS" and (self.left.kind, self.right.kind) != (
            "ASSERTION",
            "ASSERTION",
        ):
            _bad("GRAPH_INVALID", "CONTRADICTS requires two assertions")
        if self.left.kind == "TRUNK_ROOT" or self.right.kind == "TRUNK_ROOT":
            _bad("GRAPH_INVALID", "Explicit relations cannot redefine source trunk structure")
        if type(self.weight_micros) is not int or not 0 <= self.weight_micros <= MAX_WEIGHT_MICROS:
            _bad("GRAPH_INVALID", "Relation weights require bounded nonnegative integer millionths")
        refs = _refs(self.source_refs)
        for endpoint in (self.left, self.right):
            if endpoint.kind == "SOURCE_UNIT" and not any(
                ref.source_unit_id == endpoint.identity for ref in refs
            ):
                _bad(
                    "GRAPH_INVALID",
                    "Source endpoints must occur in the relation's evidence dependencies",
                )
        object.__setattr__(self, "source_refs", refs)


@dataclass(frozen=True)
class Retraction:
    bot_id: str
    operation_id: str
    target_kind: Literal["assertion", "contribution", "relation"]
    target_id: str
    schema_version: int = 1

    def __post_init__(self):
        _version(self.schema_version)
        for value in (self.bot_id, self.operation_id, self.target_id):
            _text(value)
        if self.target_kind not in {"assertion", "contribution", "relation"}:
            _bad("GRAPH_INVALID", "Retractions must name a specific stable fact identity")


@dataclass(frozen=True)
class Suppression:
    bot_id: str
    source_ids: tuple[str, ...] = ()
    source_unit_ids: tuple[str, ...] = ()
    parts: tuple[tuple[str, str], ...] = ()
    assertion_ids: tuple[str, ...] = ()
    relation_ids: tuple[str, ...] = ()

    def __post_init__(self):
        _text(self.bot_id)
        for name in ("source_ids", "source_unit_ids", "assertion_ids", "relation_ids"):
            object.__setattr__(self, name, _strings(getattr(self, name), limit=MAX_FACTS))
        parts = tuple(self.parts)
        if len(parts) > MAX_FACTS:
            _bad("GRAPH_INVALID", "Suppression reference set is too large")
        for pair in parts:
            if not isinstance(pair, tuple) or len(pair) != 2:
                _bad("GRAPH_INVALID", "Part suppression requires unit and part identities")
            for value in pair:
                _text(value)
        object.__setattr__(self, "parts", tuple(sorted(set(parts))))


@dataclass(frozen=True)
class GraphNode:
    node_id: str
    ref: NodeRef
    source: SourceUnit | None = None
    logical_key: LogicalKey | None = None
    value: ValueRef | None = None


@dataclass(frozen=True)
class GraphEdge:
    edge_id: str
    kind: str
    from_node_id: str
    to_node_id: str
    relation_key: str
    source_refs: tuple[SourceRef, ...]
    contribution_ids: tuple[str, ...]
    assertion_ids: tuple[str, ...]
    weight_micros: int


@dataclass(frozen=True)
class Candidate:
    value_kind: str
    value_digest: str
    value_refs: tuple[str, ...]
    assertion_ids: tuple[str, ...]
    source_refs: tuple[SourceRef, ...]


@dataclass(frozen=True)
class FactProjection:
    logical_key: LogicalKey
    candidates: tuple[Candidate, ...]
    conflict: bool


@dataclass(frozen=True)
class GraphProjection:
    bot_id: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    facts: tuple[FactProjection, ...]
    reducer_version: str = REDUCER_VERSION
    aggregation_version: str = AGGREGATION_VERSION

    def canonical_bytes(self) -> bytes:
        """Stable semantic bytes; caller owns digest, coverage, CAS and head."""
        return _canonical(asdict(self))


def _unique(items, expected_type, bot_id, identity):
    result = {}
    for index, item in enumerate(items):
        if index >= MAX_FACTS:
            _bad("GRAPH_INPUT_TOO_LARGE", "Graph snapshot exceeds the fact limit")
        if not isinstance(item, expected_type):
            _bad("GRAPH_INVALID", "Graph inputs must use their declared typed metadata")
        if item.bot_id != bot_id:
            _bad("GRAPH_BOT_MISMATCH", "A graph snapshot cannot combine different Bots")
        key = identity(item)
        if key in result and result[key] != item:
            raise ForestError(
                "IDEMPOTENCY_CONFLICT",
                "A stable graph identity has different accepted payloads",
                409,
            )
        result[key] = item
    return result


def reduce_graph(
    bot_id: str,
    *,
    sources: Iterable[SourceUnit],
    assertions: Iterable[Assertion] = (),
    relations: Iterable[RelationContribution] = (),
    retractions: Iterable[Retraction] = (),
    suppression: Suppression | None = None,
    source_heads: Mapping[str, int] | None = None,
) -> GraphProjection:
    """Rebuild a complete deterministic view; absence never proves completeness.

    Multiple admitted content revisions require explicitly selected source_heads.
    Missing, ineligible or suppressed dependencies hide their entire fact. Precise
    part dependencies survive removal of an unrelated part; whole-unit references
    do not. Callers must retain the full input fact set across merges/rebuilds.
    """
    _text(bot_id)
    suppression = suppression or Suppression(bot_id)
    if not isinstance(suppression, Suppression) or suppression.bot_id != bot_id:
        _bad("GRAPH_BOT_MISMATCH", "Suppression belongs to another Bot")
    versions = _unique(sources, SourceUnit, bot_id, lambda s: (s.source_unit_id, s.source_revision))
    assertions = _unique(assertions, Assertion, bot_id, lambda a: a.assertion_id)
    relations = _unique(relations, RelationContribution, bot_id, lambda r: r.contribution_id)
    retractions = _unique(retractions, Retraction, bot_id, lambda r: r.operation_id)
    _unique(
        (*assertions.values(), *relations.values(), *retractions.values()),
        (Assertion, RelationContribution, Retraction),
        bot_id,
        lambda op: op.operation_id,
    )
    by_unit, identities, positions, external_groups = defaultdict(list), {}, {}, {}
    for unit in versions.values():
        by_unit[unit.source_unit_id].append(unit)
        identity = (
            unit.source_kind,
            unit.group_source_id,
            unit.platform,
            unit.realm_id,
            unit.external_group_id,
            unit.identity_incarnation,
        )
        if unit.source_id in identities and identities[unit.source_id] != identity:
            _bad(
                "GRAPH_SOURCE_IDENTITY_CONFLICT",
                "A source identity cannot change its registered group",
            )
        identities[unit.source_id] = identity
        if unit.source_kind == "GROUP":
            external = (
                unit.platform,
                unit.realm_id,
                unit.external_group_id,
                unit.identity_incarnation,
            )
            if external in external_groups and external_groups[external] != unit.source_id:
                _bad(
                    "GRAPH_SOURCE_IDENTITY_CONFLICT",
                    "One registered external group cannot create two trunks",
                )
            external_groups[external] = unit.source_id
        position = (unit.source_id, unit.source_order_seq)
        if position in positions and positions[position] != unit.source_unit_id:
            _bad(
                "GRAPH_SOURCE_POSITION_CONFLICT",
                "Two source units cannot occupy one admitted source position",
            )
        positions[position] = unit.source_unit_id
    selected = {}
    heads = dict(source_heads) if source_heads is not None else {}
    for unit_id, items in by_unit.items():
        fixed = {(s.source_id, s.source_order_seq) for s in items}
        if len(fixed) != 1:
            _bad("GRAPH_SOURCE_POSITION_CONFLICT", "Content revisions cannot move a source unit")
        if unit_id in heads:
            _positive(heads[unit_id])
            match = versions.get((unit_id, heads[unit_id]))
            if match is None:
                _bad(
                    "GRAPH_SOURCE_HEAD_MISSING",
                    "The selected source head is absent from this snapshot",
                )
            selected[unit_id] = match
        elif len(items) == 1:
            selected[unit_id] = items[0]
        else:
            _bad(
                "GRAPH_SOURCE_HEAD_REQUIRED",
                "Multiple source revisions require an explicitly admitted head",
            )
    if set(heads) - set(by_unit):
        _bad("GRAPH_SOURCE_HEAD_MISSING", "A selected source head is absent from this snapshot")
    suppressed_parts = set(suppression.parts)
    suppressed_units = set(suppression.source_unit_ids)
    suppressed_sources = set(suppression.source_ids)
    units_with_suppressed_parts = {identity for identity, _ in suppressed_parts}

    def base_visible(unit):
        return (
            unit.eligible
            and unit.available
            and unit.source_id not in suppressed_sources
            and unit.source_unit_id not in suppressed_units
            and (unit.source_unit_id, "*") not in suppressed_parts
        )

    def readable(ref):
        unit = selected.get(ref.source_unit_id)
        if unit is None or unit.source_revision != ref.source_revision or not base_visible(unit):
            return False
        if ref.part_id == "*":
            return unit.source_unit_id not in units_with_suppressed_parts and set(
                unit.available_parts
            ) == {p.part_id for p in unit.parts}
        return (
            ref.part_id in unit.available_parts
            and (unit.source_unit_id, ref.part_id) not in suppressed_parts
        )

    visible_sources = {
        identity: unit
        for identity, unit in selected.items()
        if base_visible(unit)
        and (
            not unit.parts
            or any(
                readable(SourceRef(identity, unit.source_revision, part.part_id))
                for part in unit.parts
            )
        )
    }
    withdrawn = {(r.target_kind, r.target_id) for r in retractions.values()}
    active = {
        identity: assertion
        for identity, assertion in assertions.items()
        if identity not in suppression.assertion_ids
        and ("assertion", identity) not in withdrawn
        and all(readable(ref) for ref in assertion.source_refs)
    }
    nodes, edge_data = {}, {}

    def add_node(ref, **metadata):
        result = node_id(ref)
        nodes[result] = GraphNode(result, ref, **metadata)
        return result

    def edge(kind, left, right, *, key="", refs=(), contribution=None, assertion_id=None):
        identity = relation_id(bot_id, kind, left, right, key)
        if identity in suppression.relation_ids or ("relation", identity) in withdrawn:
            if kind in {"MEMBER_OF", "TRUNK_NEXT"}:
                _bad(
                    "GRAPH_STRUCTURAL_RETRACTION",
                    "Trunk structure is derived from source membership, not mutable relation facts",
                )
            return
        ends = (node_id(left), node_id(right))
        if kind in _SYMMETRIC:
            ends = tuple(sorted(ends))
        row = edge_data.setdefault(
            identity,
            {
                "kind": kind,
                "ends": ends,
                "key": key,
                "refs": set(),
                "contributions": {},
                "assertions": set(),
            },
        )
        row["refs"].update(refs)
        if contribution:
            row["contributions"][contribution.contribution_id] = contribution
        if assertion_id:
            row["assertions"].add(assertion_id)

    trunks = defaultdict(list)
    for unit in visible_sources.values():
        ref = NodeRef(bot_id, "SOURCE_UNIT", unit.source_unit_id)
        effective_parts = tuple(
            part.part_id
            for part in unit.parts
            if readable(SourceRef(unit.source_unit_id, unit.source_revision, part.part_id))
        )
        add_node(ref, source=replace(unit, available_parts=effective_parts))
        if unit.source_kind == "GROUP":
            root = NodeRef(bot_id, "TRUNK_ROOT", unit.group_source_id)
            add_node(root)
            edge("MEMBER_OF", ref, root)
            trunks[unit.group_source_id].append(unit)
    for units in trunks.values():
        units.sort(key=lambda unit: (unit.source_order_seq, unit.source_unit_id))
        for left, right in zip(units, units[1:]):
            edge(
                "TRUNK_NEXT",
                NodeRef(bot_id, "SOURCE_UNIT", left.source_unit_id),
                NodeRef(bot_id, "SOURCE_UNIT", right.source_unit_id),
            )
    contracts, groups = {}, defaultdict(lambda: defaultdict(list))
    for assertion in active.values():
        ref = NodeRef(bot_id, "ASSERTION", assertion.assertion_id)
        entity = NodeRef(bot_id, "ENTITY", assertion.logical_key.subject_id)
        add_node(ref, logical_key=assertion.logical_key, value=assertion.value)
        add_node(entity)
        edge(
            "SUBJECT_OF",
            entity,
            ref,
            refs=assertion.source_refs,
            assertion_id=assertion.assertion_id,
        )
        for source in assertion.source_refs:
            edge(
                "EVIDENCE",
                ref,
                NodeRef(bot_id, "SOURCE_UNIT", source.source_unit_id),
                refs=(source,),
                assertion_id=assertion.assertion_id,
            )
        key = assertion.logical_key
        semantic = (key.key_version, key.subject_id, key.predicate, key.valid_time_scope)
        if semantic in contracts and contracts[semantic] != key.cardinality:
            _bad("GRAPH_KEY_CONTRACT_CONFLICT", "A logical key cannot mix cardinality contracts")
        contracts[semantic] = key.cardinality
        groups[key][(assertion.value.value_kind, assertion.value.value_digest)].append(assertion)
    for contribution in relations.values():
        if ("contribution", contribution.contribution_id) in withdrawn:
            continue
        if node_id(contribution.left) not in nodes or node_id(contribution.right) not in nodes:
            continue
        if not all(readable(ref) for ref in contribution.source_refs):
            continue
        edge(
            contribution.kind,
            contribution.left,
            contribution.right,
            key=contribution.relation_key,
            refs=contribution.source_refs,
            contribution=contribution,
        )
    edges = []
    for identity, row in sorted(edge_data.items()):
        weights = {}
        for contribution in row["contributions"].values():
            refs = contribution.source_refs
            weights[refs] = max(weights.get(refs, 0), contribution.weight_micros)
        edges.append(
            GraphEdge(
                identity,
                row["kind"],
                *row["ends"],
                row["key"],
                tuple(
                    sorted(
                        row["refs"], key=lambda r: (r.source_unit_id, r.source_revision, r.part_id)
                    )
                ),
                tuple(sorted(row["contributions"])),
                tuple(sorted(row["assertions"])),
                sum(weights.values()),
            )
        )
    facts = []
    for key, values in sorted(groups.items(), key=lambda item: _canonical(asdict(item[0]))):
        candidates = []
        for (kind, digest), supports in sorted(values.items()):
            refs = {ref for support in supports for ref in support.source_refs}
            candidates.append(
                Candidate(
                    kind,
                    digest,
                    tuple(sorted({a.value.value_ref for a in supports})),
                    tuple(sorted(a.assertion_id for a in supports)),
                    tuple(
                        sorted(refs, key=lambda r: (r.source_unit_id, r.source_revision, r.part_id))
                    ),
                )
            )
        facts.append(
            FactProjection(key, tuple(candidates), key.cardinality == "one" and len(candidates) > 1)
        )
    return GraphProjection(
        bot_id, tuple(nodes[key] for key in sorted(nodes)), tuple(edges), tuple(facts)
    )
