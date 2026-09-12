"""Semantic convergence, provenance closure and explicit source-head selection."""

import hashlib
import itertools
import random
from dataclasses import replace

import pytest

from forest.errors import ForestError
from forest.memory_graph import (
    AGGREGATION_VERSION,
    Assertion,
    LogicalKey,
    NodeRef,
    RelationContribution,
    Retraction,
    SourcePart,
    SourceUnit,
    Suppression,
    ValueRef,
    node_id,
    reduce_graph,
    relation_id,
)
from forest.memory_types import SourceRef


def digest(value):
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def source(identity="a", group="group-a", position=1, *, bot="bot", revision=1, **changes):
    values = dict(
        bot_id=bot,
        source_unit_id=identity,
        source_id=group,
        source_kind="GROUP",
        source_order_seq=position,
        source_revision=revision,
        content_digest=digest(identity),
        payload_ref="payload:" + digest(identity),
        unit_kind="GROUP_MESSAGE",
        author_id="author:" + identity,
        group_source_id=group,
        platform="onebot-v11",
        realm_id="qq-global",
        external_group_id=group,
        source_conversation_id="conversation:" + group,
        source_ingress_seq=position,
        parts=(
            SourcePart("text", 0, "text", digest("text:" + identity)),
            SourcePart("image", 1, "image", digest("shared-image")),
        ),
    )
    return SourceUnit(**(values | changes))


def assertion(identity, refs, value="green", *, bot="bot", key=None):
    return Assertion(
        bot,
        "op:" + identity,
        identity,
        key or LogicalKey("person-1", "tea"),
        ValueRef(digest(value), "payload:" + digest(value)),
        tuple(refs),
    )


def association(
    identity="relation",
    refs=None,
    *,
    weight=1000,
    bot="bot",
    left="a",
    right="b",
    kind="ASSOCIATES_WITH",
):
    return RelationContribution(
        bot,
        "op:" + identity,
        identity,
        kind,
        NodeRef(bot, "SOURCE_UNIT", left),
        NodeRef(bot, "SOURCE_UNIT", right),
        tuple(refs or (SourceRef(left), SourceRef(right))),
        weight_micros=weight,
    )


def projected(sources, assertions=(), relations=(), retractions=(), **kwargs):
    return reduce_graph(
        "bot",
        sources=sources,
        assertions=assertions,
        relations=relations,
        retractions=retractions,
        **kwargs,
    )


def edges(graph, kind):
    return [edge for edge in graph.edges if edge.kind == kind]


def pairs(graph, kind):
    identities = {node.node_id: node.ref.identity for node in graph.nodes}
    return {
        (identities[edge.from_node_id], identities[edge.to_node_id]) for edge in edges(graph, kind)
    }


def fails(code, call):
    with pytest.raises(ForestError) as caught:
        call()
    assert caught.value.code == code


def test_all_source_permutations_late_insertion_and_group_threads_share_one_trunk():
    first = source("a101", position=101)
    middle = source("a102", position=102, source_conversation_id="another-thread")
    last = source("a103", position=103)
    other = source("b", "group-b", 101)
    partial = projected([last, first, other])
    assert pairs(partial, "TRUNK_NEXT") == {("a101", "a103")}
    full = projected([first, middle, last, other])
    assert pairs(full, "TRUNK_NEXT") == {("a101", "a102"), ("a102", "a103")}
    assert pairs(full, "MEMBER_OF") == {
        ("a101", "group-a"),
        ("a102", "group-a"),
        ("a103", "group-a"),
        ("b", "group-b"),
    }
    assert len([n for n in full.nodes if n.ref.kind == "TRUNK_ROOT"]) == 2
    for order in itertools.permutations([first, middle, last, other]):
        assert projected(order + order).canonical_bytes() == full.canonical_bytes()


def test_random_reordering_duplicate_delivery_and_partition_merges_preserve_complete_graph():
    sources = [source("a"), source("b", "group-b"), source("c", position=2)]
    facts = [
        assertion("a-green", [SourceRef("a")]),
        assertion("b-black", [SourceRef("b")], "black"),
        assertion("c-green", [SourceRef("c")]),
    ]
    relations = [association("ab"), association("bc", left="b", right="c")]
    retracts = [Retraction("bot", "withdraw-c", "assertion", "c-green")]
    expected = projected(sources, facts, relations, retracts).canonical_bytes()
    operations = [*sources, *facts, *relations, *retracts]
    rng = random.Random(7941)
    for _ in range(80):
        delivery = operations + rng.choices(operations, k=15)
        rng.shuffle(delivery)
        partitions = [delivery[::3], delivery[1::3], delivery[2::3]]
        rng.shuffle(partitions)
        joined = list(itertools.chain.from_iterable(partitions))
        result = projected(
            [v for v in joined if isinstance(v, SourceUnit)],
            [v for v in joined if isinstance(v, Assertion)],
            [v for v in joined if isinstance(v, RelationContribution)],
            [v for v in joined if isinstance(v, Retraction)],
        )
        assert result.canonical_bytes() == expected


def test_conflicts_stay_parallel_and_same_value_keeps_every_independent_source():
    a, b, c = source(), source("b", "group-b"), source("c", position=2)
    green_a = assertion("green-a", [SourceRef("a")])
    green_b = assertion("green-b", [SourceRef("b")])
    black = assertion("black", [SourceRef("c")], "black")
    graph = projected([a, b, c], [green_a, black, green_b])
    assert len(graph.facts) == 1 and graph.facts[0].conflict
    green = next(
        value for value in graph.facts[0].candidates if value.value_digest == digest("green")
    )
    assert green.assertion_ids == ("green-a", "green-b")
    assert green.source_refs == (SourceRef("a"), SourceRef("b"))
    assert pairs(graph, "EVIDENCE") == {("green-a", "a"), ("green-b", "b"), ("black", "c")}
    assert pairs(graph, "SUBJECT_OF") == {
        ("person-1", "green-a"),
        ("person-1", "green-b"),
        ("person-1", "black"),
    }
    encoded = graph.canonical_bytes()
    assert b'"text":"green"' not in encoded and b'"text":"black"' not in encoded
    many = LogicalKey("person-1", "tea", cardinality="many")
    plural = projected(
        [a, c], [replace(green_a, logical_key=many), replace(black, logical_key=many)]
    )
    assert len(plural.facts[0].candidates) == 2 and not plural.facts[0].conflict
    fails(
        "GRAPH_KEY_CONTRACT_CONFLICT",
        lambda: projected([a, c], [green_a, replace(black, logical_key=many)]),
    )


def test_retract_before_assert_does_not_resurrect_or_suppress_an_unseen_candidate():
    withdrawn = Retraction("bot", "withdraw-a", "assertion", "old")
    assert not projected([source()], retractions=[withdrawn]).facts
    old = assertion("old", [SourceRef("a")])
    concurrent = assertion("concurrent", [SourceRef("a")], "black")
    graph = projected([source()], [concurrent, old], retractions=[withdrawn, withdrawn])
    assert graph.facts[0].candidates[0].assertion_ids == ("concurrent",)
    assert not graph.facts[0].conflict
    assert all(node.ref.identity != "old" for node in graph.nodes)


def test_symmetric_relation_identity_and_integer_evidence_aggregation_are_order_independent():
    a, b, c = source(), source("b", "group-b"), source("c", position=2)
    base = association("first", weight=3)
    reverse = replace(
        base, left=base.right, right=base.left, source_refs=tuple(reversed(base.source_refs))
    )
    assert reverse == base
    repeated_source = association("second", weight=5)
    extra_source = association(
        "third", refs=[SourceRef("c"), SourceRef("a"), SourceRef("b")], weight=7
    )
    graph = projected([a, b, c], relations=[extra_source, repeated_source, base, reverse])
    edge = edges(graph, "ASSOCIATES_WITH")[0]
    assert graph.aggregation_version == AGGREGATION_VERSION
    assert edge.weight_micros == 12 and edge.contribution_ids == ("first", "second", "third")
    assert edge.source_refs == (SourceRef("a"), SourceRef("b"), SourceRef("c"))
    for permutation in itertools.permutations([base, repeated_source, extra_source]):
        assert (
            projected([c, a, b], relations=permutation * 3).canonical_bytes()
            == graph.canonical_bytes()
        )
    retracted = Retraction("bot", "retract-second", "contribution", "second")
    assert (
        edges(
            projected(
                [a, b, c], relations=[base, repeated_source, extra_source], retractions=[retracted]
            ),
            "ASSOCIATES_WITH",
        )[0].weight_micros
        == 10
    )
    cancelled = Retraction("bot", "retract-edge", "relation", edge.edge_id)
    assert not edges(
        projected([a, b], relations=[base], retractions=[cancelled]), "ASSOCIATES_WITH"
    )


def test_unavailable_source_drops_its_entire_contribution_but_keeps_other_evidence():
    a, b, c = source(), source("b", "group-b"), source("c", position=2, available=False)
    visible = association("visible", weight=3)
    mixed = association("mixed", refs=[SourceRef("a"), SourceRef("b"), SourceRef("c")], weight=999)
    graph = projected([a, b, c], relations=[mixed, visible])
    only = edges(graph, "ASSOCIATES_WITH")[0]
    assert only.contribution_ids == ("visible",) and only.weight_micros == 3
    assert SourceRef("c") not in only.source_refs
    assert graph.canonical_bytes() == projected([a, b, c], relations=[visible]).canonical_bytes()


@pytest.mark.parametrize("state", ["missing", "unavailable", "ineligible", "deleted"])
def test_necessary_source_closure_hides_assertions_conflicts_and_relation_counts(state):
    a, b = source(), source("b", "group-b")
    public = assertion("public", [SourceRef("a")])
    hidden = assertion("private", [SourceRef("a"), SourceRef("b")], "black")
    link = RelationContribution(
        "bot",
        "link-op",
        "link",
        "ASSOCIATES_WITH",
        NodeRef("bot", "ENTITY", "person-1"),
        NodeRef("bot", "ASSERTION", "public"),
        (SourceRef("a"), SourceRef("b")),
    )
    sources = (
        [a]
        if state == "missing"
        else [
            a,
            replace(
                b,
                **(
                    {"available": False}
                    if state == "unavailable"
                    else {"eligible": False}
                    if state == "ineligible"
                    else {}
                ),
            ),
        ]
    )
    policy = Suppression("bot", source_unit_ids=("b",)) if state == "deleted" else None
    result = projected(sources, [public, hidden], [link], suppression=policy)
    assert (
        result.canonical_bytes()
        == projected(sources, [public], suppression=policy).canonical_bytes()
    )
    assert not result.facts[0].conflict and len(result.facts[0].candidates) == 1


def test_part_deletion_is_stable_across_revisions_and_distinguishes_whole_unit_dependencies():
    first = source()
    second = replace(
        first,
        source_revision=2,
        content_digest=digest("revision-two"),
        parts=(
            first.parts[0],
            replace(first.parts[1], part_revision=2, content_digest=digest("new-image")),
        ),
    )
    whole = assertion("whole", [SourceRef("a", 2)])
    text = assertion("text", [SourceRef("a", 2, "text")])
    image = assertion("image", [SourceRef("a", 2, "image")])
    deleted_image = Suppression("bot", parts=(("a", "image"),))
    graph = projected(
        [first, second], [whole, text, image], suppression=deleted_image, source_heads={"a": 2}
    )
    assert graph.facts[0].candidates[0].assertion_ids == ("text",)
    assert pairs(graph, "EVIDENCE") == {("text", "a")}
    assert next(node.source for node in graph.nodes if node.source).available_parts == ("text",)
    assert not projected(
        [first, second],
        [whole, text, image],
        suppression=Suppression("bot", parts=(("a", "*"),)),
        source_heads={"a": 2},
    ).nodes
    old = assertion("old-text", [SourceRef("a", 1, "text")])
    old_graph = projected([first, second], [old], suppression=deleted_image, source_heads={"a": 1})
    assert old_graph.facts[0].candidates[0].assertion_ids == ("old-text",)
    assert not projected([first, second], [old], source_heads={"a": 2}).facts


def test_partial_media_availability_keeps_precise_text_evidence_without_claiming_the_image():
    unit = source(available_parts=("text",))
    assertions = [
        assertion("whole", [SourceRef("a")]),
        assertion("text", [SourceRef("a", 1, "text")]),
        assertion("image", [SourceRef("a", 1, "image")]),
    ]
    graph = projected([unit], assertions)
    assert graph.facts[0].candidates[0].assertion_ids == ("text",)
    assert (
        graph.nodes[
            next(i for i, n in enumerate(graph.nodes) if n.ref.kind == "SOURCE_UNIT")
        ].source.parts
        == unit.parts
    )
    ready = projected([replace(unit, available_parts=("image", "text"))], assertions)
    assert ready.facts[0].candidates[0].assertion_ids == ("image", "text", "whole")


def test_deleting_middle_source_rebuilds_neighbors_without_reassigning_positions():
    units = [source("a", position=1), source("b", position=2), source("c", position=3)]
    graph = projected(units, suppression=Suppression("bot", source_unit_ids=("b",)))
    assert pairs(graph, "TRUNK_NEXT") == {("a", "c")}
    assert {(n.ref.identity, n.source.source_order_seq) for n in graph.nodes if n.source} == {
        ("a", 1),
        ("c", 3),
    }
    assert not projected(units, suppression=Suppression("bot", source_ids=("group-a",))).nodes
    assert not projected(units, suppression=Suppression("bot", parts=(("b", "*"),))).facts


def test_source_revisions_need_an_explicit_head_and_same_revision_payload_conflicts_even_when_deleted():
    old = source()
    new = replace(old, source_revision=2, content_digest=digest("new"))
    fails("GRAPH_SOURCE_HEAD_REQUIRED", lambda: projected([old, new]))
    chosen = projected([new, old], source_heads={"a": 1})
    assert chosen.canonical_bytes() == projected([old]).canonical_bytes()
    fails("GRAPH_SOURCE_HEAD_MISSING", lambda: projected([old], source_heads={"a": 2}))
    fails(
        "IDEMPOTENCY_CONFLICT",
        lambda: projected(
            [old, replace(old, content_digest=digest("other"))],
            suppression=Suppression("bot", source_unit_ids=("a",)),
        ),
    )
    fails(
        "GRAPH_SOURCE_POSITION_CONFLICT",
        lambda: projected([old, replace(new, source_order_seq=2)], source_heads={"a": 2}),
    )


def test_stable_operation_or_fact_identity_cannot_accept_another_payload():
    a = assertion("a", [SourceRef("a")])
    fails(
        "IDEMPOTENCY_CONFLICT",
        lambda: projected(
            [source()], [a, replace(a, value=ValueRef(digest("black"), "other-payload"))]
        ),
    )
    fails(
        "IDEMPOTENCY_CONFLICT",
        lambda: projected(
            [source()], [a], retractions=[Retraction("bot", a.operation_id, "assertion", "a")]
        ),
    )
    relation = association()
    fails(
        "IDEMPOTENCY_CONFLICT",
        lambda: projected(
            [source(), source("b", "group-b")],
            relations=[relation, replace(relation, weight_micros=2)],
        ),
    )


def test_positions_external_identity_and_bot_scope_never_guess_a_merge():
    a = source()
    fails("GRAPH_SOURCE_POSITION_CONFLICT", lambda: projected([a, source("b")]))
    fails(
        "GRAPH_SOURCE_IDENTITY_CONFLICT",
        lambda: projected([a, source("b", "another-id", external_group_id="group-a")]),
    )
    fails("GRAPH_BOT_MISMATCH", lambda: projected([a, source("b", bot="other")]))
    fails("GRAPH_BOT_MISMATCH", lambda: projected([a], suppression=Suppression("other")))
    fails(
        "GRAPH_BOT_MISMATCH",
        lambda: RelationContribution(
            "bot",
            "cross-op",
            "cross",
            "ASSOCIATES_WITH",
            NodeRef("bot", "SOURCE_UNIT", "a"),
            NodeRef("other", "SOURCE_UNIT", "b"),
            (SourceRef("a"), SourceRef("b")),
        ),
    )
    other = reduce_graph(
        "other",
        sources=[source(bot="other")],
        assertions=[assertion("fact", [SourceRef("a")], bot="other")],
    )
    ours = projected([a], [assertion("fact", [SourceRef("a")])])
    assert not {n.node_id for n in ours.nodes} & {n.node_id for n in other.nodes}
    assert not {e.edge_id for e in ours.edges} & {e.edge_id for e in other.edges}
    # Equal media bytes and equal subject labels in separate groups are separate
    # source units. Entity identity comes only from the explicit stable key.
    two_people = projected(
        [a, source("b", "group-b")],
        [
            assertion("one", [SourceRef("a")]),
            assertion("two", [SourceRef("b")], key=LogicalKey("different-person", "tea")),
        ],
    )
    assert len([n for n in two_people.nodes if n.ref.kind == "ENTITY"]) == 2


def test_non_group_sources_do_not_receive_a_fabricated_group_trunk():
    doc = source(source_kind="DOCUMENT", source_id="doc", group_source_id="", external_group_id="")
    graph = projected([doc], [assertion("document-claim", [SourceRef("a")])])
    assert not edges(graph, "MEMBER_OF") and not edges(graph, "TRUNK_NEXT")
    assert len(edges(graph, "EVIDENCE")) == 1


def test_relation_types_keep_direction_and_source_structure_cannot_be_retracted():
    left = NodeRef("bot", "ASSERTION", "fact")
    right = NodeRef("bot", "SOURCE_UNIT", "a")
    assert relation_id("bot", "EVIDENCE", left, right) != relation_id(
        "bot", "EVIDENCE", right, left
    )
    fails(
        "GRAPH_INVALID",
        lambda: RelationContribution(
            "bot", "bad", "bad", "EVIDENCE", right, left, (SourceRef("a"),)
        ),
    )
    graph = projected([source()])
    member = edges(graph, "MEMBER_OF")[0]
    fails(
        "GRAPH_STRUCTURAL_RETRACTION",
        lambda: projected(
            [source()], retractions=[Retraction("bot", "bad-structure", "relation", member.edge_id)]
        ),
    )
    assert node_id(left) != node_id(NodeRef("bot", "SOURCE_UNIT", "fact"))


def test_explicit_evidence_and_contradiction_keep_types_and_all_sources_after_retraction():
    units = [source(), source("b", "group-b")]
    facts = [assertion("a-fact", [SourceRef("a")]), assertion("b-fact", [SourceRef("b")], "black")]
    left, right = NodeRef("bot", "ASSERTION", "a-fact"), NodeRef("bot", "ASSERTION", "b-fact")
    contradictory = RelationContribution(
        "bot",
        "contradict",
        "contradict",
        "CONTRADICTS",
        left,
        right,
        (SourceRef("a"), SourceRef("b")),
    )
    associated = replace(
        contradictory, operation_id="associate", contribution_id="associate", kind="ASSOCIATES_WITH"
    )
    evidence = RelationContribution(
        "bot",
        "extra-evidence",
        "extra-evidence",
        "EVIDENCE",
        left,
        NodeRef("bot", "SOURCE_UNIT", "b"),
        (SourceRef("b"),),
    )
    graph = projected(units, facts, [evidence, associated, contradictory])
    assert len(edges(graph, "CONTRADICTS")) == len(edges(graph, "ASSOCIATES_WITH")) == 1
    assert edges(graph, "CONTRADICTS")[0].edge_id != edges(graph, "ASSOCIATES_WITH")[0].edge_id
    assert pairs(graph, "EVIDENCE") == {("a-fact", "a"), ("a-fact", "b"), ("b-fact", "b")}
    removed = projected(
        units,
        facts,
        [evidence, contradictory],
        [Retraction("bot", "retract-extra", "contribution", "extra-evidence")],
    )
    assert pairs(removed, "EVIDENCE") == {("a-fact", "a"), ("b-fact", "b")}
    deleted = projected(
        units,
        facts,
        [evidence, contradictory],
        suppression=Suppression("bot", source_unit_ids=("b",)),
    )
    assert not edges(deleted, "CONTRADICTS") and pairs(deleted, "EVIDENCE") == {("a-fact", "a")}


@pytest.mark.parametrize("weight", [True, 1.5, float("nan"), -1, 10**12 + 1])
def test_relation_weights_are_bounded_integer_fixed_point(weight):
    fails("GRAPH_INVALID", lambda: association(weight=weight))


def test_source_metadata_and_suppression_are_immutable_and_have_no_payload_fields():
    unit = source()
    with pytest.raises(AttributeError):
        unit.source_order_seq = 99
    with pytest.raises(TypeError):
        SourceUnit(**(unit.__dict__ | {"text": "must remain in payload storage"}))
    fails("GRAPH_VERSION_UNSUPPORTED", lambda: replace(unit, schema_version=2))
    fails("GRAPH_INVALID", lambda: replace(unit, source_order_seq=True))
    fails("GRAPH_INVALID", lambda: replace(unit, source_unit_id="x" * 257))
    fails("GRAPH_INVALID", lambda: replace(unit, available_parts=("not-a-part",)))
