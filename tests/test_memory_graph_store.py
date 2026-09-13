"""Actual PostgreSQL graph publication, source fences and immutable adjacency."""

import asyncio
import json
from dataclasses import replace

import psycopg
import pytest
from test_jobs import configure as configure_job
from test_jobs import enqueue, manual
from test_jobs import environment as environment
from test_jobs import jobs as jobs
from test_media import media_server, picture, respond
from test_memory_media import configure as configure_media
from test_memory_media import media as media
from test_memory_sources import drain, rows
from test_messages import event, outgoing_chat, owner, receive
from test_messages import messages as messages

from forest.errors import ForestError
from forest.memory_graph import (
    Assertion,
    LogicalKey,
    NodeRef,
    RelationContribution,
    Retraction,
    ValueRef,
)
from forest.memory_types import SourceRef
from forest.storage.memory_graph import GraphCandidate, GraphScope, GraphStore, GraphWindow, build
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def units(repository):
    return await rows(
        repository,
        "SELECT * FROM public.bot_memory_source_units ORDER BY source_id,source_order_seq",
    )


async def scope_for(repository):
    return GraphScope(
        tuple(
            GraphWindow(row["source_id"], 1, 16)
            for row in await rows(
                repository, "SELECT source_id FROM public.bot_memory_sources ORDER BY source_id"
            )
        )
    )


async def publish(graph, lease, scope, operation):
    candidate = build(await graph.capture(lease, scope))
    return candidate, await graph.publish(lease, candidate, operation)


def trunk_pairs(page):
    nodes = {n["node_id"]: n["ref"]["identity"] for n in page["nodes"]}
    return {
        (nodes[e["from_node_id"]], nodes[e["to_node_id"]])
        for e in page["edges"]
        if e["kind"] == "TRUNK_NEXT"
    }


async def assertion(repository, lease, identity, evidence, value="private-assertion-body-canary"):
    refs = tuple(SourceRef(r["source_unit_id"]) for r in evidence)
    payload = await PayloadStore(repository.store).put_json(
        lease,
        owner_kind="memory.assertion",
        owner_id=identity,
        value=value,
        sources=refs,
        expected_deletion_epoch=0,
    )
    return Assertion(
        "bot-a",
        "operation:" + identity,
        identity,
        LogicalKey("stable-user", "favorite-color"),
        ValueRef(payload.digest, payload.payload_id),
        refs,
    )


async def test_graph_only_admits_actual_batches_and_pins_global_revision(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    scope = await scope_for(repository)
    empty, zero = await publish(graph, lease, scope, "empty")
    assert json.loads(empty.canonical_projection)["nodes"] == []
    await drain(repository, lease, first)
    _, current = await publish(graph, lease, scope, "qualified")
    assert current["graph_revision"] == zero["graph_revision"] + 1
    page = await graph.read(lease, scope.scope_id, graph_revision=zero["graph_revision"])
    assert page["nodes"] == [] and page["edges"] == []
    page = await graph.read(lease, scope.scope_id, graph_revision=current["graph_revision"])
    assert {n["ref"]["kind"] for n in page["nodes"]} == {"TRUNK_ROOT", "SOURCE_UNIT"}
    assert page["requires_conversation_authorization"]
    assert len(page["required_source_refs"]) == 1
    coverage = await graph.coverage(lease, graph_revision=zero["graph_revision"])
    assert coverage["scopes"][0]["revision"] == 1
    assert "message 1" not in str(page)


async def test_late_eligibility_rebuilds_neighbor_edges_and_fences_old_candidate(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    middle = await repository.ingest(
        lease, "binding-a", {**event(2), "thread_id": "lagged"}, decision="NO_TRIGGER"
    )
    await receive(repository, lease, 3)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    _, initial = await publish(graph, lease, scope, "initial")
    all_units = await units(repository)
    assert trunk_pairs(await graph.read(lease, scope.scope_id)) == {
        (all_units[0]["source_unit_id"], all_units[2]["source_unit_id"])
    }
    stale = build(await graph.capture(lease, scope))
    await drain(repository, lease, middle)
    with pytest.raises(ForestError) as caught:
        await graph.publish(lease, stale, "late-stale")
    assert caught.value.code == "GRAPH_SOURCE_CHANGED"
    await publish(graph, lease, scope, "late-current")
    assert trunk_pairs(await graph.read(lease, scope.scope_id)) == {
        (all_units[0]["source_unit_id"], all_units[1]["source_unit_id"]),
        (all_units[1]["source_unit_id"], all_units[2]["source_unit_id"]),
    }
    # An explicitly pinned historical version retains its original coverage.
    assert (
        len(
            trunk_pairs(
                await graph.read(lease, scope.scope_id, graph_revision=initial["graph_revision"])
            )
        )
        == 1
    )


async def test_halo_connects_physical_shards_and_global_coverage_pagination(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    for number in (2, 3):
        await receive(repository, lease, number)
    await drain(repository, lease, first)
    source_units = await units(repository)
    source_id = source_units[0]["source_id"]
    left = GraphScope((GraphWindow(source_id, 1, 1),))
    right = GraphScope((GraphWindow(source_id, 2, 3),))
    await publish(graph, lease, left, "left")
    candidate, receipt = await publish(graph, lease, right, "right")
    assert candidate.snapshot.metadata["halos"][source_id] == source_units[0]["source_unit_id"]
    assert len(trunk_pairs(await graph.read(lease, right.scope_id))) == 2
    page = await graph.coverage(lease, limit=1)
    assert page["next_after"]
    continued = await graph.coverage(
        lease, graph_revision=page["graph_revision"], after_scope_id=page["next_after"], limit=1
    )
    assert len(continued["scopes"]) == 1 and continued["next_after"] is None
    assert {page["scopes"][0]["scope_id"], continued["scopes"][0]["scope_id"]} == {
        left.scope_id,
        right.scope_id,
    }
    assert continued["graph_revision"] == receipt["graph_revision"]
    first_page = await graph.read(
        lease, right.scope_id, graph_revision=receipt["graph_revision"], limit=1
    )
    second_page = await graph.read(
        lease,
        right.scope_id,
        graph_revision=first_page["graph_revision"],
        after=first_page["next_after"],
        limit=1,
    )
    assert first_page["edges"][0]["edge_id"] != second_page["edges"][0]["edge_id"]


async def test_cross_trunk_evidence_conflicts_and_values_stay_in_payload_store(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    second = await receive(repository, lease, 2, group="group-2")
    await drain(repository, lease, first)
    await drain(repository, lease, second)
    evidence = await units(repository)
    scope = await scope_for(repository)
    a = await assertion(repository, lease, "a", (evidence[0],))
    b = await assertion(repository, lease, "b", (evidence[1],), value="another-secret-body")
    link = RelationContribution(
        "bot-a",
        "association",
        "association",
        "ASSOCIATES_WITH",
        NodeRef("bot-a", "SOURCE_UNIT", evidence[0]["source_unit_id"]),
        NodeRef("bot-a", "SOURCE_UNIT", evidence[1]["source_unit_id"]),
        tuple(SourceRef(r["source_unit_id"]) for r in evidence),
    )
    for command in (b, link, a):
        await graph.append(lease, scope, command, expected_deletion_epoch=0)
    candidate, _ = await publish(graph, lease, scope, "with-facts")
    projection = json.loads(candidate.canonical_projection)
    assert projection["facts"][0]["conflict"] and len(projection["facts"][0]["candidates"]) == 2
    associations = [e for e in projection["edges"] if e["kind"] == "ASSOCIATES_WITH"]
    assert len(associations) == 1 and len(associations[0]["source_refs"]) == 2
    assert len([n for n in projection["nodes"] if n["ref"]["kind"] == "TRUNK_ROOT"]) == 2
    immutable = await rows(repository, "SELECT metadata FROM public.bot_memory_graph_operations")
    assert "secret-body" not in str(immutable) and "private-assertion-body-canary" not in str(
        immutable
    )
    assert "private-assertion-body-canary" not in candidate.canonical_projection.decode()
    await PayloadStore(repository.store).forget_sources(
        lease, "forget", (SourceRef(evidence[0]["source_unit_id"]),), expected_deletion_epoch=0
    )
    with pytest.raises(ForestError) as caught:
        await graph.read(lease, scope.scope_id)
    assert caught.value.code == "GRAPH_SOURCE_CHANGED"
    renewed, _ = await publish(graph, lease, scope, "after-delete")
    final = json.loads(renewed.canonical_projection)
    assert not final["facts"][0]["conflict"] and len(final["facts"][0]["candidates"]) == 1
    assert not [e for e in final["edges"] if e["kind"] == "ASSOCIATES_WITH"]


async def test_retraction_before_assertion_duplicate_and_identity_conflict(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    assertion_a = await assertion(repository, lease, "a", await units(repository))
    retraction = Retraction("bot-a", "withdraw-a", "assertion", "a")
    await graph.append(lease, scope, retraction, expected_deletion_epoch=0)
    results = await asyncio.gather(
        *(graph.append(lease, scope, assertion_a, expected_deletion_epoch=0) for _ in range(2))
    )
    assert sorted(r["duplicate"] for r in results) == [False, True]
    candidate, _ = await publish(graph, lease, scope, "withdrawn-first")
    assert json.loads(candidate.canonical_projection)["facts"] == []
    changed = replace(assertion_a, logical_key=LogicalKey("different-subject", "favorite-color"))
    with pytest.raises(ForestError) as caught:
        await graph.append(lease, scope, changed, expected_deletion_epoch=0)
    assert caught.value.code == "IDEMPOTENCY_CONFLICT"
    await graph.append(
        lease,
        scope,
        Retraction("bot-a", "new-withdraw", "assertion", "other"),
        expected_deletion_epoch=0,
    )
    with pytest.raises(ForestError) as caught:
        await graph.read(lease, scope.scope_id)
    assert caught.value.code == "GRAPH_STALE"


async def test_two_workers_publish_cas_and_exact_retry_survives_restart(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    candidate = build(await graph.capture(lease, scope))
    results = await asyncio.gather(
        *(graph.publish(lease, candidate, operation) for operation in ("worker-a", "worker-b")),
        return_exceptions=True,
    )
    assert len([r for r in results if isinstance(r, dict)]) == 1
    loser = next(r for r in results if isinstance(r, ForestError))
    assert loser.code == "GRAPH_HEAD_CONFLICT"
    winner = "worker-a" if isinstance(results[0], dict) else "worker-b"
    replay = await GraphStore(repository).publish(lease, candidate, winner)
    assert replay["duplicate"] and replay["graph_revision"] == 1
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_graph_publications")) == 1
    with pytest.raises(ForestError) as caught:
        await graph.publish(lease, GraphCandidate(candidate.snapshot, b"{}"), "forged")
    assert caught.value.code == "GRAPH_CANDIDATE_INVALID"


async def test_echo_or_restricted_source_change_without_clock_bump_blocks_publish(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    candidate = build(await graph.capture(lease, scope))
    old_clock = await rows(repository, "SELECT * FROM public.bot_memory_clock")
    async with repository.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_message_inbox SET context_disposition='RESTRICTED' WHERE event_id=%s",
            (first["event_id"],),
        )
    assert await rows(repository, "SELECT * FROM public.bot_memory_clock") == old_clock
    with pytest.raises(ForestError) as caught:
        await graph.publish(lease, candidate, "restricted")
    assert caught.value.code == "GRAPH_SOURCE_CHANGED"
    assert not await rows(repository, "SELECT * FROM public.bot_memory_graph_publications")


async def test_database_immutability_and_expired_final_host_roll_back_entire_publication(
    messages, monkeypatch
):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    candidate = build(await graph.capture(lease, scope))
    original = repository.store._host
    count = 0

    async def expire(conn, *args, **kwargs):
        nonlocal count
        count += 1
        if count == 2:
            await conn.execute(
                "UPDATE public.bot_host_lease SET expires_at=clock_timestamp()-interval '1 second'"
            )
        return await original(conn, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(repository.store, "_host", expire)
        with pytest.raises(ForestError) as caught:
            await graph.publish(lease, candidate, "expires")
        assert caught.value.code == "STALE_HOST"
    for table in (
        "bot_memory_graph_publications",
        "bot_memory_graph_nodes",
        "bot_memory_graph_edges",
        "bot_memory_graph_clock",
    ):
        assert not await rows(repository, "SELECT * FROM public." + table)
    await graph.publish(lease, candidate, "works")
    async with repository.store.pool.connection() as conn:
        for statement in (
            "UPDATE public.bot_memory_graph_publications SET projection_digest='changed'",
            "DELETE FROM public.bot_memory_graph_edges",
            "UPDATE public.bot_memory_graph_nodes SET metadata='{}'",
        ):
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                await conn.execute(statement)


async def test_scope_limits_and_other_bot_commands_fail_closed(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    for start, end in ((0, 1), (1, 257)):
        with pytest.raises(ForestError):
            GraphWindow(scope.windows[0].source_id, start, end)
    with pytest.raises(ForestError):
        GraphScope((scope.windows[0], scope.windows[0]))
    with pytest.raises(ForestError) as caught:
        await graph.append(
            lease, scope, Retraction("other-bot", "op", "assertion", "a"), expected_deletion_epoch=0
        )
    assert caught.value.code == "GRAPH_BOT_MISMATCH"
    discovery = await graph.discover(lease, limit=1)
    assert discovery["scopes"][0].windows[0] == GraphWindow(scope.windows[0].source_id, 1, 256)
    assert discovery["next_after"] is None


async def test_refresh_is_noop_across_restart_and_unrelated_source_activity(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    initial = await graph.refresh(lease, scope)
    for _ in range(3):
        result = await GraphStore(repository).refresh(lease, scope)
        assert result["unchanged"] and result["graph_revision"] == initial["graph_revision"]
    second = await receive(repository, lease, 2, group="unrelated-group")
    await drain(repository, lease, second)
    assert (await graph.refresh(lease, scope))["unchanged"]
    assert len(await rows(repository, "SELECT * FROM public.bot_memory_graph_publications")) == 1


@pytest.mark.parametrize("field", ["maintenance_epoch", "storage_generation"])
async def test_restore_active_does_not_revalidate_old_runtime_epoch(messages, field):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    scope = await scope_for(repository)
    candidate = build(await graph.capture(lease, scope))
    async with repository.store.pool.connection() as conn:
        # The runtime remains ACTIVE under the same Host; only the prior
        # maintenance/storage decision invalidates this old derived candidate.
        await conn.execute("UPDATE public.bot_runtime_state SET " + field + "=" + field + "+1")
    with pytest.raises(ForestError) as caught:
        await graph.publish(lease, candidate, "stale-epoch")
    assert caught.value.code == "GRAPH_SOURCE_CHANGED"


async def test_real_late_echo_receipt_advances_facts_and_invalidates_candidate(messages):
    repository, lease, _ = messages
    actor, delivery, permit, _ = await outgoing_chat(repository, lease)
    echo = await receive(repository, lease, 777, group="42", user="1000", text="exact output")
    await repository.select_input(actor, "drain-before-receipt")
    graph, scope = GraphStore(repository), await scope_for(repository)
    candidate = build(await graph.capture(lease, scope))
    echo_source = (await rows(repository,
        "SELECT source_unit_id FROM public.bot_memory_source_occurrences WHERE event_id=%s",
        (echo["event_id"],),
    ))[0]["source_unit_id"]
    before_sources = {item["source_unit_id"] for item in candidate.snapshot.metadata["sources"]}
    assert echo_source in before_sources
    old_clock = await rows(repository, "SELECT * FROM public.bot_memory_clock")
    await delivery.complete(permit, "SENT", {"message_id": "777"})
    assert await repository.reconcile_echoes(lease) == 1
    # 035 records the real alias as a new source dependency. This advances the
    # fact revision once, without pretending that confirmation is a deletion.
    expected_clock = [{**old_clock[0], "fact_revision": old_clock[0]["fact_revision"] + 1}]
    assert await rows(repository, "SELECT * FROM public.bot_memory_clock") == expected_clock
    assert await repository.reconcile_echoes(lease) == 0
    assert await rows(repository, "SELECT * FROM public.bot_memory_clock") == expected_clock
    with pytest.raises(ForestError) as caught:
        await graph.publish(lease, candidate, "echo-invalid")
    assert caught.value.code == "GRAPH_SOURCE_CHANGED"
    current, _ = await publish(graph, lease, scope, "echo-current")
    assert {item["source_unit_id"] for item in current.snapshot.metadata["sources"]} == before_sources - {echo_source}


async def test_real_job_run_cancellation_rechecks_original_source_state(jobs):
    env, job_store, conversation = jobs
    await configure_job(env, job_store, conversation)
    await manual(env, job_store)
    await enqueue(env, job_store)
    repository = job_store.messages
    actor = await owner(repository, env.lease, conversation)
    batch = await repository.select_input(actor, "job-input")
    graph, scope = GraphStore(repository), await scope_for(repository)
    candidate = build(await graph.capture(env.lease, scope))
    assert any(s["source_kind"] == "SYSTEM" for s in candidate.snapshot.metadata["sources"])
    old_clock = await rows(repository, "SELECT * FROM public.bot_memory_clock")
    await repository.end_run(actor, run_id=batch["run_id"], reason="cancelled", status="CANCELLED")
    assert await rows(repository, "SELECT * FROM public.bot_memory_clock") == old_clock
    with pytest.raises(ForestError) as caught:
        await graph.publish(env.lease, candidate, "job-cancelled")
    assert caught.value.code == "GRAPH_SOURCE_CHANGED"


async def test_managed_original_image_changes_availability_and_qualified_halo(media):
    env, media_store, _ = media
    repository = media_store.messages
    image = picture()

    async def handler(writer, header):
        await respond(writer, image)

    async with media_server(handler) as (origin, _):
        await configure_media(env, [origin])
        first = await receive(repository, env.lease, 1)
        image_event = event(2)
        image_event["segments"] = [
            {"type": "image", "media_ref": {"kind": "onebot-source", "uri": origin + "/image"}}
        ]
        await repository.ingest(env.lease, "binding-a", image_event, decision="NO_TRIGGER")
        await receive(repository, env.lease, 3)
        await drain(repository, env.lease, first)
        source_units = await units(repository)
        scope = GraphScope((GraphWindow(source_units[0]["source_id"], 3, 3),))
        graph = GraphStore(repository)
        pending = build(await graph.capture(env.lease, scope))
        assert (
            pending.snapshot.metadata["halos"][source_units[0]["source_id"]]
            == source_units[0]["source_unit_id"]
        )
        media_candidate = (await media_store.scan(env.lease))[0]
        result = await media_store.capture_once(env.lease, media_candidate["capture_id"])
        assert result["status"] == "CAPTURED"
        with pytest.raises(ForestError) as caught:
            await graph.publish(env.lease, pending, "missing-image-stale")
        assert caught.value.code == "GRAPH_SOURCE_CHANGED"
        current, _ = await publish(graph, env.lease, scope, "with-original")
        assert (
            current.snapshot.metadata["halos"][source_units[0]["source_id"]]
            == source_units[1]["source_unit_id"]
        )
        image_unit = next(
            s
            for s in current.snapshot.metadata["sources"]
            if s["source_unit_id"] == source_units[1]["source_unit_id"]
        )
        assert image_unit["available_parts"] == ["0"] and image_unit["parts"][0]["kind"] == "image"
        assert origin not in current.snapshot.metadata_json


async def test_discovery_and_halos_cover_more_than_512_sources_in_bounded_pages(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    for number in range(2, 514):
        await receive(repository, lease, number)
    actor, _ = await drain(repository, lease, first)
    await repository.select_input(actor, "drain-page-2")
    await repository.select_input(actor, "drain-page-3")
    cursor, scopes, global_revision = {}, [], 0
    while True:
        page = await graph.discover(lease, **cursor, limit=1)
        scopes.extend(page["scopes"])
        if page["next_after"] is None:
            break
        cursor = page["next_after"]
    assert len(scopes) == 3
    source_ids, roots, pair_count = set(), set(), 0
    for index, scope in enumerate(scopes):
        candidate, receipt = await publish(graph, lease, scope, "large:" + str(index))
        assert len(candidate.snapshot.metadata["sources"]) <= 257
        projection = json.loads(candidate.canonical_projection)
        source_ids.update(
            n["ref"]["identity"] for n in projection["nodes"] if n["ref"]["kind"] == "SOURCE_UNIT"
        )
        roots.update(n["node_id"] for n in projection["nodes"] if n["ref"]["kind"] == "TRUNK_ROOT")
        pair_count += sum(e["kind"] == "TRUNK_NEXT" for e in projection["edges"])
        global_revision = receipt["graph_revision"]
    assert len(source_ids) == 513 and len(roots) == 1 and pair_count == 512
    assert len((await graph.coverage(lease, graph_revision=global_revision))["scopes"]) == 3


async def test_cross_partition_retractions_apply_before_add_and_invalidate_old_heads(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    base = await scope_for(repository)
    facts = GraphScope(base.windows, "facts-0001")
    commands = GraphScope(base.windows, "withdrawals-0001")
    a = await assertion(repository, lease, "a", await units(repository))
    await graph.append(
        lease,
        commands,
        Retraction("bot-a", "retract-before", "assertion", "a"),
        expected_deletion_epoch=0,
    )
    await graph.append(lease, facts, a, expected_deletion_epoch=0)
    candidate, _ = await publish(graph, lease, facts, "already-withdrawn")
    assert not json.loads(candidate.canonical_projection)["facts"]
    b = await assertion(repository, lease, "b", await units(repository), value="different")
    await graph.append(lease, facts, b, expected_deletion_epoch=0)
    await graph.refresh(lease, facts)
    await graph.append(
        lease,
        commands,
        Retraction("bot-a", "retract-after", "assertion", "b"),
        expected_deletion_epoch=0,
    )
    with pytest.raises(ForestError) as caught:
        await graph.read(lease, facts.scope_id)
    assert caught.value.code == "GRAPH_STALE"
    final = await graph.refresh(lease, facts)
    assert not final.get("unchanged")
    stored = (
        await rows(
            repository,
            "SELECT canonical_projection FROM public.bot_memory_graph_publications WHERE scope_id=%s ORDER BY revision DESC LIMIT 1",
            (facts.scope_id,),
        )
    )[0]
    assert not json.loads(stored["canonical_projection"])["facts"]


async def test_invalid_evidence_revision_does_not_become_an_immutable_fact(messages):
    repository, lease, _ = messages
    graph = GraphStore(repository)
    first = await receive(repository, lease, 1)
    await drain(repository, lease, first)
    source = (await units(repository))[0]
    scope = await scope_for(repository)
    for reference in (
        SourceRef(source["source_unit_id"], 2),
        SourceRef(source["source_unit_id"], 1, "absent"),
    ):
        payload = await PayloadStore(repository.store).put_json(
            lease,
            owner_kind="memory.assertion",
            owner_id=reference.part_id,
            value="v",
            sources=(reference,),
            expected_deletion_epoch=0,
        )
        command = Assertion(
            "bot-a",
            "invalid:" + reference.part_id,
            reference.part_id,
            LogicalKey("subject", "predicate"),
            ValueRef(payload.digest, payload.payload_id),
            (reference,),
        )
        with pytest.raises(ForestError) as caught:
            await graph.append(lease, scope, command, expected_deletion_epoch=0)
        assert caught.value.code == "GRAPH_SOURCE_REFERENCE"
    assert not await rows(repository, "SELECT * FROM public.bot_memory_graph_operations")
