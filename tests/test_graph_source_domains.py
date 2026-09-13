"""Real graph selection preserves registered Host domains and conversation scope."""

import pytest
from test_agent_store import prepare, respond, start
from test_bot_store import activate
from test_kernel_content_authority import admit
from test_memory_graph_store import trunk_pairs
from test_memory_sources import drain, rows, source_read
from test_messages import event, receive
from test_messages import messages as messages
from test_source_domains import domain_payload

from forest.memory_types import SourceRef
from forest.storage.delivery import DeliveryStore
from forest.storage.memory_graph import GraphScope, GraphStore, GraphWindow
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def eligible(repository, unit_id):
    return (
        await rows(
            repository,
            "SELECT public.forest_graph_source_eligible(%s,%s) AS eligible",
            (repository.store.bot_id, unit_id),
        )
    )[0]["eligible"]


async def test_formal_kernel_derived_reply_is_a_real_graph_halo_and_private_scope_stays_private(
    messages,
    monkeypatch,
):
    repository, lease, _ = messages
    await activate(repository.store, lease)
    grant = await admit(repository.store, lease, "template", {"topic": "tree"})
    result = await repository.store.finish_call(grant, {"template": "tree answer"})
    state, actor, head = await start(messages)
    original = state.payloads.step_parents_in

    async def with_kernel_result(*args, **kwargs):
        return (*await original(*args, **kwargs), result["result_payload_id"])

    monkeypatch.setattr(state.payloads, "step_parents_in", with_kernel_result)
    _, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, text="template-derived graph reply")
    reply = await state.commit_final(
        actor, head["run_id"], "final", DeliveryStore(repository.store)
    )
    unit = (
        await rows(
            repository,
            "SELECT * FROM public.bot_memory_source_units WHERE source_event_id=%s",
            (MemorySourceStore.reply_event_id(reply["reply_id"]),),
        )
    )[0]
    domain_kinds = await rows(
        repository,
        "SELECT DISTINCT d.kind FROM public.bot_payload_sources p "
        "JOIN public.bot_payload_domains d USING(bot_id,source_unit_id) WHERE p.payload_id=%s",
        (unit["payload_id"],),
    )
    assert {row["kind"] for row in domain_kinds} == {
        "AGENT_SPEC",
        "CONTROL_INPUT",
        "GENERATION_CONFIG",
        "SERVICE_INPUT",
    }
    assert unit["lineage_complete"] and await eligible(repository, unit["source_unit_id"])

    next_message = await receive(repository, lease, "next-after-reply")
    await repository.select_input(actor, "drain-next")
    following = (
        await rows(
            repository,
            "SELECT u.* FROM public.bot_memory_source_occurrences o "
            "JOIN public.bot_memory_source_units u USING(bot_id,source_unit_id) WHERE o.event_id=%s",
            (next_message["event_id"],),
        )
    )[0]
    scope = GraphScope(
        (
            GraphWindow(
                unit["source_id"], following["source_order_seq"], following["source_order_seq"]
            ),
        )
    )
    graph = GraphStore(repository)
    snapshot = await graph.capture(lease, scope)
    assert snapshot.metadata["halos"][unit["source_id"]] == unit["source_unit_id"]
    await graph.publish(lease, graph.build(snapshot), "domain-halo")
    page = await graph.read(lease, scope.scope_id)
    assert trunk_pairs(page) == {(unit["source_unit_id"], following["source_unit_id"])}

    private = await repository.ingest(
        lease,
        "binding-a",
        {
            **event("private", text="PRIVATE-CANARY"),
            "channel_kind": "private",
            "channel_id": "friend-42",
        },
        decision="NO_TRIGGER",
    )
    await drain(repository, lease, private)
    private_unit = (
        await rows(
            repository,
            "SELECT u.* FROM public.bot_memory_source_occurrences o "
            "JOIN public.bot_memory_source_units u USING(bot_id,source_unit_id) WHERE o.event_id=%s",
            (private["event_id"],),
        )
    )[0]
    assert await eligible(repository, private_unit["source_unit_id"])
    sources = MemorySourceStore(repository)
    assert await source_read(
        sources, lease, private, 1, source_unit_ids=[private_unit["source_unit_id"]]
    )
    assert not await source_read(
        sources, lease, next_message, 2, source_unit_ids=[private_unit["source_unit_id"]]
    )
    # Domains are dependencies only; the graph never invents source nodes for them.
    assert not any(node["ref"]["identity"].startswith("domain:") for node in page["nodes"])
    config_source = (
        await rows(
            repository,
            "SELECT * FROM public.bot_payload_domains WHERE kind='GENERATION_CONFIG'",
        )
    )[0]["source_unit_id"]
    await sources.payloads.forget_sources(
        lease,
        "forget-config-domain",
        (SourceRef(config_source),),
        expected_deletion_epoch=0,
    )
    assert not await eligible(repository, unit["source_unit_id"])
    current = await graph.capture(lease, scope)
    assert current.metadata["halos"][unit["source_id"]] != unit["source_unit_id"]


@pytest.mark.parametrize("claim", ["unknown", "wrong_revision", "wrong_part"])
async def test_sql_does_not_accept_inexact_domain_claim_even_if_import_claims_complete(
    messages, claim
):
    repository, lease, _ = messages
    source, _ = await domain_payload(repository, lease)
    incoming = await receive(repository, lease, "original")
    await drain(repository, lease, incoming)
    original = (await rows(repository, "SELECT * FROM public.bot_memory_source_units"))[0]
    invalid = {
        "unknown": SourceRef("domain:sha256:" + "f" * 64),
        "wrong_revision": SourceRef(source.source_unit_id, 2),
        "wrong_part": SourceRef(source.source_unit_id, part_id="invented"),
    }[claim]
    payload = await PayloadStore(repository.store).put_json(
        lease,
        owner_kind="memory.source",
        owner_id="bad-import",
        value={
            "parts": [
                {"type": "text", "text": "unproven imported source"},
            ]
        },
        parents=(original["payload_id"],),
        sources=(invalid,),
        expected_deletion_epoch=0,
    )
    # A hostile or old importer can claim lineage_complete in metadata. Keep
    # existing registered input dependencies; only the extra domain claim is bad.
    # The SQL prefilter must independently reject it before graph selection.
    async with repository.store.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO public.bot_memory_source_units(bot_id,source_unit_id,source_id,source_event_id,"
            "source_revision,source_order_seq,order_origin,kind,principal_ref,identity_origin,"
            "payload_id,content_digest,lineage_complete,occurred_at) "
            "SELECT bot_id,'bad-import-unit',source_id,'bad-import-event',1,source_order_seq+1,"
            "order_origin,kind,principal_ref,identity_origin,%s,%s,true,occurred_at "
            "FROM public.bot_memory_source_units WHERE source_unit_id=%s",
            (payload.payload_id, payload.digest, original["source_unit_id"]),
        )
    assert await eligible(repository, original["source_unit_id"])
    assert not await eligible(repository, "bad-import-unit")
