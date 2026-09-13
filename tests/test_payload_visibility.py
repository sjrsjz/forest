"""Actual private/shared source authority at business-output publication."""

from uuid import uuid4

import pytest
from test_agent_runner import agent_env as agent_env
from test_agent_runner import begin, install_tool, proposed, query, resume
from test_memory_sources import drain
from test_messages import event, owner, receive
from test_messages import messages as messages

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.delivery import DeliveryStore
from forest.storage.payload_scope import guard_payload_scope_in
from forest.storage.payloads import PayloadStore
from forest.storage.source_domains import SourceDomainStore

pytestmark = pytest.mark.postgres


async def admitted(repository, lease, number, *, private=False, group="origin"):
    value = event(number, group=group, user="origin-user")
    if private:
        value["channel_kind"] = "private"
    receipt = await repository.ingest(lease, "binding-a", value, decision="NO_TRIGGER")
    await drain(repository, lease, receipt, operation="drain-" + str(number))
    content = await query(repository,
        "SELECT i.payload_id,o.source_unit_id FROM public.bot_message_inbox i "
        "JOIN public.bot_memory_source_occurrences o USING(bot_id,event_id) WHERE i.event_id=%s",
        (receipt["event_id"],))
    return receipt, content


async def check(repository, actor, ids, *, cut=None):
    async with repository.store.pool.connection() as conn, conn.transaction():
        binding, conversation = await repository._actor(conn, actor)
        await guard_payload_scope_in(
            conn, repository.store, conversation,
            conversation["context_applied_seq"] if cut is None else cut, ids,
        )
        await repository._actor_final(conn, actor, binding)


async def test_private_scope_shared_groups_current_cut_and_exact_reference_identity(messages, monkeypatch):
    repository, lease, _ = messages
    _, private = await admitted(repository, lease, 1, private=True)
    _, shared = await admitted(repository, lease, 2)
    current = await receive(repository, lease, 3, trigger=True, group="target")
    actor = await owner(repository, lease, current["conversation_id"])
    await repository.select_input(actor, "target-cut")
    current_body = await query(repository, "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s", (current["event_id"],))
    await check(repository, actor, [shared["payload_id"], current_body["payload_id"]])
    with pytest.raises(ForestError) as caught:
        await check(repository, actor, [private["payload_id"]])
    assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
    assert private["source_unit_id"] not in str(caught.value) and caught.value.details is None
    tail = await receive(repository, lease, 4, group="target")
    tail_body = await query(repository, "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s", (tail["event_id"],))
    with pytest.raises(ForestError):
        await check(repository, actor, [tail_body["payload_id"]], cut=1)
    payloads = PayloadStore(repository.store)
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        await payloads.lock_in(conn)
        domain = await SourceDomainStore(repository.store).source_in(conn, "SERVICE_INPUT", "test-domain")
    references = [
        (SourceRef(shared["source_unit_id"], 1, "0"), True),
        (SourceRef(shared["source_unit_id"], 2, "*"), False),
        (SourceRef(shared["source_unit_id"], 1, "does-not-exist"), False),
        (SourceRef("legacy:unknown"), False),
        (domain, True),
        (SourceRef(domain.source_unit_id, 2), False),
        (SourceRef(domain.source_unit_id, 1, "0"), False),
    ]
    for ref, allowed in references:
        body = await payloads.put_json(lease, owner_kind="kernel.result", owner_id=uuid4().hex,
            value="derived", sources=(ref,), expected_deletion_epoch=0)
        if allowed:
            await check(repository, actor, [body.payload_id])
        else:
            with pytest.raises(ForestError) as caught:
                await check(repository, actor, [body.payload_id])
            assert caught.value.code == "PAYLOAD_SCOPE_DENIED"

    async def no_body(*args, **kwargs):
        pytest.fail("Visibility authorization loaded source body into Python")

    monkeypatch.setattr(PayloadStore, "read_json_in", no_body)
    await check(repository, actor, [shared["payload_id"]])


@pytest.mark.parametrize("private,lost_ack", [(True, False), (True, True), (False, False)])
async def test_long_lived_worker_output_cannot_import_private_source_into_agent(
    agent_env, tmp_path, monkeypatch, private, lost_ack
):
    repository, lease, kernel, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    _, previous = await admitted(repository, lease, 1, private=private)
    # Real native state and generation exposure carry this earlier input into
    # the next call, whose own prompt belongs only to the target group.
    await kernel.invoke("example.counter", "counter.increment", {"increment": 7},
        operation_id="previous-input", input_parent_ids=(previous["payload_id"],))
    actor, selected, context = await begin(agent_env, group="target", number=2)
    client.responses = [proposed(selected["trigger_ids"][0])]
    if lost_ack:
        invoke = kernel.invoke_with_receipt

        async def lost(*args, **kwargs):
            await invoke(*args, **kwargs)
            raise OSError("lost local acknowledgement")

        monkeypatch.setattr(kernel, "invoke_with_receipt", lost)
    if private and not lost_ack:
        with pytest.raises(ForestError) as caught:
            await runner.advance(actor, selected, context)
        assert caught.value.code == "AGENT_TOOL_RECOVERY_REQUIRED"
        actor, selected, context = await resume(agent_env, actor)
        result = await runner.advance(actor, selected, context)
    else:
        result = await runner.advance(actor, selected, context)
    tool = await query(repository, "SELECT * FROM public.bot_agent_tools")
    call = await query(repository, "SELECT status,result_payload_id FROM public.bot_calls WHERE agent_tool_id=%s", (tool["operation_id"],))
    assert call["status"] == "COMPLETED" and call["result_payload_id"]
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_memory_config_heads"))["n"] == 0
    if private:
        assert result.status == "FAILED" and tool["status"] == "UNKNOWN"
        assert len(client.prepared) == 1
        assert (await query(repository, "SELECT count(*) AS n FROM public.bot_send_intents"))["n"] == 0
        if tool["result_payload_id"]:
            # A lost acknowledgement may have a safe UNKNOWN error observation,
            # never a result whose parents include the completed private body.
            assert await query(repository,
                "SELECT 1 FROM public.bot_payload_derivations WHERE payload_id=%s AND parent_payload_id=%s",
                (tool["result_payload_id"], call["result_payload_id"])) is None
    else:
        assert result.status == "YIELD" and tool["status"] == "SUCCEEDED"
        actor, selected, context = await resume(agent_env, actor)
        assert (await runner.advance(actor, selected, context)).status == "FINAL_READY"
        assert len(client.prepared) == 2 and b'8' in client.prepared[-1].body


async def test_non_agent_reply_checks_actual_worker_result_scope(messages):
    repository, lease, _ = messages
    _, private = await admitted(repository, lease, 1, private=True)
    trigger = await receive(repository, lease, 2, trigger=True, group="target")
    actor = await owner(repository, lease, trigger["conversation_id"])
    selected = await repository.select_input(actor, "cut")
    body = await PayloadStore(repository.store).put_json(
        lease, owner_kind="kernel.result", owner_id="template-output", value="private-derived",
        parents=(private["payload_id"],), expected_deletion_epoch=0,
    )
    with pytest.raises(ForestError) as caught:
        await repository.finalize_reply(actor, "reply", run_id=selected["run_id"],
            expected_input_revision=1, reply_parts=[[{"type": "text", "text": "private-derived"}]],
            delivery=DeliveryStore(repository.store), result_parent_ids=(body.payload_id,))
    assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_conversation_replies"))["n"] == 0
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_send_intents"))["n"] == 0


async def test_already_accepted_tool_sources_are_rechecked_without_rag_binding(agent_env, tmp_path):
    repository, lease, kernel, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    archived_event = {**event(1, group="target"), "thread_id": "archive"}
    archive = await repository.ingest(lease, "binding-a", archived_event, decision="NO_TRIGGER")
    await drain(repository, lease, archive)
    source = await query(repository, "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s", (archive["event_id"],))
    await kernel.invoke("example.counter", "counter.increment", {"increment": 7},
        operation_id="archive-input", input_parent_ids=(source["payload_id"],))
    actor, selected, context = await begin(agent_env, group="target", number=2)
    client.responses = [proposed(selected["trigger_ids"][0])]
    assert (await runner.advance(actor, selected, context)).status == "YIELD"
    assert (await query(repository, "SELECT status FROM public.bot_agent_tools"))["status"] == "SUCCEEDED"
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_agent_memory_bindings WHERE status='BOUND'"))["n"] == 0
    # The same proved event arrives in this conversation only after its frozen
    # cut. It now cannot be borrowed through the previously accepted tool body.
    tail = await repository.ingest(lease, "binding-a", {**archived_event, "thread_id": ""}, decision="NO_TRIGGER")
    assert tail["ingress_seq"] == 2
    actor, selected, context = await resume(agent_env, actor)
    assert (await runner.advance(actor, selected, context)).status == "FAILED"
    assert len(client.prepared) == 1
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_send_intents"))["n"] == 0


async def test_non_agent_reply_replay_keeps_original_cut_scope(messages):
    repository, lease, _ = messages
    original = {**event(1, group="target"), "thread_id": "archive"}
    archive = await repository.ingest(lease, "binding-a", original, decision="NO_TRIGGER")
    await drain(repository, lease, archive)
    parent = await query(repository, "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s", (archive["event_id"],))
    trigger = await receive(repository, lease, 2, trigger=True, group="target")
    actor = await owner(repository, lease, trigger["conversation_id"])
    selected = await repository.select_input(actor, "target-cut")
    output = await PayloadStore(repository.store).put_json(
        lease, owner_kind="kernel.result", owner_id="shared-template", value="allowed-shared",
        parents=(parent["payload_id"],), expected_deletion_epoch=0,
    )
    arguments = dict(run_id=selected["run_id"], expected_input_revision=1,
        reply_parts=[[{"type": "text", "text": "allowed-shared"}]],
        delivery=DeliveryStore(repository.store), result_parent_ids=(output.payload_id,))
    saved = await repository.finalize_reply(actor, "shared-reply", **arguments)
    registered = await query(repository,
        "SELECT u.* FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o "
        "USING(bot_id,source_unit_id) WHERE o.reply_id=%s", (saved["reply_id"],))
    assert registered["lineage_complete"]
    await check(repository, actor, [saved["payload_id"]], cut=1)
    assert (await repository.finalize_reply(actor, "shared-reply", **arguments))["reply_id"] == saved["reply_id"]
    assert await query(repository,
        "SELECT u.* FROM public.bot_memory_source_units u JOIN public.bot_memory_source_occurrences o "
        "USING(bot_id,source_unit_id) WHERE o.reply_id=%s", (saved["reply_id"],)) == registered
    await repository.ingest(lease, "binding-a", {**original, "thread_id": ""}, decision="NO_TRIGGER")
    with pytest.raises(ForestError) as caught:
        await repository.finalize_reply(actor, "shared-reply", **arguments)
    assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
    assert (await query(repository, "SELECT count(*) AS n FROM public.bot_send_intents"))["n"] == 1
