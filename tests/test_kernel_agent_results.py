"""Kernel results remain actual parents across Agent tool observation recovery."""

import pytest
from test_agent_runner import agent_env as agent_env
from test_agent_runner import begin, install_tool, proposed, query, resume
from test_messages import messages as messages

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("handoff", [False, True])
async def test_tool_result_inherits_kernel_body_after_normal_return_or_actor_handoff(
    agent_env, tmp_path, monkeypatch, handoff
):
    repository, lease, kernel, runner, client, _, _ = agent_env
    await install_tool(agent_env, tmp_path)
    actor, selected, context = await begin(agent_env)
    client.responses = [proposed(selected["trigger_ids"][0], increment=7)]
    invoke, recover = kernel.invoke_with_receipt, runner.state.recover_tool
    calls = []

    async def lose_return(*args, **kwargs):
        calls.append(kwargs["operation_id"])
        receipt = await invoke(*args, **kwargs)
        if handoff:
            raise OSError("lost local acknowledgement")
        return receipt

    async def defer_recovery(actor, run_id, tool_call_id):
        return await runner.state.load(actor, run_id)

    monkeypatch.setattr(kernel, "invoke_with_receipt", lose_return)
    if handoff:
        monkeypatch.setattr(runner.state, "recover_tool", defer_recovery)
    disposition = await runner.advance(actor, selected, context)
    if handoff:
        assert disposition.status == "BLOCKED_UNKNOWN"
        old_actor = actor
        actor, selected, context = await resume(agent_env, actor)
        assert actor.actor_epoch > old_actor.actor_epoch
        monkeypatch.setattr(runner.state, "recover_tool", recover)
        disposition = await runner.advance(actor, selected, context)
    assert disposition.status == "YIELD" and len(calls) == 1
    saved = await query(repository,
        "SELECT t.result AS tool_body,t.result_payload_id AS tool_payload,"
        "c.result AS call_body,c.result_payload_id AS call_payload,c.call_id "
        "FROM public.bot_agent_tools t JOIN public.bot_calls c ON c.bot_id=t.bot_id AND c.agent_tool_id=t.operation_id")
    assert saved["tool_body"] is None and saved["call_body"] is None
    parent = await query(repository,
        "SELECT 1 AS found FROM public.bot_payload_derivations WHERE payload_id=%s AND parent_payload_id=%s",
        (saved["tool_payload"], saved["call_payload"]))
    assert parent == {"found": 1}
    # Forget only this concrete service input domain, independent from the
    # original user source. The result -> Agent chain must still be removed.
    domain = await query(repository,
        "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='SERVICE_INPUT' AND owner_id=%s",
        (saved["call_id"],))
    payloads = PayloadStore(repository.store)
    await payloads.forget_sources(lease, "forget-tool-domain", (SourceRef(domain["source_unit_id"]),), expected_deletion_epoch=0)
    for payload_id in (saved["tool_payload"], saved["call_payload"]):
        with pytest.raises(ForestError):
            await payloads.read_json(lease, payload_id, expected_deletion_epoch=1)
    await payloads.scrub(lease, limit=256)
    assert (await query(repository,
        "SELECT count(*) AS n FROM public.bot_payload_bodies WHERE payload_id=ANY(%s)",
        ([saved["tool_payload"], saved["call_payload"]],)))["n"] == 0
    assert (await query(repository, "SELECT status FROM public.bot_calls WHERE call_id=%s", (saved["call_id"],)))["status"] == "COMPLETED"
