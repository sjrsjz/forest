"""Real control admissions share Agent operations while preserving group scope."""

import asyncio

import pytest
from test_agent_store import prepare, start
from test_group_scheduler import row, until
from test_messages import event, receive
from test_messages import messages as messages
from test_shell import allow, completed, dispatcher, shell_record

from forest.errors import ForestError
from forest.storage.agent import AgentStore
from forest.storage.delivery_payloads import DeliveryContentStore
from forest.storage.input_payloads import InputContentStore
from forest.storage.memory_administration import MemoryAdministration
from forest.storage.shell_control import ShellControl

pytestmark = pytest.mark.postgres


async def claim_command(repository, lease, text, number="control", group="group-1"):
    incoming = await receive(repository, lease, number, text=text, group=group)
    return await repository.claim_control(lease, incoming["command_id"], "shell-test", ttl_seconds=30)


async def test_shell_cancel_inflight_model_keeps_cost_and_replays_same_operation(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    state, actor, head = await start(messages)
    _, prepared, permit = await prepare(state, actor, head)
    await state.check_model_permit(actor, permit, prepared)
    head = await state.load(actor, head["run_id"])
    claim = await claim_command(repository, lease,
        f'#forest run cancel {head["run_id"]} --expect-checkpoint {head["checkpoint_seq"]} --reason "停止回答"')
    controls = dispatcher(repository, lease)
    first = await controls._typed(lease, claim)
    second = await controls._typed(lease, claim)
    assert first == second and first[1]["phase"] == "CANCELLED"
    current = await row(repository, "SELECT phase FROM public.bot_agent_runs")
    assert current["phase"] == "CANCELLED"
    attempt = await row(repository, "SELECT status FROM public.bot_model_transport_attempts")
    assert attempt["status"] == "UNKNOWN"
    budget = await row(repository, "SELECT reserved_tokens FROM public.bot_agent_budget_days")
    assert budget["reserved_tokens"] > 0
    assert (await row(repository, "SELECT count(*) AS n FROM public.bot_web_operations WHERE action='agent.cancel'"))["n"] == 1
    assert (await row(repository, "SELECT count(*) AS n FROM public.bot_conversation_replies"))["n"] == 0
    source = await row(repository, "SELECT segments FROM public.bot_message_inbox WHERE event_id=%s", (claim["event_id"],))
    assert source["segments"] is None
    async with repository.store.pool.connection() as conn, conn.transaction():
        content = InputContentStore(repository.store)
        clock = await content.payloads.lock_in(conn)
        source = await content.read_input_in(conn, claim["event_id"],
            expected_deletion_epoch=clock["deletion_epoch"], required=True)
    assert "停止回答" not in str(source["segments"])


async def test_shell_cannot_cancel_other_group_or_change_admitted_payload(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    state, actor, head = await start(messages, group="private-group")
    claim = await claim_command(repository, lease,
        f'#forest run cancel {head["run_id"]} --expect-checkpoint {head["checkpoint_seq"]} --reason test')
    controls = dispatcher(repository, lease)
    with pytest.raises(ForestError) as denied:
        await controls._typed(lease, claim)
    assert denied.value.code == "SHELL_SCOPE_DENIED"
    assert (await state.load(actor, head["run_id"]))["phase"] == head["phase"]
    guard = ShellControl(repository, lease, claim)
    changed = {**claim["arguments"], "reason": "different"}
    target = AgentStore(repository)
    target.control = guard
    with pytest.raises(ForestError) as altered:
        await target.cancel(guard.admission, "shell-action:" + claim["command_id"], changed)
    assert altered.value.code == "SHELL_AUTHORITY_REJECTED"


async def test_revocation_after_claim_prevents_the_business_effect(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    state, actor, head = await start(messages)
    claim = await claim_command(repository, lease,
        f'#forest run cancel {head["run_id"]} --expect-checkpoint {head["checkpoint_seq"]} --reason test')
    await allow(repository, "another-user")
    with pytest.raises(ForestError) as denied:
        await dispatcher(repository, lease)._typed(lease, claim)
    # A real allowlist publication replaces the Binding epoch. The old Shell
    # claim and Actor both lose ownership before a new business effect starts.
    assert denied.value.code == "SHELL_AUTHORITY_REJECTED"
    persisted = await row(repository, "SELECT phase,checkpoint_seq FROM public.bot_agent_runs WHERE run_id=%s", (head["run_id"],))
    assert persisted == {"phase": head["phase"], "checkpoint_seq": head["checkpoint_seq"]}


async def test_typed_query_publishes_only_current_group_metadata_on_control_lane(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    _, _, ours = await start(messages)
    _, _, other = await start(messages, group="secret-other-group")
    await receive(repository, lease, "list", text="#forest run list")
    controls = dispatcher(repository, lease)
    try:
        await controls.start()
        await until(lambda: completed(repository, 1))
        receipt = await shell_record(repository)
        assert receipt["outcome"]["code"] == "RUN_STATUS"
        assert receipt["delivery"]["send_lane"] == "CONTROL"
        async with repository.store.pool.connection() as conn, conn.transaction():
            parts = await (await conn.execute("SELECT * FROM public.bot_send_parts")).fetchall()
            assert parts and all(part["payload"] is None and part["payload_id"] for part in parts)
            content = DeliveryContentStore(repository.store)
            clock = await content.payloads.lock_in(conn)
            text = str([await content.read_part_in(conn, part,
                expected_deletion_epoch=clock["deletion_epoch"]) for part in parts])
        assert ours["run_id"] in text and other["run_id"] not in text
        assert "secret-other-group" not in text
    finally:
        await controls.close()


async def test_empty_anonymous_identity_cannot_use_admin_commands(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = event("anonymous", text="#forest run list")
    incoming["author"]["anonymous"] = {}
    await repository.ingest(lease, "binding-a", incoming)
    controls = dispatcher(repository, lease)
    try:
        await controls.start()
        await until(lambda: completed(repository, 1))
        assert (await shell_record(repository))["outcome"]["code"] == "DENIED"
    finally:
        await controls.close()


async def test_urgent_cancel_has_a_slot_while_normal_control_is_waiting(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    _, _, head = await start(messages)
    controls = dispatcher(repository, lease)
    controls.max_concurrency = 2
    entered, release = asyncio.Event(), asyncio.Event()
    original = controls._typed

    async def held(lease, claim):
        if claim["command_name"] == "run.read":
            entered.set()
            await release.wait()
        return await original(lease, claim)

    controls._typed = held
    try:
        await receive(repository, lease, "list", text="#forest run list")
        await controls.start()
        await asyncio.wait_for(entered.wait(), 3)
        await receive(repository, lease, "cancel", text=
            f'#forest run cancel {head["run_id"]} --expect-checkpoint {head["checkpoint_seq"]} --reason test')
        controls.notify()
        await until(lambda: completed(repository, 1))
        assert not release.is_set()
        assert (await row(repository, "SELECT phase FROM public.bot_agent_runs"))["phase"] == "CANCELLED"
    finally:
        release.set()
        await controls.close()


async def test_shell_memory_forget_plan_is_bound_to_origin_group(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    ours = await receive(repository, lease, "memory-one", group="group-1", text="erase this")
    other = await receive(repository, lease, "memory-two", group="group-2", text="keep this")
    for index, receipt in enumerate((ours, other)):
        actor = await repository.acquire_actor(
            lease, receipt["conversation_id"], f"memory-actor-{index}", ttl_seconds=30
        )
        await repository.select_input(actor, f"memory-admit-{index}")
    admin = MemoryAdministration(repository)
    async with repository.store.pool.connection() as conn, conn.transaction():
        view = await admin.sources.authorize_sources_in(conn, limit=10, include_payload=True)
    source = next(
        row for row in view["sources"] if row["source"]["channel_id"] == "group-1"
    )

    controls = dispatcher(repository, lease)
    plan_claim = await claim_command(
        repository,
        lease,
        f"#forest memory forget-plan {source['source_unit_id']} --expect-deletion-epoch 0",
        number="memory-plan",
        group="group-1",
    )
    assert plan_claim["gate_version"] == "shell-gate-v6"
    _, proposal = await controls._typed(lease, plan_claim)
    apply_text = (
        f"#forest memory forget-apply {proposal['plan_id']} --digest {proposal['plan_digest']} "
        f"--expect-deletion-epoch {proposal['deletion_epoch']}"
    )
    foreign_claim = await claim_command(
        repository, lease, apply_text, number="memory-foreign", group="group-2"
    )
    with pytest.raises(ForestError) as denied:
        await controls._typed(lease, foreign_claim)
    assert denied.value.code == "SHELL_SCOPE_DENIED"

    own_claim = await claim_command(
        repository, lease, apply_text, number="memory-apply", group="group-1"
    )
    text, outcome = await controls._typed(lease, own_claim)
    assert outcome["code"] == "ACCEPTED" and outcome["deletion_epoch"] == 1
    assert "删除版本 1" in text
    async with repository.store.pool.connection() as conn, conn.transaction():
        current = await admin.sources.authorize_sources_in(conn, limit=10, include_payload=True)
    assert {row["source"]["channel_id"] for row in current["sources"]} == {"group-2"}
