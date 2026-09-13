"""Real Shell admissions never interpret erased or null receipts as a fresh success."""

import pytest
from test_agent_store import start
from test_messages import messages as messages
from test_shell import allow, dispatcher
from test_shell_management import claim_command

from forest.memory_types import SourceRef
from forest.storage.shell_control import ShellControl

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("erased", [False, True])
async def test_shell_recorded_unavailable_and_null_skip_success_and_notification(messages, erased):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    state, actor, head = await start(messages)
    command = await claim_command(
        repository,
        lease,
        f"#forest run cancel {head['run_id']} --expect-checkpoint {head['checkpoint_seq']} --reason test",
    )
    control = ShellControl(repository, lease, command)
    operation = "shell-action:" + command["command_id"]
    async with repository.store.pool.connection() as conn, conn.transaction():
        claims = await control._guard(
            conn, control.admission, "agent.cancel", operation, command["arguments"]
        )
        await control._record(conn, claims, {"phase": "CANCELLED"} if erased else None)
    if erased:
        async with repository.store.pool.connection() as conn:
            domain = await (
                await conn.execute(
                    "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='CONTROL_INPUT' AND owner_id=%s",
                    (control.content._owner("input", operation),),
                )
            ).fetchone()
        await control.content.payloads.forget_sources(
            lease,
            "erase-receipt",
            (SourceRef(domain["source_unit_id"]),),
            expected_deletion_epoch=0,
        )
    notifications = []
    dispatch = dispatcher(repository, lease)
    dispatch.on_change = lambda: notifications.append("unexpected")
    text, outcome = await dispatch._typed(lease, command)
    assert outcome["code"] == ("RECORDED_UNAVAILABLE" if erased else "RECORDED_NULL")
    assert outcome["operation_status"] == "RECORDED" and "ACCEPTED" not in str(outcome)
    assert "没有再次执行" in text and not notifications
    assert (await state.load(actor, head["run_id"]))["phase"] == head["phase"]
