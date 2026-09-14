"""Actual durable Shell processing uses frozen typed arguments and safe help."""

from dataclasses import replace

import pytest
from test_agent_store import start
from test_group_scheduler import row
from test_messages import messages as messages
from test_messages import receive
from test_shell import allow, dispatcher, shell_record

from forest import shell_commands
from forest.shell_registry import CURRENT_GATE_VERSION, registry_manifest, render_help
from forest.storage import messages as message_module
from forest.storage.delivery_payloads import DeliveryContentStore

pytestmark = pytest.mark.postgres


async def delivered_text(repository):
    async with repository.store.pool.connection() as conn, conn.transaction():
        parts = await (await conn.execute("SELECT * FROM public.bot_send_parts")).fetchall()
        assert parts and all(part["payload"] is None and part["payload_id"] for part in parts)
        content = DeliveryContentStore(repository.store)
        clock = await content.payloads.lock_in(conn)
        hydrated = [await content.read_part_in(conn, part,
            expected_deletion_epoch=clock["deletion_epoch"]) for part in parts]
    return "".join(segment["text"] for part in hydrated for segment in part)


async def test_durable_help_is_generated_from_unchangeable_contract_and_sent_on_control(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = await receive(repository, lease, "help", text="#forest help")
    exported = registry_manifest()
    exported["commands"].clear()
    controls = dispatcher(repository, lease)
    try:
        await controls._process(incoming["command_id"])
        assert not controls.errors
        record = await shell_record(repository)
        assert record["gate_version"] == CURRENT_GATE_VERSION and record["arguments"] == {}
        assert record["outcome"]["code"] == "HELP" and record["delivery"]["send_lane"] == "CONTROL"
        assert await delivered_text(repository) == render_help(CURRENT_GATE_VERSION)
    finally:
        await controls.close()


async def test_accepted_cancel_survives_dispatcher_restart_without_reparsing(messages, monkeypatch):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    _, _, head = await start(messages)
    current_parser = message_module.parse_command

    def v2_parser(value):
        parsed = current_parser(value)
        return replace(parsed, gate_version="shell-gate-v2") if parsed is not None else None

    monkeypatch.setattr(message_module, "parse_command", v2_parser)
    incoming = await receive(repository, lease, "cancel", text=
        f'#forest runs cancel {head["run_id"]} --reason "原参数" --expect-checkpoint {head["checkpoint_seq"]}')
    monkeypatch.setattr(message_module, "parse_command", current_parser)
    accepted = await shell_record(repository)
    assert accepted["status"] == "PENDING" and accepted["gate_version"] == "shell-gate-v2"
    assert accepted["priority"] == 0 and accepted["arguments"]["reason"] == "原参数"
    before = dispatcher(repository, lease)
    await before.close()
    entered = []

    def cannot_reparse(*args, **kwargs):
        entered.append(True)
        raise AssertionError("A durable command cannot be interpreted a second time")

    monkeypatch.setattr(shell_commands, "_parse", cannot_reparse)
    monkeypatch.setattr(shell_commands, "parse_command", cannot_reparse)
    monkeypatch.setattr(message_module, "parse_command", cannot_reparse)
    after = dispatcher(repository, lease)
    try:
        await after._process(incoming["command_id"])
        await after._process(incoming["command_id"])
        assert not after.errors and not entered
        record = await shell_record(repository)
        assert record["arguments"] == accepted["arguments"] and record["priority"] == accepted["priority"]
        assert record["status"] == "COMPLETED" and record["outcome"]["phase"] == "CANCELLED"
        assert (await row(repository, "SELECT phase FROM public.bot_agent_runs WHERE run_id=%s",
                          (head["run_id"],)))["phase"] == "CANCELLED"
        assert (await row(repository, "SELECT count(*) AS n FROM public.bot_web_operations WHERE action='agent.cancel'"))["n"] == 1
        assert (await row(repository, "SELECT count(*) AS n FROM public.bot_send_parts"))["n"] == 1
        text = await delivered_text(repository)
        assert head["run_id"] in text and "原参数" not in text
    finally:
        await after.close()
