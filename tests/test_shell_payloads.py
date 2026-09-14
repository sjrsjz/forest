"""Shell arguments/results are source-bound and never fall back to row JSONB."""

import asyncio
import json

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_agent_config_payloads import erase_payload
from test_agent_store import start
from test_messages import messages as messages
from test_messages import receive
from test_shell import allow, dispatcher, shell_record

from forest.errors import ForestError

pytestmark = pytest.mark.postgres


async def test_typed_shell_command_freezes_arguments_result_and_delivery_parent_chain(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    _, _, run = await start(messages)
    incoming = await receive(
        repository,
        lease,
        "payload-chain",
        text=(
            f'#forest run cancel {run["run_id"]} --expect-checkpoint '
            f'{run["checkpoint_seq"]} --reason "PRIVATE SHELL REASON"'
        ),
    )
    controls = dispatcher(repository, lease)
    try:
        await controls._process(incoming["command_id"])
        assert not controls.errors
    finally:
        await controls.close()
    visible = await shell_record(repository, incoming["command_id"])
    assert visible["arguments"]["reason"] == "PRIVATE SHELL REASON"
    assert visible["outcome"]["code"] == "ACCEPTED"
    assert visible["delivery"]["send_lane"] == "CONTROL"

    async with repository.store.pool.connection() as conn:
        saved = await (await conn.execute(
            "SELECT * FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=%s",
            (repository.store.bot_id, incoming["command_id"]),
        )).fetchone()
        source = await (await conn.execute(
            "SELECT payload_id FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
            (repository.store.bot_id, saved["event_id"]),
        )).fetchone()
        operation = await (await conn.execute(
            "SELECT input_payload_id,result_payload_id FROM public.bot_web_operations "
            "WHERE bot_id=%s AND operation_id=%s",
            (repository.store.bot_id, "shell-action:" + incoming["command_id"]),
        )).fetchone()
        part = await (await conn.execute(
            "SELECT payload_id FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s",
            (repository.store.bot_id, visible["delivery"]["intent_id"]),
        )).fetchone()
        edges = await (await conn.execute(
            "SELECT payload_id,parent_payload_id FROM public.bot_payload_derivations "
            "WHERE bot_id=%s AND payload_id=ANY(%s)",
            (
                repository.store.bot_id,
                [saved["arguments_payload_id"], operation["input_payload_id"],
                 saved["outcome_payload_id"], part["payload_id"], saved["delivery_payload_id"]],
            ),
        )).fetchall()
    edge_set = {(row["payload_id"], row["parent_payload_id"]) for row in edges}
    assert saved["arguments"] is saved["outcome"] is saved["delivery"] is None
    assert "PRIVATE" not in json.dumps(saved["arguments_metadata"])
    assert saved["outcome_metadata"] == {"code": "ACCEPTED"}
    assert (saved["arguments_payload_id"], source["payload_id"]) in edge_set
    assert (operation["input_payload_id"], saved["arguments_payload_id"]) in edge_set
    assert (saved["outcome_payload_id"], operation["result_payload_id"]) in edge_set
    assert (part["payload_id"], saved["outcome_payload_id"]) in edge_set
    assert (saved["delivery_payload_id"], part["payload_id"]) in edge_set


async def test_forgotten_pending_shell_command_is_terminally_cancelled(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = await receive(repository, lease, "forgotten", text="#forest status")
    async with repository.store.pool.connection() as conn:
        saved = await (await conn.execute(
            "SELECT arguments_payload_id FROM public.bot_shell_requests WHERE command_id=%s",
            (incoming["command_id"],),
        )).fetchone()
    await erase_payload(repository.store, lease, saved["arguments_payload_id"])
    claim = await repository.claim_control(lease, incoming["command_id"], "revoked")
    assert claim["status"] == "CANCELLED"
    assert claim["terminal_reason"] == "CONTROL_CONTENT_REVOKED"
    assert claim["arguments"] is None and claim["arguments_payload_state"] == "UNAVAILABLE"
    replay = await repository.claim_control(lease, incoming["command_id"], "another")
    assert replay["status"] == "CANCELLED"
    async with repository.store.pool.connection() as conn:
        effects = await (await conn.execute(
            "SELECT (SELECT count(*) FROM public.bot_web_operations "
            "WHERE operation_id LIKE 'shell-action:%') AS operations,"
            "(SELECT count(*) FROM public.bot_send_intents) AS sends"
        )).fetchone()
    assert effects == {"operations": 0, "sends": 0}


async def test_completed_shell_content_becomes_unavailable_without_reexecution(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = await receive(repository, lease, "completed-forget", text="#forest help")
    controls = dispatcher(repository, lease)
    try:
        await controls._process(incoming["command_id"])
    finally:
        await controls.close()
    saved = await shell_record(repository, incoming["command_id"])
    await erase_payload(repository.store, lease, saved["arguments_payload_id"])
    replay = await repository.claim_control(lease, incoming["command_id"], "after-forget")
    assert replay["status"] == "COMPLETED"
    assert replay["outcome"] is replay["delivery"] is None
    assert replay["outcome_payload_state"] == replay["delivery_payload_state"] == "UNAVAILABLE"
    async with repository.store.pool.connection() as conn:
        assert (await (await conn.execute(
            "SELECT count(*) AS n FROM public.bot_send_intents"
        )).fetchone())["n"] == 1


async def test_forget_and_claim_race_converges_without_deadlock_or_execution(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    incoming = await receive(repository, lease, "race", text="#forest status")
    async with repository.store.pool.connection() as conn:
        saved = await (await conn.execute(
            "SELECT arguments_payload_id FROM public.bot_shell_requests WHERE command_id=%s",
            (incoming["command_id"],),
        )).fetchone()

    async def claim_once():
        return await repository.claim_control(lease, incoming["command_id"], "racer")

    first, _ = await asyncio.wait_for(
        asyncio.gather(
            claim_once(),
            erase_payload(repository.store, lease, saved["arguments_payload_id"]),
        ),
        timeout=10,
    )
    assert first["status"] in {"CLAIMED", "CANCELLED"}
    terminal = await repository.claim_control(lease, incoming["command_id"], "racer")
    assert terminal["status"] == "CANCELLED"
    with pytest.raises(ForestError) as caught:
        await repository.complete_control(
            lease,
            first,
            "late",
            reply_parts=[[{"type": "text", "text": "must not send"}]],
            outcome={"code": "STATUS"},
            delivery=None,
        )
    assert caught.value.code in {"STALE_COMMAND", "CONTROL_CONTENT_REVOKED"}


async def test_shell_payload_identity_and_terminal_metadata_are_database_immutable(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    pending = await receive(repository, lease, "immutable-pending", text="#forest status")
    completed = await receive(repository, lease, "immutable-completed", text="#forest help")
    controls = dispatcher(repository, lease)
    try:
        await controls._process(completed["command_id"])
    finally:
        await controls.close()

    mutations = (
        (
            "UPDATE public.bot_shell_requests SET arguments_metadata=%s "
            "WHERE bot_id=%s AND command_id=%s",
            (Jsonb({"argument_keys": ["forged"]}), repository.store.bot_id,
             pending["command_id"]),
        ),
        (
            "UPDATE public.bot_shell_requests SET legacy_result_untracked=true "
            "WHERE bot_id=%s AND command_id=%s",
            (repository.store.bot_id, pending["command_id"]),
        ),
        (
            "UPDATE public.bot_shell_requests SET outcome_metadata=%s "
            "WHERE bot_id=%s AND command_id=%s",
            (Jsonb({"code": "FORGED"}), repository.store.bot_id,
             completed["command_id"]),
        ),
        (
            "DELETE FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=%s",
            (repository.store.bot_id, completed["command_id"]),
        ),
    )
    async with repository.store.pool.connection() as conn:
        for statement, params in mutations:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                async with conn.transaction():
                    await conn.execute(statement, params)

        rows = await (await conn.execute(
            "SELECT command_id,status,legacy_result_untracked,outcome_metadata "
            "FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=ANY(%s) "
            "ORDER BY command_id",
            (repository.store.bot_id, [pending["command_id"], completed["command_id"]]),
        )).fetchall()
    assert {row["command_id"] for row in rows} == {
        pending["command_id"], completed["command_id"]
    }
    assert next(row for row in rows if row["command_id"] == pending["command_id"])[
        "legacy_result_untracked"
    ] is False
    terminal = next(row for row in rows if row["command_id"] == completed["command_id"])
    assert terminal["status"] == "COMPLETED" and terminal["outcome_metadata"] == {
        "code": "HELP"
    }
