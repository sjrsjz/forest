"""Business-command selection is frozen with ordinary message admission."""

import pytest
from binding_seed import _ticket_factory
from test_business_command_registry import activate, definition, register
from test_messages import event
from test_messages import messages as messages

from forest.memory_types import SourceRef
from forest.storage.command_policies import CommandPolicyStore
from forest.storage.input_payloads import InputContentStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def install_command(repository, lease):
    declared = definition()
    declared["argument_mode"] = "tail_text"
    await register(repository.store, lease, "greeter", "gen-1", [declared])
    await activate(repository.store, lease, "greeter", "gen-1", 0)


async def set_policy(repository, lease, values, *, revision=0):
    tickets = await _ticket_factory(repository.store)
    policies = CommandPolicyStore(repository.store)
    suffix = str(revision)
    proposal = {
        "binding_id": "binding-a",
        "command_id": "hello",
        "expected_revision": revision,
        "values": values,
    }
    plan = await policies.plan(
        await tickets("command.policy.plan", "ingress-policy-plan-" + suffix, proposal),
        "ingress-policy-plan-" + suffix,
        proposal,
    )
    apply = {
        "binding_id": "binding-a",
        "command_id": "hello",
        "expected_revision": revision,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    return await policies.apply(
        await tickets(
            "command.policy.apply",
            "ingress-policy-apply-" + suffix,
            apply,
            plan_digest=plan["plan_digest"],
        ),
        "ingress-policy-apply-" + suffix,
        apply,
    )


async def request_row(repository, request_id):
    async with repository.store.pool.connection() as conn:
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_business_command_requests "
                "WHERE bot_id=%s AND request_id=%s",
                (repository.store.bot_id, request_id),
            )
        ).fetchone()


async def test_enabled_command_freezes_route_policy_author_and_arguments(messages):
    repository, lease, _ = messages
    await install_command(repository, lease)
    await set_policy(repository, lease, {"mode": "EVERYONE", "user_ids": []})

    incoming = event(1, user="20001", text="  /hello   世界  ")
    incoming["segments"].insert(0, {"type": "mention", "external_user_id": "1000"})
    accepted = await repository.ingest(
        lease, "binding-a", incoming, decision="TRIGGER", rule_revision="binding:1"
    )
    assert accepted["trigger_decision"] == "BUSINESS_COMMAND"
    row = await request_row(repository, accepted["business_request_id"])
    assert row["status"] == "PENDING"
    assert row["principal_ref"].startswith("principal:")
    assert row["external_user_id"] == "20001"
    assert (row["generation_id"], row["route_epoch"], row["registry_revision"]) == (
        "gen-1",
        1,
        1,
    )
    assert row["command_policy_revision"] == 1
    async with repository.store.pool.connection() as conn, conn.transaction():
        epoch = (await PayloadStore(repository.store).lock_in(conn))["deletion_epoch"]
        arguments = await PayloadStore(repository.store).read_json_in(
            conn, row["arguments_payload_id"], expected_deletion_epoch=epoch
        )
        source = await InputContentStore(repository.store).read_input_in(
            conn, accepted["event_id"], expected_deletion_epoch=epoch, required=True
        )
        triggers = await (
            await conn.execute(
                "SELECT count(*) AS count FROM public.bot_conversation_triggers WHERE bot_id=%s",
                (repository.store.bot_id,),
            )
        ).fetchone()
    assert arguments["argument_text"] == "世界  "
    assert source["author"]["external_user_id"] == "20001"
    assert source["segments"] == incoming["segments"]
    assert triggers["count"] == 0


async def test_policy_denial_is_terminal_and_never_falls_back_to_agent(messages):
    repository, lease, _ = messages
    await install_command(repository, lease)
    await set_policy(repository, lease, {"mode": "USER_SET", "user_ids": ["20002"]})
    accepted = await repository.ingest(
        lease, "binding-a", event(1, user="20003", text="/hello secret"), decision="TRIGGER"
    )
    row = await request_row(repository, accepted["business_request_id"])
    assert accepted["trigger_decision"] == "BUSINESS_COMMAND"
    assert row["status"] == "DENIED"
    assert row["terminal_reason"] == "BUSINESS_COMMAND_DENIED"
    assert row["completed_at"] is not None
    async with repository.store.pool.connection() as conn:
        trigger = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_conversation_triggers WHERE bot_id=%s",
                (repository.store.bot_id,),
            )
        ).fetchone()
    assert trigger is None


async def test_disabled_or_non_contiguous_token_uses_existing_trigger_rule(messages):
    repository, lease, _ = messages
    await install_command(repository, lease)
    disabled = await repository.ingest(
        lease, "binding-a", event(1, user="20001", text="/hello"), decision="TRIGGER"
    )
    assert disabled["trigger_decision"] == "TRIGGER"
    assert disabled["business_request_id"] is None

    split = event(2, user="20001", text="/hel")
    split["segments"] += [
        {"type": "image", "media_ref": {"kind": "remote", "uri": "https://invalid.test/x"}},
        {"type": "text", "text": "lo"},
    ]
    fragmented = await repository.ingest(
        lease, "binding-a", split, decision="NO_TRIGGER"
    )
    assert fragmented["trigger_decision"] == "NO_TRIGGER"
    assert fragmented["business_request_id"] is None


async def test_forget_cancels_unstarted_request_and_duplicate_does_not_restore_body(messages):
    repository, lease, _ = messages
    await install_command(repository, lease)
    await set_policy(repository, lease, {"mode": "EVERYONE", "user_ids": []})
    original = event(1, user="20001", text="/hello erase-this")
    accepted = await repository.ingest(
        lease, "binding-a", original, decision="NO_TRIGGER"
    )
    row = await request_row(repository, accepted["business_request_id"])
    async with repository.store.pool.connection() as conn:
        source = await (
            await conn.execute(
                "SELECT source_unit_id FROM public.bot_memory_source_occurrences "
                "WHERE bot_id=%s AND event_id=%s",
                (repository.store.bot_id, accepted["event_id"]),
            )
        ).fetchone()
    payloads = PayloadStore(repository.store)
    await payloads.forget_sources(
        lease,
        "forget-business-command",
        (SourceRef(source["source_unit_id"], 1, "*"),),
        expected_deletion_epoch=0,
    )
    cancelled = await request_row(repository, accepted["business_request_id"])
    assert cancelled["status"] == "CANCELLED"
    assert cancelled["terminal_reason"] == "CONTROL_CONTENT_REVOKED"
    duplicate = await repository.ingest(
        lease, "binding-a", original, decision="TRIGGER"
    )
    assert duplicate["duplicate"]
    assert duplicate["business_request_id"] == accepted["business_request_id"]
    await payloads.scrub(lease, limit=64)
    async with repository.store.pool.connection() as conn:
        body = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=%s",
                (repository.store.bot_id, row["arguments_payload_id"]),
            )
        ).fetchone()
    assert body is None


async def test_policy_restriction_cancels_only_newly_unauthorized_pending_requests(messages):
    repository, lease, _ = messages
    await install_command(repository, lease)
    await set_policy(repository, lease, {"mode": "EVERYONE", "user_ids": []})
    first = await repository.ingest(
        lease, "binding-a", event(1, user="20001", text="/hello one"), decision="NO_TRIGGER"
    )
    second = await repository.ingest(
        lease, "binding-a", event(2, user="20002", text="/hello two"), decision="NO_TRIGGER"
    )
    await set_policy(
        repository,
        lease,
        {"mode": "USER_SET", "user_ids": ["20002"]},
        revision=1,
    )
    left = await request_row(repository, first["business_request_id"])
    right = await request_row(repository, second["business_request_id"])
    assert (left["status"], left["terminal_reason"]) == (
        "CANCELLED",
        "COMMAND_POLICY_REVOKED",
    )
    assert right["status"] == "PENDING"
