"""Compatible hot reload of untouched frozen ordinary-message Handler items."""

import asyncio
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from binding_seed import create_online_binding, signed_ticket_factory
from test_kernel import bot_database as bot_database
from test_message_handler_chains import event, prepared, set_policy

from forest.kernel import Kernel
from forest.packages import PackageCatalog, digest_json
from forest.storage.bindings import BindingStore
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


def handler_source(tmp_path: Path, version: str, marker: str) -> Path:
    root = tmp_path / ("takeover-kernel-" + version)
    module = root / "src" / "takeover_kernel"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname="takeover-kernel"\nversion="{version}"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        f'''from forest.sdk import Reply, message_handler, plugin

@plugin(
    id="takeover.component", version="{version}",
    message_handler_takeover="compatible",
)
class TakeoverKernel:
    @message_handler("ordinary", priority=10)
    async def ordinary(self, event, ctx):
        return Reply([[{{"type": "text", "text": "{marker}"}}]])
''',
        encoding="utf-8",
    )
    return root


def definition(*, priority=10):
    return {"handler_id": "ordinary", "handler": "ordinary", "priority": priority}


async def register(store, lease, generation, *, mode="compatible", priority=10):
    await store.register_generation(
        lease,
        "takeover.component",
        generation,
        "sha256:" + generation[-1] * 64,
        "sha256:" + "0" * 64,
        "worker-" + generation,
        message_handler_definitions=[definition(priority=priority)],
        message_handler_takeover_mode=mode,
    )
    await store.mark_ready(lease, generation)


async def activate(store, lease, generation, epoch):
    operation = await store.begin_operation(
        lease,
        "takeover.component",
        "activate-" + generation,
        "plan-" + generation,
        generation,
        epoch,
    )
    await store.freeze(lease, operation["operation_id"], operation["op_epoch"])
    return await store.publish(lease, operation["operation_id"], operation["op_epoch"])


async def frozen_chain(bot_database, number=8601):
    store, lease, ticket = await prepared(bot_database)
    await register(store, lease, "gen-1")
    await activate(store, lease, "gen-1", 0)
    await set_policy(
        store,
        ticket,
        "binding-a",
        "takeover.component",
        "ordinary",
        {"mode": "EVERYONE", "user_ids": []},
    )
    messages = MessageStore(store, BindingStore(store))
    accepted = await messages.ingest(
        lease,
        "binding-a",
        event(number),
        decision="TRIGGER",
        rule_revision="binding-trigger-v1",
    )
    return store, lease, messages, accepted


async def takeover_rows(store, chain_id):
    async with store.pool.connection() as conn:
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_item_takeovers "
                "WHERE bot_id=%s AND chain_id=%s ORDER BY takeover_no",
                (store.bot_id, chain_id),
            )
        ).fetchall()


async def test_compatible_publication_takes_over_an_untouched_item(bot_database):
    store, lease, messages, accepted = await frozen_chain(bot_database)
    await register(store, lease, "gen-2")
    await activate(store, lease, "gen-2", 1)

    rows = await takeover_rows(store, accepted["message_handler_chain_id"])
    assert len(rows) == 1
    assert rows[0]["from_generation_id"] == "gen-1"
    assert rows[0]["to_generation_id"] == "gen-2"
    assert rows[0]["takeover_no"] == 1

    actor = await messages.acquire_actor(
        lease, accepted["conversation_id"], "takeover-owner", ttl_seconds=30
    )
    try:
        selected = await messages.select_input(actor, "takeover-select:" + uuid4().hex)
        assert selected["status"] == "MESSAGE_HANDLER"
        assert selected["request"]["generation_id"] == "gen-2"
        assert selected["request"]["frozen_generation_id"] == "gen-1"
        assert selected["request"]["takeover_id"] == rows[0]["takeover_id"]
        assert selected["request"]["handler_spec_digest"] == rows[0]["spec_digest"]
    finally:
        await messages.release_actor(actor)


async def test_kernel_executes_and_commits_the_taken_over_generation(
    bot_database, tmp_path
):
    store, _ = bot_database
    kernel = Kernel(store)
    actor = None
    try:
        await kernel.start()
        lease = kernel.lease
        ticket = await signed_ticket_factory(store, lease)
        await create_online_binding(
            store,
            lease,
            "binding-a",
            "10001",
            ticket=ticket,
            values={
                "enabled": True,
                "api_base_url": "http://127.0.0.1:5700",
                "response_mode": "disabled",
                "response_template": "",
                "trigger_mentions": True,
                "trigger_prefix": "",
                "private_trigger": True,
                "admin_user_ids": [],
            },
        )
        catalog = PackageCatalog(tmp_path / "takeover-catalog")
        await kernel.install(
            catalog.build(handler_source(tmp_path, "0.1.0", "from-v1")),
            message_handler_takeover=True,
        )
        await set_policy(
            store,
            ticket,
            "binding-a",
            "takeover.component",
            "ordinary",
            {"mode": "EVERYONE", "user_ids": []},
        )
        messages = MessageStore(store, BindingStore(store))
        accepted = await messages.ingest(
            lease,
            "binding-a",
            event(8606),
            decision="TRIGGER",
            rule_revision="binding-trigger-v1",
        )
        await kernel.install(
            catalog.build(handler_source(tmp_path, "0.2.0", "from-v2")),
            message_handler_takeover=True,
        )
        actor = await messages.acquire_actor(
            lease, accepted["conversation_id"], "kernel-takeover-owner", ttl_seconds=30
        )
        selected = await messages.select_input(actor, "kernel-select:" + uuid4().hex)
        receipt = await kernel.invoke_message_handler_with_receipt(
            selected["request"],
            selected["event"],
            operation_id=selected["attempt"]["call_operation_id"],
            input_parent_ids=tuple(selected["input_parent_ids"]),
        )
        assert receipt.value == {
            "kind": "REPLY",
            "parts": [[{"type": "text", "text": "from-v2"}]],
        }
        result = await messages.finalize_message_handler(
            actor,
            "kernel-finalize:" + uuid4().hex,
            chain_id=selected["chain"]["chain_id"],
            call_operation_id=selected["attempt"]["call_operation_id"],
            result=receipt.value,
            result_payload_id=receipt.result_payload_id,
            result_payload_digest=receipt.result_digest,
            delivery=DeliveryStore(store, BindingStore(store)),
        )
        assert result["disposition"] == "REPLY"
    finally:
        if actor is not None:
            await messages.release_actor(actor)
        await kernel.close()


async def test_repeated_reload_appends_a_complete_takeover_chain(bot_database):
    store, lease, messages, accepted = await frozen_chain(bot_database, 8602)
    await register(store, lease, "gen-2")
    await activate(store, lease, "gen-2", 1)
    await register(store, lease, "gen-3")
    await activate(store, lease, "gen-3", 2)

    rows = await takeover_rows(store, accepted["message_handler_chain_id"])
    assert [(row["takeover_no"], row["from_generation_id"], row["to_generation_id"])
            for row in rows] == [(1, "gen-1", "gen-2"), (2, "gen-2", "gen-3")]
    actor = await messages.acquire_actor(
        lease, accepted["conversation_id"], "repeat-owner", ttl_seconds=30
    )
    try:
        selected = await messages.select_input(actor, "repeat-select:" + uuid4().hex)
        assert selected["request"]["generation_id"] == "gen-3"
        assert selected["request"]["takeover_no"] == 2
    finally:
        await messages.release_actor(actor)


@pytest.mark.parametrize(
    "mode,priority",
    [("none", 10), ("compatible", 11)],
    ids=("management-or-manifest-opt-in-missing", "spec-changed"),
)
async def test_publication_does_not_take_over_without_both_proofs(
    bot_database, mode, priority
):
    store, lease, _, accepted = await frozen_chain(bot_database, 8603)
    await register(store, lease, "gen-2", mode=mode, priority=priority)
    await activate(store, lease, "gen-2", 1)
    assert await takeover_rows(store, accepted["message_handler_chain_id"]) == []


async def test_claimed_or_attempted_item_is_never_taken_over(bot_database):
    store, lease, messages, accepted = await frozen_chain(bot_database, 8604)
    actor = await messages.acquire_actor(
        lease, accepted["conversation_id"], "begun-owner", ttl_seconds=30
    )
    try:
        selected = await messages.select_input(actor, "begun-select:" + uuid4().hex)
        assert selected["request"]["generation_id"] == "gen-1"
        await register(store, lease, "gen-2")
        await activate(store, lease, "gen-2", 1)
        assert await takeover_rows(store, accepted["message_handler_chain_id"]) == []
    finally:
        await messages.release_actor(actor)


async def test_publication_and_actor_claim_converge_to_one_generation(bot_database):
    store, lease, messages, accepted = await frozen_chain(bot_database, 8607)
    await register(store, lease, "gen-2")
    actor = await messages.acquire_actor(
        lease, accepted["conversation_id"], "race-owner", ttl_seconds=30
    )
    try:
        selected, _ = await asyncio.gather(
            messages.select_input(actor, "race-select:" + uuid4().hex),
            activate(store, lease, "gen-2", 1),
        )
        rows = await takeover_rows(store, accepted["message_handler_chain_id"])
        observed = (selected["request"]["generation_id"], len(rows))
        assert observed in {("gen-1", 0), ("gen-2", 1)}
        if rows:
            assert selected["request"]["takeover_id"] == rows[0]["takeover_id"]
        else:
            assert selected["request"]["takeover_id"] is None
    finally:
        await messages.release_actor(actor)


async def test_takeover_fact_cannot_be_forged_outside_lifecycle_publication(bot_database):
    store, lease, _, accepted = await frozen_chain(bot_database, 8605)
    await register(store, lease, "gen-2")
    chain_id = accepted["message_handler_chain_id"]
    async with store.pool.connection() as conn:
        item = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chain_items "
                "WHERE bot_id=%s AND chain_id=%s AND route_index=0",
                (store.bot_id, chain_id),
            )
        ).fetchone()
        takeover_id = "message-handler-takeover:" + digest_json(
            [chain_id, 0, 1, "gen-2"]
        )
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState,
            match="MESSAGE_HANDLER_TAKEOVER_PROOF_INVALID",
        ):
            await conn.execute(
                "INSERT INTO public.bot_message_handler_item_takeovers("
                "bot_id,takeover_id,chain_id,route_index,takeover_no,component_id,"
                "handler_id,spec_digest,from_generation_id,from_package_digest,"
                "from_route_epoch,from_registry_revision,to_generation_id,"
                "to_package_digest,to_route_epoch,to_registry_revision,"
                "lifecycle_operation_id) VALUES "
                "(%s,%s,%s,0,1,%s,%s,%s,'gen-1',%s,1,1,'gen-2',%s,2,2,'forged')",
                (
                    store.bot_id,
                    takeover_id,
                    chain_id,
                    item["component_id"],
                    item["handler_id"],
                    item["spec_digest"],
                    item["package_digest"],
                    "sha256:" + "2" * 64,
                ),
            )
