"""Reviewed global memory source listing and convergent forgetting."""

import asyncio
from uuid import uuid4

import pytest
from binding_seed import _ticket_factory
from psycopg.types.json import Jsonb
from test_config_writer_fence import resources
from test_messages import event, owner, receive
from test_messages import messages as messages
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage import bot as bot_module
from forest.storage.bot import BotStore
from forest.storage.memory_administration import MemoryAdministration
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def admit(repository, lease, receipt, operation):
    actor = await owner(repository, lease, receipt["conversation_id"], operation + "-actor")
    await repository.select_input(actor, operation)


async def request(admin, ticket, action, method, payload, *, lease=None, operation=None):
    operation = operation or action + ":" + uuid4().hex
    signed = await ticket(
        action,
        operation,
        payload,
        plan_digest=payload.get("plan_digest", ""),
    )
    kwargs = {"lease": lease} if lease is not None else {}
    return await getattr(admin, method)(signed, operation, payload, **kwargs)


async def test_global_source_listing_keeps_group_author_and_multimodal_provenance(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, group="group-one", user="alice", text="first")
    picture = event(2, group="group-two", user="bob")
    picture["segments"] = [
        {"type": "text", "text": "picture"},
        {"type": "image", "media_ref": {"kind": "remote", "uri": "https://example.invalid/x"}},
    ]
    second = await repository.ingest(lease, "binding-a", picture, decision="NO_TRIGGER")
    await admit(repository, lease, first, "admit-one")
    await admit(repository, lease, second, "admit-two")

    ticket = await _ticket_factory(repository.store, lease)
    result = await request(
        MemoryAdministration(repository), ticket, "memory.forget.read", "read", {"limit": 50}
    )

    assert result["deletion_epoch"] == 0
    assert {row["source"]["channel_id"] for row in result["sources"]} == {
        "group-one", "group-two"
    }
    assert {row["author"]["external_user_id"] for row in result["sources"]} == {"alice", "bob"}
    image = next(row for row in result["sources"] if row["source"]["channel_id"] == "group-two")
    assert [part["type"] for part in image["parts"]] == ["text", "image"]
    assert image["source"]["group_source_id"] == image["source"]["target_group_source_id"]


async def test_reviewed_forget_is_atomic_replayable_and_immediately_suppressed(messages):
    repository, lease, _ = messages
    receipt = await receive(repository, lease, 1, text="erase me")
    await admit(repository, lease, receipt, "admit")
    ticket = await _ticket_factory(repository.store, lease)
    admin = MemoryAdministration(repository)
    listed = await request(admin, ticket, "memory.forget.read", "read", {"limit": 50})
    source_id = listed["sources"][0]["source_unit_id"]
    proposal = {"expected_deletion_epoch": 0, "source_unit_ids": [source_id]}
    plan = await request(
        admin, ticket, "memory.forget.plan", "plan", proposal, operation="forget-plan"
    )
    assert plan["source_count"] == 1 and plan["affected_payloads"] >= 1
    payload = {
        "expected_deletion_epoch": plan["expected_deletion_epoch"],
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    applied = await request(
        admin, ticket, "memory.forget.apply", "apply", payload,
        lease=lease, operation="forget-apply",
    )
    replay = await request(
        admin, ticket, "memory.forget.apply", "apply", payload,
        lease=lease, operation="forget-apply",
    )
    assert replay == applied
    assert applied["deletion_epoch"] == 1 and applied["scrub_pending"]
    current = await request(admin, ticket, "memory.forget.read", "read", {"limit": 50})
    assert current["deletion_epoch"] == 1 and current["sources"] == []
    async with repository.store.pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT action,count(*) AS count FROM public.bot_web_operations "
            "WHERE action LIKE 'memory.forget.%' GROUP BY action ORDER BY action"
        )).fetchall()
        clock = await (await conn.execute(
            "SELECT deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s",
            (repository.store.bot_id,),
        )).fetchone()
        effects = await (await conn.execute(
            "SELECT action AS name FROM public.bot_audit_records WHERE operation_id='forget-apply' "
            "UNION ALL SELECT topic AS name FROM public.bot_outbox_events "
            "WHERE operation_id='forget-apply' ORDER BY name"
        )).fetchall()
    assert {row["action"]: row["count"] for row in rows} == {
        "memory.forget.apply": 1,
        "memory.forget.plan": 1,
    }
    assert clock["deletion_epoch"] == 1
    assert [row["name"] for row in effects] == [
        "memory.forget.apply", "memory.forget.apply",
        "memory.payload_sources_revoked", "memory.payload_sources_revoked",
    ]


async def test_competing_same_epoch_plans_converge_to_one_forget(messages):
    repository, lease, _ = messages
    receipts = await asyncio.gather(
        receive(repository, lease, 1, text="one"),
        receive(repository, lease, 2, text="two"),
    )
    await admit(repository, lease, receipts[0], "admit")
    ticket = await _ticket_factory(repository.store, lease)
    admin = MemoryAdministration(repository)
    listed = await request(admin, ticket, "memory.forget.read", "read", {"limit": 50})
    plans = []
    for index, source in enumerate(listed["sources"]):
        plans.append(await request(
            admin,
            ticket,
            "memory.forget.plan",
            "plan",
            {"expected_deletion_epoch": 0, "source_unit_ids": [source["source_unit_id"]]},
            operation=f"competing-plan-{index}",
        ))

    async def apply(index):
        plan = plans[index]
        payload = {key: plan[key] for key in (
            "expected_deletion_epoch", "plan_id", "plan_digest"
        )}
        return await request(
            admin, ticket, "memory.forget.apply", "apply", payload,
            lease=lease, operation=f"competing-apply-{index}",
        )

    results = await asyncio.gather(apply(0), apply(1), return_exceptions=True)
    assert sum(isinstance(item, dict) for item in results) == 1
    errors = [item for item in results if isinstance(item, ForestError)]
    assert len(errors) == 1 and errors[0].code == "DELETION_CONFLICT"
    current = await request(admin, ticket, "memory.forget.read", "read", {"limit": 50})
    assert current["deletion_epoch"] == 1 and len(current["sources"]) == 1


async def test_apply_rejects_a_new_affected_derivation_after_preview(messages):
    repository, lease, _ = messages
    receipt = await receive(repository, lease, 1, text="review impact")
    await admit(repository, lease, receipt, "admit")
    ticket = await _ticket_factory(repository.store, lease)
    admin = MemoryAdministration(repository)
    listed = await request(admin, ticket, "memory.forget.read", "read", {"limit": 50})
    source_id = listed["sources"][0]["source_unit_id"]
    plan = await request(
        admin,
        ticket,
        "memory.forget.plan",
        "plan",
        {"expected_deletion_epoch": 0, "source_unit_ids": [source_id]},
    )
    await PayloadStore(repository.store).put_json(
        lease,
        owner_kind="model.snapshot",
        owner_id="late-reviewed-derivation",
        value={"late": "derived after preview"},
        sources=(SourceRef(source_id),),
        expected_deletion_epoch=0,
    )
    payload = {key: plan[key] for key in (
        "expected_deletion_epoch", "plan_id", "plan_digest"
    )}
    with pytest.raises(ForestError) as changed:
        await request(
            admin, ticket, "memory.forget.apply", "apply", payload, lease=lease
        )
    assert changed.value.code == "FORGET_PLAN_CHANGED"
    current = await request(admin, ticket, "memory.forget.read", "read", {"limit": 50})
    assert current["deletion_epoch"] == 0 and len(current["sources"]) == 1


async def test_051_and_052_upgrades_add_only_implied_memory_rights_and_keep_disabled_issuer(
    upgrade_database, tmp_path, monkeypatch
):
    current_files = bot_module.files
    old = resources(tmp_path / "schema-050", cutoff="050")
    monkeypatch.setattr(bot_module, "files", lambda namespace: old)
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        await store.migrate()
        issuers = {
            "reader": ["memory.read"],
            "planner": ["memory.read", "memory.config.plan"],
            "applier": ["memory.read", "memory.config.apply"],
            "config-only": ["memory.config.plan", "memory.config.apply"],
            "disabled": ["memory.read", "memory.config.plan", "memory.config.apply"],
        }
        async with store.pool.connection() as conn:
            for issuer, actions in issuers.items():
                await conn.execute(
                    "INSERT INTO public.bot_web_issuers"
                    "(bot_id,issuer_id,key_revision,public_key,enabled,actions) "
                    "VALUES (%s,%s,1,'unused-test-key',%s,%s)",
                    (store.bot_id, issuer, issuer != "disabled", Jsonb(actions)),
                )
        monkeypatch.setattr(bot_module, "files", current_files)
        await store.migrate()
        async with store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT issuer_id,enabled,actions FROM public.bot_web_issuers ORDER BY issuer_id"
            )).fetchall()
            relation = await (await conn.execute(
                "SELECT to_regclass('public.bot_memory_forget_plans') AS forget_relation,"
                "to_regclass('public.bot_memory_association_heads') AS association_relation"
            )).fetchone()
        upgraded = {row["issuer_id"]: set(row["actions"]) for row in rows}
        assert upgraded["reader"] == {
            "memory.read", "memory.forget.read", "memory.association.read"
        }
        assert upgraded["planner"] == {
            "memory.read", "memory.config.plan", "memory.forget.read", "memory.forget.plan",
            "memory.association.read", "memory.association.plan",
        }
        assert upgraded["applier"] == {
            "memory.read", "memory.config.apply", "memory.forget.read", "memory.forget.apply",
            "memory.association.read", "memory.association.apply",
        }
        assert upgraded["config-only"] == {"memory.config.plan", "memory.config.apply"}
        assert not next(row for row in rows if row["issuer_id"] == "disabled")["enabled"]
        assert upgraded["disabled"] == {
            "memory.read", "memory.config.plan", "memory.config.apply", "memory.forget.read",
            "memory.forget.plan", "memory.forget.apply", "memory.association.read",
            "memory.association.plan", "memory.association.apply",
        }
        assert relation == {
            "forget_relation": "bot_memory_forget_plans",
            "association_relation": "bot_memory_association_heads",
        }
    finally:
        await store.close()
