"""Reviewed cross-trunk associations and their convergent graph projection."""

import asyncio
from uuid import uuid4

import pytest
from binding_seed import _ticket_factory
from psycopg.types.json import Jsonb
from test_config_writer_fence import resources
from test_messages import messages as messages
from test_messages import owner, receive
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.storage import bot as bot_module
from forest.storage.bot import BotStore
from forest.storage.memory_administration import MemoryAdministration
from forest.storage.memory_associations import MemoryAssociationAdministration
from forest.storage.memory_graph import GraphScope, GraphWindow

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


async def endpoints(repository, lease, ticket):
    listed = await request(
        MemoryAdministration(repository), ticket, "memory.forget.read", "read", {"limit": 50}
    )
    return listed, {row["source"]["channel_id"]: row for row in listed["sources"]}


async def apply_plan(admin, ticket, lease, plan, operation):
    payload = {
        key: plan[key]
        for key in ("expected_deletion_epoch", "plan_id", "plan_digest")
    }
    return await request(
        admin,
        ticket,
        "memory.association.apply",
        "apply",
        payload,
        lease=lease,
        operation=operation,
    )


async def test_reviewed_association_retract_and_reactivate_publish_only_explicit_edge(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, group="group-one", user="alice", text="alpha")
    second = await receive(repository, lease, 2, group="group-two", user="bob", text="beta")
    await admit(repository, lease, first, "admit-one")
    await admit(repository, lease, second, "admit-two")
    ticket = await _ticket_factory(repository.store, lease)
    listed, by_group = await endpoints(repository, lease, ticket)
    ids = [by_group[group]["source_unit_id"] for group in ("group-two", "group-one")]
    admin = MemoryAssociationAdministration(repository)

    proposal = {
        "expected_deletion_epoch": listed["deletion_epoch"],
        "expected_revision": 0,
        "active": True,
        "source_unit_ids": ids,
    }
    plan = await request(
        admin, ticket, "memory.association.plan", "plan", proposal, operation="associate-plan"
    )
    reverse = await request(
        admin,
        ticket,
        "memory.association.plan",
        "plan",
        {**proposal, "source_unit_ids": list(reversed(ids))},
        operation="associate-plan-reverse",
    )
    assert plan["association_id"] == reverse["association_id"]
    created = await apply_plan(admin, ticket, lease, plan, "associate-apply")
    replay = await apply_plan(admin, ticket, lease, plan, "associate-apply")
    assert replay == created
    assert created["revision"] == 1 and created["projection_state"] == "PENDING"
    pending = await admin.graph.pending(lease)
    assert len(pending["scopes"]) == 1
    scope = pending["scopes"][0]
    await admin.graph.refresh(lease, scope)
    current = await request(
        admin, ticket, "memory.association.read", "read", {"limit": 50}
    )
    assert current["associations"][0]["projection_state"] == "CURRENT"
    relation = current["associations"][0]["relation"]
    assert relation["kind"] == "ASSOCIATES_WITH"
    assert relation["origin"] == "REVIEWED_MANUAL"
    assert relation["cross_trunk"]
    assert relation["contribution_id"] == created["association_id"] + ":revision:1"
    assert {
        current["associations"][0][side]["source"]["channel_id"]
        for side in ("left", "right")
    } == {"group-one", "group-two"}
    projection = await admin.graph.read(lease, scope.scope_id)
    assert [edge["kind"] for edge in projection["edges"]] == ["ASSOCIATES_WITH"]
    assert {node["ref"]["kind"] for node in projection["nodes"]} == {"SOURCE_UNIT"}

    filtered = await request(
        admin, ticket, "memory.association.read", "read",
        {
            "limit": 50,
            "source_unit_id": ids[0],
            "group_source_id": by_group["group-one"]["source"]["group_source_id"],
            "active": True,
            "projection_state": "CURRENT",
        },
    )
    assert [item["association_id"] for item in filtered["associations"]] == [
        created["association_id"]
    ]
    scopes = await request(
        admin, ticket, "memory.association.read", "graph_scopes",
        {"limit": 25}, lease=lease,
    )
    assert scopes["graph_revision"] >= 1
    assert any(item["scope_id"] == scope.scope_id for item in scopes["scopes"])
    graph_view = await request(
        admin, ticket, "memory.association.read", "graph_scope",
        {"scope_id": scope.scope_id, "graph_revision": scopes["graph_revision"], "limit": 100},
        lease=lease,
    )
    assert graph_view["coverage"]["partition"] == "associations-v1"
    assert [edge["kind"] for edge in graph_view["edges"]] == ["ASSOCIATES_WITH"]
    assert graph_view["required_source_refs"]

    retract = await request(
        admin,
        ticket,
        "memory.association.plan",
        "plan",
        {
            "expected_deletion_epoch": 0,
            "expected_revision": 1,
            "active": False,
            "association_id": created["association_id"],
        },
        operation="retract-plan",
    )
    removed = await apply_plan(admin, ticket, lease, retract, "retract-apply")
    assert removed["revision"] == 2 and not removed["active"]
    await admin.graph.refresh(lease, scope)
    inactive = await request(admin, ticket, "memory.association.read", "read", {"limit": 50})
    assert len(inactive["associations"]) == 1
    assert not inactive["associations"][0]["active"]
    assert inactive["associations"][0]["projection_state"] == "CURRENT"

    reactivate = await request(
        admin,
        ticket,
        "memory.association.plan",
        "plan",
        {**proposal, "expected_revision": 2},
        operation="reactivate-plan",
    )
    restored = await apply_plan(admin, ticket, lease, reactivate, "reactivate-apply")
    assert restored["revision"] == 3 and restored["active"]
    await admin.graph.refresh(lease, scope)
    projection = await admin.graph.read(lease, scope.scope_id)
    assert [edge["kind"] for edge in projection["edges"]] == ["ASSOCIATES_WITH"]
    assert projection["edges"][0]["contribution_ids"] == [
        created["association_id"] + ":revision:3"
    ]


async def test_competing_create_plans_converge_through_revision_cas(messages):
    repository, lease, _ = messages
    first = await receive(repository, lease, 1, group="left")
    second = await receive(repository, lease, 2, group="right")
    await admit(repository, lease, first, "admit-left")
    await admit(repository, lease, second, "admit-right")
    ticket = await _ticket_factory(repository.store, lease)
    listed, by_group = await endpoints(repository, lease, ticket)
    proposal = {
        "expected_deletion_epoch": listed["deletion_epoch"],
        "expected_revision": 0,
        "active": True,
        "source_unit_ids": [by_group["left"]["source_unit_id"], by_group["right"]["source_unit_id"]],
    }
    admin = MemoryAssociationAdministration(repository)
    plans = await asyncio.gather(*(
        request(
            admin,
            ticket,
            "memory.association.plan",
            "plan",
            proposal,
            operation=f"competing-plan-{index}",
        )
        for index in range(2)
    ))
    results = await asyncio.gather(*(
        apply_plan(admin, ticket, lease, plan, f"competing-apply-{index}")
        for index, plan in enumerate(plans)
    ), return_exceptions=True)
    assert sum(isinstance(item, dict) for item in results) == 1
    errors = [item for item in results if isinstance(item, ForestError)]
    assert len(errors) == 1 and errors[0].code == "ASSOCIATION_REVISION_CONFLICT"
    async with repository.store.pool.connection() as conn:
        heads = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_memory_association_heads"
        )).fetchone()
        changes = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_memory_association_changes"
        )).fetchone()
        effects = await (await conn.execute(
            "SELECT action AS name FROM public.bot_audit_records WHERE operation_id LIKE 'competing-apply-%' "
            "UNION ALL SELECT topic AS name FROM public.bot_outbox_events "
            "WHERE operation_id LIKE 'competing-apply-%' ORDER BY name"
        )).fetchall()
    assert heads["count"] == changes["count"] == 1
    assert [row["name"] for row in effects] == [
        "memory.association.apply", "memory.association.apply"
    ]


def test_non_trunk_scope_accepts_disjoint_positions_without_accepting_overlap():
    scope = GraphScope(
        (GraphWindow("same-trunk", 1, 1), GraphWindow("same-trunk", 300, 300)),
        "associations-v1",
    )
    assert len(scope.windows) == 2
    with pytest.raises(ForestError) as overlap:
        GraphScope(
            (GraphWindow("same-trunk", 1, 4), GraphWindow("same-trunk", 4, 8)),
            "associations-v1",
        )
    assert overlap.value.code == "GRAPH_INVALID"


async def test_052_upgrade_adds_only_implied_association_rights(
    upgrade_database, tmp_path, monkeypatch
):
    current_files = bot_module.files
    old = resources(tmp_path / "schema-051", cutoff="051")
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
        }
        async with store.pool.connection() as conn:
            for issuer, actions in issuers.items():
                await conn.execute(
                    "INSERT INTO public.bot_web_issuers"
                    "(bot_id,issuer_id,key_revision,public_key,enabled,actions) "
                    "VALUES (%s,%s,1,'unused-test-key',true,%s)",
                    (store.bot_id, issuer, Jsonb(actions)),
                )
        monkeypatch.setattr(bot_module, "files", current_files)
        await store.migrate()
        async with store.pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT issuer_id,actions FROM public.bot_web_issuers ORDER BY issuer_id"
            )).fetchall()
            relation = await (await conn.execute(
                "SELECT to_regclass('public.bot_memory_association_heads') AS relation"
            )).fetchone()
        upgraded = {row["issuer_id"]: set(row["actions"]) for row in rows}
        assert upgraded["reader"] == {"memory.read", "memory.association.read"}
        assert upgraded["planner"] == {
            "memory.read", "memory.config.plan", "memory.association.read",
            "memory.association.plan",
        }
        assert upgraded["applier"] == {
            "memory.read", "memory.config.apply", "memory.association.read",
            "memory.association.apply",
        }
        assert upgraded["config-only"] == {"memory.config.plan", "memory.config.apply"}
        assert relation["relation"] == "bot_memory_association_heads"
    finally:
        await store.close()
