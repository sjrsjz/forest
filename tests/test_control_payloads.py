"""Real PostgreSQL control identities, parent closure, replay and late writes."""

import asyncio

import psycopg
import pytest
import pytest_asyncio
from test_bot_store import database as database
from test_bot_store import store as store

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.control_payloads import ControlPayloadStore

pytestmark = pytest.mark.postgres
ACTION = "config.plan"


@pytest_asyncio.fixture
async def content(store):
    return ControlPayloadStore(store), await store.acquire_host("host", "incarnation", ttl_seconds=120)


async def input_body(content, operation="preview", value=None, *, action=ACTION, parents=(), epoch=0):
    repo, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        result = await repo.put_input_in(conn, operation, value, action=action, parents=parents, epoch=epoch)
        await repo.store._host(conn, lease)
        return result


async def result_body(content, original, value=None, *, operation="preview", action=ACTION, parents=(), epoch=0):
    repo, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        result = await repo.put_result_in(
            conn, operation, value, action=action, input_payload_id=original.payload_id,
            parents=parents, epoch=epoch,
        )
        await repo.store._host(conn, lease)
        return result


def metadata(kind, ref, *, operation="preview", action=ACTION):
    return {"operation_id": operation, "action": action, "status": "COMPLETED",
            kind + "_payload_id": ref.payload_id, kind + "_payload_digest": ref.digest,
            kind: {"legacy": "MUST-NOT-FALLBACK"}}


async def hydrate(content, kind, row, *, epoch=0, required=True):
    repo, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        value = await getattr(repo, "hydrate_" + kind + "_in")(conn, row, epoch=epoch, required=required)
        await repo.store._host(conn, lease)
        return value


async def rows(content, query, args=()):
    async with content[0].store.pool.connection() as conn:
        return await (await conn.execute(query, args)).fetchall()


async def fail(code, operation):
    with pytest.raises(ForestError) as exc:
        await operation
    assert exc.value.code == code
    assert "MUST-NOT-FALLBACK" not in str(exc.value)
    return exc.value


async def domain(content, operation="preview"):
    repo = content[0]
    result = await rows(content, "SELECT source_unit_id FROM public.bot_payload_domains "
                        "WHERE bot_id=%s AND kind='CONTROL_INPUT' AND owner_id=%s",
                        (repo.store.bot_id, repo._owner("input", operation)))
    return SourceRef(result[0]["source_unit_id"])


async def test_concurrent_same_operation_has_one_domain_input_and_result(content):
    operation = "long-operation:" + "x" * 1000

    async def transaction():
        original = await input_body(content, operation, {"config": {"label": "A"}})
        result = await result_body(content, original, {"values": {"label": "A"}}, operation=operation)
        return original, result

    outcomes = await asyncio.gather(*(transaction() for _ in range(8)))
    assert all(result == outcomes[0] for result in outcomes)
    assert len(await rows(content, "SELECT * FROM public.bot_payload_domains")) == 1
    bodies = await rows(content, "SELECT owner_kind,owner_id,manifest FROM public.bot_payload_headers")
    assert sorted(row["owner_kind"] for row in bodies) == ["control.input", "control.result"]
    assert all(len(row["owner_id"].encode()) <= 256 for row in bodies)
    result = next(row for row in bodies if row["owner_kind"] == "control.result")
    assert result["manifest"]["parents"] == [outcomes[0][0].payload_id]
    loaded = await hydrate(content, "result", metadata("result", outcomes[0][1], operation=operation))
    assert loaded["result"] == {"values": {"label": "A"}}
    assert loaded["result_payload_state"] == "ACTIVE"


async def test_json_null_keeps_receipts_but_erased_body_never_replays_as_json_null(content):
    repo, lease = content
    original = await input_body(content)
    result = await result_body(content, original)
    for kind, ref in (("input", original), ("result", result)):
        loaded = await hydrate(content, kind, metadata(kind, ref))
        assert loaded[kind] is None and loaded[kind + "_payload_state"] == "ACTIVE"
    await repo.payloads.forget_sources(lease, "forget-control", (await domain(content),), expected_deletion_epoch=0)
    assert (await repo.payloads.scrub(lease))["removed"] == 2
    for kind, ref in (("input", original), ("result", result)):
        row = metadata(kind, ref)
        loaded = await hydrate(content, kind, row, epoch=1, required=False)
        assert loaded[kind] is None and loaded[kind + "_payload_state"] == "UNAVAILABLE"
        assert loaded["status"] == "COMPLETED" and loaded[kind + "_payload_digest"] == ref.digest
        await fail("CONTROL_CONTENT_REVOKED", hydrate(content, kind, row, epoch=1))
    await fail("CONTROL_CONTENT_REVOKED", input_body(content, epoch=1))
    await fail("CONTROL_CONTENT_REVOKED", result_body(content, original, epoch=1))
    assert not await rows(content, "SELECT * FROM public.bot_payload_bodies")
    assert len(await rows(content, "SELECT * FROM public.bot_payload_domains")) == 1
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 2


async def test_actual_input_plan_current_previous_parents_form_one_complete_closure(content):
    repo, lease = content
    original = await input_body(content, value={"values": "REQUEST-CANARY"})
    leaves = []
    for name in ("plan", "current", "previous"):
        ref = await repo.payloads.put_json(
            lease, owner_kind="control.config", owner_id=name, value={"name": name},
            sources=(SourceRef(name),), expected_deletion_epoch=0,
        )
        leaves.append(ref)
    parents = tuple(ref.payload_id for ref in leaves)
    result = await result_body(content, original, {"before_values": "copied"}, parents=parents)
    assert result == await result_body(content, original, {"before_values": "copied"}, parents=parents[::-1] * 2)
    head = (await rows(content, "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
                       (result.payload_id,)))[0]
    assert set(head["manifest"]["parents"]) == {original.payload_id, *parents}
    assert {s["source_unit_id"] for s in head["manifest"]["sources"]} == {
        "plan", "current", "previous", (await domain(content)).source_unit_id,
    }
    await repo.payloads.forget_sources(lease, "forget-previous", (SourceRef("previous"),), expected_deletion_epoch=0)
    await fail("CONTROL_CONTENT_REVOKED", hydrate(content, "result", metadata("result", result), epoch=1))
    assert (await hydrate(content, "input", metadata("input", original), epoch=1))["input"] == {"values": "REQUEST-CANARY"}
    assert await repo.payloads.read_json(lease, leaves[1].payload_id, expected_deletion_epoch=1) == {"name": "current"}


async def test_same_owner_rejects_changed_action_value_and_result_dependencies(content):
    original = await input_body(content, value={"a": 1})
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", input_body(content, value={"a": 1}, action="config.apply"))
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", input_body(content, value={"a": 2}))
    await result_body(content, original, {"approved": True})
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", result_body(content, original, {"approved": False}))
    extra = await input_body(content, "independent", {"a": 1})
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", result_body(
        content, original, {"approved": True}, parents=(extra.payload_id,),
    ))
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 3


async def test_result_cannot_use_another_operation_or_action_input(content):
    original = await input_body(content, value={"same": "JSON"})
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", result_body(content, original, operation="other"))
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", result_body(content, original, action="config.apply"))
    assert not await rows(content, "SELECT * FROM public.bot_payload_headers WHERE owner_kind='control.result'")


async def test_hydrate_rejects_wrong_reference_digest_action_and_missing_reference(content):
    original = await input_body(content)
    result = await result_body(content, original)
    row = metadata("result", result)
    for replacement in (
        {"result_payload_id": original.payload_id, "result_payload_digest": original.digest},
        {"result_payload_digest": "sha256:" + "0" * 64},
        {"action": "config.apply"}, {"operation_id": "another"},
    ):
        await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", hydrate(content, "result", {**row, **replacement}))
    await fail("CONTROL_PAYLOAD_MISSING", hydrate(content, "result", {**row, "result_payload_id": None}, required=False))
    await fail("CONTROL_PAYLOAD_MISSING", hydrate(content, "result", {**row, "result_payload_digest": None}, required=False))


async def test_late_copy_and_new_operation_cannot_revive_deleted_plan_parent(content):
    repo, lease = content
    plan_input = await input_body(content, "plan", {"prompt": "PRIVATE-PLAN-CANARY"})
    plan = await result_body(content, plan_input, {"values": "PRIVATE-PLAN-CANARY"}, operation="plan")
    # A preview computation can happen outside the final metadata transaction.
    prepared = asyncio.Event()
    resume = asyncio.Event()

    async def late_writer():
        copied = await hydrate(content, "result", metadata("result", plan, operation="plan"))
        prepared.set()
        await resume.wait()
        async with repo.store.pool.connection() as conn, conn.transaction():
            await repo.store._host(conn, lease)
            original = await repo.put_input_in(conn, "late-apply", {}, action="config.apply", epoch=1)
            return await repo.put_result_in(
                conn, "late-apply", copied["result"], action="config.apply",
                input_payload_id=original.payload_id, parents=(plan.payload_id,), epoch=1,
            )

    task = asyncio.create_task(late_writer())
    try:
        await asyncio.wait_for(prepared.wait(), 5)
        await repo.payloads.forget_sources(lease, "forget-plan", (await domain(content, "plan"),), expected_deletion_epoch=0)
        resume.set()
        await fail("CONTROL_CONTENT_REVOKED", task)
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    # The failed final transaction rolls back its new independent input/domain,
    # not just the derived result. Erased old owners remain permanent receipts.
    assert len(await rows(content, "SELECT * FROM public.bot_payload_domains")) == 1
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 2
    await fail("CONTROL_CONTENT_REVOKED", input_body(
        content, "copied-new-input", {"prompt": "PRIVATE-PLAN-CANARY"}, parents=(plan.payload_id,), epoch=1,
    ))


async def test_current_source_scope_overflow_becomes_typed_unavailable(content, monkeypatch):
    repo, lease = content
    parent = await repo.payloads.put_json(
        lease, owner_kind="control.config", owner_id="multi-source", value="config",
        sources=(SourceRef("a"), SourceRef("b")), expected_deletion_epoch=0,
    )
    original = await input_body(content, parents=(parent.payload_id,))
    result = await result_body(content, original)
    monkeypatch.setattr("forest.storage.payloads.MAX_SOURCES", 2)
    row = metadata("result", result)
    unavailable = await hydrate(content, "result", row, required=False)
    assert unavailable["result_payload_state"] == "UNAVAILABLE" and unavailable["result"] is None
    await fail("CONTROL_CONTENT_REVOKED", hydrate(content, "result", row))
    await fail("CONTROL_CONTENT_REVOKED", result_body(content, original))
    # A bounded read failure does not invent a forget fact or erase receipts.
    assert len(await rows(content, "SELECT * FROM public.bot_payload_bodies")) == 3
    assert not await rows(content, "SELECT * FROM public.bot_payload_forget_requests")


async def test_control_domain_extension_keeps_database_kind_and_identity_constraints(content):
    await input_body(content)
    record = (await rows(content, "SELECT * FROM public.bot_payload_domains"))[0]
    assert record["kind"] == "CONTROL_INPUT"
    for statement in (
        "UPDATE public.bot_payload_domains SET kind='SERVICE_INPUT'",
        "DELETE FROM public.bot_payload_domains",
        "INSERT INTO public.bot_payload_domains(bot_id,kind,owner_id,source_unit_id) "
        "VALUES ('bot-a','ARBITRARY','bad','domain:sha256:" + "a" * 64 + "')",
    ):
        with pytest.raises(psycopg.Error):
            async with content[0].store.pool.connection() as conn:
                await conn.execute(statement)


async def test_input_and_result_require_actual_domain_and_matching_input_parent(content):
    repo, lease = content
    # These are trusted-DB writer mistakes, not an advertised remote exploit.
    # Syntactically correct owner/envelope/digest alone is not real provenance.
    fake_input = await repo.payloads.put_json(
        lease, owner_kind="control.input", owner_id=repo._owner("input", "unregistered"),
        value={"action": ACTION, "operation_id": "unregistered", "value": None},
        expected_deletion_epoch=0,
    )
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", result_body(
        content, fake_input, operation="unregistered",
    ))
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", hydrate(
        content, "input", metadata("input", fake_input, operation="unregistered"),
    ))
    await input_body(content)
    orphan_result = await repo.payloads.put_json(
        lease, owner_kind="control.result", owner_id=repo._owner("result", "preview"),
        value={"action": ACTION, "operation_id": "preview", "value": None},
        expected_deletion_epoch=0,
    )
    await fail("CONTROL_PAYLOAD_IDENTITY_CONFLICT", hydrate(
        content, "result", metadata("result", orphan_result),
    ))
