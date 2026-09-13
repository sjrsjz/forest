"""Configuration identity/provenance against a real migrated PostgreSQL DB.

These are helper tests, not a configuration-family cutover or upgrade claim.
Business authority is represented by the existing Host lease/transaction; the
actual parent sources are independently registered control input domains.
"""

import asyncio
import json

import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb
from test_bot_store import database as database
from test_bot_store import store as store

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.config_payloads import FAMILIES, KINDS, ConfigPayloadStore
from forest.storage.control_payloads import ControlPayloadStore

pytestmark = pytest.mark.postgres
DEFAULT = {"family": "config", "kind": "PLAN", "resource": "counter", "identity": "plan-a"}


@pytest_asyncio.fixture
async def content(store):
    return (ConfigPayloadStore(store), ControlPayloadStore(store),
            await store.acquire_host("config-test", "incarnation", ttl_seconds=300))


async def input_body(content, operation="input-a", *, value=None, epoch=0):
    repo, control, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        ref = await control.put_input_in(
            conn, operation, value, action="config.plan", epoch=epoch,
        )
        await repo.store._host(conn, lease)
        return ref


async def put(content, parents, values=None, *, epoch=0, **identity):
    repo, _, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        ref = await repo.put_values_in(
            conn, **(DEFAULT | identity), values=values, parents=parents, epoch=epoch,
        )
        await repo.store._host(conn, lease)
        return ref


def metadata(ref):
    return {
        "bot_id": "bot-a", "values_payload_id": ref.payload_id,
        "values_payload_digest": ref.digest,
        "values": {"old": "PRIVATE-OLD-VALUES"}, "defaults": "PRIVATE-DEFAULT",
        "secret_ciphertext": "PRIVATE-SECRET", "cached_values": "PRIVATE-CACHE",
    }


async def hydrate(content, row, *, epoch=0, required=True, **identity):
    repo, _, lease = content
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        result = await repo.hydrate_values_in(
            conn, row, **(DEFAULT | identity), epoch=epoch, required=required,
        )
        await repo.store._host(conn, lease)
        return result


async def rows(content, statement, args=()):
    async with content[0].store.pool.connection() as conn:
        return await (await conn.execute(statement, args)).fetchall()


async def source(content, operation="input-a"):
    _, control, _ = content
    found = await rows(
        content, "SELECT source_unit_id FROM public.bot_payload_domains "
        "WHERE bot_id=%s AND kind='CONTROL_INPUT' AND owner_id=%s",
        (control.store.bot_id, control._owner("input", operation)),
    )
    return SourceRef(found[0]["source_unit_id"])


async def fail(code, awaitable):
    with pytest.raises(ForestError) as caught:
        await awaitable
    assert caught.value.code == code
    assert "PRIVATE-" not in str(caught.value)


async def test_closed_families_and_kinds_have_distinct_bot_scoped_identities(content):
    original = await input_body(content)
    refs = []
    for family in sorted(FAMILIES):
        for kind in sorted(KINDS):
            key = {"family": family, "kind": kind, "resource": "", "identity": "0"}
            ref = await put(content, (original.payload_id,), {"label": "same"}, **key)
            refs.append(ref.payload_id)
            loaded = await hydrate(content, metadata(ref), **key)
            assert loaded == {
                **key, "values": {"label": "same"}, "values_payload_id": ref.payload_id,
                "values_payload_digest": ref.digest, "values_payload_state": "ACTIVE",
            }
    assert len(set(refs)) == len(FAMILIES) * len(KINDS)
    assert len(await rows(content, "SELECT * FROM public.bot_payload_domains")) == 1
    assert not await rows(content, "SELECT * FROM public.bot_config_plans")
    assert not await rows(content, "SELECT * FROM public.bot_web_operations")


async def test_long_identity_concurrent_replay_is_exact_and_does_not_guess_text_sources(content):
    original = await input_body(content)
    identity = "计划 \\\"/" + "长" * 1200
    resource = "component / \\\" " + "r" * 1000
    key = {"identity": identity, "resource": resource}
    values = {"nested": {"answer": [1, None, "line\n中文"]}}
    refs = await asyncio.gather(*(
        put(content, (original.payload_id, original.payload_id), values, **key)
        for _ in range(6)
    ))
    assert all(ref == refs[0] for ref in refs)
    record = (await rows(content, "SELECT * FROM public.bot_payload_headers WHERE payload_id=%s",
                         (refs[0].payload_id,)))[0]
    expected = DEFAULT | key | {"values": values}
    assert refs[0].digest == digest_json(expected)
    assert record["owner_id"] == ConfigPayloadStore.owner_id(**(DEFAULT | key))
    assert len(record["owner_id"].encode("utf-8")) <= 256
    assert record["manifest"]["parents"] == [original.payload_id]
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 2
    assert (await hydrate(content, metadata(refs[0]), **key))["values"] == values


async def test_two_actual_inputs_remain_independent_and_derived_revision_inherits_both(content):
    repo, _, lease = content
    a = await input_body(content, "input-a", value={"same": "text"})
    b = await input_body(content, "input-b", value={"same": "text"})
    plan_a = await put(content, (a.payload_id,), {"same": "text"})
    plan_b = await put(content, (b.payload_id,), {"same": "text"}, identity="plan-b")
    revision = await put(content, (plan_a.payload_id, plan_b.payload_id), {"same": "text"},
                         kind="REVISION", identity="1")
    head = (await rows(content, "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
                       (revision.payload_id,)))[0]
    assert {s["source_unit_id"] for s in head["manifest"]["sources"]} == {
        (await source(content, "input-a")).source_unit_id,
        (await source(content, "input-b")).source_unit_id,
    }
    await repo.payloads.forget_sources(
        lease, "erase-a", (await source(content),), expected_deletion_epoch=0,
    )
    await repo.payloads.scrub(lease)
    await fail("CONTROL_CONTENT_REVOKED", hydrate(content, metadata(plan_a), epoch=1))
    await fail("CONTROL_CONTENT_REVOKED", hydrate(
        content, metadata(revision), kind="REVISION", identity="1", epoch=1,
    ))
    assert (await hydrate(content, metadata(plan_b), identity="plan-b", epoch=1))["values"] == {"same": "text"}
    await fail("CONTROL_CONTENT_REVOKED", put(content, (a.payload_id,), {"same": "text"}, epoch=1))
    # Reusing an erased identity with a different still-live source is a
    # conflicting owner, not a way to wash old values into the new epoch.
    await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", put(content, (b.payload_id,), {"same": "text"}, epoch=1))
    await fail("CONTROL_CONTENT_REVOKED", put(
        content, (plan_a.payload_id, b.payload_id), {"same": "text"}, identity="late-plan", epoch=1,
    ))
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 5
    assert len(await rows(content, "SELECT * FROM public.bot_payload_bodies")) == 2


async def test_json_null_and_sql_null_and_erased_body_remain_distinct(content):
    repo, _, lease = content
    original = await input_body(content)
    ref = await put(content, (original.payload_id,), None)
    loaded = await hydrate(content, metadata(ref))
    assert loaded["values"] is None and loaded["values_payload_state"] == "ACTIVE"
    wire = (await rows(content, "SELECT content FROM public.bot_payload_bodies WHERE payload_id=%s",
                       (ref.payload_id,)))[0]
    assert json.loads(bytes(wire["content"]))["values"] is None
    # Both SQL NULL and JSON null decode as Python None. Only a real immutable
    # payload identity can prove the latter was admitted as configuration data.
    missing = (await rows(content, "SELECT NULL::text AS values_payload_id, "
                          "NULL::text AS values_payload_digest, 'null'::jsonb AS values"))[0]
    await fail("CONFIG_PAYLOAD_MISSING", hydrate(content, missing, required=False))
    await repo.payloads.forget_sources(
        lease, "erase-null", (await source(content),), expected_deletion_epoch=0,
    )
    await repo.payloads.scrub(lease)
    gone = await hydrate(content, metadata(ref), epoch=1, required=False)
    assert gone == {
        **DEFAULT, "values": None, "values_payload_id": ref.payload_id,
        "values_payload_digest": ref.digest, "values_payload_state": "UNAVAILABLE",
    }
    assert "PRIVATE-" not in json.dumps(gone)
    await fail("CONTROL_CONTENT_REVOKED", hydrate(content, metadata(ref), epoch=1))
    await fail("CONTROL_CONTENT_REVOKED", put(content, (original.payload_id,), None, epoch=1))
    assert not await rows(content, "SELECT * FROM public.bot_payload_bodies")


async def test_wrong_owner_envelope_digest_or_bot_is_not_unavailability(content):
    repo, _, lease = content
    original = await input_body(content)
    ref = await put(content, (original.payload_id,), {"label": "A"})
    for key in ({"family": "agent.config"}, {"kind": "INITIAL"},
                {"resource": "another"}, {"identity": "another"}):
        await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", hydrate(content, metadata(ref), **key))
    for replacement in (
        {"bot_id": "another-bot"}, {"values_payload_digest": "sha256:" + "0" * 64},
        {"values_payload_digest": "bad"},
        {"values_payload_id": original.payload_id, "values_payload_digest": original.digest},
    ):
        await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", hydrate(
            content, metadata(ref) | replacement, required=False,
        ))
    for replacement in ({"values_payload_id": None}, {"values_payload_digest": None}):
        await fail("CONFIG_PAYLOAD_MISSING", hydrate(
            content, metadata(ref) | replacement, required=False,
        ))
    # A trusted writer can create a correctly named payload with a wrong
    # envelope. Header/digest agreement alone must not authorize reading it.
    for number, envelope in enumerate((None, DEFAULT | {"values": 1, "extra": 2},
                                       DEFAULT | {"values": 1, "family": "watch"})):
        key = DEFAULT | {"identity": "bad-" + str(number)}
        invalid = await repo.payloads.put_json(
            lease, owner_kind="control.config", owner_id=repo.owner_id(**key), value=envelope,
            parents=(original.payload_id,), expected_deletion_epoch=0,
        )
        await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", hydrate(
            content, metadata(invalid), **key, required=False,
        ))


async def test_changed_values_or_parent_set_cannot_rebind_existing_identity(content):
    a = await input_body(content)
    b = await input_body(content, "input-b")
    await put(content, (a.payload_id,), {"v": 1})
    await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", put(content, (a.payload_id,), {"v": 2}))
    await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", put(content, (b.payload_id,), {"v": 1}))
    await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", put(content, (a.payload_id, b.payload_id), {"v": 1}))
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 3


async def test_no_parent_and_actual_parent_body_loss_are_fail_closed(content):
    repo, _, lease = content
    await fail("CONFIG_PAYLOAD_PARENTS_REQUIRED", put(content, (), {"v": 1}))
    orphan = await repo.payloads.put_json(
        lease, owner_kind="control.config", owner_id=repo.owner_id(**DEFAULT),
        value=DEFAULT | {"values": 1}, expected_deletion_epoch=0,
    )
    await fail("CONFIG_PAYLOAD_IDENTITY_CONFLICT", hydrate(content, metadata(orphan), required=False))
    original = await input_body(content)
    ref = await put(content, (original.payload_id,), {"v": 1}, identity="with-parent")
    # A trusted DB writer can leave a partial cleanup with the parent's body
    # gone but no source suppression. This is not PayloadStore.forget's normal
    # atomic path. Its legal immutable receipt must still make this child fail
    # closed; checking the child's flattened sources alone would miss it.
    target = [{"source_unit_id": (await source(content)).source_unit_id, "part_id": "*"}]
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        await repo.payloads.lock_in(conn)
        await conn.execute(
            "INSERT INTO public.bot_payload_forget_requests "
            "(bot_id,operation_id,input_digest,targets,deletion_epoch) VALUES (%s,%s,%s,%s,1)",
            (repo.store.bot_id, "partial-cleanup", digest_json([0, target]), Jsonb(target)),
        )
        await conn.execute("UPDATE public.bot_memory_clock SET deletion_epoch=1 WHERE bot_id=%s",
                           (repo.store.bot_id,))
        await conn.execute("UPDATE public.bot_payload_headers SET state='REVOKED', "
                           "forget_operation_id='partial-cleanup' "
                           "WHERE bot_id=%s AND payload_id=%s", (repo.store.bot_id, original.payload_id))
        await conn.execute("DELETE FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=%s",
                           (repo.store.bot_id, original.payload_id))
    assert (await rows(content, "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
                       (ref.payload_id,)))[0]["state"] == "ACTIVE"
    assert not await rows(content, "SELECT * FROM public.bot_payload_suppression")
    gone = await hydrate(content, metadata(ref), identity="with-parent", required=False, epoch=1)
    assert gone["values"] is None and gone["values_payload_state"] == "UNAVAILABLE"
    await fail("CONTROL_CONTENT_REVOKED", put(
        content, (original.payload_id,), {"v": 1}, identity="with-parent", epoch=1,
    ))


async def test_real_parent_limit_keeps_all_256_dependencies_and_rejects_257(content):
    repo, _, lease = content
    original = await input_body(content)
    parents = []
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        for number in range(256):
            ref = await repo.payloads.put_json_in(
                conn, owner_kind="control.result", owner_id="host-parent:" + str(number),
                value={"page": number}, parents=(original.payload_id,), expected_deletion_epoch=0,
            )
            parents.append(ref.payload_id)
    ref = await put(content, parents, {"v": 1})
    actual = await rows(content, "SELECT parent_payload_id FROM public.bot_payload_derivations "
                        "WHERE payload_id=%s", (ref.payload_id,))
    assert {row["parent_payload_id"] for row in actual} == set(parents)
    assert (await hydrate(content, metadata(ref)))["values"] == {"v": 1}
    await fail("PAYLOAD_DEPENDENCY_LIMIT", put(content, (*parents, original.payload_id), identity="overflow"))
    await fail("PAYLOAD_DEPENDENCY_LIMIT", put(content, (parents[0] for _ in range(257)), identity="duplicates"))
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 258


async def test_invalid_canonical_identities_fail_before_any_admission(content):
    original = await input_body(content)
    for key in (
        {"family": "arbitrary"}, {"kind": "revision"}, {"family": []},
        {"resource": "bad\x00resource"}, {"identity": "bad\x7fidentity"},
        {"identity": ""}, {"identity": "长" * 1366}, {"resource": "x" * 4097},
        {"identity": "\ud800"},
        *({"kind": "REVISION", "identity": v} for v in (1, "01", "+1", "-1", "1.0", "１", "1\n")),
    ):
        await fail("INVALID_CONFIG_IDENTITY", put(content, (original.payload_id,), **key))
    assert len(await rows(content, "SELECT * FROM public.bot_payload_headers")) == 1


async def test_mutating_caller_values_during_clock_wait_cannot_change_admitted_body(content, monkeypatch):
    repo, _, _ = content
    original = await input_body(content)
    waiting = asyncio.Event()
    resume = asyncio.Event()
    real_lock = repo.payloads.lock_in
    first = True

    async def held_lock(conn):
        nonlocal first
        if first:
            first = False
            waiting.set()
            await resume.wait()
        return await real_lock(conn)

    monkeypatch.setattr(repo.payloads, "lock_in", held_lock)
    values = {"nested": {"value": "authorized"}}
    task = asyncio.create_task(put(content, (original.payload_id,), values))
    try:
        await asyncio.wait_for(waiting.wait(), 5)
        values["nested"]["value"] = "PRIVATE-MUTATED"
        resume.set()
        ref = await task
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    loaded = await hydrate(content, metadata(ref))
    assert loaded["values"] == {"nested": {"value": "authorized"}}
    assert ref.digest == digest_json(DEFAULT | {"values": loaded["values"]})


async def test_new_epoch_and_scope_limit_never_turn_receipts_into_authorization(content, monkeypatch):
    repo, _, lease = content
    a = await input_body(content)
    b = await input_body(content, "input-b")
    ref = await put(content, (a.payload_id, b.payload_id), {"v": 1})
    monkeypatch.setattr("forest.storage.payloads.MAX_SOURCES", 1)
    unavailable = await hydrate(content, metadata(ref), required=False)
    assert unavailable["values_payload_state"] == "UNAVAILABLE"
    await fail("CONTROL_CONTENT_REVOKED", put(content, (a.payload_id, b.payload_id), {"v": 1}))
    monkeypatch.setattr("forest.storage.payloads.MAX_SOURCES", 4096)
    # An unrelated deletion also advances the required read/write epoch. An
    # existing owner's otherwise valid bytes do not bypass this fence.
    await repo.payloads.forget_sources(
        lease, "unrelated", (SourceRef("unrelated-source"),), expected_deletion_epoch=0,
    )
    await fail("DELETION_CONFLICT", hydrate(content, metadata(ref), epoch=0))
    await fail("DELETION_CONFLICT", put(content, (a.payload_id, b.payload_id), {"v": 1}, epoch=0))
    assert (await hydrate(content, metadata(ref), epoch=1))["values"] == {"v": 1}
    assert len(await rows(content, "SELECT * FROM public.bot_payload_bodies")) == 3
