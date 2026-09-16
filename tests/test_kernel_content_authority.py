"""Actual BotStore gates attach content before delivery and inherit Worker history."""

import asyncio
import json

import pytest
import pytest_asyncio
from test_bot_store import activate, candidate, operation
from test_bot_store import database as database
from test_bot_store import store as store

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.exposures import ExposureCapacityError, ExposureStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def authority(store):
    lease = await store.acquire_host("host", "content-incarnation", ttl_seconds=300)
    await activate(store, lease)
    return store, lease


async def rows(store, query, parameters=()):
    async with store.pool.connection() as conn:
        return await (await conn.execute(query, parameters)).fetchall()


async def source(store, lease, name):
    return await PayloadStore(store).put_json(
        lease,
        owner_kind="memory.source",
        owner_id="upstream:" + name,
        value={"text": "SOURCE-CANARY-" + name},
        sources=(SourceRef("source:" + name),),
        expected_deletion_epoch=0,
    )


async def admit(
    store, lease, name, value=None, *, parents=(), component="counter", dependencies=()
):
    return await store.admit(
        lease,
        component,
        "call:" + name,
        "operation:" + name,
        digest_json(value),
        input_value=value,
        input_parent_ids=parents,
        dependency_components=dependencies,
        timeout_seconds=120,
    )


async def manifest(store, payload_id):
    return (
        await rows(
            store,
            "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
            (payload_id,),
        )
    )[0]["manifest"]


async def denied(awaitable, *codes):
    with pytest.raises(ForestError) as exc:
        await awaitable
    assert exc.value.code in codes
    return exc.value


@pytest.mark.parametrize("value", [None, {"parts": [{"type": "text", "text": "INPUT-CANARY"}]}])
async def test_every_host_input_has_real_payload_including_json_null(authority, value):
    store, lease = authority
    grant = await admit(store, lease, "input", value)
    call = await store.read_call(grant.operation_id)
    assert call["input_payload_id"] and not call["input_legacy_unknown"]
    assert (
        await PayloadStore(store).read_json(
            lease, call["input_payload_id"], expected_deletion_epoch=0
        )
        == value
    )
    body = (
        await rows(
            store,
            "SELECT content FROM public.bot_payload_bodies WHERE payload_id=%s",
            (call["input_payload_id"],),
        )
    )[0]
    if value is None:
        assert bytes(body["content"]) == b"null"
    exposure = await rows(
        store,
        "SELECT * FROM public.bot_generation_exposures WHERE generation_id=%s AND kind='CALL_INPUT'",
        (grant.generation_id,),
    )
    assert len(exposure) == 1 and exposure[0]["payload_refs"] == [call["input_payload_id"]]
    completed = await store.finish_call(grant, None)
    assert completed["status"] == "COMPLETED" and completed["result_payload_id"]
    assert completed["result"] is None and completed["payload_state"] == "ACTIVE"
    replay = await store.authorized_call(lease, grant.operation_id, grant.policy_revision)
    assert (
        replay["result"] is None and replay["result_payload_id"] == completed["result_payload_id"]
    )


async def test_independent_calls_share_generation_sources_and_forget_blocks_every_late_commit(
    authority,
):
    store, lease = authority
    a, b = await source(store, lease, "A"), await source(store, lease, "B")
    first, second = await asyncio.gather(
        admit(store, lease, "a", {"text": "SOURCE-CANARY-A"}, parents=(a.payload_id,)),
        admit(store, lease, "b", {"text": "SOURCE-CANARY-B"}, parents=(b.payload_id,)),
    )
    assert first.parent_call_id is None and second.parent_call_id is None
    assert first.generation_id == second.generation_id
    state = await store.commit_state(second, "cached", 0, {"text": "A+B"}, "write", "input-write")
    complete = await store.finish_call(first, {"text": "A+B"})
    current = (await rows(store, "SELECT * FROM public.bot_component_state WHERE key='cached'"))[0]
    history = (await rows(store, "SELECT * FROM public.bot_state_revisions WHERE key='cached'"))[0]
    receipt = (
        await rows(store, "SELECT * FROM public.bot_state_steps WHERE operation_id='write'")
    )[0]
    assert state["revision"] == 1
    assert current["value"] is None and history["value"] is None and receipt["receipt"] is None
    assert current["value_payload_id"] == history["value_payload_id"] == receipt["value_payload_id"]
    for payload_id in (current["value_payload_id"], complete["result_payload_id"]):
        dependencies = await manifest(store, payload_id)
        assert {s["source_unit_id"] for s in dependencies["sources"]} >= {"source:A", "source:B"}
        assert {first.call_id, second.call_id} == {
            r["call_id"]
            for r in await rows(
                store,
                "SELECT call_id FROM public.bot_calls WHERE input_payload_id=ANY(%s)",
                (dependencies["parents"],),
            )
        }
    payloads = PayloadStore(store)
    await payloads.forget_sources(
        lease, "forget-A", (SourceRef("source:A"),), expected_deletion_epoch=0
    )
    before = await rows(store, "SELECT * FROM public.bot_payload_headers ORDER BY payload_id")
    await denied(store.read_state(second, "cached"), "GENERATION_EXPOSURE_REVOKED")
    await denied(
        store.commit_state(second, "cached", 1, {"text": "SOURCE-CANARY-A"}, "late", "late"),
        "GENERATION_EXPOSURE_REVOKED",
    )
    await denied(
        store.finish_call(second, {"text": "SOURCE-CANARY-A"}), "GENERATION_EXPOSURE_REVOKED"
    )
    await denied(
        admit(store, lease, "new-epoch", {"text": "SOURCE-CANARY-A"}), "GENERATION_EXPOSURE_REVOKED"
    )
    await denied(
        store.authorized_call(lease, first.operation_id, first.policy_revision),
        "KERNEL_CONTENT_REVOKED",
    )
    assert (
        await rows(store, "SELECT * FROM public.bot_payload_headers ORDER BY payload_id") == before
    )
    assert (await store.read_call(first.operation_id))["status"] == "COMPLETED"
    assert (await store.read_call(second.operation_id))["status"] == "ADMITTED"
    assert await rows(store, "SELECT * FROM public.bot_component_state WHERE key='cached'") == [
        current
    ]
    while (await payloads.scrub(lease))["remaining"]:
        pass
    assert not await rows(
        store,
        "SELECT 1 FROM public.bot_payload_bodies WHERE position(%s::bytea in content)>0",
        (b"SOURCE-CANARY-A",),
    )
    assert await payloads.read_json(lease, b.payload_id, expected_deletion_epoch=1) == {
        "text": "SOURCE-CANARY-B"
    }


async def test_nested_state_read_and_exact_result_are_exposed_to_parent_before_return(authority):
    store, lease = authority
    await candidate(store, lease, generation="child-generation", component="child")
    op = await operation(store, lease, generation="child-generation", component="child")
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    origin = await source(store, lease, "child-state")
    seed = await admit(
        store,
        lease,
        "seed",
        {"text": "child-state"},
        component="child",
        parents=(origin.payload_id,),
    )
    await store.commit_state(seed, "key", 0, {"secret": "CHILD-CANARY"}, "seed-step", "seed-digest")
    await store.finish_call(seed, {"seeded": True})
    value = (
        await rows(
            store,
            "SELECT value_payload_id FROM public.bot_component_state WHERE component_id='child' AND key='key'",
        )
    )[0]
    parent = await admit(store, lease, "parent", {"question": "read"}, dependencies=("child",))
    before = (
        await rows(
            store,
            "SELECT * FROM public.bot_generation_exposure_heads WHERE generation_id=%s",
            (parent.generation_id,),
        )
    )[0]
    child = await store.admit_nested(
        parent, "child", "child-call", "child-operation", "child-input", input_value={"key": "key"}
    )
    assert await store.read_state(child, "key") == {
        "revision": 1,
        "value": {"secret": "CHILD-CANARY"},
    }
    observed = await rows(
        store,
        "SELECT payload_refs FROM public.bot_generation_exposures WHERE generation_id='child-generation' AND kind='STATE_READ'",
    )
    assert observed == [{"payload_refs": [value["value_payload_id"]]}]
    completed = await store.finish_call(child, {"secret": "CHILD-CANARY"})
    assert (
        value["value_payload_id"]
        in (await manifest(store, completed["result_payload_id"]))["parents"]
    )
    exposures = await rows(
        store,
        "SELECT * FROM public.bot_generation_exposures WHERE generation_id=%s AND kind='NESTED_RESULT'",
        (parent.generation_id,),
    )
    assert len(exposures) == 1 and exposures[0]["payload_refs"] == [completed["result_payload_id"]]
    after = (
        await rows(
            store,
            "SELECT * FROM public.bot_generation_exposure_heads WHERE generation_id=%s",
            (parent.generation_id,),
        )
    )[0]
    assert after["revision"] == before["revision"] + 1
    replay = await store.authorized_nested_call(parent, child.operation_id, parent.policy_revision)
    assert (
        replay["result_payload_id"] == completed["result_payload_id"]
        and replay["result"] == completed["result"]
    )
    assert (
        await rows(
            store,
            "SELECT * FROM public.bot_generation_exposure_heads WHERE generation_id=%s",
            (parent.generation_id,),
        )
    )[0] == after
    finished = await store.finish_call(parent, {"answer": "CHILD-CANARY"})
    dependencies = await manifest(store, finished["result_payload_id"])
    assert completed["result_payload_id"] in dependencies["parents"]
    assert "source:child-state" in {r["source_unit_id"] for r in dependencies["sources"]}
    await PayloadStore(store).forget_sources(
        lease, "forget-child", (SourceRef("source:child-state"),), expected_deletion_epoch=0
    )
    await denied(
        store.authorized_call(lease, parent.operation_id, parent.policy_revision),
        "KERNEL_CONTENT_REVOKED",
    )


@pytest.mark.parametrize("concurrent_known_read", [False, True])
async def test_public_capacity_failure_invalidates_after_input_rollback_and_existing_call_drains(
    authority,
    monkeypatch,
    concurrent_known_read,
):
    store, lease = authority
    live = await admit(store, lease, "draining", {"work": "already-admitted"})
    exposures = ExposureStore(store)
    async with store.pool.connection() as conn, conn.transaction():
        await store._routes(conn, ["counter"])
        await store._host(conn, lease)
        head = await exposures.ensure_generation_in(conn, live.generation_id)
        payloads = PayloadStore(store)
        epoch = (await payloads.lock_in(conn))["deletion_epoch"]
        refs = []
        for i in range(256 - head["payload_count"]):
            ref = await payloads.put_json_in(
                conn,
                owner_kind="state.value",
                owner_id=f"delivered-{i}",
                value=i,
                expected_deletion_epoch=epoch,
            )
            refs.append(ref.payload_id)
        await exposures.expose_in(
            conn, live.generation_id, "full-restore", "RESTORE_STATE", refs, "snapshot"
        )
    original = ExposureStore.invalidate
    invalidation_attempts = 0

    async def racing_invalidation(self, *args, **kwargs):
        nonlocal invalidation_attempts
        invalidation_attempts += 1
        if concurrent_known_read and invalidation_attempts == 1:
            # The failed public admission transaction has already rolled back.
            # A real receipt for an existing member can commit before its
            # independent capacity decision, advancing the CAS revision.
            async with store.pool.connection() as conn, conn.transaction():
                await store._routes(conn, ["counter"])
                await store._host(conn, lease)
                await exposures.expose_in(
                    conn,
                    live.generation_id,
                    "concurrent-read",
                    "STATE_READ",
                    refs[:1],
                    "known-value",
                )
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(ExposureStore, "invalidate", racing_invalidation)
    exc = await denied(
        admit(store, lease, "overflow", {"text": "ROLLED-BACK-CANARY"}), "GENERATION_EXPOSURE_LIMIT"
    )
    assert exc._candidate_payload_refs
    assert "candidate_payload_refs" not in exc.as_dict()["details"]
    assert invalidation_attempts == (2 if concurrent_known_read else 1)
    assert not await rows(store, "SELECT 1 FROM public.bot_calls WHERE call_id='call:overflow'")
    assert not await rows(
        store,
        "SELECT 1 FROM public.bot_payload_headers WHERE payload_id=ANY(%s)",
        (list(exc._candidate_payload_refs),),
    )
    assert not await rows(
        store,
        "SELECT 1 FROM public.bot_payload_domains WHERE kind='SERVICE_INPUT' AND owner_id='call:overflow'",
    )
    head = (
        await rows(
            store,
            "SELECT * FROM public.bot_generation_exposure_heads WHERE generation_id=%s",
            (live.generation_id,),
        )
    )[0]
    assert head["status"] == "EXHAUSTED" and head["payload_count"] == 256
    assert (await exposures.scan(lease))["generations"][0]["generation_id"] == live.generation_id
    written = await store.commit_state(
        live, "drained", 0, {"completed": True}, "drain-step", "drain-digest"
    )
    assert written["revision"] == 1
    complete = await store.finish_call(live, {"completed": True})
    assert complete["status"] == "COMPLETED" and complete["payload_state"] == "ACTIVE"
    await denied(admit(store, lease, "still-closed", None), "GENERATION_EXPOSURE_EXHAUSTED")


async def wide_source(store, lease, name, sources):
    return await PayloadStore(store).put_json(
        lease,
        owner_kind="memory.source",
        owner_id=name,
        value={"text": "CONTENT-" + name},
        sources=sources,
        expected_deletion_epoch=0,
    )


@pytest.mark.parametrize("concurrent_known_read", [False, True])
async def test_disjoint_2500_source_inputs_reject_second_delivery_then_old_call_drains(
    authority,
    monkeypatch,
    concurrent_known_read,
):
    store, lease = authority
    payloads = PayloadStore(store)
    left = await wide_source(
        store, lease, "left-2500", tuple(SourceRef(f"left-{i:04}") for i in range(2500))
    )
    right = await wide_source(
        store, lease, "right-2500", tuple(SourceRef(f"right-{i:04}") for i in range(2500))
    )
    # Both candidate bodies are independently usable; only the complete Worker
    # source union is over budget. The second Call must never reach the Worker.
    for ref in (left, right):
        assert await payloads.read_json(lease, ref.payload_id, expected_deletion_epoch=0)
    live = await admit(store, lease, "wide-left", {"use": "left"}, parents=(left.payload_id,))
    before = (await rows(store, "SELECT * FROM public.bot_generation_exposure_heads"))[0]
    original = ExposureStore.invalidate
    attempts = 0

    async def race(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if concurrent_known_read and attempts == 1:
            call = await store.read_call(live.operation_id)
            async with store.pool.connection() as conn, conn.transaction():
                await store._routes(conn, ["counter"])
                await store._host(conn, lease)
                await self.expose_in(
                    conn,
                    live.generation_id,
                    "repeat-wide-left",
                    "STATE_READ",
                    (call["input_payload_id"],),
                    "already-known",
                )
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(ExposureStore, "invalidate", race)
    error = await denied(
        admit(store, lease, "wide-right", {"text": "ROLLBACK-CANARY"}, parents=(right.payload_id,)),
        "GENERATION_EXPOSURE_LIMIT",
    )
    assert type(error) is ExposureCapacityError and error._reason == "SOURCE_CAPACITY"
    assert 1 <= len(error._candidate_source_refs) <= 4096
    public = json.dumps(error.as_dict())
    assert len(public) < 512 and "right-" not in public and "ROLLBACK-CANARY" not in public
    assert "candidate_source_refs" not in public and "candidate_payload_refs" not in public
    assert attempts == (2 if concurrent_known_read else 1)
    assert not await rows(store, "SELECT 1 FROM public.bot_calls WHERE call_id='call:wide-right'")
    assert not await rows(
        store,
        "SELECT 1 FROM public.bot_payload_headers WHERE payload_id=ANY(%s)",
        (list(error._candidate_payload_refs),),
    )
    head = (await rows(store, "SELECT * FROM public.bot_generation_exposure_heads"))[0]
    assert head["status"] == "EXHAUSTED" and head["reason_code"] == "SOURCE_CAPACITY"
    assert head["payload_count"] == before["payload_count"]
    assert head["revision"] == before["revision"] + int(concurrent_known_read)
    # Valid old sources remain derivable during drain, including state writes.
    await store.commit_state(live, "wide-finish", 0, {"ok": True}, "wide-step", "wide-digest")
    result = await store.finish_call(live, {"done": "left"})
    result_sources = (await manifest(store, result["result_payload_id"]))["sources"]
    assert len(result_sources) == 2502  # 2500 + real config/input source domains.
    assert not any(source["source_unit_id"].startswith("right-") for source in result_sources)
    await denied(admit(store, lease, "cannot-resume"), "GENERATION_EXPOSURE_EXHAUSTED")


async def test_overlapping_2500_source_inputs_merge_and_finish_without_false_capacity(authority):
    store, lease = authority
    shared = tuple(SourceRef(f"shared-wide-{i:04}") for i in range(2500))
    left = await wide_source(store, lease, "shared-left", shared)
    right = await wide_source(store, lease, "shared-right", shared)
    a = await admit(store, lease, "shared-a", parents=(left.payload_id,))
    b = await admit(store, lease, "shared-b", parents=(right.payload_id,))
    exposures = ExposureStore(store)
    head = (await rows(store, "SELECT * FROM public.bot_generation_exposure_heads"))[0]
    await denied(
        exposures.invalidate(
            lease,
            a.generation_id,
            head["revision"],
            reason="SOURCE_CAPACITY",
            candidate_source_refs=shared,
        ),
        "EXPOSURE_INVALIDATION_UNPROVEN",
    )
    await store.commit_state(a, "shared-state", 0, {"ok": True}, "shared-step", "shared-digest")
    for grant in (a, b):
        finished = await store.finish_call(grant, {"ok": True})
        assert len((await manifest(store, finished["result_payload_id"]))["sources"]) == 2503
    assert (await ExposureStore(store).scan(lease))["generations"] == []


async def test_public_lookalike_capacity_error_cannot_supply_host_proof(authority, monkeypatch):
    from forest.storage.kernel_content import capacity_boundary

    store, lease = authority
    called = False

    async def invalidate(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("Public error fields must never reach supervisor invalidation")

    @capacity_boundary
    async def worker_error(self, authority):
        raise ForestError(
            "GENERATION_EXPOSURE_LIMIT",
            "untrusted",
            details={
                "generation_id": "gen-1",
                "revision": 1,
                "candidate_source_refs": ["forged"],
            },
        )

    monkeypatch.setattr(ExposureStore, "invalidate", invalidate)
    await denied(worker_error(store, lease), "GENERATION_EXPOSURE_LIMIT")
    assert not called
