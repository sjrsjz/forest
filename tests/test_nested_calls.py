"""Real database call-tree authority, dependency-drain, and race verification."""

import asyncio
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.storage.bot import BotStore

pytestmark = pytest.mark.postgres
CALLER, TARGET, LEAF = "z.caller", "a.target", "m.leaf"


async def activate(store, lease, component, suffix=""):
    generation = "gen-" + component + suffix
    await store.register_generation(
        lease, component, generation, "pkg", "cfg", "worker-" + generation
    )
    await store.mark_ready(lease, generation)
    current = await store.get_route(component)
    op = await store.begin_operation(
        lease,
        component,
        "install-" + generation,
        "plan-" + generation,
        generation,
        current["route_epoch"],
    )
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    return generation


@pytest_asyncio.fixture
async def environment():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_nested_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "bot-a")
    await store.open()
    try:
        await store.migrate()
        lease = await store.acquire_host("host", "incarnation")
        for component in (CALLER, TARGET, LEAF):
            await activate(store, lease, component)
        yield store, lease, uri
    finally:
        await store.close()
        assert name.startswith("forest_nested_") and len(name) == 46
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def parent(store, lease, name="root", timeout=30, dependencies=(TARGET, LEAF)):
    return await store.admit(
        lease,
        CALLER,
        name,
        "op-" + name,
        "input-" + name,
        timeout_seconds=timeout,
        dependency_components=dependencies,
    )


async def nested_call(
    store, parent_grant, name="nested", target=TARGET, dependencies=(LEAF,), timeout=60
):
    route = await store.get_route(target)
    return await store.admit_nested(
        parent_grant,
        target,
        name,
        "op-" + name,
        "input-" + name,
        timeout_seconds=timeout,
        expected_policy_revision=1,
        expected_generation_id=route["generation_id"],
        expected_worker_incarnation=route["worker_incarnation"],
        expected_route_epoch=route["route_epoch"],
        dependency_components=dependencies,
    )


async def state_commit(store, grant, step="step", revision=0):
    return await store.commit_state(grant, "count", revision, revision + 1, step, "digest-" + step)


async def rejected(code, action):
    with pytest.raises(ForestError) as caught:
        await action
    assert caught.value.code == code


async def wait_for_call_lock(store):
    for _ in range(200):
        async with store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                    "AND wait_event_type='Lock' AND query LIKE 'SELECT * FROM public.bot_calls%FOR UPDATE') AS waiting"
                )
            ).fetchone()
        if row["waiting"]:
            return
        await asyncio.sleep(0.005)
    pytest.fail("Expected operation did not reach its real call row lock")


async def test_nested_calls_share_deadline_and_parent_cannot_finish_early(environment):
    store, lease, _ = environment
    root = await parent(store, lease, timeout=5)
    nested = await nested_call(store, root)
    leaf = await nested_call(store, nested, "leaf", LEAF, ())
    assert root.call_depth == 0 and nested.call_depth == 1 and leaf.call_depth == 2
    assert leaf.root_call_id == nested.root_call_id == root.call_id
    assert leaf.parent_call_id == nested.call_id and nested.parent_call_id == root.call_id
    assert leaf.deadline_at <= nested.deadline_at <= root.deadline_at
    await rejected("NESTED_CALLS_PENDING", store.finish_call(root, "premature"))
    await rejected("NESTED_CALLS_PENDING", store.finish_call(nested, "premature"))
    await state_commit(store, leaf)
    await store.finish_call(leaf, {"value": 1})
    await store.finish_call(nested, {"nested_value": 1})
    await store.finish_call(root, {"final_value": 1})
    assert await store.inflight(TARGET) == 0
    assert (await store.read_call("op-leaf"))["parent_call_id"] == nested.call_id


async def test_parent_cancel_recursively_terminates_nested_calls_and_preserves_prior_facts(
    environment,
):
    store, lease, _ = environment
    root = await parent(store, lease)
    nested = await nested_call(store, root)
    leaf = await nested_call(store, nested, "leaf", LEAF, ())
    await state_commit(store, leaf)
    await store.cancel_call(lease, root.call_id)
    for grant in (root, nested, leaf):
        row = await store.read_call(grant.operation_id)
        assert row["status"] == "CANCELLED" and row["cancel_epoch"] == 1
        await rejected("CALL_CANCELLED", store.read_state(grant, "count"))
        await rejected("CALL_CANCELLED", state_commit(store, grant, "late", 1))
        await rejected("CALL_CANCELLED", store.finish_call(grant, "late"))
    assert (await store.snapshot_state(lease, LEAF))["count"] == {"revision": 1, "value": 1}
    assert await store.inflight(TARGET) == 0
    await rejected("CALL_CANCELLED", store.authorized_nested_call(root, nested.operation_id, 1))


@pytest.mark.parametrize("failure", ["call", "generation"])
async def test_failed_caller_terminates_descendants_without_requiring_target_failure(
    environment, failure
):
    store, lease, _ = environment
    root = await parent(store, lease)
    nested = await nested_call(store, root)
    leaf = await nested_call(store, nested, "leaf", LEAF, ())
    if failure == "call":
        await store.fail_call(lease, root.call_id, "CALLER_FAILED")
    else:
        await store.fail_generation(lease, root.generation_id, "process-exited")
    for grant in (root, nested, leaf):
        assert (await store.read_call(grant.operation_id))["status"] == "FAILED"
    assert (await store.get_route(LEAF))["admission"] is True
    await rejected(
        "CALL_TERMINAL" if failure == "call" else "STALE_ROUTE", state_commit(store, leaf)
    )


@pytest.mark.parametrize("action", ["read", "commit", "finish"])
async def test_leaf_operations_recheck_every_caller_route(environment, action):
    store, lease, _ = environment
    root = await parent(store, lease)
    nested = await nested_call(store, root)
    leaf = await nested_call(store, nested, "leaf", LEAF, ())
    # Fault injection changes only the caller fence; target rows and leaf token
    # remain unchanged, so validating the target alone would incorrectly pass.
    async with store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_component_routes SET route_epoch=route_epoch+1 WHERE bot_id=%s AND component_id=%s",
            (store.bot_id, CALLER),
        )
    if action == "read":
        request = store.read_state(leaf, "count")
    elif action == "commit":
        request = state_commit(store, leaf)
    else:
        request = store.finish_call(leaf, {"value": 1})
    await rejected("STALE_ROUTE", request)
    assert await store.inflight(LEAF) == 0


async def test_parent_deadline_bounds_descendant_work(environment):
    store, lease, _ = environment
    root = await parent(store, lease, timeout=0.2)
    nested = await nested_call(store, root)
    assert nested.deadline_at == root.deadline_at
    await asyncio.sleep(0.22)
    await rejected("CALL_EXPIRED", state_commit(store, nested))
    await rejected("CALL_EXPIRED", store.authorized_nested_call(root, nested.operation_id, 1))


async def test_nested_receipts_are_bound_to_live_parent_and_cannot_be_top_level(environment):
    store, lease, _ = environment
    root = await parent(store, lease)
    other = await parent(store, lease, "other")
    nested = await nested_call(store, root)
    await store.finish_call(nested, {"result": "saved"})
    assert (await store.authorized_nested_call(root, nested.operation_id, 1))["result"] == {
        "result": "saved"
    }
    await rejected(
        "IDEMPOTENCY_CONFLICT", store.authorized_nested_call(other, nested.operation_id, 1)
    )
    await rejected("NESTED_AUTHORITY_REQUIRED", store.authorized_call(lease, nested.operation_id, 1))
    await rejected(
        "IDEMPOTENCY_CONFLICT",
        store.admit(lease, TARGET, "top-replay", nested.operation_id, nested.input_digest),
    )
    await store.cancel_call(lease, root.call_id)
    await rejected("CALL_CANCELLED", store.authorized_nested_call(root, nested.operation_id, 1))


async def test_dependency_reservation_blocks_publish_before_a_nested_call_exists(environment):
    store, lease, _ = environment
    root = await parent(store, lease)
    new_generation = "replacement"
    await store.register_generation(
        lease, TARGET, new_generation, "pkg", "cfg", "replacement-worker"
    )
    await store.mark_ready(lease, new_generation)
    op = await store.begin_operation(lease, TARGET, "replace", "replace-input", new_generation, 1)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    assert await store.inflight(TARGET) == 1
    await rejected(
        "DRAIN_PENDING", store.quiesce_operation(lease, op["operation_id"], op["op_epoch"])
    )
    await rejected("DRAIN_PENDING", store.publish(lease, op["operation_id"], op["op_epoch"]))
    await rejected("ADMISSION_CLOSED", parent(store, lease, "new-root"))
    assert await store.read_call("op-new-root") is None
    nested = await nested_call(store, root)  # Existing parent reservation may enter QUIESCING.
    assert nested.generation_id == "gen-" + TARGET
    await state_commit(store, nested)
    await store.finish_call(nested, "done")
    await rejected(
        "DRAIN_PENDING", store.quiesce_operation(lease, op["operation_id"], op["op_epoch"])
    )
    await store.finish_call(root, "done")
    quiescent = await store.quiesce_operation(lease, op["operation_id"], op["op_epoch"])
    assert quiescent["phase"] == "QUIESCENT"
    assert (await store.get_route(TARGET))["generation_status"] == "QUIESCENT"
    await rejected("CALL_TERMINAL", nested_call(store, root, "too-late"))
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    fresh = await parent(store, lease, "fresh-root")
    assert (await nested_call(store, fresh, "fresh-nested")).generation_id == new_generation


async def test_undeclared_dependency_and_nested_expansion_are_rejected(environment):
    store, lease, _ = environment
    root = await parent(store, lease, dependencies=(TARGET,))
    await rejected("DEPENDENCY_DENIED", nested_call(store, root, target=LEAF, dependencies=()))
    await rejected("DEPENDENCY_DENIED", nested_call(store, root, dependencies=(LEAF,)))
    await rejected("CALL_CYCLE", nested_call(store, root, target=CALLER, dependencies=()))
    assert await store.read_call("op-nested") is None


async def test_depth_limit_and_tree_metadata_survive_reopen(environment):
    store, lease, uri = environment
    components = tuple("chain-" + str(i) for i in range(9))
    for component in components:
        await activate(store, lease, component)
    root = await parent(store, lease, dependencies=components)
    current = root
    for index, component in enumerate(components[:8]):
        current = await nested_call(
            store, current, "nested-" + str(index), component, components[index + 1 :]
        )
    assert current.call_depth == 8
    await rejected(
        "CALL_DEPTH_EXCEEDED", nested_call(store, current, "too-deep", components[8], ())
    )
    reopened = BotStore(uri, "bot-a")
    await reopened.open()
    try:
        await reopened.migrate()
        assert (await reopened.read_call(current.operation_id))["root_call_id"] == root.call_id
        await state_commit(reopened, current)
        await reopened.cancel_call(lease, root.call_id)
        await rejected("CALL_CANCELLED", state_commit(store, current, "late", 1))
    finally:
        await reopened.close()


async def test_nested_admission_outbox_failure_rolls_back_without_consuming_id(environment):
    store, lease, _ = environment
    root = await parent(store, lease)
    async with store.pool.connection() as conn:
        await conn.execute("""
            CREATE FUNCTION public.fail_nested_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.topic='call.admitted' AND NEW.payload ? 'parent_call_id'
              THEN RAISE EXCEPTION 'nested admission injection'; END IF; RETURN NEW; END; $$;
            CREATE TRIGGER fail_nested_outbox BEFORE INSERT ON public.bot_outbox_events
              FOR EACH ROW EXECUTE FUNCTION public.fail_nested_outbox();
        """)
    with pytest.raises(psycopg.errors.RaiseException, match="nested admission injection"):
        await nested_call(store, root)
    assert await store.read_call("op-nested") is None
    async with store.pool.connection() as conn:
        await conn.execute("DROP TRIGGER fail_nested_outbox ON public.bot_outbox_events")
    assert (await nested_call(store, root)).parent_call_id == root.call_id


@pytest.mark.parametrize("first", ["finish", "nested"])
async def test_parent_finish_and_nested_admission_have_one_transaction_order(environment, first):
    store, lease, uri = environment
    root = await parent(store, lease)
    blocker = await psycopg.AsyncConnection.connect(uri, autocommit=True)
    tasks = []
    try:
        async with blocker.transaction():
            await blocker.execute(
                "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s FOR UPDATE",
                (store.bot_id, root.call_id),
            )
            if first == "finish":
                initial = asyncio.create_task(store.finish_call(root, "finished"))
            else:
                initial = asyncio.create_task(nested_call(store, root))
            tasks.append(initial)
            await wait_for_call_lock(store)
            if first == "finish":
                second = asyncio.create_task(nested_call(store, root))
            else:
                second = asyncio.create_task(store.finish_call(root, "finished"))
            tasks.append(second)
        result = await initial
        if first == "finish":
            assert result["status"] == "COMPLETED"
            await rejected("CALL_TERMINAL", second)
            assert await store.read_call("op-nested") is None
        else:
            assert result.parent_call_id == root.call_id
            await rejected("NESTED_CALLS_PENDING", second)
            assert (await store.read_call(root.operation_id))["status"] == "ADMITTED"
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await blocker.close()


async def test_parent_cancel_wins_against_leaf_commit_waiting_on_database_locks(environment):
    store, lease, uri = environment
    root = await parent(store, lease)
    nested = await nested_call(store, root)
    blocker = await psycopg.AsyncConnection.connect(uri, autocommit=True)
    tasks = []
    try:
        async with blocker.transaction():
            await blocker.execute(
                "SELECT * FROM public.bot_calls WHERE bot_id=%s AND call_id=%s FOR UPDATE",
                (store.bot_id, root.call_id),
            )
            cancellation = asyncio.create_task(store.cancel_call(lease, root.call_id))
            tasks.append(cancellation)
            await wait_for_call_lock(store)
            write = asyncio.create_task(state_commit(store, nested))
            tasks.append(write)
        assert (await cancellation)["status"] == "CANCELLED"
        await rejected("CALL_CANCELLED", write)
        assert await store.snapshot_state(lease, TARGET) == {}
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await blocker.close()
