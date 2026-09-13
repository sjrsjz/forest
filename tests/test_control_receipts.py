"""Real target authority and exact 041 event/plan references, including erasure races."""

import asyncio
from datetime import timedelta

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_web_authority import COMPONENT, VALUES, wait_for_lock
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.control_receipts import ControlReceiptStore

pytestmark = pytest.mark.postgres


async def rows(env, query, args=()):
    async with env.store.pool.connection() as conn:
        return await (await conn.execute(query, args)).fetchall()


async def operation(env, op):
    return (
        await rows(env, "SELECT * FROM public.bot_web_operations WHERE operation_id=%s", (op,))
    )[0]


async def forget(env, op):
    owner = env.control.content._owner("input", op)
    source = (
        await rows(
            env,
            "SELECT source_unit_id FROM public.bot_payload_domains WHERE kind='CONTROL_INPUT' AND owner_id=%s",
            (owner,),
        )
    )[0]
    await env.control.content.payloads.forget_sources(
        env.lease, "forget:" + op, (SourceRef(source["source_unit_id"]),), expected_deletion_epoch=0
    )
    await env.control.content.payloads.scrub(env.lease)


async def test_real_plan_apply_events_share_receipt_and_erased_replay_never_reexecutes(environment):
    env = environment
    request = {"component_id": COMPONENT, "expected_revision": 0, "values": VALUES}
    results = await asyncio.gather(*(env.plan(operation="preview") for _ in range(4)))
    assert all(value == results[0] for value in results)
    plan = results[0]
    applied = await env.apply(plan, "apply")
    original, result = await operation(env, "preview"), await operation(env, "apply")
    assert original["result"] is None and result["result"] is None
    assert original["input_digest"] == digest_json(request)
    assert not original["input_legacy_unknown"]
    header = (
        await rows(
            env,
            "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
            (result["result_payload_id"],),
        )
    )[0]
    assert set(header["manifest"]["parents"]) == {
        result["input_payload_id"],
        original["result_payload_id"],
    }
    for table, body, kind in [
        ("bot_audit_records", "details", "audit"),
        ("bot_outbox_events", "payload", "outbox"),
    ]:
        events = await rows(env, f"SELECT * FROM public.{table} WHERE operation_id='preview'")
        assert len(events) == 1 and events[0][body] is None
        assert events[0]["control_result_payload_id"] == original["result_payload_id"]
        async with env.store.pool.connection() as conn, conn.transaction():
            hydrated = await ControlReceiptStore(env.store).hydrate_event_in(
                conn, events[0], kind=kind
            )
        assert hydrated[body] == {"subject_id": env.subject, **plan}
    await forget(env, "preview")
    for op, action, invoke in [
        ("preview", "config.plan", env.plan(operation="preview")),
        ("apply", "config.apply", env.apply(plan, "apply")),
    ]:
        assert await invoke == {
            "operation_id": op,
            "action": action,
            "result_payload_state": "UNAVAILABLE",
            "operation_status": "RECORDED",
        }
    with pytest.raises(ForestError) as caught:
        await env.apply(plan, "fresh-apply")
    assert caught.value.code == "REVISION_CONFLICT"
    assert len(await rows(env, "SELECT * FROM public.bot_web_operations")) == 2
    assert (
        (await rows(env, "SELECT desired_revision FROM public.bot_config_heads"))[0][
            "desired_revision"
        ]
        == applied["desired_revision"]
        == 1
    )
    for table, kind, body in [
        ("bot_audit_records", "audit", "details"),
        ("bot_outbox_events", "outbox", "payload"),
    ]:
        event = (await rows(env, f"SELECT * FROM public.{table} WHERE operation_id='preview'"))[0]
        async with env.store.pool.connection() as conn, conn.transaction():
            hydrated = await ControlReceiptStore(env.store).hydrate_event_in(
                conn, event, kind=kind, required=False
            )
        assert hydrated[body] is None and hydrated["control_payload_state"] == "UNAVAILABLE"


@pytest.mark.parametrize("mode", ["plan", "replay", "apply"])
async def test_clock_wait_expiry_cannot_record_replay_or_advance_revision(environment, mode):
    env = environment
    if mode == "apply":
        plan = await env.plan(operation="before")
        request = {
            "component_id": COMPONENT,
            **{k: plan[k] for k in ("expected_revision", "plan_id", "plan_digest")},
        }
        action, method = "config.apply", env.control.apply
    else:
        request = {"component_id": COMPONENT, "expected_revision": 0, "values": VALUES}
        action, method = "config.plan", env.control.plan
        if mode == "replay":
            await env.plan(operation="waiting")
    initial = await rows(
        env, "SELECT operation_id FROM public.bot_web_operations ORDER BY operation_id"
    )
    ticket = await env.ticket(action, "waiting", request)
    async with env.store.pool.connection() as blocker, blocker.transaction():
        await env.control.content.payloads.lock_in(blocker)
        now = await env.store._now(blocker)
        claims = {
            **ticket["claims"],
            "issued_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=1)).isoformat(),
        }
        pending = asyncio.create_task(method(env.sign(claims), "waiting", request))
        await wait_for_lock(env.store, "%bot_memory_clock%")
        await asyncio.sleep(
            max(0, (now + timedelta(seconds=1.05) - await env.store._now(blocker)).total_seconds())
        )
    with pytest.raises(ForestError) as caught:
        await pending
    assert caught.value.code == "WEB_AUTHORITY_REJECTED"
    assert (
        await rows(env, "SELECT operation_id FROM public.bot_web_operations ORDER BY operation_id")
        == initial
    )
    heads = await rows(env, "SELECT desired_revision FROM public.bot_config_heads")
    assert not heads or heads[0]["desired_revision"] == 0


async def test_event_reference_facts_cannot_be_substituted_or_replaced_by_plaintext(environment):
    env = environment
    plan = await env.plan(operation="preview")
    original = await operation(env, "preview")

    async def invalid(query, args):
        with pytest.raises(psycopg.Error):
            async with env.store.pool.connection() as conn, conn.transaction():
                await conn.execute(query, args)

    await invalid(
        "INSERT INTO public.bot_audit_records(bot_id,audit_id,action,operation_id,details) VALUES (%s,'plaintext','config.plan','preview',%s)",
        (env.store.bot_id, Jsonb({"subject_id": env.subject, **plan})),
    )
    await invalid(
        "UPDATE public.bot_outbox_events SET control_result_payload_id=%s WHERE operation_id='preview'",
        (original["input_payload_id"],),
    )
    await invalid(
        "INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload,control_subject_id,control_result_payload_id,control_result_payload_digest) VALUES (%s,'wrong','config.apply','preview',NULL,%s,%s,%s)",
        (
            env.store.bot_id,
            env.subject,
            original["result_payload_id"],
            original["result_payload_digest"],
        ),
    )
    async with env.store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE public.bot_outbox_events SET delivered_at=clock_timestamp() WHERE operation_id='preview'"
        )
    assert (
        await rows(
            env, "SELECT delivered_at FROM public.bot_outbox_events WHERE operation_id='preview'"
        )
    )[0]["delivered_at"] is not None


async def test_invocation_admission_returns_actual_transitive_input_parent_and_null_is_replay(
    environment,
):
    env = environment
    request = {
        "component_id": COMPONENT,
        "service": "counter.increment",
        "payload": {"increment": 3},
    }
    ticket = await env.ticket("service.invoke", "invoke", request)
    accepted, parents = await env.control.admit_invocation(
        ticket, "invoke", request, return_parents=True
    )
    actual = await operation(env, "invoke")
    assert accepted["status"] == "ACCEPTED" and parents == (actual["result_payload_id"],)
    assert actual["result"] is None
    # A genuine JSON null result is recorded with the same authorized host path.
    signed = await env.ticket("service.invoke", "null", request)
    async with env.store.pool.connection() as conn, conn.transaction():
        claims = await env.control._guard(conn, signed, "service.invoke", "null", request)
        await env.control._record(conn, claims, None)
    assert await env.control.admit_invocation(signed, "null", request, return_parents=True) == (
        None,
        (),
    )
    await forget(env, "invoke")
    unavailable, parents = await env.control.admit_invocation(
        ticket, "invoke", request, return_parents=True
    )
    assert env.control.is_unavailable(unavailable) and parents == ()
    assert len(await rows(env, "SELECT * FROM public.bot_web_operations")) == 2


@pytest.mark.parametrize("family", ["agent", "memory"])
async def test_current_and_before_values_require_actual_revision_source_without_storing_raw_secret(
    environment, family
):
    from importlib import import_module

    helpers = import_module("test_" + family + "_config")
    env = environment
    secret = "RAW-CREDENTIAL-MUST-STAY-OUT-OF-CONTROL-PAYLOADS"
    plan = await helpers.plan(env, secret=secret, operation="first-plan")
    await helpers.apply(env, plan, operation="first-apply")
    preview = await helpers.plan(env, revision=1, operation="next-plan")
    assert preview["before_values"] == plan["values"]
    original = await operation(env, "first-apply")
    follow = await operation(env, "next-plan")
    header = (
        await rows(
            env,
            "SELECT manifest FROM public.bot_payload_headers WHERE payload_id=%s",
            (follow["result_payload_id"],),
        )
    )[0]
    assert original["result_payload_id"] in header["manifest"]["parents"]
    bodies = await rows(
        env,
        "SELECT b.content FROM public.bot_payload_bodies b JOIN public.bot_payload_headers h USING(bot_id,payload_id) WHERE h.owner_kind IN ('control.input','control.result')",
    )
    assert all(secret.encode() not in bytes(body["content"]) for body in bodies)
    await forget(env, "first-plan")
    with pytest.raises(ForestError) as caught:
        await helpers.read(env)
    assert caught.value.code == "CONTROL_CONTENT_REVOKED"
    with pytest.raises(ForestError) as caught:
        await helpers.plan(env, revision=1, operation="new-preview")
    assert caught.value.code == "CONTROL_CONTENT_REVOKED"
    assert (await helpers.plan(env, revision=1, operation="next-plan"))[
        "result_payload_state"
    ] == "UNAVAILABLE"


async def test_component_current_history_and_unconsumed_plan_fail_closed_after_erasure(environment):
    env = environment
    plan = await env.plan(operation="first-plan")
    await env.apply(plan, "first-apply")
    pending = await env.plan(expected=1, operation="pending")
    await forget(env, "first-plan")
    for invoke in (
        env.read(),
        env.control.history(
            await env.ticket(
                "config.history", "history", {"component_id": COMPONENT, "before": 100}
            ),
            "history",
            {"component_id": COMPONENT, "before": 100},
        ),
    ):
        with pytest.raises(ForestError) as caught:
            await invoke
        assert caught.value.code == "CONTROL_CONTENT_REVOKED"
    # This independent component preview copied no previous body and remains valid.
    assert (await env.plan(expected=1, operation="pending")) == pending


async def test_mutating_callers_payload_and_ticket_during_clock_await_cannot_change_signed_input(
    environment, monkeypatch
):
    from copy import deepcopy

    env = environment
    payload = {"component_id": COMPONENT, "expected_revision": 0, "values": deepcopy(VALUES)}
    ticket = await env.ticket("config.plan", "immutable-input", payload)
    original_digest = ticket["claims"]["input_digest"]
    entered, resume = asyncio.Event(), asyncio.Event()
    actual_now = env.store._now
    first = True

    async def gated(conn):
        nonlocal first
        if first:
            first = False
            entered.set()
            await resume.wait()
        return await actual_now(conn)

    monkeypatch.setattr(env.store, "_now", gated)
    task = asyncio.create_task(env.control.plan(ticket, "immutable-input", payload))
    await asyncio.wait_for(entered.wait(), 3)
    payload["values"]["config"]["increment"] = 999
    ticket["claims"]["input_digest"] = digest_json(payload)
    resume.set()
    result = await task
    assert result["values"]["config"]["increment"] == 1
    saved = await operation(env, "immutable-input")
    async with env.store.pool.connection() as conn, conn.transaction():
        actual = await env.control.content.hydrate_input_in(conn, saved, epoch=0)
    assert saved["input_digest"] == saved["ticket_basis"]["input_digest"] == original_digest
    assert digest_json(actual["input"]) == original_digest


async def test_erased_unconsumed_plan_cannot_create_configuration_revision(environment):
    env = environment
    plan = await env.plan(operation="unconsumed")
    await forget(env, "unconsumed")
    with pytest.raises(ForestError) as caught:
        await env.apply(plan, "after-erasure")
    assert caught.value.code == "CONTROL_CONTENT_REVOKED"
    assert not await rows(env, "SELECT * FROM public.bot_config_revisions")
    assert len(await rows(env, "SELECT * FROM public.bot_web_operations")) == 1


async def test_binding_configuration_and_list_do_not_reveal_revoked_values(environment):
    from test_bindings import BindingEnvironment

    env = environment
    binding = BindingEnvironment(env)
    await binding.create()
    plan = await binding.plan(operation="binding-plan")
    await binding.apply(plan)
    assert (await binding.read())["desired_values"] == plan["values"]
    await forget(env, "binding-plan")
    for read in (
        binding.read(),
        binding.bindings.list(await env.ticket("binding.read", "binding-list", {}), "binding-list"),
    ):
        with pytest.raises(ForestError) as caught:
            await read
        assert caught.value.code == "CONTROL_CONTENT_REVOKED"
