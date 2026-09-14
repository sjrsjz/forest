"""Group Shell module metadata and reload use the ordinary config authority."""

import asyncio
from uuid import uuid4

import pytest
from binding_seed import _ticket_factory
from test_messages import messages as messages
from test_messages import receive
from test_shell import allow, dispatcher, shell_record
from test_shell_management import claim_command

from forest.errors import ForestError
from forest.storage.control import BotControl
from forest.storage.module_administration import ModuleAdministration
from forest.storage.shell_control import ShellControl

pytestmark = pytest.mark.postgres

COMPONENT = "forest.test-module"
VALUES = {
    "package_digest": "sha256:test-package-v1",
    "config": {"private_label": "must-not-enter-group-output"},
    "grants": ["state.read"],
}


async def configure(repository, lease):
    control = BotControl(repository.store)
    ticket = await _ticket_factory(repository.store, lease)
    operation = "module-seed-plan:" + uuid4().hex
    payload = {
        "component_id": COMPONENT,
        "expected_revision": 0,
        "values": VALUES,
    }
    plan = await control.plan(
        await ticket("config.plan", operation, payload), operation, payload
    )
    operation = "module-seed-apply:" + uuid4().hex
    payload = {
        "component_id": COMPONENT,
        "expected_revision": 0,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    await control.apply(
        await ticket("config.apply", operation, payload, plan_digest=plan["plan_digest"]),
        operation,
        payload,
    )
    return control, ticket


async def test_shell_lists_metadata_and_reload_creates_an_ordinary_config_revision(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    await configure(repository, lease)
    controls = dispatcher(repository, lease)

    status_claim = await claim_command(
        repository, lease, f"#forest module status {COMPONENT}", number="module-status"
    )
    status_text, status = await controls._typed(lease, status_claim)
    assert status == {"code": "MODULE_STATUS", "count": 1, "pending": 1}
    assert COMPONENT in status_text and "desired 1 / effective 0" in status_text
    assert "must-not-enter-group-output" not in status_text

    reload_claim = await claim_command(
        repository,
        lease,
        f"#forest module reload {COMPONENT} --expect-revision 1",
        number="module-reload",
    )
    first = await controls._typed(lease, reload_claim)
    second = await controls._typed(lease, reload_claim)
    assert first == second
    text, outcome = first
    assert outcome["code"] == "MODULE_RELOAD_ACCEPTED"
    assert outcome["reload_of_revision"] == 1
    assert outcome["desired_revision"] == 2
    assert outcome["effective_revision"] == 0
    assert "发布后才算生效" in text

    async with repository.store.pool.connection() as conn, conn.transaction():
        revisions = await (await conn.execute(
            "SELECT revision,previous_revision,operation_id FROM public.bot_config_revisions "
            "WHERE bot_id=%s AND component_id=%s ORDER BY revision",
            (repository.store.bot_id, COMPONENT),
        )).fetchall()
        operations = await (await conn.execute(
            "SELECT action,count(*) AS count FROM public.bot_web_operations "
            "WHERE bot_id=%s GROUP BY action ORDER BY action",
            (repository.store.bot_id,),
        )).fetchall()
        lifecycles = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_lifecycle_operations "
            "WHERE bot_id=%s AND component_id=%s",
            (repository.store.bot_id, COMPONENT),
        )).fetchone()
        copied = await BotControl(repository.store).config_revision_in(
            conn, COMPONENT, 2
        )
    assert [(row["revision"], row["previous_revision"]) for row in revisions] == [
        (1, 0), (2, 1)
    ]
    assert copied["values"] == VALUES
    counts = {row["action"]: row["count"] for row in operations}
    assert counts["config.plan"] == 2
    assert counts["config.apply"] == 2
    assert counts["module.reload"] == 1
    # Shell accepts a desired revision. Only the normal host reconciler creates
    # and publishes lifecycle operations; the command itself never edits routes.
    assert lifecycles["count"] == 0

    stale = await claim_command(
        repository,
        lease,
        f"#forest module reload {COMPONENT} --expect-revision 1",
        number="module-stale",
    )
    with pytest.raises(ForestError) as conflict:
        await controls._typed(lease, stale)
    assert conflict.value.code == "REVISION_CONFLICT"

    # The complete dispatcher path links the wrapper management receipt into
    # the CONTROL outcome and still reports desired/effective separately.
    incoming = await receive(
        repository,
        lease,
        "module-complete",
        text=f"#forest module reload {COMPONENT} --expect-revision 2",
    )
    controls.command_timeout = 5
    await controls._process(incoming["command_id"])
    assert incoming["command_id"] not in controls.errors
    completed = await shell_record(repository, incoming["command_id"])
    assert completed["status"] == "COMPLETED"
    assert completed["outcome"]["code"] == "MODULE_RELOAD_ACCEPTED"
    assert completed["outcome"]["desired_revision"] == 3
    assert completed["outcome"]["effective_revision"] == 0


async def test_web_apply_and_shell_reload_converge_through_one_config_head_cas(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    control, ticket = await configure(repository, lease)

    changed = {
        "package_digest": "sha256:test-package-v2",
        "config": {"private_label": "web-winner"},
        "grants": [],
    }
    plan_operation = "web-race-plan:" + uuid4().hex
    plan_payload = {
        "component_id": COMPONENT,
        "expected_revision": 1,
        "values": changed,
    }
    plan = await control.plan(
        await ticket("config.plan", plan_operation, plan_payload),
        plan_operation,
        plan_payload,
    )
    apply_operation = "web-race-apply:" + uuid4().hex
    apply_payload = {
        "component_id": COMPONENT,
        "expected_revision": 1,
        "plan_id": plan["plan_id"],
        "plan_digest": plan["plan_digest"],
    }
    apply_ticket = await ticket(
        "config.apply", apply_operation, apply_payload, plan_digest=plan["plan_digest"]
    )
    shell_claim = await claim_command(
        repository,
        lease,
        f"#forest module reload {COMPONENT} --expect-revision 1",
        number="module-web-race",
    )
    web, shell = await asyncio.gather(
        control.apply(apply_ticket, apply_operation, apply_payload),
        dispatcher(repository, lease)._typed(lease, shell_claim),
        return_exceptions=True,
    )
    successes = [value for value in (web, shell) if not isinstance(value, BaseException)]
    conflicts = [value for value in (web, shell) if isinstance(value, ForestError)]
    assert len(successes) == len(conflicts) == 1
    assert conflicts[0].code == "REVISION_CONFLICT"

    async with repository.store.pool.connection() as conn, conn.transaction():
        head = await (await conn.execute(
            "SELECT desired_revision,effective_revision FROM public.bot_config_heads "
            "WHERE bot_id=%s AND component_id=%s",
            (repository.store.bot_id, COMPONENT),
        )).fetchone()
        revision = await control.config_revision_in(conn, COMPONENT, 2)
        wrappers = await (await conn.execute(
            "SELECT count(*) AS count FROM public.bot_web_operations "
            "WHERE bot_id=%s AND action='module.reload'",
            (repository.store.bot_id,),
        )).fetchone()
    assert head == {"desired_revision": 2, "effective_revision": 0}
    assert revision["values"] in (VALUES, changed)
    assert wrappers["count"] == (1 if revision["values"] == VALUES else 0)


async def test_reload_recovers_when_apply_committed_before_wrapper_receipt(
    messages, monkeypatch,
):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    await configure(repository, lease)
    claim = await claim_command(
        repository,
        lease,
        f"#forest module reload {COMPONENT} --expect-revision 1",
        number="module-reload-crash",
    )
    control = ShellControl(repository, lease, claim)
    module = ModuleAdministration(repository.store, control=control)
    operation = "shell-action:" + claim["command_id"]
    record = control._record

    async def stop_before_wrapper(conn, claims, result, *, parents=()):
        if claims["action"] == "module.reload":
            raise RuntimeError("process stopped before wrapper receipt")
        return await record(conn, claims, result, parents=parents)

    monkeypatch.setattr(control, "_record", stop_before_wrapper)
    with pytest.raises(RuntimeError, match="before wrapper"):
        await module.reload(control.admission, operation, claim["arguments"])

    monkeypatch.setattr(control, "_record", record)
    recovered = await module.reload(control.admission, operation, claim["arguments"])
    replay = await module.reload(control.admission, operation, claim["arguments"])
    assert recovered == replay
    assert recovered["desired_revision"] == 2
    async with repository.store.pool.connection() as conn:
        counts = await (await conn.execute(
            "SELECT action,count(*) AS count FROM public.bot_web_operations "
            "WHERE bot_id=%s AND action IN ('config.plan','config.apply','module.reload') "
            "GROUP BY action",
            (repository.store.bot_id,),
        )).fetchall()
    assert {row["action"]: row["count"] for row in counts} == {
        "config.plan": 2,
        "config.apply": 2,
        "module.reload": 1,
    }
