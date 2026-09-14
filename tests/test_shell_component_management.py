"""Group Shell uses the same component event/job management state machines."""

import asyncio
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from binding_seed import create_online_binding
from psycopg import sql
from test_component_jobs import component_job_package, wait_jobs
from test_events import event_package, install_spec, wait_deliveries
from test_messages import receive
from test_shell import allow, dispatcher, shell_record
from test_shell_management import claim_command
from test_shell_registry_runtime import delivered_text

from forest.kernel import Kernel
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore
from forest.storage.component_jobs import ComponentJobStore
from forest.storage.events import EventStore
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def shell_components():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a dedicated test PostgreSQL")
    name = "forest_shell_components_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    store = BotStore(uri, "shell-component-bot")
    kernel = Kernel(store)
    try:
        await store.open()
        await store.migrate()
        await kernel.start()
        await create_online_binding(store, kernel.lease, "binding-a", "1000")
        messages = MessageStore(store, BindingStore(store))
        await allow(messages, "user-1")
        yield messages, kernel
    finally:
        await kernel.close()
        await store.close()
        assert name.startswith("forest_shell_components_") and len(name) == 56
        await admin.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
        )
        await admin.close()


async def test_shell_replays_and_retires_component_jobs_without_agent_semantics(
    tmp_path, shell_components
):
    messages, kernel = shell_components
    package = component_job_package(tmp_path, fail=True, max_attempts=1)
    grants = frozenset(package.component["capabilities"]["requests"])
    await kernel.install(package, grants=grants)
    dead_receipt = await kernel.invoke(
        "component.jobs",
        "component.jobs.schedule",
        {"value": 71, "job_operation": "shell-job-dead"},
        operation_id="shell-job-dead-call",
    )
    await wait_jobs(
        messages.store,
        lambda rows: len(rows) == 1 and rows[0]["status"] == "DEAD",
    )
    kernel._component_job_dispatcher.cancel()
    await asyncio.gather(kernel._component_job_dispatcher, return_exceptions=True)
    await kernel.invoke(
        "component.jobs",
        "component.jobs.schedule",
        {"value": 72, "job_operation": "shell-job-pending", "delay_seconds": 60},
        operation_id="shell-job-pending-call",
    )
    incoming = await receive(
        messages, kernel.lease, "job-list", text="#forest component-jobs list"
    )
    complete = dispatcher(messages, kernel.lease)
    try:
        await complete._process(incoming["command_id"])
        record = await shell_record(messages, incoming["command_id"])
        text, outcome = await delivered_text(messages), record["outcome"]
        assert record["delivery"]["send_lane"] == "CONTROL"
    finally:
        await complete.close()
    assert outcome == {"code": "COMPONENT_JOB_STATUS", "handler_count": 1, "job_count": 2}
    assert dead_receipt["job_id"] in text and "不属于会话 Agent 计划" in text
    assert "shell-job-dead" not in text and "private job failure" not in text

    controls = dispatcher(messages, kernel.lease)

    replay = await claim_command(
        messages,
        kernel.lease,
        f"#forest component-job replay {dead_receipt['job_id']} --expect-revision 0",
        number="job-replay",
    )
    first = await controls._typed(kernel.lease, replay)
    assert await controls._typed(kernel.lease, replay) == first
    assert first[1]["code"] == "COMPONENT_JOB_REPLAYED"
    assert first[1]["replay_epoch"] == 1

    plan_claim = await claim_command(
        messages,
        kernel.lease,
        "#forest component-job retire-plan component.jobs work.persist --expect-revision 0",
        number="job-retire-plan",
    )
    plan_text, plan = await controls._typed(kernel.lease, plan_claim)
    assert plan["code"] == "COMPONENT_JOB_RETIREMENT_PLAN"
    assert plan["retire_pending"] == 2 and plan["inflight"] == 0
    assert "#forest component-job retire-apply" in plan_text
    apply_claim = await claim_command(
        messages,
        kernel.lease,
        f"#forest component-job retire-apply {plan['plan_id']} --digest "
        f"{plan['plan_digest']} --expect-revision {plan['management_revision']}",
        number="job-retire-apply",
    )
    _, applied = await controls._typed(kernel.lease, apply_claim)
    assert applied["code"] == "COMPONENT_JOB_HANDLER_RETIRED"
    assert applied["retired_job_count"] == 2
    assert {
        row["status"] for row in await ComponentJobStore(messages.store).jobs()
    } == {"RETIRED"}
    async with messages.store.pool.connection() as conn:
        assert (
            await (
                await conn.execute("SELECT count(*) AS n FROM public.bot_agent_runs")
            ).fetchone()
        )["n"] == 0


async def test_shell_replays_and_retires_component_event_subscription(
    tmp_path, shell_components
):
    messages, kernel = shell_components
    subscriber_id = "profile.shell-audit"
    consumer = event_package(
        tmp_path,
        "shell-consumer",
        subscriber_id=subscriber_id,
        fail=True,
        max_attempts=1,
    )
    publisher = event_package(tmp_path, "shell-publisher", publisher=True)
    await kernel.install_many([install_spec(consumer), install_spec(publisher)])
    await kernel.invoke(
        "shell-publisher",
        "shell-publisher.run",
        {"value": 81, "event_operation": "shell-event-dead"},
        operation_id="shell-event-dead-call",
    )
    dead = (
        await wait_deliveries(
            messages.store,
            lambda rows: len(rows) == 1 and rows[0]["status"] == "DEAD",
        )
    )[0]
    kernel._event_dispatcher.cancel()
    await asyncio.gather(kernel._event_dispatcher, return_exceptions=True)
    await kernel.invoke(
        "shell-publisher",
        "shell-publisher.run",
        {"value": 82, "event_operation": "shell-event-pending"},
        operation_id="shell-event-pending-call",
    )
    controls = dispatcher(messages, kernel.lease)

    listing = await claim_command(
        messages, kernel.lease, "#forest events list", number="event-list"
    )
    text, outcome = await controls._typed(kernel.lease, listing)
    assert outcome == {"code": "EVENT_STATUS", "subscription_count": 1, "delivery_count": 2}
    assert subscriber_id in text and dead["delivery_id"] in text
    assert "private subscriber failure" not in text and "payload" not in text.lower()

    replay = await claim_command(
        messages,
        kernel.lease,
        f"#forest event replay {dead['delivery_id']} --expect-revision 0",
        number="event-replay",
    )
    _, replayed = await controls._typed(kernel.lease, replay)
    assert replayed["code"] == "EVENT_REPLAYED" and replayed["replay_epoch"] == 1

    plan_claim = await claim_command(
        messages,
        kernel.lease,
        f"#forest event retire-plan {subscriber_id} --contract-major 1 "
        "--expect-revision 0",
        number="event-retire-plan",
    )
    plan_text, plan = await controls._typed(kernel.lease, plan_claim)
    assert plan["code"] == "EVENT_RETIREMENT_PLAN"
    assert plan["retire_pending"] == 2 and plan["inflight"] == 0
    assert "#forest event retire-apply" in plan_text
    apply_claim = await claim_command(
        messages,
        kernel.lease,
        f"#forest event retire-apply {plan['plan_id']} --digest {plan['plan_digest']} "
        f"--expect-revision {plan['management_revision']}",
        number="event-retire-apply",
    )
    _, applied = await controls._typed(kernel.lease, apply_claim)
    assert applied["code"] == "EVENT_SUBSCRIPTION_RETIRED"
    assert applied["retired_delivery_count"] == 2
    assert {
        row["status"] for row in await EventStore(messages.store).deliveries()
    } == {"RETIRED"}
