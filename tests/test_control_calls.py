"""The bounded host codec lane survives pause, while reload still drains it."""

import asyncio
import base64
import json
import os
import shutil
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.bot import BotStore

pytestmark = pytest.mark.postgres


@pytest_asyncio.fixture
async def store():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private test instance")
    name = "forest_control_calls_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parts = urlsplit(dsn)
    uri = urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))
    instance = BotStore(uri, "control-bot")
    try:
        await instance.open()
        await instance.migrate()
        yield instance
    finally:
        await instance.close()
        assert name.startswith("forest_control_calls_") and len(name) == 53
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def expect(code, awaitable):
    with pytest.raises(ForestError) as exc:
        await awaitable
    assert exc.value.code == code


async def generation(store, lease, name="g1", grants=()):
    await store.register_generation(
        lease,
        "adapter.onebot",
        name,
        "package",
        "config",
        "worker-" + name,
        config={},
        instance_id="instance-" + name,
        capability_grants=grants,
    )
    await store.mark_ready(lease, name)


async def activated(store):
    lease = await store.acquire_host("host", "inc", ttl_seconds=120)
    await generation(store, lease)
    op = await store.begin_operation(lease, "adapter.onebot", "activate", "plan", "g1", 0)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    return lease


async def control(store, lease, call="control", **kwargs):
    return await store.admit_control(
        lease,
        "adapter.onebot",
        call,
        "op-" + call,
        "digest-" + call,
        service_name="onebot.prepare",
        **kwargs,
    )


async def test_pause_fences_data_but_keeps_existing_and_new_codec_calls(store):
    lease = await activated(store)
    data = await store.admit(lease, "adapter.onebot", "data", "op-data", "digest")
    first = await control(store, lease)
    await store.set_maintenance(lease, 1, False)
    await expect("STALE_MAINTENANCE", store.finish_call(data, {}))
    await expect(
        "ADMISSION_CLOSED", store.admit(lease, "adapter.onebot", "data2", "op-data2", "digest")
    )
    await store.finish_call(first, {"pure": "first"})
    second = await control(store, lease, "second")
    await store.finish_call(second, {"pure": "second"})
    assert second.admission_lane == "CONTROL" and second.control_service == "onebot.prepare"
    saved = await store.authorized_call(lease, "op-second", 1, admission_lane="CONTROL")
    assert saved["result"] == {"pure": "second"}
    await expect("IDEMPOTENCY_CONFLICT", store.authorized_call(lease, "op-second", 1))


async def test_control_identity_cannot_escalate_to_state_or_children(store):
    lease = await activated(store)
    grant = await control(store, lease)
    await expect("CONTROL_SCOPE_DENIED", store.read_state(grant, "secret"))
    await expect("CONTROL_SCOPE_DENIED", store.commit_state(grant, "x", 0, 1, "step", "digest"))
    await expect(
        "CONTROL_SCOPE_DENIED", store.admit_nested(grant, "business", "child", "op-child", "digest")
    )
    await expect(
        "INVALID_GRANT",
        store.read_state(replace(grant, admission_lane="DATA", control_service=None), "secret"),
    )
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
            await conn.execute(
                "UPDATE public.bot_calls SET admission_lane='DATA',control_service=NULL"
            )
    await expect(
        "CONTROL_SCOPE_DENIED",
        store.admit_control(lease, "business", "c", "op-c", "d", service_name="onebot.prepare"),
    )
    await expect(
        "CONTROL_SCOPE_DENIED",
        store.admit_control(lease, "adapter.onebot", "c", "op-c", "d", service_name="exec"),
    )
    await expect(
        "CONTROL_SCOPE_DENIED",
        control(store, lease, "dependent", dependency_components=("business",)),
    )


async def test_control_still_obeys_current_policy_cancel_deadline_and_control_switch(store):
    lease = await activated(store)
    grant = await control(store, lease)
    await store.cancel_call(lease, grant.call_id)
    await expect("CALL_CANCELLED", store.finish_call(grant, {}))
    expired = await control(store, lease, "expired", timeout_seconds=0.1)
    await asyncio.sleep(0.15)
    await expect("CALL_EXPIRED", store.finish_call(expired, {}))
    paused = await control(store, lease, "paused")
    async with store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_runtime_state SET control_admission=false")
    await expect("CONTROL_CLOSED", store.finish_call(paused, {}))
    await expect("ADMISSION_CLOSED", control(store, lease, "closed"))
    async with store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_runtime_state SET control_admission=true")
    await store.revoke_policy(lease, 1)
    await expect("POLICY_REVOKED", store.finish_call(paused, {}))


async def test_reload_counts_control_calls_during_pause_and_aborts_to_control_route(store):
    lease = await activated(store)
    grant = await control(store, lease)
    await store.set_maintenance(lease, 1, False)
    await generation(store, lease, "g2")
    op = await store.begin_operation(lease, "adapter.onebot", "reload", "plan2", "g2", 1)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await expect("ADMISSION_CLOSED", control(store, lease, "during-freeze"))
    await expect("DRAIN_PENDING", store.publish(lease, op["operation_id"], op["op_epoch"]))
    await store.finish_call(grant, {"complete": True})
    await store.abort_operation(lease, op["operation_id"], op["op_epoch"])
    route = await store.get_route("adapter.onebot")
    assert route["control_admission"] and not route["admission"]
    await store.finish_call(await control(store, lease, "after-abort"), {})


async def test_publication_during_pause_opens_only_codec_control_lane(store):
    lease = await store.acquire_host("host", "inc", ttl_seconds=120)
    await store.set_maintenance(lease, 1, False)
    await generation(store, lease)
    op = await store.begin_operation(lease, "adapter.onebot", "install", "plan", "g1", 0)
    await store.freeze(lease, op["operation_id"], op["op_epoch"])
    await store.publish(lease, op["operation_id"], op["op_epoch"])
    assert (await control(store, lease)).admission_lane == "CONTROL"
    await expect(
        "ADMISSION_CLOSED", store.admit(lease, "adapter.onebot", "data", "op-data", "digest")
    )


def codec_package(tmp_path, *, slow=False, privileged=False):
    source = tmp_path / "source"
    shutil.copytree(Path(str(files("forest").joinpath("builtin", "onebot"))), source)
    if slow:
        path = source / "python" / "onebot.py"
        code = path.read_text(encoding="utf-8")
        code = "import asyncio\n" + code.replace(
            "async def normalize(self, payload, ctx):",
            "async def normalize(self, payload, ctx):\n        await asyncio.sleep(1)",
        )
        path.write_text(code, encoding="utf-8")
    if privileged:
        path = source / "module.toml"
        path.write_text(
            path.read_text(encoding="utf-8")
            + '\n[components.capabilities]\nrequests = ["state.read"]\n',
            encoding="utf-8",
        )
    return PackageCatalog(tmp_path / "catalog").build(source)


PREPARE = {
    "channel_kind": "group",
    "channel_id": "123",
    "segments": [{"type": "text", "text": "status"}],
}
NORMALIZE = {
    "body_b64": base64.b64encode(
        json.dumps({"post_type": "meta_event", "self_id": 123}).encode()
    ).decode(),
    "self_id": "123",
}


async def test_real_worker_control_codec_runs_after_install_while_paused(store, tmp_path):
    kernel = Kernel(store)
    await kernel.start()
    try:
        await store.set_maintenance(kernel.lease, 1, False)
        await kernel.install(codec_package(tmp_path))
        result = await kernel.invoke_control(
            "adapter.onebot", "onebot.prepare", PREPARE, operation_id="prepare"
        )
        assert result["action"] == "send_group_msg"
        assert (
            await kernel.invoke_control(
                "adapter.onebot", "onebot.prepare", PREPARE, operation_id="prepare"
            )
            == result
        )
        await expect(
            "ADMISSION_CLOSED",
            kernel.invoke("adapter.onebot", "onebot.prepare", PREPARE, operation_id="data"),
        )
        await expect(
            "CONTROL_SCOPE_DENIED",
            kernel.invoke_control("adapter.onebot", "arbitrary", {}, operation_id="bad"),
        )
    finally:
        await kernel.close()


async def test_control_has_separate_bounded_capacity_from_busy_data(store, tmp_path):
    kernel = Kernel(store, max_calls=1, max_control_calls=1)
    await kernel.start()
    try:
        await kernel.install(codec_package(tmp_path, slow=True))
        data = asyncio.create_task(
            kernel.invoke("adapter.onebot", "onebot.normalize", NORMALIZE, operation_id="slow-data")
        )
        async with asyncio.timeout(3):
            while await store.read_call("slow-data") is None:
                await asyncio.sleep(0.01)
        await expect(
            "BACKPRESSURE",
            kernel.invoke("adapter.onebot", "onebot.prepare", PREPARE, operation_id="second-data"),
        )
        assert (
            await kernel.invoke_control(
                "adapter.onebot", "onebot.prepare", PREPARE, operation_id="control"
            )
        )["action"] == "send_group_msg"
        await data
        slow = asyncio.create_task(
            kernel.invoke_control(
                "adapter.onebot", "onebot.normalize", NORMALIZE, operation_id="slow-control"
            )
        )
        async with asyncio.timeout(3):
            while await store.read_call("slow-control") is None:
                await asyncio.sleep(0.01)
        await expect(
            "BACKPRESSURE",
            kernel.invoke_control(
                "adapter.onebot", "onebot.prepare", PREPARE, operation_id="second-control"
            ),
        )
        await slow
    finally:
        await kernel.close()


async def test_privileged_codec_manifest_cannot_enter_control_lane(store, tmp_path):
    kernel = Kernel(store)
    await kernel.start()
    try:
        await kernel.install(codec_package(tmp_path, privileged=True), grants={"state.read"})
        await expect(
            "CONTROL_SCOPE_DENIED",
            kernel.invoke_control("adapter.onebot", "onebot.prepare", PREPARE, operation_id="bad"),
        )
        assert await store.read_call("bad") is None
    finally:
        await kernel.close()
