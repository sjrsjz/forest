"""Hard-process-loss recovery of a pending Host-to-Bot driver handoff."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from shutil import copytree
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from test_bot_host import (
    FakeEmbeddingClient,
    FakeMediaClient,
    FakeMilvusAuditClient,
    FakeMilvusClient,
    FakeModelClient,
    FakeWebSocketClient,
)
from test_joint_driver_postgres import (
    _consuming_bot_source,
    _exporting_host_source,
    _select_consumer,
    _settings,
)

from forest.bot_host import BotHost
from forest.control_driver_binding_gate import ControlDriverBindingGate
from forest.control_driver_joint_transition import execute_joint_host_driver_transition
from forest.control_driver_recovery import HostDriverHandoffRecovery
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog, digest_json
from forest.storage.host_graph_changes import HostGraphChangeAuthority
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore
from forest.storage.host_web_authority import HostWebAuthorityRepository
from forest.web_authority_ticket import WebAuthorityTicketAuthority


def _dsn(state: dict, field: str) -> str:
    base = os.environ["FOREST_TEST_DSN"]
    parsed = urlsplit(base)
    return urlunsplit((
        parsed.scheme, parsed.netloc, "/" + state[field], "", "",
    ))


def _tickets():
    return WebAuthorityTicketAuthority(
        issuer_id="deployment/test", active_key_id="key/process-test",
        key_resolver=lambda _key_id: bytes(range(32)),
    )


async def _start_host(state: dict, gate: ControlDriverBindingGate):
    store = HostStore(_dsn(state, "host_database"), state["host_id"])
    await store.open()
    supervisor = HostSupervisor(HostIdentityAuthority(store))
    await supervisor.start()
    changes = HostGraphChangeAuthority(store, ticket_authority=_tickets())
    root = Path(state["root"])
    controller = HostPackageController(
        HostRuntimeScope(store, supervisor, graph_change_authority=changes),
        PackageCatalog(root / "host-catalog"),
        official_sources=(root / "host-v1",),
        driver_binding_gate=gate,
        joint_driver_transition=execute_joint_host_driver_transition,
    )
    await controller.start()
    return store, supervisor, changes, controller


async def _start_bot(state: dict, gate: ControlDriverBindingGate, controller):
    settings = _settings(state["bot_id"], _dsn(state, "bot_database"))

    async def websocket(_url, _token, **_limits):
        return FakeWebSocketClient(settings.onebot_self_id)

    bot = BotHost(
        settings,
        model_client_factory=FakeModelClient,
        embedding_client_factory=FakeEmbeddingClient,
        media_client_factory=FakeMediaClient,
        vector_index_client_factory=FakeMilvusClient,
        vector_index_audit_client_factory=FakeMilvusAuditClient,
        websocket_client_factory=websocket,
        package_catalog=PackageCatalog(Path(state["root"]) / "bot-catalog"),
        driver_binding_gate=gate,
        host_driver_controller_provider=lambda: controller,
    )
    await bot.start()
    return bot


async def _crash_phase(state_path: Path) -> None:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    root = Path(state["root"])
    source_v1 = root / "host-v1"
    _exporting_host_source(source_v1)
    host_catalog = PackageCatalog(root / "host-catalog")
    host_catalog.build_official(source_v1, scope="host")
    source_v2 = root / "host-v2"
    copytree(source_v1, source_v2)
    manifest = source_v2 / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            'version = "1.0.0"', 'version = "1.0.1"', 1,
        ), encoding="utf-8",
    )
    reviewed_staging = PackageCatalog(root / "host-staging")
    reviewed = reviewed_staging.build(source_v2)
    archive = reviewed_staging.export_archive(reviewed.digest)
    host_catalog.publish_reviewed_control_archive(
        archive, scope="host",
        expected_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
        expected_package_digest=reviewed.digest,
        expected_manifest_digest=digest_json(reviewed.manifest),
        expected_package_id=reviewed.manifest["package_id"],
        expected_component_id=reviewed.component["id"],
        expected_runtime="python-worker",
    )
    bot_source = root / "bot-consumer"
    _consuming_bot_source(bot_source, source_v1)
    staging = PackageCatalog(root / "bot-staging")
    consumer = staging.build(bot_source)
    gate = ControlDriverBindingGate()
    store, supervisor, changes, controller = await _start_host(state, gate)
    bot = await _start_bot(state, gate, controller)
    await _select_consumer(bot, consumer, staging)
    old_bot_graph = bot.registry.graph_identity.graph_id
    expected_revision = controller.registry.graph_identity.graph_revision
    operation_id = "handoff/process/" + uuid4().hex
    body = {
        "component_id": "host.sample", "package_digest": reviewed.digest,
        "expected_graph_revision": expected_revision,
        "plan_digest": digest_json({"package": reviewed.digest}),
    }
    session = SimpleNamespace(
        subject=SimpleNamespace(subject_id="web/process-test", role="ADMIN"),
        session_ref="session/process-test", session_revision=1,
    )
    ticket = await _tickets().issue(
        session=session, target_scope_kind="host",
        target_scope_id=state["host_id"], target_component_id="$runtime",
        target_generation_id=controller.registry.graph_identity.graph_id,
        target_graph_revision=expected_revision,
        target_operation_ref=operation_id, action_key="module.replace",
        input_digest=digest_json(body), plan_digest=body["plan_digest"],
    )
    receipts = HostWebAuthorityRepository(store, ticket_authority=_tickets())

    async def authorize_in(conn):
        return await receipts.accept_in(conn, ticket=ticket)

    queued = await changes.enqueue_web(
        lease=supervisor.authority_lease(), ticket=ticket,
        body=body, authorize_in=authorize_in,
    )
    claimed = await changes.claim(supervisor.authority_lease())
    assert claimed["operation_id"] == queued["operation_id"]

    async def lose_result(*_args, **_kwargs):
        return None

    changes.finish_consumer = lose_result
    published = await controller.replace(
        "host.sample", reviewed.digest,
        expected_graph_revision=expected_revision,
        activation_id=claimed["activation_id"],
    )
    pending = await changes.consumer_results(published.activation_id)
    assert len(pending) == 1 and pending[0]["state"] == "PENDING"
    assert pending[0]["source_bot_graph_id"] == old_bot_graph
    assert bot.registry.graph_identity.graph_id != old_bot_graph
    marker = {
        "operation_id": operation_id,
        "activation_id": published.activation_id,
        "published_host_graph_id": published.graph_identity.graph_id,
        "source_bot_graph_id": old_bot_graph,
        "host_owner_epoch": supervisor.authority_lease().owner_epoch,
        "bot_owner_epoch": bot._lease.owner_epoch,
        "bot_runtime_namespace": bot._kernel.runtime.namespace,
    }
    temporary = state_path.with_suffix(".ready.tmp")
    temporary.write_text(json.dumps(marker), encoding="utf-8")
    os.replace(temporary, state_path.with_suffix(".ready"))
    await asyncio.Event().wait()


@pytest.mark.postgres
@pytest.mark.skipif(os.name != "posix", reason="Process group kill requires POSIX")
async def test_process_kill_rebinds_pending_host_driver_handoff(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    names = ["forest_process_joint_" + uuid4().hex for _ in range(2)]
    parsed = urlsplit(dsn)
    host_dsn, bot_dsn = (
        urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
        for name in names
    )
    state = {
        "root": str(tmp_path), "host_id": "host/" + uuid4().hex,
        "bot_id": "process-consumer",
        "host_database": names[0], "bot_database": names[1],
    }
    state_path = tmp_path / "process-state.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    marker_path = state_path.with_suffix(".ready")
    stage_path = tmp_path / "process-stage.txt"

    def stage(value: str) -> None:
        stage_path.write_text(value, encoding="utf-8")

    process = None
    store = supervisor = controller = bot = None
    try:
        for name in names:
            await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        env = os.environ.copy()
        repo = Path(__file__).parents[1]
        env["PYTHONPATH"] = str(repo / "src") + os.pathsep + env.get("PYTHONPATH", "")
        output_path = tmp_path / "crash-phase.log"
        with output_path.open("wb") as output:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), str(state_path)],
                cwd=repo, env=env, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        async with asyncio.timeout(360):
            while not marker_path.exists():
                if process.poll() is not None:
                    pytest.fail(
                        "Crash phase exited before durable PENDING: "
                        + output_path.read_text(encoding="utf-8", errors="replace")[-1600:]
                    )
                await asyncio.sleep(0.2)
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        stage("durable-pending")
        os.killpg(process.pid, signal.SIGKILL)
        await asyncio.to_thread(process.wait, 15)
        process = None
        stage("process-killed")

        # A killed incarnation retains its lease until expiry; takeover must
        # use a new epoch rather than treating process death as an implicit release.
        async with (
            await psycopg.AsyncConnection.connect(host_dsn) as host_probe,
            await psycopg.AsyncConnection.connect(bot_dsn) as bot_probe,
        ):
            async with asyncio.timeout(150):
                while True:
                    host_lease = await (await host_probe.execute(
                        "SELECT expires_at<=clock_timestamp() "
                        "FROM forest_host.supervisor_lease"
                    )).fetchone()
                    bot_lease = await (await bot_probe.execute(
                        sql.SQL("SELECT expires_at<=clock_timestamp() FROM {}").format(
                            sql.Identifier(marker["bot_runtime_namespace"], "host_lease")
                        )
                    )).fetchone()
                    if host_lease[0] and bot_lease[0]:
                        break
                    await asyncio.sleep(0.5)
        stage("leases-expired")
        gate = ControlDriverBindingGate()
        store, supervisor, changes, controller = await _start_host(state, gate)
        stage("host-started")
        assert supervisor.authority_lease().owner_epoch > marker["host_owner_epoch"]
        bot = await _start_bot(state, gate, controller)
        stage("bot-started")
        assert bot._lease.owner_epoch > marker["bot_owner_epoch"]
        pending = await changes.consumer_results(marker["activation_id"])
        assert len(pending) == 1 and pending[0]["state"] == "PENDING"
        assert pending[0]["source_bot_graph_id"] == marker["source_bot_graph_id"]
        current_host = controller.registry.graph_identity
        current_bot = bot.registry.graph_identity
        source = controller.driver_exports()[0]["source_ref"]
        assert current_host.graph_id != marker["published_host_graph_id"]
        assert current_bot.graph_id != marker["source_bot_graph_id"]
        descriptor = next(
            item for item in bot.registry.descriptors
            if item.component_id == "forest.counter"
        )
        assert ("tests.host.driver", "tests.host.exported-driver", source) in (
            descriptor.effect_driver_bindings
        )

        @asynccontextmanager
        async def management_host(scope_id):
            assert scope_id == state["bot_id"]
            yield bot

        port = SimpleNamespace(
            management_scope_id=lambda selected: selected,
            management_host=management_host,
        )
        recovery = HostDriverHandoffRecovery(
            supervisor=port, controller=controller, gate=gate, ledger=changes,
        )
        reconciliation = asyncio.create_task(
            controller.serve_graph_changes(changes, recover_consumers=recovery)
        )
        stage("reconciliation-started")
        try:
            async with asyncio.timeout(30):
                while True:
                    receipt = await changes.get(marker["operation_id"])
                    if receipt["state"] in {"SUCCEEDED", "FAILED", "PARTIAL"}:
                        stage("receipt-terminal")
                        break
                    if reconciliation.done():
                        await reconciliation
                    await asyncio.sleep(0.1)
        finally:
            stage("reconciliation-cancelling")
            reconciliation.cancel()
            await asyncio.gather(reconciliation, return_exceptions=True)
            stage("reconciliation-cancelled")
        assert receipt["state"] == "SUCCEEDED"
        result = (await changes.consumer_results(marker["activation_id"]))[0]
        assert result["state"] == "SUCCEEDED"
        assert result["recovery_mode"] == "COLD_REBOUND"
        assert result["result_host_graph_id"] == current_host.graph_id
        assert result["result_bot_graph_id"] == current_bot.graph_id
        assert result["result_bot_activation_id"] == bot.registry.activation_id
        stage("result-verified")
    finally:
        stage("cleanup-started")
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            await asyncio.to_thread(process.wait, 15)
        if bot is not None:
            stage("bot-closing")
            await bot.close()
            stage("bot-closed")
        if controller is not None:
            stage("controller-closing")
            await controller.close()
            stage("controller-closed")
        if supervisor is not None:
            stage("supervisor-closing")
            await supervisor.close()
            stage("supervisor-closed")
        if store is not None:
            stage("store-closing")
            await store.close()
            stage("store-closed")
        stage("database-cleaning")
        for name in reversed(names):
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)
            ))
        await admin.close()
        stage("done")


if __name__ == "__main__":
    asyncio.run(_crash_phase(Path(sys.argv[1])))
