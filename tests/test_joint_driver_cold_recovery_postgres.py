"""Cold rebound of an interrupted Host-to-Bot driver handoff."""

from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
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


@pytest.mark.postgres
async def test_cold_host_and_bot_graphs_rebind_pending_handoff(tmp_path, monkeypatch):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    names = ["forest_cold_joint_" + uuid4().hex for _ in range(2)]
    parsed = urlsplit(dsn)
    host_dsn, bot_dsn = (
        urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
        for name in names
    )
    host_id = "host/" + uuid4().hex
    bot_id = "cold-consumer"
    store = host_supervisor = controller = bot = None
    try:
        for name in names:
            await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        source_v1 = tmp_path / "host-v1"
        _exporting_host_source(source_v1)
        host_catalog = PackageCatalog(tmp_path / "host-catalog")
        host_catalog.build_official(source_v1, scope="host")
        source_v2 = tmp_path / "host-v2"
        copytree(source_v1, source_v2)
        manifest = source_v2 / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.0"', 'version = "1.0.1"', 1
            ), encoding="utf-8",
        )
        reviewed_staging = PackageCatalog(tmp_path / "host-staging")
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
        bot_source = tmp_path / "bot-consumer"
        _consuming_bot_source(bot_source, source_v1)
        staging = PackageCatalog(tmp_path / "bot-staging")
        consumer_package = staging.build(bot_source)
        bot_catalog = PackageCatalog(tmp_path / "bot-catalog")
        settings = _settings(bot_id, bot_dsn)
        tickets = WebAuthorityTicketAuthority(
            issuer_id="deployment/test", active_key_id="key/cold-test",
            key_resolver=lambda _key_id: bytes(range(32)),
        )

        async def start_host(gate):
            opened = HostStore(host_dsn, host_id)
            await opened.open()
            host = HostSupervisor(HostIdentityAuthority(opened))
            await host.start()
            ledger = HostGraphChangeAuthority(opened, ticket_authority=tickets)
            control = HostPackageController(
                HostRuntimeScope(opened, host, graph_change_authority=ledger),
                host_catalog, official_sources=(source_v1,),
                driver_binding_gate=gate,
                joint_driver_transition=execute_joint_host_driver_transition,
            )
            await control.start()
            return opened, host, ledger, control

        async def start_bot(gate, control):
            async def create_websocket(_url, _token, **_limits):
                return FakeWebSocketClient(settings.onebot_self_id)

            opened = BotHost(
                settings,
                model_client_factory=FakeModelClient,
                embedding_client_factory=FakeEmbeddingClient,
                media_client_factory=FakeMediaClient,
                vector_index_client_factory=FakeMilvusClient,
                vector_index_audit_client_factory=FakeMilvusAuditClient,
                websocket_client_factory=create_websocket,
                package_catalog=bot_catalog,
                driver_binding_gate=gate,
                host_driver_controller_provider=lambda: control,
            )
            await opened.start()
            return opened

        gate = ControlDriverBindingGate()
        store, host_supervisor, changes, controller = await start_host(gate)
        bot = await start_bot(gate, controller)
        await _select_consumer(bot, consumer_package, staging)
        old_source = controller.driver_exports()[0]["source_ref"]
        source_bot_graph = bot.registry.graph_identity
        expected_revision = controller.registry.graph_identity.graph_revision
        operation_id = "handoff/cold/" + uuid4().hex
        body = {
            "component_id": "host.sample", "package_digest": reviewed.digest,
            "expected_graph_revision": expected_revision,
            "plan_digest": digest_json({"package": reviewed.digest}),
        }
        session = SimpleNamespace(
            subject=SimpleNamespace(subject_id="web/cold-test", role="ADMIN"),
            session_ref="session/cold-test", session_revision=1,
        )
        ticket = await tickets.issue(
            session=session, target_scope_kind="host", target_scope_id=host_id,
            target_component_id="$runtime",
            target_generation_id=controller.registry.graph_identity.graph_id,
            target_graph_revision=expected_revision,
            target_operation_ref=operation_id, action_key="module.replace",
            input_digest=digest_json(body), plan_digest=body["plan_digest"],
        )
        receipts = HostWebAuthorityRepository(store, ticket_authority=tickets)

        async def authorize_in(conn):
            return await receipts.accept_in(conn, ticket=ticket)

        queued = await changes.enqueue_web(
            lease=host_supervisor.authority_lease(), ticket=ticket,
            body=body, authorize_in=authorize_in,
        )
        claimed = await changes.claim(host_supervisor.authority_lease())
        assert claimed["operation_id"] == queued["operation_id"]
        planned_host_activation = claimed["activation_id"]

        # This is the durable state after both graphs publish but before the
        # first per-Bot result write. A process loss at this point leaves PENDING.
        async def lose_result(*_args, **_kwargs):
            return None

        with monkeypatch.context() as patch:
            patch.setattr(changes, "finish_consumer", lose_result)
            published = await controller.replace(
                "host.sample", reviewed.digest,
                expected_graph_revision=expected_revision,
                activation_id=planned_host_activation,
            )
        assert published.activation_id == planned_host_activation
        planned_host_graph = published.graph_identity.graph_id
        planned_source = controller.driver_exports()[0]["source_ref"]
        pending = await changes.consumer_results(planned_host_activation)
        assert len(pending) == 1 and pending[0]["state"] == "PENDING"
        assert pending[0]["source_bot_graph_id"] == source_bot_graph.graph_id
        assert bot.registry.graph_identity.graph_id != source_bot_graph.graph_id
        assert planned_source != old_source

        await bot.close()
        bot = None
        await controller.close()
        controller = None
        await host_supervisor.close()
        host_supervisor = None
        await store.close()
        store = None

        gate = ControlDriverBindingGate()
        store, host_supervisor, changes, controller = await start_host(gate)
        bot = await start_bot(gate, controller)
        current_host_graph = controller.registry.graph_identity
        current_bot_graph = bot.registry.graph_identity
        current_source = controller.driver_exports()[0]["source_ref"]
        assert current_host_graph.graph_id != planned_host_graph
        assert current_source != planned_source
        assert current_bot_graph.graph_id != pending[0]["source_bot_graph_id"]
        descriptor = next(
            item for item in bot.registry.descriptors
            if item.component_id == "forest.counter"
        )
        assert ("tests.host.driver", "tests.host.exported-driver", current_source) in (
            descriptor.effect_driver_bindings
        )
        assert old_source not in {
            source for item in bot.registry.descriptors
            for _service, _kind, source in item.effect_driver_bindings
        }

        @asynccontextmanager
        async def management_host(scope_id):
            assert scope_id == bot_id
            yield bot

        supervisor_port = SimpleNamespace(
            management_scope_id=lambda selected: selected,
            management_host=management_host,
        )
        recovery = HostDriverHandoffRecovery(
            supervisor=supervisor_port, controller=controller,
            gate=gate, ledger=changes,
        )
        reconciliation = asyncio.create_task(
            controller.serve_graph_changes(changes, recover_consumers=recovery)
        )
        try:
            async with asyncio.timeout(30):
                while True:
                    receipt = await changes.get(operation_id)
                    if receipt["state"] in {"SUCCEEDED", "FAILED", "PARTIAL"}:
                        break
                    if reconciliation.done():
                        await reconciliation
                    await asyncio.sleep(0.1)
        finally:
            reconciliation.cancel()
            await asyncio.gather(reconciliation, return_exceptions=True)
        assert receipt["state"] == "SUCCEEDED"
        request = receipt
        result = (await changes.consumer_results(planned_host_activation))[0]
        assert result["state"] == "SUCCEEDED"
        assert result["recovery_mode"] == "COLD_REBOUND"
        assert result["result_host_graph_id"] == current_host_graph.graph_id
        assert result["result_bot_graph_id"] == current_bot_graph.graph_id
        assert result["result_bot_activation_id"] == bot.registry.activation_id
        assert (await changes.published_graph(request))["graph_id"] == planned_host_graph
    finally:
        if bot is not None:
            await bot.close()
        if controller is not None:
            await controller.close()
        if host_supervisor is not None:
            await host_supervisor.close()
        if store is not None:
            await store.close()
        for name in reversed(names):
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)
            ))
        await admin.close()
