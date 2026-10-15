"""Real PostgreSQL handoff from a Host driver to two Bot graphs."""

from __future__ import annotations

import asyncio
import hashlib
import os
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
from test_host_package_compiler import _host_package

from forest.bot_host import BotHost, BotSettings
from forest.builtin_packages import bootstrap_installed_package
from forest.control_driver_binding_gate import ControlDriverBindingGate
from forest.control_driver_joint_transition import execute_joint_host_driver_transition
from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_supervisor import HostSupervisor
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.storage.host_graph_changes import HostGraphChangeAuthority
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore
from forest.web_authority_ticket import WebAuthorityTicketAuthority


def _exporting_host_source(path: Path) -> None:
    _host_package(path)
    manifest = path / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8")
        + "\n[[components.provides]]\n"
          'name = "host.sample.driver"\n'
          'contract = "1.0"\n'
          'semantics = "at-most-once"\n'
          'visibility = "public"\n'
          'input_schema = "schemas/echo.json"\n'
          'output_schema = "schemas/echo.json"\n'
          "\n[[components.effect_driver_exports]]\n"
          'kind = "tests.host.exported-driver"\n'
          'service = "host.sample.driver"\n',
        encoding="utf-8",
    )
    worker = path / "python/host_sample.py"
    worker.write_text(
        worker.read_text(encoding="utf-8")
        + "    @service('host.sample.driver', semantics='at-most-once')\n"
          "    async def driver(self, payload, ctx):\n"
          "        return payload\n",
        encoding="utf-8",
    )


def _holdable_host_driver(source: Path) -> None:
    worker = source / "python/host_sample.py"
    text = worker.read_text(encoding="utf-8")
    original = "    async def driver(self, payload, ctx):\n        return payload\n"
    assert text.count(original) == 1
    replacement = '''    async def driver(self, payload, ctx):
        if payload["value"].startswith("hold:"):
            import asyncio
            from pathlib import Path

            gate = Path(payload["value"][5:])
            with gate.with_suffix(".entered").open("a", encoding="utf-8") as marker:
                marker.write("entered\\n")
            while not gate.with_suffix(".release").exists():
                await asyncio.sleep(0.02)
        return payload
'''
    worker.write_text(text.replace(original, replacement, 1), encoding="utf-8")


def _consuming_bot_source(path: Path, host_source: Path) -> None:
    copytree(Path(__file__).parents[1] / "src/forest/builtin/counter", path)
    (path / "schemas/host-echo.json").write_bytes(
        (host_source / "schemas/echo.json").read_bytes()
    )
    manifest = path / "module.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8")
        + "\n[[components.effect_services]]\n"
          'name = "tests.host.driver"\n'
          'contract = "1.0"\n'
          'kind = "tests.host.exported-driver"\n'
          'semantics = "at-most-once"\n'
          'input_schema = "schemas/host-echo.json"\n'
          'output_schema = "schemas/host-echo.json"\n',
        encoding="utf-8",
    )


def _settings(bot_id: str, dsn: str) -> BotSettings:
    return BotSettings(
        bot_id=bot_id, dsn=dsn,
        model_base_url="http://127.0.0.1:8000/v1", model_api_key="test-model-key",
        model="test-model", model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024, model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/qiyu-vl/test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B", embedding_dimensions=4096,
        onebot_self_id="123456789", onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609",), group_trigger_mode="MENTION",
        private_enabled=True, shell_administrator_ids=("42",),
        agent_system_prompt="Joint Host driver lifecycle fixture.",
    )


async def _select_consumer(bot: BotHost, snapshot, staging: PackageCatalog) -> None:
    approved = tuple(snapshot.component["capabilities"]["requests"])
    archive = staging.export_archive(snapshot.digest)
    bot.package_catalog.publish_reviewed_bot_archive(
        archive,
        expected_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
        expected_package_digest=snapshot.digest,
        expected_manifest_digest=digest_json(snapshot.manifest),
        expected_package_id=snapshot.manifest["package_id"],
        expected_component_id=snapshot.component["id"],
        expected_runtime="python-worker",
    )
    await bootstrap_installed_package(
        snapshot.digest, bot.package_catalog, bot.store,
        bot.schema_bundle_installer, kernel=bot._kernel,
        initial_configuration={"label": "reviewed", "multiplier": 1},
        approved_capabilities=approved,
        effect_driver_registry=bot._official_effect_drivers,
    )
    before = bot.registry.graph_identity
    payload = {
        "component_id": "forest.counter", "source_kind": "installed",
        "package_digest": snapshot.digest,
        "manifest_digest": digest_json(snapshot.manifest),
        "approved_capabilities": list(approved), "desired_enabled": True,
        "expected_configuration_revision": 1,
        "expected_selection_revision": 0,
        "expected_graph_revision": before.graph_revision,
    }
    contract = bot.registry.get("component.worker-host").management_contract
    preview = await contract.query("validate", payload)
    await contract.action(
        "module.enable", operation_id=f"selection/{bot.settings.bot_id}",
        payload={**payload, "plan_digest": preview["plan_digest"]},
        authorization=SimpleNamespace(actor_ref="subject/host-admin"),
        authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
    )
    async with asyncio.timeout(180):
        while True:
            active = next(
                (item for item in bot.registry.descriptors
                 if item.component_id == "forest.counter"), None
            )
            latest = (await bot._kernel.runtime.status())["latest_replacement"]
            if latest is not None and latest["state"] == "FAILED":
                pytest.fail(f"Consumer package selection failed: {latest}")
            if (active is not None and active.artifact.package_digest == snapshot.digest
                    and latest is not None and latest["state"] == "SUCCEEDED"):
                return
            await asyncio.sleep(0.2)


@pytest.mark.postgres
async def test_joint_host_driver_replaces_two_consumers_but_not_unrelated_bot(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    names = ["forest_joint_" + uuid4().hex for _ in range(4)]
    parsed = urlsplit(dsn)
    uris = [urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
            for name in names]
    store = None
    supervisor = None
    controller = None
    bots = []
    try:
        for name in names:
            await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        store = HostStore(uris[0], "host/" + uuid4().hex)
        await store.open()
        supervisor = HostSupervisor(HostIdentityAuthority(store))
        await supervisor.start()
        tickets = WebAuthorityTicketAuthority(
            issuer_id="deployment/test", active_key_id="key/joint-test",
            key_resolver=lambda _key_id: bytes(range(32)),
        )
        changes = HostGraphChangeAuthority(store, ticket_authority=tickets)
        gate = ControlDriverBindingGate()
        source_v1 = tmp_path / "host-v1"
        _exporting_host_source(source_v1)
        _holdable_host_driver(source_v1)
        host_catalog = PackageCatalog(tmp_path / "host-catalog")
        host_catalog.build_official(source_v1, scope="host")
        controller = HostPackageController(
            HostRuntimeScope(store, supervisor, graph_change_authority=changes),
            host_catalog, official_sources=(source_v1,),
            driver_binding_gate=gate,
            joint_driver_transition=execute_joint_host_driver_transition,
        )
        await controller.start()
        old_source = controller.driver_exports()[0]["source_ref"]

        bot_source = tmp_path / "bot-consumer"
        _consuming_bot_source(bot_source, source_v1)
        staging = PackageCatalog(tmp_path / "bot-staging")
        consumer_package = staging.build(bot_source)
        for index, bot_id in enumerate(("joint-consumer-a", "joint-consumer-b", "joint-unrelated")):
            settings = _settings(bot_id, uris[index + 1])

            async def create_websocket(_url, _token, *, _self_id=settings.onebot_self_id, **_limits):
                return FakeWebSocketClient(_self_id)

            bot = BotHost(
                settings,
                model_client_factory=FakeModelClient,
                embedding_client_factory=FakeEmbeddingClient,
                media_client_factory=FakeMediaClient,
                vector_index_client_factory=FakeMilvusClient,
                vector_index_audit_client_factory=FakeMilvusAuditClient,
                websocket_client_factory=create_websocket,
                package_catalog=PackageCatalog(tmp_path / f"catalog-{bot_id}"),
                driver_binding_gate=gate,
                host_driver_controller_provider=lambda: controller,
            )
            bots.append(bot)
            await bot.start()
            if index < 2:
                await _select_consumer(bot, consumer_package, staging)

        before_consumers = [bot.registry.graph_identity for bot in bots[:2]]
        unrelated_before = bots[2].registry.graph_identity
        for bot in bots[:2]:
            descriptor = next(item for item in bot.registry.descriptors
                              if item.component_id == "forest.counter")
            assert ("tests.host.driver", "tests.host.exported-driver", old_source) in (
                descriptor.effect_driver_bindings
            )

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
        previous_host = controller.registry.graph_identity
        original_joint = controller._joint_driver_transition

        async def reject_host_prewarm(
            target, previous, candidate, snapshots, affected, consumers,
        ):
            async def fail_prewarm():
                raise ForestError(
                    "TEST_HOST_PREWARM_FAILURE", "Injected Host candidate failure", 503,
                )

            candidate.prewarm = fail_prewarm
            return await original_joint(
                target, previous, candidate, snapshots, affected, consumers,
            )

        controller._joint_driver_transition = reject_host_prewarm
        try:
            with pytest.raises(ForestError) as rejected_host:
                await controller.replace(
                    "host.sample", reviewed.digest,
                    expected_graph_revision=previous_host.graph_revision,
                )
            assert rejected_host.value.code == "TEST_HOST_PREWARM_FAILURE"
        finally:
            controller._joint_driver_transition = original_joint
        assert controller.registry.graph_identity == previous_host
        assert [bot.registry.graph_identity for bot in bots[:2]] == before_consumers
        assert bots[2].registry.graph_identity == unrelated_before

        original_assemble_bot = bots[0]._assemble_candidate

        async def reject_bot_prepare(*args, **kwargs):
            assembled = await original_assemble_bot(*args, **kwargs)

            async def fail_prepare():
                raise ForestError(
                    "TEST_BOT_PREPARE_FAILURE", "Injected Bot candidate failure", 503,
                )

            assembled.registry.prepare = fail_prepare
            return assembled

        bots[0]._assemble_candidate = reject_bot_prepare
        try:
            with pytest.raises(ForestError) as rejected_bot:
                await controller.replace(
                    "host.sample", reviewed.digest,
                    expected_graph_revision=previous_host.graph_revision,
                )
            assert rejected_bot.value.code == "TEST_BOT_PREPARE_FAILURE"
        finally:
            bots[0]._assemble_candidate = original_assemble_bot
        assert controller.registry.graph_identity == previous_host
        assert [bot.registry.graph_identity for bot in bots[:2]] == before_consumers
        assert bots[2].registry.graph_identity == unrelated_before

        old_bot_registry = bots[0].registry
        old_bot_graph = old_bot_registry.graph_identity
        route = FrozenServiceRoute(
            service_id="tests.host.driver", version=1,
            caller_component_id="tests.joint",
            provider_component_id="forest.counter",
            provider_generation_id=old_bot_graph.generations["forest.counter"],
            graph_id=old_bot_graph.graph_id,
            graph_revision=old_bot_graph.graph_revision,
            admission_lane="DATA", owned_work=False,
        )
        gate_path = tmp_path / "driver-inflight"
        payload = {"value": "hold:" + str(gate_path)}
        invoke = RuntimeServiceInvocation(
            "joint-driver/inflight/one", 60, "tests.joint", "handoff",
        )
        inflight = None
        drain_entered = asyncio.Event()
        original_drain = old_bot_registry.drain_owned_work_for_replacement

        async def observe_drain(*args, **kwargs):
            nonlocal inflight
            inflight = asyncio.create_task(
                old_bot_registry.contract_router.invoke_external(route, invoke, payload)
            )
            async with asyncio.timeout(15):
                while not gate_path.with_suffix(".entered").exists():
                    if inflight.done():
                        await inflight
                    await asyncio.sleep(0.05)
            drain_entered.set()
            return await original_drain(*args, **kwargs)

        old_bot_registry.drain_owned_work_for_replacement = observe_drain
        replacement_task = asyncio.create_task(controller.replace(
            "host.sample", reviewed.digest,
            expected_graph_revision=previous_host.graph_revision,
        ))
        try:
            await asyncio.wait_for(drain_entered.wait(), timeout=180)
            await asyncio.sleep(0.2)
            assert not replacement_task.done()
            assert inflight is not None and not inflight.done()
        finally:
            gate_path.with_suffix(".release").write_text("release", encoding="utf-8")
        assert await inflight == payload
        replaced = await replacement_task
        assert gate_path.with_suffix(".entered").read_text(
            encoding="utf-8"
        ).splitlines() == ["entered"]
        with pytest.raises(ForestError):
            await old_bot_registry.contract_router.invoke_external(
                route, RuntimeServiceInvocation(
                    "joint-driver/inflight/stale", 5, "tests.joint", "handoff",
                ), payload,
            )
        new_source = controller.driver_exports()[0]["source_ref"]
        assert new_source != old_source
        assert replaced.graph_identity.graph_revision > previous_host.graph_revision
        results = await changes.consumer_results(replaced.activation_id)
        assert [(row["bot_id"], row["state"]) for row in results] == [
            ("joint-consumer-a", "SUCCEEDED"),
            ("joint-consumer-b", "SUCCEEDED"),
        ]
        for index, bot in enumerate(bots[:2]):
            graph = bot.registry.graph_identity
            assert graph.graph_revision > before_consumers[index].graph_revision
            assert graph.generations["forest.counter"] != (
                before_consumers[index].generations["forest.counter"]
            )
            descriptor = next(item for item in bot.registry.descriptors
                              if item.component_id == "forest.counter")
            assert ("tests.host.driver", "tests.host.exported-driver", new_source) in (
                descriptor.effect_driver_bindings
            )
            assert old_source not in {
                source for item in bot.registry.descriptors
                for _service, _kind, source in item.effect_driver_bindings
            }
        assert bots[2].registry.graph_identity == unrelated_before

        source_v3 = tmp_path / "host-v3"
        copytree(source_v2, source_v3)
        manifest = source_v3 / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.1"', 'version = "1.0.2"', 1,
            ), encoding="utf-8",
        )
        postcommit = reviewed_staging.build(source_v3)
        archive = reviewed_staging.export_archive(postcommit.digest)
        host_catalog.publish_reviewed_control_archive(
            archive, scope="host",
            expected_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest(),
            expected_package_digest=postcommit.digest,
            expected_manifest_digest=digest_json(postcommit.manifest),
            expected_package_id=postcommit.manifest["package_id"],
            expected_component_id=postcommit.component["id"],
            expected_runtime="python-worker",
        )
        published_host = controller.registry
        published_bots = [bot.registry for bot in bots[:2]]
        postcommit_activation_id = str(uuid4())
        original_assemble_for_activation = bots[0]._assemble_candidate

        async def reject_bot_activation(*args, **kwargs):
            assembled = await original_assemble_for_activation(*args, **kwargs)

            async def fail_prewarm():
                raise ForestError(
                    "TEST_BOT_ACTIVATION_FAILURE", "Injected Bot activation failure", 503,
                )

            assembled.registry.prewarm = fail_prewarm
            return assembled

        bots[0]._assemble_candidate = reject_bot_activation
        try:
            with pytest.raises(ForestError) as rejected_activation:
                await controller.replace(
                    "host.sample", postcommit.digest,
                    expected_graph_revision=published_host.graph_identity.graph_revision,
                    activation_id=postcommit_activation_id,
                )
            assert rejected_activation.value.code == (
                "CONTROL_DRIVER_JOINT_POST_PUBLICATION_FAILURE"
            )
            assert rejected_activation.value.details["cause"] == (
                "TEST_BOT_ACTIVATION_FAILURE"
            )
        finally:
            bots[0]._assemble_candidate = original_assemble_for_activation
        assert controller.fatal_error is rejected_activation.value
        assert controller.fatal_event.is_set()
        assert controller._registry.graph_identity.graph_revision > (
            published_host.graph_identity.graph_revision
        )
        assert published_host._closed
        assert [bot.registry for bot in bots[:2]] == published_bots
        assert all(bot._fatal_replacement_error is rejected_activation.value
                   for bot in bots[:2])
        assert bots[2].registry.graph_identity == unrelated_before
        assert [(row["bot_id"], row["state"]) for row in
                await changes.consumer_results(postcommit_activation_id)] == [
            ("joint-consumer-a", "FAILED"),
            ("joint-consumer-b", "FAILED"),
        ]
        assert rejected_activation.value.details["ledger_errors"] == []
        assert rejected_activation.value.details["cleanup_errors"] == []
    finally:
        for bot in reversed(bots):
            await bot.close()
        if controller is not None:
            await controller.close()
        if supervisor is not None:
            await supervisor.close()
        if store is not None:
            await store.close()
        for name in reversed(names):
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)
            ))
        await admin.close()
