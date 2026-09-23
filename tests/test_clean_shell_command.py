from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.builtin_agent_shell import AGENT_SHELL_COMMANDS, execute_agent_shell
from forest.builtin_audit_shell import AUDIT_SHELL_COMMANDS, audit_shell_service
from forest.builtin_memory_shell import MEMORY_SHELL_COMMANDS, execute_memory_shell
from forest.builtin_runtime_shell import RUNTIME_SHELL_COMMANDS, runtime_shell_service
from forest.errors import ForestError
from forest.runtime_modules import RuntimeShellCommandContribution, official_storage_bundle
from forest.shell_command_parser import ShellCommandParser
from forest.shell_command_protocol import ShellCommandExecutionRequest
from forest.shell_gate import CoreShellGate
from forest.storage.adapter_onebot_authority import (
    OneBotAdapterRepository,
    OneBotPayloadRef,
    OneBotPlatformEvent,
)
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.conversation_scheduler import ConversationSchedulerRepository
from forest.storage.ingress_unit_of_work import OneBotIngressUnitOfWork, ShellIngressCommand
from forest.storage.kernel_audit import KernelAuditRepository
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller
from forest.storage.shell_command_authority import (
    ShellCommandRepository,
    ShellCommandRoute,
    ShellPayloadRef,
    ShellPolicyGrant,
)

SCHEMA = "sha256:" + "1" * 64
CAPABILITIES = "sha256:" + "2" * 64
GRAPH = "sha256:" + "3" * 64
FENCE = "sha256:" + "4" * 64


def test_core_maintenance_parser_requires_explicit_positive_epoch():
    parser = ShellCommandParser(None, RUNTIME_SHELL_COMMANDS)
    paused = parser.parse("core pause --expect-maintenance 7")
    resumed = parser.parse("core resume --expect-maintenance=8")
    assert (paused.command_id, paused.document["arguments"]) == (
        "core.pause",
        {"expected_maintenance_epoch": 7},
    )
    assert (resumed.command_id, resumed.document["arguments"]) == (
        "core.resume",
        {"expected_maintenance_epoch": 8},
    )
    assert parser.parse("core pause").command_id == "core.pause"
    assert parser.parse("core resume --expect-maintenance 0").document["arguments"] == {
        "error_code": "SHELL_COMMAND_NOT_FOUND"
    }


def test_core_reload_parser_requires_exact_graph_revision():
    parser = ShellCommandParser(None, RUNTIME_SHELL_COMMANDS)
    parsed = parser.parse("core reload --expect-graph 17")
    assert parsed.document["arguments"] == {"expected_graph_revision": 17}
    assert parser.parse("core reload").document["arguments"] == {
        "error_code": "SHELL_COMMAND_NOT_FOUND"
    }


@pytest.mark.asyncio
async def test_core_reload_shell_only_persists_supervisor_request():
    class Runtime:
        async def request_graph_replacement(self, lease, **values):
            assert lease == "host-lease/one"
            assert values == {
                "operation_id": "shell-reload:request/reload",
                "expected_graph_revision": 17,
                "requested_by": "qq:42",
            }
            return {
                **values,
                "state": "QUEUED",
                "activation_id": "00000000-0000-0000-0000-000000000017",
            }

    context = SimpleNamespace(
        host_lease="host-lease/one",
        participant=lambda component_id: SimpleNamespace(service=Runtime())
        if component_id == "kernel.runtime"
        else None,
    )
    execute = runtime_shell_service(context)
    result = await execute(
        ShellCommandExecutionRequest(
            "core.reload",
            1,
            {"expected_graph_revision": 17},
            "request/reload",
            "qq:42",
            "onebot:group:933179609",
        )
    )
    assert "reload QUEUED" in result.text


def test_module_list_parser_is_exact_and_argument_free():
    parser = ShellCommandParser(None, RUNTIME_SHELL_COMMANDS)
    parsed = parser.parse("module list")
    assert parsed.command_id == "module.list"
    assert parsed.command_version == 1
    assert parsed.document == {
        "protocol": "forest.shell.arguments/1",
        "command_id": "module.list",
        "arguments": {},
    }
    assert parser.parse("module list extra").document["arguments"] == {
        "error_code": "SHELL_COMMAND_NOT_FOUND"
    }


def test_parser_consumes_module_contribution_without_hardcoded_command_branch():
    contribution = RuntimeShellCommandContribution(
        "plugin.echo",
        3,
        "plugin.echo.execute",
        2,
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "protocol": {"const": "forest.shell.arguments/1"},
                "command_id": {"const": "plugin.echo"},
                "arguments": {
                    "type": "object",
                    "properties": {"message": {"type": "string", "minLength": 1}},
                    "required": ["message"],
                    "additionalProperties": False,
                },
            },
            "required": ["protocol", "command_id", "arguments"],
            "additionalProperties": False,
        },
        "plugin.echo",
        "BOT",
        "P1",
        ("plugin.echo",),
    )
    parser = ShellCommandParser(None, (contribution,))

    parsed = parser.parse('plugin echo --message "hello forest"')

    assert parsed.command_id == "plugin.echo"
    assert parsed.command_version == 3
    assert parsed.document["arguments"] == {"message": "hello forest"}


def test_memory_commands_are_derived_from_memory_owned_schemas():
    parser = ShellCommandParser(None, MEMORY_SHELL_COMMANDS)

    sources = parser.parse(
        "memory sources --trunk-id trunk/933179609 --modality IMAGE --limit 7"
    )
    relations = parser.parse(
        "memory relations --endpoint-kind SOURCE_UNIT --endpoint-id unit/42"
    )

    assert sources.document["arguments"] == {
        "trunk_id": "trunk/933179609",
        "modality": "IMAGE",
        "limit": 7,
    }
    assert relations.document["arguments"] == {
        "endpoint_kind": "SOURCE_UNIT",
        "endpoint_id": "unit/42",
    }
    assert parser.parse("memory sources --limit 11").document["arguments"] == {
        "error_code": "SHELL_COMMAND_NOT_FOUND"
    }


def test_audit_tail_parser_is_derived_from_kernel_audit_schema():
    parser = ShellCommandParser(None, AUDIT_SHELL_COMMANDS)

    parsed = parser.parse(
        "audit tail --action shell.command.succeeded --resource-kind bot "
        "--resource-id bot-shell --limit 7"
    )

    assert parsed.document["arguments"] == {
        "action": "shell.command.succeeded",
        "resource_kind": "bot",
        "resource_id": "bot-shell",
        "limit": 7,
    }
    assert parser.parse("audit tail --limit 11").document["arguments"] == {
        "error_code": "SHELL_COMMAND_NOT_FOUND"
    }


def test_agent_run_commands_are_derived_from_agent_owned_schemas():
    parser = ShellCommandParser(None, AGENT_SHELL_COMMANDS)

    inspected = parser.parse("run inspect --run-id run/42")
    cancelled = parser.parse(
        "run cancel --run-id run/42 --expect-checkpoint 7 --reason ADMIN_REQUESTED"
    )

    assert inspected.document["arguments"] == {"run_id": "run/42"}
    assert cancelled.document["arguments"] == {
        "run_id": "run/42",
        "expected_checkpoint_seq": 7,
        "reason_code": "ADMIN_REQUESTED",
    }
    assert parser.parse("run cancel --run-id run/42").document["arguments"] == {
        "error_code": "SHELL_COMMAND_NOT_FOUND"
    }


@pytest.mark.asyncio
async def test_agent_run_shell_uses_agent_runtime_control_port():
    class Runtime:
        def __init__(self):
            self.cancel = None

        async def load(self, run_id):
            assert run_id == "run/42"
            return {
                "run_id": run_id,
                "state": "PREPARING_MODEL",
                "checkpoint_seq": 7,
                "input_revision": 2,
                "session_id": "session/42",
                "session": {"conversation_ref": "onebot:group:933179609"},
                "cancel_request": None,
            }

        async def request_cancel(self, **values):
            self.cancel = values
            return {
                "run_id": values["run_id"],
                "expected_checkpoint_seq": values["expected_checkpoint_seq"],
                "operation_id": values["operation_id"],
            }

    runtime = Runtime()
    inspected = await execute_agent_shell(
        runtime,
        ShellCommandExecutionRequest(
            "run.inspect",
            1,
            {"run_id": "run/42"},
            "request/inspect",
            "qq:3244658766",
            "onebot:group:933179609",
        ),
    )
    cancelled = await execute_agent_shell(
        runtime,
        ShellCommandExecutionRequest(
            "run.cancel",
            1,
            {"run_id": "run/42", "expected_checkpoint_seq": 7},
            "request/cancel",
            "qq:3244658766",
            "onebot:group:933179609",
        ),
    )

    assert "state=PREPARING_MODEL checkpoint=7" in inspected.text
    assert "conversation=onebot:group:933179609 cancel=none" in inspected.text
    assert "cancellation accepted" in cancelled.text
    assert runtime.cancel == {
        "operation_id": "shell-cancel:request/cancel",
        "run_id": "run/42",
        "expected_checkpoint_seq": 7,
        "requested_by": "qq:3244658766",
        "reason_code": "ADMIN_REQUESTED",
    }


@pytest.mark.asyncio
async def test_audit_shell_uses_module_query_port_and_emits_cursor():
    class Audit:
        async def scan(self, **arguments):
            assert arguments == {"limit": 2, "action": "shell.command.succeeded"}
            return {
                "records": [
                    {
                        "created_at": "2026-09-20T08:00:00+00:00",
                        "severity": "INFO",
                        "action": "shell.command.succeeded",
                        "producer_component_id": "shell.command",
                        "actor_kind": "principal",
                        "actor_id": "qq:3244658766",
                        "resource_kind": "bot",
                        "resource_id": "bot-shell",
                        "operation_kind": "shell.request",
                        "operation_id": "request/1",
                        "audit_id": "audit:1",
                    }
                ],
                "next_before": "audit:1",
            }

    execute = audit_shell_service(Audit())
    result = await execute(
        ShellCommandExecutionRequest(
            "audit.tail",
            1,
            {"action": "shell.command.succeeded", "limit": 2},
            "request/query",
            "qq:3244658766",
            "onebot:group:933179609",
        )
    )

    assert "producer=shell.command" in result.text
    assert "actor=principal:qq:3244658766" in result.text
    assert "next: --before audit:1" in result.text


@pytest.mark.asyncio
async def test_memory_relation_shell_output_names_evidence_groups():
    class Repository:
        async def query_relations(self, **arguments):
            assert arguments == {
                "endpoint_kind": "SOURCE_UNIT",
                "endpoint_id": "unit/a",
                "limit": 2,
            }
            return {
                "items": [
                    {
                        "relation_id": "relation/a-b",
                        "relation_kind": "ASSOCIATES_WITH",
                        "left_kind": "SOURCE_UNIT",
                        "left_id": "unit/a",
                        "right_kind": "SOURCE_UNIT",
                        "right_id": "unit/b",
                        "evidence": [
                            {"external_group_id": "933179609"},
                            {"external_group_id": "200000001"},
                        ],
                    }
                ],
                "has_more": False,
            }

    memory = type("Memory", (), {"repository": Repository()})()
    result = await execute_memory_shell(
        memory,
        ShellCommandExecutionRequest(
            "memory.relations",
            1,
            {"endpoint_kind": "SOURCE_UNIT", "endpoint_id": "unit/a", "limit": 2},
            "request/relations",
            "qq:42",
            "onebot:group:933179609",
        ),
    )

    assert "relation/a-b ASSOCIATES_WITH" in result.text
    assert "来源群 200000001,933179609" in result.text


async def _parse(*, command_text, command_id, command_version, argument_schema_digest, arguments):
    expected = {
        "core cancel": "core.cancel",
        "core status": "core.status",
        "module list": "module.list",
    }.get(command_text)
    return (
        expected == command_id
        and command_version == 1
        and argument_schema_digest == SCHEMA
        and arguments.owner_kind == "shell.arguments"
    )


@pytest_asyncio.fixture
async def clean_shell():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_shell_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = CleanBotStore(uri, "bot-shell")
    await store.open()
    installer = SchemaBundleInstaller(store)
    await installer.initialize_kernel()
    bundles = [
        official_storage_bundle(
            "kernel.payload",
            "forest.kernel.payload",
            "forest.kernel-payload.repository/1",
            "storage/official/kernel_payload.sql",
        ),
        official_storage_bundle(
            "kernel.audit",
            "forest.kernel.audit",
            "forest.kernel-audit.repository/1",
            "storage/official/kernel_audit.sql",
        ),
        official_storage_bundle(
            "adapter.onebot",
            "forest.adapter.onebot",
            "forest.adapter-onebot.repository/1",
            "storage/official/adapter_onebot.sql",
        ),
        official_storage_bundle(
            "conversation.scheduler",
            "forest.conversation.scheduler",
            "forest.conversation-scheduler.repository/1",
            "storage/official/conversation_scheduler.sql",
        ),
        official_storage_bundle(
            "shell.command",
            "forest.shell.command",
            "forest.shell-command.repository/1",
            "storage/official/shell_command.sql",
        ),
    ]
    for bundle in bundles:
        await installer.install(
            bundle.spec,
            bundle.baseline,
            bundle.package_digest,
            execution_profile=bundle.execution_profile,
        )
    storage = {item.spec.component_id: item for item in bundles}
    payloads = KernelPayloadRepository(store, namespace=storage["kernel.payload"].spec.namespace)

    async def verify_adapter_generation(
        _conn, *, component_id, generation_id, admission_lane
    ):
        assert (component_id, generation_id) == (
            "adapter.onebot",
            "generation/adapter/1",
        )
        assert admission_lane in {"DATA", "CONTROL"}
        return {"active": True}

    adapter = OneBotAdapterRepository(
        store,
        namespace=storage["adapter.onebot"].spec.namespace,
        payload_verifier=payloads.verify_in,
        generation_verifier=verify_adapter_generation,
    )
    scheduler = ConversationSchedulerRepository(
        store,
        namespace=storage["conversation.scheduler"].spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    gate = CoreShellGate(store.bot_id)

    async def verify_generation(
        _conn, *, component_id, generation_id, graph_digest, route_digest
    ):
        assert component_id and generation_id and graph_digest == GRAPH and route_digest
        return {"active": True}

    async def consume_authorization(_conn, **values):
        assert values["action"] == "management.invoke"
        assert values["resource_kind"] == "bot"
        assert values["resource_id"] == "bot-shell"
        assert values["plan_digest"] == CAPABILITIES
        return {"ticket_id": values["ticket_id"], "accepted": True}

    shell = ShellCommandRepository(
        store,
        namespace=storage["shell.command"].spec.namespace,
        payload_verifier=payloads.verify_in,
        proof_verifier=gate.verify,
        admission_parser=_parse,
        binding_fence_verifier=adapter.verify_transport_fence_in,
        generation_verifier=verify_generation,
        authorization_consumer=consume_authorization,
    )
    try:
        yield store, storage, payloads, adapter, gate, shell, scheduler
    finally:
        await store.close()
        assert name.startswith("forest_shell_") and len(name) == 45
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _put(payloads, *, owner_id: str, kind: str, body: bytes):
    saved = await payloads.put_bytes(
        owner_component_id="shell.command" if kind.startswith("shell.") else "adapter.onebot",
        owner_kind=kind,
        owner_id=owner_id,
        media_type="application/json",
        content=body,
        sources=(
            PayloadSource(
                source_component_id="test.fixture",
                source_kind="test.source",
                source_id=owner_id,
                source_revision=1,
            ),
        ),
        expected_deletion_epoch=await payloads.epoch(),
    )
    return saved


async def _active_binding(payloads, adapter):
    display = await _put(
        payloads,
        owner_id="binding/main",
        kind="binding.display",
        body=b'{"display_name":"Forest"}',
    )
    await adapter.register_binding(
        operation_id="binding/register",
        binding_id="binding/main",
        self_id="123456789",
        display=OneBotPayloadRef(
            display.payload_id,
            display.content_digest,
            "adapter.onebot",
            "binding.display",
            "binding/main",
        ),
    )
    config = await _put(
        payloads,
        owner_id="binding/plan/1",
        kind="binding.config",
        body=b'{"transport":"FORWARD_WEBSOCKET"}',
    )
    await adapter.plan_configuration(
        plan_id="binding/plan/1",
        binding_id="binding/main",
        expected_revision=0,
        enabled=True,
        receive_enabled=True,
        transport="FORWARD_WEBSOCKET",
        config=OneBotPayloadRef(
            config.payload_id,
            config.content_digest,
            "adapter.onebot",
            "binding.config",
            "binding/plan/1",
        ),
        secret_ref="secret://onebot/main",
        secret_digest="sha256:" + "5" * 64,
        actor_principal_ref="admin/root",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    revision = await adapter.apply_configuration(
        operation_id="binding/apply/1", plan_id="binding/plan/1"
    )
    await adapter.prepare_client(
        client_incarnation="client/main/1",
        binding_id="binding/main",
        revision=revision["revision"],
        expected_binding_epoch=0,
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest=FENCE,
    )
    return await adapter.publish_client(
        operation_id="binding/publish/1",
        binding_id="binding/main",
        client_incarnation="client/main/1",
        revision=revision["revision"],
        expected_binding_epoch=0,
    )


def _route(command_id: str, service_class: str):
    return ShellCommandRoute(
        command_id=command_id,
        command_version=1,
        owner_component_id="kernel.runtime" if command_id.startswith("core.") else "component.worker-host",
        owner_generation_id="generation/owner/1",
        argument_schema_digest=SCHEMA,
        management_operation_id="operation/" + command_id,
        target_scope="BOT",
        service_class=service_class,
        required_capabilities_digest=CAPABILITIES,
        output_policy="SHORT",
        timeout_seconds=30,
    )


async def _ready(clean_shell, grants):
    _store, _storage, payloads, adapter, _gate, shell, _scheduler = clean_shell
    publication = await _active_binding(payloads, adapter)
    await shell.publish_registry(
        operation_id="registry/1",
        expected_revision=0,
        graph_digest=GRAPH,
        routes=(
            _route("core.cancel", "P0"),
            _route("core.status", "P0"),
            _route("module.list", "P1"),
        ),
    )
    await shell.publish_policy(
        operation_id="policy/1",
        binding_ref="binding/main",
        expected_revision=0,
        actor_principal_ref="admin/root",
        grants=grants,
    )
    return publication


async def _admit(
    clean_shell,
    publication,
    *,
    suffix: str,
    principal: str,
    text: str,
    command_id: str,
    now: datetime | None = None,
):
    _store, _storage, payloads, _adapter, gate, shell, _scheduler = clean_shell
    match = gate.inspect(
        text="#forest " + text,
        binding_ref="binding/main",
        binding_epoch=publication["resulting_binding_epoch"],
        connection_epoch=publication["resulting_connection_epoch"],
        client_incarnation="client/main/1",
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest=FENCE,
        conversation_ref="conversation/main",
        event_ref="event/" + suffix,
        principal_ref=principal,
    )
    assert match is not None
    saved = await _put(
        payloads,
        owner_id="request/" + suffix,
        kind="shell.arguments",
        body=("{\"suffix\":\"" + suffix + "\"}").encode(),
    )
    now = now or datetime.now(timezone.utc)
    return await shell.admit_command(
        operation_id="admit/" + suffix,
        request_id="request/" + suffix,
        proof=match.proof,
        command_id=command_id,
        command_version=1,
        arguments=ShellPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "shell.command",
            "shell.arguments",
            "request/" + suffix,
        ),
        idempotency_key="idempotency/" + suffix,
        received_at=now,
        admission_deadline=now + timedelta(seconds=30),
    )


async def _platform_event(payloads, suffix: str, principal: str):
    event_id = "platform/" + suffix
    raw = await _put(
        payloads,
        owner_id=event_id,
        kind="onebot.raw-event",
        body=b'{"post_type":"message"}',
    )
    normalized = await _put(
        payloads,
        owner_id=event_id,
        kind="onebot.normalized-event",
        body=b'{"segments":[{"type":"text","text":"#forest core status"}]}',
    )
    author = await _put(
        payloads,
        owner_id=event_id,
        kind="onebot.author-snapshot",
        body=("{\"principal_ref\":\"" + principal + "\"}").encode(),
    )
    now = datetime.now(timezone.utc)
    def ref(saved, kind):
        return OneBotPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "adapter.onebot",
            kind,
            event_id,
        )
    return OneBotPlatformEvent(
        event_id=event_id,
        platform_message_ref="message/" + suffix,
        event_kind="group.message",
        channel_kind="GROUP",
        channel_ref="933179609",
        conversation_ref="conversation/main",
        author_subject_ref=principal,
        raw=ref(raw, "onebot.raw-event"),
        normalized=ref(normalized, "onebot.normalized-event"),
        author=ref(author, "onebot.author-snapshot"),
        sent_at=now - timedelta(milliseconds=10),
        received_at=now,
        classification_deadline=now + timedelta(minutes=1),
    )


@pytest.mark.postgres
async def test_shell_schema_owns_policy_registry_requests_and_no_inline_body(clean_shell):
    _store, storage, *_rest = clean_shell
    baseline = storage["shell.command"].baseline.lower()
    for expected in (
        b"policy_heads",
        b"policy_grants",
        b"command_routes",
        b"shell_requests",
        b"claim_operations",
        b"request_results",
    ):
        assert expected in baseline
    for forbidden in (b"jsonb", b"admin_user_ids", b"access_token", b"socket"):
        assert forbidden not in baseline


@pytest.mark.postgres
async def test_policy_and_registry_revision_races_converge(clean_shell):
    _store, _storage, _payloads, _adapter, _gate, shell, _scheduler = clean_shell

    async def policy(operation_id, principal):
        try:
            return await shell.publish_policy(
                operation_id=operation_id,
                binding_ref="binding/race",
                expected_revision=0,
                actor_principal_ref="admin/root",
                grants=(ShellPolicyGrant("BINDING", principal, True),),
            )
        except ForestError as exc:
            return exc.code

    left, right = await asyncio.gather(
        policy("policy/race/left", "admin/left"),
        policy("policy/race/right", "admin/right"),
    )
    assert [isinstance(value, dict) for value in (left, right)].count(True) == 1
    assert "SHELL_POLICY_CONFLICT" in {value for value in (left, right) if isinstance(value, str)}

    async def registry(operation_id, digest):
        try:
            return await shell.publish_registry(
                operation_id=operation_id,
                expected_revision=0,
                graph_digest=digest,
                routes=(_route("core.status", "P0"),),
            )
        except ForestError as exc:
            return exc.code

    first, second = await asyncio.gather(
        registry("registry/race/left", "sha256:" + "6" * 64),
        registry("registry/race/right", "sha256:" + "7" * 64),
    )
    assert [isinstance(value, dict) for value in (first, second)].count(True) == 1
    assert "SHELL_REGISTRY_CONFLICT" in {
        value for value in (first, second) if isinstance(value, str)
    }


@pytest.mark.postgres
async def test_gate_only_reserves_prefix_and_shell_policy_decides_authority(clean_shell):
    _store, _storage, _payloads, _adapter, gate, shell, _scheduler = clean_shell
    assert gate.inspect(
        text="hello",
        binding_ref="binding/main",
        binding_epoch=1,
        connection_epoch=1,
        client_incarnation="client/main/1",
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest=FENCE,
        conversation_ref="conversation/main",
        event_ref="event/ordinary",
        principal_ref="user/ordinary",
    ) is None
    publication = await _ready(clean_shell, ())
    admission_time = datetime.now(timezone.utc)
    rejected = await _admit(
        clean_shell,
        publication,
        suffix="not-admin",
        principal="user/not-admin",
        text="core cancel",
        command_id="core.cancel",
        now=admission_time,
    )
    assert rejected["state"] == "REJECTED"
    assert rejected["rejection_code"] == "SHELL_FORBIDDEN"
    assert rejected["effective_service_class"] == "P1"
    replay = await _admit(
        clean_shell,
        publication,
        suffix="not-admin",
        principal="user/not-admin",
        text="core cancel",
        command_id="core.cancel",
        now=admission_time,
    )
    assert replay["request_id"] == rejected["request_id"]
    assert replay["input_digest"] == rejected["input_digest"]
    assert await shell.claim(operation_id="claim/none", worker_id="worker/1") is None


@pytest.mark.postgres
async def test_adapter_conversation_and_shell_share_one_ingress_unit_of_work(clean_shell):
    publication = await _ready(
        clean_shell,
        (ShellPolicyGrant("BINDING", "admin/main", True),),
    )
    store, storage, payloads, adapter, gate, shell, scheduler = clean_shell
    event = await _platform_event(payloads, "atomic", "admin/main")
    match = gate.inspect(
        text="#forest core status",
        binding_ref="binding/main",
        binding_epoch=publication["resulting_binding_epoch"],
        connection_epoch=publication["resulting_connection_epoch"],
        client_incarnation="client/main/1",
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest=FENCE,
        conversation_ref=event.conversation_ref,
        event_ref=event.event_id,
        principal_ref=event.author_subject_ref,
    )
    assert match is not None
    arguments = await _put(
        payloads,
        owner_id="request/atomic",
        kind="shell.arguments",
        body=b'{}',
    )
    command = ShellIngressCommand(
        request_id="request/atomic",
        proof=match.proof,
        command_id="core.status",
        command_version=1,
        arguments=ShellPayloadRef(
            arguments.payload_id,
            arguments.content_digest,
            "shell.command",
            "shell.arguments",
            "request/atomic",
        ),
        idempotency_key="idempotency/atomic",
    )
    ingress = OneBotIngressUnitOfWork(
        store, adapter=adapter, conversation=scheduler, shell=shell
    )
    first, second = await asyncio.gather(
        ingress.admit(
            operation_id="platform-admit/atomic/a",
            binding_id="binding/main",
            expected_binding_epoch=publication["resulting_binding_epoch"],
            client_incarnation="client/main/1",
            event=event,
            shell_command=command,
        ),
        ingress.admit(
            operation_id="platform-admit/atomic/b",
            binding_id="binding/main",
            expected_binding_epoch=publication["resulting_binding_epoch"],
            client_incarnation="client/main/1",
            event=event,
            shell_command=command,
        ),
    )
    assert {first["adapter"]["duplicate"], second["adapter"]["duplicate"]} == {False, True}
    assert first["shell"]["state"] == second["shell"]["state"] == "QUEUED"
    assert first["classification"]["decision"] == "CONTROL_COMMAND"
    assert second["classification"]["decision"] == "CONTROL_COMMAND"
    assert first["conversation"]["ingress_seq"] == second["conversation"]["ingress_seq"] == 1

    failed_event = await _platform_event(payloads, "rollback", "admin/main")
    failed_match = gate.inspect(
        text="#forest core status",
        binding_ref="binding/main",
        binding_epoch=publication["resulting_binding_epoch"],
        connection_epoch=publication["resulting_connection_epoch"],
        client_incarnation="client/main/1",
        adapter_generation_id="generation/adapter/1",
        executor_fence_digest=FENCE,
        conversation_ref=failed_event.conversation_ref,
        event_ref=failed_event.event_id,
        principal_ref=failed_event.author_subject_ref,
    )
    assert failed_match is not None
    failed_arguments = await _put(
        payloads,
        owner_id="request/rollback",
        kind="shell.arguments",
        body=b'{}',
    )
    failed_command = ShellIngressCommand(
        request_id="request/rollback",
        proof=failed_match.proof,
        command_id="core.status",
        command_version=1,
        arguments=ShellPayloadRef(
            failed_arguments.payload_id,
            failed_arguments.content_digest,
            "shell.command",
            "shell.arguments",
            "request/rollback",
        ),
        idempotency_key="idempotency/rollback",
    )

    class FailingShell:
        @staticmethod
        async def admit_command_in(_conn, **_values):
            raise ForestError("TEST_SHELL_FAILED", "forced", 503)

    failing = OneBotIngressUnitOfWork(
        store, adapter=adapter, conversation=scheduler, shell=FailingShell()
    )
    with pytest.raises(ForestError) as caught:
        await failing.admit(
            operation_id="platform-admit/rollback",
            binding_id="binding/main",
            expected_binding_epoch=publication["resulting_binding_epoch"],
            client_incarnation="client/main/1",
            event=failed_event,
            shell_command=failed_command,
        )
    assert caught.value.code == "TEST_SHELL_FAILED"
    async with store.pool.connection() as conn:
        adapter_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.platform_events WHERE bot_id=%s").format(
                    sql.Identifier(storage["adapter.onebot"].spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
        inbox_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.inbox_messages WHERE bot_id=%s").format(
                    sql.Identifier(storage["conversation.scheduler"].spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
        shell_count = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.shell_requests WHERE bot_id=%s").format(
                    sql.Identifier(storage["shell.command"].spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
    assert adapter_count["count"] == inbox_count["count"] == shell_count["count"] == 1


@pytest.mark.postgres
async def test_policy_derives_priority_and_claim_prefers_p0(clean_shell):
    publication = await _ready(
        clean_shell,
        (
            ShellPolicyGrant("BINDING", "admin/p0", True),
            ShellPolicyGrant("BINDING", "admin/p1", False),
        ),
    )
    _store, _storage, _payloads, _adapter, _gate, shell, _scheduler = clean_shell
    older_p1 = await _admit(
        clean_shell,
        publication,
        suffix="p1",
        principal="admin/p1",
        text="module list",
        command_id="module.list",
    )
    newer_p0 = await _admit(
        clean_shell,
        publication,
        suffix="p0",
        principal="admin/p0",
        text="core cancel",
        command_id="core.cancel",
    )
    downgraded = await _admit(
        clean_shell,
        publication,
        suffix="downgraded",
        principal="admin/p1",
        text="core status",
        command_id="core.status",
    )
    assert older_p1["effective_service_class"] == "P1"
    assert newer_p0["effective_service_class"] == "P0"
    assert downgraded["requested_service_class"] == "P0"
    assert downgraded["effective_service_class"] == "P1"
    claim = await shell.claim(operation_id="claim/p0", worker_id="worker/1")
    assert claim is not None and claim.request_id == "request/p0"
    replay = await shell.claim(operation_id="claim/p0", worker_id="worker/1")
    assert replay == claim


@pytest.mark.postgres
async def test_claim_rechecks_current_policy_and_result_payload_is_authoritative(clean_shell):
    publication = await _ready(
        clean_shell,
        (ShellPolicyGrant("CONVERSATION", "admin/main", True, "conversation/main"),),
    )
    _store, _storage, payloads, _adapter, _gate, shell, _scheduler = clean_shell
    await _admit(
        clean_shell,
        publication,
        suffix="execute",
        principal="admin/main",
        text="core cancel",
        command_id="core.cancel",
    )
    claim = await shell.claim(operation_id="claim/execute", worker_id="worker/1")
    assert claim is not None
    authority = await shell.authorize_claim(
        claim,
        ticket_id="ticket/execute",
        authorization_operation_id="invoke/execute",
    )
    assert authority["owner_component_id"] == "kernel.runtime"
    await shell.start(claim)
    result = await _put(
        payloads,
        owner_id="request/execute",
        kind="shell.result",
        body=b'{"status":"cancelled"}',
    )
    completed = await shell.complete(
        claim,
        operation_id="result/execute",
        resulting_state="SUCCEEDED",
        result=ShellPayloadRef(
            result.payload_id,
            result.content_digest,
            "shell.command",
            "shell.result",
            "request/execute",
        ),
        authorization_ticket_id="ticket/execute/result",
        authorization_operation_id="invoke/execute/result",
    )
    assert completed["resulting_state"] == "SUCCEEDED"
    assert (await shell.load_request("request/execute"))["state"] == "SUCCEEDED"


@pytest.mark.postgres
async def test_policy_revocation_blocks_claim_before_effect(clean_shell):
    publication = await _ready(
        clean_shell,
        (ShellPolicyGrant("BINDING", "admin/revoked", True),),
    )
    _store, _storage, payloads, _adapter, _gate, shell, _scheduler = clean_shell
    await _admit(
        clean_shell,
        publication,
        suffix="revoked",
        principal="admin/revoked",
        text="core cancel",
        command_id="core.cancel",
    )
    claim = await shell.claim(operation_id="claim/revoked", worker_id="worker/1")
    assert claim is not None
    await shell.publish_policy(
        operation_id="policy/2",
        binding_ref="binding/main",
        expected_revision=1,
        actor_principal_ref="admin/root",
        grants=(),
    )
    with pytest.raises(ForestError) as caught:
        await shell.authorize_claim(
            claim,
            ticket_id="ticket/revoked",
            authorization_operation_id="invoke/revoked",
        )
    assert caught.value.code == "SHELL_FORBIDDEN"
    saved = await _put(
        payloads,
        owner_id="request/revoked",
        kind="shell.result",
        body=b'{"status":"failed"}',
    )
    failed = await shell.fail_claim(
        claim,
        operation_id="result/revoked",
        resulting_state="FAILED",
        result=ShellPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "shell.command",
            "shell.result",
            "request/revoked",
        ),
        error_code="SHELL_FORBIDDEN",
    )
    assert failed["resulting_state"] == "FAILED"
    assert (await shell.load_request("request/revoked"))["state"] == "FAILED"


@pytest.mark.postgres
async def test_malformed_binding_between_text_and_command_is_reserved_rejection(clean_shell):
    publication = await _ready(
        clean_shell,
        (ShellPolicyGrant("BINDING", "admin/main", True),),
    )
    _store, _storage, _payloads, _adapter, _gate, shell, _scheduler = clean_shell
    rejected = await _admit(
        clean_shell,
        publication,
        suffix="mismatch",
        principal="admin/main",
        text="module list",
        command_id="core.cancel",
    )
    assert rejected["state"] == "REJECTED"
    assert rejected["rejection_code"] == "SHELL_ARGUMENTS_INVALID"
    assert await shell.claim(operation_id="claim/mismatch", worker_id="worker/1") is None
