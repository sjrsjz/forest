import asyncio
import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.errors import CheckViolation
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.shell_command.python.shell_command import ShellCommand
from forest.core_runtime import bootstrap_kernel
from forest.errors import ForestError
from forest.kernel_rescue_executor import KernelCoreRescueExecutor
from forest.kernel_rescue_ingress import KernelCoreRescueAdmission
from forest.kernel_shell_gate import (
    KernelShellGateUnitOfWorkParticipant,
    core_rescue_inspect_service,
    shell_gate_inspect_service,
)


async def _gate(normalized):
    return await shell_gate_inspect_service(None)({
        "protocol": "forest.shell-gate-request/1", "normalized": normalized,
    })


@pytest.mark.parametrize(
    ("text", "claimed", "action", "error_code"),
    [
        ("#forest core status", True, "status", None),
        ("#forest core cancel run:42 --expect-revision 3", True, "cancel", None),
        ("#forest core cancel run:42 --expect-revision 0", True, "cancel", None),
        (
            "#forest core cancel core:" + "a" * 64 + "/reload --expect-revision 0",
            True, "cancel", "CORE_RESCUE_SYNTAX_INVALID",
        ),
        (
            "#forest core cancel core:" + "a" * 64 + "/reload --expect-revision 4",
            True, "cancel", None,
        ),
        ("#forest core disable --expect-maintenance 2", True, "disable", None),
        ("#forest core reload --expect-graph 4", True, "reload", None),
        ("#forest coreX status", False, None, None),
        ("#forest core status extra", True, "status", "CORE_RESCUE_SYNTAX_INVALID"),
        ("#forest core reload --expect-graph 0", True, "reload", "CORE_RESCUE_SYNTAX_INVALID"),
        ("#forest core\nstatus", True, None, "CORE_RESCUE_SYNTAX_INVALID"),
    ],
)
async def test_core_rescue_has_fixed_grammar_without_shell_fallback(
    text, claimed, action, error_code
):
    gate = await _gate({
        "self_id": "10000", "segments": [{"type": "text", "text": text}],
    })
    if not gate["claimed"]:
        assert not claimed
        return
    inspection = await core_rescue_inspect_service(None)({
        "protocol": "forest.core-rescue-inspect-request/1", "gate": gate,
    })
    assert inspection["claimed"] is claimed
    if claimed:
        assert inspection["action"] == action
        assert inspection["error_code"] == error_code
        if text == "#forest core cancel run:42 --expect-revision 0":
            assert inspection["expected_revision"] == 0
        if text.startswith("#forest core cancel core:"):
            assert inspection["target_ref"] == "core:" + "a" * 64 + "/reload"


@pytest.mark.postgres
async def test_core_rescue_admission_serializes_replay_and_queue_capacity(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    policy = kernel.rescue_policy

    async def authorized(_conn, **_values):
        return None

    await policy.put(
        operation_id="test:rescue-grant", expected_revision=1,
        binding_ref="binding/main", group_ref="933179609",
        principal_ref="qq:42", actions=["status"],
        actor_ref="web:test", authorization=None, authorization_guard=authorized,
    )
    admission = KernelCoreRescueAdmission(
        SimpleNamespace(generation_identity=SimpleNamespace(generation_id="kernel/one")),
        policy, kernel.audit,
    )
    received = datetime.now(timezone.utc)
    inspection = {
        "protocol": "forest.core-rescue-inspection/1", "claimed": True,
        "action": "status", "target_ref": None, "expected_revision": None,
        "error_code": None,
    }
    body = {
        "protocol": "forest.core-rescue-admit/1", "binding_ref": "binding/main",
        "binding_epoch": 1, "connection_epoch": 1,
        "client_incarnation": "client/one", "adapter_generation_id": "adapter/one",
        "executor_fence_digest": "sha256:" + "a" * 64,
        "conversation_ref": "onebot:group:933179609",
        "event_ref": "event/0", "principal_ref": "qq:42",
        "group_ref": "933179609", "received_at": received.isoformat(),
        "admission_deadline": (received + timedelta(seconds=30)).isoformat(),
        "inspection": inspection,
    }
    envelope = {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "adapter.onebot", "generation_id": "adapter/one"},
        "input": body,
        "prior": {
            "adapter": {"event_id": body["event_ref"], "binding_ref": "binding/main",
                        "binding_epoch": 1},
            "conversation": {"conversation_id": body["conversation_ref"], "ingress_seq": 1},
            "gate": {
                "protocol": "forest.shell-ingress-proof/1",
                "core_inspection": inspection,
                "conversation_ref": body["conversation_ref"],
                "event_ref": body["event_ref"], "binding_ref": "binding/main",
                "binding_epoch": 1, "adapter_generation_id": "adapter/one",
                "principal_ref": "qq:42", "self_id": "10000",
                "received_at": body["received_at"],
                "admission_deadline": body["admission_deadline"], "ingress_seq": 1,
            },
            "origin": {
                "protocol": "forest.onebot-shell-origin-proof/1",
                "conversation_ref": body["conversation_ref"],
                "event_ref": body["event_ref"], "binding_ref": "binding/main",
                "binding_epoch": 1, "adapter_generation_id": "adapter/one",
                "principal_ref": "qq:42", "self_id": "10000",
            },
        },
    }

    async def admit(index):
        request = deepcopy(envelope)
        event_ref = f"event/{index}"
        request["input"]["event_ref"] = event_ref
        for proof in ("adapter", "gate", "origin"):
            request["prior"][proof]["event_id" if proof == "adapter" else "event_ref"] = event_ref
        request["prior"]["conversation"]["ingress_seq"] = index + 1
        request["prior"]["gate"]["ingress_seq"] = index + 1
        async with store.pool.connection() as conn, conn.transaction():
            return await admission.invoke_in(conn, request)

    results = await asyncio.gather(*(admit(index) for index in range(20)))
    assert sum(item["state"] == "QUEUED" for item in results) == 16
    assert sum(item["rejection_code"] == "CORE_RESCUE_QUEUE_FULL" for item in results) == 4
    assert len({item["request_id"] for item in results}) == 20
    duplicate = await asyncio.gather(admit(0), admit(0))
    assert duplicate == [{**results[0], "duplicate": True}] * 2
    async with store.pool.connection() as conn:
        saved = await (await conn.execute(
            sql.SQL("SELECT self_id FROM {}.rescue_requests WHERE bot_id=%s AND event_ref=%s").format(
                sql.Identifier(policy.namespace)
            ),
            (store.bot_id, "event/0"),
        )).fetchone()
    assert saved["self_id"] == "10000"


@pytest.mark.postgres
async def test_core_rescue_policy_revoke_waits_for_inflight_network_send(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    policy = kernel.rescue_policy

    async def authorized(_conn, **_values):
        return None

    await policy.put(
        operation_id="test:grant-send", expected_revision=1,
        binding_ref="binding/main", group_ref="933179609",
        principal_ref="qq:42", actions=["status"],
        actor_ref="web:test", authorization=None, authorization_guard=authorized,
    )
    request_id = "core:" + "a" * 64
    intent_id = "core-intent:" + "a" * 64
    row = {
        "request_id": request_id,
        "delivery_state": "QUEUED",
        "delivery_intent_id": intent_id,
        "binding_ref": "binding/main",
        "group_ref": "933179609",
        "principal_ref": "qq:42",
        "policy_revision": 2,
        "action": "status",
    }

    class Requests:
        async def delivery_in(self, _conn, _lease, selected_id):
            assert selected_id == request_id
            return row

    async def verify_active_generation_in(_conn, **_values):
        return None

    host = SimpleNamespace(
        registry=SimpleNamespace(instances={
            "kernel.runtime": SimpleNamespace(
                generation_identity=SimpleNamespace(generation_id="kernel/test")
            )
        }),
        authority_lease=lambda: object(),
    )
    runtime = SimpleNamespace(
        store=store, verify_active_generation_in=verify_active_generation_in,
    )
    executor = KernelCoreRescueExecutor(host, runtime, policy, Requests(), None, None)

    async def prove_binding(_conn, _row, _generation_id):
        return None

    executor._prove_binding_in = prove_binding
    entered_send = asyncio.Event()
    release_send = asyncio.Event()
    entered_revoke = asyncio.Event()

    async def send():
        entered_send.set()
        await release_send.wait()
        return "SENT"

    kwargs = {
        "effect_operation_id": intent_id + "/effect",
        "binding_ref": "binding/main",
        "channel_kind": "group",
        "channel_ref": "933179609",
    }
    inflight = asyncio.create_task(executor.dispatch_with_policy_guard(**kwargs, send=send))
    await asyncio.wait_for(entered_send.wait(), 5)

    async def revoke_guard(_conn, **_values):
        entered_revoke.set()

    revoked = asyncio.create_task(policy.put(
        operation_id="test:revoke-send", expected_revision=2,
        binding_ref="binding/main", group_ref="933179609",
        principal_ref="qq:42", actions=[], actor_ref="web:test",
        authorization=None, authorization_guard=revoke_guard,
    ))
    try:
        await asyncio.wait_for(entered_revoke.wait(), 5)
        await asyncio.sleep(0.1)
        assert not revoked.done()
    finally:
        release_send.set()
    assert await inflight == "SENT"
    assert (await revoked)["revision"] == 3

    async def unexpected_send():
        pytest.fail("A revoked Core result must never reach OneBot")

    with pytest.raises(ForestError) as denied:
        await executor.dispatch_with_policy_guard(**kwargs, send=unexpected_send)
    assert denied.value.code == "CORE_RESCUE_DELIVERY_REVOKED"


@pytest.mark.parametrize("trailing", [
    {"type": "mention", "external_user_id": "10000"},
    {"type": "image", "media_ref": "media/1"},
])
async def test_reserved_shell_prefix_with_nontext_tail_is_rejected_by_shell(trailing):
    async def no_directory_lookup():
        pytest.fail("Malformed Shell input must not resolve or execute a command")

    gate = await _gate({
        "self_id": "10000",
        "segments": [{"type": "text", "text": "#forest core status"}, trailing],
    })
    assert gate["claimed"] is True
    context = SimpleNamespace(
        identity={"generation_id": "shell/generation"},
        shell_commands=SimpleNamespace(list=no_directory_lookup),
    )
    ingress = await ShellCommand().inspect(
        {"gate": gate, "event_ref": "event/1"}, context,
    )
    assert ingress["command_id"] == "shell.invalid"
    assert ingress["arguments"] == {"error_code": "SHELL_SYNTAX_INVALID"}


async def test_incomplete_shell_prefix_is_not_claimed():
    gate = await _gate({
        "self_id": "10000",
        "segments": [
            {"type": "text", "text": "#forestX core status"},
            {"type": "mention", "external_user_id": "10000"},
        ],
    })
    assert gate == {"protocol": "forest.shell-gate/1", "claimed": False}


async def test_reserved_shell_prefix_with_line_break_is_rejected_by_shell():
    gate = await _gate({
        "self_id": "10000",
        "segments": [{"type": "text", "text": "#forest\ncore status"}],
    })
    assert gate["claimed"] is True
    context = SimpleNamespace(identity={"generation_id": "shell/generation"})
    ingress = await ShellCommand().inspect(
        {"gate": gate, "event_ref": "event/3"}, context,
    )
    assert ingress["command_id"] == "shell.invalid"
    assert ingress["arguments"] == {"error_code": "SHELL_SYNTAX_INVALID"}


def _admission_envelope(ingress):
    return {
        "protocol": "forest.uow-step/1",
        "caller": {
            "component_id": "adapter.onebot",
            "generation_id": ingress["adapter_generation_id"],
        },
        "input": ingress,
        "prior": {
            "conversation": {"ingress_seq": 1},
            "gate": {
                "protocol": "forest.shell-ingress-proof/1",
                "event_ref": ingress["event_ref"],
                "binding_ref": ingress["binding_ref"],
                "binding_epoch": ingress["binding_epoch"],
                "conversation_ref": ingress["conversation_ref"],
                "principal_ref": ingress["principal_ref"],
                "ingress_seq": 1,
                "adapter_generation_id": ingress["adapter_generation_id"],
                "received_at": ingress["received_at"],
                "admission_deadline": ingress["admission_deadline"],
                "normalized_payload_digest": "sha256:" + "d" * 64,
                "self_id": "10000",
            },
            "origin": {
                "protocol": "forest.onebot-shell-origin-proof/1",
                "event_ref": ingress["event_ref"],
                "binding_ref": ingress["binding_ref"],
                "binding_epoch": ingress["binding_epoch"],
                "conversation_ref": ingress["conversation_ref"],
                "principal_ref": ingress["principal_ref"],
                "self_id": "10000",
                "adapter_generation_id": ingress["adapter_generation_id"],
                "platform_message_ref": "message/1",
            },
        },
    }


@pytest.mark.postgres
async def test_shell_admission_preserves_rejections_and_priority_capacity(clean_authority):
    store, _installer = clean_authority
    schema_name = "shell_priority_test"
    source = Path(__file__).resolve().parents[1] / (
        "src/forest/builtin/shell_command/schema/shell_command.sql"
    )
    baseline = source.read_text(encoding="utf-8").replace(
        "__forest_module_schema__", '"' + schema_name + '"'
    )
    now = datetime.now(timezone.utc)
    async with store.pool.connection() as conn:
        await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        await conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))
        await conn.execute(baseline)
        await conn.execute(
            sql.SQL("INSERT INTO {}.shell_generation_snapshots "
                    "(shell_generation_id,graph_id,graph_revision,binding_ref,policy_revision) "
                    "VALUES ('generation/shell','graph/1',1,'binding/1',1)").format(
                sql.Identifier(schema_name)
            )
        )
        await conn.execute(
            sql.SQL("INSERT INTO {}.shell_administrators "
                    "(shell_generation_id,principal_ref) "
                    "VALUES ('generation/shell','qq:42')").format(sql.Identifier(schema_name))
        )
        cases = [
            ("qq:42", "SHELL_SYNTAX_INVALID", "SHELL_SYNTAX_INVALID"),
            ("qq:42", "SHELL_ARGUMENTS_INVALID", "SHELL_ARGUMENTS_INVALID"),
            ("qq:42", "SHELL_COMMAND_NOT_FOUND", "SHELL_COMMAND_NOT_FOUND"),
            ("qq:43", "SHELL_SYNTAX_INVALID", "SHELL_PRINCIPAL_DENIED"),
            ("qq:42", "NOT_A_SHELL_ERROR", "SHELL_COMMAND_NOT_FOUND"),
        ]
        for index, (principal, parsed_code, expected_code) in enumerate(cases):
            context = {
                "bot_id": "bot-bundle", "generation_id": "generation/shell",
                "input_digest": "sha256:" + format(index + 1, "064x"),
                "operation_id": f"operation/{index}",
            }
            ingress = {
                "protocol": "forest.shell-admission/2",
                "request_id": f"request/{index}",
                "shell_generation_id": "generation/shell",
                "binding_ref": "binding/1", "binding_epoch": 1,
                "connection_epoch": 1, "client_incarnation": "client/1",
                "adapter_generation_id": "generation/adapter",
                "executor_fence_digest": "sha256:" + "a" * 64,
                "conversation_ref": "conversation/1",
                "event_ref": f"event/{index}",
                "principal_ref": principal, "command_id": "shell.invalid",
                "command_version": 1, "arguments": {"error_code": parsed_code},
                "rejection": {"owner_id": f"rejection/{index}", "value": {"text": "rejected"}},
                "received_at": (now - timedelta(seconds=1)).isoformat(),
                "admission_deadline": (now + timedelta(minutes=1)).isoformat(),
            }
            result = await (
                await conn.execute(
                    sql.SQL("SELECT {}.shell_admit_ingress(%s,%s)").format(
                        sql.Identifier(schema_name)
                    ),
                    (Jsonb(context), Jsonb(_admission_envelope(ingress))),
                )
            ).fetchone()
            receipt = result["shell_admit_ingress"]
            assert receipt["ok"] is True
            assert receipt["value"]["state"] == "REJECTION_PENDING"
            assert receipt["value"]["rejection_code"] == expected_code

        forged = deepcopy(ingress)
        forged["request_id"] = "request/forged-proof"
        forged_envelope = _admission_envelope(forged)
        forged_envelope["prior"]["gate"]["principal_ref"] = "qq:forged"
        forged_result = await (
            await conn.execute(
                sql.SQL("SELECT {}.shell_admit_ingress(%s,%s)").format(
                    sql.Identifier(schema_name)
                ),
                (
                    Jsonb({
                        "bot_id": "bot-bundle", "generation_id": "generation/shell",
                        "input_digest": "sha256:" + "f" * 64,
                        "operation_id": "operation/forged-proof",
                    }),
                    Jsonb(forged_envelope),
                ),
            )
        ).fetchone()
        assert forged_result["shell_admit_ingress"]["ok"] is False
        assert forged_result["shell_admit_ingress"]["error"]["code"] == (
            "SHELL_ADMISSION_INVALID"
        )

        for offset, (command_id, service_class) in enumerate((
            ("core.status", "P0"), ("module.reload", "P1"),
        ), start=5):
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.shell_command_catalog "
                    "(shell_generation_id,command_id,command_version,graph_id,graph_revision,"
                    "argument_schema,argument_schema_digest,management_operation_id,"
                    "target_scope,service_class,required_capabilities,"
                    "required_capabilities_digest,output_policy,timeout_seconds,"
                    "owner_component_id,target_generation_id,target_service_id,"
                    "target_service_version) VALUES "
                    "('generation/shell',%s,1,'graph/1',1,%s,%s,'runtime.status',"
                    "'BOT',%s,%s,%s,'SHORT',10,'component.worker-host',"
                    "'generation/target','shell.target',1)"
                ).format(sql.Identifier(schema_name)),
                (
                    command_id, Jsonb({"type": "object"}), "sha256:" + "b" * 64,
                    service_class, Jsonb([]), "sha256:" + "c" * 64,
                ),
            )
            queued = deepcopy(ingress)
            queued.update({
                "request_id": f"request/{service_class}",
                "event_ref": f"event/{service_class}",
                "principal_ref": "qq:42", "command_id": command_id,
                "arguments": {}, "rejection": {
                    "owner_id": f"rejection/{service_class}",
                    "value": {"text": "rejected"},
                },
            })
            admitted = await (
                await conn.execute(
                    sql.SQL("SELECT {}.shell_admit_ingress(%s,%s)").format(
                        sql.Identifier(schema_name)
                    ),
                    (Jsonb({
                        "bot_id": "bot-bundle", "generation_id": "generation/shell",
                        "input_digest": "sha256:" + format(offset + 1, "064x"),
                        "operation_id": f"operation/{offset}",
                    }), Jsonb(_admission_envelope(queued))),
                )
            ).fetchone()
            assert admitted["shell_admit_ingress"]["value"]["state"] == "QUEUED"

        with pytest.raises(CheckViolation):
            async with conn.transaction():
                await conn.execute(
                    sql.SQL(
                        "UPDATE {}.shell_command_catalog SET timeout_seconds=21 "
                        "WHERE command_id='core.status'"
                    ).format(sql.Identifier(schema_name))
                )
        changed = await conn.execute(
            sql.SQL(
                "UPDATE {}.shell_command_catalog SET timeout_seconds=900 "
                "WHERE command_id='module.reload'"
            ).format(sql.Identifier(schema_name))
        )
        assert changed.rowcount == 1

        class_context = Jsonb({"bot_id": "bot-bundle", "generation_id": "generation/shell"})

        async def call(function, payload):
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s,%s)").format(
                        sql.Identifier(schema_name), sql.Identifier(function)
                    ),
                    (class_context, Jsonb(payload)),
                )
            ).fetchone()
            return row[function]

        p1 = await call("shell_claim_work_p1", {
            "claim_token": "claim/p1", "lease_seconds": 120,
        })
        assert p1["value"]["work_id"] == "request/P1"
        p0 = await call("shell_claim_work_p0", {
            "claim_token": "claim/p0", "lease_seconds": 120,
        })
        assert p0["value"]["work_id"] == "request/P0"
        assert (await call("shell_claim_work_p1", {
            "claim_token": "claim/p1-again", "lease_seconds": 120,
        }))["value"] is None
        settlement = {
            "work_id": "request/P0", "claim_token": "claim/p0",
            "outcome": {"error": {"code": "TEST_EXECUTION_FAILED"}},
        }
        wrong_lane = await call("shell_settle_work_p1", settlement)
        assert wrong_lane["ok"] is False
        assert wrong_lane["error"]["code"] == "STALE_SHELL_CLAIM"
        correct_lane = await call("shell_settle_work_p0", settlement)
        assert correct_lane["ok"] is True
        assert correct_lane["value"]["work_id"] == "request/P0"


async def test_shell_proof_requires_the_transports_original_frame_root():
    raw_body = b'{"post_type":"message","self_id":10000}'
    parts = [{"type": "text", "text": "#forest core status"}]
    normalized_body = json.dumps({"parts": parts}).encode()
    event_ref = "onebot-event:sample"
    conversation_ref = "onebot:group:933179609"
    event = {
        "event_id": event_ref,
        "conversation_ref": conversation_ref,
        "author_subject_ref": "qq:42",
        "received_at": "2026-10-16T00:00:00+00:00",
        "classification_deadline": "2026-10-16T00:02:00+00:00",
        "raw": {"payload_id": "raw/1", "digest": "sha256:" + "a" * 64},
        "normalized": {"payload_id": "normalized/1", "digest": "sha256:" + "b" * 64},
    }
    adapter = {
        "binding_ref": "binding/1", "binding_epoch": 1,
        "client_incarnation": "onebot-client:sample", "event": event,
    }
    conversation = {
        "conversation_id": conversation_ref,
        "event_id": "onebot:" + event_ref,
        "source_event_ref": event_ref,
        "binding_ref": "binding/1",
        "author_subject_ref": "qq:42",
        "content": event["normalized"],
        "content_byte_length": len(normalized_body),
    }
    shell = {
        "event_ref": event_ref, "binding_ref": "binding/1", "binding_epoch": 1,
        "client_incarnation": "onebot-client:sample", "connection_epoch": 9,
        "principal_ref": "qq:42", "conversation_ref": conversation_ref,
        "adapter_generation_id": "adapter/generation",
        "received_at": event["received_at"],
        "admission_deadline": event["classification_deadline"],
    }
    gate = {
        "self_id": "10000",
        "inspection": await _gate({"self_id": "10000", "segments": parts}),
    }
    envelope = {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "adapter.onebot", "generation_id": "adapter/generation"},
        "input": {"adapter": adapter, "conversation": conversation, "shell": shell, "gate": gate,
                  "shell_origin": {"raw_frame": json.loads(raw_body)}},
        "prior": {
            "adapter": {"event_id": event_ref, "binding_ref": "binding/1", "binding_epoch": 1},
            "conversation": {
                "conversation_id": conversation_ref,
                "event_id": "onebot:" + event_ref, "ingress_seq": 1,
            },
        },
    }

    class Payloads:
        async def verify_in(self, _conn, **_reference):
            return None

        async def read_bytes_in(self, _conn, payload_id):
            return raw_body if payload_id == "raw/1" else normalized_body

    participant = KernelShellGateUnitOfWorkParticipant(
        SimpleNamespace(
            service=object(),
            generation_identity=SimpleNamespace(generation_id="runtime/generation"),
        ),
        Payloads(),
    )
    frame_ref = "onebot-frame:" + hashlib.sha256(
        b"binding/1\0" + b"9\0" + raw_body
    ).hexdigest()
    proof = await participant.invoke_in(
        None, "shell.gate-prove-ingress", envelope,
        causal_root_kind="adapter.platform-frame", causal_root_id=frame_ref,
    )
    assert proof["protocol"] == "forest.shell-ingress-proof/1"
    assert proof["self_id"] == "10000"
    assert proof["principal_ref"] == "qq:42"
    forged_origin = deepcopy(envelope)
    forged_origin["input"]["shell_origin"]["raw_frame"]["self_id"] = 99999
    with pytest.raises(ForestError) as error:
        await participant.invoke_in(
            None, "shell.gate-prove-ingress", forged_origin,
            causal_root_kind="adapter.platform-frame", causal_root_id=frame_ref,
        )
    assert error.value.code == "SHELL_GATE_PROOF_INVALID"
    with pytest.raises(ForestError) as error:
        await participant.invoke_in(
            None, "shell.gate-prove-ingress", envelope,
            causal_root_kind="adapter.platform-frame",
            causal_root_id="onebot-frame:" + "0" * 64,
        )
    assert error.value.code == "SHELL_GATE_PROOF_INVALID"
