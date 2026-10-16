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
from forest.kernel_shell_gate import shell_gate_inspect_service


async def _gate(normalized):
    return await shell_gate_inspect_service(None)({
        "protocol": "forest.shell-gate-request/1", "normalized": normalized,
    })


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
