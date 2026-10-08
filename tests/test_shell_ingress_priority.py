from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.shell_command.python.shell_command import ShellCommand


@pytest.mark.parametrize("trailing", [
    {"type": "mention", "external_user_id": "10000"},
    {"type": "image", "media_ref": "media/1"},
])
async def test_reserved_shell_prefix_with_nontext_tail_is_rejected_by_shell(trailing):
    async def no_directory_lookup():
        pytest.fail("Malformed Shell input must not resolve or execute a command")

    context = SimpleNamespace(
        identity={"generation_id": "shell/generation"},
        shell_commands=SimpleNamespace(list=no_directory_lookup),
    )
    ingress = await ShellCommand().inspect({
        "normalized": {
            "self_id": "10000",
            "segments": [
                {"type": "text", "text": "#forest core status"},
                trailing,
            ],
        },
        "event_ref": "event/1",
    }, context)

    assert ingress["command_id"] == "shell.invalid"
    assert ingress["arguments"] == {"error_code": "SHELL_SYNTAX_INVALID"}
    assert ingress["protocol"] == "forest.shell-ingress/2"


async def test_incomplete_shell_prefix_is_not_claimed():
    context = SimpleNamespace(identity={"generation_id": "shell/generation"})
    ingress = await ShellCommand().inspect({
        "normalized": {
            "self_id": "10000",
            "segments": [
                {"type": "text", "text": "#forestX core status"},
                {"type": "mention", "external_user_id": "10000"},
            ],
        },
        "event_ref": "event/2",
    }, context)
    assert ingress is None


async def test_reserved_shell_prefix_with_line_break_is_rejected_by_shell():
    context = SimpleNamespace(identity={"generation_id": "shell/generation"})
    ingress = await ShellCommand().inspect({
        "normalized": {
            "self_id": "10000",
            "segments": [{"type": "text", "text": "#forest\ncore status"}],
        },
        "event_ref": "event/3",
    }, context)
    assert ingress["command_id"] == "shell.invalid"
    assert ingress["arguments"] == {"error_code": "SHELL_SYNTAX_INVALID"}


@pytest.mark.postgres
async def test_shell_admission_preserves_parser_rejection_after_authorization(clean_authority):
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
                    (Jsonb(context), Jsonb({"protocol": "forest.uow-step/1", "input": ingress})),
                )
            ).fetchone()
            receipt = result["shell_admit_ingress"]
            assert receipt["ok"] is True
            assert receipt["value"]["state"] == "REJECTION_PENDING"
            assert receipt["value"]["rejection_code"] == expected_code
