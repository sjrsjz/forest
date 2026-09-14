"""Stable, redacted Bot audit reads through Web authority and group Shell."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb
from test_messages import messages as messages
from test_shell import allow, dispatcher
from test_shell_management import claim_command
from test_web_authority import environment as environment

from forest.errors import ForestError
from forest.storage.bot_audit import BotAuditStore

pytestmark = pytest.mark.postgres


async def _insert(store, rows, created_at):
    async with store.pool.connection() as conn, conn.transaction():
        for audit_id, action, secret in rows:
            await conn.execute(
                "INSERT INTO public.bot_audit_records("
                "bot_id,audit_id,action,operation_id,details,created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    store.bot_id,
                    audit_id,
                    action,
                    "runtime:" + audit_id,
                    Jsonb({"secret": secret, "body": "must-not-leak"}),
                    created_at,
                ),
            )


async def _read(env, payload):
    operation = "audit-query:" + uuid4().hex
    ticket = await env.ticket("audit.read", operation, payload)
    return await BotAuditStore(env.store, control=env.control).read(
        ticket, operation, payload
    )


async def test_web_authority_audit_cursor_is_stable_and_never_hydrates_details(environment):
    env = environment
    stamp = datetime.now(UTC) - timedelta(minutes=1)
    await _insert(
        env.store,
        [
            ("audit:a", "fixture.audit", "alpha"),
            ("audit:b", "fixture.audit", "bravo"),
            ("audit:c", "fixture.audit", "charlie"),
        ],
        stamp,
    )
    first = await _read(env, {"action": "fixture.audit", "limit": 2})
    assert [row["audit_id"] for row in first["records"]] == ["audit:c", "audit:b"]
    assert first["next_before"] == "audit:b"
    assert all(set(row) == {
        "audit_id", "action", "operation_id", "origin_kind", "has_receipt", "created_at"
    } for row in first["records"])
    assert all(row["origin_kind"] == "runtime" for row in first["records"])
    assert "secret" not in str(first) and "must-not-leak" not in str(first)

    await env.plan()
    controlled = await _read(env, {"action": "config.plan", "limit": 2})
    assert controlled["records"][0]["origin_kind"] == "web"
    assert controlled["records"][0]["has_receipt"] is True

    async with env.deployment.pool.connection() as conn:
        await conn.execute(
            "UPDATE web_accounts SET role='viewer',revision=revision+1 WHERE id=%s",
            (env.subject,),
        )
    assert (await _read(env, {"action": "fixture.audit", "limit": 1}))["records"]

    await _insert(
        env.store,
        [("audit:new", "fixture.audit", "newer")],
        datetime.now(UTC),
    )
    second = await _read(
        env, {"action": "fixture.audit", "before": first["next_before"], "limit": 2}
    )
    assert [row["audit_id"] for row in second["records"]] == ["audit:a"]
    assert second["next_before"] is None

    with pytest.raises(ForestError) as missing:
        await _read(env, {"before": "audit:missing", "limit": 2})
    assert getattr(missing.value, "code", None) == "AUDIT_CURSOR_NOT_FOUND"


async def test_group_shell_audit_is_bot_scoped_control_output_without_body(messages):
    repository, lease, _ = messages
    await allow(repository, "user-1")
    await _insert(
        repository.store,
        [
            ("audit:shell-a", "fixture.audit", "shell-alpha"),
            ("audit:shell-b", "fixture.audit", "shell-bravo"),
            ("audit:shell-c", "fixture.audit", "shell-charlie"),
        ],
        datetime.now(UTC),
    )
    controls = dispatcher(repository, lease)
    first = await claim_command(
        repository,
        lease,
        "#forest audit list --action fixture.audit --limit 2",
        number="audit-first",
        group="group-1",
    )
    assert first["gate_version"] == "shell-gate-v8"
    text, outcome = await controls._typed(lease, first)
    assert outcome == {
        "code": "AUDIT_STATUS", "count": 2, "next_before": "audit:shell-b"
    }
    assert "audit:shell-c" in text and "audit:shell-b" in text
    assert "audit:shell-a" not in text
    assert "shell-alpha" not in text and "must-not-leak" not in text

    second = await claim_command(
        repository,
        lease,
        "#forest audit list --action fixture.audit --limit 2 --before audit:shell-b",
        number="audit-second",
        group="group-2",
    )
    text, outcome = await controls._typed(lease, second)
    assert outcome == {"code": "AUDIT_STATUS", "count": 1, "next_before": None}
    assert "audit:shell-a" in text and "audit:shell-c" not in text
