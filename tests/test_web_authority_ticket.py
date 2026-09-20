from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.packages import digest_json
from forest.web_authority_ticket import WebAuthorityTicketAuthority


@pytest.mark.asyncio
async def test_web_authority_ticket_binds_session_target_input_and_expiry():
    key = bytes(range(32))
    authority = WebAuthorityTicketAuthority(
        issuer_id="deployment/test",
        active_key_id="master/test/1",
        key_resolver={"master/test/1": key}.get,
    )
    session = SimpleNamespace(
        session_ref="web-session/test",
        session_revision=3,
        subject=SimpleNamespace(subject_id="web-subject/admin", role="ADMIN"),
    )
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    input_digest = digest_json({"enabled": True})
    ticket = await authority.issue(
        session=session,
        target_scope_kind="bot",
        target_scope_id="bot-test",
        target_component_id="agent.runtime",
        target_generation_id="generation/agent/7",
        target_graph_revision=11,
        target_operation_ref="operation/test",
        action_key="agent.run.cancel",
        input_digest=input_digest,
        ttl_seconds=30,
        now=now,
    )
    await authority.verify(
        ticket,
        target_scope_kind="bot",
        target_scope_id="bot-test",
        target_component_id="agent.runtime",
        target_generation_id="generation/agent/7",
        target_graph_revision=11,
        target_operation_ref="operation/test",
        action_key="agent.run.cancel",
        input_digest=input_digest,
        required_role="ADMIN",
        now=now + timedelta(seconds=29),
    )

    with pytest.raises(ForestError) as mismatch:
        await authority.verify(
            ticket,
            target_scope_kind="bot",
            target_scope_id="bot-test",
            target_component_id="agent.runtime",
            target_generation_id="generation/agent/8",
            target_graph_revision=11,
            target_operation_ref="operation/test",
            action_key="agent.run.cancel",
            input_digest=input_digest,
            required_role="ADMIN",
            now=now,
        )
    assert mismatch.value.code == "WEB_AUTHORITY_TICKET_TARGET_MISMATCH"

    with pytest.raises(ForestError) as tampered:
        await authority.verify(
            replace(ticket, web_subject_ref="web-subject/attacker"),
            target_scope_kind="bot",
            target_scope_id="bot-test",
            target_component_id="agent.runtime",
            target_generation_id="generation/agent/7",
            target_graph_revision=11,
            target_operation_ref="operation/test",
            action_key="agent.run.cancel",
            input_digest=input_digest,
            required_role="ADMIN",
            now=now,
        )
    assert tampered.value.code == "WEB_AUTHORITY_TICKET_SIGNATURE_INVALID"

    with pytest.raises(ForestError) as expired:
        await authority.verify(
            ticket,
            target_scope_kind="bot",
            target_scope_id="bot-test",
            target_component_id="agent.runtime",
            target_generation_id="generation/agent/7",
            target_graph_revision=11,
            target_operation_ref="operation/test",
            action_key="agent.run.cancel",
            input_digest=input_digest,
            required_role="ADMIN",
            now=now + timedelta(seconds=30),
        )
    assert expired.value.code == "WEB_AUTHORITY_TICKET_EXPIRED"


@pytest.mark.asyncio
async def test_auditor_ticket_cannot_authorize_management_write():
    authority = WebAuthorityTicketAuthority(
        issuer_id="deployment/test",
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: b"z" * 32,
    )
    session = SimpleNamespace(
        session_ref="web-session/test",
        session_revision=1,
        subject=SimpleNamespace(subject_id="web-subject/auditor", role="AUDITOR"),
    )
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ticket = await authority.issue(
        session=session,
        target_scope_kind="deployment",
        target_scope_id="deployment",
        target_component_id="deployment.catalog",
        target_generation_id="deployment/control-plane/1",
        target_graph_revision=1,
        target_operation_ref="operation/test",
        action_key="bot.intent.set",
        input_digest=digest_json({}),
        now=now,
    )
    with pytest.raises(ForestError) as denied:
        await authority.verify(
            ticket,
            target_scope_kind="deployment",
            target_scope_id="deployment",
            target_component_id="deployment.catalog",
            target_generation_id="deployment/control-plane/1",
            target_graph_revision=1,
            target_operation_ref="operation/test",
            action_key="bot.intent.set",
            input_digest=digest_json({}),
            required_role="ADMIN",
            now=now,
        )
    assert denied.value.code == "MANAGEMENT_AUTHORIZATION_DENIED"
