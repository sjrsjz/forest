"""A durable schedule renews current authority, not its expired creator session."""

from copy import deepcopy
from datetime import datetime

import pytest
from test_web_authority import environment as environment

from forest.errors import ForestError

pytestmark = pytest.mark.postgres


async def issue(env, **updates):
    values = dict(subject_id=env.subject, bot_id=env.store.bot_id,
                  operation_id="job-dispatch:occurrence:1:1", payload={"delegation_id": "delegation:1"},
                  policy_revision=1, delegation_id="delegation:1",
                  expected_account_revision=1, expected_key_revision=1)
    values.update(updates)
    return await env.authority.issue_delegation(**values)


async def test_delegation_survives_logout_but_remains_purpose_bound(environment):
    env = environment
    async with env.deployment.pool.connection() as conn:
        await conn.execute("UPDATE web_sessions SET revoked_at=clock_timestamp()")
    proof = await issue(env)
    claims = proof["claims"]
    assert claims["session_ref"] == "delegation:delegation:1"
    assert claims["action"] == "job.run"
    assert (datetime.fromisoformat(claims["expires_at"]) - datetime.fromisoformat(claims["issued_at"])).total_seconds() == 30
    async with env.store.pool.connection() as conn, conn.transaction():
        accepted = await env.control._guard(conn, proof, "job.run", claims["operation_id"],
                                            {"delegation_id": "delegation:1"})
        assert accepted["subject_id"] == env.subject
    for action, operation, payload in [
        ("job.cancel", claims["operation_id"], {"delegation_id": "delegation:1"}),
        ("job.run", "job-dispatch:another", {"delegation_id": "delegation:1"}),
        ("job.run", claims["operation_id"], {"delegation_id": "delegation:2"}),
    ]:
        async with env.store.pool.connection() as conn, conn.transaction():
            with pytest.raises(ForestError, match="authority"):
                await env.control._guard(conn, proof, action, operation, payload)


@pytest.mark.parametrize("change", ["disabled=true", "role='viewer'", "revision=revision+1"])
async def test_current_account_revocation_stops_new_dispatch_proof(environment, change):
    env = environment
    await issue(env)
    async with env.deployment.pool.connection() as conn:
        # These are fixed test-owned SQL expressions, never administrator input.
        await conn.execute("UPDATE web_accounts SET " + change + " WHERE id=%s", (env.subject,))
    with pytest.raises(ForestError):
        await issue(env)


async def test_stale_cached_signer_and_altered_proof_are_rejected(environment):
    env = environment
    proof = await issue(env)
    altered = deepcopy(proof)
    altered["claims"]["subject_id"] = "another-admin"
    async with env.store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError):
            await env.control._guard(conn, altered, "job.run", proof["claims"]["operation_id"],
                                     {"delegation_id": "delegation:1"})
    env.authority.key_revision += 1
    with pytest.raises(ForestError) as caught:
        await issue(env)
    assert caught.value.code == "JOB_DELEGATION_REVOKED"
