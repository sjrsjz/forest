"""Real PostgreSQL CAS, replay and encrypted Agent credential rotation."""

import asyncio
from copy import deepcopy
from uuid import uuid4

import pytest
from test_web_authority import environment as environment

from forest.agent_settings import DEFAULT_AGENT_SETTINGS, AgentSettings
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_config import AgentConfig

pytestmark = pytest.mark.postgres


async def plan(env, *, revision=0, values=None, secret=None, operation=None):
    operation = operation or "agent-plan-" + uuid4().hex
    payload = {"expected_revision": revision, "values": deepcopy(values or DEFAULT_AGENT_SETTINGS),
               "credentials_changed": secret is not None, "secret_digest": digest_json({"api_key": secret})}
    ticket = await env.ticket("agent.config.plan", operation, payload)
    return await AgentConfig(env.store, env.vault).plan(
        ticket, operation, payload,
        secret_ciphertext=env.vault.encrypt(secret) if secret is not None else None,
    )


async def apply(env, proposed, operation=None):
    operation = operation or "agent-apply-" + uuid4().hex
    payload = {key: proposed[key] for key in ("expected_revision", "plan_id", "plan_digest")}
    ticket = await env.ticket("agent.config.apply", operation, payload,
                              plan_digest=payload["plan_digest"])
    return await AgentConfig(env.store, env.vault).apply(ticket, operation, payload)


async def read(env):
    operation = "agent-read-" + uuid4().hex
    return await AgentConfig(env.store, env.vault).read(
        await env.ticket("agent.config.read", operation, {}), operation
    )


async def test_secret_replay_preservation_and_rotation(environment):
    env = environment
    assert (await read(env))["revision"] == 0
    first = await plan(env, secret="private-first-key", operation="first-plan")
    assert await plan(env, secret="private-first-key", operation="first-plan") == first
    saved = await apply(env, first, "first-apply")
    assert await apply(env, first, "first-apply") == saved
    assert saved["revision"] == saved["secret_revision"] == 1
    assert (await read(env))["has_api_key"]
    assert "private-first-key" not in repr(first) + repr(await read(env))
    newer = await plan(env, revision=1)
    assert (await apply(env, newer))["secret_revision"] == 1
    runtime = await AgentConfig(env.store, env.vault).runtime(env.lease)
    assert env.vault.decrypt(runtime["secret_ciphertext"]) == "private-first-key"
    cleared = await plan(env, revision=2, secret="")
    assert (await apply(env, cleared))["secret_revision"] == 2
    assert not (await read(env))["has_api_key"]
    # A historical request replay is its original receipt, not the new head.
    assert await apply(env, first, "first-apply") == saved


async def test_concurrent_config_plans_only_one_head_wins(environment):
    env = environment
    a, b = await asyncio.gather(plan(env), plan(env, secret="replacement"))
    results = await asyncio.gather(apply(env, a), apply(env, b), return_exceptions=True)
    assert sum(isinstance(row, dict) for row in results) == 1
    errors = [row for row in results if isinstance(row, ForestError)]
    assert [error.code for error in errors] == ["REVISION_CONFLICT"]
    assert (await read(env))["revision"] == 1


async def test_key_input_and_plan_owner_are_bound(environment):
    env = environment
    await plan(env, secret="key-a", operation="same-key")
    with pytest.raises(ForestError, match="different"):
        await plan(env, secret="key-b", operation="same-key")
    payload = {"expected_revision": 0, "values": DEFAULT_AGENT_SETTINGS,
               "credentials_changed": True, "secret_digest": digest_json({"api_key": "key-a"})}
    signed = await env.ticket("agent.config.plan", "tamper", payload)
    with pytest.raises(ForestError) as error:
        await AgentConfig(env.store, env.vault).plan(signed, "tamper", payload,
                                                    secret_ciphertext=env.vault.encrypt("key-b"))
    assert error.value.code == "CREDENTIALS_CONFLICT"


@pytest.mark.parametrize("changes", [
    {"enabled": True, "model": ""}, {"endpoint": "https://key:secret@example.com/v1"},
    {"enabled": True, "model": "   "}, {"model": "configured\nmodel"}, {"model": "model\x7f"},
    {"endpoint": "https://@example.com/v1"}, {"endpoint": "https://:password@example.com/v1"},
    {"preset_context": ["中" * 12000]},
    {"allowed_tools": [{"name": f"tool_{index}", "component_id": "service", "service": "call"}
                       for index in range(17)]},
    {"media_allowed_origins": ["https://images.example.com/path"]},
    {"context_tokens": 1024, "max_output_tokens": 2048},
    {"model_timeout": 100, "run_timeout": 90},
    {"children_enabled": False},
    {"system_prompt": "retired compatibility field"},
])
def test_agent_settings_reject_invalid_provider_inputs(changes):
    with pytest.raises(ValueError):
        AgentSettings.model_validate({**DEFAULT_AGENT_SETTINGS, **changes})


def test_agent_settings_allow_exact_preset_and_tool_bounds():
    values = AgentSettings.model_validate({
        **DEFAULT_AGENT_SETTINGS, "enabled": True, "model": "explicit-model", "endpoint": "http://localhost:8080/v1/",
        "preset_context": ["中" * 10922 + "ab"],
        "allowed_tools": [{"name": f"tool_{index}", "component_id": "service", "service": "call"}
                          for index in range(16)],
    })
    assert len(values.preset_context[0].encode()) == 32768
    assert len(values.allowed_tools) == 16 and values.endpoint == "http://localhost:8080/v1"


async def test_agent_configuration_exposes_core_protocol_read_only(environment):
    row = await read(environment)
    assert row["protocol"]["protocol_id"] == "forest-agent-protocol"
    assert row["protocol"]["protocol_version"] == 1
    assert row["protocol"]["core_prompt_digest"].startswith("sha256:")
    assert "valid_agent_protocol_content_block" in row["protocol"]["core_prompt"]
