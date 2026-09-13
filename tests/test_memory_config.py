"""Real PostgreSQL memory configuration CAS, secret rotation and final fences."""

import asyncio
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb
from test_web_authority import environment as environment

import forest
from forest.errors import ForestError
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS, MemorySettings
from forest.packages import digest_json
from forest.storage.memory_config import MemoryConfig

pytestmark = pytest.mark.postgres


async def plan(env, *, revision=0, values=None, secret=None, operation=None):
    operation = operation or "memory-plan-" + uuid4().hex
    payload = {
        "expected_revision": revision,
        "values": deepcopy(values or DEFAULT_MEMORY_SETTINGS),
        "credentials_changed": secret is not None,
        "secret_digest": digest_json({"api_key": secret}),
    }
    return await MemoryConfig(env.store, env.vault).plan(
        await env.ticket("memory.config.plan", operation, payload),
        operation,
        payload,
        secret_ciphertext=env.vault.encrypt(secret) if secret is not None else None,
    )


async def apply(env, proposed, operation=None):
    operation = operation or "memory-apply-" + uuid4().hex
    payload = {key: proposed[key] for key in ("expected_revision", "plan_id", "plan_digest")}
    return await MemoryConfig(env.store, env.vault).apply(
        await env.ticket(
            "memory.config.apply", operation, payload, plan_digest=payload["plan_digest"]
        ),
        operation,
        payload,
    )


async def read(env):
    operation = "memory-read-" + uuid4().hex
    return await MemoryConfig(env.store, env.vault).read(
        await env.ticket("memory.config.read", operation, {}),
        operation,
    )


async def guarded(env, revision):
    async with env.store.pool.connection() as conn, conn.transaction():
        await env.store._host(conn, env.lease)
        result = await MemoryConfig.guard_in(conn, env.lease, revision)
        await env.store._host(conn, env.lease)
        return result


async def test_secret_replay_preservation_clear_and_stale_publication(environment):
    env = environment
    assert await read(env) == {
        "revision": 0,
        "values": DEFAULT_MEMORY_SETTINGS,
        "has_api_key": False,
        "secret_revision": 0,
        "defaults": DEFAULT_MEMORY_SETTINGS,
    }
    enabled = {**DEFAULT_MEMORY_SETTINGS, "enabled": True}
    first = await plan(env, values=enabled, secret="first-private-key", operation="first-plan")
    assert (
        await plan(env, values=enabled, secret="first-private-key", operation="first-plan") == first
    )
    saved = await apply(env, first, "first-apply")
    assert await apply(env, first, "first-apply") == saved
    assert saved["revision"] == saved["secret_revision"] == 1
    assert (await guarded(env, 1))["values"] == enabled
    assert "first-private-key" not in repr(first) + repr(saved) + repr(await read(env))
    await apply(env, await plan(env, revision=1, values={**enabled, "daily_request_budget": 500}))
    current = await MemoryConfig(env.store, env.vault).runtime(env.lease)
    assert (
        current["secret_revision"] == 1
        and env.vault.decrypt(current["secret_ciphertext"]) == "first-private-key"
    )
    with pytest.raises(ForestError) as revoked:
        await guarded(env, 1)
    assert revoked.value.code == "MEMORY_CONFIGURATION_REVOKED"
    assert (await guarded(env, 2))["revision"] == 2
    historical = await MemoryConfig(env.store).runtime(env.lease, revision=1)
    assert historical["values"] == enabled
    await apply(env, await plan(env, revision=2, secret=""))
    assert (await read(env))["secret_revision"] == 2 and not (await read(env))["has_api_key"]
    with pytest.raises(ForestError):
        await guarded(env, 3)
    assert await apply(env, first, "first-apply") == saved


async def test_concurrent_previews_only_one_config_wins(environment):
    env = environment
    a, b = await asyncio.gather(plan(env), plan(env, secret="replacement"))
    results = await asyncio.gather(apply(env, a), apply(env, b), return_exceptions=True)
    assert sum(isinstance(value, dict) for value in results) == 1
    assert [value.code for value in results if isinstance(value, ForestError)] == [
        "REVISION_CONFLICT"
    ]
    assert (await read(env))["revision"] == 1


async def test_endpoint_identity_cannot_be_rebound_even_via_historical_deployment(environment):
    env = environment
    await apply(env, await plan(env))
    different = {**DEFAULT_MEMORY_SETTINGS, "endpoint": "http://localhost:8001/v1/embeddings"}
    with pytest.raises(ForestError) as conflict:
        await plan(env, revision=1, values=different)
    assert conflict.value.code == "MEMORY_DEPLOYMENT_CONFLICT"
    await apply(env, await plan(env, revision=1, values={**different, "deployment_id": "local"}))
    with pytest.raises(ForestError) as historical:
        await plan(env, revision=2, values=different)
    assert historical.value.code == "MEMORY_DEPLOYMENT_CONFLICT"


async def test_policy_changes_revoke_current_memory_permission(environment):
    env = environment
    await apply(env, await plan(env, values={**DEFAULT_MEMORY_SETTINGS, "enabled": True}))
    async with env.store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_runtime_state SET policy_revision=policy_revision+1")
    with pytest.raises(ForestError) as rejected:
        await guarded(env, 1)
    assert rejected.value.code == "MEMORY_CONFIGURATION_REVOKED"


async def test_apply_is_serialized_against_host_publication_guard(environment):
    env = environment
    await apply(env, await plan(env, values={**DEFAULT_MEMORY_SETTINGS, "enabled": True}))
    proposed = await plan(env, revision=1)
    waiting = None
    try:
        async with env.store.pool.connection() as conn, conn.transaction():
            await env.store._host(conn, env.lease)
            assert (await MemoryConfig.guard_in(conn, env.lease, 1))["revision"] == 1
            waiting = asyncio.create_task(apply(env, proposed))
            # Publication holds the shared config head and Host/runtime chain;
            # a configuration change cannot commit in the middle of that tx.
            await asyncio.sleep(0.05)
            assert not waiting.done()
        assert (await asyncio.wait_for(waiting, 5))["revision"] == 2
        with pytest.raises(ForestError):
            await guarded(env, 1)
    finally:
        if waiting and not waiting.done():
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


async def test_plan_subject_issuer_and_secret_input_are_bound(environment):
    env = environment
    proposed = await plan(env, secret="key-a", operation="same-key")
    with pytest.raises(ForestError):
        await plan(env, secret="key-b", operation="same-key")
    payload = {key: proposed[key] for key in ("expected_revision", "plan_id", "plan_digest")}
    ticket = await env.ticket(
        "memory.config.apply", "wrong-owner", payload, plan_digest=payload["plan_digest"]
    )
    for changed in ({"subject_id": "other-admin"}, {"issuer_id": "other-issuer"}):
        if "issuer_id" in changed:
            async with env.store.pool.connection() as conn:
                await conn.execute(
                    "INSERT INTO public.bot_web_issuers(bot_id,issuer_id,key_revision,public_key,actions) "
                    "SELECT bot_id,'other-issuer',key_revision,public_key,actions FROM public.bot_web_issuers WHERE issuer_id=%s",
                    (env.authority.issuer_id,),
                )
        with pytest.raises(ForestError) as rejected:
            await MemoryConfig(env.store, env.vault).apply(
                env.sign({**ticket["claims"], **changed}),
                "wrong-owner",
                payload,
            )
        assert rejected.value.code == "PLAN_NOT_FOUND"
    payload = {
        "expected_revision": 0,
        "values": DEFAULT_MEMORY_SETTINGS,
        "credentials_changed": True,
        "secret_digest": digest_json({"api_key": "key-a"}),
    }
    with pytest.raises(ForestError) as mismatch:
        await MemoryConfig(env.store, env.vault).plan(
            await env.ticket("memory.config.plan", "tamper", payload),
            "tamper",
            payload,
            secret_ciphertext=env.vault.encrypt("key-b"),
        )
    assert mismatch.value.code == "CREDENTIALS_CONFLICT"


async def test_new_memory_grants_map_each_legacy_permission_without_enabling_issuer(environment):
    env = environment
    async with env.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE public.bot_web_issuers SET actions=%s,enabled=false",
            (Jsonb(["config.read", "message.read"]),),
        )
        path = Path(forest.__file__).parent / "storage/bot_migrations/023_memory_config.sql"
        permissions = path.read_text(encoding="utf-8").split("UPDATE public.bot_web_issuers", 1)[1]
        await conn.execute("UPDATE public.bot_web_issuers" + permissions)
        row = await (
            await conn.execute("SELECT actions,enabled FROM public.bot_web_issuers")
        ).fetchone()
    assert set(row["actions"]) == {
        "config.read",
        "message.read",
        "memory.config.read",
        "memory.read",
    }
    assert row["enabled"] is False


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": "true"},
        {"model": "other-model"},
        {"dimensions": 768},
        {"dimensions": True},
        {"endpoint": "https://user:password@example.com/v1/embeddings"},
        {"endpoint": "https://example.com/v1/embeddings?key=secret"},
        {"endpoint": "https://example.com/v1"},
        {"endpoint": "https://example.com:0/embeddings"},
        {"deployment_id": "  "},
        {"model_revision": "a\nb"},
        {"batch_size": 33},
        {"media_max_bytes": 4194305},
        {"capture_daily_bytes": 2**53},
        {"media_allowed_origins": ["https://images.example.com/private"]},
        {"query_limit": 0},
        {"query_max_images": 9},
        {"max_result_bytes": 33554433},
    ],
)
def test_memory_settings_reject_invalid_or_unbounded_input(changes):
    with pytest.raises(ValueError):
        MemorySettings.model_validate({**DEFAULT_MEMORY_SETTINGS, **changes})


def test_memory_settings_normalize_origins_and_keep_independent_vision_capability():
    value = MemorySettings(
        dimensions=1024,
        media_allowed_origins=["https://IMAGES.example.com/", "https://images.example.com"],
    )
    assert value.media_allowed_origins == ["https://images.example.com"]
    assert value.model == "jina-clip-v2" and "vision" not in value.model_dump()
