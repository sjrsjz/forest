"""Memory values/profile cutover, exact provenance and deletion-time settlement."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_agent_memory import bound, projection
from test_agent_store import start
from test_media import media_server, picture, respond
from test_memory_config import apply, guarded, plan, read
from test_memory_config import environment as environment
from test_memory_index_store import outcome, ready
from test_memory_media import incoming
from test_memory_media import media as media

from forest.errors import ForestError
from forest.memory_runtime import MemoryRuntime
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.agent_memory import AgentMemoryStore
from forest.storage.control import BotControl
from forest.storage.memory_config import NUMERIC_FIELDS, MemoryConfig, _metadata
from forest.storage.memory_index import MemoryIndexStore
from forest.storage.memory_queries import MemoryQueryStore
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


async def rows(env, statement, args=()):
    async with env.store.pool.connection() as conn:
        return await (await conn.execute(statement, args)).fetchall()


async def forget_input(env, operation, *, epoch=0):
    domain = (await rows(env,
        "SELECT d.source_unit_id FROM public.bot_web_operations o "
        "JOIN public.bot_payload_sources s ON s.bot_id=o.bot_id AND s.payload_id=o.input_payload_id "
        "JOIN public.bot_payload_domains d ON d.bot_id=s.bot_id AND d.source_unit_id=s.source_unit_id "
        "WHERE o.bot_id=%s AND o.operation_id=%s AND d.kind='CONTROL_INPUT'",
        (env.store.bot_id, operation),
    ))[0]
    repo = PayloadStore(env.store)
    await repo.forget_sources(env.lease, "erase-" + operation, (SourceRef(domain["source_unit_id"]),),
                              expected_deletion_epoch=epoch)
    await repo.scrub(env.lease, limit=256)


async def counts(env):
    result = {}
    for table in ("bot_payload_headers", "bot_payload_bodies", "bot_payload_domains",
                  "bot_memory_config_plans", "bot_memory_config_revisions", "bot_memory_deployment_bindings",
                  "bot_web_operations", "bot_audit_records", "bot_outbox_events", "bot_control_value_sources"):
        result[table] = (await rows(env, f"SELECT count(*) AS n FROM public.{table}"))[0]["n"]
    return result


async def test_values_are_payload_only_and_credentials_keep_their_separate_lifecycle(environment):
    env = environment
    settings = {**DEFAULT_MEMORY_SETTINGS, "enabled": True, "model_revision": "private-model-canary"}
    preview = await plan(env, values=settings, secret="private-api-key", operation="memory-a")
    applied = await apply(env, preview, "memory-a-apply")
    assert applied["revision"] == applied["secret_revision"] == 1
    for table in ("bot_memory_config_plans", "bot_memory_config_revisions"):
        row = (await rows(env, f"SELECT * FROM public.{table}"))[0]
        assert row["values"] is None
        assert row["values_payload_id"] and row["values_payload_digest"].startswith("sha256:")
        assert row["values_metadata"] == _metadata(settings)
        assert set(row["values_metadata"]) == {
            "enabled",
            *NUMERIC_FIELDS,
            "profile_deployment_digest",
            "profile_model_digest",
            "profile_revision_digest",
        }
        assert "private-model-canary" not in repr(row["values_metadata"])
        assert env.vault.decrypt(row["secret_ciphertext"]) == "private-api-key"
    assert (await read(env))["values"] == settings
    assert (await guarded(env, 1))["values"] == settings
    mapping = (await rows(env, "SELECT * FROM public.bot_memory_deployment_bindings"))[0]
    assert mapping == {
        "bot_id": env.store.bot_id, "first_revision": 1,
        "deployment_digest": digest_json([settings["deployment_id"]]),
        "endpoint_digest": digest_json([settings["endpoint"]]),
    }
    assert not await rows(env, "SELECT 1 FROM pg_indexes WHERE indexname='bot_memory_config_deployment'")
    body = await rows(env, "SELECT content FROM public.bot_payload_bodies")
    assert all(b"private-api-key" not in bytes(row["content"]) for row in body)


async def test_mixed_preview_erasure_does_not_revoke_independent_new_plan_or_revision(environment):
    env = environment
    a = {**DEFAULT_MEMORY_SETTINGS, "enabled": True, "model_revision": "A-private"}
    b = {**a, "endpoint": "https://embedding.example.com/v1/embeddings",
         "daily_request_budget": 201}
    await apply(env, await plan(env, values=a, operation="plan-a"), "apply-a")
    preview = await plan(env, revision=1, values=b, operation="plan-b")
    assert preview["before_values"] == a
    await forget_input(env, "plan-a")
    replay = await plan(env, revision=1, values=b, operation="plan-b")
    assert BotControl.is_unavailable(replay)
    unavailable = await read(env)
    assert unavailable["revision"] == 1 and unavailable["values"] is None
    assert unavailable["values_payload_state"] == "UNAVAILABLE"
    # apply verifies the true P and original bridge/operation facts; it cannot
    # demand the erased display-only mixture or silently create a new preview.
    assert (await apply(env, preview, "apply-b"))["revision"] == 2
    assert (await guarded(env, 2))["values"] == b
    assert (await read(env))["values"] == b
    assert await apply(env, preview, "apply-b") == {"revision": 2, "status": "APPLIED", "secret_revision": 1}


async def test_unavailable_current_config_keeps_cas_and_accepts_explicit_independent_repair(environment):
    env = environment
    await apply(env, await plan(env, secret="retained-key", operation="broken"))
    await forget_input(env, "broken")
    value = await read(env)
    assert value == {"revision": 1, "values": None, "values_payload_state": "UNAVAILABLE",
                     "has_api_key": True, "secret_revision": 1, "defaults": DEFAULT_MEMORY_SETTINGS}
    with pytest.raises(ForestError) as stopped:
        await MemoryConfig(env.store, env.vault).runtime(env.lease)
    assert stopped.value.code == "CONTROL_CONTENT_REVOKED"
    repaired = {**DEFAULT_MEMORY_SETTINGS, "enabled": True}
    preview = await plan(env, revision=1, values=repaired, operation="repair")
    assert preview["before_values"] is None and preview["before_values_payload_state"] == "UNAVAILABLE"
    await apply(env, preview)
    active = await guarded(env, 2)
    assert active["values"] == repaired and env.vault.decrypt(active["secret_ciphertext"]) == "retained-key"


async def test_endpoint_can_rotate_after_old_payload_erasure_but_profile_cannot(environment):
    env = environment
    await apply(env, await plan(env, operation="endpoint-a"))
    independent = {**DEFAULT_MEMORY_SETTINGS,
                   "endpoint": "http://localhost:9987/v1/embeddings"}
    await apply(env, await plan(env, revision=1, values=independent, operation="endpoint-b"))
    await forget_input(env, "endpoint-a")
    assert (await read(env))["values"] == independent
    changed = {**independent, "endpoint": "http://localhost:9988/v1/embeddings"}
    await apply(env, await plan(env, revision=2, values=changed, operation="endpoint-c"))
    before = await counts(env)
    with pytest.raises(ForestError) as conflict:
        await plan(env, revision=3, values={**changed, "deployment_id": "other"})
    assert conflict.value.code == "MEMORY_PROFILE_LIFECYCLE_CONFLICT"
    assert await counts(env) == before
    binding = await rows(env, "SELECT * FROM public.bot_memory_deployment_bindings")
    assert len(binding) == 1 and "http" not in repr(binding)


async def test_failure_after_record_rolls_back_input_values_receipt_and_plan_bridge(environment, monkeypatch):
    env = environment
    before = await counts(env)
    real = BotControl._record

    async def fail_after_record(self, conn, claims, result, *, parents=()):
        await real(self, conn, claims, result, parents=parents)
        if claims["action"] == "memory.config.plan":
            raise ForestError("TEST_AFTER_RECORD", "Injected final transaction failure", 409)

    monkeypatch.setattr(BotControl, "_record", fail_after_record)
    with pytest.raises(ForestError) as failure:
        await plan(env, operation="rollback-preview")
    assert failure.value.code == "TEST_AFTER_RECORD"
    assert await counts(env) == before


async def test_final_authority_failure_rolls_back_revision_deployment_and_head(environment, monkeypatch):
    env = environment
    proposed = await plan(env)
    before = await counts(env)
    real = BotControl._time

    async def fail_after_head(self, conn, claims):
        await real(self, conn, claims)
        if claims["action"] == "memory.config.apply":
            head = await (await conn.execute("SELECT revision FROM public.bot_memory_config_heads")).fetchone()
            if head["revision"] == 1:
                raise ForestError("WEB_AUTHORITY_EXPIRED", "Expired before publication", 403)

    monkeypatch.setattr(BotControl, "_time", fail_after_head)
    with pytest.raises(ForestError) as expired:
        await apply(env, proposed, "expired-final")
    assert expired.value.code == "WEB_AUTHORITY_EXPIRED"
    assert await counts(env) == before
    assert (await rows(env, "SELECT revision FROM public.bot_memory_config_heads"))[0]["revision"] == 0


async def test_sql_rejects_missing_receipt_wrong_metadata_and_mutable_references(environment):
    env = environment
    preview = await plan(env)
    row = (await rows(env, "SELECT * FROM public.bot_memory_config_plans"))[0]
    with pytest.raises(psycopg.Error):
        async with env.store.pool.connection() as conn:
            await conn.execute("UPDATE public.bot_memory_config_plans SET values=%s", (Jsonb(DEFAULT_MEMORY_SETTINGS),))
    content = MemoryConfig(env.store, env.vault)
    for bad_metadata in (False, True):
        identifier = "standalone-" + str(bad_metadata)
        with pytest.raises(psycopg.errors.CheckViolation) as rejected:
            async with env.store.pool.connection() as conn, conn.transaction():
                await env.store._host(conn, env.lease)
                await content._head(conn)
                original = await content.control.content.put_input_in(
                    conn, identifier, {"values": DEFAULT_MEMORY_SETTINGS}, action="memory.config.plan", epoch=0,
                )
                ref = await content.content.put_values_in(
                    conn, family="memory.config", kind="PLAN", resource="", identity=identifier,
                    values=DEFAULT_MEMORY_SETTINGS, parents=(original.payload_id,), epoch=0,
                )
                metadata = _metadata(DEFAULT_MEMORY_SETTINGS)
                if bad_metadata:
                    metadata["endpoint"] = {"private": "must-not-be-metadata"}
                await conn.execute(
                    "INSERT INTO public.bot_memory_config_plans(bot_id,plan_id,subject_id,issuer_id,expected_revision,"
                    "values_payload_id,values_payload_digest,values_metadata,secret_ciphertext,credentials_changed,plan_digest,expires_at) "
                    "VALUES (%s,%s,%s,%s,0,%s,%s,%s,%s,false,%s,clock_timestamp()+interval '1 minute')",
                    (env.store.bot_id, identifier, row["subject_id"], row["issuer_id"], ref.payload_id, ref.digest,
                     Jsonb(metadata), row["secret_ciphertext"], preview["plan_digest"]),
                )
        assert ("MEMORY_CONFIG_METADATA_CONFLICT" if bad_metadata else "MEMORY_PLAN_RECEIPT_REQUIRED") in str(rejected.value)


@pytest.mark.parametrize("outcome_status", ["RESPONDED", "UNKNOWN", "NOT_SENT"])
async def test_profile_erasure_stops_new_permits_but_settles_original_attempt_once(media, outcome_status):
    env, _, _ = media
    store, ids = await ready(media)
    prepared = (await store.prepare(env.lease, 1, ids, request_id="profile-request"))["prepared"]
    request = (await rows(env, "SELECT * FROM public.bot_memory_embedding_requests"))[0]
    assert request["profile"] is None
    assert request["profile_payload_digest"] == request["space_id"]
    assert set(request["profile_metadata"]) == {"dimensions", "protocol_revision"}
    config = (await rows(env, "SELECT * FROM public.bot_memory_config_revisions"))[0]
    for name in ("profile_payload_id", "body_payload_id", "manifest_payload_id"):
        assert await rows(env, "SELECT 1 FROM public.bot_payload_derivations WHERE payload_id=%s AND parent_payload_id=%s",
                          (request[name], config["values_payload_id"]))
    if outcome_status != "NOT_SENT":
        await store.check_permit(env.lease, prepared, dispatch_token="transport")
    operation = (await rows(env, "SELECT operation_id FROM public.bot_control_value_sources "
                           "WHERE family='memory.config' AND source_kind='PLAN'"))[0]["operation_id"]
    await forget_input(env, operation)
    assert not await rows(env, "SELECT * FROM public.bot_payload_bodies WHERE payload_id=ANY(%s)",
                          ([request[key] for key in ("profile_payload_id", "body_payload_id", "manifest_payload_id")],))
    with pytest.raises(ForestError) as revoked:
        await store.check_permit(env.lease, prepared, dispatch_token="transport")
    assert revoked.value.code == "MEMORY_CONFIGURATION_REVOKED"
    result = outcome(prepared, outcome_status, tokens=13)
    response = await store.complete(env.lease, prepared, result,
                                    dispatch_token="transport" if outcome_status != "NOT_SENT" else None)
    assert response["status"] == outcome_status and not response["published"]
    budget = deepcopy((await rows(env, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0])
    assert budget["charged_tokens"] == (13 if outcome_status == "RESPONDED" else 0)
    assert budget["reserved_tokens"] == (DEFAULT_MEMORY_SETTINGS["request_token_reservation"] if outcome_status == "UNKNOWN" else 0)
    await store.complete(env.lease, prepared, result,
                         dispatch_token="transport" if outcome_status != "NOT_SENT" else None)
    assert (await rows(env, "SELECT * FROM public.bot_memory_embedding_budget_days"))[0] == budget
    assert not await rows(env, "SELECT * FROM public.bot_memory_vectors")


async def erase_config(env):
    operation = (await rows(env, "SELECT operation_id FROM public.bot_control_value_sources "
                           "WHERE family='memory.config' AND source_kind='PLAN'"))[0]["operation_id"]
    await forget_input(env, operation)


def runtime_for(media):
    env, images, _ = media
    return MemoryRuntime(SimpleNamespace(
        bot=SimpleNamespace(store=env.store), messages=images.messages, vault=env.vault,
        kernel=SimpleNamespace(_authority=lambda: env.lease),
    ), messages=images.messages)


@pytest.mark.parametrize("pending", [False, True], ids=["before-decision", "pending-decision"])
async def test_erased_configuration_bypasses_unbound_agent_once_without_new_work(media, pending):
    env, images, _ = media
    await apply(env, await plan(env, values={**DEFAULT_MEMORY_SETTINGS, "enabled": True}))
    state, actor, head = await start((images.messages, env.lease, None))
    snapshot = await state.snapshot(actor, head["run_id"])
    memory = AgentMemoryStore(state, MemoryQueryStore(MemoryIndexStore(images.messages, images)))
    if pending:
        assert (await memory.begin(actor, snapshot))["status"] == "PENDING"
    await erase_config(env)
    runtime = runtime_for(media)
    try:
        assert await runtime.retrieve_for_step(actor, snapshot, state) == (None, {})
        assert await runtime.retrieve_for_step(actor, snapshot, state) == (None, {})
        assert await rows(env, "SELECT status,error_code FROM public.bot_agent_memory_bindings") == [
            {"status": "BYPASSED", "error_code": "MEMORY_CONFIGURATION_REVOKED"}]
        for table in ("bot_memory_queries", "bot_memory_embedding_attempts", "bot_model_transport_attempts"):
            assert not await rows(env, f"SELECT * FROM public.{table}")
        await state.prepare_model(actor, snapshot["run_id"], "without-revoked-memory", projection(snapshot))
    finally:
        await runtime.close()
    assert not runtime.clients and not runtime.executions and not runtime.workers


async def test_erased_configuration_cannot_replace_bound_evidence_with_a_bypass(media):
    env = media[0]
    state, actor, snapshot, _, _ = await bound(media)
    before = await rows(env, "SELECT * FROM public.bot_agent_memory_bindings")
    await erase_config(env)
    runtime = runtime_for(media)
    try:
        with pytest.raises(ForestError) as revoked:
            await runtime.retrieve_for_step(actor, snapshot, state)
        assert revoked.value.code == "AGENT_MEMORY_REVOKED"
        assert await rows(env, "SELECT * FROM public.bot_agent_memory_bindings") == before
        assert (await state.fail(actor, snapshot["run_id"], revoked.value.code))["phase"] == "FAILED"
        assert not await rows(env, "SELECT * FROM public.bot_conversation_sessions WHERE active_run_id IS NOT NULL")
    finally:
        await runtime.close()


async def test_erased_configuration_settles_late_image_and_keeps_independent_stored_pixels(media):
    env, images, _ = media
    pixels, entered, release = picture(), asyncio.Event(), asyncio.Event()

    async def image_response(writer, header):
        if b"/late " in header:
            entered.set()
            await release.wait()
        await respond(writer, pixels)

    async with media_server(image_response) as (origin, requests):
        await apply(env, await plan(env, values={**DEFAULT_MEMORY_SETTINGS, "enabled": True,
                                               "media_allowed_origins": [origin], "media_max_bytes": 512}))
        await incoming(images, env.lease, origin + "/stored")
        stored = (await images.scan(env.lease))[0]
        assert (await images.capture_once(env.lease, stored["capture_id"]))["status"] == "CAPTURED"
        stored_ref = (await rows(env, "SELECT payload_id FROM public.bot_memory_media_heads WHERE capture_id=%s",
                                (stored["capture_id"],)))[0]["payload_id"]
        await incoming(images, env.lease, origin + "/late", 2)
        late = (await images.scan(env.lease))[0]
        task = asyncio.create_task(images.capture_once(env.lease, late["capture_id"]))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            await erase_config(env)
            release.set()
            result = await asyncio.wait_for(task, 5)
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert result["status"] == "REJECTED" and result["error_code"] == "MEMORY_CONFIGURATION_REVOKED"
        assert len(requests) == 2
        assert await rows(env, "SELECT retained_bytes,reserved_bytes FROM public.bot_memory_media_quota") == [
            {"retained_bytes": len(pixels), "reserved_bytes": 0}]
        assert await rows(env, "SELECT charged_bytes FROM public.bot_memory_media_daily") == [
            {"charged_bytes": len(pixels) * 2}]
        assert not await rows(env, "SELECT * FROM public.bot_payload_sources s JOIN public.bot_payload_domains d "
                              "USING(bot_id,source_unit_id) WHERE s.payload_id=%s AND d.kind='CONTROL_INPUT'", (stored_ref,))
        body = await rows(env, "SELECT content FROM public.bot_payload_bodies WHERE payload_id=%s", (stored_ref,))
        assert bytes(body[0]["content"]) == pixels
        with pytest.raises(ForestError) as denied:
            await images.scan(env.lease)
        assert denied.value.code == "CONTROL_CONTENT_REVOKED"
        await apply(env, await plan(env, revision=1,
            values={**DEFAULT_MEMORY_SETTINGS, "enabled": True,
                    "media_allowed_origins": [origin], "media_max_bytes": 512}))
        retry = (await images.scan(env.lease))[0]
        assert retry["capture_id"] == late["capture_id"]
        assert (await images.capture_once(env.lease, retry["capture_id"]))["status"] == "CAPTURED"
        assert len(requests) == 3
        assert await rows(env, "SELECT retained_bytes,reserved_bytes FROM public.bot_memory_media_quota") == [
            {"retained_bytes": len(pixels) * 2, "reserved_bytes": 0}]
        assert await rows(env, "SELECT charged_bytes FROM public.bot_memory_media_daily") == [
            {"charged_bytes": len(pixels) * 3}]


@pytest.mark.parametrize("dispatched", [False, True], ids=["prepared", "dispatched"])
async def test_runtime_recovers_original_budget_without_erased_config_on_new_host(media, dispatched):
    env, images, _ = media
    index, ids = await ready(media, config={"media_allowed_origins": ["http://127.0.0.1:9"]})
    request = (await index.prepare(env.lease, 1, ids))["prepared"]
    await incoming(images, env.lease, "http://127.0.0.1:9/unrequested", 2)
    capture = (await images.scan(env.lease))[0]
    claim = await images.claim(env.lease, capture["capture_id"], "old-worker")
    assert claim is not None
    if dispatched:
        await index.check_permit(env.lease, request, dispatch_token="old-transport")
    await erase_config(env)
    await env.store.release_host(env.lease)
    env.lease = await env.store.acquire_host("recovery-host", "recovery-incarnation", ttl_seconds=120)
    runtime = runtime_for(media)
    try:
        await runtime.start()
        async with asyncio.timeout(5):
            while True:
                attempts = await rows(env, "SELECT state FROM public.bot_memory_embedding_attempts")
                captures = await rows(env, "SELECT quota_settled FROM public.bot_memory_media_attempts")
                if attempts == [{"state": "UNKNOWN" if dispatched else "NOT_SENT"}] and captures == [{"quota_settled": True}]:
                    break
                await asyncio.sleep(0.03)
        assert await rows(env, "SELECT reserved_tokens,charged_tokens FROM public.bot_memory_embedding_budget_days") == [
            {"reserved_tokens": DEFAULT_MEMORY_SETTINGS["request_token_reservation"] if dispatched else 0,
             "charged_tokens": 0}]
        assert await rows(env, "SELECT retained_bytes,reserved_bytes FROM public.bot_memory_media_quota") == [
            {"retained_bytes": 0, "reserved_bytes": 0}]
        assert await rows(env, "SELECT charged_bytes FROM public.bot_memory_media_daily") == [{"charged_bytes": 0}]
        assert not runtime.workers and not runtime.clients and not runtime.errors
    finally:
        await runtime.close()
    assert not runtime.executions and not runtime.workers and not runtime.clients
