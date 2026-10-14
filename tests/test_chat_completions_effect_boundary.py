"""The network handoff accepts only the Effect frozen for this model request."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from test_schema_bundles import clean_authority as clean_authority

from forest.chat_completions_effect_driver import (
    ChatCompletionsEffectDriver,
    _canonical,
)
from forest.core_runtime import bootstrap_kernel
from forest.errors import ForestError


@pytest.mark.asyncio
async def test_model_http_handoff_rejects_another_effect_purpose_or_producer():
    payload = {
        "effect_operation_id": "effect/model-1",
        "prepared": {"transport_attempt_id": "attempt/model-1"},
    }
    effect = {
        "state": "DISPATCHING",
        "producer_component_id": "module.agent",
        "effect_kind": "provider.model.dispatch",
        "target_kind": "component.service",
        "target_id": "module.provider/provider.model.dispatch",
    }
    attempt = {
        "state": "DISPATCHING",
        "attempt_id": "attempt/model-1",
        "executor_component_id": "module.provider",
        "executor_generation_id": "generation/provider-1",
        "wire_payload_digest": "sha256:" + hashlib.sha256(_canonical(payload)).hexdigest(),
    }

    class Runtime:
        async def verify_generation_member(self, **identity):
            assert identity == {
                "component_id": "module.provider",
                "generation_id": "generation/provider-1",
            }

    class Effects:
        async def load(self, operation_id):
            assert operation_id == payload["effect_operation_id"]
            return {"effect": effect, "attempt": attempt}

    driver = object.__new__(ChatCompletionsEffectDriver)
    driver.identity = SimpleNamespace(
        component_id="module.provider", generation_id="generation/provider-1"
    )
    driver.runtime = Runtime()
    driver.effects = Effects()
    driver.accepting = True

    await driver._verify_effect(payload, producer_component_id="module.agent")
    for field, forged in (
        ("producer_component_id", "module.other-agent"),
        ("effect_kind", "provider.other-effect"),
        ("target_kind", "component.other"),
        ("target_id", "module.other-provider/provider.model.dispatch"),
    ):
        original = effect[field]
        effect[field] = forged
        with pytest.raises(ForestError) as rejected:
            await driver._verify_effect(payload, producer_component_id="module.agent")
        assert rejected.value.code == "PROVIDER_EFFECT_FENCE_MISMATCH"
        effect[field] = original


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked_id", ["projection/1", "wire/1", "effect-wire/1"])
async def test_model_http_handoff_rechecks_payload_after_preparation(revoked_id):
    """A forget after request preparation must win before HTTP admission."""
    checked = []
    sent = []

    class Connection:
        @asynccontextmanager
        async def transaction(self):
            yield

    class Pool:
        @asynccontextmanager
        async def connection(self):
            yield Connection()

    class Payloads:
        store = SimpleNamespace(pool=Pool())

        async def verify_in(self, _conn, **reference):
            checked.append(reference["payload_id"])
            if reference["payload_id"] == revoked_id:
                raise ForestError("PAYLOAD_REVOKED", "Payload body is unavailable", 410)

    class Client:
        async def dispatch(self, prepared, *, permitcheck):
            await permitcheck(prepared)
            sent.append(prepared)
            raise AssertionError("Revoked input reached HTTP dispatch")

    projection = {
        "payload_id": "projection/1", "digest": "sha256:" + "1" * 64,
        "owner_component_id": "module.agent", "owner_kind": "model.projection",
        "owner_id": "request/1",
    }
    wire = {
        "payload_id": "wire/1", "digest": "sha256:" + "2" * 64,
        "owner_component_id": "module.agent", "owner_kind": "model.wire",
        "owner_id": "attempt/1",
    }
    attempt = {
        "attempt_id": "attempt/1", "wire_payload_id": "effect-wire/1",
        "wire_payload_digest": "sha256:" + "3" * 64,
    }
    prepared = object()
    driver = object.__new__(ChatCompletionsEffectDriver)
    driver.identity = SimpleNamespace(
        component_id="module.provider", generation_id="generation/provider-1",
    )
    driver.profile_ref = "profile/1"
    driver.capability_digest = "sha256:" + "4" * 64
    driver.client = Client()
    driver.payloads = Payloads()

    async def load(_raw):
        return prepared, projection, wire

    async def verify(_payload, *, producer_component_id):
        assert producer_component_id == "module.agent"
        return attempt

    driver._load_prepared = load
    driver._verify_effect = verify
    request = {
        "component_id": "module.provider", "generation_id": "generation/provider-1",
        "profile_ref": "profile/1", "capability_digest": driver.capability_digest,
        "effect_operation_id": "effect/1", "prepared": {},
    }
    with pytest.raises(ForestError) as rejected:
        await driver.invoke(
            request, scope=driver.identity,
            invocation=SimpleNamespace(operation_id="effect/1"),
        )
    assert rejected.value.code == "PAYLOAD_REVOKED"
    assert checked == [
        "projection/1", "wire/1", "effect-wire/1",
    ][:checked.index(revoked_id) + 1]
    assert not sent


@pytest.mark.postgres
async def test_model_final_permit_waits_for_forget_and_rejects_revoked_input(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    epoch = await kernel.payload.epoch()
    async with store.pool.connection() as conn, conn.transaction():
        projection = await kernel.payload.put_json_in(
            conn, value={"projection": 1}, owner_component_id="agent.runtime",
            owner_kind="model.projection", owner_id="request/forget",
            expected_deletion_epoch=epoch,
        )
        wire = await kernel.payload.put_json_in(
            conn, value={"wire": 1}, owner_component_id="agent.runtime",
            owner_kind="model.wire", owner_id="attempt/forget",
            parents=(projection.payload_id,), expected_deletion_epoch=epoch,
        )
        effect_wire = await kernel.payload.put_json_in(
            conn, value={"effect": 1}, owner_component_id="kernel.effect",
            owner_kind="effect.wire", owner_id="attempt/forget",
            parents=(projection.payload_id, wire.payload_id),
            expected_deletion_epoch=epoch,
        )
    driver = object.__new__(ChatCompletionsEffectDriver)
    driver.payloads = kernel.payload

    def ref(saved, component, kind, owner):
        return {
            "payload_id": saved.payload_id, "digest": saved.content_digest,
            "owner_component_id": component, "owner_kind": kind, "owner_id": owner,
        }
    projection_ref = ref(projection, "agent.runtime", "model.projection", "request/forget")
    wire_ref = ref(wire, "agent.runtime", "model.wire", "attempt/forget")
    attempt = {
        "attempt_id": "attempt/forget", "wire_payload_id": effect_wire.payload_id,
        "wire_payload_digest": effect_wire.content_digest,
    }
    forgot = asyncio.Event()
    release = asyncio.Event()
    attempted = asyncio.Event()

    async def forget():
        async with store.pool.connection() as conn, conn.transaction():
            result = await kernel.payload.forget_in(
                conn, operation_id="forget/model-final-permit",
                payload_ids=(projection.payload_id,),
            )
            forgot.set()
            await release.wait()
            return result

    async def permit():
        attempted.set()
        await driver._verify_payload_handoff(projection_ref, wire_ref, attempt)

    forgetting = asyncio.create_task(forget())
    await asyncio.wait_for(forgot.wait(), 5)
    permitting = asyncio.create_task(permit())
    await asyncio.wait_for(attempted.wait(), 5)
    await asyncio.sleep(0.1)
    assert not permitting.done()
    release.set()
    assert (await asyncio.wait_for(forgetting, 5))["selected_count"] == 3
    with pytest.raises(ForestError) as rejected:
        await asyncio.wait_for(permitting, 5)
    assert rejected.value.code == "PAYLOAD_REVOKED"
