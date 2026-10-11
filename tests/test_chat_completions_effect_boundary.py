"""The network handoff accepts only the Effect frozen for this model request."""

import hashlib
from types import SimpleNamespace

import pytest

from forest.chat_completions_effect_driver import (
    ChatCompletionsEffectDriver,
    _canonical,
)
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
