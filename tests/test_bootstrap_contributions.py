from __future__ import annotations

from types import SimpleNamespace

import pytest

from forest.bootstrap_contributions import (
    BootstrapConfigurationContribution,
    BootstrapConfigurationRegistry,
)
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.official_bot_provisioning import (
    OfficialBotCatalogSettingsResolver,
    OfficialBotSettingsResolver,
    official_bootstrap_contributions,
)
from forest.runtime_modules import RuntimeManagementContribution


def _contribution(component_id: str = "example.component"):
    return BootstrapConfigurationContribution(
        component_id=component_id,
        label="Example",
        description="Example install-time settings.",
        schema={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"enabled": {"type": "boolean"}},
            "required": ["enabled"],
            "additionalProperties": False,
        },
        initial_value={"enabled": True},
        package_digest="sha256:" + "a" * 64,
        surface=RuntimeManagementContribution(
            "bootstrap-" + component_id,
            "Example",
            ("deployment",),
            "/static/modules/bootstrap-component.html",
            "/static/bootstrap-component.js",
        ),
    )


def test_bootstrap_registry_owns_schema_digests_and_returns_defensive_facts():
    registry = BootstrapConfigurationRegistry((_contribution(),))
    fact = registry.facts()[0]
    fact["initial_value"]["enabled"] = False

    assert registry.facts()[0]["initial_value"] == {"enabled": True}
    assert registry.schema_digests()["example.component"].startswith("sha256:")
    assert registry.validate_components({"example.component": {"enabled": False}}) == {
        "example.component": {"enabled": False}
    }


def test_bootstrap_registry_rejects_duplicate_or_inexact_component_sets():
    contribution = _contribution()
    with pytest.raises(ValueError, match="duplicated"):
        BootstrapConfigurationRegistry((contribution, contribution))

    registry = BootstrapConfigurationRegistry((contribution,))
    with pytest.raises(ForestError) as missing:
        registry.validate_components({})
    assert missing.value.code == "BOOTSTRAP_COMPONENT_SET_INVALID"
    with pytest.raises(ForestError) as invalid:
        registry.validate_components({"example.component": {"enabled": "yes"}})
    assert invalid.value.code == "BOOTSTRAP_COMPONENT_CONFIGURATION_INVALID"


def test_official_package_seeds_bind_vector_index_to_embedding_space():
    registry = BootstrapConfigurationRegistry(official_bootstrap_contributions())
    values = {item.component_id: item.initial_value for item in registry.contributions}
    embedding = EmbeddingProfile(**values["embedding.multimodal"]["profile"])
    vector = values["vector.index"]

    assert (vector["space_id"], vector["dimensions"]) == (
        embedding.space_id, embedding.dimensions
    )
    invalid = {key: value.copy() for key, value in values.items()}
    invalid["embedding.multimodal"]["profile"] = {
        **values["embedding.multimodal"]["profile"], "dimensions": 4095,
    }
    with pytest.raises(ForestError) as rejected:
        registry.validate_components(invalid)
    assert rejected.value.code == "BOOTSTRAP_COMPONENT_CONFIGURATION_INVALID"


@pytest.mark.asyncio
async def test_profile_rejects_package_change_even_when_schema_is_unchanged():
    registry = BootstrapConfigurationRegistry(official_bootstrap_contributions())
    components = tuple(
        SimpleNamespace(
            component_id=item.component_id,
            config=item.initial_value,
            schema_digest=item.schema_digest,
            package_digest="sha256:" + "f" * 64
            if item.component_id == "provider.chat-completions"
            else item.package_digest,
        )
        for item in registry.contributions
    )
    profile = SimpleNamespace(
        bot_id="bot/test", storage_profile_ref="storage/test", storage_generation=1,
        components=components,
    )
    storage = SimpleNamespace(
        bot_id="bot/test", profile_ref="storage/test", storage_generation=1,
        dsn="postgresql://localhost/forest_test",
    )
    class Secrets:
        async def verify_reference_for_purpose(self, _ref, _purpose):
            return None

    resolver = OfficialBotSettingsResolver(secrets=Secrets(), bootstrap_registry=registry)

    with pytest.raises(ForestError) as rejected:
        await resolver.resolve(profile=profile, storage=storage)
    assert rejected.value.code == "OFFICIAL_BOOTSTRAP_PACKAGE_MISMATCH"

@pytest.mark.asyncio
async def test_ready_bot_launch_uses_profile_only_as_provenance():
    profile = SimpleNamespace(
        bot_id="bot/test", storage_profile_ref="storage/test", storage_generation=1,
        manifest_digest="sha256:" + "a" * 64,
        components=(),
    )

    class Profiles:
        async def load(self, _reference):
            return profile

    storage = SimpleNamespace(
        bot_id="bot/test", profile_ref="storage/test", storage_generation=1,
        dsn="postgresql://localhost/forest_test",
    )
    launch = await OfficialBotCatalogSettingsResolver(
        bootstrap_profiles=Profiles(),
    )(
        {"bot_id": "bot/test", "bootstrap_profile_ref": "bootstrap/test",
         "bootstrap_manifest_digest": profile.manifest_digest},
        storage,
    )
    assert (launch.bot_id, launch.dsn) == (storage.bot_id, storage.dsn)
    assert not hasattr(launch, "agent_preset_system")


@pytest.mark.asyncio
async def test_package_creation_constraints_bind_secret_purpose_and_origin():
    registry = BootstrapConfigurationRegistry(official_bootstrap_contributions())
    components = tuple(
        SimpleNamespace(
            component_id=item.component_id,
            config=item.initial_value.copy(),
            schema_digest=item.schema_digest,
            package_digest=item.package_digest,
        )
        for item in registry.contributions
    )
    profile = SimpleNamespace(
        bot_id="bot/test", storage_profile_ref="storage/test", storage_generation=1,
        components=components,
    )
    storage = SimpleNamespace(
        bot_id="bot/test", profile_ref="storage/test", storage_generation=1,
        dsn="postgresql://localhost/forest_test",
    )
    calls = []

    class Secrets:
        async def verify_reference_for_purpose(self, reference, purpose):
            calls.append((reference, purpose))
            return None

    resolver = OfficialBotSettingsResolver(secrets=Secrets(), bootstrap_registry=registry)
    await resolver.resolve(profile=profile, storage=storage)
    assert {purpose for _reference, purpose in calls} == {
        "provider.chat-completions.credential",
        "embedding.multimodal.credential",
        "adapter.onebot.access-token",
    }
    assert any(
        item.component_id == "vector.index"
        and item.secret_refs == (("credential_ref", "vector.index.credential", True),)
        for item in registry.contributions
    )
    onebot = next(item for item in components if item.component_id == "adapter.onebot")
    onebot.config["websocket_url"] = "https://example.com"
    with pytest.raises(ForestError) as rejected:
        await resolver.resolve(profile=profile, storage=storage)
    assert rejected.value.code == "OFFICIAL_BOOTSTRAP_CONFIGURATION_INVALID"
