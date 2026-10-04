from __future__ import annotations

import pytest

from forest.bootstrap_contributions import (
    BootstrapConfigurationContribution,
    BootstrapConfigurationRegistry,
)
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.official_bot_provisioning import official_bootstrap_contributions
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
