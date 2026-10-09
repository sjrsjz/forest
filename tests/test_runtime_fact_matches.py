from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.runtime_modules import (
    RuntimeArtifactIdentity,
    RuntimeFactMatch,
    RuntimeModuleDescriptor,
    RuntimeModuleRegistry,
)


def _descriptor(component_id, *, requires=(), fact_matches=()):
    return RuntimeModuleDescriptor(
        component_id=component_id,
        requires=requires,
        factory=lambda *_args: None,
        artifact=RuntimeArtifactIdentity(
            package_id="test." + component_id,
            package_version="1.0.0",
            package_digest="sha256:" + "0" * 64,
            scope="bot",
            driver="forest.native/1",
            trust_level="official",
        ),
        fact_matches=fact_matches,
    )


def _candidate(embedding_space, vector_space):
    matches = (
        RuntimeFactMatch(
            "embedding.multimodal", "/space/space_id", "/space/space_id"
        ),
        RuntimeFactMatch(
            "embedding.multimodal", "/space/dimensions", "/space/dimensions"
        ),
    )
    registry = RuntimeModuleRegistry(
        SimpleNamespace(),
        (
            _descriptor("embedding.multimodal"),
            _descriptor(
                "vector.index",
                requires=("embedding.multimodal",),
                fact_matches=matches,
            ),
        ),
    )
    registry.instances = {
        "embedding.multimodal": SimpleNamespace(
            replacement_candidate_fact=lambda: {"facts": {"space": embedding_space}}
        ),
        "vector.index": SimpleNamespace(
            replacement_candidate_fact=lambda: {"facts": {"space": vector_space}}
        ),
    }
    return registry


def test_candidate_fact_match_fences_space_and_dimensions_before_publication():
    space = {"space_id": "sha256:" + "a" * 64, "dimensions": 4096}
    _candidate(space, space.copy())._validate_fact_matches()

    for changed in (
        {"space_id": "sha256:" + "b" * 64, "dimensions": 4096},
        {"space_id": space["space_id"], "dimensions": 1024},
    ):
        with pytest.raises(ForestError) as caught:
            _candidate(space, changed)._validate_fact_matches()
        assert caught.value.code == "RUNTIME_GRAPH_FACT_MISMATCH"

    with pytest.raises(ForestError) as caught:
        _candidate(space, {"space_id": space["space_id"]})._validate_fact_matches()
    assert caught.value.code == "RUNTIME_GRAPH_FACT_MISSING"
