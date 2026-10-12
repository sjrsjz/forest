"""Exact host-generated SQL identity for ordinary control package state."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from importlib.resources import files

from forest.packages import storage_bundle_digest
from forest.schema_bundles import StorageBundleSpec


@lru_cache(maxsize=256)
def generated_control_storage(
    component_id: str, scope_kind: str,
) -> tuple[StorageBundleSpec, bytes]:
    if scope_kind not in {"deployment", "host"}:
        raise ValueError("Generated control storage requires a control scope")
    baseline = files("forest.storage.official").joinpath(
        "control_module_configuration.sql"
    ).read_bytes() + b"\n" + files("forest.storage.official").joinpath(
        "control_module_state.sql"
    ).read_bytes()
    schema_id = f"forest.{scope_kind}.config." + hashlib.sha256(
        component_id.encode()
    ).hexdigest()[:32]
    return StorageBundleSpec(
        component_id=component_id, schema_id=schema_id,
        schema_version="2.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract=f"forest.{scope_kind}-configuration.repository/1",
        scope=scope_kind,
    ), baseline


def is_generated_control_storage(bundle, component_id: str, scope_kind: str) -> bool:
    if scope_kind not in {"deployment", "host"}:
        return False
    spec, baseline = generated_control_storage(component_id, scope_kind)
    return (
        bundle.execution_profile == "official"
        and bundle.spec == spec
        and bundle.baseline == baseline
        and bundle.package_digest == storage_bundle_digest(spec, "official")
    )


def is_generated_control_storage_fact(
    storage: dict, component_id: str, scope_kind: str,
) -> bool:
    if scope_kind not in {"deployment", "host"}:
        return False
    spec, _baseline = generated_control_storage(component_id, scope_kind)
    expected = {
        key: value for key, value in spec.as_fact().items() if key != "scope"
    }
    expected.update({
        "package_digest": storage_bundle_digest(spec, "official"),
        "execution_profile": "official",
    })
    return storage == expected
