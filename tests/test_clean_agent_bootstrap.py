from __future__ import annotations

import json

import pytest
from test_schema_bundles import clean_authority as clean_authority

from forest.clean_agent_module import bootstrap_clean_agent
from forest.core_runtime import bootstrap_clean_kernel


@pytest.mark.postgres
async def test_agent_preset_is_supplied_once_and_restart_ignores_bootstrap_drift(
    clean_authority,
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    first = await bootstrap_clean_agent(
        store,
        installer,
        kernel=kernel,
        preset_system=("first supplied preset",),
    )
    initial = await first.configuration.desired(include_value=True)

    restarted = await bootstrap_clean_agent(
        store,
        installer,
        kernel=kernel,
        preset_system=("must not replace module state",),
    )
    current = await restarted.configuration.desired(include_value=True)

    assert current == initial
    payload = json.loads(await kernel.payload.read_bytes(current.value["preset"]["payload_id"]))
    assert payload == {"system": ["first supplied preset"]}
