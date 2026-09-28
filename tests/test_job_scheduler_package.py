from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.packages import PackageCatalog
from forest.runtime_modules import ModuleLifecycleState, RuntimeModuleRegistry
from forest.storage.authorization_authority import CapabilityGrant


@pytest.mark.postgres
async def test_job_scheduler_package_executes_graph_frozen_type_and_reloads(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    catalog = PackageCatalog(tmp_path / "catalog")
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/job_scheduler",
        catalog,
        store,
        installer,
        kernel=kernel,
        initial_configuration={"protocol": "forest.job.scheduler/1"},
        additional_grants=(
            CapabilityGrant("job.read", "bot", store.bot_id),
            CapabilityGrant("job.run", "bot", store.bot_id),
            CapabilityGrant("job.cancel", "bot", store.bot_id),
        ),
    )
    lease = await kernel.runtime.claim_host("host/job-package", "inc/job-package")
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(host, kernel.descriptors + (package.descriptor,))
    await registry.start()
    try:
        assert registry.states["job.scheduler"] is ModuleLifecycleState.ACTIVE
        runtime = registry.instances["job.scheduler"]
        types = await runtime.management_contract.query("jobs.types", {})
        assert [item["job_type"] for item in types["items"]] == [
            "runtime.health-snapshot"
        ]

        queued = await runtime.management_contract.action(
            "jobs.run",
            operation_id="test:job-package:run",
            payload={
                "job_type": "runtime.health-snapshot",
                "source_conversation_ref": None,
                "input": {},
            },
            authorization=SimpleNamespace(web_subject_ref="test-admin"),
            authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
        )
        for _ in range(200):
            item = await runtime.management_contract.query(
                "jobs.read", {"job_id": queued["job_id"]}
            )
            if item["state"] in {
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
                "UNKNOWN",
            }:
                break
            await asyncio.sleep(0.05)
        task_errors = [
            repr(task.exception())
            for task in runtime.owned_tasks()
            if task.done() and not task.cancelled()
        ]
        assert item["error_code"] is None, item["error_code"]
        assert item["requested_by"] == "test-admin"
        assert item["state"] == "SUCCEEDED", (
            item,
            runtime.failure,
            task_errors,
        )
        assert item["result"]["runtime_status"] == "ACTIVE"

        previous_generation = registry.graph_identity.generations["job.scheduler"]
        candidate = RuntimeModuleRegistry(
            host, kernel.descriptors + (package.descriptor,)
        )
        await candidate.prewarm()
        assert (
            await runtime.management_contract.query("jobs.types", {})
        )["graph_id"] == registry.graph_identity.graph_id
        await registry.drain_owned_work_for_replacement(candidate.activation_id)
        assert runtime.state.value == "QUIESCENT"
        await candidate.start()
        assert candidate.graph_identity.generations["job.scheduler"] != previous_generation
        assert candidate.instances["job.scheduler"].state.value == "ACTIVE"
        candidate_types = await candidate.instances[
            "job.scheduler"
        ].management_contract.query("jobs.types", {})
        assert candidate_types["graph_id"] == candidate.graph_identity.graph_id
        assert await registry.close() == []
        registry = candidate
    finally:
        await registry.close()
