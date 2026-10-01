from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from psycopg import sql
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
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

        input_validator, output_validator = runtime._service_schemas["jobs.enqueue"]
        runtime._service_schemas["jobs.enqueue"] = (
            input_validator, Draft202012Validator({"not": {}})
        )
        try:
            with pytest.raises(ForestError) as rejected:
                await runtime.management_contract.action(
                    "jobs.run", operation_id="test:job-package:invalid-output",
                    payload={
                        "job_type": "runtime.health-snapshot",
                        "source_conversation_ref": None, "input": {},
                    },
                    authorization=SimpleNamespace(actor_ref="test-admin", web_subject_ref="test-admin"),
                    authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
                )
            assert rejected.value.code == "MODULE_MANAGEMENT_OUTPUT_INVALID"
        finally:
            runtime._service_schemas["jobs.enqueue"] = (input_validator, output_validator)
        async with store.pool.connection() as conn:
            rows = await conn.execute(
                sql.SQL("SELECT count(*) AS n FROM {}.enqueue_operations "
                        "WHERE bot_id=%s AND operation_id=%s").format(
                    sql.Identifier(package.descriptor.storage.spec.namespace)
                ),
                (store.bot_id, "test:job-package:invalid-output"),
            )
            assert (await rows.fetchone())["n"] == 0

        identity = runtime.generation_identity
        binding = runtime._prepared_binding
        direct_operation = "test:job-package:invalid-domain-output"
        command = next(
            item for item in runtime.component["domain_commands"]
            if item["name"] == "jobs.enqueue"
        )
        with pytest.raises(ForestError) as rejected_command:
            await runtime._domain_commands.invoke_command(
                PackageCallScope(
                    bot_id=store.bot_id, component_id="job.scheduler",
                    generation_id=identity.generation_id,
                    instance_id=binding.instance_id,
                    worker_incarnation=binding.worker_incarnation,
                    call_id="call:invalid-output",
                    call_operation_id=direct_operation,
                    causal_root_kind="test", causal_root_id=direct_operation,
                ),
                command,
                {
                    "job_type": "runtime.health-snapshot",
                    "requested_by": "test-admin", "input": {},
                },
                parent_call_id="test:invalid-output", timeout_seconds=10,
                result_validator=lambda _result: False,
            )
        assert rejected_command.value.code == "DOMAIN_OUTPUT_INVALID"
        async with store.pool.connection() as conn:
            rows = await conn.execute(
                sql.SQL("SELECT count(*) AS n FROM {}.enqueue_operations "
                        "WHERE bot_id=%s AND operation_id=%s").format(
                    sql.Identifier(package.descriptor.storage.spec.namespace)
                ),
                (store.bot_id, direct_operation),
            )
            assert (await rows.fetchone())["n"] == 0

        queued = await runtime.management_contract.action(
            "jobs.run",
            operation_id="test:job-package:run",
            payload={
                "job_type": "runtime.health-snapshot",
                "source_conversation_ref": None,
                "input": {},
            },
            authorization=SimpleNamespace(actor_ref="test-admin", web_subject_ref="test-admin"),
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
