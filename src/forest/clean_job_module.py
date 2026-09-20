"""Install and describe the durable module-contributed Job scheduler."""

from __future__ import annotations

from dataclasses import dataclass

from forest.builtin_job_shell import JOB_SHELL_COMMANDS
from forest.clean_job_management import CleanJobManagementContract
from forest.clean_job_runtime import CleanJobRuntime
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.job_scheduler_authority import JobSchedulerRepository
from forest.storage.module_configuration import ModuleConfigurationRepository

_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"protocol": {"const": "forest.job.scheduler/1"}},
    "required": ["protocol"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class CleanJobModule:
    repository: JobSchedulerRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_job(store, installer, *, kernel) -> CleanJobModule:
    storage = official_storage_bundle(
        "job.scheduler",
        "forest.job.scheduler",
        "forest.job-scheduler.repository/1",
        "storage/official/job_scheduler.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="job.scheduler",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    await configuration.initialize(
        operation_id="bootstrap:job.scheduler:configuration",
        value={"protocol": "forest.job.scheduler/1"},
        validation_digest=digest_json(_SCHEMA),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="job.scheduler",
        entries=tuple(
            CapabilityGrant(capability, "bot", store.bot_id)
            for capability in ("job.read", "job.run", "job.cancel")
        ),
        expected_policy_revision=policy_revision,
    )
    repository = JobSchedulerRepository(
        store,
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
        generation_admission_verifier=kernel.runtime.verify_active_generation_in,
        generation_member_verifier=kernel.runtime.verify_generation_member_in,
    )

    def factory(_runtime, contracts):
        runtime = CleanJobRuntime(
            repository=repository,
            payloads=kernel.payload,
            module_configuration=configuration,
            grants=grants,
            generation_verifier=kernel.runtime.verify_active_generation,
            job_routes=contracts.job_types(),
        )
        runtime.management_contract = CleanJobManagementContract(
            configuration,
            runtime=runtime,
            schema=_SCHEMA,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "job.scheduler",
        ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        factory,
        official_artifact("bot"),
        management=(
            RuntimeManagementContribution(
                "job-control",
                "任务执行",
                ("bot",),
                "/static/modules/clean-jobs.html",
                "/static/clean-jobs.js",
                style="/static/modules/clean-jobs.css",
                group="operations",
                order=10,
            ),
            configuration_management_contribution("任务调度配置"),
        ),
        storage=storage,
        wakes_on_work=True,
        work_notify_order=30,
        factory_protocol="forest.contract-module-factory/1",
        shell_commands=JOB_SHELL_COMMANDS,
    )
    return CleanJobModule(repository, descriptor, configuration, grants)
