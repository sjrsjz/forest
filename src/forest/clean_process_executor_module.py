"""Install the official profile-controlled native process executor."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.packages import digest_json
from forest.process_executor_runtime import ProcessExecutorRuntime
from forest.runtime_modules import (
    RuntimeJobTypeContribution,
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository

PROCESS_EXECUTOR_CONFIGURATION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["protocol", "profiles"],
    "properties": {
        "protocol": {"const": "forest.process.executor/1"},
        "profiles": {
            "type": "array",
            "maxItems": 128,
            "items": {
                "type": "object",
                "required": [
                    "profile_id",
                    "executable",
                    "executable_sha256",
                    "arguments_schema",
                    "argv",
                    "working_directory",
                    "environment",
                    "run_as",
                    "network_policy",
                    "require_posix_limits",
                    "limits",
                ],
                "properties": {
                    "profile_id": {
                        "type": "string",
                        "pattern": "^[a-z0-9][a-z0-9._-]{0,63}$",
                    },
                    "executable": {"type": "string", "minLength": 1, "maxLength": 4096},
                    "executable_sha256": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
                    "arguments_schema": {"type": "object"},
                    "argv": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 256,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["literal"],
                                    "properties": {"literal": {"type": "string", "minLength": 1}},
                                    "additionalProperties": False,
                                },
                                {
                                    "type": "object",
                                    "required": ["argument"],
                                    "properties": {"argument": {"type": "string", "minLength": 1}},
                                    "additionalProperties": False,
                                },
                            ]
                        },
                    },
                    "working_directory": {"type": "string", "minLength": 1, "maxLength": 4096},
                    "environment": {
                        "type": "object",
                        "maxProperties": 128,
                        "propertyNames": {"pattern": "^[A-Z_][A-Z0-9_]{0,63}$"},
                        "additionalProperties": {"type": "string", "maxLength": 8192},
                    },
                    "run_as": {
                        "type": "object",
                        "required": ["uid", "gid"],
                        "properties": {
                            "uid": {"type": "integer", "minimum": 0},
                            "gid": {"type": "integer", "minimum": 0},
                        },
                        "additionalProperties": False,
                    },
                    "network_policy": {"enum": ["deny", "inherit"]},
                    "require_posix_limits": {"type": "boolean"},
                    "limits": {
                        "type": "object",
                        "required": [
                            "cpu_seconds",
                            "memory_bytes",
                            "wall_seconds",
                            "output_bytes",
                            "open_files",
                            "processes",
                            "terminate_grace_seconds",
                        ],
                        "properties": {
                            "cpu_seconds": {"type": "integer", "minimum": 1, "maximum": 3600},
                            "memory_bytes": {
                                "type": "integer",
                                "minimum": 16777216,
                                "maximum": 68719476736,
                            },
                            "wall_seconds": {"type": "integer", "minimum": 1, "maximum": 3600},
                            "output_bytes": {
                                "type": "integer",
                                "minimum": 1024,
                                "maximum": 33554432,
                            },
                            "open_files": {"type": "integer", "minimum": 8, "maximum": 4096},
                            "processes": {"type": "integer", "minimum": 1, "maximum": 1024},
                            "terminate_grace_seconds": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 60,
                            },
                        },
                        "additionalProperties": False,
                    },
                },
                "additionalProperties": False,
            },
        },
    },
    "additionalProperties": False,
}
NATIVE_EXECUTE_JOB = RuntimeJobTypeContribution(
    "native.execute",
    1,
    "job.process.execute",
    1,
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["profile_id", "arguments"],
        "properties": {
            "profile_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9._-]{0,63}$"},
            "arguments": {"type": "object"},
        },
        "additionalProperties": False,
    },
    max_attempts=1,
    timeout_seconds=3660,
    max_concurrency=2,
)


def default_process_executor_configuration() -> dict:
    return {"protocol": "forest.process.executor/1", "profiles": []}


@dataclass(frozen=True)
class CleanProcessExecutorModule:
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_process_executor(store, installer, *, kernel):
    storage = official_storage_bundle(
        "process.executor",
        "forest.process.executor",
        "forest.process-executor.repository/1",
        "storage/official/process_executor.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="process.executor",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    await configuration.ensure_initialized(
        operation_id="bootstrap:process.executor:configuration",
        value=default_process_executor_configuration(),
        validation_digest=digest_json(PROCESS_EXECUTOR_CONFIGURATION_SCHEMA),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="process.executor",
        entries=(CapabilityGrant("process.execute", "bot", store.bot_id),),
        expected_policy_revision=policy_revision,
    )

    def factory(_runtime, _contracts):
        runtime = ProcessExecutorRuntime(
            module_configuration=configuration,
            grants=grants,
            payloads=kernel.payload,
        )
        runtime.management_contract = CleanModuleConfigurationContract(
            configuration,
            schema=PROCESS_EXECUTOR_CONFIGURATION_SCHEMA,
            validator=ProcessExecutorRuntime._parse_configuration,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "process.executor",
        ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        factory,
        official_artifact("bot"),
        management=(
            RuntimeManagementContribution(
                "configuration",
                "受控进程执行",
                ("bot",),
                "/static/modules/clean-process-executor.html",
                "/static/clean-process-executor.js",
                style="/static/modules/clean-process-executor.css",
                group="configuration",
                order=10,
            ),
        ),
        storage=storage,
        job_types=(NATIVE_EXECUTE_JOB,),
    )
    return CleanProcessExecutorModule(descriptor, configuration, grants)
