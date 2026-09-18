"""Install and describe the clean asynchronous media capture module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.clean_media_runtime import CleanMediaCaptureRuntime
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.media_capture_authority import MediaCaptureRepository
from forest.storage.module_configuration import ModuleConfigurationRepository


class _RoutedMemoryCapture:
    def __init__(self, capture, unavailable) -> None:
        self._capture = capture
        self._unavailable = unavailable

    async def capture_source(self, **values):
        return await self._capture(**values)

    async def mark_unavailable(self, **values):
        return await self._unavailable(**values)


@dataclass(frozen=True)
class CleanMediaModule:
    repository: MediaCaptureRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_clean_media(
    store,
    installer,
    *,
    kernel,
    client_factory=None,
) -> CleanMediaModule:
    storage = official_storage_bundle(
        "media.capture",
        "forest.media.capture",
        "forest.media-capture.repository/1",
        "storage/official/media_capture.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="media.capture",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "protocol",
            "timeout_seconds",
            "max_connections",
            "max_attempts",
            "context_wait_seconds",
        ],
        "properties": {
            "protocol": {"const": "forest.media.capture/1"},
            "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 30},
            "max_connections": {"type": "integer", "minimum": 1, "maximum": 16},
            "max_attempts": {"type": "integer", "minimum": 1, "maximum": 10},
            "context_wait_seconds": {"type": "number", "minimum": 0, "maximum": 15},
        },
        "additionalProperties": False,
    }
    try:
        await configuration.desired()
    except ForestError as exc:
        if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        await configuration.initialize(
            operation_id="bootstrap:media.capture:configuration",
            value={
                "protocol": "forest.media.capture/1",
                "timeout_seconds": 15,
                "max_connections": 4,
                "max_attempts": 3,
                "context_wait_seconds": 3,
            },
            validation_digest=digest_json(schema),
        )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="media.capture",
        entries=(
            CapabilityGrant("media.capture", "bot", store.bot_id),
            CapabilityGrant("memory.write", "bot", store.bot_id),
        ),
        expected_policy_revision=policy_revision,
    )
    repository = MediaCaptureRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=kernel.payload.verify_in,
    )

    def factory(_runtime, contracts):
        values = {
            "repository": repository,
            "payloads": kernel.payload,
            "module_configuration": configuration,
            "grants": grants,
            "generation_verifier": kernel.runtime.verify_active_generation,
            "generation_admission_verifier": kernel.runtime.verify_active_generation_in,
            "generation_member_verifier": kernel.runtime.verify_generation_member_in,
            "memory": _RoutedMemoryCapture(
                contracts.service("memory.source.capture", 1, owned_work=True),
                contracts.service("memory.source.unavailable", 1, owned_work=True),
            ),
        }
        if client_factory is not None:
            values["client_factory"] = client_factory
        runtime = CleanMediaCaptureRuntime(**values)
        runtime.management_contract = CleanModuleConfigurationContract(
            configuration,
            schema=schema,
            validator=CleanMediaCaptureRuntime._parse_profile,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "media.capture",
        (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "memory.graph",
        ),
        factory,
        official_artifact("bot"),
        management=(configuration_management_contribution("媒体采集"),),
        storage=storage,
        wakes_on_work=True,
        work_notify_order=15,
        factory_protocol="forest.contract-module-factory/1",
    )
    return CleanMediaModule(repository, descriptor, configuration, grants)
