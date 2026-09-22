"""Provision the official durable routing authority as a runtime module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.packages import digest_json
from forest.routing_graph_runtime import RoutingGraphRuntime
from forest.runtime_modules import (
    OwnedWorkLifecycle,
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.component_event_authority import ComponentEventCommands
from forest.storage.component_job_authority import ComponentJobCommands
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.routing_graph_directory import RoutingDirectoryCommands

_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"protocol": {"const": "forest.routing.graph/1"}},
    "required": ["protocol"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class RoutingGraphModule:
    directory: RoutingDirectoryCommands
    event_commands: ComponentEventCommands
    job_commands: ComponentJobCommands
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


async def bootstrap_routing_graph(store, installer, *, kernel) -> RoutingGraphModule:
    storage = official_storage_bundle(
        "routing.graph",
        "forest.routing.graph",
        "forest.routing-graph.repository/1",
        "storage/official/routing_graph.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="routing.graph",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    await configuration.ensure_initialized(
        operation_id="bootstrap:routing.graph:configuration",
        value={"protocol": "forest.routing.graph/1"},
        validation_digest=digest_json(_SCHEMA),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="routing.graph",
        entries=(
            CapabilityGrant("routing.dispatch", "bot", store.bot_id),
        ),
        expected_policy_revision=policy_revision,
    )
    directory = RoutingDirectoryCommands(
        store,
        namespace=storage.spec.namespace,
    )
    event_commands = ComponentEventCommands(
        store,
        payloads=kernel.payload,
        generation_member_verifier=kernel.runtime.verify_generation_member_in,
        namespace=storage.spec.namespace,
    )
    job_commands = ComponentJobCommands(
        store,
        payloads=kernel.payload,
        generation_member_verifier=kernel.runtime.verify_generation_member_in,
        namespace=storage.spec.namespace,
    )

    def factory(_runtime, contracts):
        return RoutingGraphRuntime(
            directory=directory,
            event_commands=event_commands,
            job_commands=job_commands,
            payloads=kernel.payload,
            module_configuration=configuration,
            grants=grants,
            event_publications=contracts.event_publications(),
            event_subscriptions=contracts.event_subscriptions(),
            component_jobs=contracts.component_jobs(),
        )

    descriptor = RuntimeModuleDescriptor(
        "routing.graph",
        ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        factory,
        official_artifact("bot"),
        management=(configuration_management_contribution("Routing Graph"),),
        storage=storage,
        wakes_on_work=True,
        work_notify_order=25,
        owned_work_lifecycle=OwnedWorkLifecycle.FREEZE_DRAIN,
    )
    return RoutingGraphModule(
        directory,
        event_commands,
        job_commands,
        descriptor,
        configuration,
        grants,
    )
