"""Install and describe the clean group Shell module."""

from __future__ import annotations

from dataclasses import dataclass

from forest.clean_shell_runtime import CleanShellRuntime
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeKernelServiceSpec
from forest.runtime_modules import (
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.shell_authorization_port import ShellAuthorizationPort
from forest.shell_command_parser import ShellCommandParser
from forest.shell_gate import CoreShellGate
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.control_unit_of_work import ShellControlResultUnitOfWork
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.shell_command_authority import ShellCommandRepository


@dataclass(frozen=True)
class CleanShellModule:
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


def default_shell_configuration() -> dict:
    return {
        "protocol": "forest.shell.command/1",
        "binding_ref": "binding/main",
        "administrator_principals": [],
    }


def _runtime_status(context):
    return context.participant("kernel.runtime").service.status


def _runtime_maintenance(context):
    runtime = context.participant("kernel.runtime").service

    async def set_data_admission(*, expected_maintenance_epoch, data_admission):
        return await runtime.set_data_admission(
            context.host_lease,
            expected_maintenance_epoch=expected_maintenance_epoch,
            data_admission=data_admission,
        )

    return set_data_admission


def _binding_fence(context):
    return context.participant("adapter.onebot").verify_transport_fence_in


def _route_fence(context):
    runtime = context.participant("kernel.runtime").service

    async def verify(
        conn,
        *,
        component_id,
        generation_id,
        graph_digest,
        route_digest,
    ):
        graph = await runtime.verify_generation_member_in(
            conn,
            component_id=component_id,
            generation_id=generation_id,
        )
        if graph.graph_digest != graph_digest or not route_digest:
            raise ForestError("SHELL_ROUTE_STALE", "Shell command route is stale", 409)
        return graph

    return verify


def _authorization_consumer(context):
    return context.participant("kernel.authorization").service.consume_ticket_in


def _result_commit(context):
    shell = context.participant("shell.command")
    delivery = context.participant("delivery")
    unit = ShellControlResultUnitOfWork(context.store, shell=shell, delivery=delivery)

    async def commit(claim, **values):
        generation = delivery.generation_identity
        if generation is None:
            raise ForestError(
                "SHELL_RESULT_ROUTE_INVALID", "Delivery Generation is unavailable", 503
            )
        return await unit.commit(
            claim,
            delivery_generation_id=generation.generation_id,
            **values,
        )

    return commit


async def bootstrap_clean_shell(
    store,
    installer,
    *,
    kernel,
    initial_configuration: dict | None = None,
) -> CleanShellModule:
    storage = official_storage_bundle(
        "shell.command",
        "forest.shell.command",
        "forest.shell-command.repository/1",
        "storage/official/shell_command.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="shell.command",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.shell.command/1"},
            "binding_ref": {"type": "string"},
            "administrator_principals": {
                "type": "array",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
        },
        "required": ["protocol", "binding_ref", "administrator_principals"],
        "additionalProperties": False,
    }
    await configuration.initialize(
        operation_id="bootstrap:shell.command:configuration",
        value=initial_configuration or default_shell_configuration(),
        validation_digest=digest_json(schema),
    )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="shell.command",
        entries=(CapabilityGrant("management.invoke", "bot", store.bot_id, delegable=True),),
        expected_policy_revision=policy_revision,
    )
    gate = CoreShellGate(store.bot_id)
    parser = ShellCommandParser(kernel.payload)

    def factory(_runtime, contracts):
        repository = ShellCommandRepository(
            store,
            namespace=storage.spec.namespace,
            payload_verifier=kernel.payload.verify_in,
            proof_verifier=gate.verify,
            admission_parser=parser.validate_admission,
            binding_fence_verifier=contracts.service(
                "kernel.uow.shell.binding-fence", 1, owned_work=True
            ),
            generation_verifier=contracts.service(
                "kernel.uow.shell.route-fence", 1, owned_work=True
            ),
            authorization_consumer=contracts.service(
                "kernel.uow.shell.authorization.consume", 1, owned_work=True
            ),
        )
        authorization = ShellAuthorizationPort(
            authority=kernel.authorization,
            grants=grants,
            repository=repository,
        )
        return CleanShellRuntime(
            repository=repository,
            payloads=kernel.payload,
            gate=gate,
            parser=parser,
            authorization=authorization,
            module_configuration=configuration,
            grants=grants,
            status=contracts.service(
                "kernel.runtime.status", 1, admission_lane="CONTROL", owned_work=True
            ),
            maintenance=contracts.service(
                "kernel.runtime.maintenance", 1, admission_lane="CONTROL", owned_work=True
            ),
            result_commit=contracts.service(
                "kernel.uow.shell.result", 1, admission_lane="CONTROL", owned_work=True
            ),
            generation_admission_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
        )

    descriptor = RuntimeModuleDescriptor(
        "shell.command",
        ("kernel.runtime", "kernel.payload", "kernel.authorization"),
        factory,
        official_artifact("bot"),
        storage=storage,
        wakes_on_work=True,
        work_notify_order=0,
        factory_protocol="forest.contract-module-factory/1",
        kernel_services=(
            RuntimeKernelServiceSpec(
                "kernel.runtime.status", 1, ("kernel.runtime",), _runtime_status
            ),
            RuntimeKernelServiceSpec(
                "kernel.runtime.maintenance",
                1,
                ("kernel.runtime",),
                _runtime_maintenance,
            ),
            RuntimeKernelServiceSpec(
                "kernel.uow.shell.binding-fence",
                1,
                ("adapter.onebot",),
                _binding_fence,
            ),
            RuntimeKernelServiceSpec(
                "kernel.uow.shell.route-fence",
                1,
                ("kernel.runtime",),
                _route_fence,
            ),
            RuntimeKernelServiceSpec(
                "kernel.uow.shell.authorization.consume",
                1,
                ("kernel.authorization",),
                _authorization_consumer,
            ),
            RuntimeKernelServiceSpec(
                "kernel.uow.shell.result",
                1,
                ("shell.command", "delivery"),
                _result_commit,
            ),
        ),
    )
    return CleanShellModule(descriptor, configuration, grants)
