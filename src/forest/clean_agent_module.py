"""Install and describe the clean Agent runtime module."""

from __future__ import annotations

import json
from dataclasses import dataclass

from forest.agent_execution import FrozenToolRoute
from forest.agent_model_executor import AgentToolBinding
from forest.agent_tool_executor import ToolDispatchResult
from forest.builtin_agent_shell import AGENT_SHELL_COMMANDS
from forest.clean_agent_management import CleanAgentManagementContract
from forest.clean_agent_runtime import CleanAgentRuntime
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeKernelServiceSpec
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    official_artifact,
    official_storage_bundle,
)
from forest.storage.agent_runtime_authority import AgentRuntimeRepository
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.payload_authority import PayloadSource
from forest.tool_authorization import KernelToolAuthorizationPort
from forest.tool_effect_execution import KernelToolExecutionPort


@dataclass(frozen=True)
class CleanAgentModule:
    repository: AgentRuntimeRepository
    descriptor: RuntimeModuleDescriptor
    configuration: ModuleConfigurationRepository
    grants: object


class _RoutedChatProvider:
    """Provider facade backed only by frozen Contract Router handles."""

    def __init__(self, project, dispatch) -> None:
        self._project = project
        self._dispatch = dispatch

    async def prepare_request(self, *args, **kwargs):
        return await self._project(*args, **kwargs)

    async def dispatch(self, *args, **kwargs):
        return await self._dispatch(*args, **kwargs)


class _RoutedMemory:
    """Memory facade backed by the active Generation's frozen route."""

    def __init__(self, recall) -> None:
        self._recall = recall

    async def recall(self, *args, **kwargs):
        return await self._recall(*args, **kwargs)


class _RoutedMedia:
    def __init__(self, resolve_context) -> None:
        self._resolve_context = resolve_context

    async def resolve_context(self, snapshot):
        return await self._resolve_context(snapshot)


class _RoutedToolService:
    def __init__(self, dispatch) -> None:
        self._dispatch = dispatch

    async def dispatch(self, request):
        return await self._dispatch(request)

    async def query(self, _request):
        return ToolDispatchResult(
            "UNKNOWN",
            error_code="TOOL_RESULT_UNQUERYABLE_AFTER_RESTART",
            observation_ref="runtime-status:unqueryable",
        )


def _cancel_wake(context):
    conversation = context.participant("conversation.scheduler")

    async def wake(**values):
        return await conversation.wake_cancel_request(**values)

    return wake


async def bootstrap_clean_agent(
    store,
    installer,
    *,
    kernel,
    preset_system: tuple[str, ...] = (),
) -> CleanAgentModule:
    storage = official_storage_bundle(
        "agent.runtime",
        "forest.agent.runtime",
        "forest.agent-runtime.repository/1",
        "storage/official/agent_runtime.sql",
    )
    await installer.install(
        storage.spec,
        storage.baseline,
        storage.package_digest,
        execution_profile=storage.execution_profile,
    )
    configuration = ModuleConfigurationRepository(
        store,
        component_id="agent.runtime",
        namespace=storage.spec.namespace,
        payloads=kernel.payload,
    )
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "protocol": {"const": "forest.agent.runtime/1"},
            "agent_spec_revision": {"type": "string"},
            "preset_revision": {"type": "string"},
            "preset": {"type": "object"},
            "run_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
            "max_steps": {"type": "integer", "minimum": 1, "maximum": 256},
            "output_tokens": {"type": "integer", "minimum": 1},
            "stream": {"type": "boolean"},
        },
        "required": [
            "protocol",
            "agent_spec_revision",
            "preset_revision",
            "preset",
            "run_timeout_seconds",
            "max_steps",
            "output_tokens",
            "stream",
        ],
        "additionalProperties": False,
    }
    try:
        await configuration.desired()
    except ForestError as exc:
        if exc.code != "MODULE_CONFIGURATION_UNINITIALIZED":
            raise
        preset_owner_id = "preset:default:1"
        epoch = await kernel.payload.epoch()
        preset = await kernel.payload.put_bytes(
            owner_component_id="agent.runtime",
            owner_kind="preset.context",
            owner_id=preset_owner_id,
            media_type="application/json",
            content=json.dumps(
                {"system": list(preset_system)}, ensure_ascii=False, separators=(",", ":")
            ).encode(),
            sources=(
                PayloadSource(
                    "agent.runtime",
                    "DEFAULT_PRESET",
                    preset_owner_id,
                    1,
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        await configuration.initialize(
            operation_id="bootstrap:agent.runtime:configuration",
            value={
                "protocol": "forest.agent.runtime/1",
                "agent_spec_revision": "forest.agent.default/1",
                "preset_revision": "preset:default/1",
                "preset": {
                    "payload_id": preset.payload_id,
                    "digest": preset.content_digest,
                    "owner_id": preset_owner_id,
                },
                "run_timeout_seconds": 300,
                "max_steps": 32,
                "output_tokens": 1024,
                "stream": False,
            },
            validation_digest=digest_json(schema),
        )
    policy_revision = await kernel.authorization.revision()
    grants = await kernel.authorization.create_grant_set(
        grantee_kind="component",
        grantee_id="agent.runtime",
        entries=(
            CapabilityGrant("agent.run", "bot", store.bot_id),
            CapabilityGrant("provider.invoke", "bot", store.bot_id),
            CapabilityGrant("tool.invoke", "bot", store.bot_id, delegable=True),
        ),
        expected_policy_revision=policy_revision,
    )
    repository = AgentRuntimeRepository(
        store,
        namespace=storage.spec.namespace,
        payload_verifier=kernel.payload.verify_in,
    )

    def factory(_runtime, contracts):
        tool_authorization = KernelToolAuthorizationPort(
            authority=kernel.authorization,
            agent_grants=grants,
        )
        tool_route_factories = []
        tool_requirements = []
        for owner_component_id, contribution, service in contracts.agent_tools():
            tool_requirements.append((contribution.service_id, contribution.version))
            execution = KernelToolExecutionPort(
                effects=kernel.effect,
                payloads=kernel.payload,
                runtime=kernel.runtime,
                authorization=kernel.authorization,
                service=_RoutedToolService(service),
            )

            def route_factory(
                owner_component_id=owner_component_id,
                contribution=contribution,
                service=service,
                execution=execution,
            ):
                route = service.route
                if route.provider_component_id != owner_component_id:
                    raise ForestError(
                        "AGENT_TOOL_ROUTE_INVALID",
                        "Agent tool service resolved outside its contributor Generation",
                        503,
                    )
                binding = AgentToolBinding(
                    contribution.name,
                    route.provider_component_id,
                    route.provider_generation_id,
                    route.service_id,
                    contribution.contract_digest,
                    approval_required=contribution.approval_required,
                    supports_query=contribution.supports_query,
                )
                return FrozenToolRoute(
                    contribution.declaration,
                    binding,
                    tool_authorization,
                    execution,
                )

            tool_route_factories.append(route_factory)

        runtime = CleanAgentRuntime(
            repository=repository,
            module_configuration=configuration,
            grants=grants,
            payloads=kernel.payload,
            provider=_RoutedChatProvider(
                contracts.service("provider.model.project", 1, owned_work=True),
                contracts.service("provider.model.dispatch", 1, owned_work=True),
            ),
            memory=_RoutedMemory(
                contracts.service("memory.recall", 1, owned_work=True),
            ),
            media=_RoutedMedia(
                contracts.service("media.context.resolve", 1, owned_work=True),
            ),
            cancel_wake=contracts.service(
                "kernel.uow.agent.cancel-wake",
                1,
                admission_lane="CONTROL",
            ),
            tool_route_factories=tuple(tool_route_factories),
            tool_requirements=tuple(tool_requirements),
            generation_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
        )
        runtime.management_contract = CleanAgentManagementContract(
            configuration,
            payloads=kernel.payload,
            runtime=runtime,
            storage_schema=schema,
        )
        return runtime

    descriptor = RuntimeModuleDescriptor(
        "agent.runtime",
        (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "kernel.effect",
            "memory.graph",
            "media.capture",
            "provider.chat-completions",
        ),
        factory,
        official_artifact("bot"),
        management=(
            RuntimeManagementContribution(
                "agent-profile",
                "Agent 协议与预设",
                ("bot",),
                "/static/modules/clean-agent.html",
                "/static/clean-agent.js",
                style="/static/modules/clean-agent.css",
                group="runtime",
                order=5,
            ),
        ),
        storage=storage,
        kernel_services=(
            RuntimeKernelServiceSpec(
                "kernel.uow.agent.cancel-wake",
                1,
                ("conversation.scheduler",),
                _cancel_wake,
            ),
        ),
        shell_commands=AGENT_SHELL_COMMANDS,
    )
    return CleanAgentModule(repository, descriptor, configuration, grants)
