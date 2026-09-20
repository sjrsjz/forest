"""Install and describe the clean Agent runtime module."""

from __future__ import annotations

import json
from dataclasses import dataclass

from forest.agent_execution import FrozenToolRoute
from forest.agent_model_executor import AgentToolBinding
from forest.agent_tool_executor import ToolDispatchResult
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
from forest.storage.agent_runtime_authority import AgentPayloadRef, AgentRuntimeRepository
from forest.storage.authorization_authority import CapabilityGrant
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.payload_authority import PayloadSource
from forest.tool_authorization import KernelToolAuthorizationPort
from forest.tool_effect_execution import KernelToolExecutionPort

_RUNTIME_STATUS_TOOL_DECLARATION = {
    "name": "runtime_status",
    "description": "Read the current Bot RuntimeGraph and module lifecycle states.",
    "parameters": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "strict": True,
}
_RUNTIME_STATUS_TOOL_CONTRACT_DIGEST = digest_json(
    {
        "protocol": "forest.agent-tool/1",
        "service_id": "agent.tool.runtime-status",
        "version": 1,
        "declaration": _RUNTIME_STATUS_TOOL_DECLARATION,
    }
)


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


def _runtime_status_tool(context):
    runtime = context.participant("kernel.runtime").service
    payloads = context.participant("kernel.payload").service

    async def dispatch(request):
        if (
            request.executor_component_id != "kernel.runtime"
            or request.service_id != "agent.tool.runtime-status"
            or request.arguments != {}
        ):
            return ToolDispatchResult("FAILED", error_code="RUNTIME_STATUS_TOOL_INPUT_INVALID")
        status = await runtime.status()
        projected = {
            "protocol": "forest.runtime-status-tool-result/1",
            "bot_id": status["bot_id"],
            "status": status["status"],
            "maintenance_epoch": status["maintenance_epoch"],
            "data_admission": status["data_admission"],
            "control_admission": status["control_admission"],
            "graph_id": status["graph_id"],
            "graph_revision": status["graph_revision"],
            "graph_digest": status["graph_digest"],
            "graph_state": status["graph_state"],
            "members": [
                {
                    "component_id": member["component_id"],
                    "generation_id": member["generation_id"],
                    "status": member["status"],
                    "config_revision": member["config_revision"],
                }
                for member in status["members"]
            ],
        }
        epoch = await payloads.epoch()
        saved = await payloads.put_bytes(
            owner_component_id="kernel.runtime",
            owner_kind="tool.result",
            owner_id=request.tool_call_id,
            media_type="application/json",
            content=json.dumps(
                projected,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            sources=(
                PayloadSource(
                    "agent.runtime",
                    "agent.tool-call",
                    request.tool_call_id,
                    1,
                    author_subject_id=request.authority_principal_ref,
                ),
            ),
            expected_deletion_epoch=epoch,
        )
        return ToolDispatchResult(
            "SUCCEEDED",
            result=AgentPayloadRef(
                saved.payload_id,
                saved.content_digest,
                "kernel.runtime",
                "tool.result",
                request.tool_call_id,
            ),
            observation_ref="runtime-status:" + request.tool_call_id,
        )

    return dispatch


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
        status_tool = contracts.service(
            "agent.tool.runtime-status", 1, owned_work=True
        )
        tool_authorization = KernelToolAuthorizationPort(
            authority=kernel.authorization,
            agent_grants=grants,
        )
        tool_execution = KernelToolExecutionPort(
            effects=kernel.effect,
            payloads=kernel.payload,
            runtime=kernel.runtime,
            authorization=kernel.authorization,
            service=_RoutedToolService(status_tool),
        )

        def runtime_status_route():
            route = status_tool.route
            if route.provider_component_id != "kernel.runtime":
                raise ForestError(
                    "AGENT_TOOL_ROUTE_INVALID",
                    "Runtime status tool resolved to another component",
                    503,
                )
            binding = AgentToolBinding(
                "runtime_status",
                route.provider_component_id,
                route.provider_generation_id,
                route.service_id,
                _RUNTIME_STATUS_TOOL_CONTRACT_DIGEST,
                approval_required=False,
                supports_query=True,
            )
            return FrozenToolRoute(
                _RUNTIME_STATUS_TOOL_DECLARATION,
                binding,
                tool_authorization,
                tool_execution,
            )

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
            tool_route_factories=(runtime_status_route,),
            generation_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
        )
        runtime.management_contract = CleanAgentManagementContract(
            configuration,
            payloads=kernel.payload,
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
        factory_protocol="forest.contract-module-factory/1",
        kernel_services=(
            RuntimeKernelServiceSpec(
                "agent.tool.runtime-status",
                1,
                ("kernel.runtime", "kernel.payload"),
                _runtime_status_tool,
            ),
        ),
    )
    return CleanAgentModule(repository, descriptor, configuration, grants)
