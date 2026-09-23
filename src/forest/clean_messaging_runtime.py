"""Install and describe clean messaging modules without constructing a Bot monolith."""

from __future__ import annotations

from dataclasses import dataclass

from forest.adapter_onebot_runtime import CleanOneBotRuntime
from forest.approval_wait_uow import ApprovalWaitUnitOfWork
from forest.clean_management_contract import CleanModuleConfigurationContract
from forest.clean_onebot_management import CleanOneBotManagementContract
from forest.conversation_reply_planner import DurableFinalReplyPlanner
from forest.conversation_runtime import (
    CONVERSATION_CONFIGURATION_SCHEMA,
    CleanConversationSchedulerRuntime,
    default_conversation_configuration,
)
from forest.conversation_transaction_ports import (
    TransactionalConversationCompletionPort,
    TransactionalSessionDecisionPort,
)
from forest.final_reply_uow import FinalReplyUnitOfWork, RuntimeParticipant
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeKernelServiceSpec
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    configuration_management_contribution,
    official_artifact,
    official_storage_bundle,
)
from forest.session_handoff_uow import SessionHandoffUnitOfWork
from forest.storage.adapter_onebot_authority import OneBotAdapterRepository
from forest.storage.authorization_authority import CapabilityGrant, GrantSetRef
from forest.storage.conversation_memory_uow import ConversationMemoryInputUnitOfWork
from forest.storage.conversation_scheduler import ConversationSchedulerRepository
from forest.storage.ingress_unit_of_work import OneBotIngressUnitOfWork
from forest.storage.module_configuration import ModuleConfigurationRepository

_DEFINITIONS = {
    "conversation.scheduler": {
        "schema_id": "forest.conversation.scheduler",
        "repository_contract": "forest.conversation-scheduler.repository/1",
        "resource": "storage/official/conversation_scheduler.sql",
        "requires": (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "agent.runtime",
            "memory.graph",
        ),
        "capabilities": ("conversation.admit", "conversation.classify"),
    },
    "adapter.onebot": {
        "schema_id": "forest.adapter.onebot",
        "repository_contract": "forest.adapter-onebot.repository/1",
        "resource": "storage/official/adapter_onebot.sql",
        "requires": (
            "kernel.runtime",
            "kernel.payload",
            "kernel.authorization",
            "codec.onebot",
            "conversation.scheduler",
            "conversation.trigger-policy",
            "memory.graph",
            "media.capture",
            "shell.command",
        ),
        "capabilities": ("adapter.onebot.ingress", "adapter.onebot.send"),
    },
}


@dataclass(frozen=True)
class CleanMessagingModules:
    adapter: OneBotAdapterRepository
    conversation: ConversationSchedulerRepository
    descriptors: tuple[RuntimeModuleDescriptor, ...]
    configurations: dict[str, ModuleConfigurationRepository]
    grants: dict[str, GrantSetRef]


class _RoutedAgentExecution:
    def __init__(self, execute) -> None:
        self._execute = execute

    async def execute(self, request):
        return await self._execute(request)


class _RoutedCompletion:
    def __init__(self, commit) -> None:
        self._commit = commit

    async def commit(self, **values):
        return await self._commit(**values)


class _RoutedInputSelection:
    def __init__(self, select_input) -> None:
        self._select_input = select_input

    async def select_input(self, actor, operation_id: str, **values):
        return await self._select_input(actor, operation_id, **values)


class _RoutedSessionDecision:
    def __init__(self, decide) -> None:
        self._decide = decide

    async def decide(self, **values):
        return await self._decide(**values)


class _RoutedApprovalWait:
    def __init__(self, defer) -> None:
        self._defer = defer

    async def defer(self, actor, **values):
        return await self._defer(actor, **values)


class _RoutedIngress:
    def __init__(self, data_admit, control_admit, on_change=None) -> None:
        self._data_admit = data_admit
        self._control_admit = control_admit
        self._on_change = on_change

    async def admit(self, **values):
        handler = (
            self._control_admit if values.get("shell_command") is not None else self._data_admit
        )
        result = await handler(**values)
        if self._on_change is not None:
            self._on_change()
        return result


def _build_final_reply_commit(context):
    agent = context.participant("agent.runtime")
    conversation = context.participant("conversation.scheduler")
    delivery = context.participant("delivery")
    memory = context.participant("memory.graph")
    participants = (
        RuntimeParticipant("agent.runtime", agent.generation_identity.generation_id),
        RuntimeParticipant(
            "conversation.scheduler", conversation.generation_identity.generation_id
        ),
        RuntimeParticipant("delivery", delivery.generation_id),
        RuntimeParticipant("memory.graph", memory.generation_identity.generation_id),
    )
    port = TransactionalConversationCompletionPort(
        unit_of_work=FinalReplyUnitOfWork(
            store=context.store,
            agent_participant=agent,
            conversation_participant=conversation,
            delivery_participant=delivery,
            memory_participant=memory,
            generation_member_verifier=context.generation_member_verifier,
            participants=participants,
        ),
        plans=DurableFinalReplyPlanner(
            agent=agent,
            conversation=conversation,
            delivery=delivery,
        ),
    )
    return port.commit


def _build_session_handoff(context):
    agent = context.participant("agent.runtime")
    conversation = context.participant("conversation.scheduler")
    memory = context.participant("memory.graph")
    port = TransactionalSessionDecisionPort(
        unit_of_work=SessionHandoffUnitOfWork(
            store=context.store,
            agent_participant=agent,
            conversation_participant=conversation,
            memory_participant=memory,
            generation_member_verifier=context.generation_member_verifier,
            agent_generation_id=agent.generation_identity.generation_id,
            conversation_generation_id=conversation.generation_identity.generation_id,
            memory_generation_id=memory.generation_identity.generation_id,
        ),
        agent=agent,
    )
    return port.decide


def _build_input_selection(context):
    conversation = context.participant("conversation.scheduler")
    memory = context.participant("memory.graph")
    port = ConversationMemoryInputUnitOfWork(
        context.store,
        scheduler=conversation,
        memory=memory,
        deletion_epoch_guard=memory.deletion_epoch_in,
    )
    return port.select_input


def _build_approval_wait(context):
    port = ApprovalWaitUnitOfWork(
        context.store,
        authorization=context.participant("kernel.authorization").service,
        agent=context.participant("agent.runtime"),
        conversation=context.participant("conversation.scheduler"),
    )
    return port.defer


def _build_onebot_ingress(context):
    port = OneBotIngressUnitOfWork(
        context.store,
        adapter=context.participant("adapter.onebot"),
        conversation=context.participant("conversation.scheduler"),
        memory=context.participant("memory.graph"),
        media=context.participant("media.capture"),
        payloads=context.participant("kernel.payload").service,
        shell=context.participant("shell.command"),
        delivery=context.participant("delivery"),
    )
    return port.admit


async def bootstrap_clean_messaging(
    store,
    installer,
    *,
    kernel,
    secret_resolver,
    http_client_factory=None,
    websocket_client_factory=None,
    register_effect_fence: bool = True,
) -> CleanMessagingModules:
    """Create module-owned authorities and descriptor factories for one Bot."""

    bundles = {
        component_id: official_storage_bundle(
            component_id,
            definition["schema_id"],
            definition["repository_contract"],
            definition["resource"],
        )
        for component_id, definition in _DEFINITIONS.items()
    }
    for component_id in sorted(bundles):
        bundle = bundles[component_id]
        await installer.install(
            bundle.spec,
            bundle.baseline,
            bundle.package_digest,
            execution_profile=bundle.execution_profile,
        )

    configurations = {
        component_id: ModuleConfigurationRepository(
            store,
            component_id=component_id,
            namespace=bundle.spec.namespace,
            payloads=kernel.payload,
        )
        for component_id, bundle in bundles.items()
    }
    for component_id, configuration in configurations.items():
        schema = (
            CONVERSATION_CONFIGURATION_SCHEMA
            if component_id == "conversation.scheduler"
            else {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"protocol": {"type": "string"}},
                "required": ["protocol"],
                "additionalProperties": False,
            }
        )
        await configuration.ensure_initialized(
            operation_id=f"bootstrap:{component_id}:configuration",
            value=(
                default_conversation_configuration()
                if component_id == "conversation.scheduler"
                else {"protocol": f"forest.{component_id}/1"}
            ),
            validation_digest=digest_json(schema),
        )

    policy_revision = await kernel.authorization.revision()
    grants = {}
    for component_id, definition in _DEFINITIONS.items():
        grants[component_id] = await kernel.authorization.create_grant_set(
            grantee_kind="component",
            grantee_id=component_id,
            entries=tuple(
                CapabilityGrant(capability, "bot", store.bot_id)
                for capability in definition["capabilities"]
            ),
            expected_policy_revision=policy_revision,
        )

    conversation_repository = ConversationSchedulerRepository(
        store,
        namespace=bundles["conversation.scheduler"].spec.namespace,
        payload_verifier=kernel.payload.verify_in,
        payloads=kernel.payload,
    )
    adapter_repository = OneBotAdapterRepository(
        store,
        namespace=bundles["adapter.onebot"].spec.namespace,
        payload_verifier=kernel.payload.verify_in,
        generation_verifier=kernel.runtime.verify_active_generation_in,
        generation_member_verifier=kernel.runtime.verify_generation_member_in,
    )
    if register_effect_fence:
        kernel.effect.register_fence_verifier(
            "adapter.binding", adapter_repository.verify_effect_fence_in
        )

    def conversation_factory(_runtime, contracts):
        runtime = CleanConversationSchedulerRuntime(
            repository=conversation_repository,
            module_configuration=configurations["conversation.scheduler"],
            grants=grants["conversation.scheduler"],
            generation_verifier=kernel.runtime.verify_active_generation_in,
            generation_member_verifier=kernel.runtime.verify_generation_member_in,
            agent_execution=_RoutedAgentExecution(
                contracts.service("agent.execute", 1, owned_work=True)
            ),
            input_selection_port=_RoutedInputSelection(
                contracts.service("kernel.uow.conversation.select-input", 1, owned_work=True)
            ),
            completion_port=_RoutedCompletion(
                contracts.service("kernel.uow.conversation.final-reply", 1, owned_work=True)
            ),
            session_decision_port=_RoutedSessionDecision(
                contracts.service("kernel.uow.conversation.session-handoff", 1, owned_work=True)
            ),
            approval_wait_port=_RoutedApprovalWait(
                contracts.service("kernel.uow.conversation.defer-approval", 1, owned_work=True)
            ),
            graph_admission=contracts.graph_admission,
        )
        runtime.management_contract = CleanModuleConfigurationContract(
            configurations["conversation.scheduler"],
            schema=CONVERSATION_CONFIGURATION_SCHEMA,
            validator=CleanConversationSchedulerRuntime._parse_configuration,
        )
        return runtime

    def adapter_factory(_runtime, contracts):
        values = {
            "repository": adapter_repository,
            "module_configuration": configurations["adapter.onebot"],
            "grants": grants["adapter.onebot"],
            "payloads": kernel.payload,
            "secret_resolver": secret_resolver,
            "ingress_port": _RoutedIngress(
                contracts.service("kernel.uow.onebot.ingress", 1),
                contracts.service("kernel.uow.onebot.ingress", 1, admission_lane="CONTROL"),
                getattr(_runtime, "notify", None),
            ),
            "trigger_policy": contracts.service("conversation.trigger.classify", 1),
            "shell_inspector": contracts.service(
                "shell.ingress.inspect", 1, admission_lane="CONTROL"
            ),
            "normalize_codec": contracts.service(
                "onebot.codec.normalize", 1, owned_work=True
            ),
            "prepare_send_codec": contracts.service(
                "onebot.codec.prepare-send",
                1,
                admission_lane="CONTROL",
                owned_work=True,
            ),
        }
        if http_client_factory is not None:
            values["http_client_factory"] = http_client_factory
        if websocket_client_factory is not None:
            values["websocket_client_factory"] = websocket_client_factory
        runtime = CleanOneBotRuntime(**values)
        runtime.management_contract = CleanOneBotManagementContract(
            configurations["adapter.onebot"],
            repository=adapter_repository,
            payloads=kernel.payload,
            runtime=runtime,
        )
        return runtime

    artifact = official_artifact("bot")
    factories = {
        "conversation.scheduler": conversation_factory,
        "adapter.onebot": adapter_factory,
    }
    descriptors = tuple(
        RuntimeModuleDescriptor(
            component_id,
            definition["requires"],
            factories[component_id],
            artifact,
            management=(
                RuntimeManagementContribution(
                    "onebot-bindings",
                    "OneBot Binding",
                    ("bot",),
                    "/static/modules/clean-onebot.html",
                    "/static/clean-onebot.js",
                    style="/static/modules/clean-onebot.css",
                    group="transport",
                    order=5,
                ),
            )
            if component_id == "adapter.onebot"
            else (configuration_management_contribution("Context View"),)
            if component_id == "conversation.scheduler"
            else (),
            storage=bundles[component_id],
            wakes_on_work=component_id == "conversation.scheduler",
            work_notify_order=10 if component_id == "conversation.scheduler" else 100,
            kernel_services=(
                (
                    RuntimeKernelServiceSpec(
                        "kernel.uow.conversation.select-input",
                        1,
                        ("conversation.scheduler", "memory.graph"),
                        _build_input_selection,
                    ),
                    RuntimeKernelServiceSpec(
                        "kernel.uow.conversation.final-reply",
                        1,
                        (
                            "agent.runtime",
                            "conversation.scheduler",
                            "delivery",
                            "memory.graph",
                        ),
                        _build_final_reply_commit,
                    ),
                    RuntimeKernelServiceSpec(
                        "kernel.uow.conversation.session-handoff",
                        1,
                        ("agent.runtime", "conversation.scheduler", "memory.graph"),
                        _build_session_handoff,
                    ),
                    RuntimeKernelServiceSpec(
                        "kernel.uow.conversation.defer-approval",
                        1,
                        (
                            "kernel.authorization",
                            "agent.runtime",
                            "conversation.scheduler",
                        ),
                        _build_approval_wait,
                    ),
                )
                if component_id == "conversation.scheduler"
                else (
                    RuntimeKernelServiceSpec(
                        "kernel.uow.onebot.ingress",
                        1,
                        (
                            "adapter.onebot",
                            "conversation.scheduler",
                            "memory.graph",
                            "media.capture",
                            "kernel.payload",
                            "shell.command",
                            "delivery",
                        ),
                        _build_onebot_ingress,
                    ),
                )
                if component_id == "adapter.onebot"
                else ()
            ),
        )
        for component_id, definition in _DEFINITIONS.items()
    )
    return CleanMessagingModules(
        adapter_repository,
        conversation_repository,
        descriptors,
        configurations,
        grants,
    )
