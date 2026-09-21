"""Generation-bound Conversation Scheduler ingress module."""

from __future__ import annotations

from forest.conversation_worker import ConversationWorker
from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.conversation_scheduler import ContextViewPolicy

CONVERSATION_CONFIGURATION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "protocol": {"const": "forest.conversation.scheduler/1"},
        "context_view": {
            "type": "object",
            "properties": {
                "high_watermark_units": {
                    "type": "integer",
                    "minimum": 2,
                    "maximum": 1_000_000,
                },
                "retain_units": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 999_999,
                },
                "policy_revision": {
                    "const": "context-view-unit-window-v1",
                },
            },
            "required": ["high_watermark_units", "retain_units", "policy_revision"],
            "additionalProperties": False,
        },
    },
    "required": ["protocol", "context_view"],
    "additionalProperties": False,
}


def default_conversation_configuration() -> dict:
    return {
        "protocol": "forest.conversation.scheduler/1",
        "context_view": {
            "high_watermark_units": 512,
            "retain_units": 256,
            "policy_revision": "context-view-unit-window-v1",
        },
    }

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class CleanConversationSchedulerRuntime:
    """Typed scheduler authority port fenced by its exact Runtime Generation."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "conversation.admit", "version": 1},
            {"service_id": "conversation.classify", "version": 1},
        ],
        "requires": [
            {"service_id": "agent.execute", "version": 1},
            {"service_id": "kernel.uow.conversation.select-input", "version": 1},
            {"service_id": "kernel.uow.conversation.final-reply", "version": 1},
            {"service_id": "kernel.uow.conversation.session-handoff", "version": 1},
            {"service_id": "kernel.uow.conversation.defer-approval", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        module_configuration,
        grants,
        generation_verifier,
        generation_member_verifier=None,
        agent_execution=None,
        input_selection_port=None,
        completion_port=None,
        session_decision_port=None,
        approval_wait_port=None,
        graph_admission=None,
    ):
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_member_verifier = generation_member_verifier
        self.agent_execution = agent_execution
        self.input_selection_port = input_selection_port
        self.completion_port = completion_port
        self.session_decision_port = session_decision_port
        self.approval_wait_port = approval_wait_port
        self.graph_admission = graph_admission
        self.generation_identity: BoundGenerationIdentity | None = None
        self.active = False
        self.worker: ConversationWorker | None = None
        self.context_view_policy: ContextViewPolicy | None = None

    @staticmethod
    def _parse_configuration(value: object) -> ContextViewPolicy:
        if not isinstance(value, dict) or value.get("protocol") != "forest.conversation.scheduler/1":
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation configuration protocol is invalid",
                422,
            )
        view = value.get("context_view")
        if not isinstance(view, dict) or set(view) != {
            "high_watermark_units",
            "retain_units",
            "policy_revision",
        }:
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation Context view configuration is invalid",
                422,
            )
        policy = ContextViewPolicy(
            high_watermark_units=view["high_watermark_units"],
            retain_units=view["retain_units"],
            policy_revision=view["policy_revision"],
        )
        try:
            policy.validate()
        except ForestError as exc:
            raise ForestError(
                "CONVERSATION_CONFIGURATION_INVALID",
                "Conversation Context view configuration is invalid",
                422,
            ) from exc
        return policy

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        self.context_view_policy = self._parse_configuration(desired.value)
        return PreparedGenerationBinding(
            component_id=context.component_id,
            requires=context.requires,
            package_id=context.package_id,
            package_version=context.package_version,
            package_digest=context.package_digest,
            driver=context.driver,
            trust_level=context.trust_level,
            descriptor_digest=context.descriptor_digest,
            route_contract_digest=context.route_contract_digest,
            management_contract_digest=context.management_contract_digest,
            management_ui_digest=context.management_ui_digest,
            config_revision=desired.revision,
            config_payload_id=desired.payload_id,
            config_payload_digest=desired.payload_digest,
            instance_id=f"instance:conversation.scheduler:{context.activation_id}",
            worker_incarnation=f"worker:conversation.scheduler:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "conversation.scheduler":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Conversation received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Conversation Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Conversation Generation identity was not bound",
                503,
            )
        if any(
            item is None
            for item in (
                self.agent_execution,
                self.input_selection_port,
                self.completion_port,
                self.session_decision_port,
                self.approval_wait_port,
                self.graph_admission,
            )
        ):
            return
        self.worker = ConversationWorker(
            self,
            self.agent_execution,
            self.completion_port,
            self.session_decision_port,
            approval_wait=self.approval_wait_port,
            owner_id=(
                "conversation-worker/" + self.generation_identity.generation_id
            )[-256:],
            graph_admission=self.graph_admission,
        )
        self.worker.start()

    def activate_generation(self) -> None:
        self.active = True
        if self.worker is not None:
            self.worker.activate()

    async def quiesce(self) -> None:
        if self.worker is not None:
            await self.worker.quiesce()
        self.active = False

    async def close(self) -> None:
        if self.worker is not None:
            await self.worker.close()
        self.active = False

    def owned_tasks(self) -> tuple:
        return self.worker.owned_tasks() if self.worker is not None else ()

    def notify(self) -> None:
        if self.worker is not None:
            self.worker.wake()

    def begin_replacement(self, operation_id: str) -> None:
        if self.worker is not None:
            self.worker.begin_replacement(operation_id)

    async def _verify(self, conn, *, admission_lane: str) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Conversation Generation is not accepting work",
                409,
            )
        await self.generation_verifier(
            conn,
            component_id="conversation.scheduler",
            generation_id=self.generation_identity.generation_id,
            admission_lane=admission_lane,
        )

    async def _verify_member(self, conn) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Conversation Generation is not executing owned work",
                409,
            )
        if self.generation_member_verifier is None:
            await self._verify(conn, admission_lane="DATA")
            return
        await self.generation_member_verifier(
            conn,
            component_id="conversation.scheduler",
            generation_id=self.generation_identity.generation_id,
        )

    async def admit_message_in(self, conn, **values):
        admission_lane = values.pop("admission_lane", "DATA")
        await self._verify(conn, admission_lane=admission_lane)
        return await self.repository.admit_message_in(conn, **values)

    async def complete_classification_in(self, conn, **values):
        admission_lane = values.pop("admission_lane", "DATA")
        await self._verify(conn, admission_lane=admission_lane)
        return await self.repository.complete_classification_in(conn, **values)

    async def _repository_call(
        self,
        method_name: str,
        *,
        admission_lane: str = "DATA",
        owned_work: bool = False,
        **values,
    ):
        async with self.repository.store.pool.connection() as conn, conn.transaction():
            if owned_work:
                await self._verify_member(conn)
            else:
                await self._verify(conn, admission_lane=admission_lane)
            method = getattr(self.repository, method_name)
            return await method(conn, **values)

    async def claimable_conversations(self, *, limit: int = 64):
        return await self._repository_call(
            "claimable_conversations_in",
            limit=limit,
        )

    async def acquire_actor(self, conversation_id: str, owner_id: str, **values):
        return await self._repository_call(
            "acquire_actor_in",
            conversation_id=conversation_id,
            owner_id=owner_id,
            **values,
        )

    async def renew_actor(self, actor, **values):
        return await self._repository_call(
            "renew_actor_in",
            actor=actor,
            owned_work=True,
            **values,
        )

    async def release_actor(self, actor):
        return await self._repository_call(
            "release_actor_in", actor=actor, owned_work=True
        )

    async def defer_actor(self, actor, **values):
        return await self._repository_call(
            "defer_actor_in",
            actor=actor,
            owned_work=True,
            **values,
        )

    async def resume_deferred(self, conversation_id: str, **values):
        result = await self._repository_call(
            "resume_deferred_in",
            conversation_id=conversation_id,
            admission_lane="CONTROL",
            **values,
        )
        if result["resumed"]:
            self.notify()
        return result

    async def wake_cancel_request(self, conversation_id: str, **values):
        result = await self._repository_call(
            "wake_cancel_request",
            conversation_id=conversation_id,
            admission_lane="CONTROL",
            **values,
        )
        if result["resumed"]:
            self.notify()
        return result

    async def select_input(self, actor, operation_id: str, **values):
        if self.input_selection_port is None:
            raise ForestError(
                "CONVERSATION_INPUT_PORT_MISSING",
                "Conversation input selection coordinator is unavailable",
                503,
            )
        return await self.input_selection_port.select_input(
            actor,
            operation_id,
            **values,
        )

    async def execution_state(self, actor):
        return await self._repository_call(
            "execution_state_in",
            actor=actor,
            owned_work=True,
        )

    async def append_context(self, actor, **values):
        return await self._repository_call(
            "append_context_in",
            actor=actor,
            owned_work=True,
            **values,
        )

    async def append_context_in(self, conn, actor, **values):
        await self._verify_member(conn)
        return await self.repository.append_context_in(conn, actor, **values)

    async def select_input_in(self, conn, actor, operation_id: str, **values):
        await self._verify_member(conn)
        if self.context_view_policy is None:
            raise ForestError(
                "CONVERSATION_CONFIGURATION_UNAVAILABLE",
                "Conversation Context view policy is unavailable",
                503,
            )
        return await self.repository.select_input_in(
            conn,
            actor,
            operation_id,
            context_view_policy=self.context_view_policy,
            **values,
        )

    async def verify_input_batch_occurrence_in(self, conn, **values):
        await self._verify_member(conn)
        return await self.repository.verify_input_batch_occurrence_in(conn, **values)

    async def verify_context_archive_occurrence_in(self, conn, **values):
        await self._verify_member(conn)
        return await self.repository.verify_context_archive_occurrence_in(conn, **values)

    async def answered_trigger_events_in(self, conn, **values):
        await self._verify_member(conn)
        return await self.repository.answered_trigger_events_in(conn, **values)

    async def requeue_run(self, actor, **values):
        return await self._repository_call(
            "requeue_run_in",
            actor=actor,
            owned_work=True,
            **values,
        )

    async def fail_run(self, actor, **values):
        return await self._repository_call(
            "fail_run_in",
            actor=actor,
            owned_work=True,
            **values,
        )

    async def cancel_run(self, actor, **values):
        return await self._repository_call(
            "cancel_run_in",
            actor=actor,
            owned_work=True,
            **values,
        )

    async def context_snapshot(self, actor):
        return await self._repository_call(
            "context_snapshot_in",
            actor=actor,
            owned_work=True,
        )
