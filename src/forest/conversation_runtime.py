"""Generation-bound Conversation Scheduler ingress module."""

from __future__ import annotations

from forest.conversation_worker import ConversationWorker
from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

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
        agent_execution_port=None,
        worker_options=None,
    ):
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_identity: BoundGenerationIdentity | None = None
        self.active = False
        self.agent_execution_port = agent_execution_port
        self.worker_options = dict(worker_options or {})
        self.worker: ConversationWorker | None = None

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired()
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
        if self.agent_execution_port is not None:
            self.worker = ConversationWorker(
                self,
                self.agent_execution_port,
                owner_id=(
                    "conversation.scheduler/"
                    + self.generation_identity.generation_id.split(":")[-1]
                )[:256],
                **self.worker_options,
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

    async def admit_message_in(self, conn, **values):
        await self._verify(conn, admission_lane="DATA")
        result = await self.repository.admit_message_in(conn, **values)
        if self.worker is not None:
            self.worker.wake()
        return result

    async def complete_classification_in(self, conn, **values):
        await self._verify(conn, admission_lane="DATA")
        result = await self.repository.complete_classification_in(conn, **values)
        if self.worker is not None:
            self.worker.wake()
        return result

    async def _repository_call(self, method_name: str, *, admission_lane: str = "DATA", **values):
        async with self.repository.store.pool.connection() as conn, conn.transaction():
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
            **values,
        )

    async def release_actor(self, actor):
        return await self._repository_call("release_actor_in", actor=actor)

    async def select_input(self, actor, operation_id: str, **values):
        return await self._repository_call(
            "select_input_in",
            actor=actor,
            operation_id=operation_id,
            **values,
        )

    async def append_context(self, actor, **values):
        return await self._repository_call(
            "append_context_in",
            actor=actor,
            **values,
        )

    async def requeue_run(self, actor, **values):
        return await self._repository_call(
            "requeue_run_in",
            actor=actor,
            **values,
        )

    async def context_snapshot(self, actor):
        return await self._repository_call(
            "context_snapshot_in",
            actor=actor,
        )
