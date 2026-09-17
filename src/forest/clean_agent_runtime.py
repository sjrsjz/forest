"""Generation-bound runtime port for the single Agent state machine."""

from __future__ import annotations

from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class CleanAgentRuntime:
    """Expose Agent authority operations only through the exact active Generation."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "agent.run.start", "version": 1},
            {"service_id": "agent.run.advance", "version": 1},
            {"service_id": "agent.model.observe", "version": 1},
            {"service_id": "agent.tool.observe", "version": 1},
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
    ):
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_member_verifier = generation_member_verifier
        self.generation_identity: BoundGenerationIdentity | None = None
        self.active = False

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
            instance_id=f"instance:agent.runtime:{context.activation_id}",
            worker_incarnation=f"worker:agent.runtime:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "agent.runtime":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Agent received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Agent Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Agent Generation identity was not bound",
                503,
            )

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False

    async def close(self) -> None:
        self.active = False

    async def _call(
        self,
        method_name: str,
        *,
        admission_lane: str = "DATA",
        owned_work: bool = False,
        **values,
    ):
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Agent Generation is not accepting work",
                409,
            )
        async with self.repository.store.pool.connection() as conn, conn.transaction():
            if owned_work and self.generation_member_verifier is not None:
                await self.generation_member_verifier(
                    conn,
                    component_id="agent.runtime",
                    generation_id=self.generation_identity.generation_id,
                )
            else:
                await self.generation_verifier(
                    conn,
                    component_id="agent.runtime",
                    generation_id=self.generation_identity.generation_id,
                    admission_lane=admission_lane,
                )
            method = getattr(self.repository, method_name)
            return await method(_connection=conn, **values)

    async def start_run(self, **values):
        return await self._call("start_run", **values)

    async def start_successor_run(self, **values):
        return await self._call("start_successor_run", owned_work=True, **values)

    async def acquire_run(self, run_id: str, owner_id: str, **values):
        return await self._call(
            "acquire_run", run_id=run_id, owner_id=owner_id, **values
        )

    async def renew_run(self, owner, **values):
        return await self._call("renew_run", owner=owner, owned_work=True, **values)

    async def release_run(self, owner):
        return await self._call("release_run", owner=owner, owned_work=True)

    async def append_input(self, owner, input_batch):
        return await self._call(
            "append_input", owner=owner, input_batch=input_batch, owned_work=True
        )

    async def prepare_model_request(self, owner, plan):
        return await self._call(
            "prepare_model_request", owner=owner, plan=plan, owned_work=True
        )

    async def prepare_model_attempt(self, owner, **values):
        return await self._call(
            "prepare_model_attempt", owner=owner, owned_work=True, **values
        )

    async def mark_model_handoff(self, owner, **values):
        return await self._call(
            "mark_model_handoff", owner=owner, owned_work=True, **values
        )

    async def observe_model(self, owner, **values):
        return await self._call("observe_model", owner=owner, owned_work=True, **values)

    async def record_model_turn(self, owner, **values):
        return await self._call(
            "record_model_turn", owner=owner, owned_work=True, **values
        )

    async def observe_tool(self, owner, **values):
        return await self._call("observe_tool", owner=owner, owned_work=True, **values)

    async def checkpoint(self, owner, **values):
        return await self._call("checkpoint", owner=owner, owned_work=True, **values)

    async def transition(self, owner, **values):
        return await self._call("transition", owner=owner, owned_work=True, **values)

    async def close_session(self, **values):
        return await self._call("close_session", owned_work=True, **values)

    async def load(self, run_id: str):
        return await self._call("load", run_id=run_id, owned_work=True)
