"""Generation-bound runtime port for the single Agent state machine."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from forest.agent_execution import DurableSingleAgentExecution
from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.agent_runtime_authority import AgentPayloadRef

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


@dataclass(frozen=True)
class FrozenAgentProfile:
    agent_spec_revision: str
    config: AgentPayloadRef
    preset_revision: str
    preset: AgentPayloadRef
    preset_context: tuple[str, ...]
    run_timeout_seconds: int
    max_steps: int
    output_tokens: int
    stream: bool


@dataclass(frozen=True)
class PreparedAgentFinalization:
    owner: object
    checkpoint_id: str
    state: AgentPayloadRef


def _stable(kind: str, *parts: object) -> str:
    value = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{kind}:{hashlib.sha256(value).hexdigest()}"


def _utf8_size(value: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except UnicodeError:
        return -1


class CleanAgentRuntime:
    """Expose Agent authority operations only through the exact active Generation."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "agent.execute", "version": 1},
            {"service_id": "agent.run.start", "version": 1},
            {"service_id": "agent.run.advance", "version": 1},
            {"service_id": "agent.model.observe", "version": 1},
            {"service_id": "agent.tool.observe", "version": 1},
        ],
        "requires": [
            {"service_id": "provider.model.project", "version": 1},
            {"service_id": "provider.model.dispatch", "version": 1},
            {"service_id": "memory.recall", "version": 1},
            {"service_id": "media.context.resolve", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def runtime_service_handlers(self) -> dict:
        return {
            ("agent.execute", 1): self.execute,
            ("agent.run.start", 1): self.start_run,
            ("agent.run.advance", 1): self.transition,
            ("agent.model.observe", 1): self.observe_model,
            ("agent.tool.observe", 1): self.observe_tool,
        }

    def __init__(
        self,
        *,
        repository,
        module_configuration,
        grants,
        payloads,
        generation_verifier,
        generation_member_verifier=None,
        provider=None,
        memory=None,
        media=None,
        tool_route_factories=(),
        tool_requirements=(),
    ):
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.payloads = payloads
        self.provider = provider
        self.memory = memory
        self.media = media
        self.tool_route_factories = tuple(tool_route_factories)
        if any(not callable(factory) for factory in self.tool_route_factories):
            raise ValueError("Agent tool route factories must be callable")
        tool_requirements = tuple(tool_requirements)
        if any(
            not isinstance(item, tuple)
            or len(item) != 2
            or not isinstance(item[0], str)
            or type(item[1]) is not int
            or item[1] < 1
            for item in tool_requirements
        ) or len(tool_requirements) != len(set(tool_requirements)):
            raise ValueError("Agent tool service requirements are invalid")
        self.runtime_contract = {
            **type(self).runtime_contract,
            "services": [dict(item) for item in type(self).runtime_contract["services"]],
            "requires": [
                *(dict(item) for item in type(self).runtime_contract["requires"]),
                *(
                    {"service_id": service_id, "version": version}
                    for service_id, version in tool_requirements
                ),
            ],
        }
        self.generation_verifier = generation_verifier
        self.generation_member_verifier = generation_member_verifier
        self.generation_identity: BoundGenerationIdentity | None = None
        self.active = False
        self._profile: FrozenAgentProfile | None = None
        self.execution: DurableSingleAgentExecution | None = None

    @property
    def bot_id(self) -> str:
        return self.repository.store.bot_id

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        value = desired.value
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "protocol",
                "agent_spec_revision",
                "preset_revision",
                "preset",
                "run_timeout_seconds",
                "max_steps",
                "output_tokens",
                "stream",
            }
            or value.get("protocol") != "forest.agent.runtime/1"
        ):
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent configuration is invalid", 500)
        if (
            not isinstance(value.get("agent_spec_revision"), str)
            or not 1 <= _utf8_size(value["agent_spec_revision"]) <= 256
            or not isinstance(value.get("preset_revision"), str)
            or not 1 <= _utf8_size(value["preset_revision"]) <= 256
            or type(value.get("run_timeout_seconds")) is not int
            or not 1 <= value["run_timeout_seconds"] <= 86400
            or type(value.get("max_steps")) is not int
            or not 1 <= value["max_steps"] <= 256
            or type(value.get("output_tokens")) is not int
            or not 1 <= value["output_tokens"] <= 131072
            or type(value.get("stream")) is not bool
        ):
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent limits are invalid", 500)
        preset_value = value.get("preset")
        if not isinstance(preset_value, dict):
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent preset reference is invalid", 500)
        preset = AgentPayloadRef(
            preset_value.get("payload_id"),
            preset_value.get("digest"),
            "agent.runtime",
            "preset.context",
            preset_value.get("owner_id"),
        )
        preset.validate("preset")
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            reference = await self.payloads.verify_in(
                conn,
                payload_id=preset.payload_id,
                digest=preset.digest,
                owner_component_id=preset.owner_component_id,
                owner_kind=preset.owner_kind,
                owner_id=preset.owner_id,
            )
            body = await self.payloads.read_bytes_in(
                conn,
                preset.payload_id,
                expected_deletion_epoch=reference.accepted_deletion_epoch,
            )
        try:
            preset_document = json.loads(body)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent preset is invalid", 500) from exc
        messages = preset_document.get("system") if isinstance(preset_document, dict) else None
        if (
            not isinstance(messages, list)
            or len(messages) > 32
            or any(not isinstance(item, str) for item in messages)
            or any(_utf8_size(item) < 0 for item in messages)
            or sum(_utf8_size(item) for item in messages) > 32768
        ):
            raise ForestError("AGENT_CONFIGURATION_INVALID", "Agent preset is invalid", 500)
        self._profile = FrozenAgentProfile(
            agent_spec_revision=value["agent_spec_revision"],
            config=AgentPayloadRef(
                desired.payload_id,
                desired.payload_digest,
                "agent.runtime",
                "module.configuration",
                await self.module_configuration.owner_id(desired.revision),
            ),
            preset_revision=value["preset_revision"],
            preset=preset,
            preset_context=tuple(messages),
            run_timeout_seconds=value["run_timeout_seconds"],
            max_steps=value["max_steps"],
            output_tokens=value["output_tokens"],
            stream=value["stream"],
        )
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
        if self.provider is not None and self.memory is not None and self.media is not None:
            tool_routes = tuple(factory() for factory in self.tool_route_factories)
            self.execution = DurableSingleAgentExecution(
                agent=self,
                provider=self.provider,
                memory=self.memory,
                media=self.media,
                payloads=self.payloads,
                owner_id=("agent-execution/" + self.generation_identity.generation_id)[-256:],
                tool_routes=tool_routes,
            )

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False

    async def close(self) -> None:
        self.active = False
        self.execution = None

    async def execute(self, request):
        if not self.active or self.execution is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Agent execution has no Contract Router provider port",
                409,
            )
        return await self.execution.execute(request)

    async def prepare_finalization(
        self, *, run_id: str, reply_ref: str
    ) -> PreparedAgentFinalization:
        owner = await self.acquire_run(
            run_id,
            ("agent-finalize/" + self.generation_identity.generation_id)[-256:],
            lease_seconds=300,
        )
        try:
            run = await self.load(run_id)
            if run["state"] != "FINAL_READY":
                raise ForestError(
                    "AGENT_FINALIZATION_BOUNDARY_CLOSED",
                    "Agent Run is not ready for finalization",
                    409,
                )
            expected_reply_ref = _stable("reply", run_id)
            if reply_ref != expected_reply_ref:
                raise ForestError(
                    "AGENT_FINALIZATION_CONFLICT",
                    "Final reply reference differs from the Run",
                    409,
                )
            checkpoint_id = _stable("checkpoint-completed", run_id)
            epoch = await self.payloads.epoch()
            saved = await self.payloads.put_bytes(
                owner_component_id="agent.runtime",
                owner_kind="checkpoint.state",
                owner_id=checkpoint_id,
                media_type="application/json",
                content=json.dumps(
                    {"phase": "COMPLETED", "reply_ref": reply_ref},
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode(),
                expected_deletion_epoch=epoch,
            )
            return PreparedAgentFinalization(
                owner=owner,
                checkpoint_id=checkpoint_id,
                state=AgentPayloadRef(
                    saved.payload_id,
                    saved.content_digest,
                    "agent.runtime",
                    "checkpoint.state",
                    checkpoint_id,
                ),
            )
        except BaseException:
            await self.release_run(owner)
            raise

    async def release_finalization(self, prepared: PreparedAgentFinalization) -> None:
        if type(prepared) is not PreparedAgentFinalization:
            raise ForestError(
                "AGENT_FINALIZATION_INPUT_INVALID",
                "Agent finalization handle is invalid",
                422,
            )
        await self.release_run(prepared.owner)

    def profile(self) -> FrozenAgentProfile:
        if not self.active or self._profile is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Agent Generation has no active frozen profile",
                409,
            )
        return self._profile

    async def _verify_member_in(self, conn) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Agent Generation is not executing owned work",
                409,
            )
        if self.generation_member_verifier is not None:
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
                admission_lane="DATA",
            )

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

    async def mark_model_handoff_in(self, conn, owner, **values):
        await self._verify_member_in(conn)
        return await self.repository.mark_model_handoff(
            owner,
            _connection=conn,
            **values,
        )

    async def observe_model(self, owner, **values):
        return await self._call("observe_model", owner=owner, owned_work=True, **values)

    async def record_model_turn(self, owner, **values):
        return await self._call(
            "record_model_turn", owner=owner, owned_work=True, **values
        )

    async def observe_tool(self, owner, **values):
        return await self._call("observe_tool", owner=owner, owned_work=True, **values)

    async def observe_tool_in(self, conn, owner, **values):
        await self._verify_member_in(conn)
        return await self.repository.observe_tool(
            owner,
            _connection=conn,
            **values,
        )

    async def checkpoint(self, owner, **values):
        return await self._call("checkpoint", owner=owner, owned_work=True, **values)

    async def transition(self, owner, **values):
        return await self._call("transition", owner=owner, owned_work=True, **values)

    async def transition_in(self, conn, owner, **values):
        await self._verify_member_in(conn)
        return await self.repository.transition(owner, _connection=conn, **values)

    async def close_session(self, **values):
        return await self._call("close_session", owned_work=True, **values)

    async def start_successor_run_in(self, conn, **values):
        await self._verify_member_in(conn)
        return await self.repository.start_successor_run(_connection=conn, **values)

    async def close_session_in(self, conn, **values):
        await self._verify_member_in(conn)
        return await self.repository.close_session(_connection=conn, **values)

    async def load(self, run_id: str):
        return await self._call("load", run_id=run_id, owned_work=True)

    async def load_recovery(self, run_id: str):
        return await self._call("load_recovery", run_id=run_id, owned_work=True)

    async def execution_history(self, run_id: str):
        return await self._call("execution_history", run_id=run_id, owned_work=True)

    async def recoverable_runs(self, *, limit: int = 64):
        return await self._call(
            "recoverable_runs", limit=limit, owned_work=True
        )
