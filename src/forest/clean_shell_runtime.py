"""Lifecycle-owned group Shell admission and priority control worker."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass

from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.shell_command_parser import ShellCommandParser, direct_shell_text, ingress_command
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)
from forest.shell_gate import CoreShellGate
from forest.storage.delivery_authority import DeliveryPart, DeliveryPayloadRef
from forest.storage.payload_authority import PayloadSource
from forest.storage.shell_command_authority import (
    ShellCommandRoute,
    ShellPayloadRef,
    ShellPolicyGrant,
)

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}
def _stable(kind: str, *parts: object) -> str:
    body = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"shell:{kind}:{hashlib.sha256(body).hexdigest()}"


@dataclass(frozen=True)
class FrozenShellProfile:
    binding_ref: str
    administrator_principals: tuple[str, ...]


class CleanShellRuntime:
    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [{"service_id": "shell.ingress.inspect", "version": 1}],
        "requires": [
            {"service_id": "kernel.uow.shell.result", "version": 1},
            {"service_id": "kernel.uow.shell.binding-fence", "version": 1},
            {"service_id": "kernel.uow.shell.route-fence", "version": 1},
            {"service_id": "kernel.uow.shell.authorization.consume", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        payloads,
        gate: CoreShellGate,
        parser: ShellCommandParser,
        authorization,
        module_configuration,
        grants,
        command_routes,
        result_commit,
        generation_admission_verifier,
        generation_member_verifier,
    ) -> None:
        self.repository = repository
        self.payloads = payloads
        self.gate = gate
        self.parser = parser
        self.authorization = authorization
        self.module_configuration = module_configuration
        self.grants = grants
        command_routes = tuple(command_routes)
        self.command_routes = {
            (contribution.command_id, contribution.command_version): (
                owner_component_id,
                contribution,
                service,
            )
            for owner_component_id, contribution, service in command_routes
        }
        if len(self.command_routes) != len(command_routes):
            raise ValueError("Shell command route identities repeat")
        command_requirements = tuple(
            dict.fromkeys(
                (contribution.service_id, contribution.service_version)
                for _, contribution, _ in self.command_routes.values()
            )
        )
        self.runtime_contract = {
            **type(self).runtime_contract,
            "services": [dict(item) for item in type(self).runtime_contract["services"]],
            "requires": [
                *(dict(item) for item in type(self).runtime_contract["requires"]),
                *(
                    {"service_id": service_id, "version": version}
                    for service_id, version in command_requirements
                ),
            ],
        }
        self.result_commit = result_commit
        self.generation_admission_verifier = generation_admission_verifier
        self.generation_member_verifier = generation_member_verifier
        self.generation_identity: BoundGenerationIdentity | None = None
        self.profile: FrozenShellProfile | None = None
        self.active = False
        self._closing = False
        self._admission = asyncio.Event()
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._task: asyncio.Task | None = None
        self._owner_id: str | None = None
        self._sequence = 0
        self.failure: str | None = None

    def runtime_service_handlers(self) -> dict:
        return {("shell.ingress.inspect", 1): self.inspect_ingress}

    @staticmethod
    def _parse_profile(value: object) -> FrozenShellProfile:
        if (
            not isinstance(value, dict)
            or set(value) != {"protocol", "binding_ref", "administrator_principals"}
            or value.get("protocol") != "forest.shell.command/1"
            or not isinstance(value.get("binding_ref"), str)
            or not value["binding_ref"]
            or len(value["binding_ref"].encode("utf-8")) > 256
            or not isinstance(value.get("administrator_principals"), list)
            or len(value["administrator_principals"]) > 4096
            or any(
                not isinstance(item, str)
                or not item.startswith("qq:")
                or not item[3:].isdecimal()
                or len(item.encode("utf-8")) > 256
                for item in value["administrator_principals"]
            )
            or len(value["administrator_principals"])
            != len(set(value["administrator_principals"]))
        ):
            raise ForestError(
                "SHELL_CONFIGURATION_INVALID", "Shell configuration is invalid", 422
            )
        return FrozenShellProfile(
            value["binding_ref"], tuple(sorted(value["administrator_principals"]))
        )

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        self.profile = self._parse_profile(desired.value)
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
            instance_id=f"instance:shell.command:{context.activation_id}",
            worker_incarnation=f"worker:shell.command:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "shell.command":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH", "Shell binding differs", 409
            )
        self.generation_identity = identity
        self._owner_id = ("shell-command/" + identity.generation_id)[-256:]

    async def start(self) -> None:
        if self.generation_identity is None or self.profile is None or self._owner_id is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE", "Shell is not prepared", 503
            )
        self._task = asyncio.create_task(
            self._run(), name="shell-command/" + self.generation_identity.generation_id
        )

    async def publish_generation_in(self, conn, *, generation_id: str, graph_id: str) -> None:
        identity = self.generation_identity
        profile = self.profile
        if (
            identity is None
            or profile is None
            or identity.generation_id != generation_id
            or identity.graph_id != graph_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH", "Shell publication differs", 409
            )
        policy_revision = await self.repository.policy_revision_in(conn, profile.binding_ref)
        await self.repository.publish_policy(
            operation_id="shell-policy:" + identity.activation_id,
            binding_ref=profile.binding_ref,
            expected_revision=policy_revision,
            actor_principal_ref="bootstrap/local",
            grants=tuple(
                ShellPolicyGrant("BINDING", principal, True)
                for principal in profile.administrator_principals
            ),
            _connection=conn,
        )
        registry_revision = await self.repository.registry_revision_in(conn)
        routes = []
        for owner_component_id, contribution, service in self.command_routes.values():
            route = service.route
            if route.provider_component_id != owner_component_id:
                raise ForestError(
                    "SHELL_ROUTE_INVALID",
                    "Shell command resolved outside its contributor Generation",
                    503,
                )
            routes.append(
                ShellCommandRoute(
                    command_id=contribution.command_id,
                    command_version=contribution.command_version,
                    owner_component_id=route.provider_component_id,
                    owner_generation_id=route.provider_generation_id,
                    argument_schema_digest=contribution.argument_schema_digest,
                    management_operation_id=contribution.management_operation_id,
                    target_scope=contribution.target_scope,
                    service_class=contribution.service_class,
                    required_capabilities_digest=(
                        contribution.required_capabilities_digest
                    ),
                    output_policy=contribution.output_policy,
                    timeout_seconds=contribution.timeout_seconds,
                )
            )
        await self.repository.publish_registry(
            operation_id="shell-registry:" + identity.activation_id,
            expected_revision=registry_revision,
            graph_digest=identity.graph_digest,
            routes=tuple(routes),
            _connection=conn,
        )

    def activate_generation(self) -> None:
        self.active = True
        self._admission.set()
        self._wake.set()

    def notify(self) -> None:
        self._wake.set()

    async def quiesce(self) -> None:
        self.active = False
        self._admission.clear()
        self._wake.set()
        await self._idle.wait()

    async def close(self) -> None:
        self.active = False
        self._closing = True
        self._admission.set()
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def owned_tasks(self) -> tuple:
        return (self._task,) if self._task is not None else ()

    async def inspect_ingress(
        self,
        *,
        normalized: dict,
        binding_ref: str,
        binding_epoch: int,
        connection_epoch: int,
        client_incarnation: str,
        adapter_generation_id: str,
        executor_fence_digest: str,
        conversation_ref: str,
        event_ref: str,
        principal_ref: str,
    ):
        if not self.active:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Shell is not active", 409)
        text = direct_shell_text(normalized)
        if text is None:
            return None
        match = self.gate.inspect(
            text=text,
            binding_ref=binding_ref,
            binding_epoch=binding_epoch,
            connection_epoch=connection_epoch,
            client_incarnation=client_incarnation,
            adapter_generation_id=adapter_generation_id,
            executor_fence_digest=executor_fence_digest,
            conversation_ref=conversation_ref,
            event_ref=event_ref,
            principal_ref=principal_ref,
        )
        if match is None:
            return None
        parsed, arguments, rejection = await self.parser.persist(
            command_text=match.command_text,
            event_ref=event_ref,
        )
        return ingress_command(match, parsed, arguments, rejection)

    async def admit_command_in(self, conn, **values):
        if not self.active or self.generation_identity is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Shell is not active", 409)
        await self.generation_admission_verifier(
            conn,
            component_id="shell.command",
            generation_id=self.generation_identity.generation_id,
            admission_lane="CONTROL",
        )
        return await self.repository.admit_command_in(conn, **values)

    async def complete_in(self, conn, claim, **values):
        if not self.active or self.generation_identity is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Shell is not active", 409)
        await self.generation_member_verifier(
            conn,
            component_id="shell.command",
            generation_id=self.generation_identity.generation_id,
        )
        return await self.repository.complete_in(conn, claim, **values)

    async def fail_claim_in(self, conn, claim, **values):
        if not self.active or self.generation_identity is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Shell is not active", 409)
        await self.generation_member_verifier(
            conn,
            component_id="shell.command",
            generation_id=self.generation_identity.generation_id,
        )
        return await self.repository.fail_claim_in(conn, claim, **values)

    async def _wait(self) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=0.25)
        except TimeoutError:
            pass
        self._wake.clear()

    async def _run(self) -> None:
        try:
            while not self._closing:
                await self._admission.wait()
                if self._closing:
                    return
                self._idle.clear()
                claim = None
                try:
                    self._sequence += 1
                    claim = await self.repository.claim(
                        operation_id=_stable("claim", self._owner_id, self._sequence),
                        worker_id=self._owner_id,
                    )
                    if claim is not None:
                        try:
                            await self._execute(claim)
                        except asyncio.CancelledError:
                            raise
                        except ForestError as exc:
                            await self._terminal_failure(claim, exc.code, unknown=False)
                        except Exception:
                            await self._terminal_failure(
                                claim, "SHELL_EXECUTION_UNKNOWN", unknown=True
                            )
                finally:
                    self._idle.set()
                if claim is None or not self._admission.is_set():
                    await self._wait()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            raise
        except Exception:
            self.failure = "SHELL_WORKER_FAILED"
            raise

    async def _payload(self, claim, text: str) -> tuple[ShellPayloadRef, DeliveryPayloadRef]:
        owner_id = _stable("result", claim.request_id)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="shell.command",
            owner_kind="shell.result",
            owner_id=owner_id,
            media_type="application/json",
            content=json.dumps(
                {"text": text}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"),
            sources=(
                PayloadSource(
                    "shell.command",
                    "shell.request",
                    claim.request_id,
                    1,
                    observation_conversation_id=claim.conversation_ref,
                    observation_binding_id=claim.binding_ref,
                    author_subject_id=claim.principal_ref,
                ),
            ),
            parents=(claim.arguments.payload_id,),
            expected_deletion_epoch=epoch,
        )
        shell = ShellPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "shell.command",
            "shell.result",
            owner_id,
        )
        delivery = DeliveryPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "shell.command",
            "shell.result",
            owner_id,
        )
        return shell, delivery

    async def _terminal_failure(self, claim, error_code: str, *, unknown: bool) -> None:
        text = "管理命令执行状态未知。" if unknown else "管理命令执行失败。"
        shell_result, delivery_result = await self._payload(claim, text)
        channel_kind, channel_ref = self._channel(claim.conversation_ref)
        part_operation = _stable("failure-delivery-part", claim.request_id)
        await self.result_commit(
            claim,
            result_operation_id=_stable("failure", claim.request_id, claim.owner_epoch),
            resulting_state="UNKNOWN" if unknown else "FAILED",
            result=shell_result,
            error_code=error_code,
            intent_operation_id=_stable("failure-delivery-create", claim.request_id),
            intent_id=_stable("failure-delivery-intent", claim.request_id),
            binding_ref=claim.binding_ref,
            channel_kind=channel_kind,
            channel_ref=channel_ref,
            parts=(
                DeliveryPart(
                    part_operation,
                    _stable("failure-delivery-effect", part_operation),
                    delivery_result,
                ),
            ),
            terminal_without_authorization=True,
        )

    @staticmethod
    def _channel(conversation_ref: str) -> tuple[str, str]:
        for prefix, kind in (("onebot:group:", "GROUP"), ("onebot:private:", "PRIVATE")):
            if conversation_ref.startswith(prefix) and conversation_ref[len(prefix) :]:
                return kind, conversation_ref[len(prefix) :]
        raise ForestError("SHELL_RESULT_ROUTE_INVALID", "Shell conversation route is invalid", 409)

    async def _arguments(self, claim) -> dict:
        try:
            body = json.loads((await self.payloads.read_bytes(claim.arguments.payload_id)))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "SHELL_ARGUMENTS_INVALID", "Shell arguments are invalid", 422
            ) from exc
        arguments = body.get("arguments") if isinstance(body, dict) else None
        if (
            not isinstance(arguments, dict)
            or body.get("protocol") != "forest.shell.arguments/1"
            or body.get("command_id") != claim.command_id
        ):
            raise ForestError("SHELL_ARGUMENTS_INVALID", "Shell arguments are invalid", 422)
        return arguments

    async def _execute(self, claim) -> None:
        invoke_operation = _stable("authorize-invoke", claim.request_id, claim.owner_epoch)
        receipt = await self.authorization.issue(
            claim,
            phase="invoke",
            operation_id=invoke_operation,
        )
        await self.repository.authorize_claim(
            claim,
            ticket_id=receipt.ticket_id,
            authorization_operation_id=receipt.operation_id,
        )
        await self.repository.start(claim)
        route = self.command_routes.get((claim.command_id, claim.command_version))
        if route is None:
            raise ForestError("SHELL_ROUTE_UNSUPPORTED", "Shell route is unsupported", 409)
        owner_component_id, _contribution, service = route
        frozen = service.route
        if (
            frozen.provider_component_id != owner_component_id
            or claim.owner_component_id != frozen.provider_component_id
            or claim.owner_generation_id != frozen.provider_generation_id
        ):
            raise ForestError("SHELL_ROUTE_STALE", "Shell command route is stale", 409)
        result = await service(
            ShellCommandExecutionRequest(
                claim.command_id,
                claim.command_version,
                await self._arguments(claim),
                claim.request_id,
                claim.principal_ref,
                claim.conversation_ref,
            )
        )
        if type(result) is not ShellCommandExecutionResult:
            raise ForestError(
                "SHELL_EXECUTION_RESULT_INVALID",
                "Shell command service returned an invalid result",
                502,
            )
        shell_result, delivery_result = await self._payload(claim, result.text)
        final_operation = _stable("authorize-result", claim.request_id, claim.owner_epoch)
        final_receipt = await self.authorization.issue(
            claim,
            phase="result",
            operation_id=final_operation,
        )
        channel_kind, channel_ref = self._channel(claim.conversation_ref)
        part_operation = _stable("delivery-part", claim.request_id)
        await self.result_commit(
            claim,
            result_operation_id=_stable("complete", claim.request_id),
            resulting_state="SUCCEEDED",
            result=shell_result,
            intent_operation_id=_stable("delivery-create", claim.request_id),
            intent_id=_stable("delivery-intent", claim.request_id),
            binding_ref=claim.binding_ref,
            channel_kind=channel_kind,
            channel_ref=channel_ref,
            parts=(
                DeliveryPart(
                    part_operation,
                    _stable("delivery-effect", part_operation),
                    delivery_result,
                ),
            ),
            authorization_ticket_id=final_receipt.ticket_id,
            authorization_operation_id=final_receipt.operation_id,
        )
