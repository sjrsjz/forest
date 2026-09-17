"""Generation-bound OpenAI-compatible Chat Completions Provider runtime."""

from __future__ import annotations

import inspect
from dataclasses import dataclass

from forest.errors import ForestError
from forest.models import (
    ChatCompletionsClient,
    MediaSnapshot,
    ModelCapabilities,
    ModelResult,
    PreparedModelRequest,
    prepare_model_request,
)
from forest.packages import digest_json
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


async def _call(port, *args, **values):
    result = port(*args, **values)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class BoundProviderRequest:
    component_id: str
    generation_id: str
    provider_profile_ref: str
    capability_digest: str
    capabilities: ModelCapabilities
    prepared: PreparedModelRequest


class CleanChatCompletionsProviderRuntime:
    """Own the HTTP client and per-request projection for one Provider Generation."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "provider.model.project", "version": 1},
            {"service_id": "provider.model.dispatch", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        store,
        module_configuration,
        grants,
        credential_resolver,
        generation_verifier,
        client_factory=ChatCompletionsClient,
    ) -> None:
        self.store = store
        self.module_configuration = module_configuration
        self.grants = grants
        self.credential_resolver = credential_resolver
        self.generation_verifier = generation_verifier
        self.client_factory = client_factory
        self.generation_identity: BoundGenerationIdentity | None = None
        self.configuration = None
        self.configuration_revision: int | None = None
        self.capabilities: ModelCapabilities | None = None
        self.capability_digest: str | None = None
        self.client = None
        self.active = False

    @staticmethod
    def _parse_configuration(value) -> tuple[dict, ModelCapabilities]:
        if not isinstance(value, dict) or set(value) != {
            "protocol",
            "base_url",
            "timeout_seconds",
            "max_connections",
            "capabilities",
        }:
            raise ForestError(
                "PROVIDER_CONFIGURATION_INVALID",
                "Provider configuration fields differ from its contract",
                422,
            )
        if value["protocol"] != "forest.provider.chat-completions/1":
            raise ForestError(
                "PROVIDER_CONFIGURATION_INVALID", "Provider protocol is unsupported", 422
            )
        capabilities = value["capabilities"]
        if not isinstance(capabilities, dict):
            raise ForestError(
                "PROVIDER_CONFIGURATION_INVALID", "Provider capabilities are invalid", 422
            )
        try:
            parsed = ModelCapabilities(**capabilities)
        except (ForestError, TypeError) as exc:
            raise ForestError(
                "PROVIDER_CONFIGURATION_INVALID", "Provider capabilities are invalid", 422
            ) from exc
        return value, parsed

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        configuration, capabilities = self._parse_configuration(desired.value)
        self.configuration = configuration
        self.configuration_revision = desired.revision
        self.capabilities = capabilities
        self.capability_digest = digest_json(capabilities.as_dict())
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
            instance_id=f"instance:provider.chat-completions:{context.activation_id}",
            worker_incarnation=f"worker:provider.chat-completions:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "provider.chat-completions":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Provider received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Provider Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if (
            self.generation_identity is None
            or self.configuration is None
            or self.configuration_revision is None
            or self.capabilities is None
        ):
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Provider Generation was not prepared and bound",
                503,
            )
        credential = await _call(
            self.credential_resolver,
            self.configuration_revision,
        )
        if not isinstance(credential, str):
            raise ForestError(
                "PROVIDER_CREDENTIAL_UNAVAILABLE", "Provider credential is unavailable", 503
            )
        self.client = self.client_factory(
            self.configuration["base_url"],
            credential,
            timeout_seconds=self.configuration["timeout_seconds"],
            max_connections=self.configuration["max_connections"],
            deployment_id=self.capabilities.deployment_id,
        )

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False
        if self.client is not None:
            await self.client.aclose()

    async def close(self) -> None:
        self.active = False
        if self.client is not None:
            await self.client.aclose()

    async def _verify_active(self, conn) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Provider Generation is not accepting work",
                409,
            )
        await self.generation_verifier(
            conn,
            component_id="provider.chat-completions",
            generation_id=self.generation_identity.generation_id,
            admission_lane="DATA",
        )

    async def prepare_request(
        self,
        snapshot: dict,
        *,
        request_id: str,
        transport_attempt_id: str,
        preset_context=(),
        tools=(),
        media: dict[str | tuple[str, str], MediaSnapshot] | None = None,
        output_tokens: int = 1024,
        stream: bool = True,
        evidence: dict | None = None,
    ) -> BoundProviderRequest:
        if self.capabilities is None or self.capability_digest is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Provider is not prepared", 409
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._verify_active(conn)
        prepared = prepare_model_request(
            snapshot,
            self.capabilities,
            request_id=request_id,
            transport_attempt_id=transport_attempt_id,
            preset_context=preset_context,
            tools=tools,
            media=media,
            output_tokens=output_tokens,
            stream=stream,
            evidence=evidence,
        )
        identity = self.generation_identity
        return BoundProviderRequest(
            component_id=identity.component_id,
            generation_id=identity.generation_id,
            provider_profile_ref=f"configuration:{self.configuration_revision}",
            capability_digest=self.capability_digest,
            capabilities=self.capabilities,
            prepared=prepared,
        )

    async def dispatch(self, request: BoundProviderRequest, *, permit_in) -> ModelResult:
        identity = self.generation_identity
        if (
            type(request) is not BoundProviderRequest
            or identity is None
            or request.component_id != identity.component_id
            or request.generation_id != identity.generation_id
            or request.capability_digest != self.capability_digest
            or self.client is None
        ):
            raise ForestError(
                "PROVIDER_REQUEST_FENCE_MISMATCH",
                "Prepared request belongs to another Provider Generation",
                409,
            )

        async def permitcheck(prepared):
            if prepared is not request.prepared:
                raise ForestError(
                    "PROVIDER_REQUEST_FENCE_MISMATCH", "Provider request identity changed", 409
                )
            async with self.store.pool.connection() as conn, conn.transaction():
                await self._verify_active(conn)
                await _call(permit_in, conn, request=request)

        return await self.client.dispatch(request.prepared, permitcheck=permitcheck)
