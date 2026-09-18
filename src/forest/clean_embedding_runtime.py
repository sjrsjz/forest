"""Generation-bound multimodal embedding Provider."""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass

from forest.embeddings import (
    EmbeddingCancelled,
    EmbeddingClient,
    EmbeddingInput,
    EmbeddingProfile,
    EmbeddingResult,
    prepare_embedding_request,
)
from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.payload_authority import PayloadSource

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
class BoundEmbeddingRequest:
    component_id: str
    generation_id: str
    configuration_revision: int
    prepared: object


class CleanMultimodalEmbeddingRuntime:
    """Own request projection, credentials, HTTP effects and transport receipts."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [{"service_id": "embedding.encode", "version": 1}],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        store,
        repository,
        payloads,
        module_configuration,
        grants,
        credential_resolver,
        generation_member_verifier,
        client_factory=EmbeddingClient,
    ) -> None:
        self.store = store
        self.repository = repository
        self.payloads = payloads
        self.module_configuration = module_configuration
        self.grants = grants
        self.credential_resolver = credential_resolver
        self.generation_member_verifier = generation_member_verifier
        self.client_factory = client_factory
        self.generation_identity: BoundGenerationIdentity | None = None
        self.configuration: dict | None = None
        self.configuration_revision: int | None = None
        self.profile: EmbeddingProfile | None = None
        self.client = None
        self.active = False

    def runtime_service_handlers(self) -> dict:
        return {("embedding.encode", 1): self.encode}

    @staticmethod
    def _parse_configuration(value) -> tuple[dict, EmbeddingProfile]:
        expected = {
            "protocol",
            "endpoint",
            "credential_ref",
            "timeout_seconds",
            "max_connections",
            "profile",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID",
                "Embedding Provider configuration fields differ from its contract",
                422,
            )
        if value["protocol"] != "forest.embedding.multimodal/1":
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID", "Embedding protocol is unsupported", 422
            )
        profile = value["profile"]
        if not isinstance(profile, dict) or set(profile) != {
            "deployment_id",
            "model_revision",
            "model",
            "dimensions",
        }:
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID", "Embedding profile is invalid", 422
            )
        try:
            parsed = EmbeddingProfile(**profile)
        except (ForestError, TypeError, ValueError) as exc:
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID", "Embedding profile is invalid", 422
            ) from exc
        if (
            not isinstance(value["credential_ref"], str)
            or not value["credential_ref"]
            or type(value["max_connections"]) is not int
            or not 1 <= value["max_connections"] <= 32
            or type(value["timeout_seconds"]) not in {int, float}
        ):
            raise ForestError(
                "EMBEDDING_CONFIGURATION_INVALID", "Embedding transport is invalid", 422
            )
        return value, parsed

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        configuration, profile = self._parse_configuration(desired.value)
        await self.repository.initialize_identity(
            operation_id="bootstrap:embedding.multimodal:identity", profile=profile
        )
        self.configuration = configuration
        self.configuration_revision = desired.revision
        self.profile = profile
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
            instance_id=f"instance:embedding.multimodal:{context.activation_id}",
            worker_incarnation=f"worker:embedding.multimodal:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "embedding.multimodal":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Embedding Provider received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Embedding Provider Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if (
            self.generation_identity is None
            or self.configuration is None
            or self.configuration_revision is None
            or self.profile is None
        ):
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Embedding Provider was not prepared and bound",
                503,
            )
        credential = await _call(
            self.credential_resolver,
            self.configuration["credential_ref"],
            self.configuration_revision,
        )
        if not isinstance(credential, str):
            raise ForestError(
                "EMBEDDING_CREDENTIAL_UNAVAILABLE", "Embedding credential is unavailable", 503
            )
        self.client = self.client_factory(
            self.configuration["endpoint"],
            credential,
            self.profile,
            timeout_seconds=self.configuration["timeout_seconds"],
            max_connections=self.configuration["max_connections"],
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

    async def _verify_member_in(self, conn) -> None:
        identity = self.generation_identity
        if not self.active or identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Embedding Provider Generation is not accepting work",
                409,
            )
        await self.generation_member_verifier(
            conn,
            component_id="embedding.multimodal",
            generation_id=identity.generation_id,
        )

    @staticmethod
    def _sources(inputs) -> tuple[PayloadSource, ...]:
        sources = []
        for item in inputs:
            source = item.source
            sources.append(
                PayloadSource(
                    source_component_id="memory.graph",
                    source_kind="embedding.facet",
                    source_id=item.facet_id,
                    source_revision=source.get("source_revision", 1),
                    part_id=source.get("part_id", "*"),
                    source_trunk_id=source.get("trunk_id"),
                    observation_conversation_id=source.get("conversation_ref"),
                    author_subject_id=source.get("author_ref"),
                )
            )
        return tuple(sources)

    async def _save_request(self, prepared, inputs):
        epoch = await self.payloads.epoch()
        return await self.payloads.put_bytes(
            owner_component_id="embedding.multimodal",
            owner_kind="embedding.request",
            owner_id=prepared.transport_attempt_id,
            media_type="application/json",
            content=prepared.body,
            sources=self._sources(inputs),
            expected_deletion_epoch=epoch,
        )

    async def _save_result(self, result, request_payload_id: str):
        body = json.dumps(
            result.as_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
        epoch = await self.payloads.epoch()
        return await self.payloads.put_bytes(
            owner_component_id="embedding.multimodal",
            owner_kind="embedding.result",
            owner_id=result.transport_attempt_id,
            media_type="application/json",
            content=body,
            sources=(
                PayloadSource(
                    "embedding.multimodal",
                    "transport.attempt",
                    result.transport_attempt_id,
                    1,
                ),
            ),
            parents=(request_payload_id,),
            expected_deletion_epoch=epoch,
        )

    async def _settle(self, result, request_payload_id: str):
        if result.status == "RESPONDED":
            payload = await self._save_result(result, request_payload_id)
            payload_id, payload_digest = payload.payload_id, payload.content_digest
        else:
            payload_id = payload_digest = ""
        await self.repository.settle(
            attempt_id=result.transport_attempt_id,
            result=result,
            result_payload_id=payload_id,
            result_payload_digest=payload_digest,
        )

    async def encode(
        self,
        inputs,
        *,
        request_id: str,
        transport_attempt_id: str,
        purpose: str,
        permit_in,
    ) -> EmbeddingResult:
        inputs = tuple(inputs)
        if any(type(item) is not EmbeddingInput for item in inputs):
            raise ForestError("EMBEDDING_INPUT_INVALID", "Embedding inputs are invalid", 422)
        identity = self.generation_identity
        if (
            not self.active
            or identity is None
            or self.client is None
            or self.profile is None
            or self.configuration_revision is None
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Embedding Provider Generation is not accepting work",
                409,
            )
        prepared = prepare_embedding_request(
            self.profile,
            inputs,
            request_id=request_id,
            transport_attempt_id=transport_attempt_id,
            purpose=purpose,
        )
        request_payload = await self._save_request(prepared, inputs)
        await self.repository.prepare_attempt(
            attempt_id=transport_attempt_id,
            request_id=request_id,
            provider_generation_id=identity.generation_id,
            configuration_revision=self.configuration_revision,
            prepared=prepared,
            purpose=purpose,
            request_payload_id=request_payload.payload_id,
        )
        bound = BoundEmbeddingRequest(
            "embedding.multimodal", identity.generation_id, self.configuration_revision, prepared
        )

        async def permitcheck(actual):
            if actual is not prepared:
                raise ForestError(
                    "EMBEDDING_REQUEST_FENCE_MISMATCH",
                    "Embedding request changed before transport",
                    409,
                )
            async with self.store.pool.connection() as conn, conn.transaction():
                await self._verify_member_in(conn)
                await _call(permit_in, conn, request=bound)
                await self.repository.mark_sent_in(conn, attempt_id=transport_attempt_id)

        try:
            result = await self.client.dispatch(prepared, permitcheck=permitcheck)
        except EmbeddingCancelled as exc:
            await asyncio.shield(self._settle(exc.result, request_payload.payload_id))
            raise
        except Exception:
            result = EmbeddingResult(
                "NOT_SENT",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                prepared.space_id,
                error_code="EMBEDDING_PERMIT_REJECTED",
            )
            await asyncio.shield(self._settle(result, request_payload.payload_id))
            raise
        await self._settle(result, request_payload.payload_id)
        return result
