"""Host transport effect for one multimodal Embedding package Generation."""

from __future__ import annotations

import base64
import binascii
import hashlib
import inspect
import json

from forest.embeddings import EmbeddingClient, EmbeddingProfile, PreparedEmbeddingRequest
from forest.errors import ForestError
from forest.packages import digest_json


async def _call(port, *args):
    result = port(*args)
    return await result if inspect.isawaitable(result) else result


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


class MultimodalEmbeddingEffectDriver:
    def __init__(
        self,
        configuration,
        configuration_revision,
        identity,
        declaration,
        *,
        effects,
        runtime,
        credential_resolver,
        client_factory=EmbeddingClient,
    ) -> None:
        if declaration.get("kind") != "embedding.multimodal.http":
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Embedding driver received another effect kind",
                500,
            )
        self.configuration = configuration
        self.configuration_revision = configuration_revision
        self.identity = identity
        self.effects = effects
        self.runtime = runtime
        self.credential_resolver = credential_resolver
        self.client_factory = client_factory
        self.profile = EmbeddingProfile(**configuration["profile"])
        self.profile_ref = "profile:" + digest_json(configuration)[7:]
        self.client = None
        self.accepting = False

    async def start(self) -> None:
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
        self.accepting = True

    async def _verify_effect(self, payload: dict) -> None:
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        loaded = await self.effects.load(payload["effect_operation_id"])
        effect, attempt = loaded["effect"], loaded["attempt"]
        wire_digest = "sha256:" + hashlib.sha256(_canonical(payload)).hexdigest()
        if (
            not self.accepting
            or attempt is None
            or effect["state"] != "DISPATCHING"
            or attempt["state"] != "DISPATCHING"
            or attempt["attempt_id"] != payload["prepared"]["transport_attempt_id"]
            or attempt["executor_component_id"] != self.identity.component_id
            or attempt["executor_generation_id"] != self.identity.generation_id
            or attempt["wire_payload_digest"] != wire_digest
        ):
            raise ForestError(
                "EMBEDDING_EFFECT_FENCE_MISMATCH",
                "Embedding effect no longer authorizes this HTTP handoff",
                409,
            )

    async def invoke(self, payload, *, scope, invocation):
        if (
            payload["component_id"] != scope.component_id
            or payload["generation_id"] != scope.generation_id
            or payload["profile_ref"] != self.profile_ref
            or invocation.operation_id != payload["effect_operation_id"]
            or self.client is None
        ):
            raise ForestError(
                "EMBEDDING_REQUEST_FENCE_MISMATCH",
                "Embedding request belongs to another Provider Generation",
                409,
            )
        raw = payload["prepared"]
        try:
            prepared = PreparedEmbeddingRequest(
                request_id=raw["request_id"],
                transport_attempt_id=raw["transport_attempt_id"],
                body=base64.b64decode(raw["body_base64"], validate=True),
                manifest_json=raw["manifest_json"],
                payload_digest=raw["payload_digest"],
                space_id=raw["space_id"],
            )
            prepared.verify()
        except (KeyError, TypeError, ValueError, binascii.Error) as exc:
            raise ForestError(
                "EMBEDDING_REQUEST_INVALID", "Prepared embedding request is invalid", 422
            ) from exc
        if prepared.space_id != self.profile.space_id:
            raise ForestError(
                "EMBEDDING_ROUTE_MISMATCH", "Embedding request belongs to another space", 409
            )

        async def permitcheck(candidate):
            if candidate is not prepared:
                raise ForestError(
                    "EMBEDDING_REQUEST_FENCE_MISMATCH", "Embedding request changed", 409
                )
            await self._verify_effect(payload)

        return (await self.client.dispatch(prepared, permitcheck=permitcheck)).as_dict()

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        if self.client is None:
            await self.start()
        else:
            self.accepting = True

    async def close(self) -> None:
        self.accepting = False
        client, self.client = self.client, None
        if client is not None:
            await client.aclose()


def multimodal_embedding_effect_driver_factory(
    *, effects, runtime, credential_resolver, client_factory=EmbeddingClient
):
    def create(configuration, configuration_revision, identity, declaration):
        return MultimodalEmbeddingEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            effects=effects,
            runtime=runtime,
            credential_resolver=credential_resolver,
            client_factory=client_factory,
        )

    return create
