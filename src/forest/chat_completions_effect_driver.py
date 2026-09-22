"""Host HTTP effect driver owned by a Provider package Generation."""

from __future__ import annotations

import base64
import binascii
import hashlib
import inspect
import json
from collections.abc import Mapping

from forest.errors import ForestError
from forest.models import ChatCompletionsClient, ModelCapabilities, PreparedModelRequest
from forest.packages import digest_json


async def _call(port, *args):
    result = port(*args)
    return await result if inspect.isawaitable(result) else result


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


class ChatCompletionsEffectDriver:
    """Keep credentials and sockets outside the Worker while sharing its lifetime."""

    def __init__(
        self,
        configuration: dict,
        configuration_revision: int,
        identity,
        declaration: Mapping[str, object],
        *,
        effects,
        runtime,
        credential_resolver,
        client_factory=ChatCompletionsClient,
    ) -> None:
        if declaration.get("kind") != "chat-completions.http":
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Chat Completions driver received another effect kind",
                500,
            )
        self.configuration = configuration
        self.configuration_revision = configuration_revision
        self.identity = identity
        self.effects = effects
        self.runtime = runtime
        self.credential_resolver = credential_resolver
        self.client_factory = client_factory
        self.capabilities = ModelCapabilities(**configuration["capabilities"])
        self.capability_digest = digest_json(self.capabilities.as_dict())
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
                "PROVIDER_CREDENTIAL_UNAVAILABLE", "Provider credential is unavailable", 503
            )
        self.client = self.client_factory(
            self.configuration["base_url"],
            credential,
            timeout_seconds=self.configuration["timeout_seconds"],
            max_connections=self.configuration["max_connections"],
            deployment_id=self.capabilities.deployment_id,
        )
        self.accepting = True

    async def _verify_effect(self, payload: dict) -> None:
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        loaded = await self.effects.load(payload["effect_operation_id"])
        effect, attempt = loaded["effect"], loaded["attempt"]
        expected_wire_digest = "sha256:" + hashlib.sha256(_canonical(payload)).hexdigest()
        if (
            not self.accepting
            or attempt is None
            or effect["state"] != "DISPATCHING"
            or attempt["state"] != "DISPATCHING"
            or attempt["attempt_id"] != payload["prepared"]["transport_attempt_id"]
            or attempt["executor_component_id"] != self.identity.component_id
            or attempt["executor_generation_id"] != self.identity.generation_id
            or attempt["wire_payload_digest"] != expected_wire_digest
        ):
            raise ForestError(
                "PROVIDER_EFFECT_FENCE_MISMATCH",
                "Provider effect no longer authorizes this HTTP handoff",
                409,
            )

    async def invoke(self, payload, *, scope, invocation):
        if (
            payload["component_id"] != scope.component_id
            or payload["generation_id"] != scope.generation_id
            or payload["profile_ref"] != self.profile_ref
            or payload["capability_digest"] != self.capability_digest
            or invocation.operation_id != payload["effect_operation_id"]
            or self.client is None
        ):
            raise ForestError(
                "PROVIDER_REQUEST_FENCE_MISMATCH",
                "Prepared request belongs to another Provider Generation",
                409,
            )
        raw = payload["prepared"]
        try:
            body = base64.b64decode(raw["body_base64"], validate=True)
            prepared = PreparedModelRequest(
                request_id=raw["request_id"],
                transport_attempt_id=raw["transport_attempt_id"],
                body=body,
                manifest_json=raw["manifest_json"],
                payload_digest=raw["payload_digest"],
                max_output_tokens=raw["max_output_tokens"],
                stream=raw["stream"],
                serializer_revision=raw["serializer_revision"],
            )
            prepared.verify()
        except (KeyError, TypeError, ValueError, binascii.Error) as exc:
            raise ForestError(
                "PROVIDER_REQUEST_INVALID", "Prepared model request is invalid", 422
            ) from exc

        async def permitcheck(candidate):
            if candidate is not prepared:
                raise ForestError(
                    "PROVIDER_REQUEST_FENCE_MISMATCH", "Provider request identity changed", 409
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


def chat_completions_effect_driver_factory(
    *, effects, runtime, credential_resolver, client_factory=ChatCompletionsClient
):
    def create(configuration, configuration_revision, identity, declaration):
        return ChatCompletionsEffectDriver(
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
