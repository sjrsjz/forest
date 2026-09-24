"""Host HTTP effect driver owned by a Provider package Generation."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Mapping

from forest.builtin.agent_runtime.python.agent_protocol import ProtocolRepairView
from forest.errors import ForestError
from forest.models import (
    ChatCompletionsClient,
    MediaSnapshot,
    ModelCapabilities,
    PreparedModelRequest,
    prepare_model_request,
)
from forest.packages import digest_json


async def _call(port, *args):
    result = port(*args)
    return await result if inspect.isawaitable(result) else result


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
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
        payloads,
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
        self.payloads = payloads
        self.runtime = runtime
        self.credential_resolver = credential_resolver
        self.client_factory = client_factory
        self.capabilities = ModelCapabilities(**configuration["capabilities"])
        self.capability_digest = digest_json(self.capabilities.as_dict())
        self.profile_ref = "profile:" + digest_json(configuration)[7:]
        self.client = None
        self.accepting = False

    @staticmethod
    def _reference(value: object, *, owner_kind: str, owner_id: str) -> dict:
        fields = {
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or value["owner_component_id"] != "provider.chat-completions"
            or value["owner_kind"] != owner_kind
            or value["owner_id"] != owner_id
        ):
            raise ForestError(
                "PROVIDER_REQUEST_INVALID", "Prepared payload reference is invalid", 422
            )
        return value

    async def _load_prepared(self, raw: dict) -> tuple[PreparedModelRequest, dict, dict]:
        projection = self._reference(
            raw.get("projection"),
            owner_kind="model.projection",
            owner_id=raw.get("request_id"),
        )
        wire = self._reference(
            raw.get("wire"),
            owner_kind="model.wire",
            owner_id=raw.get("transport_attempt_id"),
        )
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            epoch = await self.payloads.epoch_in(conn)
            for reference in (projection, wire):
                await self.payloads.verify_in(
                    conn,
                    payload_id=reference["payload_id"],
                    digest=reference["digest"],
                    owner_component_id=reference["owner_component_id"],
                    owner_kind=reference["owner_kind"],
                    owner_id=reference["owner_id"],
                )
            manifest = await self.payloads.read_bytes_in(
                conn,
                projection["payload_id"],
                expected_deletion_epoch=epoch,
            )
            body = await self.payloads.read_bytes_in(
                conn,
                wire["payload_id"],
                expected_deletion_epoch=epoch,
            )
        try:
            prepared = PreparedModelRequest(
                request_id=raw["request_id"],
                transport_attempt_id=raw["transport_attempt_id"],
                body=body,
                manifest_json=manifest.decode("utf-8"),
                payload_digest=raw["payload_digest"],
                max_output_tokens=raw["max_output_tokens"],
                stream=raw["stream"],
                serializer_revision=raw["serializer_revision"],
            )
            prepared.verify()
        except (KeyError, TypeError, ValueError, UnicodeError) as exc:
            raise ForestError(
                "PROVIDER_REQUEST_INVALID", "Prepared model request is invalid", 422
            ) from exc
        if wire["digest"] != prepared.payload_digest:
            raise ForestError(
                "PROVIDER_REQUEST_FENCE_MISMATCH",
                "Prepared wire digest differs from its managed payload",
                409,
            )
        return prepared, projection, wire

    @staticmethod
    def _payload_ref(saved, *, owner_kind: str, owner_id: str) -> dict:
        return {
            "payload_id": saved.payload_id,
            "digest": saved.content_digest,
            "owner_component_id": "provider.chat-completions",
            "owner_kind": owner_kind,
            "owner_id": owner_id,
        }

    async def _materialize_result(
        self,
        result,
        *,
        projection: dict,
        wire: dict,
    ) -> dict:
        value = result.as_dict()
        receipt = {
            "status": value["status"],
            "request_id": value["request_id"],
            "transport_attempt_id": value["transport_attempt_id"],
            "payload_digest": value["payload_digest"],
            "result": None,
            "usage": None,
            "error_code": value["error_code"],
        }
        if value["status"] != "RESPONDED":
            return receipt

        result_document = {
            "protocol": "forest.provider-model-result/1",
            "status": value["status"],
            "request_id": value["request_id"],
            "transport_attempt_id": value["transport_attempt_id"],
            "payload_digest": value["payload_digest"],
            "text": value["text"],
            "tool_calls": value["tool_calls"],
            "finish_reason": value["finish_reason"],
            "provider_response_id": value["provider_response_id"],
            "provider_model": value["provider_model"],
            "error_code": value["error_code"],
        }
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            epoch = await self.payloads.epoch_in(conn)
            saved_result = await self.payloads.put_bytes_in(
                conn,
                owner_component_id=self.identity.component_id,
                owner_kind="model.result",
                owner_id=value["transport_attempt_id"],
                media_type="application/json",
                content=_canonical(result_document),
                parents=(projection["payload_id"], wire["payload_id"]),
                expected_deletion_epoch=epoch,
            )
            receipt["result"] = self._payload_ref(
                saved_result,
                owner_kind="model.result",
                owner_id=value["transport_attempt_id"],
            )
            if value["usage"] is not None:
                saved_usage = await self.payloads.put_bytes_in(
                    conn,
                    owner_component_id=self.identity.component_id,
                    owner_kind="model.usage",
                    owner_id=value["transport_attempt_id"],
                    media_type="application/json",
                    content=_canonical(
                        {
                            "protocol": "forest.provider-model-usage/1",
                            "request_id": value["request_id"],
                            "transport_attempt_id": value["transport_attempt_id"],
                            "usage": value["usage"],
                        }
                    ),
                    parents=(saved_result.payload_id,),
                    expected_deletion_epoch=epoch,
                )
                receipt["usage"] = self._payload_ref(
                    saved_usage,
                    owner_kind="model.usage",
                    owner_id=value["transport_attempt_id"],
                )
        return receipt

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
        prepared, projection, wire = await self._load_prepared(payload["prepared"])

        async def permitcheck(candidate):
            if candidate is not prepared:
                raise ForestError(
                    "PROVIDER_REQUEST_FENCE_MISMATCH", "Provider request identity changed", 409
                )
            await self._verify_effect(payload)

        result = await self.client.dispatch(prepared, permitcheck=permitcheck)
        return await self._materialize_result(
            result,
            projection=projection,
            wire=wire,
        )

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


class ChatCompletionsMaterializeDriver:
    """Materialize Provider-owned projection and wire payloads from exact refs."""

    def __init__(
        self,
        configuration: dict,
        _configuration_revision: int,
        identity,
        declaration: Mapping[str, object],
        *,
        payloads,
        runtime,
    ) -> None:
        if (
            declaration.get("kind") != "chat-completions.materialize"
            or declaration.get("name") != "provider.request.materialize"
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Chat Completions materializer received another effect kind",
                500,
            )
        self.identity = identity
        self.payloads = payloads
        self.runtime = runtime
        self.capabilities = ModelCapabilities(**configuration["capabilities"])
        self.capability_digest = digest_json(self.capabilities.as_dict())
        self.profile_ref = "profile:" + digest_json(configuration)[7:]
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    def _fence(self, payload: dict, scope) -> None:
        if (
            not self.accepting
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
            or payload["component_id"] != self.identity.component_id
            or payload["generation_id"] != self.identity.generation_id
            or payload["profile_ref"] != self.profile_ref
            or payload["capability_digest"] != self.capability_digest
        ):
            raise ForestError(
                "PROVIDER_REQUEST_FENCE_MISMATCH",
                "Model projection belongs to another Provider Generation",
                409,
            )

    @staticmethod
    def _input_envelope(value: object, payload: dict) -> dict:
        fields = {
            "protocol",
            "request_id",
            "transport_attempt_id",
            "snapshot",
            "preset_context",
            "tools",
            "evidence",
            "repair",
            "output_tokens",
            "stream",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or value.get("protocol") != "forest.agent-model-input/1"
            or value.get("request_id") != payload["request_id"]
            or value.get("transport_attempt_id") != payload["transport_attempt_id"]
            or not isinstance(value.get("snapshot"), dict)
            or not isinstance(value.get("preset_context"), list)
            or not isinstance(value.get("tools"), list)
            or value.get("evidence") is not None
            and not isinstance(value["evidence"], dict)
            or value.get("repair") is not None
            and not isinstance(value["repair"], dict)
            or type(value.get("output_tokens")) is not int
            or type(value.get("stream")) is not bool
        ):
            raise ForestError(
                "PROVIDER_MODEL_INPUT_INVALID",
                "Agent model input payload is invalid",
                422,
            )
        return value

    @staticmethod
    def _payload_ref(value: object) -> dict:
        fields = {
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ForestError(
                "PROVIDER_MODEL_INPUT_INVALID", "Payload reference is invalid", 422
            )
        return value

    async def invoke(self, payload, *, scope, invocation):
        self._fence(payload, scope)
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        input_ref = self._payload_ref(payload["input"])
        if (
            input_ref["owner_component_id"] != "agent.runtime"
            or input_ref["owner_kind"] != "model.input"
            or input_ref["owner_id"] != payload["transport_attempt_id"]
        ):
            raise ForestError(
                "PROVIDER_MODEL_INPUT_INVALID",
                "Model input ownership differs from its request",
                422,
            )

        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            epoch = await self.payloads.epoch_in(conn)
            await self.payloads.verify_in(
                conn,
                payload_id=input_ref["payload_id"],
                digest=input_ref["digest"],
                owner_component_id=input_ref["owner_component_id"],
                owner_kind=input_ref["owner_kind"],
                owner_id=input_ref["owner_id"],
            )
            input_body = await self.payloads.read_bytes_in(
                conn,
                input_ref["payload_id"],
                expected_deletion_epoch=epoch,
            )
            try:
                envelope = self._input_envelope(json.loads(input_body), payload)
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ForestError(
                    "PROVIDER_MODEL_INPUT_INVALID",
                    "Agent model input is not finite JSON",
                    422,
                ) from exc

            media = {}
            media_parent_ids = []
            seen = set()
            for asset in payload["media"]:
                identity = (asset["source_unit_id"], asset["part_id"])
                if identity in seen:
                    raise ForestError(
                        "PROVIDER_MEDIA_INPUT_INVALID",
                        "Media identity repeats in one model projection",
                        422,
                    )
                seen.add(identity)
                reference = self._payload_ref(asset["payload"])
                await self.payloads.verify_in(
                    conn,
                    payload_id=reference["payload_id"],
                    digest=reference["digest"],
                    owner_component_id=reference["owner_component_id"],
                    owner_kind=reference["owner_kind"],
                    owner_id=reference["owner_id"],
                )
                content = await self.payloads.read_bytes_in(
                    conn,
                    reference["payload_id"],
                    expected_deletion_epoch=epoch,
                )
                if len(content) != asset["byte_length"]:
                    raise ForestError(
                        "PROVIDER_MEDIA_INPUT_INVALID",
                        "Media byte length differs from its captured metadata",
                        409,
                    )
                image = MediaSnapshot(
                    envelope["snapshot"]["bot_id"],
                    asset["media_ref"],
                    asset["media_revision"],
                    asset["source_unit_id"],
                    asset["part_id"],
                    asset["media_type"],
                    content,
                    reference["digest"],
                    asset["width"],
                    asset["height"],
                )
                media[identity] = image
                media[asset["media_ref"]] = image
                media_parent_ids.append(reference["payload_id"])

            repair = (
                ProtocolRepairView.from_record(envelope["repair"])
                if envelope["repair"] is not None
                else None
            )
            prepared = prepare_model_request(
                envelope["snapshot"],
                self.capabilities,
                request_id=payload["request_id"],
                transport_attempt_id=payload["transport_attempt_id"],
                preset_context=envelope["preset_context"],
                tools=envelope["tools"],
                media=media,
                output_tokens=envelope["output_tokens"],
                stream=envelope["stream"],
                evidence=envelope["evidence"],
                repair_view=repair,
            )
            projection = await self.payloads.put_bytes_in(
                conn,
                owner_component_id=self.identity.component_id,
                owner_kind="model.projection",
                owner_id=prepared.request_id,
                media_type="application/json",
                content=prepared.manifest_json.encode("utf-8"),
                parents=(input_ref["payload_id"], *media_parent_ids),
                expected_deletion_epoch=epoch,
            )
            wire = await self.payloads.put_bytes_in(
                conn,
                owner_component_id=self.identity.component_id,
                owner_kind="model.wire",
                owner_id=prepared.transport_attempt_id,
                media_type="application/json",
                content=prepared.body,
                parents=(input_ref["payload_id"], *media_parent_ids),
                expected_deletion_epoch=epoch,
            )

        manifest = prepared.manifest

        def ref(saved, kind, owner_id):
            return {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": self.identity.component_id,
                "owner_kind": kind,
                "owner_id": owner_id,
            }
        return {
            "protocol": "forest.provider-materialized/1",
            "component_id": self.identity.component_id,
            "generation_id": self.identity.generation_id,
            "profile_ref": self.profile_ref,
            "capability_digest": self.capability_digest,
            "request_id": prepared.request_id,
            "transport_attempt_id": prepared.transport_attempt_id,
            "projection": ref(projection, "model.projection", prepared.request_id),
            "wire": ref(wire, "model.wire", prepared.transport_attempt_id),
            "projected_content_digest": manifest["projected_content_digest"],
            "source_manifest_digest": digest_json(manifest["source_manifest"]),
            "max_output_tokens": prepared.max_output_tokens,
            "stream": prepared.stream,
            "serializer_revision": prepared.serializer_revision,
        }

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False


def chat_completions_effect_driver_factory(
    *, effects, payloads, runtime, credential_resolver, client_factory=ChatCompletionsClient
):
    def create(configuration, configuration_revision, identity, declaration):
        if declaration.get("kind") == "chat-completions.materialize":
            return ChatCompletionsMaterializeDriver(
                configuration,
                configuration_revision,
                identity,
                declaration,
                payloads=payloads,
                runtime=runtime,
            )
        return ChatCompletionsEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            effects=effects,
            payloads=payloads,
            runtime=runtime,
            credential_resolver=credential_resolver,
            client_factory=client_factory,
        )

    return create
