"""Host HTTP effect driver owned by a Provider package Generation."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Mapping

from forest.errors import ForestError
from forest.model_contract import ModelCapabilities, PreparedModelRequest
from forest.model_transport import ChatCompletionsClient
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


def _provider_payload_ref(saved, *, owner_kind: str, owner_id: str) -> dict:
    return {
        "payload_id": saved.payload_id,
        "digest": saved.content_digest,
        "owner_component_id": "provider.chat-completions",
        "owner_kind": owner_kind,
        "owner_id": owner_id,
    }


async def _load_handed_off_model_command(effects, payloads, request):
    """Bind a read to the original Agent Effect and frozen Provider command."""

    if (
        type(request) is not dict
        or set(request) != {"protocol", "effect_operation_id", "attempt_id"}
        or request["protocol"] != "forest.provider-result-lookup-request/1"
        or not isinstance(request["effect_operation_id"], str)
        or not isinstance(request["attempt_id"], str)
    ):
        raise ForestError(
            "EFFECT_OUTCOME_INPUT_INVALID", "Executor outcome request is invalid", 422
        )
    loaded = await effects.load(request["effect_operation_id"])
    effect, attempt = loaded["effect"], loaded["attempt"]
    if (
        effect["producer_component_id"] != "agent.runtime"
        or effect["effect_kind"] != "provider.model.dispatch"
        or effect["target_kind"] != "component.service"
        or effect["target_id"]
            != "provider.chat-completions/provider.model.dispatch"
        or attempt is None
        or attempt["attempt_id"] != request["attempt_id"]
        or attempt["executor_component_id"] != "provider.chat-completions"
        or attempt["state"] not in {
            "DISPATCHING", "SUCCEEDED", "NOT_APPLIED", "UNKNOWN"
        }
        or effect["input_payload_id"] is None
    ):
        raise ForestError(
            "EFFECT_OUTCOME_BOUNDARY_CLOSED",
            "Effect is not one handed-off Agent model operation", 409,
        )
    async with payloads.store.pool.connection() as conn, conn.transaction():
        ref = await payloads.verify_in(
            conn,
            payload_id=effect["input_payload_id"],
            digest=effect["input_payload_digest"],
            owner_component_id="kernel.effect",
            owner_kind="effect.input",
            owner_id=effect["effect_operation_id"],
        )
        content = await payloads.read_bytes_in(conn, ref.payload_id)
    try:
        command = json.loads(content)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ForestError(
            "EFFECT_OUTCOME_CORRUPT", "Frozen Effect command is invalid", 500
        ) from exc
    if (
        type(command) is not dict
        or command.get("protocol") != "forest.provider-dispatch/1"
        or command.get("effect_operation_id") != effect["effect_operation_id"]
        or command.get("component_id") != attempt["executor_component_id"]
        or command.get("generation_id") != attempt["executor_generation_id"]
        or not isinstance(command.get("prepared"), dict)
        or command["prepared"].get("transport_attempt_id")
            != attempt["attempt_id"]
        or not isinstance(command["prepared"].get("request_id"), str)
        or not isinstance(command["prepared"].get("payload_digest"), str)
    ):
        raise ForestError(
            "EFFECT_OUTCOME_CORRUPT", "Frozen Effect command identity differs", 500
        )
    return effect, attempt, command


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
            or value["owner_component_id"] != (
                "agent.runtime" if owner_kind in {"model.projection", "model.wire"}
                else "provider.chat-completions"
            )
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
            "protocol": "forest.provider-model-result/2",
            "status": value["status"],
            "request_id": value["request_id"],
            "transport_attempt_id": value["transport_attempt_id"],
            "payload_digest": value["payload_digest"],
            "text": value["text"],
            "assistant_message": value["assistant_message"],
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
            receipt["result"] = _provider_payload_ref(
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
                receipt["usage"] = _provider_payload_ref(
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

    async def _verify_effect(self, payload: dict, *, producer_component_id: str) -> dict:
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
            or effect["producer_component_id"] != producer_component_id
            or effect["effect_kind"] != "provider.model.dispatch"
            or effect["target_kind"] != "component.service"
            or effect["target_id"] != self.identity.component_id + "/provider.model.dispatch"
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
        return attempt

    async def _verify_payload_handoff(self, projection: dict, wire: dict, attempt: dict) -> None:
        """Serialize final HTTP admission with Payload forget across all input refs."""
        effect_wire = {
            "payload_id": attempt["wire_payload_id"],
            "digest": attempt["wire_payload_digest"],
            "owner_component_id": "kernel.effect",
            "owner_kind": "effect.wire",
            "owner_id": attempt["attempt_id"],
        }
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            for reference in (projection, wire, effect_wire):
                await self.payloads.verify_in(
                    conn,
                    payload_id=reference["payload_id"],
                    digest=reference["digest"],
                    owner_component_id=reference["owner_component_id"],
                    owner_kind=reference["owner_kind"],
                    owner_id=reference["owner_id"],
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
            attempt = await self._verify_effect(
                payload,
                producer_component_id=projection["owner_component_id"],
            )
            await self._verify_payload_handoff(projection, wire, attempt)

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


class ChatCompletionsResultLookupDriver:
    """Read a Provider-owned result after HTTP handoff without dispatching again."""

    def __init__(self, identity, declaration, *, effects, payloads, runtime) -> None:
        if declaration.get("kind") != "chat-completions.result-lookup":
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Provider result lookup received another effect kind", 500,
            )
        self.identity = identity
        self.effects = effects
        self.payloads = payloads
        self.runtime = runtime
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False

    async def invoke(self, payload, *, scope, invocation):
        if (
            not self.accepting
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "PROVIDER_RESULT_LOOKUP_UNAVAILABLE", "Provider lookup is unavailable", 503,
            )
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        effect, attempt, command = await _load_handed_off_model_command(
            self.effects,
            self.payloads,
            {
                "protocol": "forest.provider-result-lookup-request/1",
                "effect_operation_id": payload["effect_operation_id"],
                "attempt_id": payload["attempt_id"],
            },
        )
        prepared = command["prepared"]
        common = {
            "protocol": "forest.provider-result-lookup/1",
            "effect_operation_id": effect["effect_operation_id"],
            "attempt_id": attempt["attempt_id"],
            "request_id": prepared["request_id"],
            "payload_digest": prepared["payload_digest"],
        }
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            epoch = await self.payloads.epoch_in(conn)
            result_ref = await self.payloads.find_owner_in(
                conn,
                owner_component_id=self.identity.component_id,
                owner_kind="model.result",
                owner_id=attempt["attempt_id"],
            )
            if result_ref is None:
                return {**common, "state": "ABSENT", "receipt": None}
            result_bytes = await self.payloads.read_bytes_in(
                conn, result_ref.payload_id, expected_deletion_epoch=epoch
            )
            usage_ref = await self.payloads.find_owner_in(
                conn,
                owner_component_id=self.identity.component_id,
                owner_kind="model.usage",
                owner_id=attempt["attempt_id"],
            )
            usage_bytes = (
                await self.payloads.read_bytes_in(
                    conn, usage_ref.payload_id, expected_deletion_epoch=epoch
                )
                if usage_ref is not None else None
            )
        try:
            if (
                "sha256:" + hashlib.sha256(result_bytes).hexdigest()
                != result_ref.content_digest
                or (usage_ref is not None and (
                    "sha256:" + hashlib.sha256(usage_bytes).hexdigest()
                    != usage_ref.content_digest
                ))
            ):
                raise ForestError(
                    "PROVIDER_RESULT_CORRUPT", "Provider result digest differs", 500
                )
            result = json.loads(result_bytes)
            usage = json.loads(usage_bytes) if usage_bytes is not None else None
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ForestError(
                "PROVIDER_RESULT_CORRUPT", "Provider result Payload is invalid", 500
            ) from exc
        if (
            type(result) is not dict
            or set(result) != {
                "protocol", "status", "request_id", "transport_attempt_id",
                "payload_digest", "text", "tool_calls", "finish_reason",
                "provider_response_id", "provider_model", "error_code", "assistant_message",
            }
            or result["protocol"] != "forest.provider-model-result/2"
            or result["status"] != "RESPONDED"
            or result["request_id"] != prepared["request_id"]
            or result["transport_attempt_id"] != attempt["attempt_id"]
            or result["payload_digest"] != prepared["payload_digest"]
            or not isinstance(result["text"], str)
            or not isinstance(result["assistant_message"], dict)
            or not isinstance(result["tool_calls"], list)
            or result["error_code"] not in {
                None, "MODEL_USAGE_INVALID", "MODEL_TOOL_ARGUMENTS",
                "MODEL_TOOL_SEQUENCE", "MODEL_TOOL_UNDECLARED",
                "MODEL_EMPTY_OUTPUT", "MODEL_OUTPUT_INCOMPLETE",
            }
            or (usage is not None and (
                type(usage) is not dict
                or set(usage) != {
                    "protocol", "request_id", "transport_attempt_id", "usage"
                }
                or usage["protocol"] != "forest.provider-model-usage/1"
                or usage["request_id"] != prepared["request_id"]
                or usage["transport_attempt_id"] != attempt["attempt_id"]
                or not isinstance(usage["usage"], dict)
            ))
        ):
            raise ForestError(
                "PROVIDER_RESULT_CORRUPT", "Provider result identity differs", 500
            )
        receipt = {
            "status": "RESPONDED",
            "request_id": prepared["request_id"],
            "transport_attempt_id": attempt["attempt_id"],
            "payload_digest": prepared["payload_digest"],
            "result": _provider_payload_ref(
                result_ref, owner_kind="model.result", owner_id=attempt["attempt_id"]
            ),
            "usage": (
                _provider_payload_ref(
                    usage_ref, owner_kind="model.usage", owner_id=attempt["attempt_id"]
                ) if usage_ref is not None else None
            ),
            "error_code": result["error_code"],
        }
        return {**common, "state": "RESPONDED", "receipt": receipt}


def chat_completions_effect_driver_factory(
    *, effects, payloads, runtime, credential_resolver, client_factory=ChatCompletionsClient
):
    def create(configuration, configuration_revision, identity, declaration):
        if declaration.get("kind") == "chat-completions.result-lookup":
            return ChatCompletionsResultLookupDriver(
                identity, declaration, effects=effects, payloads=payloads,
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
