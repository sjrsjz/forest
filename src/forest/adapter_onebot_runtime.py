"""Generation-bound OneBot business module for the clean runtime graph."""

from __future__ import annotations

import base64
import hashlib
import inspect
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping

from forest.adapters.onebot import (
    OneBotHTTPClient,
    OneBotWebSocketClient,
    PreparedSend,
    SendResult,
    verify_signature,
)
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.adapter_onebot_authority import (
    OneBotAdapterRepository,
    OneBotPayloadRef,
    OneBotPlatformEvent,
)
from forest.storage.ingress_unit_of_work import IngressClassification, IngressMemoryCapture
from forest.storage.media_capture_authority import IngressMediaCapture, MediaImageCapture
from forest.storage.memory_graph_authority import (
    MemoryPart,
    MemoryPayloadRef,
    MemorySourceIdentity,
)
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot config is not canonical", 422) from exc


def _decode_object(body: bytes) -> dict[str, Any]:
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(body, object_pairs_hook=object_pairs)
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot config is invalid JSON", 422) from exc
    if not isinstance(value, dict):
        raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot config must be an object", 422)
    return value


def _operation_id(kind: str, identity: str) -> str:
    return f"onebot:{kind}:" + hashlib.sha256(identity.encode()).hexdigest()


@dataclass
class _PreparedBinding:
    binding: dict
    client_incarnation: str | None
    executor_fence_digest: str | None
    client: OneBotHTTPClient | OneBotWebSocketClient | None
    webhook_secret: str


@dataclass
class _ActiveBinding:
    binding_id: str
    self_id: str
    client_incarnation: str
    binding_epoch: int
    connection_epoch: int
    executor_fence_digest: str
    client: OneBotHTTPClient | OneBotWebSocketClient
    webhook_secret: str
    transport: str
    accepting: bool = False


@dataclass(frozen=True)
class PreparedAdapterSend:
    binding_id: str
    client_incarnation: str
    binding_epoch: int
    connection_epoch: int
    adapter_generation_id: str
    executor_fence_digest: str
    prepared: PreparedSend


class CleanOneBotRuntime:
    """One Generation of Adapter transport resources and typed ingress/send ports."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "adapter.onebot.ingress", "version": 1},
            {"service_id": "adapter.onebot.prepare-send", "version": 1},
            {"service_id": "adapter.onebot.dispatch", "version": 1},
        ],
        "requires": [
            {"service_id": "kernel.uow.onebot.ingress", "version": 1},
            {"service_id": "conversation.trigger.classify", "version": 1},
            {"service_id": "shell.ingress.inspect", "version": 1},
            {"service_id": "onebot.codec.normalize", "version": 1},
            {"service_id": "onebot.codec.prepare-send", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def runtime_service_handlers(self) -> dict:
        return {
            ("adapter.onebot.ingress", 1): self.admit_http_event,
            ("adapter.onebot.prepare-send", 1): self.prepare_delivery,
            ("adapter.onebot.dispatch", 1): self.dispatch,
        }

    async def record_platform_event_in(self, conn, **values):
        return await self.repository.record_platform_event_in(conn, **values)

    async def verify_transport_fence_in(self, conn, **values):
        return await self.repository.verify_transport_fence_in(conn, **values)

    def __init__(
        self,
        *,
        repository: OneBotAdapterRepository,
        module_configuration,
        grants,
        payloads: KernelPayloadRepository,
        secret_resolver: Callable[..., Awaitable[Mapping[str, str]] | Mapping[str, str]],
        ingress_port,
        trigger_policy,
        shell_inspector,
        normalize_codec,
        prepare_send_codec,
        http_client_factory=OneBotHTTPClient,
        websocket_client_factory=OneBotWebSocketClient.create,
    ) -> None:
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.payloads = payloads
        self.secret_resolver = secret_resolver
        self.ingress_port = ingress_port
        self.trigger_policy = trigger_policy
        self.shell_inspector = shell_inspector
        self.normalize_codec = normalize_codec
        self.prepare_send_codec = prepare_send_codec
        self.http_client_factory = http_client_factory
        self.websocket_client_factory = websocket_client_factory
        self.generation_identity: BoundGenerationIdentity | None = None
        self._prepared: dict[str, _PreparedBinding] = {}
        self._active: dict[str, _ActiveBinding] = {}
        self._publications: dict[str, dict] = {}
        self._accepting = False
        self._closed = False
        self._failure: str | None = None

    @property
    def failure(self) -> str | None:
        if self._failure is not None:
            return self._failure
        for connection in self._active.values():
            if connection.transport == "FORWARD_WEBSOCKET":
                if getattr(connection.client, "failure_code", None) is not None:
                    return connection.client.failure_code
        return None

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
            instance_id=f"instance:adapter.onebot:{context.activation_id}",
            worker_incarnation=f"worker:adapter.onebot:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "adapter.onebot":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "OneBot received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "OneBot Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def _resolve_secret(self, row: dict) -> Mapping[str, str]:
        result = self.secret_resolver(
            secret_ref=row["secret_ref"], expected_digest=row["secret_digest"]
        )
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, Mapping):
            raise ForestError("BINDING_SECRET_INVALID", "OneBot secret is unavailable", 503)
        access_token = result.get("access_token")
        if not isinstance(access_token, str):
            raise ForestError("BINDING_SECRET_INVALID", "OneBot access token is invalid", 422)
        return result

    async def binding_configuration(self, row: dict) -> dict[str, Any]:
        body = await self.payloads.read_bytes(row["config_payload_id"])
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        if digest != row["config_payload_digest"]:
            raise ForestError(
                "PAYLOAD_IDENTITY_CONFLICT", "OneBot config payload digest differs", 409
            )
        values = _decode_object(body)
        allowed = {
            "transport",
            "api_base_url",
            "websocket_url",
            "timeout_seconds",
            "max_connections",
            "event_capacity",
            "pending_capacity",
        }
        if set(values) - allowed:
            raise ForestError("ONEBOT_CONFIG_INVALID", "OneBot config has unknown fields", 422)
        if "transport" in values and values["transport"] != row["transport"]:
            raise ForestError(
                "ONEBOT_CONFIG_INVALID", "OneBot transport contradicts its revision", 422
            )
        return values

    async def start(self) -> None:
        """Prewarm exact desired clients while every event stream remains buffered."""

        if self._closed:
            raise ForestError("ONEBOT_RUNTIME_CLOSED", "OneBot Generation is closed", 503)
        if self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "OneBot Generation identity was not bound",
                503,
            )
        if self._prepared:
            return
        rows = await self.repository.list_desired_bindings()
        for row in rows:
            await self._prewarm_binding(row)

    async def _prewarm_binding(self, row: dict) -> None:
        binding_id = row["binding_id"]
        if not row["enabled"]:
            self._prepared[binding_id] = _PreparedBinding(row, None, None, None, "")
            return
        values = await self.binding_configuration(row)
        secrets = await self._resolve_secret(row)
        generation = self.generation_identity
        assert generation is not None
        suffix = hashlib.sha256(binding_id.encode()).hexdigest()[:20]
        client_incarnation = f"client:{generation.activation_id}:{suffix}"
        executor_fence_digest = digest_json(
            {
                "protocol": "forest.onebot-executor-fence/1",
                "generation_id": generation.generation_id,
                "binding_id": binding_id,
                "client_incarnation": client_incarnation,
                "revision": row["desired_revision"],
            }
        )
        await self.repository.prepare_client(
            client_incarnation=client_incarnation,
            binding_id=binding_id,
            revision=row["desired_revision"],
            expected_binding_epoch=row["binding_epoch"],
            adapter_generation_id=generation.generation_id,
            executor_fence_digest=executor_fence_digest,
        )
        client = None
        try:
            timeout = values.get("timeout_seconds", 15)
            if row["transport"] == "FORWARD_WEBSOCKET":
                if "websocket_url" not in values:
                    raise ForestError(
                        "ONEBOT_CONFIG_INVALID", "WebSocket endpoint is required", 422
                    )
                client = self.websocket_client_factory(
                    values["websocket_url"],
                    secrets["access_token"],
                    timeout_seconds=timeout,
                    event_capacity=values.get("event_capacity", 256),
                    pending_capacity=values.get("pending_capacity", 64),
                )
                if inspect.isawaitable(client):
                    client = await client
                webhook_secret = ""
            else:
                if "api_base_url" not in values:
                    raise ForestError("ONEBOT_CONFIG_INVALID", "HTTP endpoint is required", 422)
                webhook_secret = secrets.get("webhook_secret", "")
                if not isinstance(webhook_secret, str) or not webhook_secret:
                    raise ForestError(
                        "BINDING_SECRET_INVALID", "HTTP webhook secret is required", 422
                    )
                client = self.http_client_factory(
                    values["api_base_url"],
                    secrets["access_token"],
                    timeout_seconds=timeout,
                    max_connections=values.get("max_connections", 16),
                )
            identity = await client.get_login_info(timeout=5)
            if identity["self_id"] != row["self_id"]:
                raise ForestError(
                    "ONEBOT_IDENTITY_MISMATCH", "API account differs from Binding", 409
                )
        except BaseException as exc:
            if client is not None:
                await client.aclose()
            code = exc.code if isinstance(exc, ForestError) else "BINDING_CONNECT_FAILED"
            await self.repository.observe_client(
                operation_id=_operation_id("prewarm-failed", client_incarnation),
                client_incarnation=client_incarnation,
                observed_state="FAILED",
                error_code=code,
            )
            raise
        self._prepared[binding_id] = _PreparedBinding(
            row,
            client_incarnation,
            executor_fence_digest,
            client,
            webhook_secret,
        )

    async def publish_generation_in(self, conn, *, generation_id: str, graph_id: str) -> None:
        """Join Binding publication to the RuntimeGraph commit transaction."""

        generation = self.generation_identity
        if (
            generation is None
            or generation.generation_id != generation_id
            or generation.graph_id != graph_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "OneBot publication belongs to another RuntimeGraph",
                409,
            )
        for binding_id, prepared in sorted(self._prepared.items()):
            row = prepared.binding
            if prepared.client is None:
                publication = await self.repository.publish_disabled_for_generation_in(
                    conn,
                    operation_id=_operation_id(
                        "publish-disabled", generation.activation_id + "\0" + binding_id
                    ),
                    binding_id=binding_id,
                    revision=row["desired_revision"],
                    expected_binding_epoch=row["binding_epoch"],
                )
            else:
                publication = await self.repository.publish_client_for_generation_in(
                    conn,
                    candidate_generation_id=generation.generation_id,
                    operation_id=_operation_id(
                        "publish-client", generation.activation_id + "\0" + binding_id
                    ),
                    binding_id=binding_id,
                    client_incarnation=prepared.client_incarnation,
                    revision=row["desired_revision"],
                    expected_binding_epoch=row["binding_epoch"],
                )
            self._publications[binding_id] = publication

    def activate_generation(self) -> None:
        """Open only local admission gates after the graph transaction commits."""

        if set(self._publications) != set(self._prepared):
            raise ForestError(
                "RUNTIME_PUBLICATION_INCOMPLETE",
                "OneBot Binding publication did not cover the prepared Generation",
                503,
            )
        for binding_id, prepared in sorted(self._prepared.items()):
            if prepared.client is None:
                continue
            row = prepared.binding
            publication = self._publications[binding_id]
            self._active[binding_id] = _ActiveBinding(
                binding_id=binding_id,
                self_id=row["self_id"],
                client_incarnation=prepared.client_incarnation,
                binding_epoch=publication["resulting_binding_epoch"],
                connection_epoch=publication["resulting_connection_epoch"],
                executor_fence_digest=prepared.executor_fence_digest,
                client=prepared.client,
                webhook_secret=prepared.webhook_secret,
                transport=row["transport"],
            )
        self._accepting = True
        for connection in self._active.values():
            connection.accepting = True
            if connection.transport == "FORWARD_WEBSOCKET":
                connection.client.activate_events(
                    lambda body, owned=connection: self._ingest(owned, body)
                )

    async def _payload(
        self,
        *,
        event_id: str,
        owner_kind: str,
        body: bytes,
        source: PayloadSource,
    ) -> OneBotPayloadRef:
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind=owner_kind,
            owner_id=event_id,
            media_type="application/json",
            content=body,
            sources=(source,),
            expected_deletion_epoch=epoch,
        )
        return OneBotPayloadRef(
            saved.payload_id,
            saved.content_digest,
            "adapter.onebot",
            owner_kind,
            event_id,
        )

    async def _ingest(self, connection: _ActiveBinding, body: bytes) -> None:
        if not self._accepting or not connection.accepting:
            raise ForestError("STALE_BINDING", "OneBot Generation is quiescing", 409)
        normalize_digest = hashlib.sha256(
            connection.binding_id.encode()
            + b"\0"
            + str(connection.connection_epoch).encode()
            + b"\0"
            + body
        ).hexdigest()
        codec_result = await self.normalize_codec.invoke(
            body_b64=base64.b64encode(body).decode("ascii"),
            self_id=connection.self_id,
            operation_id="onebot-codec-normalize:" + normalize_digest,
        )
        normalized = codec_result["message"]
        if normalized is None:
            return
        message_ref = normalized["platform_message_id"]
        event_hash = hashlib.sha256(
            (connection.binding_id + "\0" + message_ref).encode()
        ).hexdigest()
        event_id = "onebot-event:" + event_hash
        channel_kind = normalized["channel_kind"]
        channel_ref = normalized["channel_id"]
        conversation_ref = f"onebot:{channel_kind}:{channel_ref}"
        author = normalized["author"]
        anonymous = author.get("anonymous")
        author_subject_ref = (
            f"onebot:anonymous:{anonymous.get('id') or event_hash[:20]}"
            if anonymous is not None
            else "qq:" + author["external_user_id"]
        )
        received_at = datetime.now(timezone.utc)
        sent_at = datetime.fromisoformat(normalized["sent_at"].replace("Z", "+00:00"))
        source = PayloadSource(
            source_component_id="adapter.onebot",
            source_kind="onebot.platform-event",
            source_id=event_id,
            source_revision=1,
            observation_conversation_id=conversation_ref,
            observation_binding_id=connection.binding_id,
            author_subject_id=author_subject_ref,
            occurred_at=sent_at,
        )
        raw_ref = await self._payload(
            event_id=event_id,
            owner_kind="onebot.raw-event",
            body=body,
            source=source,
        )
        author_ref = await self._payload(
            event_id=event_id,
            owner_kind="onebot.author-snapshot",
            body=_canonical(author),
            source=source,
        )
        segments = normalized["segments"]
        source_hash = hashlib.sha256(
            (
                normalized["platform"]
                + "\0"
                + connection.self_id
                + "\0"
                + channel_kind
                + "\0"
                + channel_ref
            ).encode()
        ).hexdigest()
        is_group = channel_kind == "group"
        identity = MemorySourceIdentity(
            source_id="onebot-source:" + source_hash,
            source_kind="GROUP" if is_group else "DIRECT",
            platform=normalized["platform"],
            realm_id="qq:" + connection.self_id,
            external_group_id=channel_ref if is_group else None,
            trunk_id="onebot-trunk:" + source_hash if is_group else None,
        )
        memory_texts = []
        context_parts = []
        images = []
        for index, part in enumerate(segments):
            kind = part["type"]
            if kind == "image":
                media_ref = (
                    "media:" + hashlib.sha256((event_id + "\0" + str(index)).encode()).hexdigest()
                )
                context_parts.append(
                    {
                        "type": "image",
                        "media_ref": media_ref,
                        "media_revision": 1,
                        "state": "PENDING_CAPTURE",
                    }
                )
                locator = part["media_ref"]
                images.append(
                    MediaImageCapture(
                        part_id="part:image:" + str(index),
                        part_index=index,
                        media_ref=media_ref,
                        locator={
                            "kind": "onebot-source",
                            "uri": locator.get("uri"),
                            "platform_file": locator.get("platform_file"),
                        },
                    )
                )
                continue
            context_parts.append(part)
            if kind == "text":
                memory_texts.append((index, part["text"]))
            elif kind == "mention":
                memory_texts.append((index, "[mention:" + part["external_user_id"] + "]"))
            elif kind == "reply":
                memory_texts.append((index, "[reply:" + part["platform_message_id"] + "]"))
            else:
                memory_texts.append(
                    (index, "[unsupported:" + part.get("platform_type", kind) + "]")
                )
        normalized_ref = await self._payload(
            event_id=event_id,
            owner_kind="onebot.normalized-event",
            body=_canonical({"parts": context_parts}),
            source=source,
        )
        memory_capture = None
        media_capture = None
        memory_parts = []
        if not memory_texts and not images:
            memory_texts.append((0, "[empty message]"))
        for part_index, memory_text in memory_texts:
            memory_owner_id = event_id + ":memory-text:" + str(part_index)
            epoch = await self.payloads.epoch()
            saved_memory_text = await self.payloads.put_bytes(
                owner_component_id="adapter.onebot",
                owner_kind="onebot.memory-text",
                owner_id=memory_owner_id,
                media_type="text/plain; charset=utf-8",
                content=memory_text.encode("utf-8"),
                sources=(source,),
                parents=(normalized_ref.payload_id,),
                expected_deletion_epoch=epoch,
            )
            memory_parts.append(
                MemoryPart(
                    "part:text:" + str(part_index),
                    part_index,
                    "TEXT",
                    "text/plain; charset=utf-8",
                    MemoryPayloadRef(
                        saved_memory_text.payload_id,
                        saved_memory_text.content_digest,
                        "adapter.onebot",
                        "onebot.memory-text",
                        memory_owner_id,
                    ),
                    byte_length=len(memory_text.encode("utf-8")),
                )
            )
        if images:
            media_capture = IngressMediaCapture(
                capture_id="media-capture:" + event_hash,
                identity=identity,
                source_unit_id="onebot-unit:" + event_hash,
                source_revision=1,
                unit_kind="GROUP_MESSAGE" if is_group else "DIRECT_MESSAGE",
                author=MemoryPayloadRef(
                    author_ref.payload_id,
                    author_ref.digest,
                    author_ref.owner_component_id,
                    author_ref.owner_kind,
                    author_ref.owner_id,
                ),
                text_parts=tuple(memory_parts),
                image_parts=tuple(images),
                occurred_at=sent_at,
            )
        else:
            memory_capture = IngressMemoryCapture(
                identity=identity,
                source_unit_id="onebot-unit:" + event_hash,
                source_revision=1,
                unit_kind="GROUP_MESSAGE" if is_group else "DIRECT_MESSAGE",
                author=MemoryPayloadRef(
                    author_ref.payload_id,
                    author_ref.digest,
                    author_ref.owner_component_id,
                    author_ref.owner_kind,
                    author_ref.owner_id,
                ),
                parts=tuple(memory_parts),
                occurred_at=sent_at,
            )
        event = OneBotPlatformEvent(
            event_id=event_id,
            platform_message_ref=message_ref,
            event_kind=channel_kind + ".message",
            channel_kind="GROUP" if channel_kind == "group" else "PRIVATE",
            channel_ref=channel_ref,
            conversation_ref=conversation_ref,
            author_subject_ref=author_subject_ref,
            raw=raw_ref,
            normalized=normalized_ref,
            author=author_ref,
            received_at=received_at,
            classification_deadline=received_at + timedelta(minutes=2),
            sent_at=sent_at,
        )
        projected = {**normalized, "segments": context_parts}
        shell_command = await self.shell_inspector(
            normalized=projected,
            binding_ref=connection.binding_id,
            binding_epoch=connection.binding_epoch,
            connection_epoch=connection.connection_epoch,
            client_incarnation=connection.client_incarnation,
            adapter_generation_id=self.generation_identity.generation_id,
            executor_fence_digest=connection.executor_fence_digest,
            conversation_ref=conversation_ref,
            event_ref=event_id,
            principal_ref=author_subject_ref,
        )
        classification = None
        if shell_command is None:
            classified = await self.trigger_policy.invoke(
                normalized=projected,
                operation_id="onebot-trigger-classify:" + event_hash,
            )
            classification = IngressClassification(
                decision=classified["decision"],
                classifier_component_id=classified["classifier_component_id"],
                classifier_generation_id=classified["classifier_generation_id"],
                rule_revision=classified["rule_revision"],
                reason_code=classified["reason_code"],
            )
        await self.ingress_port.admit(
            operation_id="onebot-admit:" + event_hash,
            binding_id=connection.binding_id,
            expected_binding_epoch=connection.binding_epoch,
            client_incarnation=connection.client_incarnation,
            event=event,
            memory_capture=memory_capture,
            media_capture=media_capture,
            classification=classification,
            shell_command=shell_command,
        )

    async def admit_http_event(
        self, *, binding_id: str, body: bytes, signature: str | None
    ) -> None:
        connection = self._active.get(binding_id)
        if connection is None or connection.transport != "HTTP_POST" or not connection.accepting:
            raise ForestError("STALE_BINDING", "HTTP Binding is not accepting events", 409)
        verify_signature(body, connection.webhook_secret, signature)
        await self._ingest(connection, body)

    async def prepare_delivery(
        self,
        *,
        binding_id: str,
        channel_kind: str,
        channel_ref: str,
        parts: list[dict[str, Any]],
        operation_id: str,
    ) -> PreparedAdapterSend:
        generation = self.generation_identity
        connection = self._active.get(binding_id)
        if (
            not self._accepting
            or generation is None
            or connection is None
            or not connection.accepting
        ):
            raise ForestError("STALE_BINDING", "OneBot Binding is not accepting sends", 409)
        codec_result = await self.prepare_send_codec.invoke(
            channel_kind=channel_kind.lower(),
            channel_id=channel_ref,
            segments=parts,
            operation_id=operation_id,
        )
        prepared = PreparedSend.from_codec_result(codec_result)
        return PreparedAdapterSend(
            binding_id,
            connection.client_incarnation,
            connection.binding_epoch,
            connection.connection_epoch,
            generation.generation_id,
            connection.executor_fence_digest,
            prepared,
        )

    async def dispatch(
        self,
        *,
        binding_id: str,
        client_incarnation: str,
        binding_epoch: int,
        connection_epoch: int,
        adapter_generation_id: str,
        executor_fence_digest: str,
        prepared: PreparedSend,
        timeout: float | None = None,
    ) -> SendResult:
        generation = self.generation_identity
        connection = self._active.get(binding_id)
        if (
            not self._accepting
            or generation is None
            or generation.generation_id != adapter_generation_id
            or connection is None
            or not connection.accepting
            or connection.client_incarnation != client_incarnation
        ):
            return SendResult("NOT_SENT", error_code="STALE_BINDING")
        await self.repository.verify_transport_fence(
            binding_ref=binding_id,
            binding_epoch=binding_epoch,
            connection_epoch=connection_epoch,
            client_incarnation=client_incarnation,
            adapter_generation_id=adapter_generation_id,
            executor_fence_digest=executor_fence_digest,
        )
        return await connection.client.send(prepared, timeout=timeout)

    async def quiesce(self) -> None:
        self._accepting = False
        for connection in self._active.values():
            connection.accepting = False
        for connection in self._active.values():
            await self.repository.observe_client(
                operation_id=_operation_id("retire", connection.client_incarnation),
                client_incarnation=connection.client_incarnation,
                observed_state="RETIRING",
            )

    async def close(self) -> None:
        self._accepting = False
        errors = []
        active_ids = {connection.client_incarnation for connection in self._active.values()}
        for prepared in self._prepared.values():
            if prepared.client is None:
                continue
            incarnation = prepared.client_incarnation
            try:
                if incarnation in active_ids:
                    connection = next(
                        item
                        for item in self._active.values()
                        if item.client_incarnation == incarnation
                    )
                    connection.accepting = False
                    await self.repository.observe_client(
                        operation_id=_operation_id("retire", incarnation),
                        client_incarnation=incarnation,
                        observed_state="RETIRING",
                    )
                await prepared.client.aclose()
                await self.repository.observe_client(
                    operation_id=_operation_id("close", incarnation),
                    client_incarnation=incarnation,
                    observed_state="CLOSED",
                )
            except Exception as exc:
                errors.append(exc)
        self._active.clear()
        self._prepared.clear()
        self._publications.clear()
        self._closed = not errors
        if errors:
            raise ForestError(
                "ONEBOT_CLEANUP_FAILED",
                "OneBot Generation resources remain unsettled",
                503,
                details={"errors": len(errors)},
            ) from errors[0]

    def owned_tasks(self) -> tuple:
        tasks = []
        for prepared in self._prepared.values():
            owned = getattr(prepared.client, "owned_tasks", None)
            if owned is not None:
                tasks.extend(owned())
        return tuple(tasks)
