"""Generation-owned OneBot v11 transport for the adapter package."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
from collections.abc import Mapping
from dataclasses import dataclass

from forest.adapters.onebot import OneBotWebSocketClient, PreparedSend
from forest.errors import ForestError
from forest.packages import digest_json


async def _call(port, *args):
    result = port(*args)
    return await result if inspect.isawaitable(result) else result


def _connection_epoch(generation_id: str, binding_ref: str) -> int:
    digest = hashlib.sha256((generation_id + "\0" + binding_ref).encode()).hexdigest()
    return int(digest[:15], 16) + 1


def _client_incarnation(generation_id: str, binding_ref: str) -> str:
    digest = hashlib.sha256((generation_id + "\0" + binding_ref).encode()).hexdigest()
    return "onebot-client:" + digest


def _transport_digest(identity, binding: Mapping[str, object]) -> str:
    return digest_json(
        {
            "protocol": "forest.onebot-transport-fence/2",
            "binding_ref": binding["binding_ref"],
            "binding_epoch": binding["binding_epoch"],
            "connection_epoch": _connection_epoch(
                identity.generation_id, binding["binding_ref"]
            ),
            "client_incarnation": _client_incarnation(
                identity.generation_id, binding["binding_ref"]
            ),
            "adapter_generation_id": identity.generation_id,
            "graph_id": identity.graph_id,
            "graph_revision": identity.graph_revision,
        }
    )


@dataclass
class _BindingTransport:
    declaration: dict
    client: OneBotWebSocketClient
    client_incarnation: str
    connection_epoch: int
    transport_digest: str
    events_activated: bool = False


class OneBotTransportEffectDriver:
    """Own credentials, sockets and wire bytes for one Adapter Generation."""

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
        client_factory=OneBotWebSocketClient.create,
    ) -> None:
        ingress = declaration.get("ingress")
        if (
            declaration.get("kind") != "onebot.websocket-v11"
            or declaration.get("name") != "adapter.onebot.transport.send"
            or not isinstance(ingress, Mapping)
            or ingress.get("name") != "adapter.onebot.transport.frame"
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "OneBot driver received another effect or ingress declaration",
                500,
            )
        self.configuration = configuration
        self.configuration_revision = configuration_revision
        self.identity = identity
        self.effects = effects
        self.runtime = runtime
        self.credential_resolver = credential_resolver
        self.client_factory = client_factory
        self._ingress = None
        self._bindings: dict[str, _BindingTransport] = {}
        self._send_admission = False
        self._ingress_admission = False
        self._frame_lock = asyncio.Lock()
        self._inflight_frames: dict[str, asyncio.Future] = {}
        self._frames_drained = asyncio.Event()
        self._frames_drained.set()

    def bind_ingress(self, handler) -> None:
        if self._ingress is not None or not callable(handler):
            raise ForestError(
                "PACKAGE_EFFECT_INGRESS_BINDING_INVALID",
                "OneBot ingress can be bound exactly once",
                409,
            )
        self._ingress = handler

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(
            task
            for transport in self._bindings.values()
            for task in transport.client.owned_tasks()
        )

    async def start(self) -> None:
        if self._ingress is None:
            raise ForestError(
                "PACKAGE_EFFECT_INGRESS_BINDING_INVALID",
                "OneBot ingress is not bound",
                503,
            )
        started = []
        try:
            for binding in self.configuration["bindings"]:
                if not binding["enabled"]:
                    continue
                credential = await _call(
                    self.credential_resolver,
                    binding["credential_ref"],
                    self.configuration_revision,
                )
                if not isinstance(credential, str):
                    raise ForestError(
                        "ONEBOT_CREDENTIAL_UNAVAILABLE",
                        "OneBot credential is unavailable",
                        503,
                    )
                client = self.client_factory(
                    binding["websocket_url"],
                    credential,
                    timeout_seconds=binding["timeout_seconds"],
                    event_capacity=binding["event_capacity"],
                    pending_capacity=binding["pending_capacity"],
                )
                if inspect.isawaitable(client):
                    client = await client
                started.append(client)
                account = await client.get_login_info(timeout=5)
                if account["self_id"] != binding["self_id"]:
                    raise ForestError(
                        "ONEBOT_IDENTITY_MISMATCH",
                        "OneBot API account differs from the configured Binding",
                        409,
                    )
                binding_ref = binding["binding_ref"]
                self._bindings[binding_ref] = _BindingTransport(
                    dict(binding),
                    client,
                    _client_incarnation(self.identity.generation_id, binding_ref),
                    _connection_epoch(self.identity.generation_id, binding_ref),
                    _transport_digest(self.identity, binding),
                )
        except BaseException:
            await asyncio.gather(
                *(client.aclose() for client in reversed(started)),
                return_exceptions=True,
            )
            self._bindings.clear()
            raise
        self._send_admission = True

    def activate_ingress(self) -> None:
        if self._ingress is None:
            raise ForestError(
                "PACKAGE_EFFECT_INGRESS_BINDING_INVALID",
                "OneBot ingress is not bound",
                503,
            )
        self._ingress_admission = True
        for transport in self._bindings.values():
            if transport.events_activated:
                transport.client.resume_events()
                continue
            transport.client.activate_events(
                lambda body, selected=transport: self._deliver_frame(selected, body)
            )
            transport.events_activated = True

    async def _deliver_frame(self, transport: _BindingTransport, body: bytes) -> None:
        if not self._ingress_admission or self._ingress is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "OneBot Generation is not accepting ingress",
                409,
            )
        frame_hash = hashlib.sha256(
            transport.declaration["binding_ref"].encode()
            + b"\0"
            + str(transport.connection_epoch).encode()
            + b"\0"
            + body
        ).hexdigest()
        frame_id = "onebot-frame:" + frame_hash
        async with self._frame_lock:
            future = self._inflight_frames.get(frame_id)
            leader = future is None
            if leader:
                future = asyncio.get_running_loop().create_future()
                self._inflight_frames[frame_id] = future
                self._frames_drained.clear()
        assert future is not None
        if not leader:
            await asyncio.shield(future)
            return
        try:
            result = await self._ingress(
                {
                    "protocol": "forest.onebot-frame/2",
                    "frame_id": frame_id,
                    "binding_ref": transport.declaration["binding_ref"],
                    "binding_epoch": transport.declaration["binding_epoch"],
                    "connection_epoch": transport.connection_epoch,
                    "client_incarnation": transport.client_incarnation,
                    "adapter_generation_id": self.identity.generation_id,
                    "graph_id": self.identity.graph_id,
                    "graph_revision": self.identity.graph_revision,
                    "transport_digest": transport.transport_digest,
                    "body_base64": base64.b64encode(body).decode("ascii"),
                },
                operation_id=frame_id,
                causal_root_kind="adapter.platform-frame",
                causal_root_id=frame_id,
            )
        except BaseException as exc:
            future.set_exception(exc)
            future.exception()
            raise
        else:
            future.set_result(result)
        finally:
            async with self._frame_lock:
                if self._inflight_frames.get(frame_id) is future:
                    del self._inflight_frames[frame_id]
                if not self._inflight_frames:
                    self._frames_drained.set()

    async def freeze_ingress(self) -> None:
        self._ingress_admission = False
        await asyncio.gather(
            *(
                transport.client.freeze_events()
                for transport in self._bindings.values()
                if transport.events_activated
            )
        )
        await self._frames_drained.wait()

    async def _verify_effect(self, payload: dict, transport: _BindingTransport) -> None:
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        loaded = await self.effects.load(payload["effect_operation_id"])
        effect, attempt = loaded["effect"], loaded["attempt"]
        prepared = PreparedSend.from_codec_result(payload["prepared"])
        wire = json.loads(prepared.wire_body)
        target_key = "group_id" if prepared.action == "send_group_msg" else "user_id"
        if (
            not self._send_admission
            or attempt is None
            or effect["state"] != "DISPATCHING"
            or attempt["state"] != "DISPATCHING"
            or attempt["attempt_id"] != payload["attempt_id"]
            or attempt["executor_component_id"] != self.identity.component_id
            or attempt["executor_generation_id"] != self.identity.generation_id
            or attempt["fence_kind"] != "runtime.graph"
            or attempt["fence_id"] != self.identity.graph_id
            or attempt["fence_epoch"] != self.identity.graph_revision
            or attempt["wire_payload_digest"] != "sha256:" + prepared.wire_digest
            or effect["effect_kind"] != "onebot.send"
            or effect["target_kind"]
            != ("group" if target_key == "group_id" else "private")
            or effect["target_id"] != str(wire[target_key])
            or payload["binding_ref"] != transport.declaration["binding_ref"]
            or payload["binding_epoch"] != transport.declaration["binding_epoch"]
            or payload["connection_epoch"] != transport.connection_epoch
            or payload["client_incarnation"] != transport.client_incarnation
            or payload["transport_digest"] != transport.transport_digest
        ):
            raise ForestError(
                "ONEBOT_EFFECT_FENCE_MISMATCH",
                "OneBot effect no longer authorizes this transport handoff",
                409,
            )

    async def invoke(self, payload, *, scope, invocation):
        transport = self._bindings.get(payload["binding_ref"])
        if (
            transport is None
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
            or payload["adapter_generation_id"] != self.identity.generation_id
            or payload["graph_id"] != self.identity.graph_id
            or payload["graph_revision"] != self.identity.graph_revision
        ):
            raise ForestError(
                "ONEBOT_EFFECT_FENCE_MISMATCH",
                "OneBot request belongs to another transport Generation",
                409,
            )
        await self._verify_effect(payload, transport)
        prepared = PreparedSend.from_codec_result(payload["prepared"])
        result = await transport.client.send(
            prepared, timeout=float(payload["timeout_seconds"])
        )
        return {"protocol": "forest.onebot-send-result/2", **result.as_dict()}

    async def quiesce(self) -> None:
        self._send_admission = False

    async def resume(self) -> None:
        if any(transport.client.is_closed for transport in self._bindings.values()):
            raise ForestError(
                "ONEBOT_TRANSPORT_CLOSED",
                "OneBot transport cannot resume after its socket closed",
                503,
            )
        self._send_admission = True

    async def close(self) -> None:
        self._ingress_admission = False
        self._send_admission = False
        transports, self._bindings = tuple(self._bindings.values()), {}
        results = await asyncio.gather(
            *(transport.client.aclose() for transport in transports),
            return_exceptions=True,
        )
        failures = [item for item in results if isinstance(item, BaseException)]
        if failures:
            raise ForestError(
                "ONEBOT_TRANSPORT_CLOSE_FAILED",
                "OneBot transport resources did not close",
                503,
                details={"failures": len(failures)},
            ) from failures[0]


def onebot_transport_effect_driver_factory(
    *,
    effects,
    runtime,
    credential_resolver,
    client_factory=OneBotWebSocketClient.create,
):
    def create(configuration, configuration_revision, identity, declaration):
        return OneBotTransportEffectDriver(
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
