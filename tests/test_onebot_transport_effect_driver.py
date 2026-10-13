import asyncio
import base64
import hashlib
import json
from types import SimpleNamespace

import pytest

from forest.adapters.onebot import PreparedSend, SendResult
from forest.errors import ForestError
from forest.onebot_transport_effect_driver import OneBotTransportEffectDriver
from forest.package_runtime import PackageGenerationAdapter


def _prepared(text="hello"):
    body = json.dumps(
        {
            "group_id": 933179609,
            "message": [{"type": "text", "data": {"text": text}}],
            "auto_escape": True,
        },
        separators=(",", ":"),
    ).encode()
    value = PreparedSend("send_group_msg", body, hashlib.sha256(body).hexdigest())
    return {
        "action": value.action,
        "wire_body_b64": base64.b64encode(value.wire_body).decode(),
        "wire_digest": value.wire_digest,
    }


class FakeClient:
    def __init__(self):
        self.handler = None
        self.active = False
        self.closed = False
        self.sent = []
        self.status = {"online": True, "good": True}

    async def get_login_info(self, *, timeout):
        return {"self_id": "123456789", "nickname": "Forest"}

    async def get_status(self, *, timeout):
        return self.status

    def activate_events(self, handler):
        assert self.handler is None
        self.handler = handler
        self.active = True

    async def freeze_events(self):
        self.active = False

    def resume_events(self):
        assert self.handler is not None and not self.closed
        self.active = True

    async def emit(self, body):
        assert self.active
        await self.handler(body)

    async def send(self, prepared, *, timeout):
        self.sent.append((prepared, timeout))
        return SendResult("SENT", message_id="42")

    @property
    def is_closed(self):
        return self.closed

    async def aclose(self):
        self.active = False
        self.closed = True


async def test_onebot_transport_owns_ingress_send_fences_and_resource_lifecycle():
    identity = SimpleNamespace(
        component_id="adapter.onebot",
        generation_id="generation:onebot:1",
        graph_id="graph:onebot:1",
        graph_revision=7,
    )
    binding = {
        "binding_ref": "binding/main",
        "binding_epoch": 3,
        "self_id": "123456789",
        "credential_ref": "secret://onebot/main",
        "websocket_url": "ws://onebot.invalid/",
        "timeout_seconds": 15,
        "event_capacity": 32,
        "pending_capacity": 8,
        "enabled": True,
    }
    configuration = {
        "protocol": "forest.adapter.onebot/2",
        "bindings": [binding],
    }
    prepared = _prepared()
    attempt = {
        "state": "DISPATCHING",
        "attempt_id": "attempt:1",
        "executor_component_id": "adapter.onebot",
        "executor_generation_id": identity.generation_id,
        "fence_kind": "runtime.graph",
        "fence_id": identity.graph_id,
        "fence_epoch": identity.graph_revision,
        "wire_payload_digest": "sha256:" + prepared["wire_digest"],
    }
    effect = {
        "state": "DISPATCHING",
        "effect_kind": "onebot.send",
        "target_kind": "group",
        "target_id": "933179609",
    }

    class Effects:
        async def load(self, operation_id):
            assert operation_id == "effect:1"
            return {"effect": effect, "attempt": attempt}

    verified = []

    class Runtime:
        async def verify_generation_member(self, **values):
            verified.append(values)

    resolved = []

    async def credential_resolver(secret_ref, revision):
        resolved.append((secret_ref, revision))
        return "token"

    client = FakeClient()

    async def client_factory(*_args, **_kwargs):
        return client

    declaration = {
        "name": "adapter.onebot.transport.send",
        "kind": "onebot.websocket-v11",
        "ingress": {"name": "adapter.onebot.transport.frame"},
    }
    driver = OneBotTransportEffectDriver(
        configuration,
        11,
        identity,
        declaration,
        effects=Effects(),
        runtime=Runtime(),
        credential_resolver=credential_resolver,
        client_factory=client_factory,
    )
    frames = []

    async def ingest(payload, **identity_values):
        frames.append((payload, identity_values))
        return {"accepted": True}

    driver.bind_ingress(ingest)
    await driver.start()
    transport_task = asyncio.create_task(asyncio.Event().wait())
    client.owned_tasks = lambda: (transport_task,)
    account_status_task = driver._account_status_task
    assert account_status_task is not None
    assert driver.owned_tasks() == (transport_task, account_status_task)
    generation = object.__new__(PackageGenerationAdapter)
    generation._owned_work = None
    generation._effect_drivers = {"adapter.onebot.transport.send": driver}
    assert generation.owned_tasks() == (transport_task, account_status_task)
    assert resolved == [("secret://onebot/main", 11)]
    assert not client.active

    driver.activate_ingress()
    await client.emit(b'{"post_type":"message","message_id":1}')
    frame, call = frames[0]
    assert frame["binding_ref"] == "binding/main"
    assert frame["adapter_generation_id"] == identity.generation_id
    assert frame["graph_id"] == identity.graph_id
    assert call == {
        "operation_id": frame["frame_id"],
        "causal_root_kind": "adapter.platform-frame",
        "causal_root_id": frame["frame_id"],
    }

    await driver.freeze_ingress()
    assert not client.active
    await driver.quiesce()
    await driver.resume()
    assert not client.active
    driver.activate_ingress()
    assert client.active
    await client.emit(b'{"post_type":"message","message_id":1}')
    assert frames[1] == frames[0]

    transport = driver._bindings["binding/main"]
    payload = {
        "protocol": "forest.onebot-send/2",
        "effect_operation_id": "effect:1",
        "attempt_id": "attempt:1",
        "binding_ref": "binding/main",
        "binding_epoch": 3,
        "connection_epoch": transport.connection_epoch,
        "client_incarnation": transport.client_incarnation,
        "adapter_generation_id": identity.generation_id,
        "graph_id": identity.graph_id,
        "graph_revision": identity.graph_revision,
        "transport_digest": transport.transport_digest,
        "prepared": prepared,
        "timeout_seconds": 20,
    }
    result = await driver.invoke(
        payload,
        scope=SimpleNamespace(
            component_id="adapter.onebot", generation_id=identity.generation_id
        ),
        invocation=SimpleNamespace(operation_id="adapter-dispatch:1"),
    )
    assert result == {
        "protocol": "forest.onebot-send-result/2",
        "status": "SENT",
        "message_id": "42",
        "error_code": None,
    }
    assert verified == [
        {
            "component_id": "adapter.onebot",
            "generation_id": identity.generation_id,
        }
    ]
    assert len(client.sent) == 1

    stale = {**payload, "connection_epoch": payload["connection_epoch"] + 1}
    with pytest.raises(ForestError) as caught:
        await driver.invoke(
            stale,
            scope=SimpleNamespace(
                component_id="adapter.onebot", generation_id=identity.generation_id
            ),
            invocation=SimpleNamespace(operation_id="adapter-dispatch:2"),
        )
    assert caught.value.code == "ONEBOT_EFFECT_FENCE_MISMATCH"
    assert len(client.sent) == 1

    await driver.close()
    assert client.closed and account_status_task.done()
    transport_task.cancel()
    await asyncio.gather(transport_task, return_exceptions=True)


async def test_onebot_transport_rejects_offline_account_before_activation():
    client = FakeClient()
    client.status = {"online": False, "good": True}

    async def client_factory(*_args, **_kwargs):
        return client

    async def credential_resolver(*_args):
        return "token"

    driver = OneBotTransportEffectDriver(
        {
            "bindings": [{
                "enabled": True,
                "binding_ref": "binding/main",
                "self_id": "123456789",
                "credential_ref": "secret://onebot/main",
                "websocket_url": "ws://onebot.invalid/",
                "timeout_seconds": 15,
                "event_capacity": 32,
                "pending_capacity": 8,
                "binding_epoch": 1,
            }],
        },
        1,
        SimpleNamespace(
            component_id="adapter.onebot",
            generation_id="generation:onebot:offline",
            graph_id="graph:onebot:offline",
            graph_revision=1,
        ),
        {
            "name": "adapter.onebot.transport.send",
            "kind": "onebot.websocket-v11",
            "ingress": {"name": "adapter.onebot.transport.frame"},
        },
        effects=object(),
        runtime=object(),
        credential_resolver=credential_resolver,
        client_factory=client_factory,
    )
    driver.bind_ingress(lambda *_args, **_kwargs: None)
    with pytest.raises(ForestError) as caught:
        await driver.start()
    assert caught.value.code == "ONEBOT_ACCOUNT_NOT_READY"
    assert client.closed
    assert driver._bindings == {}


async def test_onebot_transport_detects_account_offline_on_open_socket(monkeypatch):
    monkeypatch.setattr(
        "forest.onebot_transport_effect_driver._ACCOUNT_STATUS_POLL_SECONDS", 0.01
    )
    client = FakeClient()
    client.owned_tasks = lambda: ()

    async def client_factory(*_args, **_kwargs):
        return client

    async def credential_resolver(*_args):
        return "token"

    driver = OneBotTransportEffectDriver(
        {
            "bindings": [{
                "enabled": True,
                "binding_ref": "binding/main",
                "self_id": "123456789",
                "credential_ref": "secret://onebot/main",
                "websocket_url": "ws://onebot.invalid/",
                "timeout_seconds": 15,
                "event_capacity": 32,
                "pending_capacity": 8,
                "binding_epoch": 1,
            }],
        },
        1,
        SimpleNamespace(
            component_id="adapter.onebot",
            generation_id="generation:onebot:runtime-offline",
            graph_id="graph:onebot:runtime-offline",
            graph_revision=1,
        ),
        {
            "name": "adapter.onebot.transport.send",
            "kind": "onebot.websocket-v11",
            "ingress": {"name": "adapter.onebot.transport.frame"},
        },
        effects=object(),
        runtime=object(),
        credential_resolver=credential_resolver,
        client_factory=client_factory,
    )
    driver.bind_ingress(lambda *_args, **_kwargs: None)
    await driver.start()
    monitor, = driver.owned_tasks()
    assert not client.closed
    client.status = {"online": False, "good": True}
    with pytest.raises(ForestError) as caught:
        await asyncio.wait_for(monitor, 1)
    assert caught.value.code == "ONEBOT_ACCOUNT_NOT_READY"
    await driver.close()
    assert client.closed
