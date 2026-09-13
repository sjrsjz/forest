import asyncio

import pytest

from forest.errors import ForestError
from forest.sdk import (
    CallScope,
    Plugin,
    PluginContext,
    _current_call,
    declared_subscriptions,
    subscribe,
)


async def unused_state(*_args):
    raise AssertionError("state RPC is not used")


async def test_event_handle_is_call_scoped_and_forwards_fixed_request():
    requests = []

    async def publish(call_id, payload):
        requests.append((call_id, payload))
        return {"event_id": "event-a"}

    root = PluginContext({}, unused_state, event_rpc=publish)
    with pytest.raises(ForestError) as missing:
        await root.events.publish("topic.a", {}, contract="1.0", operation_id="event-op")
    assert missing.value.code == "CALL_CONTEXT_REQUIRED"
    scope = CallScope("call-a", asyncio.get_running_loop().time() + 5, "root-op")
    token = _current_call.set(scope)
    call = root._for_call(scope)
    try:
        result = await call.events.publish(
            "topic.a", {"value": 1}, contract="1.0", operation_id="event-op"
        )
        assert result == {"event_id": "event-a"}
        assert requests == [("call-a", {
            "topic": "topic.a", "payload": {"value": 1},
            "contract": "1.0", "operation_id": "event-op",
        })]
    finally:
        scope.active = False
        _current_call.reset(token)
    with pytest.raises(ForestError) as stale:
        await call.events.publish("topic.a", {}, contract="1.0", operation_id="late")
    assert stale.value.code == "CALL_CONTEXT_REQUIRED"


def test_subscription_declaration_has_stable_identity_and_exact_signature():
    class Valid(Plugin):
        @subscribe("projection.a", "topic.a", "1.2")
        async def consume(self, event, ctx):
            return event, ctx

    declarations = declared_subscriptions(Valid())
    assert declarations["projection.a"][0] == {
        "subscriber_id": "projection.a", "topic": "topic.a", "contract": "1.2"
    }

    class Invalid(Plugin):
        @subscribe("projection.a", "topic.a", "1.2")
        async def consume(self, payload):
            return payload

    with pytest.raises(ForestError) as error:
        declared_subscriptions(Invalid())
    assert error.value.code == "SUBSCRIPTION_DECLARATION_INVALID"
