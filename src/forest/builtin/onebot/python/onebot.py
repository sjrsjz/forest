import base64

from forest.adapters.onebot import parse_event, prepare_send
from forest.sdk import Plugin, service


class OneBot(Plugin):
    @service("onebot.normalize")
    async def normalize(self, payload, ctx):
        event = parse_event(base64.b64decode(payload["body_b64"], validate=True),
                            self_id=payload["self_id"])
        return {"message": event.as_dict() if event is not None else None}

    @service("onebot.prepare")
    async def prepare_message(self, payload, ctx):
        prepared = prepare_send(payload["channel_kind"], payload["channel_id"], payload["segments"])
        return {"action": prepared.action, "wire_body_b64": base64.b64encode(prepared.wire_body).decode(),
                "wire_digest": prepared.wire_digest}
