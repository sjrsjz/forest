import base64
import binascii

from onebot_codec import parse_event, prepare_send

from forest.errors import ForestError
from forest.sdk import Plugin, service


class OneBotCodec(Plugin):
    @service(
        "onebot.codec.normalize",
        contract="1.0",
        input_schema="schemas/normalize.json",
        output_schema="schemas/normalize-result.json",
    )
    async def normalize(self, payload, ctx):
        try:
            body = base64.b64decode(payload["body_b64"], validate=True)
        except (binascii.Error, ValueError):
            raise ForestError(
                "ONEBOT_INVALID", "OneBot event body is not canonical base64", 422
            ) from None
        event = parse_event(body, self_id=payload["self_id"])
        return {"message": event.as_dict() if event is not None else None}

    @service(
        "onebot.codec.prepare-send",
        contract="1.0",
        input_schema="schemas/prepare.json",
        output_schema="schemas/prepare-result.json",
    )
    async def prepare_message(self, payload, ctx):
        prepared = prepare_send(
            payload["channel_kind"], payload["channel_id"], payload["segments"]
        )
        return {
            "action": prepared.action,
            "wire_body_b64": base64.b64encode(prepared.wire_body).decode("ascii"),
            "wire_digest": prepared.wire_digest,
        }
