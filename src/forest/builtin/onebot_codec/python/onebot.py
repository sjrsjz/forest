import base64
import binascii

from cq_memory import encode_message
from cq_presentation import prepare_template
from onebot_codec import parse_event

from forest.errors import ForestError
from forest.sdk import Plugin, service


class OneBotCodec(Plugin):
    @service(
        "onebot.codec.memory-message", semantics="read", contract="1.0",
        input_schema="schemas/memory-message.json",
        output_schema="schemas/memory-message-result.json",
    )
    async def memory_message(self, payload, ctx):
        return {"text": encode_message(payload["segments"])}

    @service(
        "onebot.codec.normalize",
        semantics="read",
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
        semantics="read",
        contract="2.0",
        input_schema="schemas/prepare.json",
        output_schema="schemas/prepare-result.json",
    )
    async def prepare_message(self, payload, ctx):
        return prepare_template(
            payload["channel_kind"], payload["channel_id"], payload["segments"]
        )
