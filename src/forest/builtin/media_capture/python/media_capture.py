import asyncio

from forest.errors import ForestError
from forest.sdk import Plugin, job, service

_FINAL_ACQUISITION_ERRORS = frozenset({
    "MEDIA_ENCODING_UNSUPPORTED",
    "MEDIA_IMAGE_INVALID",
    "MEDIA_PIXELS_EXCEEDED",
    "MEDIA_SIZE_EXCEEDED",
    "MEDIA_SIZE_INVALID",
    "MEDIA_URL_FORBIDDEN",
    "MEDIA_URL_INVALID",
})


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


class MediaCapture(Plugin):
    """Own capture state transitions; the host owns only bounded I/O effects."""

    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = _plain(config)
        if value.get("protocol") != "forest.media.capture/1":
            raise ForestError(
                "MEDIA_CONFIGURATION_INVALID", "Media protocol is unsupported", 422
            )
        self.max_attempts = value["max_attempts"]
        self.context_wait_seconds = value["context_wait_seconds"]

    @service(
        "media.context.resolve",
        semantics="read",
        contract="1.0",
        input_schema="schemas/context-resolve-input.json",
        output_schema="schemas/context-resolve-output.json",
    )
    async def resolve_context(self, payload, ctx):
        deadline = asyncio.get_running_loop().time() + self.context_wait_seconds
        while True:
            result = await ctx.queries.call("media.assets.resolve", payload)
            resolved = {item["media_ref"] for item in result["assets"]}
            requested = {item["media_ref"] for item in payload["items"]}
            if requested <= resolved or asyncio.get_running_loop().time() >= deadline:
                return result
            await asyncio.sleep(0.05)

    @job("media.capture")
    async def capture(self, task, ctx):
        capture = task["payload"]
        attempt = task["attempt"]
        for part in capture["parts"]:
            if part["part_kind"] != "IMAGE" or part["state"] != "PENDING":
                continue
            try:
                fetched = await ctx.services.call(
                    "media.image.fetch",
                    {
                        "protocol": "forest.media-image-fetch/1",
                        "media_ref": part["media_ref"],
                        "locator": part["locator"],
                        "provenance": {
                            "source_component_id": "adapter.onebot",
                            "source_kind": "onebot.media",
                            "source_id": capture["event_ref"],
                            "source_revision": 1,
                            "part_id": part["part_id"],
                            "source_trunk_id": capture["identity"].get("trunk_id"),
                            "conversation_ref": capture["conversation_ref"],
                            "binding_ref": capture["binding_ref"],
                            "author_subject_ref": capture["author_subject_ref"],
                            "occurred_at": capture["occurred_at"],
                        },
                    },
                    operation_id=ctx.operations.stable("fetch:" + part["part_id"]),
                    contract_major=2,
                    timeout=45,
                )
                await ctx.commands.call(
                    "media.publish-image",
                    {
                        "capture_id": capture["capture_id"],
                        "claim_token": capture["claim_token"],
                        "part_id": part["part_id"],
                        "media_ref": part["media_ref"],
                        "payload": fetched["payload"],
                        "media_type": fetched["media_type"],
                        "width": fetched["width"],
                        "height": fetched["height"],
                        "decoded_pixels": fetched["decoded_pixels"],
                        "byte_length": fetched["byte_length"],
                    },
                    operation_id=ctx.operations.stable("publish:" + part["part_id"]),
                    contract_major=2,
                )
            except ForestError as error:
                await ctx.commands.call(
                    "media.fail-part",
                    {
                        "capture_id": capture["capture_id"],
                        "claim_token": capture["claim_token"],
                        "part_id": part["part_id"],
                        "error_code": error.code,
                        "final": (
                            error.code in _FINAL_ACQUISITION_ERRORS
                            or attempt >= self.max_attempts
                        ),
                    },
                    operation_id=ctx.operations.stable("fail:" + part["part_id"]),
                )

        current = await ctx.queries.call(
            "media.capture.current",
            {"capture_id": capture["capture_id"], "claim_token": capture["claim_token"]},
            contract_major=2,
        )
        if any(part["state"] == "PENDING" for part in current["parts"]):
            return {"disposition": "RETRY", "memory_published": False}

        ready = [part for part in current["parts"] if part["state"] == "READY"]
        for part in ready:
            if part["part_kind"] != "IMAGE":
                continue
            try:
                await ctx.services.call(
                    "conversation.media-cost.record",
                    {
                        "conversation_id": current["conversation_ref"],
                        "event_id": current["event_ref"],
                        "ingress_seq": current["ingress_seq"],
                        "part_id": part["part_id"],
                        "part_index": part["part_index"],
                        "media_ref": part["media_ref"],
                        "media_revision": 1,
                        "payload": part["payload"],
                        "byte_length": part["byte_length"],
                        "width": part["width"],
                        "height": part["height"],
                        "decoded_pixels": part["decoded_pixels"],
                    },
                    operation_id=ctx.operations.stable(
                        "conversation-media-cost:" + part["part_id"]
                    ),
                    contract_major=2,
                    timeout=30,
                )
            except ForestError as error:
                if error.status >= 500 or error.code in {
                    "RUNTIME_SERVICE_ROUTE_STALE", "RUNTIME_GENERATION_NOT_ACTIVE",
                }:
                    return {"disposition": "RETRY", "memory_published": False}
                raise
        if not ready:
            await ctx.services.call(
                "memory.source.unavailable",
                {
                    "conversation_ref": current["conversation_ref"],
                    "event_ref": current["event_ref"],
                    "error_code": "MEDIA_CAPTURE_UNAVAILABLE",
                },
                operation_id=ctx.operations.stable("memory-unavailable"),
            )
            return {"disposition": "FAILED", "memory_published": True}

        await ctx.services.call(
            "memory.source.capture",
            {
                "operation_id": ctx.operations.stable("memory-capture"),
                "identity": current["identity"],
                "source_unit_id": current["source_unit_id"],
                "source_revision": current["source_revision"],
                "unit_kind": current["unit_kind"],
                "author_subject_ref": current["author_subject_ref"],
                "author": current["author"],
                "parts": [
                    dict(part)
                    for part in ready
                ],
                "occurrence": {
                    "occurrence_id": ctx.operations.stable("memory-occurrence"),
                    "binding_ref": current["binding_ref"],
                    "conversation_ref": current["conversation_ref"],
                    "event_ref": current["event_ref"],
                    "ingress_seq": current["ingress_seq"],
                    "observed_at": current["observed_at"],
                },
                "occurred_at": current["occurred_at"],
            },
            operation_id=ctx.operations.stable("memory-publish"),
            contract_major=2,
            timeout=30,
        )
        partial = any(part["state"] == "FAILED" for part in current["parts"])
        return {
            "disposition": "PARTIAL" if partial else "READY",
            "memory_published": True,
        }
