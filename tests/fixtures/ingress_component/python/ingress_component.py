import asyncio

from forest.sdk import Plugin, service


class IngressComponent(Plugin):
    @service("fixture.transport.frame", semantics="at-most-once")
    async def frame(self, payload, ctx):
        await asyncio.sleep(payload.get("delay_ms", 0) / 1000)
        return {
            "frame_id": payload["frame_id"],
            "generation_id": ctx.identity["generation_id"],
        }
