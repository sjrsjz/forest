"""OneBot protocol ingress owned by the adapter host boundary."""

from typing import Any

from fastapi import FastAPI, Request

from forest.errors import ForestError
from forest.management.binding_catalog import BindingCatalog


def install_onebot_protocol_route(app: FastAPI, runtime: Any):
    def services():
        if runtime.catalog is None or runtime.fleet is None:
            raise ForestError(
                "BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503
            )
        return BindingCatalog(runtime.catalog), runtime.fleet

    @app.post("/adapters/onebot/v11/{binding_id}")
    async def onebot_event(binding_id: str, request: Request):
        if len(binding_id) > 128:
            raise ForestError("BINDING_NOT_FOUND", "Binding is unavailable", 404)
        registry, fleet = services()
        bot = await fleet.attach(await registry.resolve(binding_id))
        await bot.runtime.module("adapter.onebot").receive(
            binding_id, await request.body(), request.headers.get("x-signature")
        )
        # OneBot treats this as an empty quick-operation response. ACK follows commit.
        return {}
