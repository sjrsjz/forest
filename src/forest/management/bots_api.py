"""Web endpoints for directory jobs and target-authorized component configuration."""

from __future__ import annotations

from typing import Any, Literal
from uuid import uuid4

from fastapi import FastAPI, Request
from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateBot(Body):
    display_name: str = Field(min_length=1, max_length=120)
    mode: Literal["managed", "external"] = "managed"
    dsn: str | None = Field(default=None, max_length=4096)
    expected_revision: Literal[0] = 0


class Revision(Body):
    expected_revision: int = Field(ge=0, strict=True)


class RenameBot(Revision):
    display_name: str = Field(min_length=1, max_length=120)


class ConfigPlan(Revision):
    values: dict[str, Any]


class ConfigApply(Revision):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class Invoke(Body):
    service: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any]


def install_bot_routes(app: FastAPI, runtime: Any) -> None:
    def services():
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        return runtime.catalog, runtime.fleet, runtime.authority

    async def principal(request: Request, write: bool = False):
        session = await runtime.principal(request, write=write)
        return {"subject_id": session["account_id"],
                "session_token_hash": digest_token(request.cookies["forest_session"])}

    def key(request: Request) -> str:
        value = request.headers.get("idempotency-key", "")
        if not 1 <= len(value) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        return value

    async def target(request: Request, bot_id: str, *, write: bool):
        who = await principal(request, write)
        catalog, fleet, authority = services()
        await catalog.get(bot_id=bot_id, **who)
        bot = await fleet.attach(bot_id)
        return who, bot, fleet, authority

    async def ticket(request: Request, bot_id: str, action: str, payload: dict, *, write=True,
                     plan_digest=""):
        who, bot, fleet, authority = await target(request, bot_id, write=write)
        try:
            digest_json(payload)
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise ForestError("VALIDATION_FAILED", "Input must be finite, valid JSON", 422) from None
        operation_id = "web:" + digest_json({"subject_id": who["subject_id"], "bot_id": bot_id,
                                             "action": action, "key": key(request) if write else str(uuid4())})
        signed = await authority.issue(**who, bot_id=bot_id, action=action, operation_id=operation_id,
                                       payload=payload, plan_digest=plan_digest,
                                       policy_revision=(await bot.store.runtime())["policy_revision"])
        return bot, operation_id, signed

    @app.get("/management/v1/bots")
    async def list_bots(request: Request):
        who = await principal(request)
        catalog, fleet, _ = services()
        return {"bots": [{**row, "runtime": fleet.status(row["bot_id"])}
                         for row in await catalog.list(**who)],
                "managed_available": runtime.settings.managed_dsn is not None}

    @app.post("/management/v1/bots", status_code=202)
    async def create_bot(body: CreateBot, request: Request):
        who = await principal(request, True)
        return await services()[0].create(**body.model_dump(), **who, idempotency_key=key(request))

    @app.get("/management/v1/bots/{bot_id}")
    async def get_bot(bot_id: str, request: Request):
        who = await principal(request)
        catalog, fleet, _ = services()
        return {**await catalog.get(bot_id=bot_id, **who), "runtime": fleet.status(bot_id)}

    @app.post("/management/v1/bots/{bot_id}/rename")
    async def rename_bot(bot_id: str, body: RenameBot, request: Request):
        who = await principal(request, True)
        return await services()[0].rename(bot_id=bot_id, **body.model_dump(), **who,
                                          idempotency_key=key(request))

    @app.post("/management/v1/bots/{bot_id}/retry", status_code=202)
    async def retry_bot(bot_id: str, body: Revision, request: Request):
        who = await principal(request, True)
        return await services()[0].retry(bot_id=bot_id, **body.model_dump(), **who,
                                         idempotency_key=key(request))

    @app.get("/management/v1/provision-jobs/{job_id}")
    async def get_job(job_id: str, request: Request):
        who = await principal(request)
        return await services()[0].read_job(job_id=job_id, **who)

    @app.get("/management/v1/packages")
    async def packages(request: Request):
        await principal(request)
        return {"packages": services()[1].available_packages()}

    @app.get("/management/v1/bots/{bot_id}/components/{component_id}/configuration")
    async def configuration(bot_id: str, component_id: str, request: Request):
        bot, operation, signed = await ticket(request, bot_id, "config.read",
                                               {"component_id": component_id}, write=False)
        return {**await bot.control.read(signed, operation, component_id),
                "runtime": services()[1].status(bot_id)}

    @app.post("/management/v1/bots/{bot_id}/components/{component_id}/change-plans")
    async def config_plan(bot_id: str, component_id: str, body: ConfigPlan, request: Request):
        await principal(request, True)
        services()[1].validate(component_id, body.values)
        payload = {"component_id": component_id, **body.model_dump()}
        payload["values"]["grants"] = sorted(payload["values"]["grants"])
        bot, operation, signed = await ticket(request, bot_id, "config.plan", payload)
        return await bot.control.plan(signed, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/components/{component_id}/management-requests", status_code=202)
    async def config_apply(bot_id: str, component_id: str, body: ConfigApply, request: Request):
        payload = {"component_id": component_id, **body.model_dump()}
        bot, operation, signed = await ticket(request, bot_id, "config.apply", payload,
                                               plan_digest=body.plan_digest)
        return await bot.control.apply(signed, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/components/{component_id}/invoke")
    async def invoke(bot_id: str, component_id: str, body: Invoke, request: Request):
        payload = {"component_id": component_id, **body.model_dump()}
        bot, operation, signed = await ticket(request, bot_id, "service.invoke", payload)
        await bot.control.admit_invocation(signed, operation, payload)
        result = await bot.kernel.invoke(component_id, body.service, body.payload, operation_id=operation,
                                          expected_policy_revision=signed["claims"]["policy_revision"])
        return {"operation_id": operation, "result": result}
