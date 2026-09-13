"""Authenticated multimodal memory configuration and observed runtime status."""

from __future__ import annotations

from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forest.errors import ForestError
from forest.memory_settings import MemorySettings
from forest.packages import digest_json
from forest.security import digest_token


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MemoryPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: MemorySettings
    api_key: str | None = Field(default=None, min_length=1, max_length=4096)
    clear_api_key: bool = Field(default=False, strict=True)

    @field_validator("api_key")
    @classmethod
    def key(cls, value):
        if value is not None and any(ord(c) < 33 or ord(c) > 126 for c in value):
            raise ValueError("Use an ASCII API key without whitespace")
        return value

    @model_validator(mode="after")
    def replacement(self):
        if self.clear_api_key and self.api_key is not None:
            raise ValueError("Choose either a replacement key or clearing the key")
        return self


class Apply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class ForgetPlan(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    source_unit_ids: list[str] = Field(min_length=1, max_length=32)

    @field_validator("source_unit_ids")
    @classmethod
    def sources(cls, value):
        if len(set(value)) != len(value) or any(
            not isinstance(item, str)
            or not 1 <= len(item) <= 256
            or any(ord(char) < 32 for char in item)
            for item in value
        ):
            raise ValueError("Select distinct bounded source identifiers")
        return value


class ForgetApply(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class AssociationPlan(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    expected_revision: int = Field(ge=0, strict=True)
    active: bool = Field(strict=True)
    source_unit_ids: list[str] | None = Field(default=None, min_length=2, max_length=2)
    association_id: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def target(self):
        if self.active:
            if (
                self.association_id is not None
                or self.source_unit_ids is None
                or len(set(self.source_unit_ids)) != 2
                or any(
                    not 1 <= len(item) <= 256 or any(ord(char) < 32 for char in item)
                    for item in self.source_unit_ids
                )
            ):
                raise ValueError("Select exactly two distinct bounded sources")
        elif self.association_id is None or self.source_unit_ids is not None:
            raise ValueError("Retraction requires one association identity")
        return self


class AssociationApply(Body):
    expected_deletion_epoch: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


def install_memory_routes(app, runtime):
    async def target(request, bot_id, *, write=False):
        session = await runtime.principal(request, write=write)
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        who = {
            "subject_id": session["account_id"],
            "session_token_hash": digest_token(request.cookies["forest_session"]),
        }
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await runtime.fleet.attach(bot_id)
        return who, bot

    async def signed(request, bot, who, action, payload, *, write=True):
        key = request.headers.get("idempotency-key", "") if write else str(uuid4())
        if not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422
            )
        op = "web:" + digest_json(
            {
                "subject_id": who["subject_id"],
                "bot_id": bot.store.bot_id,
                "action": action,
                "key": key,
            }
        )
        ticket = await runtime.authority.issue(
            **who,
            bot_id=bot.store.bot_id,
            action=action,
            operation_id=op,
            payload=payload,
            plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )
        return ticket, op

    @app.get("/management/v1/bots/{bot_id}/memory/configuration")
    async def configuration(bot_id: str, request: Request):
        who, bot = await target(request, bot_id)
        ticket, op = await signed(request, bot, who, "memory.config.read", {}, write=False)
        return await bot.platform.memory.config.read(ticket, op)

    @app.post("/management/v1/bots/{bot_id}/memory/change-plans")
    async def plan(bot_id: str, body: MemoryPlan, request: Request):
        who, bot = await target(request, bot_id, write=True)
        secret = "" if body.clear_api_key else body.api_key
        changed = body.clear_api_key or secret is not None
        payload = {
            "expected_revision": body.expected_revision,
            "values": body.values.model_dump(),
            "credentials_changed": changed,
            "secret_digest": digest_json({"api_key": secret}),
        }
        ticket, op = await signed(request, bot, who, "memory.config.plan", payload)
        return await bot.platform.memory.config.plan(
            ticket,
            op,
            payload,
            secret_ciphertext=runtime.catalog.vault.encrypt(secret) if changed else None,
        )

    @app.post("/management/v1/bots/{bot_id}/memory/management-requests")
    async def apply(bot_id: str, body: Apply, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, op = await signed(request, bot, who, "memory.config.apply", payload)
        result = await bot.platform.memory.config.apply(ticket, op, payload)
        if result is not None and not bot.control.is_unavailable(result):
            bot.platform.memory.notify()
        return result

    @app.get("/management/v1/bots/{bot_id}/memory/status")
    async def status(bot_id: str, request: Request):
        who, bot = await target(request, bot_id)
        ticket, operation = await signed(request, bot, who, "memory.read", {}, write=False)
        return await bot.platform.memory.inspect(ticket, operation, {})

    @app.get("/management/v1/bots/{bot_id}/memory/sources")
    async def sources(bot_id: str, request: Request):
        if set(request.query_params) - {"after", "limit", "group_source_id"}:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Unknown source selector", 422)
        try:
            limit = int(request.query_params.get("limit", "50"))
        except ValueError:
            raise ForestError("MEMORY_MANAGEMENT_INVALID", "Source limit is invalid", 422) from None
        payload = {"limit": limit}
        for key in ("after", "group_source_id"):
            if key in request.query_params:
                payload[key] = request.query_params[key]
        who, bot = await target(request, bot_id)
        ticket, operation = await signed(
            request, bot, who, "memory.forget.read", payload, write=False
        )
        return await bot.platform.memory.administration.read(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/memory/forget-plans")
    async def forget_plan(bot_id: str, body: ForgetPlan, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "memory.forget.plan", payload)
        return await bot.platform.memory.administration.plan(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/memory/forget-requests")
    async def forget_apply(bot_id: str, body: ForgetApply, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "memory.forget.apply", payload)
        result = await bot.platform.memory.administration.apply(
            ticket, operation, payload, lease=bot.kernel._authority()
        )
        bot.platform.memory.notify()
        return result

    @app.get("/management/v1/bots/{bot_id}/memory/associations")
    async def associations(bot_id: str, request: Request):
        if set(request.query_params) - {"after", "limit"}:
            raise ForestError("MEMORY_ASSOCIATION_INVALID", "Unknown association selector", 422)
        try:
            limit = int(request.query_params.get("limit", "50"))
        except ValueError:
            raise ForestError(
                "MEMORY_ASSOCIATION_INVALID", "Association limit is invalid", 422
            ) from None
        payload = {"limit": limit}
        if "after" in request.query_params:
            payload["after"] = request.query_params["after"]
        who, bot = await target(request, bot_id)
        ticket, operation = await signed(
            request, bot, who, "memory.association.read", payload, write=False
        )
        return await bot.platform.memory.associations.read(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/memory/association-plans")
    async def association_plan(bot_id: str, body: AssociationPlan, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump(exclude_none=True)
        ticket, operation = await signed(request, bot, who, "memory.association.plan", payload)
        return await bot.platform.memory.associations.plan(ticket, operation, payload)

    @app.post("/management/v1/bots/{bot_id}/memory/association-requests")
    async def association_apply(bot_id: str, body: AssociationApply, request: Request):
        who, bot = await target(request, bot_id, write=True)
        payload = body.model_dump()
        ticket, operation = await signed(request, bot, who, "memory.association.apply", payload)
        result = await bot.platform.memory.associations.apply(
            ticket, operation, payload, lease=bot.kernel._authority()
        )
        bot.platform.memory.notify()
        return result
