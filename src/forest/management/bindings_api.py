"""Web-managed OneBot accounts, configuration and durable conversation diagnostics."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import uuid4

from fastapi import FastAPI, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator

from forest.errors import ForestError
from forest.management.binding_catalog import BindingCatalog
from forest.packages import digest_json
from forest.security import digest_token
from forest.storage.delivery_payloads import UNAVAILABLE, DeliveryContentStore


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Registration(Body):
    self_id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    display_name: str = Field(min_length=1, max_length=120)


class BindingValues(Body):
    enabled: bool = False
    transport: Literal["http-post", "forward-websocket"] = "http-post"
    api_base_url: str = Field(default="http://127.0.0.1:5700", max_length=2048)
    websocket_url: str = Field(default="ws://127.0.0.1:8080/", max_length=2048)
    response_mode: Literal["disabled", "component", "agent"] = "disabled"
    response_template: str = Field(default="{sender}，收到：{text}", min_length=1, max_length=2000)
    trigger_mentions: bool = True
    trigger_prefix: str = Field(default="/chat", max_length=64)
    private_trigger: bool = True
    admin_user_ids: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("api_base_url")
    @classmethod
    def api_origin(cls, value):
        try:
            parts = urlsplit(value)
            if (parts.scheme not in {"http", "https"} or not parts.hostname or parts.username
                    or parts.password or parts.query or parts.fragment or parts.path not in {"", "/"}
                    or any(ord(c) < 33 for c in value)):
                raise ValueError()
            if parts.port is not None and not 1 <= parts.port <= 65535:
                raise ValueError()
        except ValueError:
            raise ValueError("Use an HTTP API origin without credentials, path or query") from None
        return value.rstrip("/")

    @field_validator("websocket_url")
    @classmethod
    def websocket_endpoint(cls, value):
        try:
            parts = urlsplit(value)
            if (
                parts.scheme not in {"ws", "wss"}
                or not parts.hostname
                or parts.username
                or parts.password
                or parts.query
                or parts.fragment
                or parts.path not in {"", "/"}
                or any(ord(c) <= 32 or ord(c) == 127 for c in value)
                or "\\" in value
            ):
                raise ValueError()
            if parts.port is not None and not 1 <= parts.port <= 65535:
                raise ValueError()
            parts.hostname.encode("idna")
        except (ValueError, UnicodeError):
            raise ValueError(
                "Use a Universal ws:// or wss:// endpoint without credentials or query"
            ) from None
        return value.rstrip("/") + "/"

    @field_validator("admin_user_ids")
    @classmethod
    def account_ids(cls, value):
        if any(not item.isascii() or not item.isdecimal() or not 1 <= len(item) <= 20
               or item.startswith("0") for item in value):
            raise ValueError("Administrator IDs must be decimal platform user IDs")
        return sorted(set(value))


class BindingPlan(Body):
    expected_revision: int = Field(ge=0, strict=True)
    values: BindingValues
    access_token: str | None = Field(default=None, min_length=1, max_length=4096)
    webhook_secret: str | None = Field(default=None, min_length=16, max_length=4096)

    @field_validator("access_token")
    @classmethod
    def api_token(cls, value):
        if value is not None and any(ord(c) < 33 or ord(c) > 126 for c in value):
            raise ValueError("Use an ASCII token without whitespace")
        return value


class Apply(Body):
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class Resolution(Body):
    expected_revision: int = Field(ge=0, strict=True)
    decision: Literal["SENT", "NOT_SENT", "SKIP"]
    reason: str = Field(min_length=1, max_length=2000)


def operational(row: dict, platform) -> dict:
    """A durable old ONLINE fact alone is not a live connection after restart."""
    result = {**row, "persisted_state": row["state"]}
    connection = platform.connections.get(row["binding_id"])
    try:
        platform.kernel._authority()
        current_host = True
    except ForestError:
        current_host = False
    if row["state"] in {"ONLINE", "DRAINING"} and (
        not current_host or connection is None or not connection.accepting
        or connection.row["binding_epoch"] != row["binding_epoch"]
        or connection.row["connection_epoch"] != row["connection_epoch"]
        or connection.row["effective_revision"] != row["effective_revision"]
    ):
        result["state"] = "OFFLINE"
    return result


def install_binding_routes(app: FastAPI, runtime: Any):
    def services():
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        return BindingCatalog(runtime.catalog), runtime.fleet, runtime.authority

    def key(request):
        value = request.headers.get("idempotency-key", "")
        if not 1 <= len(value) <= 128:
            raise ForestError("IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422)
        return value

    async def target(request, bot_id, *, write=False):
        session = await runtime.principal(request, write=write)
        who = {"subject_id": session["account_id"],
               "session_token_hash": digest_token(request.cookies["forest_session"])}
        registry, fleet, authority = services()
        await runtime.catalog.get(bot_id=bot_id, **who)
        bot = await fleet.attach(bot_id)
        return who, bot, registry, authority

    def operation(request, who, bot_id, action, *, write=True):
        return "web:" + digest_json({"subject_id": who["subject_id"], "bot_id": bot_id,
                                    "action": action, "key": key(request) if write else str(uuid4())})

    async def ticket(who, bot, authority, op, action, payload, *, plan_digest=""):
        return await authority.issue(**who, bot_id=bot.store.bot_id, action=action,
            operation_id=op, payload=payload, plan_digest=plan_digest,
            policy_revision=(await bot.store.runtime())["policy_revision"])

    async def finish_registration(who, bot, registry, authority, reserved):
        signed = await ticket(who, bot, authority, reserved["operation_id"], "binding.create", reserved["payload"])
        result = await bot.platform.bindings.create(signed, reserved["operation_id"], reserved["payload"])
        if result is None or bot.control.is_unavailable(result):
            return result
        await registry.registered(bot_id=bot.store.bot_id, binding_id=reserved["binding_id"], **who)
        return {**result, "registration_stage": "REGISTERED"}

    @app.post("/management/v1/bots/{bot_id}/bindings", status_code=201)
    async def create_binding(bot_id: str, body: Registration, request: Request):
        who, bot, registry, authority = await target(request, bot_id, write=True)
        op = operation(request, who, bot_id, "binding.create")
        reserved = await registry.reserve(bot_id=bot_id, **body.model_dump(), operation_id=op, **who)
        return await finish_registration(who, bot, registry, authority, reserved)

    @app.post("/management/v1/bots/{bot_id}/bindings/{binding_id}/registration")
    async def resume_registration(bot_id: str, binding_id: str, request: Request):
        who, bot, registry, authority = await target(request, bot_id, write=True)
        reserved = await registry.registration(bot_id=bot_id, binding_id=binding_id, **who)
        return await finish_registration(who, bot, registry, authority, reserved)

    @app.get("/management/v1/bots/{bot_id}/bindings")
    async def list_bindings(bot_id: str, request: Request):
        who, bot, registry, authority = await target(request, bot_id)
        op = operation(request, who, bot_id, "binding.read", write=False)
        signed = await ticket(who, bot, authority, op, "binding.read", {})
        rows = {r["binding_id"]: operational(r, bot.platform)
                for r in await bot.platform.bindings.list(signed, op)}
        reservations = await registry.list(bot_id=bot_id, **who)
        return {"bindings": [{**reserved, **rows.get(reserved["binding_id"], {}),
                              "registration_stage": reserved["stage"]} for reserved in reservations],
                "errors": bot.platform.errors}

    @app.get("/management/v1/bots/{bot_id}/bindings/{binding_id}/configuration")
    async def binding_configuration(bot_id: str, binding_id: str, request: Request):
        who, bot, _, authority = await target(request, bot_id)
        op = operation(request, who, bot_id, "binding.read", write=False)
        signed = await ticket(who, bot, authority, op, "binding.read", {"binding_id": binding_id})
        row = await bot.platform.bindings.read(signed, op, binding_id)
        return {**operational(row, bot.platform), "webhook_url": runtime.settings.public_origin + "/adapters/onebot/v11/" + binding_id,
                "defaults": BindingValues().model_dump()}

    @app.post("/management/v1/bots/{bot_id}/bindings/{binding_id}/change-plans")
    async def plan_binding(bot_id: str, binding_id: str, body: BindingPlan, request: Request):
        who, bot, _, authority = await target(request, bot_id, write=True)
        op = operation(request, who, bot_id, "binding.plan")
        bindings = bot.platform.bindings
        previous = await bindings.credentials(bot.kernel._authority(), binding_id)
        secret = json.loads(runtime.catalog.vault.decrypt(previous)) if previous else {}
        if body.access_token is not None:
            secret["access_token"] = body.access_token
        if body.webhook_secret is not None:
            secret["webhook_secret"] = body.webhook_secret
        missing_access = not secret.get("access_token")
        missing_webhook = (
            body.values.transport == "http-post" and not secret.get("webhook_secret")
        )
        if body.values.enabled and (missing_access or missing_webhook):
            detail = (
                "Set the API token and webhook signing secret"
                if body.values.transport == "http-post"
                else "Set the API token"
            )
            raise ForestError("BINDING_CREDENTIALS_REQUIRED", detail, 422)
        candidate = runtime.catalog.vault.encrypt(json.dumps(secret, sort_keys=True))
        credential_input = {"expected_revision": body.expected_revision,
                            "access_token": body.access_token, "webhook_secret": body.webhook_secret}
        cipher = await bindings.stage_credentials(bot.kernel._authority(), binding_id, who["subject_id"],
                                                  op, digest_json(credential_input), candidate)
        payload = {"binding_id": binding_id, "expected_revision": body.expected_revision,
                   "values": body.values.model_dump(),
                   "credentials_changed": body.access_token is not None or body.webhook_secret is not None,
                   "secret_digest": hashlib.sha256(cipher.encode()).hexdigest()}
        signed = await ticket(who, bot, authority, op, "binding.plan", payload)
        return await bindings.plan(signed, op, payload, cipher)

    @app.post("/management/v1/bots/{bot_id}/bindings/{binding_id}/management-requests", status_code=202)
    async def apply_binding(bot_id: str, binding_id: str, body: Apply, request: Request):
        who, bot, _, authority = await target(request, bot_id, write=True)
        op = operation(request, who, bot_id, "binding.apply")
        payload = {"binding_id": binding_id, **body.model_dump()}
        signed = await ticket(who, bot, authority, op, "binding.apply", payload, plan_digest=body.plan_digest)
        return await bot.platform.bindings.apply(signed, op, payload)

    @app.post("/adapters/onebot/v11/{binding_id}")
    async def onebot_event(binding_id: str, request: Request):
        if len(binding_id) > 128:
            raise ForestError("BINDING_NOT_FOUND", "Binding is unavailable", 404)
        registry, fleet, _ = services()
        bot = await fleet.attach(await registry.resolve(binding_id))
        await bot.platform.receive(binding_id, await request.body(), request.headers.get("x-signature"))
        # OneBot treats this as an empty quick-operation response. ACK follows commit.
        return {}

    @app.get("/management/v1/bots/{bot_id}/conversations")
    async def conversations(bot_id: str, request: Request):
        who, bot, _, authority = await target(request, bot_id)
        op = operation(request, who, bot_id, "message.read", write=False)
        signed = await ticket(who, bot, authority, op, "message.read", {})
        async with bot.store.pool.connection() as conn, conn.transaction():
            claims = await bot.control._guard(conn, signed, "message.read", op, {})
            rows = await (await conn.execute(
                "SELECT conversation_id,binding_id,group_source_id,platform,channel_kind,channel_id,thread_id,"
                "next_ingress_seq,classified_through_seq,context_applied_seq,context_revision,next_response_seq,"
                "(SELECT count(*) FROM public.bot_conversation_triggers t WHERE t.bot_id=c.bot_id "
                "AND t.conversation_id=c.conversation_id AND t.status IN ('PENDING','CLAIMED')) AS pending_triggers "
                "FROM public.bot_conversations c WHERE bot_id=%s ORDER BY updated_at DESC LIMIT 100", (bot_id,),
            )).fetchall()
            await bot.control._time(conn, claims)
            return {"conversations": rows}

    @app.get("/management/v1/bots/{bot_id}/delivery")
    async def delivery_status(bot_id: str, request: Request):
        who, bot, _, authority = await target(request, bot_id)
        op = operation(request, who, bot_id, "message.read", write=False)
        payload = {"resource": "delivery"}
        signed = await ticket(who, bot, authority, op, "message.read", payload)
        async with bot.store.pool.connection() as conn, conn.transaction():
            claims = await bot.control._guard(conn, signed, "message.read", op, payload)
            rows = await (await conn.execute(
                "SELECT i.intent_id,i.binding_id,i.send_lane,i.reply_route,i.operation_id,i.root_operation_id,"
                "i.created_at,p.part_index,p.part_operation_id,p.payload_id,p.payload_digest,p.state,p.revision,"
                "p.attempt_id,p.attempt_count,p.error_code,p.receipt,p.updated_at,"
                "a.permitted_at,a.started_at,a.deadline_at,a.observed_at,a.wire_digest FROM public.bot_send_intents i "
                "JOIN public.bot_send_parts p USING(bot_id,intent_id) "
                "LEFT JOIN public.bot_send_attempts a ON a.bot_id=p.bot_id AND a.attempt_id=p.attempt_id "
                "WHERE i.bot_id=%s "
                "ORDER BY i.created_at DESC,p.part_index LIMIT 100", (bot_id,),
            )).fetchall()
            content = DeliveryContentStore(bot.store)
            clock = await content.payloads.lock_in(conn)
            for row in rows:
                try:
                    row["payload"] = await content.read_part_in(
                        conn, row, expected_deletion_epoch=clock["deletion_epoch"],
                    )
                    row["payload_state"] = "ACTIVE"
                except ForestError as exc:
                    if exc.code not in UNAVAILABLE:
                        raise
                    row["payload"], row["payload_state"] = None, "UNAVAILABLE"
                row.pop("payload_id")
            await bot.control._time(conn, claims)
            return {"parts": rows}

    @app.post("/management/v1/bots/{bot_id}/delivery/{intent_id}/{part_index}/resolution")
    async def reconcile_delivery(bot_id: str, intent_id: str, part_index: int, body: Resolution, request: Request):
        who, bot, _, authority = await target(request, bot_id, write=True)
        op = operation(request, who, bot_id, "delivery.reconcile")
        payload = {"intent_id": intent_id, "part_index": part_index, **body.model_dump()}
        signed = await ticket(who, bot, authority, op, "delivery.reconcile", payload)
        async with bot.store.pool.connection() as conn, conn.transaction():
            claims = await bot.control._guard(conn, signed, "delivery.reconcile", op, payload)
            previous = await bot.control._replay(conn, claims)
            if bot.control.is_replay(previous):
                await bot.control._time(conn, claims)
                return previous
            original = await bot.control.admitted_input_in(conn, claims)
            result = await bot.platform.delivery.reconcile_in(conn, bot.kernel._authority(), intent_id, part_index,
                operation_id=op, input_parent_ids=(original.payload_id,), **body.model_dump())
            await bot.control._record(conn, claims, result)
            await bot.control._time(conn, claims)
            return result
