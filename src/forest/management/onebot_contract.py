"""Module-owned OneBot management contract."""

from __future__ import annotations

import hashlib
import json
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from forest.errors import ForestError
from forest.management.binding_catalog import BindingCatalog
from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation
from forest.packages import digest_json
from forest.storage.delivery_payloads import UNAVAILABLE, DeliveryContentStore


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Registration(Body):
    self_id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    display_name: str = Field(min_length=1, max_length=120)


class BindingReference(Body):
    binding_id: str = Field(min_length=1, max_length=128)


class BindingValues(Body):
    enabled: bool = False
    transport: Literal["http-post", "forward-websocket"] = "http-post"
    api_base_url: str = Field(default="http://127.0.0.1:5700", max_length=2048)
    websocket_url: str = Field(default="ws://127.0.0.1:8080/", max_length=2048)
    response_mode: Literal["disabled", "component", "agent"] = "disabled"
    response_template: str = Field(
        default="{sender}，收到：{text}", min_length=1, max_length=2000
    )
    trigger_mentions: bool = True
    trigger_prefix: str = Field(default="/chat", max_length=64)
    private_trigger: bool = True
    admin_user_ids: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("api_base_url")
    @classmethod
    def api_origin(cls, value):
        try:
            parts = urlsplit(value)
            if (
                parts.scheme not in {"http", "https"}
                or not parts.hostname
                or parts.username
                or parts.password
                or parts.query
                or parts.fragment
                or parts.path not in {"", "/"}
                or any(ord(char) < 33 for char in value)
            ):
                raise ValueError()
            if parts.port is not None and not 1 <= parts.port <= 65535:
                raise ValueError()
        except ValueError:
            raise ValueError(
                "Use an HTTP API origin without credentials, path or query"
            ) from None
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
                or any(ord(char) <= 32 or ord(char) == 127 for char in value)
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
        if any(
            not item.isascii()
            or not item.isdecimal()
            or not 1 <= len(item) <= 20
            or item.startswith("0")
            for item in value
        ):
            raise ValueError("Administrator IDs must be decimal platform user IDs")
        return sorted(set(value))


class BindingPlan(Body):
    binding_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)
    values: BindingValues
    access_token: str | None = Field(default=None, min_length=1, max_length=4096)
    webhook_secret: str | None = Field(default=None, min_length=16, max_length=4096)

    @field_validator("access_token")
    @classmethod
    def api_token(cls, value):
        if value is not None and any(ord(char) < 33 or ord(char) > 126 for char in value):
            raise ValueError("Use an ASCII token without whitespace")
        return value


class BindingApply(Body):
    binding_id: str = Field(min_length=1, max_length=128)
    expected_revision: int = Field(ge=0, strict=True)
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class DeliveryResolution(Body):
    intent_id: str = Field(min_length=1, max_length=128)
    part_index: int = Field(ge=0, strict=True)
    expected_revision: int = Field(ge=0, strict=True)
    decision: Literal["SENT", "NOT_SENT", "SKIP"]
    reason: str = Field(min_length=1, max_length=2000)


def operational(row: dict, platform) -> dict:
    """A durable old ONLINE fact alone is not a live connection after restart."""
    result = {**row, "persisted_state": row["state"]}
    connection = platform.module("adapter.onebot").connections.get(row["binding_id"])
    try:
        platform.kernel._authority()
        current_host = True
    except ForestError:
        current_host = False
    if row["state"] in {"ONLINE", "DRAINING"} and (
        not current_host
        or connection is None
        or not connection.accepting
        or connection.row["binding_epoch"] != row["binding_epoch"]
        or connection.row["connection_epoch"] != row["connection_epoch"]
        or connection.row["effective_revision"] != row["effective_revision"]
    ):
        result["state"] = "OFFLINE"
    return result


class OneBotManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation(
                    "binding.create", "action", "binding.create", Registration, self.create,
                    authorization_preparer=self.prepare_create,
                ),
                ModuleOperation(
                    "binding.registration.resume", "action", "binding.create",
                    BindingReference, self.resume, authorization_preparer=self.prepare_resume,
                ),
                ModuleOperation("bindings", "query", "binding.read", EmptyInput, self.bindings),
                ModuleOperation(
                    "binding.configuration", "query", "binding.read",
                    BindingReference, self.configuration,
                ),
                ModuleOperation(
                    "binding.plan", "action", "binding.plan", BindingPlan, self.plan,
                    authorization_preparer=self.prepare_plan,
                ),
                ModuleOperation(
                    "binding.apply", "action", "binding.apply", BindingApply, self.apply,
                ),
                ModuleOperation(
                    "conversations", "query", "message.read", EmptyInput, self.conversations
                ),
                ModuleOperation(
                    "delivery", "query", "message.read", EmptyInput, self.delivery,
                    authorization_payload=lambda _: {"resource": "delivery"},
                ),
                ModuleOperation(
                    "delivery.reconcile", "action", "delivery.reconcile",
                    DeliveryResolution, self.reconcile,
                ),
            )
        )

    @staticmethod
    def _catalog(request):
        return BindingCatalog(request.web.catalog)

    async def prepare_create(self, request, body):
        reserved = await self._catalog(request).reserve(
            bot_id=request.bot.store.bot_id, **body.model_dump(),
            operation_id=request.operation_id, **request.principal,
        )
        return reserved["payload"], reserved

    async def prepare_resume(self, request, body):
        reserved = await self._catalog(request).registration(
            bot_id=request.bot.store.bot_id, binding_id=body.binding_id, **request.principal
        )
        return reserved["payload"], reserved

    async def _finish_registration(self, request):
        reserved = request.prepared
        result = await request.module.bindings.create(
            request.ticket, request.operation_id, request.payload
        )
        if result is None or request.bot.control.is_unavailable(result):
            return result
        await self._catalog(request).registered(
            bot_id=request.bot.store.bot_id,
            binding_id=reserved["binding_id"],
            **request.principal,
        )
        return {**result, "registration_stage": "REGISTERED"}

    async def create(self, request, body):
        return await self._finish_registration(request)

    async def resume(self, request, body):
        return await self._finish_registration(request)

    async def bindings(self, request, body):
        rows = {
            row["binding_id"]: operational(row, request.bot.platform)
            for row in await request.module.bindings.list(request.ticket, request.operation_id)
        }
        reservations = await self._catalog(request).list(
            bot_id=request.bot.store.bot_id, **request.principal
        )
        return {
            "bindings": [
                {
                    **reserved,
                    **rows.get(reserved["binding_id"], {}),
                    "registration_stage": reserved["stage"],
                }
                for reserved in reservations
            ],
            "errors": dict(request.module.errors),
        }

    async def configuration(self, request, body):
        row = await request.module.bindings.read(
            request.ticket, request.operation_id, body.binding_id
        )
        return {
            **operational(row, request.bot.platform),
            "webhook_url": request.web.settings.public_origin
            + "/adapters/onebot/v11/"
            + body.binding_id,
            "defaults": BindingValues().model_dump(),
        }

    async def prepare_plan(self, request, body):
        previous = await request.module.bindings.credentials(
            request.bot.kernel._authority(), body.binding_id
        )
        secret = json.loads(request.web.catalog.vault.decrypt(previous)) if previous else {}
        if body.access_token is not None:
            secret["access_token"] = body.access_token
        if body.webhook_secret is not None:
            secret["webhook_secret"] = body.webhook_secret
        missing_access = not secret.get("access_token")
        missing_webhook = body.values.transport == "http-post" and not secret.get(
            "webhook_secret"
        )
        if body.values.enabled and (missing_access or missing_webhook):
            detail = (
                "Set the API token and webhook signing secret"
                if body.values.transport == "http-post"
                else "Set the API token"
            )
            raise ForestError("BINDING_CREDENTIALS_REQUIRED", detail, 422)
        candidate = request.web.catalog.vault.encrypt(json.dumps(secret, sort_keys=True))
        credential_input = {
            "expected_revision": body.expected_revision,
            "access_token": body.access_token,
            "webhook_secret": body.webhook_secret,
        }
        cipher = await request.module.bindings.stage_credentials(
            request.bot.kernel._authority(), body.binding_id,
            request.principal["subject_id"], request.operation_id,
            digest_json(credential_input), candidate,
        )
        payload = {
            "binding_id": body.binding_id,
            "expected_revision": body.expected_revision,
            "values": body.values.model_dump(),
            "credentials_changed": body.access_token is not None or body.webhook_secret is not None,
            "secret_digest": hashlib.sha256(cipher.encode()).hexdigest(),
        }
        return payload, cipher

    async def plan(self, request, body):
        return await request.module.bindings.plan(
            request.ticket, request.operation_id, request.payload, request.prepared
        )

    async def apply(self, request, body):
        return await request.module.bindings.apply(
            request.ticket, request.operation_id, request.payload
        )

    async def conversations(self, request, body):
        return await request.module.messages.list_conversations(
            request.ticket, request.operation_id
        )

    async def delivery(self, request, body):
        async with request.bot.store.pool.connection() as conn, conn.transaction():
            claims = await request.bot.control._guard(
                conn, request.ticket, "message.read", request.operation_id, request.payload
            )
            rows = await (await conn.execute(
                "SELECT i.intent_id,i.binding_id,i.send_lane,i.reply_route,i.operation_id,"
                "i.root_operation_id,i.created_at,p.part_index,p.part_operation_id,p.payload_id,"
                "p.payload_digest,p.state,p.revision,p.attempt_id,p.attempt_count,p.error_code,"
                "p.receipt,p.updated_at,a.permitted_at,a.started_at,a.deadline_at,a.observed_at,"
                "a.wire_digest FROM public.bot_send_intents i JOIN public.bot_send_parts p "
                "USING(bot_id,intent_id) LEFT JOIN public.bot_send_attempts a "
                "ON a.bot_id=p.bot_id AND a.attempt_id=p.attempt_id WHERE i.bot_id=%s "
                "ORDER BY i.created_at DESC,p.part_index LIMIT 100",
                (request.bot.store.bot_id,),
            )).fetchall()
            content = DeliveryContentStore(request.bot.store)
            clock = await content.payloads.lock_in(conn)
            for row in rows:
                try:
                    row["payload"] = await content.read_part_in(
                        conn, row, expected_deletion_epoch=clock["deletion_epoch"]
                    )
                    row["payload_state"] = "ACTIVE"
                except ForestError as exc:
                    if exc.code not in UNAVAILABLE:
                        raise
                    row["payload"], row["payload_state"] = None, "UNAVAILABLE"
                row.pop("payload_id")
            await request.bot.control._time(conn, claims)
            return {"parts": rows}

    async def reconcile(self, request, body):
        async with request.bot.store.pool.connection() as conn, conn.transaction():
            claims = await request.bot.control._guard(
                conn, request.ticket, "delivery.reconcile",
                request.operation_id, request.payload,
            )
            previous = await request.bot.control._replay(conn, claims)
            if request.bot.control.is_replay(previous):
                await request.bot.control._time(conn, claims)
                return previous
            original = await request.bot.control.admitted_input_in(conn, claims)
            result = await request.module.delivery.reconcile_in(
                conn, request.bot.kernel._authority(), body.intent_id, body.part_index,
                operation_id=request.operation_id,
                input_parent_ids=(original.payload_id,),
                expected_revision=body.expected_revision,
                decision=body.decision,
                reason=body.reason,
            )
            await request.bot.control._record(conn, claims, result)
            await request.bot.control._time(conn, claims)
            return result
