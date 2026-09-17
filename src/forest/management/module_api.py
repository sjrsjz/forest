"""Generic authenticated transport for module-owned management contracts."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Literal, Protocol
from uuid import uuid4

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, ValidationError

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token


class EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class ModuleOperation:
    operation_id: str
    kind: Literal["query", "action", "upload", "download"]
    authority: str
    input_model: type[BaseModel]
    handler: Callable[["ModuleRequest", BaseModel], Awaitable[Any]]
    authorization_payload: Callable[[BaseModel], dict] | None = None
    authorization_preparer: (
        Callable[["ModuleRequest", BaseModel], Awaitable[tuple[dict, Any]]] | None
    ) = None
    principal: Literal["authenticated", "admin"] = "authenticated"
    media_type: str | None = None
    max_bytes: int | None = None
    disposition: Literal["attachment", "inline"] | None = None
    success_status: int = 200

    def __post_init__(self) -> None:
        artifact = self.kind in {"upload", "download"}
        if (
            artifact and (self.media_type is None or self.max_bytes is None)
        ) or (
            not artifact and (self.media_type is not None or self.max_bytes is not None)
        ):
            raise ValueError("Artifact operations require media type and byte limit")
        if artifact and (
            type(self.max_bytes) is not int
            or not 1 <= self.max_bytes <= 64 * 1024 * 1024
            or re.fullmatch(
                r"[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*(?:; charset=utf-8)?",
                self.media_type,
            ) is None
        ):
            raise ValueError("Artifact operation byte limit is invalid")
        if self.kind == "download" and self.disposition not in {"attachment", "inline"}:
            raise ValueError("Download operations require a disposition")
        if self.kind != "download" and self.disposition is not None:
            raise ValueError("Only download operations declare a disposition")
        if type(self.success_status) is not int or self.success_status not in {200, 201, 202}:
            raise ValueError("Module operation success status is invalid")


@dataclass(frozen=True)
class ArtifactResult:
    content: bytes
    filename: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.content, bytes):
            raise TypeError("Artifact content must be bytes")
        if self.filename is not None and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", self.filename
        ) is None:
            raise ValueError("Artifact filename is invalid")


@dataclass(frozen=True)
class ModuleRequest:
    web: Any
    bot: Any
    module: Any
    principal: dict
    operation_id: str
    ticket: dict
    payload: dict
    prepared: Any = None
    scope_kind: str = "bot"
    scope_id: str = ""
    artifact: bytes | None = None


class ModuleManagementContract:
    protocol = "forest.module-management/1"

    def __init__(self, operations: tuple[ModuleOperation, ...]):
        self.operations = {operation.operation_id: operation for operation in operations}
        if len(self.operations) != len(operations):
            raise ValueError("Module management operation identities must be unique")

    def describe(self) -> dict:
        return {
            "protocol": self.protocol,
            "operations": [
                {
                    "operation_id": operation.operation_id,
                    "kind": operation.kind,
                    "authority": operation.authority,
                    **(
                        {"principal": operation.principal}
                        if operation.principal != "authenticated"
                        else {}
                    ),
                    **(
                        {
                            "artifact": {
                                "media_type": operation.media_type,
                                "max_bytes": operation.max_bytes,
                                **(
                                    {"disposition": operation.disposition}
                                    if operation.disposition is not None
                                    else {}
                                ),
                            }
                        }
                        if operation.kind in {"upload", "download"}
                        else {}
                    ),
                }
                for operation in self.operations.values()
            ],
        }

    def prepare(self, operation_id: str, kind: str, payload: dict):
        operation = self.operations.get(operation_id)
        if operation is None or operation.kind != kind:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND",
                "The module does not declare this management operation",
                404,
            )
        try:
            body = operation.input_model.model_validate(payload)
        except ValidationError as error:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "The module management input is invalid",
                422,
                {
                    "errors": error.errors(
                        include_url=False, include_input=False, include_context=False
                    )
                },
            ) from None
        authorized = (
            operation.authorization_payload(body)
            if operation.authorization_payload is not None
            else body.model_dump(exclude_none=True)
        )
        return operation, body, authorized

    async def invoke(self, request: ModuleRequest, operation: ModuleOperation, body: BaseModel):
        return await operation.handler(request, body)


@dataclass(frozen=True)
class ScopeTarget:
    kind: str
    identity: str
    resource: Any
    registry: Any
    provider: "ManagementScopeProvider"


class ManagementScopeProvider(Protocol):
    """Resolve one scope namespace and adapt its authorization mechanism."""

    kind: str

    async def resolve(self, runtime: Any, who: dict, scope_id: str) -> ScopeTarget: ...

    async def issue(
        self,
        runtime: Any,
        target: ScopeTarget,
        who: dict,
        authority: str,
        operation_id: str,
        payload: dict,
    ) -> dict: ...


class BotManagementScopeProvider:
    kind = "bot"

    async def resolve(self, runtime, who, scope_id):
        if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
            raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
        await runtime.catalog.get(bot_id=scope_id, **who)
        bot = await runtime.fleet.attach(scope_id)
        return ScopeTarget(self.kind, bot.store.bot_id, bot, bot.runtime.module_registry, self)

    async def issue(self, runtime, target, who, authority, operation_id, payload):
        bot = target.resource
        return await runtime.authority.issue(
            **who,
            bot_id=bot.store.bot_id,
            action=authority,
            operation_id=operation_id,
            payload=payload,
            plan_digest=payload.get("plan_digest", ""),
            policy_revision=(await bot.store.runtime())["policy_revision"],
        )


class DeploymentManagementScopeProvider:
    kind = "deployment"

    async def resolve(self, runtime, who, scope_id):
        if (
            runtime.deployment_id is None
            or runtime.deployment_modules is None
            or scope_id != runtime.deployment_id
        ):
            raise ForestError(
                "MANAGEMENT_SCOPE_NOT_FOUND",
                "The requested management scope is unavailable",
                404,
            )
        return ScopeTarget(self.kind, scope_id, None, runtime.deployment_modules, self)

    async def issue(self, runtime, target, who, authority, operation_id, payload):
        # The authenticated deployment session is the authority boundary. Each
        # repository still rechecks the session/account facts in its transaction.
        return {}


class HostManagementScopeProvider:
    kind = "host"

    async def resolve(self, runtime, who, scope_id):
        host = runtime.host
        if (
            host is None
            or runtime.host_modules is None
            or runtime.host_id is None
            or scope_id != runtime.host_id
        ):
            raise ForestError(
                "MANAGEMENT_SCOPE_NOT_FOUND",
                "The requested management scope is unavailable",
                404,
            )
        return ScopeTarget(self.kind, host.scope_id, host, runtime.host_modules, self)

    async def issue(self, runtime, target, who, authority, operation_id, payload):
        # Host operations bind to the stable host scope and the current process
        # incarnation. Repositories that mutate physical resources must recheck
        # both values before committing an effect.
        return {
            "claims": {
                "host_id": target.identity,
                "host_incarnation": target.resource.incarnation,
                "subject_id": who["subject_id"],
            }
        }


OFFICIAL_MANAGEMENT_SCOPE_PROVIDERS: tuple[ManagementScopeProvider, ...] = (
    BotManagementScopeProvider(),
    DeploymentManagementScopeProvider(),
    HostManagementScopeProvider(),
)


def management_scope_provider_registry(
    providers: tuple[ManagementScopeProvider, ...] = OFFICIAL_MANAGEMENT_SCOPE_PROVIDERS,
) -> dict[str, ManagementScopeProvider]:
    registry = {provider.kind: provider for provider in providers}
    if len(registry) != len(providers):
        raise ValueError("Management scope provider identities must be unique")
    return registry


async def _target(runtime, request, scope_kind, scope_id, *, write=False):
    session = await runtime.principal(
        request, write=write, require_admin=False if write else None
    )
    who = {
        "subject_id": session["account_id"],
        "session_token_hash": digest_token(request.cookies["forest_session"]),
    }
    provider = runtime.management_scope_providers.get(scope_kind)
    if provider is None:
        raise ForestError(
            "MANAGEMENT_SCOPE_NOT_FOUND", "The requested management scope is unavailable", 404
        )
    return session, who, await provider.resolve(runtime, who, scope_id)


def _operation_id(request, target, who, authority, *, write):
    key = request.headers.get("idempotency-key", "") if write else str(uuid4())
    if not 1 <= len(key) <= 128:
        raise ForestError(
            "IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422
        )
    return "web:" + digest_json(
        {
            "subject_id": who["subject_id"],
            "scope_kind": target.kind,
            "scope_id": target.identity,
            "action": authority,
            "key": key,
        }
    )


async def _signed(runtime, target, who, authority, operation, payload):
    return await target.provider.issue(
        runtime, target, who, authority, operation, payload
    )


def _contract(target, component_id: str) -> tuple[Any, ModuleManagementContract]:
    try:
        module = target.registry.get(component_id)
    except KeyError:
        raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Runtime module not found", 404) from None
    contract = getattr(module, "management_contract", None)
    if not isinstance(contract, ModuleManagementContract):
        raise ForestError(
            "MODULE_MANAGEMENT_UNAVAILABLE", "Runtime module has no management contract", 404
        )
    return module, contract


def install_module_routes(app, runtime):
    async def dispatch(
        scope_kind, scope_id, component_id, operation_id, request, payload, kind
    ):
        module_write = kind == "action"
        session, who, target = await _target(
            runtime, request, scope_kind, scope_id, write=module_write
        )
        module, contract = _contract(target, component_id)
        declared, body, authorized_payload = contract.prepare(operation_id, kind, payload)
        if declared.principal == "admin" and session["role"] != "admin":
            raise ForestError("PERMISSION_DENIED", "Current account cannot use this operation", 403)
        operation = _operation_id(
            request, target, who, declared.authority, write=module_write
        )
        context = ModuleRequest(
            runtime, target.resource, module, who, operation, {}, authorized_payload,
            scope_kind=target.kind, scope_id=target.identity,
        )
        prepared = None
        if declared.authorization_preparer is not None:
            authorized_payload, prepared = await declared.authorization_preparer(context, body)
        ticket = await _signed(
            runtime, target, who, declared.authority, operation, authorized_payload
        )
        context = ModuleRequest(
            runtime, target.resource, module, who, operation, ticket, authorized_payload, prepared,
            target.kind, target.identity,
        )
        result = await contract.invoke(context, declared, body)
        if kind == "action" and declared.success_status != 200:
            return JSONResponse(result, status_code=declared.success_status)
        return result

    async def artifact_target(
        scope_kind, scope_id, component_id, operation_id, request, kind
    ):
        if any(len(request.query_params.getlist(key)) != 1 for key in request.query_params):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Duplicate query fields are not allowed", 422
            )
        session, who, target = await _target(
            runtime, request, scope_kind, scope_id, write=False
        )
        module, contract = _contract(target, component_id)
        declared, body, authorized = contract.prepare(
            operation_id, kind, dict(request.query_params)
        )
        if declared.principal == "admin" and session["role"] != "admin":
            raise ForestError("PERMISSION_DENIED", "Current account cannot use this operation", 403)
        return session, who, target, module, contract, declared, body, authorized

    async def bounded_artifact(request: Request, limit: int) -> bytes:
        declared = request.headers.get("content-length")
        if declared is not None:
            try:
                length = int(declared)
            except ValueError:
                raise ForestError("CONTENT_LENGTH_INVALID", "Content-Length is invalid", 400) from None
            if length < 0 or length > limit:
                raise ForestError("REQUEST_TOO_LARGE", "Artifact exceeds its byte limit", 413)
        body = bytearray()
        try:
            async with asyncio.timeout(30):
                async for chunk in request.stream():
                    body.extend(chunk)
                    if len(body) > limit:
                        raise ForestError(
                            "REQUEST_TOO_LARGE", "Artifact exceeds its byte limit", 413
                        )
        except TimeoutError:
            raise ForestError("REQUEST_TIMEOUT", "Artifact upload timed out", 408) from None
        return bytes(body)

    async def chunks(content: bytes) -> AsyncIterator[bytes]:
        for offset in range(0, len(content), 64 * 1024):
            yield content[offset : offset + 64 * 1024]

    @app.get("/management/v1/scopes/{scope_kind}/{scope_id}/modules")
    async def modules(scope_kind: str, scope_id: str, request: Request):
        _, _, target = await _target(runtime, request, scope_kind, scope_id)
        return {
            "scope": {"kind": target.kind, "id": target.identity},
            "modules": target.registry.describe(),
        }

    @app.get(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/queries/{operation_id}"
    )
    async def query(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        operation_id: str,
        request: Request,
    ):
        if any(len(request.query_params.getlist(key)) != 1 for key in request.query_params):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Duplicate query fields are not allowed", 422
            )
        return await dispatch(
            scope_kind,
            scope_id,
            component_id,
            operation_id,
            request,
            dict(request.query_params),
            "query",
        )

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/actions/{operation_id}"
    )
    async def action(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        operation_id: str,
        body: dict,
        request: Request,
    ):
        return await dispatch(
            scope_kind, scope_id, component_id, operation_id, request, body, "action"
        )

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/uploads/{operation_id}"
    )
    async def upload(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        operation_id: str,
        request: Request,
    ):
        session, who, target, module, contract, declared, body, authorized = (
            await artifact_target(
                scope_kind, scope_id, component_id, operation_id, request, "upload"
            )
        )
        session = await runtime.principal(
            request, write=True, content_type=declared.media_type
        )
        if declared.principal == "admin" and session["role"] != "admin":
            raise ForestError("PERMISSION_DENIED", "Current account cannot use this operation", 403)
        operation = _operation_id(
            request, target, who, declared.authority, write=True
        )
        artifact = await bounded_artifact(request, declared.max_bytes)
        authorized = {
            **authorized,
            "artifact": {
                "media_type": declared.media_type,
                "bytes": len(artifact),
                "sha256": hashlib.sha256(artifact).hexdigest(),
            },
        }
        ticket = await _signed(
            runtime, target, who, declared.authority, operation, authorized
        )
        context = ModuleRequest(
            runtime, target.resource, module, who, operation, ticket, authorized,
            scope_kind=target.kind, scope_id=target.identity, artifact=artifact,
        )
        result = await contract.invoke(context, declared, body)
        return JSONResponse(result, status_code=declared.success_status)

    @app.get(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/downloads/{operation_id}"
    )
    async def download(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        operation_id: str,
        request: Request,
    ):
        _, who, target, module, contract, declared, body, authorized = (
            await artifact_target(
                scope_kind, scope_id, component_id, operation_id, request, "download"
            )
        )
        operation = _operation_id(
            request, target, who, declared.authority, write=False
        )
        ticket = await _signed(
            runtime, target, who, declared.authority, operation, authorized
        )
        context = ModuleRequest(
            runtime, target.resource, module, who, operation, ticket, authorized,
            scope_kind=target.kind, scope_id=target.identity,
        )
        result = await contract.invoke(context, declared, body)
        if not isinstance(result, ArtifactResult) or len(result.content) > declared.max_bytes:
            raise ForestError("MODULE_ARTIFACT_INVALID", "Module returned an invalid artifact", 500)
        headers = {
            "Cache-Control": "private, no-store",
            "Content-Length": str(len(result.content)),
            "X-Content-Type-Options": "nosniff",
        }
        if result.filename is not None:
            headers["Content-Disposition"] = (
                f'{declared.disposition}; filename="{result.filename}"'
            )
        if declared.media_type.startswith("text/html"):
            headers["Content-Security-Policy"] = (
                "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
                "img-src data: blob:; font-src data:; connect-src 'none'; "
                "form-action 'none'; base-uri 'none'; frame-ancestors 'self'"
            )
        return StreamingResponse(
            chunks(result.content), media_type=declared.media_type, headers=headers
        )
