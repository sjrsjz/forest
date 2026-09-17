"""Generic authenticated transport for module-owned management contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Protocol
from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel, ConfigDict, ValidationError

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import digest_token


class EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class ModuleOperation:
    operation_id: str
    kind: Literal["query", "action"]
    authority: str
    input_model: type[BaseModel]
    handler: Callable[["ModuleRequest", BaseModel], Awaitable[Any]]
    authorization_payload: Callable[[BaseModel], dict] | None = None
    authorization_preparer: (
        Callable[["ModuleRequest", BaseModel], Awaitable[tuple[dict, Any]]] | None
    ) = None
    principal: Literal["authenticated", "admin"] = "authenticated"


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


OFFICIAL_MANAGEMENT_SCOPE_PROVIDERS: tuple[ManagementScopeProvider, ...] = (
    BotManagementScopeProvider(),
    DeploymentManagementScopeProvider(),
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
        return await contract.invoke(context, declared, body)

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
