"""Generic authenticated transport for module-owned management contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal
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


@dataclass(frozen=True)
class ModuleRequest:
    web: Any
    bot: Any
    module: Any
    ticket: dict
    operation_id: str
    payload: dict


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


async def _target(runtime, request, bot_id, *, write=False):
    session = await runtime.principal(request, write=write)
    if runtime.catalog is None or runtime.fleet is None or runtime.authority is None:
        raise ForestError("BOT_MANAGEMENT_UNAVAILABLE", "Bot management is initializing", 503)
    who = {
        "subject_id": session["account_id"],
        "session_token_hash": digest_token(request.cookies["forest_session"]),
    }
    await runtime.catalog.get(bot_id=bot_id, **who)
    return who, await runtime.fleet.attach(bot_id)


async def _signed(runtime, request, bot, who, authority, payload, *, write):
    key = request.headers.get("idempotency-key", "") if write else str(uuid4())
    if not 1 <= len(key) <= 128:
        raise ForestError(
            "IDEMPOTENCY_KEY_REQUIRED", "A stable idempotency key is required", 422
        )
    operation = "web:" + digest_json(
        {
            "subject_id": who["subject_id"],
            "bot_id": bot.store.bot_id,
            "action": authority,
            "key": key,
        }
    )
    ticket = await runtime.authority.issue(
        **who,
        bot_id=bot.store.bot_id,
        action=authority,
        operation_id=operation,
        payload=payload,
        plan_digest=payload.get("plan_digest", ""),
        policy_revision=(await bot.store.runtime())["policy_revision"],
    )
    return ticket, operation


def _contract(bot, component_id: str) -> tuple[Any, ModuleManagementContract]:
    try:
        module = bot.platform.module_registry.get(component_id)
    except KeyError:
        raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Runtime module not found", 404) from None
    contract = getattr(module, "management_contract", None)
    if not isinstance(contract, ModuleManagementContract):
        raise ForestError(
            "MODULE_MANAGEMENT_UNAVAILABLE", "Runtime module has no management contract", 404
        )
    return module, contract


def install_module_routes(app, runtime):
    async def dispatch(bot_id, component_id, operation_id, request, payload, kind):
        module_write = kind == "action"
        who, bot = await _target(runtime, request, bot_id, write=module_write)
        module, contract = _contract(bot, component_id)
        declared, body, authorized_payload = contract.prepare(operation_id, kind, payload)
        ticket, operation = await _signed(
            runtime,
            request,
            bot,
            who,
            declared.authority,
            authorized_payload,
            write=module_write,
        )
        context = ModuleRequest(runtime, bot, module, ticket, operation, authorized_payload)
        return await contract.invoke(context, declared, body)

    @app.get(
        "/management/v1/bots/{bot_id}/runtime-modules/{component_id}/queries/{operation_id}"
    )
    async def query(
        bot_id: str, component_id: str, operation_id: str, request: Request
    ):
        if any(len(request.query_params.getlist(key)) != 1 for key in request.query_params):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Duplicate query fields are not allowed", 422
            )
        return await dispatch(
            bot_id, component_id, operation_id, request, dict(request.query_params), "query"
        )

    @app.post(
        "/management/v1/bots/{bot_id}/runtime-modules/{component_id}/actions/{operation_id}"
    )
    async def action(
        bot_id: str,
        component_id: str,
        operation_id: str,
        body: dict,
        request: Request,
    ):
        return await dispatch(bot_id, component_id, operation_id, request, body, "action")
