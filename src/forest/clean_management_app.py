"""Compact management gateway for one active clean Bot host."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
from importlib.resources import files
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from forest.clean_management_contract import (
    CleanMemoryManagementContract,
    CleanModuleConfigurationContract,
)
from forest.errors import ForestError
from forest.packages import digest_json


class ConfigurationPlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_desired_revision: int
    value: Any


class ConfigurationApplyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_id: str
    expected_desired_revision: int


class CleanManagementGateway:
    def __init__(self, host, access_token: str) -> None:
        if not isinstance(access_token, str) or not 16 <= len(access_token) <= 4096:
            raise ValueError("Management access token must contain at least 16 characters")
        self.host = host
        self._token_digest = hashlib.sha256(access_token.encode()).digest()
        self._apply_lock = asyncio.Lock()

    def authorize(self, supplied: str | None) -> None:
        if supplied is None or not hmac.compare_digest(
            hashlib.sha256(supplied.encode()).digest(), self._token_digest
        ):
            raise ForestError("MANAGEMENT_AUTHENTICATION_REQUIRED", "Management token is invalid", 401)

    def contract(self, component_id: str) -> CleanModuleConfigurationContract:
        registry = self.host.registry
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot runtime is unavailable", 503)
        try:
            module = registry.get(component_id)
        except KeyError:
            raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Runtime module does not exist", 404) from None
        contract = getattr(module, "management_contract", None)
        if not isinstance(contract, CleanModuleConfigurationContract):
            raise ForestError(
                "MODULE_MANAGEMENT_UNAVAILABLE",
                "Runtime module does not publish a configuration contract",
                404,
            )
        return contract

    def operation_id(self, component_id: str, kind: str, key: str | None) -> str:
        if key is None or not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422
            )
        return "web:" + digest_json(
            {
                "protocol": "forest.clean-management-operation/1",
                "bot_id": self.host.settings.bot_id,
                "component_id": component_id,
                "kind": kind,
                "key": key,
            }
        )


def create_clean_management_app(host, *, access_token: str) -> FastAPI:
    gateway = CleanManagementGateway(host, access_token)
    app = FastAPI(title="Forest Runtime Console", docs_url=None, redoc_url=None)
    static = files("forest").joinpath("clean_management_static")
    app.mount("/static", StaticFiles(directory=str(static)), name="static")

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["x-content-type-options"] = "nosniff"
        response.headers["x-frame-options"] = "DENY"
        response.headers["referrer-policy"] = "no-referrer"
        response.headers["content-security-policy"] = (
            "default-src 'none'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; font-src 'self'; "
            "frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        )
        if request.url.path.startswith("/management/"):
            response.headers["cache-control"] = "no-store"
        return response

    @app.exception_handler(ForestError)
    async def forest_error(_request: Request, exc: ForestError):
        return JSONResponse(
            {"code": exc.code, "message": exc.message, "details": exc.details},
            status_code=exc.status,
        )

    def authenticated(token: str | None) -> None:
        gateway.authorize(token)

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(str(static.joinpath("index.html")))

    @app.get("/management/v1/runtime")
    async def runtime(x_forest_management_token: str | None = Header(default=None)):
        authenticated(x_forest_management_token)
        registry = host.registry
        if registry is None:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot runtime is unavailable", 503)
        status = await host.generation_authority.status()
        return {
            "protocol": "forest.clean-management-runtime/1",
            "runtime": status,
            "modules": registry.describe(),
        }

    @app.get("/management/v1/modules/{component_id}/configuration")
    async def read_configuration(
        component_id: str,
        x_forest_management_token: str | None = Header(default=None),
    ):
        authenticated(x_forest_management_token)
        return await gateway.contract(component_id).read()

    @app.post("/management/v1/modules/{component_id}/configuration/plans")
    async def plan_configuration(
        component_id: str,
        body: ConfigurationPlanInput,
        x_forest_management_token: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        authenticated(x_forest_management_token)
        return await gateway.contract(component_id).plan(
            operation_id=gateway.operation_id(component_id, "configuration.plan", idempotency_key),
            expected_desired_revision=body.expected_desired_revision,
            value=body.value,
        )

    @app.post("/management/v1/modules/{component_id}/configuration/apply")
    async def apply_configuration(
        component_id: str,
        body: ConfigurationApplyInput,
        x_forest_management_token: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        authenticated(x_forest_management_token)
        operation_id = gateway.operation_id(component_id, "configuration.apply", idempotency_key)
        async with gateway._apply_lock:
            contract = gateway.contract(component_id)
            saved = await contract.apply(
                operation_id=operation_id,
                plan_id=body.plan_id,
                expected_desired_revision=body.expected_desired_revision,
            )
            effective = await contract.configuration.effective()
            replaced = effective.revision != saved["revision"]
            if replaced:
                registry = await host.replace_runtime_graph()
            else:
                registry = host.registry
            graph = registry.graph_identity
            return {
                "configuration": saved,
                "replaced": replaced,
                "graph_id": graph.graph_id,
                "graph_revision": graph.graph_revision,
            }

    @app.post("/management/v1/modules/{component_id}/queries/{query_id}")
    async def module_query(
        component_id: str,
        query_id: str,
        body: dict,
        x_forest_management_token: str | None = Header(default=None),
    ):
        authenticated(x_forest_management_token)
        contract = gateway.contract(component_id)
        if not isinstance(contract, CleanMemoryManagementContract):
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Module query does not exist", 404
            )
        return await contract.query(query_id, body)

    return app
