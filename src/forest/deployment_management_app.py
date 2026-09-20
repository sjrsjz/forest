"""Generic module-contributed Web gateway for deployment scope."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
from importlib.resources import files

from fastapi import FastAPI, Header, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from forest.errors import ForestError
from forest.packages import digest_json


def create_deployment_management_app(
    *, deployment_id: str, components: tuple, access_token: str
) -> FastAPI:
    if not isinstance(access_token, str) or not 16 <= len(access_token) <= 4096:
        raise ValueError("Management access token must contain at least 16 characters")
    token_digest = hashlib.sha256(access_token.encode()).digest()
    registry = {component.component_id: component for component in components}
    operation_lock = asyncio.Lock()
    static = files("forest").joinpath("clean_management_static")
    app = FastAPI(title="Forest Deployment Console", docs_url=None, redoc_url=None)
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

    def authorize(token: str | None) -> None:
        if token is None or not hmac.compare_digest(
            hashlib.sha256(token.encode()).digest(), token_digest
        ):
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Management token is invalid", 401
            )

    def contract(component_id: str):
        component = registry.get(component_id)
        if component is None:
            raise ForestError("RUNTIME_MODULE_NOT_FOUND", "Component does not exist", 404)
        return component.contract

    def operation_id(component_id: str, action_id: str, key: str | None) -> str:
        if key is None or not 1 <= len(key) <= 128:
            raise ForestError(
                "IDEMPOTENCY_KEY_REQUIRED", "A stable Idempotency-Key is required", 422
            )
        return "web:" + digest_json(
            ["forest.deployment-management-operation/1", deployment_id, component_id, action_id, key]
        )

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(str(static.joinpath("index.html")))

    @app.get("/management/v1/runtime")
    async def runtime(x_forest_management_token: str | None = Header(default=None)):
        authorize(x_forest_management_token)
        return {
            "protocol": "forest.deployment-management-runtime/1",
            "runtime": {
                "bot_id": deployment_id,
                "graph_state": "CONTROL_PLANE",
                "graph_id": "deployment/control-plane/1",
            },
            "modules": [component.describe() for component in components],
        }

    @app.post("/management/v1/modules/{component_id}/queries/{query_id}")
    async def query(
        component_id: str,
        query_id: str,
        body: dict,
        x_forest_management_token: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        return await contract(component_id).query(query_id, body)

    @app.post("/management/v1/modules/{component_id}/actions/{action_id}")
    async def action(
        component_id: str,
        action_id: str,
        body: dict,
        x_forest_management_token: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with operation_lock:
            return await contract(component_id).action(
                action_id,
                operation_id=operation_id(component_id, action_id, idempotency_key),
                payload=body,
            )

    return app
