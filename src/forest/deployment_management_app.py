"""Generic module-contributed Web gateway across registered management scopes."""

from __future__ import annotations

import hashlib
import hmac
from importlib.resources import files

from fastapi import FastAPI, Header, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from forest.clean_management_app import ConfigurationApplyInput, ConfigurationPlanInput
from forest.errors import ForestError
from forest.management_scopes import (
    BotManagementScopeProvider,
    DeploymentManagementGateway,
    DeploymentManagementScopeProvider,
    ManagementScopeRegistry,
)


def create_deployment_management_app(
    *, deployment_id: str, components: tuple, access_token: str, bot_supervisor=None
) -> FastAPI:
    if not isinstance(access_token, str) or not 16 <= len(access_token) <= 4096:
        raise ValueError("Management access token must contain at least 16 characters")
    token_digest = hashlib.sha256(access_token.encode()).digest()
    deployment_gateway = DeploymentManagementGateway(
        deployment_id=deployment_id,
        components=components,
    )
    providers = [
        DeploymentManagementScopeProvider(
            deployment_id=deployment_id,
            gateway=deployment_gateway,
        )
    ]
    if bot_supervisor is not None:
        providers.append(
            BotManagementScopeProvider(
                supervisor=bot_supervisor,
                access_token=access_token,
            )
        )
    scopes = ManagementScopeRegistry(tuple(providers))
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

    async def runtime_projection(gateway) -> dict:
        result = await gateway.runtime()
        return {**result, "scopes": await scopes.list_scopes()}

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(str(static.joinpath("index.html")))

    @app.get("/management/v1/runtime")
    async def runtime(x_forest_management_token: str | None = Header(default=None)):
        authorize(x_forest_management_token)
        return await runtime_projection(deployment_gateway)

    @app.get("/management/v1/scopes/{scope_kind}/{scope_id}/runtime")
    async def scoped_runtime(
        scope_kind: str,
        scope_id: str,
        x_forest_management_token: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with scopes.gateway(scope_kind, scope_id) as gateway:
            return await runtime_projection(gateway)

    @app.get(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/configuration"
    )
    async def scoped_configuration(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        x_forest_management_token: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with scopes.gateway(scope_kind, scope_id) as gateway:
            return await gateway.read_configuration(component_id)

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/configuration/plans"
    )
    async def scoped_configuration_plan(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        body: ConfigurationPlanInput,
        x_forest_management_token: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with scopes.gateway(scope_kind, scope_id) as gateway:
            return await gateway.plan_configuration(
                component_id,
                body=body,
                idempotency_key=idempotency_key,
            )

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/configuration/apply"
    )
    async def scoped_configuration_apply(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        body: ConfigurationApplyInput,
        x_forest_management_token: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with scopes.gateway(scope_kind, scope_id) as gateway:
            return await gateway.apply_configuration(
                component_id,
                body=body,
                idempotency_key=idempotency_key,
            )

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/queries/{query_id}"
    )
    async def scoped_module_query(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        query_id: str,
        body: dict,
        x_forest_management_token: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with scopes.gateway(scope_kind, scope_id) as gateway:
            return await gateway.module_query(component_id, query_id, body)

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/actions/{action_id}"
    )
    async def scoped_module_action(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        action_id: str,
        body: dict,
        x_forest_management_token: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ):
        authorize(x_forest_management_token)
        async with scopes.gateway(scope_kind, scope_id) as gateway:
            return await gateway.module_action(
                component_id,
                action_id,
                body=body,
                idempotency_key=idempotency_key,
            )

    return app
