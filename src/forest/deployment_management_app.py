"""Session-authenticated module-contributed deployment management gateway."""

from __future__ import annotations

from datetime import datetime, timezone
from importlib.resources import files
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from forest.clean_management_app import ConfigurationApplyInput, ConfigurationPlanInput
from forest.errors import ForestError
from forest.management_scopes import (
    BotManagementScopeProvider,
    DeploymentManagementGateway,
    DeploymentManagementScopeProvider,
    ManagementScopeRegistry,
)
from forest.web_authority_ticket import WebAuthorityTicketAuthority

_SESSION_COOKIE = "forest_management_session"
_CSRF_COOKIE = "forest_management_csrf"


class LoginInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=12, max_length=1024)


class SetupCompleteInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pairing_code: str = Field(min_length=43, max_length=128)
    username: str = Field(min_length=3, max_length=64)
    display_name: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=12, max_length=1024)


def create_deployment_management_app(
    *,
    deployment_id: str,
    components: tuple,
    identity_authority,
    allowed_origin: str,
    bot_supervisor=None,
    deployment_web_receipts=None,
    deployment_audit=None,
) -> FastAPI:
    parsed_origin = urlsplit(allowed_origin)
    if (
        parsed_origin.scheme not in {"http", "https"}
        or not parsed_origin.netloc
        or parsed_origin.path not in {"", "/"}
        or parsed_origin.query
        or parsed_origin.fragment
    ):
        raise ValueError("Management origin must be an exact HTTP origin")
    allowed_origin = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
    secure_cookie = parsed_origin.scheme == "https"
    ticket_authority = WebAuthorityTicketAuthority(
        issuer_id=deployment_id,
        active_key_id=identity_authority.active_key_id,
        key_resolver=identity_authority.key_resolver,
        session_validator=identity_authority.revalidate_session,
    )
    deployment_gateway = DeploymentManagementGateway(
        deployment_id=deployment_id,
        components=components,
        ticket_authority=ticket_authority,
        web_receipts=deployment_web_receipts,
        audit=deployment_audit,
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
                ticket_authority=ticket_authority,
            )
        )
    scopes = ManagementScopeRegistry(tuple(providers), ticket_authority=ticket_authority)
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
        headers = None
        if exc.code == "MANAGEMENT_LOGIN_RATE_LIMITED":
            retry_after = exc.details.get("retry_after_seconds")
            if isinstance(retry_after, int) and retry_after > 0:
                headers = {"retry-after": str(retry_after)}
        return JSONResponse(
            {"code": exc.code, "message": exc.message, "details": exc.details},
            status_code=exc.status,
            headers=headers,
        )

    def same_origin(request: Request) -> None:
        if request.headers.get("origin") != allowed_origin:
            raise ForestError("MANAGEMENT_ORIGIN_DENIED", "Request origin is invalid", 403)

    async def authenticated(
        request: Request, *, csrf: bool = False, admin: bool = False
    ):
        token = request.cookies.get(_SESSION_COOKIE)
        if token is None:
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Web session is required", 401
            )
        session = await identity_authority.authenticate(token)
        if csrf:
            same_origin(request)
            session.verify_csrf(request.headers.get("x-forest-csrf"))
        if admin and session.subject.role != "ADMIN":
            raise ForestError(
                "MANAGEMENT_AUTHORIZATION_DENIED",
                "Administrator role is required",
                403,
            )
        return session

    async def runtime_projection(gateway) -> dict:
        result = await gateway.runtime()
        return {**result, "scopes": await scopes.list_scopes()}

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(str(static.joinpath("index.html")))

    @app.post("/management/v1/auth/login")
    async def login(request: Request, body: LoginInput):
        same_origin(request)
        peer_id = request.client.host if request.client is not None else "unknown"
        session = await identity_authority.login(
            username=body.username,
            password=body.password,
            peer_id=peer_id,
        )
        response = JSONResponse(
            {
                "subject": session.subject.as_dict(),
                "session_ref": session.session_ref,
                "expires_at": session.expires_at.isoformat(),
            }
        )
        lifetime = max(
            0,
            int((session.expires_at - datetime.now(timezone.utc)).total_seconds()),
        )
        response.set_cookie(
            _SESSION_COOKIE,
            session.session_token,
            httponly=True,
            secure=secure_cookie,
            samesite="strict",
            path="/management/",
            max_age=lifetime,
        )
        response.set_cookie(
            _CSRF_COOKIE,
            session.csrf_token,
            httponly=False,
            secure=secure_cookie,
            samesite="strict",
            path="/",
            max_age=lifetime,
        )
        return response

    @app.get("/management/v1/setup/status")
    async def setup_status():
        return await identity_authority.setup_status()

    @app.post("/management/v1/setup/complete")
    async def complete_setup(request: Request, body: SetupCompleteInput):
        same_origin(request)
        subject = await identity_authority.complete_setup(
            pairing_code=body.pairing_code,
            username=body.username,
            display_name=body.display_name,
            password=body.password,
        )
        return {"state": "COMPLETED", "subject": subject.as_dict()}

    @app.get("/management/v1/auth/session")
    async def current_session(request: Request):
        session = await authenticated(request)
        return {
            "subject": session.subject.as_dict(),
            "session_ref": session.session_ref,
            "expires_at": session.expires_at.isoformat(),
        }

    @app.post("/management/v1/auth/logout")
    async def logout(request: Request):
        session = await authenticated(request, csrf=True)
        await identity_authority.logout(request.cookies[_SESSION_COOKIE])
        response = JSONResponse({"session_ref": session.session_ref, "revoked": True})
        response.delete_cookie(_SESSION_COOKIE, path="/management/")
        response.delete_cookie(_CSRF_COOKIE, path="/")
        return response

    @app.get("/management/v1/runtime")
    async def runtime(request: Request):
        session = await authenticated(request)
        async with scopes.gateway("deployment", "deployment", session) as gateway:
            return await runtime_projection(gateway)

    @app.get("/management/v1/scopes/{scope_kind}/{scope_id}/runtime")
    async def scoped_runtime(scope_kind: str, scope_id: str, request: Request):
        session = await authenticated(request)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await runtime_projection(gateway)

    @app.get(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/configuration"
    )
    async def scoped_configuration(
        scope_kind: str, scope_id: str, component_id: str, request: Request
    ):
        session = await authenticated(request)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
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
        request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await gateway.plan_configuration(
                component_id,
                body=body,
                idempotency_key=request.headers.get("idempotency-key"),
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
        request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await gateway.apply_configuration(
                component_id,
                body=body,
                idempotency_key=request.headers.get("idempotency-key"),
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
        request: Request,
    ):
        session = await authenticated(request, csrf=True)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
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
        request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await gateway.module_action(
                component_id,
                action_id,
                body=body,
                idempotency_key=request.headers.get("idempotency-key"),
            )

    return app
