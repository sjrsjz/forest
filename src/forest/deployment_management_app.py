"""Session-authenticated module-contributed deployment management gateway."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from importlib.resources import files
from tempfile import SpooledTemporaryFile
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from forest.bootstrap_contributions import BootstrapPackageSurface
from forest.errors import ForestError
from forest.management_gateway import ConfigurationApplyInput, ConfigurationPlanInput
from forest.management_scopes import (
    BotManagementScopeProvider,
    DeploymentManagementGateway,
    DeploymentManagementScopeProvider,
    ManagementScopeRegistry,
)
from forest.packages import MAX_ARCHIVE_BYTES
from forest.storage.management_asset_authority import CHUNK_BYTES
from forest.web_authority_ticket import WebAuthorityTicketAuthority

_SESSION_COOKIE = "forest_management_session"
_CSRF_COOKIE = "forest_management_csrf"
_ARTIFACT_REQUEST_BYTES = 16384
_ARTIFACT_UPLOAD_SECONDS = 20


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
    identity_authority,
    allowed_origin: str,
    bot_supervisor=None,
    deployment_web_receipts=None,
    deployment_audit=None,
    deployment_registry_provider=None,
    deployment_graph_changes=None,
    deployment_change_previewer=None,
    bootstrap_registry=None,
    bootstrap_package_catalog=None,
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
        ticket_authority=ticket_authority,
        web_receipts=deployment_web_receipts,
        audit=deployment_audit,
        runtime_registry_provider=deployment_registry_provider,
        graph_change_authority=deployment_graph_changes,
        graph_change_previewer=deployment_change_previewer,
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
    static = files("forest").joinpath("management_static")
    app = FastAPI(title="Forest Deployment Console", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(static)), name="static")

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["x-content-type-options"] = "nosniff"
        response.headers.setdefault("x-frame-options", "DENY")
        response.headers["referrer-policy"] = "no-referrer"
        response.headers.setdefault("content-security-policy", (
            "default-src 'none'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data: blob:; connect-src 'self'; font-src 'self'; "
            "frame-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        ))
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

    @app.get(
        "/management/v1/bootstrap-ui/{component_id}/{package_digest}/{artifact_sha256}"
    )
    async def bootstrap_ui(
        component_id: str, package_digest: str, artifact_sha256: str, request: Request
    ):
        await authenticated(request, admin=True)
        if bootstrap_registry is None or bootstrap_package_catalog is None:
            raise ForestError("BOOTSTRAP_UI_UNAVAILABLE", "Bootstrap UI catalog is unavailable", 503)
        contribution = next(
            (
                item for item in bootstrap_registry.contributions
                if item.component_id == component_id
            ),
            None,
        )
        surface = None if contribution is None else contribution.surface
        if (
            not isinstance(surface, BootstrapPackageSurface)
            or contribution.package_digest != "sha256:" + package_digest
            or surface.artifact_sha256 != artifact_sha256
        ):
            raise ForestError("BOOTSTRAP_UI_NOT_FOUND", "Bootstrap UI artifact is not current", 404)
        snapshot = bootstrap_package_catalog.get(contribution.package_digest)
        captured = snapshot.bootstrap_ui()
        if captured is None or captured[1] != artifact_sha256:
            raise ForestError("BOOTSTRAP_UI_NOT_FOUND", "Bootstrap UI artifact differs", 404)
        return Response(
            captured[0],
            media_type="text/html; charset=utf-8",
            headers={
                "x-frame-options": "SAMEORIGIN",
                "content-security-policy": (
                    "sandbox allow-scripts allow-forms; default-src 'none'; "
                    "script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                    "img-src data:; connect-src 'none'; font-src 'none'; "
                    "frame-ancestors 'self'; base-uri 'none'; form-action 'none'"
                ),
            },
        )

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

    @app.get(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/management-ui/{artifact_sha256}"
    )
    async def scoped_management_ui(
        scope_kind: str,
        scope_id: str,
        component_id: str,
        artifact_sha256: str,
        request: Request,
    ):
        session = await authenticated(request)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            content = await gateway.read_management_ui(component_id, artifact_sha256)
            return Response(
                content,
                media_type="text/html; charset=utf-8",
                headers={
                    "x-frame-options": "SAMEORIGIN",
                    "content-security-policy": (
                        "sandbox allow-scripts allow-forms; default-src 'none'; "
                        "script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                        "img-src blob: data:; connect-src 'none'; font-src 'none'; "
                        "frame-ancestors 'self'; base-uri 'none'; form-action 'none'"
                    ),
                },
            )

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

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/artifacts/{artifact_id}/download"
    )
    async def scoped_artifact_download(
        scope_kind: str, scope_id: str, component_id: str, artifact_id: str,
        request: Request,
    ):
        session = await authenticated(request, csrf=True)
        if request.headers.get("content-type") != "application/json":
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact input must be JSON", 415,
            )
        content = bytearray()
        async for fragment in request.stream():
            if len(content) + len(fragment) > _ARTIFACT_REQUEST_BYTES:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact input exceeds 16 KiB", 413,
                )
            content.extend(fragment)
        try:
            body = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact input is not valid JSON", 422,
            ) from exc
        if not isinstance(body, dict):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact input must be an object", 422,
            )
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            artifact = await gateway.artifact_download(component_id, artifact_id, body)

        async def chunks():
            for offset in range(0, len(artifact.content), 65536):
                yield artifact.content[offset:offset + 65536]

        return StreamingResponse(
            chunks(), media_type=artifact.media_type,
            headers={
                "content-length": str(len(artifact.content)),
                "content-disposition": f'attachment; filename="{artifact.filename}"',
                "x-content-type-options": "nosniff",
                "cache-control": "no-store",
            },
        )

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/artifacts/{artifact_id}/upload"
    )
    async def scoped_artifact_upload(
        scope_kind: str, scope_id: str, component_id: str, artifact_id: str,
        request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        length_header = request.headers.get("x-forest-artifact-length", "")
        transport_length = request.headers.get("content-length")
        content_sha256 = request.headers.get("x-forest-artifact-sha256", "")
        metadata_header = request.headers.get("x-forest-artifact-input", "{}")
        if request.headers.get("content-type") != "application/zip":
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact upload must be ZIP", 415,
            )
        if (
            not 1 <= len(length_header) <= 10
            or not length_header.isascii() or not length_header.isdigit()
            or not 1 <= int(length_header) <= MAX_ARCHIVE_BYTES
            or (transport_length is not None and transport_length != length_header)
            or re.fullmatch(r"sha256:[a-f0-9]{64}", content_sha256) is None
            or len(metadata_header) > _ARTIFACT_REQUEST_BYTES
        ):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Artifact upload headers are invalid or exceed their bound", 422,
            )
        try:
            payload = json.loads(metadata_header)
        except json.JSONDecodeError as exc:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact metadata is not JSON", 422,
            ) from exc
        if not isinstance(payload, dict):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID", "Artifact metadata must be an object", 422,
            )
        expected_length = int(length_header)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            async with gateway.artifact_upload(
                component_id, artifact_id, payload,
                content_length=expected_length, content_sha256=content_sha256,
                idempotency_key=request.headers.get("idempotency-key"),
            ) as (max_bytes, commit):
                try:
                    async with asyncio.timeout(_ARTIFACT_UPLOAD_SECONDS):
                        checksum = hashlib.sha256()
                        received = 0
                        with SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b") as staged:
                            async for fragment in request.stream():
                                received += len(fragment)
                                if received > expected_length or received > max_bytes:
                                    raise ForestError(
                                        "PACKAGE_TOO_LARGE",
                                        "Artifact upload exceeds its declared size", 413,
                                    )
                                checksum.update(fragment)
                                staged.write(fragment)
                            if received != expected_length or (
                                "sha256:" + checksum.hexdigest() != content_sha256
                            ):
                                raise ForestError(
                                    "PACKAGE_ARCHIVE_DIGEST_MISMATCH",
                                    "Artifact bytes differ from the signed upload declaration", 422,
                                )
                            staged.seek(0)
                            return await commit(staged.read())
                except TimeoutError as exc:
                    raise ForestError(
                        "PACKAGE_ARTIFACT_TIMEOUT", "Artifact upload exceeded its deadline", 504,
                    ) from exc

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/assets/{asset_id}/uploads"
    )
    async def scoped_asset_begin(
        scope_kind: str, scope_id: str, component_id: str, asset_id: str,
        body: dict, request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await gateway.asset_begin(component_id, asset_id, body)

    @app.put(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/assets/{asset_id}/uploads/{upload_id}/chunks/{chunk_index}"
    )
    async def scoped_asset_chunk(
        scope_kind: str, scope_id: str, component_id: str, asset_id: str,
        upload_id: str, chunk_index: int, request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        if request.headers.get("content-type") != "application/octet-stream":
            raise ForestError("MANAGEMENT_ASSET_CHUNK_INVALID", "Chunk must be binary", 415)
        content = bytearray()
        async for fragment in request.stream():
            if len(content) + len(fragment) > CHUNK_BYTES:
                raise ForestError("MANAGEMENT_ASSET_CHUNK_INVALID", "Chunk exceeds 256 KiB", 413)
            content.extend(fragment)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await gateway.asset_chunk(
                component_id, asset_id, upload_id, chunk_index, bytes(content)
            )

    @app.post(
        "/management/v1/scopes/{scope_kind}/{scope_id}/modules/"
        "{component_id}/assets/{asset_id}/uploads/{upload_id}/finalize"
    )
    async def scoped_asset_finalize(
        scope_kind: str, scope_id: str, component_id: str, asset_id: str,
        upload_id: str, request: Request,
    ):
        session = await authenticated(request, csrf=True, admin=True)
        async with scopes.gateway(scope_kind, scope_id, session) as gateway:
            return await gateway.asset_finalize(component_id, asset_id, upload_id)

    return app
