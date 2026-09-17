from __future__ import annotations

import asyncio
import re
import secrets
import time
from collections import OrderedDict
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from psycopg import OperationalError
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.staticfiles import StaticFiles

from forest.bootstrap import BootstrapState, normalize_dsn
from forest.errors import ForestError
from forest.management.activations import RestoreActivationManager
from forest.management.authority import WebAuthority
from forest.management.backup_api import install_backup_routes
from forest.management.backup_imports import BackupImportManager
from forest.management.backups import BackupManager
from forest.management.bots_api import install_bot_routes
from forest.management.catalog import BotCatalog
from forest.management.cleanup import StorageCleanupManager
from forest.management.deployment_configuration_contract import DEFAULT_VALUES
from forest.management.deployment_modules import (
    OFFICIAL_DEPLOYMENT_MODULES,
    DeploymentModuleHost,
)
from forest.management.fleet import BotFleet
from forest.management.module_api import (
    install_module_routes,
    management_scope_provider_registry,
)
from forest.management.onebot_protocol import install_onebot_protocol_route
from forest.management.provision import BotProvisioner
from forest.management.restores import RestoreManager
from forest.management.verifications import BackupVerificationManager
from forest.runtime_modules import RuntimeModuleRegistry
from forest.security import SecretVault, digest_token, hash_password, random_token, verify_password
from forest.storage.postgres import PostgresStore


@dataclass(frozen=True)
class WebSettings:
    data_dir: Path
    public_origin: str = "http://127.0.0.1:8080"
    managed_dsn: str | None = None

    @property
    def secure_cookie(self) -> bool:
        return self.public_origin.startswith("https://")


class StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SetupBody(StrictBody):
    token: str = Field(min_length=1, max_length=512)
    dsn: str | None = Field(default=None, max_length=4096)
    username: str = Field(min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(min_length=12, max_length=1024)


class LoginBody(StrictBody):
    username: str = Field(min_length=1, max_length=80)
    password: str = Field(min_length=1, max_length=1024)


class BodyLimit:
    """Bound JSON intake before framework buffering, including chunked requests."""

    def __init__(self, app: Any, limit: int = 65536, runtime: Any = None) -> None:
        self.app, self.limit, self.runtime = app, limit, runtime
        self.archive_slots = asyncio.Semaphore(2)
        self.backup_import_slots = asyncio.Semaphore(2)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, send)
            return
        if scope["path"] == "/management/v1/packages" and scope["method"] == "POST":
            try:
                await self.runtime.principal(Request(scope), write=True, content_type="application/zip")
            except ForestError as error:
                await JSONResponse({"code": error.code, "message": error.message}, error.status)(scope, receive, send)
                return
            if self.archive_slots.locked():
                await JSONResponse({"code": "PACKAGE_IMPORT_BUSY"}, 429)(scope, receive, send)
                return
            async with self.archive_slots:
                await self._bounded(scope, receive, send, 32 * 1024 * 1024)
            return
        if (
            scope["method"] == "PUT"
            and re.fullmatch(r"/management/v1/backup-imports/[^/]+/artifact", scope["path"])
        ):
            try:
                await self.runtime.principal(
                    Request(scope), write=True, content_type="application/vnd.forest.backup"
                )
            except ForestError as error:
                await JSONResponse(
                    {"code": error.code, "message": error.message}, error.status
                )(scope, receive, send)
                return
            if self.backup_import_slots.locked():
                await JSONResponse({"code": "BACKUP_IMPORT_BUSY"}, 429)(scope, receive, send)
                return
            async with self.backup_import_slots:
                await self.app(scope, receive, send)
            return
        await self._bounded(scope, receive, send, self.limit)

    async def _bounded(self, scope, receive, send, limit):
        body = bytearray()
        try:
            async with asyncio.timeout(10):
                while True:
                    event = await receive()
                    if event["type"] == "http.disconnect":
                        return
                    body.extend(event.get("body", b""))
                    if len(body) > limit:
                        await JSONResponse({"code": "REQUEST_TOO_LARGE"}, 413)(scope, receive, send)
                        return
                    if not event.get("more_body", False):
                        break
        except TimeoutError:
            await JSONResponse({"code": "REQUEST_TIMEOUT"}, 408)(scope, receive, send)
            return
        supplied = False

        async def bounded_receive() -> dict[str, Any]:
            nonlocal supplied
            if not supplied:
                supplied = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)


class AttemptLimiter:
    def __init__(self) -> None:
        self.entries: OrderedDict[str, list[float]] = OrderedDict()

    def check(self, identity: str) -> None:
        now = time.monotonic()
        recent = [t for t in self.entries.pop(identity, []) if now - t < 60]
        if len(recent) >= 15:
            self.entries[identity] = recent
            raise ForestError("RATE_LIMITED", "请稍后再试。", 429)
        recent.append(now)
        self.entries[identity] = recent
        while len(self.entries) > 1024:
            self.entries.popitem(last=False)


class ManagementRuntime:
    def __init__(self, settings: WebSettings) -> None:
        self.settings = settings
        self.bootstrap = BootstrapState(settings.data_dir)
        self.store: PostgresStore | None = None
        self.setup_lock = asyncio.Lock()
        self.password_slots = asyncio.Semaphore(4)
        self.attempts = AttemptLimiter()
        self.recovery_error: str | None = None
        self.catalog: BotCatalog | None = None
        self.authority: WebAuthority | None = None
        self.fleet: BotFleet | None = None
        self.provisioner: BotProvisioner | None = None
        self.backups: BackupManager | None = None
        self.restores: RestoreManager | None = None
        self.activations: RestoreActivationManager | None = None
        self.cleanup: StorageCleanupManager | None = None
        self.verifications: BackupVerificationManager | None = None
        self.backup_imports: BackupImportManager | None = None
        self.service_tasks: list[asyncio.Task] = []
        self.service_errors: dict[str, str] = {}
        self.deployment_id: str | None = None
        self.deployment_modules: RuntimeModuleRegistry | None = None
        self.management_scope_providers = management_scope_provider_registry()

    async def connect(self, dsn: str, *, migrate: bool = True) -> PostgresStore:
        store = PostgresStore(dsn)
        try:
            async with asyncio.timeout(12):
                await store.open()
                if migrate:
                    await store.migrate()
            return store
        except BaseException:
            await store.close()
            raise

    async def restore(self) -> None:
        plan = await asyncio.to_thread(self.bootstrap.load_plan)
        if plan is None:
            return
        completed = (await asyncio.to_thread(self.bootstrap.status))["stage"] == "COMPLETE"
        candidate = await self.connect(plan["dsn"], migrate=not completed)
        try:
            if completed:
                try:
                    saved = await candidate.setup_status(plan["setup_id"])
                except Exception:
                    raise ForestError(
                        "DATABASE_IDENTITY_MISMATCH", "无法确认已初始化的管理数据库。", 503
                    ) from None
                if saved is None or saved["plan_digest"] != plan["plan_digest"]:
                    raise ForestError("DATABASE_IDENTITY_MISMATCH", "管理数据库身份不匹配。", 503)
                await candidate.migrate()
            else:
                await candidate.setup(
                    plan["setup_id"], plan["plan_digest"], plan["username"], plan["password_hash"]
                )
                await asyncio.to_thread(
                    self.bootstrap.complete, plan["setup_id"], plan["plan_digest"]
                )
            vault = await asyncio.to_thread(SecretVault, self.settings.data_dir)
            authority = WebAuthority(candidate, vault, plan["deployment_id"])
            await authority.initialize()
            catalog = BotCatalog(candidate, vault, managed_dsn=self.settings.managed_dsn)
            fleet = BotFleet(catalog, authority, self.settings.data_dir)
            await fleet.initialize()
            backups = BackupManager(
                catalog, fleet.artifacts, self.settings.data_dir, plan["dsn"]
            )
            restores = RestoreManager(catalog, backups, self.settings.data_dir)
            activations = RestoreActivationManager(catalog)
            cleanup = StorageCleanupManager(catalog, backups)
            verifications = BackupVerificationManager(
                catalog, backups, restores, self.settings.data_dir
            )
            backup_imports = BackupImportManager(
                catalog, backups, restores, self.settings.data_dir
            )
            deployment_modules = RuntimeModuleRegistry(
                DeploymentModuleHost(
                    "deployment",
                    plan["deployment_id"],
                    candidate,
                    authority,
                    self.password_slots,
                ),
                OFFICIAL_DEPLOYMENT_MODULES,
            )
            await deployment_modules.start()
        except BaseException:
            if "deployment_modules" in locals():
                await deployment_modules.close()
            await candidate.close()
            raise
        await self.close_services()
        if self.store is not None:
            await self.store.close()
        self.store = candidate
        self.catalog, self.authority, self.fleet = catalog, authority, fleet
        self.backups = backups
        self.restores = restores
        self.activations = activations
        self.cleanup = cleanup
        self.verifications = verifications
        self.backup_imports = backup_imports
        self.deployment_id = plan["deployment_id"]
        self.deployment_modules = deployment_modules
        self.provisioner = BotProvisioner(catalog)
        self.service_tasks = [
            asyncio.create_task(self.provisioner.run_forever(), name="bot-provisioning"),
            asyncio.create_task(fleet.run_forever(), name="bot-fleet"),
            asyncio.create_task(backups.run_forever(), name="backup-jobs"),
            asyncio.create_task(restores.run_forever(), name="restore-stage-jobs"),
            asyncio.create_task(activations.run_forever(), name="restore-activation-jobs"),
            asyncio.create_task(cleanup.run_forever(), name="storage-cleanup-jobs"),
            asyncio.create_task(verifications.run_forever(), name="backup-verification-jobs"),
            asyncio.create_task(backup_imports.run_forever(), name="backup-import-jobs"),
        ]
        self.recovery_error = None

    async def close_services(self) -> None:
        deployment_modules, self.deployment_modules = self.deployment_modules, None
        if deployment_modules is not None:
            errors = await deployment_modules.close()
            if errors:
                self.service_errors["deployment-modules"] = "MODULE_CLEANUP_FAILED"
        if self.provisioner is not None:
            await self.provisioner.close()
        if self.backups is not None:
            await self.backups.close()
        if self.restores is not None:
            await self.restores.close()
        if self.activations is not None:
            await self.activations.close()
        if self.cleanup is not None:
            await self.cleanup.close()
        if self.verifications is not None:
            await self.verifications.close()
        if self.backup_imports is not None:
            await self.backup_imports.close()
        for task in self.service_tasks:
            task.cancel()
        await asyncio.gather(*self.service_tasks, return_exceptions=True)
        self.service_tasks.clear()
        if self.fleet is not None:
            await self.fleet.close()

    async def recovery_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            async with self.setup_lock:
                if self.store is not None:
                    for index, (name, factory) in enumerate((
                        ("bot-provisioning", self.provisioner.run_forever),
                        ("bot-fleet", self.fleet.run_forever),
                        ("backup-jobs", self.backups.run_forever),
                        ("restore-stage-jobs", self.restores.run_forever),
                        ("restore-activation-jobs", self.activations.run_forever),
                        ("storage-cleanup-jobs", self.cleanup.run_forever),
                        ("backup-verification-jobs", self.verifications.run_forever),
                        ("backup-import-jobs", self.backup_imports.run_forever),
                    )):
                        task = self.service_tasks[index]
                        if task.done():
                            error = None if task.cancelled() else task.exception()
                            self.service_errors[name] = (
                                error.code if isinstance(error, ForestError) else "BACKGROUND_SERVICE_RESTARTED"
                            )
                            self.service_tasks[index] = asyncio.create_task(factory(), name=name)
                    continue
                try:
                    await self.restore()
                except Exception as exc:
                    self.recovery_error = (
                        exc.code if isinstance(exc, ForestError) else "DATABASE_UNAVAILABLE"
                    )

    def require_store(self) -> PostgresStore:
        if self.store is None:
            raise ForestError("DATABASE_UNAVAILABLE", "管理数据库尚未就绪。", 503)
        return self.store

    async def reauthenticate_admin(self, account_id: str, password: str) -> None:
        """Verify a fresh administrator secret without exposing it to module tickets."""
        store = self.require_store()
        async with store.pool.connection() as conn:
            account = await (
                await conn.execute(
                    "SELECT password_hash,disabled,role FROM web_accounts WHERE id=%s",
                    (account_id,),
                )
            ).fetchone()
        encoded = account["password_hash"] if account else "malformed"
        async with self.password_slots:
            valid = await asyncio.to_thread(verify_password, password, encoded)
        if not valid or account is None or account["disabled"] or account["role"] != "admin":
            raise ForestError(
                "REAUTHENTICATION_FAILED", "Current password is incorrect", 403
            )

    def origin(self, request: Request, *, content_type: str = "application/json") -> None:
        if request.headers.get("origin") != self.settings.public_origin:
            raise ForestError("ORIGIN_REJECTED", "请求来源不受信任。", 403)
        if request.headers.get("content-type", "").split(";", 1)[0] != content_type:
            raise ForestError("CONTENT_TYPE_REQUIRED", f"此接口只接收 {content_type}。", 415)

    async def principal(self, request: Request, *, write: bool = False,
                        content_type: str = "application/json",
                        require_admin: bool | None = None) -> dict[str, Any]:
        token = request.cookies.get("forest_session", "")
        if not token or len(token) > 512:
            raise ForestError("UNAUTHENTICATED", "请先登录。", 401)
        session = await self.require_store().get_session(digest_token(token))
        if session is None:
            raise ForestError("UNAUTHENTICATED", "会话已失效，请重新登录。", 401)
        if (write if require_admin is None else require_admin) and session["role"] != "admin":
            raise ForestError("PERMISSION_DENIED", "当前账号没有此管理权限。", 403)
        if write:
            self.origin(request, content_type=content_type)
            csrf = request.headers.get("x-csrf-token", "")
            if (
                not csrf
                or len(csrf) > 512
                or not secrets.compare_digest(digest_token(csrf), session["csrf_hash"])
            ):
                raise ForestError("CSRF_REJECTED", "请求验证失效，请刷新页面。", 403)
        return session


def create_app(settings: WebSettings) -> FastAPI:
    runtime = ManagementRuntime(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(runtime.bootstrap.initialize)
        try:
            await runtime.restore()
        except Exception as exc:
            # Keep the independent setup/diagnostic service available. Never log the DSN.
            runtime.recovery_error = (
                exc.code if isinstance(exc, ForestError) else "DATABASE_UNAVAILABLE"
            )
        recovery = asyncio.create_task(runtime.recovery_loop(), name="management-database-recovery")
        try:
            yield
        finally:
            recovery.cancel()
            with suppress(asyncio.CancelledError):
                await recovery
            await runtime.close_services()
            if runtime.store is not None:
                await runtime.store.close()

    app = FastAPI(title="Forest Management", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.runtime = runtime
    app.add_middleware(BodyLimit, runtime=runtime)
    host = urlsplit(settings.public_origin).hostname
    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=list({host, "127.0.0.1", "localhost"} - {None})
    )

    @app.middleware("http")
    async def headers(request: Request, call_next: Any):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        if "Content-Security-Policy" not in response.headers:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; script-src 'self'; style-src 'self'; "
                "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; "
                "base-uri 'none'; form-action 'self'"
            )
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(ForestError)
    async def domain_error(request: Request, exc: ForestError):
        return JSONResponse(exc.as_dict(), status_code=exc.status)

    @app.exception_handler(OperationalError)
    @app.exception_handler(PoolTimeout)
    async def database_unavailable(request: Request, exc: Exception):
        # Driver errors may contain connection details. Preserve only a stable code.
        return JSONResponse(
            {"code": "DATABASE_UNAVAILABLE", "message": "数据库暂时不可用，请稍后重试。"},
            status_code=503,
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_body(request: Request, exc: RequestValidationError):
        # Pydantic's normal response can echo password/token input. Return locations only.
        return JSONResponse(
            {
                "code": "VALIDATION_FAILED",
                "message": "请求字段不符合要求。",
                "fields": [{"path": list(err["loc"]), "type": err["type"]} for err in exc.errors()],
            },
            status_code=422,
        )

    @app.get("/health/live")
    async def live():
        return {"status": "ok", "service": "forest-management"}

    @app.get("/management/v1/setup")
    async def setup_status():
        result = await asyncio.to_thread(runtime.bootstrap.status)
        result["database_ready"] = runtime.store is not None
        result["managed_database_available"] = settings.managed_dsn is not None
        result["recovery_error"] = runtime.recovery_error
        result["background_errors"] = runtime.service_errors
        return result

    @app.post("/management/v1/setup")
    async def setup(body: SetupBody, request: Request):
        runtime.origin(request)
        runtime.attempts.check(request.client.host if request.client else "unknown")
        async with runtime.setup_lock:
            if not await asyncio.to_thread(runtime.bootstrap.verify_token, body.token):
                raise ForestError("SETUP_TOKEN_INVALID", "配对码无效、过期或初始化已完成。", 403)
            dsn = body.dsn or settings.managed_dsn
            if not dsn:
                raise ForestError("DSN_REQUIRED", "请填写部署数据库连接地址。", 422)
            canonical, _ = normalize_dsn(dsn)
            frozen = await asyncio.to_thread(runtime.bootstrap.load_plan)
            async with runtime.password_slots:
                if frozen is not None:
                    if not await asyncio.to_thread(
                        verify_password, body.password, frozen["password_hash"]
                    ):
                        raise ForestError("SETUP_PLAN_CONFLICT", "初始化计划已固定。", 409)
                    password_hash = frozen["password_hash"]
                else:
                    password_hash = await asyncio.to_thread(hash_password, body.password)
            # A connection probe is read-only; schema/account writes follow the local freeze.
            from psycopg import AsyncConnection

            try:
                async with asyncio.timeout(6):
                    async with await AsyncConnection.connect(canonical, connect_timeout=5) as conn:
                        await conn.execute("SELECT 1")
            except Exception:
                raise ForestError("DATABASE_UNAVAILABLE", "无法连接数据库，请检查连接设置。", 503)
            await asyncio.to_thread(
                runtime.bootstrap.freeze, body.token, dsn, body.username, password_hash
            )
            try:
                await runtime.restore()
            except Exception:
                raise ForestError(
                    "SETUP_RECOVERY_REQUIRED", "初始化计划已保存；恢复时将核对同一目标。", 503
                )
            return {"status": "complete"}

    @app.post("/management/v1/login")
    async def login(body: LoginBody, request: Request):
        runtime.origin(request)
        runtime.attempts.check(request.client.host if request.client else "unknown")
        try:
            password_size = len(body.password.encode("utf-8"))
        except UnicodeEncodeError:
            password_size = 0
        if not 12 <= password_size <= 1024:
            # Apply the same password policy before looking up either username.
            raise ForestError("INVALID_CREDENTIALS", "账号或密码不正确。", 401)
        store = runtime.require_store()
        account = await store.find_account(body.username)
        async with runtime.password_slots:
            if account is None:
                await asyncio.to_thread(hash_password, "dummy-password-for-equal-work")
                valid = False
            else:
                valid = await asyncio.to_thread(
                    verify_password, body.password, account["password_hash"]
                )
        if not valid or account is None or account["disabled"]:
            raise ForestError("INVALID_CREDENTIALS", "账号或密码不正确。", 401)
        config = await store.read_config("deployment")
        values = config["effective"]["values"] if config["effective"] else DEFAULT_VALUES
        ttl = int(values["session_ttl_minutes"])
        token, csrf = random_token(), random_token()
        await store.create_session(
            digest_token(token),
            digest_token(csrf),
            account["id"],
            datetime.now(UTC) + timedelta(minutes=ttl),
        )
        response = JSONResponse({
            "account_id": account["id"],
            "username": account["username"],
            "role": account["role"],
            "can_write": account["role"] == "admin",
            "deployment_id": runtime.deployment_id,
            "display_name": values["display_name"],
        })
        response.set_cookie(
            "forest_session",
            token,
            max_age=ttl * 60,
            httponly=True,
            secure=settings.secure_cookie,
            samesite="strict",
            path="/",
        )
        response.set_cookie(
            "forest_csrf",
            csrf,
            max_age=ttl * 60,
            httponly=False,
            secure=settings.secure_cookie,
            samesite="strict",
            path="/",
        )
        return response

    @app.get("/management/v1/session")
    async def session(request: Request):
        value = await runtime.principal(request)
        config = await runtime.require_store().read_config("deployment")
        values = config["effective"]["values"] if config["effective"] else DEFAULT_VALUES
        return {"account_id": value["account_id"], "username": value["username"],
                "role": value["role"], "can_write": value["role"] == "admin",
                "deployment_id": runtime.deployment_id,
                "display_name": values["display_name"]}

    @app.post("/management/v1/logout")
    async def logout(request: Request):
        await runtime.principal(request, write=True, require_admin=False)
        await runtime.require_store().revoke_session(
            digest_token(request.cookies["forest_session"])
        )
        response = JSONResponse({"status": "logged_out"})
        response.delete_cookie("forest_session", path="/")
        response.delete_cookie("forest_csrf", path="/")
        return response

    install_backup_routes(app, runtime)
    install_bot_routes(app, runtime)
    install_onebot_protocol_route(app, runtime)
    install_module_routes(app, runtime)
    static = Path(__file__).with_name("static")
    app.mount("/static", StaticFiles(directory=static), name="static")

    @app.get("/")
    async def index():
        return FileResponse(static / "index.html")

    return app
