"""Production process boundary for the deployment-scoped multi-Bot supervisor."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

from forest.clean_multi_bot_supervisor import CleanMultiBotSupervisor
from forest.deployment_management import deployment_management_components
from forest.deployment_management_app import create_deployment_management_app
from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.deployment_provisioning import DeploymentProvisioningExecutor
from forest.deployment_web_bootstrap import DeploymentWebBootstrap
from forest.errors import ForestError
from forest.official_bot_provisioning import (
    OfficialBotCatalogSettingsResolver,
    OfficialBotProvisioner,
    OfficialBotSettingsResolver,
    official_bootstrap_contributions,
)
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_audit import DeploymentAuditRepository
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_web_authority import DeploymentWebAuthorityRepository
from forest.storage.deployment_web_identity import DeploymentWebIdentityAuthority


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ForestError("SUPERVISOR_CONFIGURATION_REQUIRED", f"{name} is required", 422)
    return value


def _positive_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = -1
    if not minimum <= value <= maximum:
        raise ForestError(
            "SUPERVISOR_CONFIGURATION_INVALID", f"{name} is outside its bound", 422
        )
    return value


def _deployment_dsn() -> str:
    configured = os.environ.get("FOREST_DEPLOYMENT_DSN")
    if configured:
        return configured
    password_path = Path(_required("FOREST_DEPLOYMENT_DATABASE_PASSWORD_FILE"))
    try:
        password = password_path.read_text(encoding="utf-8").strip()
    except OSError:
        raise ForestError(
            "SUPERVISOR_DATABASE_CREDENTIAL_UNAVAILABLE",
            "Deployment database credential is unavailable",
            503,
        ) from None
    host = os.environ.get("FOREST_DEPLOYMENT_DATABASE_HOST", "postgres")
    port = _positive_int("FOREST_DEPLOYMENT_DATABASE_PORT", 5432, 1, 65535)
    database = os.environ.get("FOREST_DEPLOYMENT_DATABASE_NAME", "forest_deployment")
    user = os.environ.get("FOREST_DEPLOYMENT_DATABASE_USER", "forest")
    if not password or not host or not database or not user:
        raise ForestError(
            "SUPERVISOR_CONFIGURATION_INVALID", "Deployment database identity is invalid", 422
        )
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{quote(database, safe='')}"
    )


@dataclass(frozen=True)
class CleanSupervisorSettings:
    deployment_id: str
    dsn: str = field(repr=False)
    master_keyring: DeploymentMasterKeyring = field(repr=False)
    web_bootstrap: DeploymentWebBootstrap = field(repr=False)
    poll_seconds: float = 1.0
    max_starting: int = 2
    management_host: str = "127.0.0.1"
    management_port: int = 8081
    management_origin: str = "http://127.0.0.1:8081"

    @classmethod
    def from_environment(cls) -> "CleanSupervisorSettings":
        keyring = DeploymentMasterKeyring.read(
            Path(_required("FOREST_DEPLOYMENT_MASTER_KEYRING_FILE"))
        )
        web_bootstrap = DeploymentWebBootstrap.read(
            Path(_required("FOREST_DEPLOYMENT_WEB_BOOTSTRAP_FILE"))
        )
        try:
            poll_seconds = float(os.environ.get("FOREST_SUPERVISOR_POLL_SECONDS", "1"))
            max_starting = int(os.environ.get("FOREST_SUPERVISOR_MAX_STARTING", "2"))
            management_port = int(os.environ.get("FOREST_DEPLOYMENT_MANAGEMENT_PORT", "8081"))
        except ValueError:
            raise ForestError(
                "SUPERVISOR_CONFIGURATION_INVALID",
                "Supervisor concurrency or poll interval is invalid",
                422,
            ) from None
        if (
            not 0.05 <= poll_seconds <= 30
            or not 1 <= max_starting <= 32
            or not 1 <= management_port <= 65535
        ):
            raise ForestError(
                "SUPERVISOR_CONFIGURATION_INVALID",
                "Supervisor concurrency or poll interval is outside its bound",
                422,
            )
        return cls(
            deployment_id=os.environ.get("FOREST_DEPLOYMENT_ID", "forest/default"),
            dsn=_deployment_dsn(),
            master_keyring=keyring,
            web_bootstrap=web_bootstrap,
            poll_seconds=poll_seconds,
            max_starting=max_starting,
            management_host=os.environ.get("FOREST_DEPLOYMENT_MANAGEMENT_HOST", "127.0.0.1"),
            management_port=management_port,
            management_origin=_required("FOREST_DEPLOYMENT_MANAGEMENT_ORIGIN"),
        )


async def run_clean_supervisor(settings: CleanSupervisorSettings) -> None:
    store = CleanDeploymentStore(settings.dsn, settings.deployment_id)
    await store.open()
    incarnation = "incarnation/" + str(uuid4())
    secrets = DeploymentSecretAuthority(
        store,
        active_key_id=settings.master_keyring.active_key_id,
        key_resolver=settings.master_keyring.resolve,
    )
    catalog = DeploymentCatalogRepository(store)
    web_identity = DeploymentWebIdentityAuthority(
        store,
        active_key_id=settings.master_keyring.active_key_id,
        key_resolver=settings.master_keyring.resolve,
    )
    web_receipts = DeploymentWebAuthorityRepository(store)
    deployment_audit = DeploymentAuditRepository(store)
    storage_profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)
    bootstrap_profiles = DeploymentBootstrapProfileAuthority(store, secrets=secrets)
    jobs = DeploymentProvisioningJobAuthority(store)
    settings_resolver = OfficialBotSettingsResolver(secrets=secrets)
    database_allocator = LocalPostgresDatabaseAllocator(settings.dsn)
    executor = DeploymentProvisioningExecutor(
        jobs=jobs,
        catalog=catalog,
        storage_profiles=storage_profiles,
        bootstrap_profiles=bootstrap_profiles,
        provisioner=OfficialBotProvisioner(
            settings_resolver=settings_resolver,
            database_allocator=database_allocator,
        ),
        worker_id="deployment-provisioner/" + incarnation,
    )
    supervisor = CleanMultiBotSupervisor(
        catalog,
        storage_profiles=storage_profiles,
        settings_resolver=OfficialBotCatalogSettingsResolver(
            bootstrap_profiles=bootstrap_profiles,
            settings_resolver=settings_resolver,
        ),
        host_id="deployment-host/" + settings.deployment_id,
        incarnation=incarnation,
        poll_seconds=settings.poll_seconds,
        max_starting=settings.max_starting,
        provisioning_executor=executor,
    )
    stop = asyncio.Event()
    management_server = None
    management_task = None
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(event, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(event, lambda *_args: loop.call_soon_threadsafe(stop.set))
    stop_task = None
    try:
        await supervisor.start()
        await web_identity.ensure_bootstrap_admin(
            operation_id="deployment/web/bootstrap/1",
            username=settings.web_bootstrap.username,
            display_name=settings.web_bootstrap.display_name,
            password=settings.web_bootstrap.password,
        )
        import uvicorn

        management_server = uvicorn.Server(
            uvicorn.Config(
                create_deployment_management_app(
                    deployment_id=settings.deployment_id,
                    components=deployment_management_components(
                        catalog=catalog,
                        secrets=secrets,
                        storage_profiles=storage_profiles,
                        bootstrap_profiles=bootstrap_profiles,
                        jobs=jobs,
                        supervisor=supervisor,
                        audit=deployment_audit,
                        bootstrap_contributions=official_bootstrap_contributions(),
                        database_allocator=database_allocator,
                        web_identity=web_identity,
                    ),
                    identity_authority=web_identity,
                    deployment_web_receipts=web_receipts,
                    deployment_audit=deployment_audit,
                    allowed_origin=settings.management_origin,
                    bot_supervisor=supervisor,
                ),
                host=settings.management_host,
                port=settings.management_port,
                access_log=False,
                proxy_headers=False,
                limit_concurrency=64,
            )
        )
        management_server.install_signal_handlers = lambda: None
        management_task = asyncio.create_task(
            management_server.serve(), name="deployment-management-web"
        )
        while not management_server.started:
            if management_task.done():
                await management_task
                raise RuntimeError("Deployment management server stopped during startup")
            await asyncio.sleep(0.01)
        print(
            json.dumps(
                {
                    "status": "active",
                    "scope": "deployment",
                    "deployment_id": settings.deployment_id,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        stop_task = asyncio.create_task(stop.wait(), name="deployment-supervisor-stop")
        tasks = (stop_task, management_task, *supervisor.owned_tasks())
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if not stop.is_set():
            failed = next(task for task in tasks if task in done)
            await failed
            raise RuntimeError("Deployment supervisor task stopped unexpectedly")
    finally:
        if stop_task is not None and not stop_task.done():
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
        if management_server is not None:
            management_server.should_exit = True
        if management_task is not None:
            await asyncio.gather(management_task, return_exceptions=True)
        await supervisor.close()
        await store.close()
