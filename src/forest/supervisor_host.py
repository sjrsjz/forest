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

from forest.bootstrap_contributions import BootstrapConfigurationRegistry
from forest.bot_host import BotHost
from forest.deployment_kernel_management import (
    BootstrapCatalogKernelQuery,
    BootstrapProfileKernelCommand,
    LocalStorageProfileKernelCommand,
    ProvisioningJobKernelCommand,
    ProvisioningReconcileKernelCommand,
    SecretPutKernelCommand,
    StorageProfileKernelCommand,
    WebAccountKernelCommand,
)
from forest.deployment_management_app import create_deployment_management_app
from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.deployment_package_controller import DeploymentPackageController
from forest.deployment_packages import auto_install_deployment_sources
from forest.deployment_provisioning_driver import DeploymentProvisioningEffectDriver
from forest.deployment_provisioning_owned_work import DeploymentProvisioningOwnedWorkAuthority
from forest.deployment_runtime import DeploymentRuntimeScope
from forest.deployment_web_bootstrap import DeploymentWebBootstrap
from forest.errors import ForestError
from forest.multi_bot_supervisor import MultiBotSupervisor
from forest.official_bot_provisioning import (
    OfficialBotCatalogSettingsResolver,
    OfficialBotProvisioner,
    OfficialBotSettingsResolver,
    official_bootstrap_contributions,
)
from forest.packages import PackageCatalog
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.storage.deployment_audit import DeploymentAuditRepository
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_graph_changes import DeploymentGraphChangeAuthority
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_store import DeploymentStore
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
class SupervisorSettings:
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
    def from_environment(cls) -> "SupervisorSettings":
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


async def run_supervisor(settings: SupervisorSettings) -> None:
    store = DeploymentStore(settings.dsn, settings.deployment_id)
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
    graph_changes = DeploymentGraphChangeAuthority(store)
    storage_profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)
    bootstrap_profiles = DeploymentBootstrapProfileAuthority(store, secrets=secrets)
    jobs = DeploymentProvisioningJobAuthority(store)
    package_catalog = PackageCatalog(
        Path(os.environ.get("FOREST_PACKAGE_CATALOG_ROOT", ".forest/packages"))
    )
    bootstrap_registry = BootstrapConfigurationRegistry(
        official_bootstrap_contributions(package_catalog)
    )
    deployment_controller = None

    async def read_deployment_packages(query_id: str, payload: dict) -> dict:
        if deployment_controller is None:
            raise ForestError(
                "DEPLOYMENT_PACKAGE_REGISTRY_UNAVAILABLE",
                "Deployment package registry Generation is unavailable", 503,
            )
        return await deployment_controller.read_package_registry(query_id, payload)

    def bot_host_factory(bot_settings):
        return BotHost(
            bot_settings, package_catalog=package_catalog,
            package_registry_reader=read_deployment_packages,
            credential_resolver=secrets.resolve_text_for_purpose,
        )

    settings_resolver = OfficialBotSettingsResolver(
        secrets=secrets, bootstrap_registry=bootstrap_registry
    )
    database_allocator = LocalPostgresDatabaseAllocator(settings.dsn)
    provisioner = OfficialBotProvisioner(
        settings_resolver=settings_resolver,
        database_allocator=database_allocator,
    )
    supervisor = MultiBotSupervisor(
        catalog,
        storage_profiles=storage_profiles,
        settings_resolver=OfficialBotCatalogSettingsResolver(
            bootstrap_profiles=bootstrap_profiles,
        ),
        host_factory=bot_host_factory,
        host_id="deployment-host/" + settings.deployment_id,
        incarnation=incarnation,
        poll_seconds=settings.poll_seconds,
        max_starting=settings.max_starting,
    )

    def notify_deployment_work():
        if deployment_controller is not None:
            try:
                deployment_controller.registry.notify_work()
            except ForestError:
                pass
    stop = asyncio.Event()
    management_server = None
    management_task = None
    graph_change_task = None
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(event, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(event, lambda *_args: loop.call_soon_threadsafe(stop.set))
    stop_task = None
    fatal_task = None
    try:
        await supervisor.start()
        await web_identity.initialize_setup(settings.web_bootstrap)
        deployment_runtime = DeploymentRuntimeScope(
            store, supervisor, package_catalog=package_catalog,
            graph_change_authority=graph_changes,
            kernel_query_handlers={
                ("deployment.provisioning", "deployment.bootstrap-catalog"):
                    BootstrapCatalogKernelQuery(bootstrap_registry),
            },
            kernel_management_handlers={
                ("deployment.secrets", "deployment.secret-put"):
                    SecretPutKernelCommand(secrets),
                **{
                    ("deployment.accounts", kind): WebAccountKernelCommand(web_identity, kind)
                    for kind in WebAccountKernelCommand._METHODS
                },
                ("deployment.provisioning", "deployment.storage-create-local"):
                    LocalStorageProfileKernelCommand(secrets, storage_profiles, database_allocator),
                ("deployment.provisioning", "deployment.storage-create"):
                    StorageProfileKernelCommand(storage_profiles),
                ("deployment.provisioning", "deployment.bootstrap-create"):
                    BootstrapProfileKernelCommand(bootstrap_profiles, bootstrap_registry),
                **{
                    ("deployment.provisioning", kind): ProvisioningJobKernelCommand(
                        jobs, kind, notify=notify_deployment_work,
                    ) for kind in ProvisioningJobKernelCommand._METHODS
                },
                ("deployment.provisioning", "deployment.provisioning-reconcile"):
                    ProvisioningReconcileKernelCommand(
                        jobs, catalog, storage_profiles, bootstrap_profiles, provisioner,
                    ),
            },
        )
        deployment_runtime.kernel_owned_work_authorities["deployment.provisioning"] = (
            DeploymentProvisioningOwnedWorkAuthority(deployment_runtime, jobs, catalog)
        )
        deployment_runtime.effect_driver_factories[
            "deployment.provisioning-materializer"
        ] = lambda configuration, revision, identity, declaration: (
            DeploymentProvisioningEffectDriver(
                configuration, revision, identity, declaration,
                runtime=deployment_runtime, jobs=jobs,
                storage_profiles=storage_profiles,
                bootstrap_profiles=bootstrap_profiles, provisioner=provisioner,
            )
        )
        deployment_controller = DeploymentPackageController(
            deployment_runtime, package_catalog,
            official_sources=auto_install_deployment_sources(),
        )
        await deployment_controller.start()
        import uvicorn

        management_server = uvicorn.Server(
            uvicorn.Config(
                create_deployment_management_app(
                    deployment_id=settings.deployment_id,
                    deployment_registry_provider=lambda: deployment_controller.registry,
                    deployment_graph_changes=graph_changes,
                    deployment_change_previewer=deployment_controller.preview_change,
                    identity_authority=web_identity,
                    deployment_web_receipts=web_receipts,
                    deployment_audit=deployment_audit,
                    allowed_origin=settings.management_origin,
                    bot_supervisor=supervisor,
                    bootstrap_registry=bootstrap_registry,
                    bootstrap_package_catalog=package_catalog,
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
        graph_change_task = asyncio.create_task(
            deployment_controller.serve_graph_changes(graph_changes),
            name="deployment-package-graph-changes",
        )
        fatal_task = asyncio.create_task(
            deployment_controller.fatal_event.wait(), name="deployment-package-fatal"
        )
        tasks = (
            stop_task, fatal_task, management_task, graph_change_task,
            *supervisor.owned_tasks(),
        )
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if not stop.is_set():
            if fatal_task in done and deployment_controller.fatal_error is not None:
                raise deployment_controller.fatal_error
            failed = next(task for task in tasks if task in done)
            await failed
            raise RuntimeError("Deployment supervisor task stopped unexpectedly")
    finally:
        for task in (stop_task, fatal_task, graph_change_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if management_server is not None:
            management_server.should_exit = True
        if management_task is not None:
            await asyncio.gather(management_task, return_exceptions=True)
        try:
            if deployment_controller is not None:
                await deployment_controller.close()
        finally:
            try:
                await supervisor.close()
            finally:
                await store.close()
