"""Production process boundary for the deployment-scoped multi-Bot supervisor."""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import uvicorn

from forest.bootstrap_contributions import BootstrapConfigurationRegistry
from forest.bot_host import BotHost
from forest.deployment_access_settings import DeploymentAccessSettings
from forest.deployment_access_settings import bounded_environment_int as _positive_int
from forest.deployment_access_settings import required_environment as _required
from forest.deployment_host_storage_bridge import DeploymentHostStorageBridge
from forest.deployment_kernel_management import (
    BootstrapCatalogKernelQuery,
    BootstrapProfileKernelCommand,
    BotBeginRetirementKernelCommand,
    BotCatalogKernelQuery,
    BotSetDesiredKernelCommand,
    ProvisioningJobKernelCommand,
    ProvisioningJobsKernelQuery,
    ProvisioningReconcileKernelCommand,
    RetirementJobsKernelQuery,
    RetirementRequeueKernelCommand,
    SecretPutKernelCommand,
    StorageRequestKernelCommand,
    StorageRequestsKernelQuery,
    WebAccountKernelCommand,
)
from forest.deployment_management_app import create_deployment_management_app
from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.deployment_package_controller import DeploymentPackageController
from forest.deployment_provisioning_driver import DeploymentProvisioningEffectDriver
from forest.deployment_provisioning_owned_work import DeploymentProvisioningOwnedWorkAuthority
from forest.deployment_retirement_owned_work import DeploymentRetirementOwnedWorkAuthority
from forest.deployment_retirement_ports import DeploymentRetirementPort
from forest.deployment_runtime import DeploymentRuntimeScope
from forest.deployment_storage_owned_work import DeploymentStorageOwnedWorkAuthority
from forest.deployment_storage_ports import DeploymentStoragePortEffectDriver
from forest.deployment_storage_settlement import DeploymentStorageSettlementAuthority
from forest.deployment_web_bootstrap import DeploymentWebBootstrap
from forest.errors import ForestError
from forest.host_package_controller import HostPackageController
from forest.host_runtime import HostRuntimeScope
from forest.host_storage_postgres_effect import (
    HostPostgresMaterializationEffect,
    HostPostgresReclamationEffect,
)
from forest.host_supervisor import HostSupervisor
from forest.multi_bot_supervisor import MultiBotSupervisor
from forest.official_bot_provisioning import (
    OfficialBotCatalogSettingsResolver,
    OfficialBotProvisioner,
    OfficialBotSettingsResolver,
    official_bootstrap_contributions,
)
from forest.packages import PackageCatalog, auto_install_official_sources
from forest.storage.deployment_audit import DeploymentAuditRepository
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_graph_changes import DeploymentGraphChangeAuthority
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_retirement_jobs import DeploymentRetirementJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_dispatch import DeploymentStorageDispatchAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_storage_requests import DeploymentStorageRequestAuthority
from forest.storage.deployment_store import DeploymentStore
from forest.storage.deployment_web_authority import DeploymentWebAuthorityRepository
from forest.storage.deployment_web_identity import DeploymentWebIdentityAuthority
from forest.storage.host_graph_changes import HostGraphChangeAuthority
from forest.storage.host_identity import HostIdentityAuthority
from forest.storage.host_store import HostStore
from forest.storage.host_web_authority import HostWebAuthorityRepository
from forest.web_authority_ticket import WebAuthorityTicketAuthority


class _EmbeddedManagementServer(uvicorn.Server):
    """Leave process signal ownership with the deployment Supervisor."""

    def capture_signals(self):
        return nullcontext()


def _host_dsn() -> str:
    configured = os.environ.get("FOREST_HOST_DSN")
    if configured:
        return configured
    password_path = Path(_required("FOREST_HOST_DATABASE_PASSWORD_FILE"))
    try:
        password = password_path.read_text(encoding="utf-8").strip()
    except OSError:
        raise ForestError(
            "SUPERVISOR_DATABASE_CREDENTIAL_UNAVAILABLE",
            "Host database credential is unavailable", 503,
        ) from None
    host = os.environ.get("FOREST_HOST_DATABASE_HOST", "postgres")
    port = _positive_int("FOREST_HOST_DATABASE_PORT", 5432, 1, 65535)
    database = os.environ.get("FOREST_HOST_DATABASE_NAME", "forest_host")
    user = os.environ.get("FOREST_HOST_DATABASE_USER", "forest_host")
    if not password or not host or not database or not user:
        raise ForestError(
            "SUPERVISOR_CONFIGURATION_INVALID", "Host database identity is invalid", 422,
        )
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{quote(database, safe='')}"
    )


def _host_id() -> str:
    inline = os.environ.get("FOREST_HOST_ID")
    path = os.environ.get("FOREST_HOST_ID_FILE")
    if bool(inline) == bool(path):
        raise ForestError(
            "SUPERVISOR_HOST_ID_REQUIRED",
            "Exactly one stable host identity source is required", 422,
        )
    if path:
        try:
            inline = Path(path).read_text(encoding="ascii")
        except (OSError, UnicodeError):
            raise ForestError(
                "SUPERVISOR_HOST_ID_UNAVAILABLE", "Host identity is unavailable", 503
            ) from None
    if re.fullmatch(r"host/[a-f0-9]{32}", inline or "") is None:
        raise ForestError(
            "SUPERVISOR_HOST_ID_INVALID", "Host identity is invalid", 422
        )
    return inline


@dataclass(frozen=True)
class SupervisorSettings:
    deployment_id: str
    host_id: str
    dsn: str = field(repr=False)
    host_dsn: str = field(repr=False)
    master_keyring: DeploymentMasterKeyring = field(repr=False)
    web_bootstrap: DeploymentWebBootstrap = field(repr=False)
    poll_seconds: float = 1.0
    max_starting: int = 2
    management_host: str = "127.0.0.1"
    management_port: int = 8081
    management_origin: str = "http://127.0.0.1:8081"

    @classmethod
    def from_environment(cls) -> "SupervisorSettings":
        deployment = DeploymentAccessSettings.from_environment()
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
            deployment_id=deployment.deployment_id,
            host_id=_host_id(),
            dsn=deployment.dsn,
            host_dsn=_host_dsn(),
            master_keyring=deployment.master_keyring,
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
    host_store = HostStore(settings.host_dsn, settings.host_id)
    try:
        await host_store.open()
    except BaseException:
        await store.close()
        raise
    incarnation = "incarnation/" + str(uuid4())
    host_supervisor = HostSupervisor(
        HostIdentityAuthority(host_store), incarnation=incarnation,
    )
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
    host_ticket_authority = WebAuthorityTicketAuthority(
        issuer_id=settings.deployment_id,
        active_key_id=web_identity.active_key_id,
        key_resolver=web_identity.key_resolver,
        session_validator=web_identity.revalidate_session,
    )
    host_graph_changes = HostGraphChangeAuthority(
        host_store, ticket_authority=host_ticket_authority,
    )
    host_web_receipts = HostWebAuthorityRepository(
        host_store, ticket_authority=host_ticket_authority,
    )
    storage_profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)
    storage_requests = DeploymentStorageRequestAuthority(store, secrets=secrets)
    storage_dispatch = DeploymentStorageDispatchAuthority(store)
    storage_settlement = DeploymentStorageSettlementAuthority(
        store, secrets=secrets, profiles=storage_profiles,
        host_id=settings.host_id, maintenance_dsn=settings.dsn,
    )
    bootstrap_profiles = DeploymentBootstrapProfileAuthority(store, secrets=secrets)
    jobs = DeploymentProvisioningJobAuthority(store)
    retirement_jobs = DeploymentRetirementJobAuthority(store)
    package_catalog = PackageCatalog(
        Path(os.environ.get("FOREST_PACKAGE_CATALOG_ROOT", ".forest/packages"))
    )
    bootstrap_registry = BootstrapConfigurationRegistry(
        official_bootstrap_contributions(package_catalog)
    )
    deployment_controller = None
    host_controller = None

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
    provisioner = OfficialBotProvisioner(
        settings_resolver=settings_resolver,
    )
    supervisor = MultiBotSupervisor(
        catalog,
        storage_profiles=storage_profiles,
        settings_resolver=OfficialBotCatalogSettingsResolver(
            bootstrap_profiles=bootstrap_profiles,
        ),
        host_factory=bot_host_factory,
        host_id=settings.host_id,
        incarnation=incarnation,
        poll_seconds=settings.poll_seconds,
        max_starting=settings.max_starting,
    )
    host_storage_bridge = DeploymentHostStorageBridge(
        requests=storage_requests, profiles=storage_profiles,
        deployment_supervisor=supervisor,
        host_supervisor=host_supervisor,
        host_controller_provider=lambda: host_controller,
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
    host_graph_change_task = None
    host_fatal_task = None
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(event, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(event, lambda *_args: loop.call_soon_threadsafe(stop.set))
    stop_task = None
    fatal_task = None
    try:
        await host_supervisor.start()
        host_runtime = HostRuntimeScope(
            host_store, host_supervisor,
            graph_change_authority=host_graph_changes,
        )
        host_runtime.effect_driver_factories[
            "host.postgresql-materializer"
        ] = lambda configuration, revision, identity, declaration: (
            HostPostgresMaterializationEffect(
                configuration, revision, identity, declaration,
                store=host_store, supervisor=host_supervisor,
                maintenance_dsn=settings.dsn,
                credential_resolver=secrets.resolve_text_for_purpose,
            )
        )
        host_runtime.effect_driver_factories[
            "host.postgresql-reclaimer"
        ] = lambda configuration, revision, identity, declaration: (
            HostPostgresReclamationEffect(
                configuration, revision, identity, declaration,
                store=host_store, supervisor=host_supervisor,
                maintenance_dsn=settings.dsn,
            )
        )
        host_controller = HostPackageController(
            host_runtime, package_catalog,
            official_sources=auto_install_official_sources("host"),
        )
        await host_controller.start()
        await supervisor.start()
        await web_identity.initialize_setup(settings.web_bootstrap)
        deployment_runtime = DeploymentRuntimeScope(
            store, supervisor, package_catalog=package_catalog,
            graph_change_authority=graph_changes,
            kernel_query_handlers={
                ("deployment.bot-catalog", "deployment.bot-catalog-page"):
                    BotCatalogKernelQuery(catalog),
                ("deployment.retirement", "deployment.retirement-jobs-page"):
                    RetirementJobsKernelQuery(retirement_jobs),
                ("deployment.provisioning", "deployment.provisioning-jobs-page"):
                    ProvisioningJobsKernelQuery(jobs),
                ("deployment.provisioning", "deployment.bootstrap-catalog"):
                    BootstrapCatalogKernelQuery(bootstrap_registry),
                ("deployment.storage-orchestrator", "deployment.storage-requests-page"):
                    StorageRequestsKernelQuery(storage_dispatch),
            },
            kernel_management_handlers={
                ("deployment.bot-catalog", "deployment.bot-set-desired"):
                    BotSetDesiredKernelCommand(catalog),
                ("deployment.retirement", "deployment.bot-begin-retirement"):
                    BotBeginRetirementKernelCommand(
                        catalog, notify=notify_deployment_work,
                    ),
                ("deployment.retirement", "deployment.retirement-requeue"):
                    RetirementRequeueKernelCommand(
                        retirement_jobs, notify=notify_deployment_work,
                    ),
                ("deployment.secrets", "deployment.secret-put"):
                    SecretPutKernelCommand(secrets),
                **{
                    ("deployment.accounts", kind): WebAccountKernelCommand(web_identity, kind)
                    for kind in WebAccountKernelCommand._METHODS
                },
                ("deployment.storage-orchestrator", "deployment.storage-request"):
                    StorageRequestKernelCommand(storage_requests, host_id=settings.host_id),
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
        deployment_runtime.kernel_owned_work_authorities["deployment.retirement"] = (
            DeploymentRetirementOwnedWorkAuthority(
                deployment_runtime, retirement_jobs, catalog, host_storage_bridge,
            )
        )
        deployment_runtime.kernel_owned_work_authorities[
            "deployment.storage-orchestrator"
        ] = DeploymentStorageOwnedWorkAuthority(deployment_runtime, storage_dispatch)
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
        deployment_runtime.effect_driver_factories[
            "deployment.retirement-port"
        ] = lambda configuration, revision, identity, declaration: (
            DeploymentRetirementPort(
                configuration, revision, identity, declaration,
                runtime=deployment_runtime, jobs=retirement_jobs,
                catalog=catalog, profiles=storage_profiles,
                bridge=host_storage_bridge,
            )
        )
        deployment_runtime.effect_driver_factories[
            "deployment.host-storage-port"
        ] = lambda configuration, revision, identity, declaration: (
            DeploymentStoragePortEffectDriver(
                configuration, revision, identity, declaration,
                runtime=deployment_runtime, dispatch=storage_dispatch,
                requests=storage_requests, profiles=storage_profiles,
                secrets=secrets, bridge=host_storage_bridge,
                settlement=storage_settlement,
            )
        )
        deployment_controller = DeploymentPackageController(
            deployment_runtime, package_catalog,
            official_sources=auto_install_official_sources("deployment"),
        )
        await deployment_controller.start()
        management_server = _EmbeddedManagementServer(
            uvicorn.Config(
                create_deployment_management_app(
                    deployment_id=settings.deployment_id,
                    deployment_registry_provider=lambda: deployment_controller.registry,
                    deployment_graph_changes=graph_changes,
                    deployment_change_previewer=deployment_controller.preview_change,
                    host_id=settings.host_id,
                    host_controller=host_controller,
                    host_web_receipts=host_web_receipts,
                    host_graph_changes=host_graph_changes,
                    host_change_previewer=host_controller.preview_change,
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
        host_graph_change_task = asyncio.create_task(
            host_controller.serve_graph_changes(host_graph_changes),
            name="host-package-graph-changes",
        )
        fatal_task = asyncio.create_task(
            deployment_controller.fatal_event.wait(), name="deployment-package-fatal"
        )
        host_fatal_task = asyncio.create_task(
            host_controller.fatal_event.wait(), name="host-package-fatal"
        )
        tasks = (
            stop_task, fatal_task, host_fatal_task, management_task,
            graph_change_task, host_graph_change_task,
            *host_supervisor.owned_tasks(),
            *supervisor.owned_tasks(),
        )
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if not stop.is_set():
            if fatal_task in done and deployment_controller.fatal_error is not None:
                raise deployment_controller.fatal_error
            if host_fatal_task in done and host_controller.fatal_error is not None:
                raise host_controller.fatal_error
            completed = [task for task in tasks if task in done]
            for task in completed:
                if not task.cancelled():
                    error = task.exception()
                    if error is not None:
                        raise error
            for task in completed:
                if task.cancelled():
                    raise RuntimeError(
                        f"Deployment supervisor task {task.get_name()} was cancelled"
                    )
            raise RuntimeError(
                "Deployment supervisor task stopped unexpectedly: "
                + ", ".join(task.get_name() for task in completed)
            )
    finally:
        for task in (
            stop_task, fatal_task, host_fatal_task,
            graph_change_task, host_graph_change_task,
        ):
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
                if host_controller is not None:
                    await host_controller.close()
            finally:
                try:
                    await supervisor.close()
                finally:
                    try:
                        await host_supervisor.close()
                    finally:
                        try:
                            await host_store.close()
                        finally:
                            await store.close()
