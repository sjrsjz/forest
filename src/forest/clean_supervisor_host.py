"""Production process boundary for the deployment-scoped multi-Bot supervisor."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from forest.clean_multi_bot_supervisor import CleanMultiBotSupervisor
from forest.deployment_provisioning import DeploymentProvisioningExecutor
from forest.errors import ForestError
from forest.official_bot_provisioning import (
    OfficialBotCatalogSettingsResolver,
    OfficialBotProvisioner,
    OfficialBotSettingsResolver,
)
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ForestError("SUPERVISOR_CONFIGURATION_REQUIRED", f"{name} is required", 422)
    return value


@dataclass(frozen=True)
class CleanSupervisorSettings:
    deployment_id: str
    dsn: str
    active_key_id: str
    master_key: bytes
    poll_seconds: float = 1.0
    max_starting: int = 2

    @classmethod
    def from_environment(cls) -> "CleanSupervisorSettings":
        key_path = Path(_required("FOREST_DEPLOYMENT_MASTER_KEY_FILE"))
        try:
            encoded = key_path.read_bytes().strip()
            key = base64.b64decode(encoded, validate=True)
        except (OSError, ValueError, binascii.Error):
            raise ForestError(
                "SUPERVISOR_MASTER_KEY_INVALID",
                "Deployment master key file must contain one base64 value",
                422,
            ) from None
        if len(key) != 32:
            raise ForestError(
                "SUPERVISOR_MASTER_KEY_INVALID",
                "Deployment master key must decode to 32 bytes",
                422,
            )
        try:
            poll_seconds = float(os.environ.get("FOREST_SUPERVISOR_POLL_SECONDS", "1"))
            max_starting = int(os.environ.get("FOREST_SUPERVISOR_MAX_STARTING", "2"))
        except ValueError:
            raise ForestError(
                "SUPERVISOR_CONFIGURATION_INVALID",
                "Supervisor concurrency or poll interval is invalid",
                422,
            ) from None
        if not 0.05 <= poll_seconds <= 30 or not 1 <= max_starting <= 32:
            raise ForestError(
                "SUPERVISOR_CONFIGURATION_INVALID",
                "Supervisor concurrency or poll interval is outside its bound",
                422,
            )
        return cls(
            deployment_id=os.environ.get("FOREST_DEPLOYMENT_ID", "forest/default"),
            dsn=_required("FOREST_DEPLOYMENT_DSN"),
            active_key_id=os.environ.get("FOREST_DEPLOYMENT_ACTIVE_KEY_ID", "master/default/1"),
            master_key=key,
            poll_seconds=poll_seconds,
            max_starting=max_starting,
        )


async def run_clean_supervisor(settings: CleanSupervisorSettings) -> None:
    store = CleanDeploymentStore(settings.dsn, settings.deployment_id)
    await store.open()
    incarnation = "incarnation/" + str(uuid4())
    secrets = DeploymentSecretAuthority(
        store,
        active_key_id=settings.active_key_id,
        key_resolver=lambda key_id: (
            settings.master_key if key_id == settings.active_key_id else None
        ),
    )
    catalog = DeploymentCatalogRepository(store)
    storage_profiles = DeploymentStorageProfileAuthority(store, secrets=secrets)
    bootstrap_profiles = DeploymentBootstrapProfileAuthority(store, secrets=secrets)
    jobs = DeploymentProvisioningJobAuthority(store)
    settings_resolver = OfficialBotSettingsResolver(secrets=secrets)
    executor = DeploymentProvisioningExecutor(
        jobs=jobs,
        catalog=catalog,
        storage_profiles=storage_profiles,
        bootstrap_profiles=bootstrap_profiles,
        provisioner=OfficialBotProvisioner(settings_resolver=settings_resolver),
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
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(event, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(event, lambda *_args: loop.call_soon_threadsafe(stop.set))
    stop_task = None
    try:
        await supervisor.start()
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
        tasks = (stop_task, *supervisor.owned_tasks())
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if not stop.is_set():
            failed = next(task for task in tasks if task in done)
            await failed
            raise RuntimeError("Deployment supervisor task stopped unexpectedly")
    finally:
        if stop_task is not None and not stop_task.done():
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
        await supervisor.close()
        await store.close()
