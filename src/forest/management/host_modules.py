"""Official host-scoped management modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from forest.management.host_runtime_contract import HostRuntimeManagementContract
from forest.management.host_storage_contract import HostStorageManagementContract
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    official_artifact,
)


@dataclass
class HostModuleHost:
    scope_kind: str
    scope_id: str
    incarnation: str
    deployment_id: str
    store: Any
    password_slots: Any
    cleanup: Any
    module_registry: Any = None


class HostRuntime:
    def __init__(self, host):
        self.host = host
        self.management_contract = HostRuntimeManagementContract()


class HostStorageRuntime:
    def __init__(self, host):
        self.store = host.store
        self.password_slots = host.password_slots
        self.cleanup = host.cleanup
        self.management_contract = HostStorageManagementContract(self)


def _runtime(host, _modules):
    return HostRuntime(host)


def _storage(host, _modules):
    return HostStorageRuntime(host)


OFFICIAL_HOST_MODULES = (
    RuntimeModuleDescriptor(
        "host.runtime",
        (),
        _runtime,
        official_artifact("host"),
        management=(
            RuntimeManagementContribution(
                "runtime",
                "宿主运行时",
                ("host",),
                "/static/modules/host.runtime.html",
                "/static/host-runtime.js",
                group="runtime",
                order=10,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "host.storage",
        ("host.runtime",),
        _storage,
        official_artifact("host"),
        management=(
            RuntimeManagementContribution(
                "storage",
                "存储资源",
                ("host",),
                "/static/modules/host.storage.html",
                "/static/host-storage.js",
                group="storage",
                order=10,
            ),
        ),
    ),
)
