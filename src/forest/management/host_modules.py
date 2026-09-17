"""Official host-scoped management modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from forest.management.host_runtime_contract import HostRuntimeManagementContract
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
    module_registry: Any = None


class HostRuntime:
    def __init__(self, host):
        self.host = host
        self.management_contract = HostRuntimeManagementContract()


def _runtime(host, _modules):
    return HostRuntime(host)


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
)
