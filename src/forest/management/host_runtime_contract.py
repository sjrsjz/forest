"""Host-scoped identity and lifecycle management contract."""

from __future__ import annotations

from forest.management.module_api import (
    EmptyInput,
    ModuleManagementContract,
    ModuleOperation,
)


class HostRuntimeManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation(
                    "runtime", "query", "host.runtime.read", EmptyInput, self.runtime
                ),
            )
        )

    async def runtime(self, request, body):
        host = request.bot
        return {
            "protocol": "forest.host-runtime/1",
            "host_id": host.scope_id,
            "incarnation": host.incarnation,
            "deployment_id": host.deployment_id,
            "module_activation_id": host.module_registry.activation_id,
        }
