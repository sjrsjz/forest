"""Module-owned management contract for the authoritative RouteGraph."""

from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation


class RoutingManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__((
            ModuleOperation("snapshot", "query", "module.read", EmptyInput, self.snapshot),
        ))

    async def snapshot(self, request, body):
        return await request.module.graph.read(request.ticket, request.operation_id)
