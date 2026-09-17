"""Read-only module management surface for OneBot bindings and conversations."""

from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation


class OneBotManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__((
            ModuleOperation("bindings", "query", "binding.read", EmptyInput, self.bindings),
            ModuleOperation(
                "conversations", "query", "message.read", EmptyInput, self.conversations
            ),
        ))

    async def bindings(self, request, body):
        return {
            "bindings": await request.module.bindings.list(
                request.ticket, request.operation_id
            )
        }

    async def conversations(self, request, body):
        return await request.module.messages.list_conversations(
            request.ticket, request.operation_id
        )
