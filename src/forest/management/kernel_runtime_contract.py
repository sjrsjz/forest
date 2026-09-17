"""Bot runtime admission projection owned by the non-removable kernel module."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import EmptyInput, ModuleManagementContract, ModuleOperation
from forest.storage.bot_runtime import BotRuntimeAdministration


class Maintenance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_maintenance_epoch: int = Field(ge=0, le=10**18 - 1, strict=True)
    data_admission: bool = Field(strict=True)
    reason: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[^\x00-\x1f]*[^\s\x00-\x1f][^\x00-\x1f]*$",
    )


class KernelRuntimeManagementRuntime:
    """Own the Bot host/admission projection exposed to management clients."""

    def __init__(self, supervisor):
        self.supervisor = supervisor
        self.management_contract = KernelRuntimeManagementContract()

    def status(self) -> dict:
        bot = self.supervisor.bot
        attached = bot.ready and bot.cleanup_task is None and self.supervisor.healthy
        adapter = self.supervisor.module_registry.instances.get("adapter.onebot")
        ingress = getattr(adapter, "shell_ingress", None)
        return {
            "attached": attached,
            "storage_generation": bot.directory_generation,
            "error_code": (
                self.supervisor.failure.code
                if self.supervisor.failure is not None
                else ("BOT_RECOVERING" if not attached else None)
            ),
            "ingress": ingress.snapshot() if ingress is not None else None,
            "runtime_modules": self.supervisor.module_registry.describe(),
            "components": [
                {
                    "component_id": key,
                    "loaded_revision": bot.applied.get(key, 0),
                    "error_code": bot.errors.get(key),
                    "worker_state": bot.kernel.components[key].worker.state
                    if key in bot.kernel.components
                    else None,
                }
                for key in sorted(set(bot.applied) | set(bot.errors))
            ],
        }


class KernelRuntimeManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation(
                    "runtime", "query", "bot.runtime.read", EmptyInput, self.runtime
                ),
                ModuleOperation(
                    "maintenance",
                    "action",
                    "bot.maintenance",
                    Maintenance,
                    self.maintenance,
                    principal="admin",
                    success_status=202,
                ),
            )
        )

    async def runtime(self, request, body):
        state = await BotRuntimeAdministration(
            request.bot.store, control=request.bot.control
        ).read(request.ticket, request.operation_id, {})
        return {**state, "supervisor": request.module.status()}

    async def maintenance(self, request, body):
        payload = body.model_dump()
        result = await BotRuntimeAdministration(
            request.bot.store, control=request.bot.control
        ).set_maintenance(
            request.ticket,
            request.operation_id,
            payload,
            lease=request.bot.kernel._authority(),
        )
        if (
            request.bot.runtime is not None
            and result is not None
            and not request.bot.control.is_unavailable(result)
        ):
            request.bot.runtime.notify()
        return result
