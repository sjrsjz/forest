"""Bot-scoped management contract for installable component workers."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from forest.management.module_api import (
    ArtifactResult,
    EmptyInput,
    ModuleManagementContract,
    ModuleOperation,
)
from forest.packages import MAX_MANAGEMENT_UI_BYTES
from forest.storage.module_administration import ModuleAdministration


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ComponentIdentity(Body):
    component_id: str = Field(min_length=1, max_length=128)


class HistoryQuery(ComponentIdentity):
    before: int = Field(default=2**63 - 1, ge=1, le=2**63 - 1)


class Revision(ComponentIdentity):
    expected_revision: int = Field(ge=0, strict=True)


class ConfigPlan(Revision):
    values: dict[str, Any]
    source_revision: int | None = Field(default=None, ge=1, le=2**63 - 1, strict=True)


class ConfigApply(Revision):
    plan_id: str = Field(min_length=1, max_length=128)
    plan_digest: str = Field(min_length=1, max_length=128)


class ModuleValidate(ComponentIdentity):
    expected_config_revision: int = Field(ge=1, le=2**63 - 1)
    expected_state_revision: int = Field(ge=0, le=2**63 - 1)


class ModuleState(ComponentIdentity):
    expected_state_revision: int = Field(ge=0, le=2**63 - 1, strict=True)
    reason: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[^\x00-\x1f]*[^\s\x00-\x1f][^\x00-\x1f]*$",
    )


class Invoke(ComponentIdentity):
    service: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any]


class PackageIdentity(Body):
    package_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


def _plan_payload(body: ConfigPlan) -> dict:
    payload = body.model_dump(exclude_none=True)
    values = payload.get("values")
    if isinstance(values, dict) and isinstance(values.get("grants"), list):
        values["grants"] = sorted(values["grants"])
    return payload


def _state_payload(enabled: bool):
    def project(body: ModuleState) -> dict:
        return {**body.model_dump(), "enabled": enabled}

    return project


async def _prepare_plan(request, body):
    request.module.validate(body.component_id, request.payload["values"])
    return request.payload, None


class ComponentWorkerManagementContract(ModuleManagementContract):
    def __init__(self):
        super().__init__(
            (
                ModuleOperation("packages", "query", "config.read", EmptyInput, self.packages),
                ModuleOperation(
                    "configuration", "query", "config.read", ComponentIdentity,
                    self.configuration,
                ),
                ModuleOperation(
                    "configuration-history", "query", "config.history", HistoryQuery,
                    self.configuration_history,
                ),
                ModuleOperation("module", "query", "module.read", ComponentIdentity, self.module),
                ModuleOperation(
                    "module.validate", "query", "module.validate", ModuleValidate,
                    self.module_validate,
                ),
                ModuleOperation(
                    "configuration.plan", "action", "config.plan", ConfigPlan,
                    self.configuration_plan,
                    authorization_payload=_plan_payload,
                    authorization_preparer=_prepare_plan,
                    principal="admin",
                ),
                ModuleOperation(
                    "configuration.apply", "action", "config.apply", ConfigApply,
                    self.configuration_apply,
                    principal="admin",
                    success_status=202,
                ),
                ModuleOperation(
                    "module.enable", "action", "module.enable", ModuleState,
                    self.module_enable,
                    authorization_payload=_state_payload(True),
                    principal="admin",
                    success_status=202,
                ),
                ModuleOperation(
                    "module.disable", "action", "module.disable", ModuleState,
                    self.module_disable,
                    authorization_payload=_state_payload(False),
                    principal="admin",
                    success_status=202,
                ),
                ModuleOperation(
                    "service.invoke", "action", "service.invoke", Invoke,
                    self.service_invoke,
                    principal="admin",
                ),
                ModuleOperation(
                    "package.management-ui",
                    "download",
                    "config.read",
                    PackageIdentity,
                    self.package_management_ui,
                    media_type="text/html; charset=utf-8",
                    max_bytes=MAX_MANAGEMENT_UI_BYTES,
                    disposition="inline",
                ),
            )
        )

    @staticmethod
    def _administration(request):
        return ModuleAdministration(
            request.bot.store,
            control=request.bot.control,
            packages=request.module.module_packages,
        )

    @staticmethod
    def _component_projection(request, component_id: str) -> dict:
        component = request.bot.kernel.components.get(component_id)
        return {
            "component_id": component_id,
            "loaded_revision": request.bot.applied.get(component_id, 0),
            "error_code": request.bot.errors.get(component_id),
            "worker_state": component.worker.state if component is not None else None,
        }

    async def packages(self, request, body):
        return {"packages": request.module.available_packages()}

    async def configuration(self, request, body):
        state = await request.bot.control.read(
            request.ticket, request.operation_id, body.component_id
        )
        return {**state, "component": self._component_projection(request, body.component_id)}

    async def configuration_history(self, request, body):
        return await request.bot.control.history(
            request.ticket, request.operation_id, request.payload
        )

    async def module(self, request, body):
        return await self._administration(request).read(
            request.ticket, request.operation_id, request.payload
        )

    async def module_validate(self, request, body):
        return await self._administration(request).validate(
            request.ticket, request.operation_id, request.payload
        )

    async def configuration_plan(self, request, body):
        return await request.bot.control.plan(
            request.ticket, request.operation_id, request.payload
        )

    async def configuration_apply(self, request, body):
        return await request.bot.control.apply(
            request.ticket, request.operation_id, request.payload
        )

    async def _set_state(self, request):
        result = await self._administration(request).set_state(
            request.ticket, request.operation_id, request.payload
        )
        if request.bot.runtime is not None and not request.bot.control.is_unavailable(result):
            request.bot.runtime.notify()
        return result

    async def module_enable(self, request, body):
        return await self._set_state(request)

    async def module_disable(self, request, body):
        return await self._set_state(request)

    async def service_invoke(self, request, body):
        accepted, parents = await request.bot.control.admit_invocation(
            request.ticket,
            request.operation_id,
            request.payload,
            return_parents=True,
        )
        if accepted is None or request.bot.control.is_unavailable(accepted):
            return accepted
        result = await request.bot.kernel.invoke(
            body.component_id,
            body.service,
            body.payload,
            operation_id=request.operation_id,
            input_parent_ids=parents,
            expected_policy_revision=request.ticket["claims"]["policy_revision"],
        )
        return {"operation_id": request.operation_id, "result": result}

    async def package_management_ui(self, request, body):
        asset = request.module.package(body.package_digest).management_ui()
        if asset is None:
            from forest.errors import ForestError

            raise ForestError("PACKAGE_UI_UNAVAILABLE", "Package has no management UI", 404)
        return ArtifactResult(asset[1])
