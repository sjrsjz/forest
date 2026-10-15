"""Bot-owned proxy for one exact control-package effect driver export."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_contracts import RuntimeServiceInvocation
from forest.runtime_generation import BoundGenerationIdentity


class ControlExportEffectDriver:
    """A Bot Generation owns this proxy; the Host owns the provider Worker."""

    def __init__(
        self,
        *, export: Mapping[str, Any], identity: BoundGenerationIdentity,
        controller_provider: Callable[[], Any],
    ) -> None:
        self._kind = export["kind"]
        self._source_ref = export["source_ref"]
        self._identity = identity
        self._controller_provider = controller_provider
        self._state = "NEW"

    def _controller(self):
        controller = self._controller_provider()
        if controller is None:
            raise ForestError(
                "CONTROL_DRIVER_HOST_UNAVAILABLE",
                "Control driver provider is unavailable", 503,
            )
        return controller

    def _require_source(self) -> None:
        if not any(
            item["kind"] == self._kind and item["source_ref"] == self._source_ref
            for item in self._controller().driver_exports()
        ):
            raise ForestError(
                "CONTROL_DRIVER_SOURCE_STALE",
                "Bot driver binding no longer names an active control Generation", 409,
            )

    async def start(self) -> None:
        if self._state != "NEW":
            raise ForestError(
                "CONTROL_DRIVER_LIFECYCLE_INVALID", "Driver has already started", 409,
            )
        self._require_source()
        self._state = "ACTIVE"

    async def quiesce(self) -> None:
        if self._state != "ACTIVE":
            raise ForestError(
                "CONTROL_DRIVER_LIFECYCLE_INVALID", "Driver is not active", 409,
            )
        self._state = "QUIESCENT"

    async def resume(self) -> None:
        if self._state != "QUIESCENT":
            raise ForestError(
                "CONTROL_DRIVER_LIFECYCLE_INVALID", "Driver is not quiescent", 409,
            )
        self._require_source()
        self._state = "ACTIVE"

    async def close(self) -> None:
        self._state = "CLOSED"

    async def invoke(self, payload: dict, *, scope, invocation: RuntimeServiceInvocation) -> dict:
        if self._state != "ACTIVE":
            raise ForestError(
                "CONTROL_DRIVER_NOT_ACTIVE", "Driver proxy is not accepting calls", 409,
            )
        if (
            scope.scope_kind != "bot"
            or not scope.bot_id
            or scope.component_id != self._identity.component_id
            or scope.generation_id != self._identity.generation_id
        ):
            raise ForestError(
                "CONTROL_DRIVER_CONSUMER_STALE",
                "Driver call differs from its Bot Generation binding", 409,
            )
        operation_id = "driver:" + digest_json({
            "bot_id": scope.bot_id,
            "component_id": scope.component_id,
            "kind": self._kind,
            "source_ref": self._source_ref,
            "operation_id": invocation.operation_id,
        })[7:]
        host_invocation = RuntimeServiceInvocation(
            operation_id=operation_id,
            timeout_seconds=invocation.timeout_seconds,
            causal_root_kind=invocation.causal_root_kind,
            causal_root_id=invocation.causal_root_id,
            parent_call_id=scope.call_id,
            read_only=invocation.read_only,
        )
        return await self._controller().invoke_driver_export(
            kind=self._kind,
            source_ref=self._source_ref,
            caller_component_id=scope.component_id,
            payload=payload,
            invocation=host_invocation,
        )


def control_export_effect_driver_factory(
    *, export: Mapping[str, Any], controller_provider: Callable[[], Any],
):
    frozen_export = dict(export)

    def create(_configuration, _revision, identity, _dependencies):
        return ControlExportEffectDriver(
            export=frozen_export,
            identity=identity,
            controller_provider=controller_provider,
        )

    return create
