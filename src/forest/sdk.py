"""Small native SDK. Context handles cross an authenticated host boundary.

Persistent jobs and arbitrary subprocesses are not provided by this P0 SDK.
Use owned async tasks for local work; per-call children complete before reply.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import json
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from forest.errors import ForestError


def service(name: str):
    if not isinstance(name, str) or not name or len(name) > 128:
        raise ValueError("Service name must contain 1 to 128 characters")

    def decorate(function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A service must be an async method(payload, ctx)")
        function.__forest_service__ = name
        return function

    return decorate


def declared_services(plugin: Plugin) -> dict[str, Callable[..., Awaitable[Any]]]:
    result = {}
    for _, method in inspect.getmembers(plugin, predicate=inspect.ismethod):
        name = getattr(method, "__forest_service__", None)
        if name is None:
            continue
        parameters = list(inspect.signature(method).parameters.values())
        if (
            not inspect.iscoroutinefunction(method)
            or [parameter.name for parameter in parameters] != ["payload", "ctx"]
            or any(
                parameter.kind not in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                for parameter in parameters
            )
            or name in result
        ):
            raise ForestError(
                "SERVICE_DECLARATION_INVALID",
                "Service declarations must be unique async methods(payload, ctx)",
                422,
            )
        result[name] = method
    return result


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass
class CallScope:
    call_id: str
    deadline_at: float
    operation_id: str
    active: bool = True


_current_call: contextvars.ContextVar[CallScope | None] = contextvars.ContextVar(
    "forest_current_call", default=None
)


def _require_call(scope: CallScope | None) -> CallScope:
    if scope is None or not scope.active or _current_call.get() is not scope:
        raise ForestError(
            "CALL_CONTEXT_REQUIRED",
            "This handle is not bound to the current live business call",
            403,
        )
    return scope


class OwnedTasks:
    def __init__(self, limit: int = 64):
        self.limit = limit
        self.tasks: dict[asyncio.Task, CallScope | None] = {}
        self.failures: dict[str, list[str]] = {}
        self.background_failures: deque[str] = deque(maxlen=8)
        self.closed = False

    def spawn_owned(self, coroutine, *, name: str | None = None) -> asyncio.Task:
        if not inspect.iscoroutine(coroutine):
            raise TypeError("spawn_owned requires a coroutine")
        scope = _current_call.get()
        if self.closed or (scope is not None and not scope.active) or len(self.tasks) >= self.limit:
            coroutine.close()
            raise ForestError(
                "OWNED_TASK_LIMIT", "Task owner is closed or has reached its resource limit", 429
            )
        task = asyncio.create_task(coroutine, name=name)
        self.tasks[task] = scope
        task.add_done_callback(self._done)
        return task

    def _done(self, task: asyncio.Task) -> None:
        scope = self.tasks.pop(task, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            if scope is None:
                self.background_failures.append(type(error).__name__)
            else:
                failures = self.failures.setdefault(scope.call_id, [])
                if len(failures) < 8:
                    failures.append(type(error).__name__)

    async def finish_call(self, scope: CallScope) -> None:
        while pending := [task for task, owner in self.tasks.items() if owner is scope]:
            await asyncio.gather(*pending, return_exceptions=True)
        if self.failures.pop(scope.call_id, None):
            raise ForestError("OWNED_TASK_FAILED", "A supervised child task failed", 500)

    async def close_call(self, scope: CallScope) -> None:
        scope.active = False
        pending = [task for task, owner in self.tasks.items() if owner is scope]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.failures.pop(scope.call_id, None)

    async def close(self) -> None:
        self.closed = True
        pending = list(self.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


class StateHandle:
    def __init__(self, rpc: Callable[[str, str, dict], Awaitable[Any]], scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def _call(self, action: str, payload: dict) -> Any:
        scope = _require_call(self._scope)
        return await self._rpc(scope.call_id, action, payload)

    async def get(self, key: str) -> Any:
        return await self._call("get", {"key": key})

    async def commit(self, key: str, value: Any, expected_revision: int, operation_id: str) -> Any:
        return await self._call(
            "commit",
            {
                "key": key,
                "value": value,
                "expected_revision": expected_revision,
                "operation_id": operation_id,
            },
        )


class TasksHandle:
    def __init__(self, owner: OwnedTasks, scope: CallScope | None):
        self._owner = owner
        self._scope = scope

    def spawn_owned(self, coroutine, *, name: str | None = None) -> asyncio.Task:
        try:
            if self._scope is None:
                if _current_call.get() is not None:
                    raise ForestError(
                        "CALL_CONTEXT_REQUIRED",
                        "Component task handles cannot be reused inside a business call",
                        403,
                    )
            else:
                _require_call(self._scope)
        except ForestError:
            if inspect.iscoroutine(coroutine):
                coroutine.close()
            raise
        return self._owner.spawn_owned(coroutine, name=name)


class ServicesHandle:
    def __init__(self, rpc: Callable[[str, dict], Awaitable[Any]] | None, scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def call(
        self,
        service: str,
        payload: Any,
        *,
        operation_id: str,
        contract_major: int = 1,
        timeout: float | None = None,
    ) -> Any:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No component service gateway is available", 403)
        if not isinstance(service, str) or not 1 <= len(service) <= 128:
            raise ForestError("INVALID_SERVICE_REQUEST", "A bounded service name is required", 422)
        if type(contract_major) is not int or contract_major < 1:
            raise ForestError(
                "INVALID_SERVICE_REQUEST", "Contract major must be a positive integer", 422
            )
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ForestError(
                "INVALID_SERVICE_REQUEST", "A stable service operation ID is required", 422
            )
        if timeout is not None and (type(timeout) not in {int, float} or not 0 < timeout <= 120):
            raise ForestError(
                "INVALID_SERVICE_REQUEST",
                "Service timeout must be greater than zero and at most 120 seconds",
                422,
            )
        return await self._rpc(
            scope.call_id,
            {
                "service": service,
                "payload": payload,
                "contract_major": contract_major,
                "operation_id": operation_id,
                "timeout": timeout,
            },
        )


class PluginContext:
    def __init__(
        self,
        config: Mapping,
        rpc: Callable[[str, str, dict], Awaitable[Any]],
        *,
        task_limit: int = 64,
        scope: CallScope | None = None,
        task_owner: OwnedTasks | None = None,
        service_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
    ):
        self.config: Mapping[str, Any] = _freeze(config)
        self._scope = scope
        self._rpc = rpc
        self._service_rpc = service_rpc
        self._task_owner = task_owner if task_owner is not None else OwnedTasks(task_limit)
        self.state = StateHandle(rpc, scope)
        self.tasks = TasksHandle(self._task_owner, scope)
        self.operations = OperationsHandle(scope)
        self.services = ServicesHandle(service_rpc, scope)

    def _for_call(self, scope: CallScope) -> PluginContext:
        return PluginContext(
            self.config,
            self._rpc,
            scope=scope,
            task_owner=self._task_owner,
            service_rpc=self._service_rpc,
        )

    @property
    def call_id(self) -> str:
        return _require_call(self._scope).call_id


class OperationsHandle:
    def __init__(self, scope: CallScope | None):
        self._scope = scope

    def stable(self, step: str) -> str:
        scope = _require_call(self._scope)
        if not isinstance(step, str) or not 1 <= len(step) <= 256:
            raise ForestError(
                "OPERATION_STEP_INVALID",
                "Stable operation step must contain 1 to 256 characters",
                422,
            )
        encoded = json.dumps(
            [scope.operation_id, step], ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return "op:" + hashlib.sha256(encoded).hexdigest()


class Plugin:
    """Lifecycle hooks execute inside an isolated process, before admission."""

    async def prepare(self, ctx: PluginContext, config: Mapping, restored_state: Any) -> None:
        self.ctx = ctx
        self._snapshot = restored_state

    async def start(self) -> None:
        pass

    async def health(self) -> dict:
        return {"healthy": True}

    async def quiesce(self, reason: str, deadline: float) -> None:
        pass

    async def snapshot(self) -> Any:
        return self._snapshot

    async def restore(self, state: Any) -> None:
        self._snapshot = state

    async def resume(self) -> None:
        pass

    async def stop(self, reason: str) -> None:
        pass
