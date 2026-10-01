"""Small native SDK. Context handles cross an authenticated host boundary.

Durable component jobs cross that boundary explicitly. Arbitrary subprocesses
remain outside the SDK; per-call owned nested tasks complete before reply.
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import inspect
import json
import re
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from forest.command_contract import (
    command_identifier,
    normalize_command_name,
    validate_argument_mode,
    validate_command_permission,
    validate_command_priority,
)
from forest.errors import ForestError


def plugin(
    id: str,
    version: str,
    scope: str = "bot",
    *,
    requests=(),
    **declaration,
):
    """Mark the single package component; build metadata is read statically from its AST."""

    metadata = {"id": id, "version": version, "scope": scope, "requests": requests, **declaration}

    def decorate(candidate):
        if not inspect.isclass(candidate):
            raise TypeError("@plugin must decorate a class")
        selected = candidate
        if not issubclass(candidate, Plugin):
            selected = type(
                candidate.__name__,
                (candidate, Plugin),
                {"__module__": candidate.__module__, "__qualname__": candidate.__qualname__},
            )
        selected.__forest_plugin__ = metadata
        return selected

    return decorate


def service(
    name: str,
    *,
    semantics: str,
    contract: str = "1.0",
    input_schema=None,
    output_schema=None,
):
    if not isinstance(name, str) or not name or len(name) > 128:
        raise ValueError("Service name must contain 1 to 128 characters")
    if semantics not in {"at-most-once", "idempotent", "read"}:
        raise ValueError("Service semantics must be at-most-once, idempotent or read")

    def decorate(function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A service must be an async method(payload, ctx)")
        function.__forest_service__ = name
        function.__forest_service_manifest__ = {
            "name": name,
            "contract": contract,
            "semantics": semantics,
            "input_schema": input_schema,
            "output_schema": output_schema,
        }
        return function

    return decorate


def subscribe(
    subscriber_id: str,
    topic: str,
    contract: str,
    *,
    schema=None,
    max_attempts: int | None = None,
):
    identifier = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
    if not identifier.fullmatch(subscriber_id) or not identifier.fullmatch(topic):
        raise ValueError("Subscriber ID and topic must be bounded identifiers")
    if not re.fullmatch(r"[1-9]\d*\.\d+", contract):
        raise ValueError("Subscription contract must be an exact major.minor version")

    def decorate(function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("An event subscriber must be an async method(event, ctx)")
        function.__forest_subscription__ = {
            "subscriber_id": subscriber_id,
            "topic": topic,
            "contract": contract,
        }
        function.__forest_subscription_manifest__ = {
            **function.__forest_subscription__,
            "schema": schema,
            "max_attempts": max_attempts,
        }
        return function

    return decorate


def job(
    handler_id: str,
    *,
    input_schema=None,
    max_attempts: int | None = None,
    timeout_s: float | None = None,
    max_concurrency: int | None = None,
):
    identifier = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
    if not isinstance(handler_id, str) or not identifier.fullmatch(handler_id):
        raise ValueError("Job handler ID must be a bounded identifier")

    def decorate(function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A component job must be an async method(task, ctx)")
        function.__forest_job__ = handler_id
        function.__forest_job_manifest__ = {
            "handler_id": handler_id,
            "input_schema": input_schema,
            "max_attempts": max_attempts,
            "timeout_s": timeout_s,
            "max_concurrency": max_concurrency,
        }
        return function

    return decorate


def command(
    name: str,
    permission: str,
    *,
    id: str | None = None,
    args: str = "none",
    schema=None,
    priority: int = 0,
):
    """Declare a deterministic business command; execution remains host-authorized."""

    normalized_name = normalize_command_name(name)
    command_id = command_identifier(normalized_name, id)
    permission = validate_command_permission(permission)
    argument_mode = validate_argument_mode(args)
    priority = validate_command_priority(priority)
    if schema is not None and not isinstance(schema, (str, dict)):
        raise ValueError("Command schema must be a package path or JSON object")

    def decorate(function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A command must be an async method(event, ctx)")
        function.__forest_command__ = {
            "command_id": command_id,
            "name": normalized_name,
            "handler": function.__name__,
            "permission": permission,
            "argument_mode": argument_mode,
            "schema": schema,
            "priority": priority,
        }
        return function

    return decorate


def message_handler(handler_id: str, *, priority: int = 0):
    """Declare an ordinary-message handler executed by the conversation Actor."""

    identifier = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
    if not isinstance(handler_id, str) or not identifier.fullmatch(handler_id):
        raise ValueError("Message handler ID must be a bounded identifier")
    if type(priority) is not int or not -100 <= priority <= 100:
        raise ValueError("Message handler priority must be an integer from -100 to 100")

    def decorate(function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A message handler must be an async method(event, ctx)")
        function.__forest_message_handler__ = {
            "handler_id": handler_id,
            "handler": function.__name__,
            "priority": priority,
        }
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


def declared_subscriptions(plugin: Plugin) -> dict[str, tuple[dict, Callable[..., Awaitable[Any]]]]:
    result = {}
    for _, method in inspect.getmembers(plugin, predicate=inspect.ismethod):
        declaration = getattr(method, "__forest_subscription__", None)
        if declaration is None:
            continue
        parameters = list(inspect.signature(method).parameters.values())
        subscriber_id = declaration["subscriber_id"]
        if (
            not inspect.iscoroutinefunction(method)
            or [parameter.name for parameter in parameters] != ["event", "ctx"]
            or any(
                parameter.kind not in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                for parameter in parameters
            )
            or subscriber_id in result
        ):
            raise ForestError(
                "SUBSCRIPTION_DECLARATION_INVALID",
                "Subscriptions must be unique async methods(event, ctx)",
                422,
            )
        result[subscriber_id] = (dict(declaration), method)
    return result


def declared_jobs(plugin: Plugin) -> dict[str, Callable[..., Awaitable[Any]]]:
    result = {}
    for _, method in inspect.getmembers(plugin, predicate=inspect.ismethod):
        handler_id = getattr(method, "__forest_job__", None)
        if handler_id is None:
            continue
        parameters = list(inspect.signature(method).parameters.values())
        if (
            not inspect.iscoroutinefunction(method)
            or [parameter.name for parameter in parameters] != ["task", "ctx"]
            or any(
                parameter.kind not in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                for parameter in parameters
            )
            or handler_id in result
        ):
            raise ForestError(
                "JOB_DECLARATION_INVALID",
                "Jobs must be unique async methods(task, ctx)",
                422,
            )
        result[handler_id] = method
    return result


def declared_commands(
    plugin: Plugin,
) -> dict[str, tuple[dict, Callable[..., Awaitable[Any]]]]:
    result = {}
    names = set()
    for _, method in inspect.getmembers(plugin, predicate=inspect.ismethod):
        declaration = getattr(method, "__forest_command__", None)
        if declaration is None:
            continue
        parameters = list(inspect.signature(method).parameters.values())
        command_id = declaration["command_id"]
        name = declaration["name"]
        if (
            not inspect.iscoroutinefunction(method)
            or [parameter.name for parameter in parameters] != ["event", "ctx"]
            or any(
                parameter.kind not in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                for parameter in parameters
            )
            or command_id in result
            or name in names
        ):
            raise ForestError(
                "COMMAND_DECLARATION_INVALID",
                "Commands must be unique async methods(event, ctx)",
                422,
            )
        result[command_id] = (dict(declaration), method)
        names.add(name)
    return result


def declared_message_handlers(
    plugin: Plugin,
) -> dict[str, tuple[dict, Callable[..., Awaitable[Any]]]]:
    result = {}
    methods = set()
    for _, method in inspect.getmembers(plugin, predicate=inspect.ismethod):
        declaration = getattr(method, "__forest_message_handler__", None)
        if declaration is None:
            continue
        parameters = list(inspect.signature(method).parameters.values())
        handler_id = declaration["handler_id"]
        handler = declaration["handler"]
        if (
            not inspect.iscoroutinefunction(method)
            or [parameter.name for parameter in parameters] != ["event", "ctx"]
            or any(
                parameter.kind not in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
                for parameter in parameters
            )
            or handler_id in result
            or handler in methods
        ):
            raise ForestError(
                "MESSAGE_HANDLER_DECLARATION_INVALID",
                "Message handlers must be unique async methods(event, ctx)",
                422,
            )
        result[handler_id] = (dict(declaration), method)
        methods.add(handler)
    return result


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, init=False)
class Reply:
    """An immutable business-command proposal for one or more chat messages."""

    parts: tuple

    def __init__(self, parts):
        if (
            not isinstance(parts, (list, tuple))
            or not 1 <= len(parts) <= 64
            or any(
                not isinstance(message, (list, tuple))
                or not 1 <= len(message) <= 256
                or any(not isinstance(segment, dict) for segment in message)
                for message in parts
            )
        ):
            raise TypeError("Reply requires one to 64 nonempty message segment sequences")
        object.__setattr__(self, "parts", _freeze(list(parts)))


@dataclass(frozen=True)
class Consumed:
    """The command completed without a chat reply."""


@dataclass(frozen=True)
class Pass:
    """The current command or ordinary-message handler declines the input."""


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


@dataclass
class _OwnedResource:
    value: Any
    closer: Callable[[], Any]
    scope: CallScope | None
    name: str


class OwnedResources:
    """Bounded LIFO ownership for call-scoped and generation-scoped resources."""

    def __init__(self, limit: int = 64):
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("Resource limit must be 1 to 256")
        self.limit = limit
        self.entries: list[_OwnedResource] = []
        self.failures: deque[str] = deque(maxlen=8)
        self._closing_scopes: set[int] = set()
        self.closed = False

    @staticmethod
    def _scope_key(scope: CallScope | None) -> int:
        return 0 if scope is None else id(scope)

    @staticmethod
    def _closer(resource: Any, closer: Callable[[], Any] | None) -> Callable[[], Any]:
        if closer is None:
            closer = getattr(resource, "aclose", None) or getattr(resource, "close", None)
        if not callable(closer):
            raise ForestError(
                "RESOURCE_CLOSER_REQUIRED",
                "Owned resources require a callable close or aclose method",
                422,
            )
        return closer

    def register(
        self,
        resource: Any,
        *,
        scope: CallScope | None,
        closer: Callable[[], Any] | None = None,
        name: str | None = None,
    ) -> Any:
        if self.closed:
            raise ForestError("RESOURCE_OWNER_CLOSED", "Resource owner is closed", 409)
        if self._scope_key(scope) in self._closing_scopes:
            raise ForestError("RESOURCE_SCOPE_CLOSING", "Resource scope is closing", 409)
        if any(entry.value is resource for entry in self.entries):
            raise ForestError("RESOURCE_ALREADY_OWNED", "Resource is already registered", 409)
        if len(self.entries) >= self.limit:
            raise ForestError("OWNED_RESOURCE_LIMIT", "Resource owner reached its limit", 429)
        if name is not None and (
            not isinstance(name, str)
            or not 1 <= len(name) <= 128
            or any(ord(character) < 32 for character in name)
        ):
            raise ForestError("RESOURCE_NAME_INVALID", "Resource name is invalid", 422)
        selected = self._closer(resource, closer)
        self.entries.append(
            _OwnedResource(resource, selected, scope, name or type(resource).__name__[:128])
        )
        return resource

    def _entry(self, resource: Any, scope: CallScope | None) -> _OwnedResource:
        entry = next((item for item in self.entries if item.value is resource), None)
        if entry is None:
            raise ForestError("RESOURCE_NOT_OWNED", "Resource is not registered", 404)
        if entry.scope is not scope:
            raise ForestError(
                "RESOURCE_SCOPE_MISMATCH", "Resource belongs to another Context scope", 403
            )
        return entry

    async def _close_entry(self, entry: _OwnedResource) -> str | None:
        # Remove first so cancellation, a failing closer, or explicit retry never
        # calls a resource closer twice.
        index = next(index for index, current in enumerate(self.entries) if current is entry)
        self.entries.pop(index)
        try:
            result = entry.closer()
            if inspect.isawaitable(result):
                await result
            return None
        except Exception as error:
            failure = type(error).__name__
            self.failures.append(failure)
            return failure

    async def release(self, resource: Any, *, scope: CallScope | None) -> None:
        if self.closed:
            raise ForestError("RESOURCE_OWNER_CLOSED", "Resource owner is closed", 409)
        if self._scope_key(scope) in self._closing_scopes:
            raise ForestError("RESOURCE_SCOPE_CLOSING", "Resource scope is closing", 409)
        failure = await self._close_entry(self._entry(resource, scope))
        if failure is not None:
            raise ForestError(
                "OWNED_RESOURCE_FAILED", "An owned resource failed to close", 500,
                {"failures": [failure]},
            )

    async def _close_scope(self, scope: CallScope | None) -> list[str]:
        key, failures = self._scope_key(scope), []
        cancellation: asyncio.CancelledError | None = None
        self._closing_scopes.add(key)
        try:
            for entry in reversed(tuple(self.entries)):
                if entry.scope is scope and any(current is entry for current in self.entries):
                    try:
                        failure = await self._close_entry(entry)
                    except asyncio.CancelledError as error:
                        # A closer may itself be cancelled, or cancellation may
                        # arrive while it is awaited. Finish the ownership sweep
                        # before preserving cancellation at the call boundary.
                        failure = "CancelledError"
                        self.failures.append(failure)
                        cancellation = error
                    if failure is not None and len(failures) < 8:
                        failures.append(failure)
            if cancellation is not None:
                raise cancellation
            return failures
        finally:
            self._closing_scopes.discard(key)

    async def finish_call(self, scope: CallScope) -> None:
        failures = await self._close_scope(scope)
        if failures:
            raise ForestError(
                "OWNED_RESOURCE_FAILED", "One or more call resources failed to close", 500,
                {"failures": failures},
            )

    async def close_call(self, scope: CallScope) -> None:
        await self._close_scope(scope)

    async def close(self) -> None:
        self.closed = True
        failures = []
        cancellation: asyncio.CancelledError | None = None
        for entry in reversed(tuple(self.entries)):
            try:
                failure = await self._close_entry(entry)
            except asyncio.CancelledError as error:
                failure = "CancelledError"
                self.failures.append(failure)
                cancellation = error
            if failure is not None and len(failures) < 8:
                failures.append(failure)
        if cancellation is not None:
            raise cancellation
        if failures:
            raise ForestError(
                "OWNED_RESOURCE_FAILED", "One or more generation resources failed to close", 500,
                {"failures": failures},
            )


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


class ResourcesHandle:
    def __init__(self, owner: OwnedResources, scope: CallScope | None):
        self._owner = owner
        self._scope = scope

    def _require_scope(self) -> None:
        if self._scope is None:
            if _current_call.get() is not None:
                raise ForestError(
                    "CALL_CONTEXT_REQUIRED",
                    "Generation resource handles cannot be reused inside a business call",
                    403,
                )
        else:
            _require_call(self._scope)

    def register(
        self,
        resource: Any,
        *,
        closer: Callable[[], Any] | None = None,
        name: str | None = None,
    ) -> Any:
        self._require_scope()
        return self._owner.register(
            resource, scope=self._scope, closer=closer, name=name
        )

    async def release(self, resource: Any) -> None:
        self._require_scope()
        await self._owner.release(resource, scope=self._scope)


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
        if timeout is not None and (type(timeout) not in {int, float} or not 0 < timeout <= 86_400):
            raise ForestError(
                "INVALID_SERVICE_REQUEST",
                "Service timeout must be greater than zero and at most 86400 seconds",
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


class PayloadsHandle:
    """Chunked exact-reference payload I/O for one active package call."""

    _CHUNK_BYTES = 96 * 1024

    def __init__(
        self,
        rpc: Callable[[str, dict], Awaitable[Any]] | None,
        scope: CallScope | None,
    ) -> None:
        self._rpc = rpc
        self._scope = scope

    def _call_scope(self) -> CallScope:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED", "No payload capability gateway is available", 403
            )
        return scope

    @staticmethod
    def _reference(value: Mapping) -> dict:
        fields = (
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        )
        if not isinstance(value, Mapping) or set(value) != set(fields):
            raise ForestError(
                "PAYLOAD_REFERENCE_INVALID",
                "Payload access requires one complete exact reference",
                422,
            )
        result = {field: value[field] for field in fields}
        if any(not isinstance(item, str) or not item for item in result.values()):
            raise ForestError(
                "PAYLOAD_REFERENCE_INVALID", "Payload reference identity is invalid", 422
            )
        return result

    async def read_chunk(
        self, reference: Mapping, *, offset: int, max_bytes: int = _CHUNK_BYTES
    ) -> dict:
        scope = self._call_scope()
        exact = self._reference(reference)
        if (
            type(offset) is not int or offset < 0
            or type(max_bytes) is not int or not 1 <= max_bytes <= self._CHUNK_BYTES
        ):
            raise ForestError("PAYLOAD_RANGE_INVALID", "Payload read range is invalid", 422)
        result = await self._rpc(
            scope.call_id,
            {
                "action": "read-chunk", "reference": exact,
                "offset": offset, "max_bytes": max_bytes,
            },
        )
        if (
            not isinstance(result, dict)
            or set(result) != {"data_base64", "offset", "byte_length", "eof"}
            or result["offset"] != offset
            or type(result["byte_length"]) is not int
            or result["byte_length"] < 0
            or type(result["eof"]) is not bool
        ):
            raise ForestError(
                "PAYLOAD_PROTOCOL_INVALID", "Host returned an invalid payload chunk", 502
            )
        try:
            chunk = base64.b64decode(result["data_base64"], validate=True)
        except (TypeError, ValueError) as exc:
            raise ForestError(
                "PAYLOAD_PROTOCOL_INVALID", "Host returned invalid payload bytes", 502
            ) from exc
        total = result["byte_length"]
        if (
            len(chunk) > max_bytes or offset + len(chunk) > total
            or not chunk and offset < total
            or result["eof"] != (offset + len(chunk) == total)
        ):
            raise ForestError(
                "PAYLOAD_PROTOCOL_INVALID", "Host returned an invalid payload range", 502
            )
        return {"content": chunk, "offset": offset, "byte_length": total,
                "eof": result["eof"]}

    async def read_bytes(self, reference: Mapping) -> bytes:
        exact = self._reference(reference)
        chunks: list[bytes] = []
        offset = 0
        total: int | None = None
        while total is None or offset < total:
            result = await self.read_chunk(exact, offset=offset)
            if total is None:
                total = result["byte_length"]
            elif total != result["byte_length"]:
                raise ForestError(
                    "PAYLOAD_PROTOCOL_INVALID", "Payload length changed during one read", 409
                )
            chunk = result["content"]
            chunks.append(chunk)
            offset += len(chunk)
        content = b"".join(chunks)
        if "sha256:" + hashlib.sha256(content).hexdigest() != exact["digest"]:
            raise ForestError(
                "PAYLOAD_DIGEST_MISMATCH", "Payload bytes do not match the exact reference", 409
            )
        return content

    async def read_json(self, reference: Mapping) -> Any:
        try:
            return json.loads((await self.read_bytes(reference)).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "PAYLOAD_CONTENT_INVALID", "Payload is not valid UTF-8 JSON", 422
            ) from exc

    async def put_bytes(
        self,
        content: bytes,
        *,
        owner_kind: str,
        owner_id: str,
        media_type: str,
        parents=(),
    ) -> dict:
        scope = self._call_scope()
        if type(content) is not bytes or len(content) > 32 * 1024 * 1024:
            raise ForestError(
                "PAYLOAD_TOO_LARGE", "Payload must be bytes of at most 32 MiB", 413
            )
        exact_parents = [self._reference(item) for item in parents]
        upload_id = str(uuid4())
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        await self._rpc(
            scope.call_id,
            {
                "action": "begin-write",
                "upload_id": upload_id,
                "owner_kind": owner_kind,
                "owner_id": owner_id,
                "media_type": media_type,
                "byte_length": len(content),
                "digest": digest,
                "parents": exact_parents,
            },
        )
        try:
            for offset in range(0, len(content), self._CHUNK_BYTES):
                chunk = content[offset : offset + self._CHUNK_BYTES]
                await self._rpc(
                    scope.call_id,
                    {
                        "action": "append-write",
                        "upload_id": upload_id,
                        "offset": offset,
                        "data_base64": base64.b64encode(chunk).decode("ascii"),
                    },
                )
            return self._reference(
                await self._rpc(
                    scope.call_id,
                    {"action": "commit-write", "upload_id": upload_id},
                )
            )
        except BaseException:
            try:
                await self._rpc(
                    scope.call_id,
                    {"action": "abort-write", "upload_id": upload_id},
                )
            except BaseException:
                pass
            raise

    async def put_json(
        self,
        value: Any,
        *,
        owner_kind: str,
        owner_id: str,
        parents=(),
    ) -> dict:
        try:
            content = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
            raise ForestError(
                "PAYLOAD_CONTENT_INVALID", "Value is not canonical JSON", 422
            ) from exc
        return await self.put_bytes(
            content,
            owner_kind=owner_kind,
            owner_id=owner_id,
            media_type="application/json",
            parents=parents,
        )


class DomainQueriesHandle:
    def __init__(self, rpc: Callable[[str, dict], Awaitable[Any]] | None, scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def call(
        self,
        query: str,
        payload: Any,
        *,
        contract_major: int = 1,
        timeout: float | None = None,
    ) -> Any:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No domain query gateway is available", 403)
        _validate_domain_request(query, contract_major, timeout)
        return await self._rpc(
            scope.call_id,
            {
                "name": query,
                "payload": payload,
                "contract_major": contract_major,
                "timeout": timeout,
            },
        )


class DomainCommandsHandle:
    def __init__(self, rpc: Callable[[str, dict], Awaitable[Any]] | None, scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def call(
        self,
        command: str,
        payload: Any,
        *,
        operation_id: str,
        contract_major: int = 1,
        timeout: float | None = None,
    ) -> Any:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No domain command gateway is available", 403)
        _validate_domain_request(command, contract_major, timeout)
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ForestError(
                "INVALID_DOMAIN_COMMAND", "A stable domain operation ID is required", 422
            )
        return await self._rpc(
            scope.call_id,
            {
                "name": command,
                "payload": payload,
                "contract_major": contract_major,
                "operation_id": operation_id,
                "timeout": timeout,
            },
        )


class UnitOfWorkPlansHandle:
    def __init__(self, rpc: Callable[[str, dict], Awaitable[Any]] | None, scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def call(
        self,
        plan: str,
        payload: Any,
        *,
        operation_id: str,
        contract_major: int = 1,
        timeout: float | None = None,
    ) -> Any:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No UnitOfWork gateway is available", 403)
        _validate_domain_request(plan, contract_major, timeout)
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ForestError(
                "INVALID_UNIT_OF_WORK", "A stable UnitOfWork operation ID is required", 422
            )
        return await self._rpc(
            scope.call_id,
            {
                "name": plan,
                "payload": payload,
                "contract_major": contract_major,
                "operation_id": operation_id,
                "timeout": timeout,
            },
        )


def _validate_domain_request(name: str, contract_major: int, timeout: float | None) -> None:
    if not isinstance(name, str) or not 1 <= len(name) <= 128:
        raise ForestError("INVALID_DOMAIN_REQUEST", "A bounded domain port name is required", 422)
    if type(contract_major) is not int or contract_major < 1:
        raise ForestError(
            "INVALID_DOMAIN_REQUEST", "Domain contract major must be a positive integer", 422
        )
    if timeout is not None and (
        type(timeout) not in {int, float} or not 0 < timeout <= 86_400
    ):
        raise ForestError(
            "INVALID_DOMAIN_REQUEST",
            "Domain timeout must be greater than zero and at most 86400 seconds",
            422,
        )


class EventsHandle:
    def __init__(self, rpc: Callable[[str, dict], Awaitable[Any]] | None, scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def publish(
        self,
        topic: str,
        payload: Any,
        *,
        contract: str,
        operation_id: str,
    ) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No component event gateway is available", 403)
        if not isinstance(topic, str) or not 1 <= len(topic) <= 128:
            raise ForestError("INVALID_EVENT_REQUEST", "A bounded event topic is required", 422)
        if not isinstance(contract, str) or not re.fullmatch(r"[1-9]\d*\.\d+", contract):
            raise ForestError(
                "INVALID_EVENT_REQUEST", "Event contract must be an exact major.minor version", 422
            )
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ForestError(
                "INVALID_EVENT_REQUEST", "A stable event operation ID is required", 422
            )
        return await self._rpc(
            scope.call_id,
            {
                "topic": topic,
                "payload": payload,
                "contract": contract,
                "operation_id": operation_id,
            },
        )


class JobsHandle:
    def __init__(self, rpc: Callable[[str, dict], Awaitable[Any]] | None, scope: CallScope | None):
        self._rpc = rpc
        self._scope = scope

    async def schedule(
        self,
        handler_id: str,
        payload: Any,
        *,
        operation_id: str,
        delay_seconds: float = 0,
    ) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No component job gateway is available", 403)
        if (
            not isinstance(handler_id, str)
            or not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}", handler_id)
        ):
            raise ForestError("INVALID_JOB_REQUEST", "A bounded job handler ID is required", 422)
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ForestError(
                "INVALID_JOB_REQUEST", "A stable job operation ID is required", 422
            )
        if (
            isinstance(delay_seconds, bool)
            or not isinstance(delay_seconds, (int, float))
            or not 0 <= delay_seconds <= 30 * 24 * 60 * 60
        ):
            raise ForestError(
                "INVALID_JOB_REQUEST", "Job delay must be between zero and 30 days", 422
            )
        return await self._rpc(
            scope.call_id,
            {
                "handler_id": handler_id,
                "payload": payload,
                "operation_id": operation_id,
                "delay_seconds": delay_seconds,
            },
        )


class JobTypesDirectoryHandle:
    """Read and invoke the RuntimeGraph-frozen durable Job type directory."""

    def __init__(
        self,
        rpc: Callable[[str, dict], Awaitable[Any]] | None,
        scope: CallScope | None,
    ) -> None:
        self._rpc = rpc
        self._scope = scope

    async def list(self) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED", "No contribution directory gateway is available", 403
            )
        return await self._rpc(
            scope.call_id,
            {"kind": "job-types", "action": "list"},
        )

    async def execute(
        self,
        *,
        job_id: str,
        job_type: str,
        job_version: int,
        attempt: int,
        requested_by: str,
        input: dict,
        claim_token: str,
        target_component_id: str,
        target_generation_id: str,
        target_service_id: str,
        target_service_version: int,
        operation_id: str,
        timeout: float,
    ) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED", "No contribution directory gateway is available", 403
            )
        return await self._rpc(
            scope.call_id,
            {
                "kind": "job-types",
                "action": "execute",
                "job_id": job_id,
                "job_type": job_type,
                "job_version": job_version,
                "attempt": attempt,
                "requested_by": requested_by,
                "input": input,
                "claim_token": claim_token,
                "target_component_id": target_component_id,
                "target_generation_id": target_generation_id,
                "target_service_id": target_service_id,
                "target_service_version": target_service_version,
                "operation_id": operation_id,
                "timeout": timeout,
            },
        )


class ShellCommandsDirectoryHandle:
    """Read and invoke the RuntimeGraph-frozen Shell command directory."""

    def __init__(
        self,
        rpc: Callable[[str, dict], Awaitable[Any]] | None,
        scope: CallScope | None,
    ) -> None:
        self._rpc = rpc
        self._scope = scope

    async def list(self) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED", "No contribution directory gateway is available", 403
            )
        return await self._rpc(
            scope.call_id,
            {"kind": "shell-commands", "action": "list"},
        )

    async def execute(
        self,
        *,
        command_id: str,
        command_version: int,
        arguments: dict,
        request_id: str,
        principal_ref: str,
        conversation_ref: str,
        claim_token: str | None = None,
        target_component_id: str,
        target_generation_id: str,
        target_service_id: str,
        target_service_version: int,
        operation_id: str,
        timeout: float,
    ) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED", "No contribution directory gateway is available", 403
            )
        return await self._rpc(
            scope.call_id,
            {
                "kind": "shell-commands",
                "action": "execute",
                "command_id": command_id,
                "command_version": command_version,
                "arguments": arguments,
                "request_id": request_id,
                "principal_ref": principal_ref,
                "conversation_ref": conversation_ref,
                "claim_token": claim_token,
                "target_component_id": target_component_id,
                "target_generation_id": target_generation_id,
                "target_service_id": target_service_id,
                "target_service_version": target_service_version,
                "operation_id": operation_id,
                "timeout": timeout,
            },
        )


class AgentToolsDirectoryHandle:
    """Read the exact RuntimeGraph-frozen Agent tool directory."""

    def __init__(
        self,
        rpc: Callable[[str, dict], Awaitable[Any]] | None,
        scope: CallScope | None,
    ) -> None:
        self._rpc = rpc
        self._scope = scope

    async def list(self) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED",
                "No contribution directory gateway is available",
                403,
            )
        return await self._rpc(
            scope.call_id,
            {"kind": "agent-tools", "action": "list"},
        )

    async def dispatch_prepared(self, request: dict) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED",
                "No contribution directory gateway is available",
                403,
            )
        if not isinstance(request, dict):
            raise ForestError(
                "AGENT_TOOL_COMMAND_INVALID",
                "Prepared Agent tool dispatch must be an object",
                422,
            )
        return await self._rpc(
            scope.call_id,
            {"kind": "agent-tools", "action": "dispatch-prepared", **request},
        )

    async def query_handed_off(self, request: dict) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError(
                "CAPABILITY_DENIED",
                "No contribution directory gateway is available",
                403,
            )
        if not isinstance(request, dict):
            raise ForestError(
                "AGENT_TOOL_QUERY_INVALID",
                "Agent tool reconciliation query must be an object",
                422,
            )
        return await self._rpc(
            scope.call_id,
            {"kind": "agent-tools", "action": "query-handed-off", **request},
        )


class RoutingRoutesDirectoryHandle:
    """Invoke targets from the RuntimeGraph-frozen routing contribution directory."""

    def __init__(
        self,
        rpc: Callable[[str, dict], Awaitable[Any]] | None,
        scope: CallScope | None,
    ) -> None:
        self._rpc = rpc
        self._scope = scope

    async def list(self) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No routing directory is available", 403)
        return await self._rpc(
            scope.call_id, {"kind": "routing-routes", "action": "list"}
        )

    async def deliver_event(self, **request) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No routing directory is available", 403)
        return await self._rpc(
            scope.call_id,
            {"kind": "routing-routes", "action": "deliver-event", **request},
        )

    async def deliver_component_job(self, **request) -> dict:
        scope = _require_call(self._scope)
        if self._rpc is None:
            raise ForestError("CAPABILITY_DENIED", "No routing directory is available", 403)
        return await self._rpc(
            scope.call_id,
            {
                "kind": "routing-routes",
                "action": "deliver-component-job",
                **request,
            },
        )

class PluginContext:
    def __init__(
        self,
        config: Mapping,
        rpc: Callable[[str, str, dict], Awaitable[Any]],
        *,
        task_limit: int = 64,
        resource_limit: int = 64,
        scope: CallScope | None = None,
        task_owner: OwnedTasks | None = None,
        resource_owner: OwnedResources | None = None,
        service_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        domain_query_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        domain_command_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        unit_of_work_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        event_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        job_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        contribution_directory_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        payload_rpc: Callable[[str, dict], Awaitable[Any]] | None = None,
        identity: Mapping | None = None,
    ):
        self.config: Mapping[str, Any] = _freeze(config)
        self.identity: Mapping[str, Any] = _freeze(identity or {})
        self._scope = scope
        self._rpc = rpc
        self._service_rpc = service_rpc
        self._domain_query_rpc = domain_query_rpc
        self._domain_command_rpc = domain_command_rpc
        self._unit_of_work_rpc = unit_of_work_rpc
        self._event_rpc = event_rpc
        self._job_rpc = job_rpc
        self._contribution_directory_rpc = contribution_directory_rpc
        self._payload_rpc = payload_rpc
        self._task_owner = task_owner if task_owner is not None else OwnedTasks(task_limit)
        self._resource_owner = (
            resource_owner if resource_owner is not None else OwnedResources(resource_limit)
        )
        self.state = StateHandle(rpc, scope)
        self.payloads = PayloadsHandle(payload_rpc, scope)
        self.tasks = TasksHandle(self._task_owner, scope)
        self.resources = ResourcesHandle(self._resource_owner, scope)
        self.operations = OperationsHandle(scope)
        self.services = ServicesHandle(service_rpc, scope)
        self.queries = DomainQueriesHandle(domain_query_rpc, scope)
        self.commands = DomainCommandsHandle(domain_command_rpc, scope)
        self.uow = UnitOfWorkPlansHandle(unit_of_work_rpc, scope)
        self.events = EventsHandle(event_rpc, scope)
        self.jobs = JobsHandle(job_rpc, scope)
        self.job_types = JobTypesDirectoryHandle(contribution_directory_rpc, scope)
        self.shell_commands = ShellCommandsDirectoryHandle(
            contribution_directory_rpc, scope
        )
        self.agent_tools = AgentToolsDirectoryHandle(
            contribution_directory_rpc, scope
        )
        self.routing_routes = RoutingRoutesDirectoryHandle(
            contribution_directory_rpc, scope
        )

    def _for_call(self, scope: CallScope) -> PluginContext:
        return PluginContext(
            self.config,
            self._rpc,
            scope=scope,
            task_owner=self._task_owner,
            resource_owner=self._resource_owner,
            service_rpc=self._service_rpc,
            domain_query_rpc=self._domain_query_rpc,
            domain_command_rpc=self._domain_command_rpc,
            unit_of_work_rpc=self._unit_of_work_rpc,
            event_rpc=self._event_rpc,
            job_rpc=self._job_rpc,
            contribution_directory_rpc=self._contribution_directory_rpc,
            payload_rpc=self._payload_rpc,
            identity=self.identity,
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

    async def migrate(self, state: dict, *, from_schema: int, to_schema: int) -> dict:
        """Pure durable values transformation; no live CallContext or state writes.

        Revisions and deleted-key fences belong to the host. A version transition
        must be declared in the immutable manifest and implemented explicitly.
        """
        if from_schema != to_schema:
            raise ForestError("STATE_MIGRATION_MISSING", "No state migration is implemented", 422)
        return state

    async def start(self) -> None:
        pass

    def replacement_facts(self) -> dict:
        """Immutable compatibility facts published by a prepared Generation."""
        return {}

    async def health(self) -> dict:
        return {"healthy": True}

    async def prepare_replacement(self, operation_id: str) -> None:
        pass

    async def abort_replacement(self, operation_id: str) -> None:
        pass

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
