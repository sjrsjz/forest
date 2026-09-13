"""Private process entrypoint. Bootstrap arrives on stdin, never argv or logs."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
import sys
import time
from typing import Any
from uuid import uuid4

from forest.drivers.protocol import MAX_FRAME_BYTES, FrameWriter, read_frame, validate_json
from forest.drivers.python_worker import WorkerLimits, _error_details, _error_envelope
from forest.errors import ForestError
from forest.sdk import (
    CallScope,
    Plugin,
    PluginContext,
    _current_call,
    declared_jobs,
    declared_services,
    declared_subscriptions,
)


class WorkerRuntime:
    def __init__(self, bootstrap: dict, writer: FrameWriter):
        self.bootstrap = bootstrap
        self.writer = writer
        self.limits = WorkerLimits(**bootstrap["limits"])
        self.state = "CONNECTED"
        self.plugin: Plugin | None = None
        self.ctx: PluginContext | None = None
        self.services: dict = {}
        self.subscriptions: dict = {}
        self.jobs: dict = {}
        self.commands: dict[str, asyncio.Task] = {}
        self.calls: dict[str, asyncio.Task] = {}
        self.state_pending: dict[str, asyncio.Future] = {}
        self.lifecycle_lock = asyncio.Lock()
        self.stopping = False

    async def state_rpc(self, call_id: str, action: str, payload: dict) -> Any:
        return await self._rpc("state", call_id, payload, action=action)

    async def service_rpc(self, call_id: str, payload: dict) -> Any:
        return await self._rpc("service", call_id, payload)

    async def event_rpc(self, call_id: str, payload: dict) -> Any:
        return await self._rpc("event", call_id, payload)

    async def job_rpc(self, call_id: str, payload: dict) -> Any:
        return await self._rpc("job", call_id, payload)

    async def _rpc(
        self, kind: str, call_id: str, payload: dict, *, action: str | None = None
    ) -> Any:
        if len(self.state_pending) >= self.limits.max_state_inflight:
            raise ForestError("OVERLOADED", "Worker state request capacity exhausted", 429)
        identifier = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self.state_pending[identifier] = future
        try:
            await self.writer.send(
                {
                    "kind": kind,
                    "id": identifier,
                    "call_id": call_id,
                    "action": action,
                    "payload": payload,
                }
            )
            return await future
        finally:
            self.state_pending.pop(identifier, None)

    def require(self, *states: str) -> None:
        if self.state not in states:
            raise ForestError(
                "LIFECYCLE_INVALID", "Operation is invalid in the current worker stage", 409
            )

    async def execute(self, command: str, payload: dict, deadline: float) -> Any:
        if command in {"invoke", "invoke_event", "invoke_job"}:
            self.require("ACTIVE")
            call_id = payload.get("call_id")
            if command == "invoke":
                name = payload.get("service")
                handler = self.services.get(name)
                missing = "SERVICE_NOT_FOUND"
            elif command == "invoke_event":
                name = payload.get("subscriber_id")
                handler = self.subscriptions.get(name)
                missing = "SUBSCRIPTION_NOT_FOUND"
            else:
                name = payload.get("handler_id")
                handler = self.jobs.get(name)
                missing = "JOB_HANDLER_NOT_FOUND"
            if handler is None:
                raise ForestError(missing, "Requested invocation target is not registered", 404)
            if len(self.calls) >= self.limits.max_inflight or call_id in self.calls:
                raise ForestError("OVERLOADED", "Worker service slots are exhausted", 429)
            scope = CallScope(call_id, deadline, payload["operation_id"])
            token = _current_call.set(scope)
            context = self.ctx._for_call(scope)
            self.calls[call_id] = asyncio.current_task()
            try:
                result = await handler(payload.get("payload"), context)
                await self.ctx._task_owner.finish_call(scope)
                await self.ctx._resource_owner.finish_call(scope)
                return result
            finally:
                try:
                    # Tasks may still be using call-owned resources. Cancel and
                    # join them before closing those resources on every exit.
                    await self.ctx._task_owner.close_call(scope)
                finally:
                    try:
                        await self.ctx._resource_owner.close_call(scope)
                    finally:
                        self.calls.pop(call_id, None)
                        _current_call.reset(token)

        if command == "cancel":
            task = self.calls.get(payload.get("call_id"))
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            return {"cancelled": task is not None}

        if command == "stop":
            self.state = "STOPPING"
            tasks = [task for task in self.commands.values() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            failure = None
            try:
                if self.plugin:
                    await self.plugin.stop(str(payload.get("reason", "shutdown")))
            except BaseException as error:
                failure = error
            if self.ctx:
                try:
                    await self.ctx._task_owner.close()
                except BaseException as error:
                    if failure is None:
                        failure = error
                try:
                    await self.ctx._resource_owner.close()
                except BaseException as error:
                    if failure is None:
                        failure = error
            if failure is not None:
                raise failure
            self.state = "STOPPED"
            self.stopping = True
            return {"state": self.state}

        async with self.lifecycle_lock:
            if command == "prepare":
                self.require("CONNECTED")
                self.state = "PREPARING"
                sys.path.insert(0, self.bootstrap["module_root"])
                module_name, class_name = self.bootstrap["entrypoint"].split(":", 1)
                candidate = getattr(importlib.import_module(module_name), class_name)
                if not inspect.isclass(candidate) or not issubclass(candidate, Plugin):
                    raise ForestError(
                        "PLUGIN_CLASS_INVALID", "Entrypoint must be a Plugin subclass", 422
                    )
                self.plugin = candidate()
                self.services = declared_services(self.plugin)
                declared = declared_subscriptions(self.plugin)
                self.subscriptions = {
                    subscriber_id: method
                    for subscriber_id, (_, method) in declared.items()
                }
                self.jobs = declared_jobs(self.plugin)
                if sorted(self.services) != sorted(self.bootstrap["services"]):
                    raise ForestError(
                        "SERVICE_MANIFEST_MISMATCH",
                        "Decorated services differ from the immutable manifest",
                        422,
                    )
                actual_subscriptions = sorted(
                    (declaration for declaration, _ in declared.values()),
                    key=lambda item: item["subscriber_id"],
                )
                if actual_subscriptions != self.bootstrap.get("subscriptions", []):
                    raise ForestError(
                        "SUBSCRIPTION_MANIFEST_MISMATCH",
                        "Decorated subscriptions differ from the immutable manifest",
                        422,
                    )
                if sorted(self.jobs) != sorted(self.bootstrap.get("jobs", [])):
                    raise ForestError(
                        "JOB_MANIFEST_MISMATCH",
                        "Decorated jobs differ from the immutable manifest",
                        422,
                    )
                config = payload.get("config", {})
                if not isinstance(config, dict):
                    raise ForestError(
                        "CONFIGURATION_INVALID", "Plugin configuration must be a JSON object", 422
                    )
                self.ctx = PluginContext(
                    config,
                    self.state_rpc,
                    task_limit=self.limits.max_owned_tasks,
                    resource_limit=self.limits.max_owned_resources,
                    service_rpc=self.service_rpc,
                    event_rpc=self.event_rpc,
                    job_rpc=self.job_rpc,
                )
                await self.plugin.prepare(self.ctx, self.ctx.config, payload.get("restored_state"))
                self.state = "PREPARED"
                return {
                    "state": self.state,
                    "services": sorted(self.services),
                    "subscriptions": sorted(self.subscriptions),
                    "jobs": sorted(self.jobs),
                }
            if command == "start":
                self.require("PREPARED")
                await self.plugin.start()
                self.state = "READY"
                return {"state": self.state}
            if command == "health":
                self.require("PREPARED", "READY", "ACTIVE", "QUIESCENT")
                health = await self.plugin.health()
                healthy = health is True or (
                    isinstance(health, dict) and health.get("healthy") is True
                )
                return {
                    "healthy": healthy and not self.ctx._task_owner.background_failures,
                    "plugin": health,
                    "state": self.state,
                    "services": sorted(self.services),
                    "subscriptions": sorted(self.subscriptions),
                    "jobs": sorted(self.jobs),
                    "owned_tasks": len(self.ctx._task_owner.tasks),
                    "task_failures": list(self.ctx._task_owner.background_failures),
                    "owned_resources": len(self.ctx._resource_owner.entries),
                    "resource_failures": list(self.ctx._resource_owner.failures),
                }
            if command == "activate":
                self.require("READY")
                self.state = "ACTIVE"
                return {"state": self.state}
            if command == "quiesce":
                self.require("ACTIVE")
                self.state = "QUIESCING"
                await self.plugin.quiesce(str(payload.get("reason", "reload")), deadline)
                while self.calls:
                    await asyncio.gather(*list(self.calls.values()), return_exceptions=True)
                self.state = "QUIESCENT"
                return {"state": self.state}
            if command == "snapshot":
                self.require("QUIESCENT", "READY")
                return await self.plugin.snapshot()
            if command == "restore":
                self.require("READY")
                await self.plugin.restore(payload.get("state"))
                return {"state": self.state}
            if command == "migrate":
                self.require("READY")
                # There is no CallScope in lifecycle execution. Even a retained
                # state/service handle cannot acquire a host production grant.
                return await self.plugin.migrate(
                    payload["state"], from_schema=payload["from_schema"],
                    to_schema=payload["to_schema"],
                )
            if command == "resume":
                self.require("QUIESCENT")
                await self.plugin.resume()
                self.state = "ACTIVE"
                return {"state": self.state}
            raise ForestError("COMMAND_UNKNOWN", "Unsupported runtime command", 400)

    async def handle(self, message: dict) -> None:
        response = {"kind": "result", "id": message["id"]}
        try:
            deadline = message["deadline_at"]
            if not isinstance(deadline, (int, float)) or deadline <= time.time():
                raise ForestError("DEADLINE_EXCEEDED", "Command deadline has expired", 504)
            async with asyncio.timeout(deadline - time.time()):
                value = await self.execute(message["command"], message.get("payload", {}), deadline)
            response.update(ok=True, value=value)
        except asyncio.CancelledError:
            response.update(
                ok=False,
                error={
                    "code": "CALL_CANCELLED",
                    "message": "Worker call was cancelled",
                    "status": 409,
                },
            )
        except asyncio.TimeoutError:
            response.update(
                ok=False,
                error={
                    "code": "DEADLINE_EXCEEDED",
                    "message": "Worker command exceeded its deadline",
                    "status": 504,
                },
            )
        except ForestError as error:
            response.update(ok=False, error=_error_envelope(error))
        except Exception:
            response.update(
                ok=False,
                error={
                    "code": "PLUGIN_FAILED",
                    "message": "Plugin operation failed",
                    "status": 500,
                },
            )
        try:
            await self.writer.send(response)
        except ForestError as error:
            await self.writer.send(
                {
                    "kind": "result",
                    "id": message["id"],
                    "ok": False,
                    "error": {
                        "code": error.code,
                        "message": "Plugin result violates the JSON protocol",
                        "status": error.status,
                    },
                }
            )
        finally:
            self.commands.pop(message["id"], None)
            if self.stopping:
                self.writer.writer.close()

    async def run(self, reader: asyncio.StreamReader) -> None:
        try:
            while not self.stopping:
                message = await read_frame(reader, self.limits.max_frame_bytes)
                kind = message.get("kind")
                if kind in {"state_result", "service_result", "event_result", "job_result"}:
                    future = self.state_pending.get(message.get("id"))
                    if future is None or future.done():
                        continue
                    if message.get("ok") is True:
                        future.set_result(message.get("value"))
                    else:
                        error = message.get("error", {})
                        future.set_exception(
                            ForestError(
                                error.get("code", "HOST_STATE_FAILED"),
                                error.get("message", "State request failed"),
                                error.get("status", 500),
                                details=_error_details(error),
                            )
                        )
                    continue
                identifier = message.get("id")
                if (
                    kind != "command"
                    or not isinstance(identifier, str)
                    or len(identifier) > 128
                    or identifier in self.commands
                ):
                    raise ForestError("IPC_FRAME_INVALID", "Invalid command envelope", 400)
                if len(self.commands) >= self.limits.max_inflight + 8:
                    await self.writer.send(
                        {
                            "kind": "result",
                            "id": identifier,
                            "ok": False,
                            "error": {
                                "code": "OVERLOADED",
                                "message": "Worker command capacity exhausted",
                                "status": 429,
                            },
                        }
                    )
                    continue
                task = asyncio.create_task(self.handle(message))
                self.commands[identifier] = task
        finally:
            self.stopping = True
            for task in list(self.commands.values()):
                task.cancel()
            for future in self.state_pending.values():
                if not future.done():
                    future.cancel()
            if self.commands:
                await asyncio.gather(*list(self.commands.values()), return_exceptions=True)
            if self.ctx:
                await self.ctx._task_owner.close()


async def run(bootstrap: dict) -> None:
    reader, stream = await asyncio.open_connection(
        "127.0.0.1", bootstrap["port"], limit=MAX_FRAME_BYTES + 4
    )
    writer = FrameWriter(stream, bootstrap["limits"]["max_frame_bytes"])
    try:
        await writer.send(
            {
                "kind": "hello",
                "token": bootstrap["token"],
                "identity": bootstrap["identity"],
                "pid": os.getpid(),
            }
        )
        acknowledgement = await asyncio.wait_for(read_frame(reader), timeout=5)
        if acknowledgement.get("kind") != "hello_ack":
            return
        bootstrap.pop("token", None)
        await WorkerRuntime(bootstrap, writer).run(reader)
    finally:
        stream.close()
        await stream.wait_closed()


def main() -> None:
    line = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
    if len(line) > MAX_FRAME_BYTES or not line.endswith(b"\n"):
        raise SystemExit(2)
    try:
        bootstrap = json.loads(line)
        validate_json(bootstrap)
        asyncio.run(run(bootstrap))
    except (Exception, KeyboardInterrupt):
        # Protocol reports safe structured errors. stdout/stderr are not IPC,
        # and secrets or plugin traceback text must not enter ordinary logs.
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
