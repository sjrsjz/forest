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
from forest.drivers.python_worker import WorkerLimits
from forest.errors import ForestError
from forest.sdk import CallScope, Plugin, PluginContext, _current_call, declared_services


class WorkerRuntime:
    def __init__(self, bootstrap: dict, writer: FrameWriter):
        self.bootstrap = bootstrap
        self.writer = writer
        self.limits = WorkerLimits(**bootstrap["limits"])
        self.state = "CONNECTED"
        self.plugin: Plugin | None = None
        self.ctx: PluginContext | None = None
        self.services: dict = {}
        self.commands: dict[str, asyncio.Task] = {}
        self.calls: dict[str, asyncio.Task] = {}
        self.state_pending: dict[str, asyncio.Future] = {}
        self.lifecycle_lock = asyncio.Lock()
        self.stopping = False

    async def state_rpc(self, call_id: str, action: str, payload: dict) -> Any:
        if len(self.state_pending) >= self.limits.max_state_inflight:
            raise ForestError("OVERLOADED", "Worker state request capacity exhausted", 429)
        identifier = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self.state_pending[identifier] = future
        try:
            await self.writer.send(
                {
                    "kind": "state",
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
        if command == "invoke":
            self.require("ACTIVE")
            name, call_id = payload.get("service"), payload.get("call_id")
            if name not in self.services:
                raise ForestError("SERVICE_NOT_FOUND", "Requested service is not registered", 404)
            if len(self.calls) >= self.limits.max_inflight or call_id in self.calls:
                raise ForestError("OVERLOADED", "Worker service slots are exhausted", 429)
            scope = CallScope(call_id, deadline, payload["operation_id"])
            token = _current_call.set(scope)
            context = self.ctx._for_call(scope)
            self.calls[call_id] = asyncio.current_task()
            try:
                result = await self.services[name](payload.get("payload"), context)
                await self.ctx._task_owner.finish_call(scope)
                return result
            finally:
                await self.ctx._task_owner.close_call(scope)
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
            if self.ctx:
                await self.ctx._task_owner.close()
            if self.plugin:
                await self.plugin.stop(str(payload.get("reason", "shutdown")))
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
                if sorted(self.services) != sorted(self.bootstrap["services"]):
                    raise ForestError(
                        "SERVICE_MANIFEST_MISMATCH",
                        "Decorated services differ from the immutable manifest",
                        422,
                    )
                config = payload.get("config", {})
                if not isinstance(config, dict):
                    raise ForestError(
                        "CONFIGURATION_INVALID", "Plugin configuration must be a JSON object", 422
                    )
                self.ctx = PluginContext(
                    config, self.state_rpc, task_limit=self.limits.max_owned_tasks
                )
                await self.plugin.prepare(self.ctx, self.ctx.config, payload.get("restored_state"))
                self.state = "PREPARED"
                return {"state": self.state, "services": sorted(self.services)}
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
                    "owned_tasks": len(self.ctx._task_owner.tasks),
                    "task_failures": list(self.ctx._task_owner.background_failures),
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
            response.update(ok=False, error=error.as_dict() | {"status": error.status})
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
                if kind == "state_result":
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
