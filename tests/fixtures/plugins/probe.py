"""Synthetic subprocess fixtures; never connect to real external services."""

import asyncio
import os
import time
from pathlib import Path

from forest.errors import ForestError
from forest.sdk import Plugin, service


def _mark(path, value):
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(value + "\n")


class _SyncMarker:
    def __init__(self, path, value):
        self.path, self.value = path, value

    def close(self):
        _mark(self.path, self.value)


class _AsyncMarker(_SyncMarker):
    async def aclose(self):
        await asyncio.sleep(0)
        self.close()


class Probe(Plugin):
    async def prepare(self, ctx, config, restored_state):
        await super().prepare(ctx, config, restored_state)
        if config.get("background"):
            ctx.tasks.spawn_owned(self.background(), name="probe-background")
        if config.get("candidate_write"):
            await ctx.state.commit("counter", 1, 0, "forbidden-candidate-operation")
        if marker := config.get("resource_marker"):
            ctx.resources.register(_SyncMarker(marker, "root-first"))
            ctx.resources.register(_AsyncMarker(marker, "root-second"))

    async def background(self):
        try:
            await asyncio.Event().wait()
        finally:
            if marker := self.ctx.config.get("cleanup_marker"):
                Path(marker).write_text("cancelled", encoding="utf-8")

    @service("probe.echo", semantics="read")
    async def echo(self, payload, ctx):
        print("Plugin stdout cannot become an IPC frame")
        return {
            "payload": payload,
            "pid": os.getpid(),
            "call_id": ctx.call_id,
            "operation": ctx.operations.stable("probe:v1"),
            "restored": self._snapshot,
            "environment": {
                name: name in os.environ
                for name in [
                    "FOREST_TEST_DSN",
                    "FOREST_DATABASE_PASSWORD_FILE",
                    "OPENAI_API_KEY",
                    "PGPASSWORD",
                    "AWS_SECRET_ACCESS_KEY",
                ]
            },
        }

    @service("probe.counter", semantics="at-most-once")
    async def counter(self, payload, ctx):
        current = await ctx.state.get("counter")
        return await ctx.state.commit(
            "counter",
            current["value"] + 1,
            current["revision"],
            ctx.operations.stable("counter:v1"),
        )

    @service("probe.wait", semantics="read")
    async def wait(self, payload, ctx):
        await ctx.state.get("gate")
        return {"done": True}

    @service("probe.child", semantics="at-most-once")
    async def child(self, payload, ctx):
        async def finish():
            await ctx.state.get("gate")
            await ctx.state.commit("child", "finished", 0, ctx.operations.stable("child:v1"))

        ctx.tasks.spawn_owned(finish())
        return {"handler_returned": True}

    @service("probe.failchild", semantics="idempotent")
    async def failchild(self, payload, ctx):
        async def fail():
            raise ValueError("synthetic child failure")

        ctx.tasks.spawn_owned(fail())
        return None

    @service("probe.block", semantics="read")
    async def block(self, payload, ctx):
        await ctx.state.get("started")
        time.sleep(60)

    @service("probe.crash", semantics="read")
    async def crash(self, payload, ctx):
        os._exit(19)

    @service("probe.badreturn", semantics="read")
    async def badreturn(self, payload, ctx):
        return {"not": {"a", "json", "value"}}

    @service("probe.large", semantics="read")
    async def large(self, payload, ctx):
        return {"value": "x" * payload["size"]}

    @service("probe.forge", semantics="read")
    async def forge(self, payload, ctx):
        return await ctx.state._rpc(
            payload.get("call_id", ctx.call_id), "get", payload.get("request", {"key": "counter"})
        )

    @service("probe.latecommit", semantics="at-most-once")
    async def latecommit(self, payload, ctx):
        # Deliberately bypass the SDK to exercise the host protocol boundary.
        runtime = ctx.state._rpc.__self__
        request_id = next(
            key for key, task in runtime.commands.items() if task is asyncio.current_task()
        )
        await runtime.writer.send(
            {"kind": "result", "id": request_id, "ok": True, "value": "early-result"}
        )
        await ctx.state.commit("late", "must-not-commit", 0, ctx.operations.stable("late:v1"))

    @service("probe.config", semantics="read")
    async def config(self, payload, ctx):
        try:
            ctx.config["nested"]["value"] = "changed"
        except TypeError:
            return {"immutable": True}
        return {"immutable": False}

    @service("probe.cache", semantics="idempotent")
    async def cache(self, payload, ctx):
        if payload["action"] == "save":
            self.cached = ctx
            self.cached_state = ctx.state
            self.cached_operations = ctx.operations
            self.cached_tasks = ctx.tasks
            if payload.get("wait"):
                await ctx.state.get("cache-gate")
            return {"cached": True}
        results = {}
        for name, action in {
            "context": lambda: self.cached.call_id,
            "operations": lambda: self.cached_operations.stable("cached:v1"),
            "tasks": lambda: self.cached_tasks.spawn_owned(asyncio.sleep(0)),
            "component_operations": lambda: self.ctx.operations.stable("component:v1"),
            "component_tasks": lambda: self.ctx.tasks.spawn_owned(asyncio.sleep(0)),
        }.items():
            try:
                action()
            except ForestError as error:
                results[name] = error.code
            else:
                results[name] = "INCORRECTLY_ALLOWED"
        for name, handle in {"state": self.cached_state, "component_state": self.ctx.state}.items():
            try:
                await handle.get("counter")
            except ForestError as error:
                results[name] = error.code
            else:
                results[name] = "INCORRECTLY_ALLOWED"
        results["fresh_call"] = ctx.call_id
        results["fresh_state"] = await ctx.state.get("counter")
        return results

    @service("probe.resources", semantics="idempotent")
    async def resources(self, payload, ctx):
        marker = payload["marker"]
        ctx.resources.register(_SyncMarker(marker, "call-first"))
        ctx.resources.register(_AsyncMarker(marker, "call-second"))
        _mark(marker, "handler")
        if payload.get("owned_task"):
            ctx.tasks.spawn_owned(self.resource_task(marker), name="resource-task")
            await asyncio.sleep(0)
        if payload.get("wait"):
            await asyncio.Event().wait()
        if payload.get("fail"):
            raise RuntimeError("synthetic resource handler failure")
        return {"registered": True}

    async def resource_task(self, marker):
        try:
            await asyncio.Event().wait()
        finally:
            _mark(marker, "task-finally")

    async def stop(self, reason):
        if marker := self.ctx.config.get("resource_marker"):
            _mark(marker, "stop")


class FailPrepare(Probe):
    async def prepare(self, ctx, config, restored_state):
        await super().prepare(ctx, config, restored_state)
        raise RuntimeError("synthetic prepare failure")


class FailStart(Probe):
    async def start(self):
        raise RuntimeError("synthetic start failure")


class FailStop(Probe):
    async def stop(self, reason):
        if marker := self.ctx.config.get("resource_marker"):
            _mark(marker, "stop-failed")
        raise RuntimeError("synthetic stop failure")


class BlockStop(Probe):
    async def stop(self, reason):
        time.sleep(60)
