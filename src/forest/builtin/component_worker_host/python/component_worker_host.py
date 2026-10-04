"""Component control projection and durable reload submission."""

import json
from hashlib import md5, sha256

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


class ComponentWorkerHost(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.component-worker-host/1"}:
            raise ForestError(
                "COMPONENT_HOST_CONFIGURATION_INVALID",
                "Component Worker Host configuration is invalid",
                422,
            )

    async def _graph(self, ctx):
        return await ctx.services.call(
            "runtime.graph.snapshot",
            {"protocol": "forest.runtime-graph-snapshot-request/1"},
            operation_id=ctx.operations.stable("graph-snapshot"),
            timeout=10,
        )

    @service("component.host.packages", semantics="read")
    async def packages(self, payload, ctx):
        return await ctx.services.call(
            "deployment.package-registry.packages",
            {"protocol": "forest.package-catalog-page-request/1", **payload},
            operation_id=ctx.operations.stable("package-catalog-page"),
            timeout=30,
        )

    @service("component.host.artifact", semantics="read")
    async def artifact(self, payload, ctx):
        return await ctx.services.call(
            "deployment.package-registry.artifact",
            {"protocol": "forest.package-catalog-artifact-request/1", **payload},
            operation_id=ctx.operations.stable("package-catalog-artifact"),
            timeout=30,
        )

    @service("component.host.validate", semantics="read")
    async def validate(self, payload, ctx):
        return await ctx.services.call(
            "runtime.component-candidate.preview",
            {"protocol": "forest.component-candidate-preview-request/1", **payload},
            operation_id=ctx.operations.stable("component-candidate-preview"),
            timeout=45,
        )

    @service("component.host.modules", semantics="read")
    async def modules(self, payload, ctx):
        graph = await self._graph(ctx)
        expected_graph = payload.get("graph_id")
        if expected_graph is not None and expected_graph != graph["graph_id"]:
            raise ForestError(
                "COMPONENT_HOST_GRAPH_CHANGED",
                "RuntimeGraph changed during component pagination",
                409,
            )
        after = payload.get("after", "")
        limit = payload.get("limit", 32)
        members = sorted(graph["members"], key=lambda item: item["component_id"])
        following = [item for item in members if item["component_id"] > after]
        page = following[:limit]
        return {
            "protocol": "forest.component-host-modules/1",
            "bot_id": graph["bot_id"],
            "graph_id": graph["graph_id"],
            "graph_revision": graph["graph_revision"],
            "members": [
                {
                    "component_id": item["component_id"],
                    "status": item["status"],
                    "package_id": item["package_id"],
                    "package_version": item["package_version"],
                }
                for item in page
            ],
            "next_after": page[-1]["component_id"] if len(following) > limit else None,
        }

    @service("component.host.module", semantics="read")
    async def module(self, payload, ctx):
        graph = await self._graph(ctx)
        member = next(
            (
                item for item in graph["members"]
                if item["component_id"] == payload["component_id"]
            ),
            None,
        )
        if member is None:
            raise ForestError(
                "COMPONENT_HOST_MODULE_NOT_FOUND", "Component is not in the active graph", 404
            )
        return {
            "protocol": "forest.component-host-module/1",
            "bot_id": graph["bot_id"],
            "graph_id": graph["graph_id"],
            "graph_revision": graph["graph_revision"],
            "member": member,
        }

    @service("shell.component-host.execute", semantics="at-most-once")
    async def shell(self, payload, ctx):
        graph = await self._graph(ctx)
        return {
            "text": "Forest RuntimeGraph components:\n"
            + "\n".join(
                f"{item['component_id']} {item['status']} "
                f"r{item['config_revision']} {item['generation_id']}"
                for item in graph["members"]
            )
        }

    @service("shell.component-host.core-status", semantics="read")
    async def core_status(self, payload, ctx):
        status = await ctx.services.call(
            "runtime.status.snapshot",
            {"protocol": "forest.runtime-status-snapshot-request/1"},
            operation_id=ctx.operations.stable("core-status"),
            timeout=10,
        )
        failures = [item for item in status["members"] if item["status"] != "ACTIVE"]
        text = (
            f"Forest {status['bot_id']}：{status['runtime_status']} / {status['graph_state']}；"
            f"Graph r{status['graph_revision']}，{len(status['members'])} 个模块；"
            f"DATA={'open' if status['data_admission'] else 'closed'}，"
            f"CONTROL={'open' if status['control_admission'] else 'closed'}"
        )
        if failures:
            text += f"；异常模块 {len(failures)} 个"
        replacement = status["latest_replacement"]
        if replacement is not None:
            text += (
                f"；最近重载={replacement['state']}"
                f" operation={replacement['operation_id']}"
            )
            if replacement["error_code"]:
                text += f" error={replacement['error_code']}"
        return {"text": text}

    @service("shell.component-host.core-reload", semantics="idempotent")
    async def core_reload(self, payload, ctx):
        saved = await ctx.services.call(
            "runtime.graph-replacement.request",
            {
                "protocol": "forest.runtime-graph-replacement-request/2",
                "operation_id": "shell-reload:" + payload["request_id"],
                "expected_graph_revision": payload["arguments"]["expected_graph_revision"],
                "requested_by": payload["principal_ref"],
                "target_component_id": None,
            },
            operation_id=ctx.operations.stable("core-reload"),
            timeout=10,
        )
        return {
            "text": f"Forest RuntimeGraph reload {saved['state']}: "
            f"operation={saved['operation_id']} activation={saved['activation_id']}"
        }

    @service("shell.component-host.core-maintenance", semantics="at-most-once")
    async def core_maintenance(self, payload, ctx):
        data_admission = payload["command_id"] == "core.resume"
        saved = await ctx.services.call(
            "runtime.maintenance.set",
            {
                "protocol": "forest.runtime-maintenance-set-request/1",
                "expected_maintenance_epoch": payload["arguments"]["expected_maintenance_epoch"],
                "data_admission": data_admission,
            },
            operation_id=ctx.operations.stable("core-maintenance"),
            timeout=10,
        )
        return {
            "text": f"Forest DATA admission 已{'恢复' if data_admission else '暂停'}；"
            f"maintenance_epoch={saved['maintenance_epoch']}"
        }

    @service("component.host.reload", semantics="read")
    async def reload_status(self, payload, ctx):
        saved = await ctx.queries.call("component.reload.read", payload)
        kernel = None
        if saved["state"] == "SUBMITTED":
            kernel = await ctx.services.call(
                "runtime.graph-replacement.status",
                {
                    "protocol": "forest.runtime-graph-replacement-status-request/1",
                    "operation_id": "component-reload:" + saved["request_id"],
                },
                operation_id=ctx.operations.stable("read-graph-replacement"),
                timeout=10,
            )
        return {
            "protocol": "forest.component-reload-status/1",
            "request_id": saved["request_id"],
            "component_id": saved["component_id"],
            "expected_graph_revision": saved["expected_graph_revision"],
            "requested_by": saved["requested_by"],
            "state": saved["state"],
            "attempt_count": saved["attempt_count"],
            "error_code": saved["error_code"],
            "kernel": kernel,
            "created_at": saved["created_at"],
            "updated_at": saved["updated_at"],
        }

    @service("component.host.selection-application", semantics="read")
    async def selection_application_status(self, payload, ctx):
        saved = await ctx.queries.call("component.selection-application.read", payload)
        kernel = None
        if saved["state"] == "SUBMITTED":
            kernel = await ctx.services.call(
                "runtime.graph-replacement.status",
                {
                    "protocol": "forest.runtime-graph-replacement-status-request/1",
                    "operation_id": "component-selection:" + saved["request_id"],
                },
                operation_id=ctx.operations.stable("read-selection-replacement"),
                timeout=10,
            )
        return {
            **saved,
            "protocol": "forest.component-selection-application-status/1",
            "kernel": kernel,
        }

    @service("shell.component-host.reload", semantics="idempotent")
    async def reload(self, payload, ctx):
        arguments = payload["arguments"]
        component_id = arguments["component_id"]
        expected = arguments["expected_graph_revision"]
        graph = await self._graph(ctx)
        operation_id = ctx.operations.stable("module-reload-enqueue")
        command = {
            "component_id": component_id,
            "expected_graph_revision": expected,
            "requested_by": payload["principal_ref"],
        }
        if graph["graph_revision"] != expected:
            request_id = "reload:" + md5(
                (graph["bot_id"] + "\x1f" + operation_id).encode("utf-8"),
                usedforsecurity=False,
            ).hexdigest()
            try:
                saved = await ctx.queries.call(
                    "component.reload.read", {"request_id": request_id}
                )
            except ForestError as error:
                if error.code != "COMPONENT_RELOAD_NOT_FOUND":
                    raise
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT", "RuntimeGraph revision changed", 409
                ) from error
            if (
                saved["component_id"] != component_id
                or saved["expected_graph_revision"] != expected
                or saved["requested_by"] != payload["principal_ref"]
            ):
                raise ForestError(
                    "COMPONENT_RELOAD_OPERATION_CONFLICT", "Reload operation input differs", 409
                )
            return {
                "text": f"Reload request for earlier Graph r{expected}: "
                f"{saved['request_id']} ({saved['state']})."
            }
        if not any(item["component_id"] == component_id for item in graph["members"]):
            raise ForestError(
                "COMPONENT_HOST_MODULE_NOT_FOUND", "Component is not in the active graph", 404
            )
        saved = await ctx.commands.call(
            "component.reload.enqueue", command, operation_id=operation_id
        )
        return {
            "text": f"Reload request queued for {component_id}: {saved['request_id']} "
            f"({saved['state']}). The target and its dependent Generations will be "
            "replaced; unaffected Generations retain their identity."
        }

    @service("shell.component-host.validate", semantics="read")
    async def shell_validate(self, payload, ctx):
        proposal = await self.validate(payload["arguments"], ctx)
        impact = proposal["impact"]
        return {
            "text": (
                f"{proposal['target_component_id']} "
                f"{'enable' if proposal['desired_enabled'] else 'disable'}: "
                f"plan {proposal['plan_digest']}\n"
                f"Graph r{proposal['source_graph_revision']}, "
                f"selection r{payload['arguments']['expected_selection_revision']}"
                f" -> r{proposal['selection_revision']}; "
                f"replace {len(impact['replace'])}, add {len(impact['add'])}, "
                f"remove {len(impact['remove'])}."
            )
        }

    @service("shell.component-host.selection", semantics="idempotent")
    async def shell_selection_route(self, payload, ctx):
        raise ForestError(
            "SHELL_MANAGEMENT_ROUTE_REQUIRED",
            "Selection actions require the Generation-bound management route",
            403,
        )

    @job("component.reload.submit")
    async def submit_reload(self, task, ctx):
        work = task["payload"]
        try:
            graph = await self._graph(ctx)
            if graph["graph_revision"] == work["expected_graph_revision"] and not any(
                member["component_id"] == work["component_id"]
                for member in graph["members"]
            ):
                return {
                    "disposition": "FAILED",
                    "receipt": None,
                    "error_code": "COMPONENT_HOST_MODULE_NOT_FOUND",
                }
            receipt = await ctx.services.call(
                "runtime.graph-replacement.request",
                {
                    "protocol": "forest.runtime-graph-replacement-request/2",
                    "operation_id": "component-reload:" + work["request_id"],
                    "expected_graph_revision": work["expected_graph_revision"],
                    "requested_by": work["requested_by"],
                    "target_component_id": work["component_id"],
                },
                operation_id=ctx.operations.stable("submit-graph-replacement"),
                timeout=10,
            )
            return {"disposition": "SUCCEEDED", "receipt": receipt, "error_code": None}
        except ForestError as error:
            return {
                "disposition": "RETRY" if error.status >= 500 else "FAILED",
                "receipt": None,
                "error_code": error.code,
            }

    @job("runtime.health-snapshot", input_schema="schemas/runtime-health-input.json",
         max_attempts=3, timeout_s=30, max_concurrency=1)
    async def runtime_health_snapshot(self, task, ctx):
        if not isinstance(task, dict) or task.get("input") != {}:
            raise ForestError("JOB_EXECUTION_INPUT_INVALID", "Job input is invalid", 422)
        status = await ctx.services.call(
            "runtime.status.snapshot",
            {"protocol": "forest.runtime-status-snapshot-request/1"},
            operation_id=ctx.operations.stable("runtime-status-snapshot"),
            timeout=10,
        )
        if status.get("protocol") != "forest.runtime-status-snapshot/1":
            raise ForestError(
                "RUNTIME_STATUS_SNAPSHOT_INVALID",
                "Kernel runtime status snapshot is invalid",
                409,
            )
        return {
            **{key: value for key, value in status.items() if key != "latest_replacement"},
            "protocol": "forest.runtime-health-snapshot/1",
        }

    @job("component.selection.submit")
    async def submit_selection(self, task, ctx):
        work = task["payload"]
        try:
            current = await ctx.queries.call("component.selection.snapshot", {})
            if current != work["snapshot"]:
                return {
                    "disposition": "SUPERSEDED", "receipt": None,
                    "error_code": "COMPONENT_SELECTION_CHANGED",
                }
            # Kernel replays this exact operation before checking a new Graph revision.
            encoded = json.dumps(
                work["snapshot"], sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False,
            ).encode("utf-8")
            receipt = await ctx.services.call(
                "runtime.graph-replacement.request",
                {
                    "protocol": "forest.runtime-graph-replacement-request/3",
                    "operation_id": "component-selection:" + work["request_id"],
                    "expected_graph_revision": work["expected_graph_revision"],
                    "requested_by": work["requested_by"],
                    "target_component_id": work["component_id"],
                    "selection_revision": work["selection_revision"],
                    "selection_digest": "sha256:" + sha256(encoded).hexdigest(),
                },
                operation_id=ctx.operations.stable("submit-selection-replacement"),
                timeout=10,
            )
            return {"disposition": "SUBMITTED", "receipt": receipt, "error_code": None}
        except ForestError as error:
            return {
                "disposition": "RETRY" if error.status >= 500 else "FAILED",
                "receipt": None,
                "error_code": error.code,
            }

    @job("component.configuration-rebuild.submit")
    async def submit_configuration_rebuild(self, task, ctx):
        work = task["payload"]
        try:
            graph = await self._graph(ctx)
            member = next(
                (
                    item for item in graph["members"]
                    if item["component_id"] == work["target_component_id"]
                ),
                None,
            )
            if member is None:
                return {
                    "disposition": "FAILED",
                    "receipt": None,
                    "error_code": "COMPONENT_HOST_MODULE_NOT_FOUND",
                }
            if member["config_revision"] == work["desired_revision"]:
                return {"disposition": "EFFECTIVE", "receipt": None, "error_code": None}
            if member["config_revision"] > work["desired_revision"]:
                return {
                    "disposition": "FAILED",
                    "receipt": None,
                    "error_code": "COMPONENT_CONFIGURATION_REBUILD_SUPERSEDED",
                }
            previous = work["kernel_receipt"]
            if previous is not None and previous["expected_graph_revision"] == graph["graph_revision"]:
                status = await ctx.services.call(
                    "runtime.graph-replacement.status",
                    {
                        "protocol": "forest.runtime-graph-replacement-status-request/1",
                        "operation_id": previous["operation_id"],
                    },
                    operation_id=ctx.operations.stable("read-configuration-rebuild"),
                    timeout=10,
                )
                if status["state"] == "FAILED":
                    return {
                        "disposition": "FAILED",
                        "receipt": None,
                        "error_code": status["error_code"],
                    }
                if status["state"] != "SUCCEEDED":
                    return {
                        "disposition": "REQUESTED",
                        "receipt": previous,
                        "error_code": None,
                    }
                return {
                    "disposition": "RETRY",
                    "receipt": None,
                    "error_code": "COMPONENT_CONFIGURATION_REBUILD_GRAPH_PENDING",
                }
            receipt = await ctx.services.call(
                "runtime.graph-replacement.request",
                {
                    "protocol": "forest.runtime-graph-replacement-request/2",
                    "operation_id": (
                        "configuration-rebuild:" + work["request_id"]
                        + ":graph:" + str(graph["graph_revision"])
                    ),
                    "expected_graph_revision": graph["graph_revision"],
                    "requested_by": work["requested_by"],
                    "target_component_id": work["target_component_id"],
                },
                operation_id=ctx.operations.stable("submit-configuration-rebuild"),
                timeout=10,
            )
            return {"disposition": "REQUESTED", "receipt": receipt, "error_code": None}
        except ForestError as error:
            return {
                "disposition": "RETRY" if error.status in (409, 429) or error.status >= 500 else "FAILED",
                "receipt": None,
                "error_code": error.code,
            }
