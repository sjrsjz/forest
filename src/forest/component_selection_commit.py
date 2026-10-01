"""Kernel Graph-selection preflight for a package-owned management command."""

from __future__ import annotations

from contextlib import asynccontextmanager

from forest.component_graph_preview import project_selection_graph_in
from forest.errors import ForestError
from forest.packages import digest_json


def resolve_selection_commit_preflight(
    host, identifier: str, *, component_id: str, package_digest: str
):
    """Bind a Graph capability; its package command still owns the selection write."""

    if identifier != "runtime.selection-commit":
        return None

    @asynccontextmanager
    async def preflight(payload: dict, *, operation_id: str):
        lock = host._replacement_lock
        if lock is None or host.registry is None or host._closing:
            raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
        async with lock:
            registry = host.registry
            reader = host.selection_reader
            official = host._official_descriptors
            if registry is None or reader is None or host._closing:
                raise ForestError("KERNEL_UNAVAILABLE", "Bot host is not active", 503)
            if not any(
                item.component_id == component_id
                and item.artifact.package_digest == package_digest
                for item in official
            ):
                raise ForestError(
                    "MODULE_MANAGEMENT_PREFLIGHT_UNAVAILABLE",
                    "Graph selection preflight requires the current official package",
                    503,
                )
            if (
                not isinstance(payload, dict)
                or type(payload.get("desired_enabled")) is not bool
                or not isinstance(payload.get("plan_digest"), str)
            ):
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID",
                    "Graph selection requires a direction and frozen plan digest",
                    422,
                )
            desired_enabled = payload["desired_enabled"]

            async def verify_in(conn) -> None:
                await reader.exclusive_fence_in(conn)
                replay = await reader.operation_replay_in(
                    conn,
                    operation_id=operation_id,
                    input_digest=digest_json(payload),
                    desired_enabled=desired_enabled,
                )
                if replay is not None:
                    return
                current = await reader.snapshot_in(conn)
                if current.revision != payload["expected_selection_revision"]:
                    raise ForestError(
                        "COMPONENT_SELECTION_REVISION_CONFLICT",
                        "Component selection changed before commit",
                        409,
                    )
                target = payload["component_id"]
                expected_configuration_revision = payload.get(
                    "expected_configuration_revision"
                )
                if expected_configuration_revision is not None and (
                    payload["source_kind"] != "installed" or not desired_enabled
                ):
                    raise ForestError(
                        "MODULE_CONFIGURATION_REVISION_REQUIRED",
                        "Only an enabled installed package accepts a target configuration revision",
                        422,
                    )
                preview = await project_selection_graph_in(
                    registry,
                    reader,
                    official,
                    host.package_catalog,
                    host.store,
                    kernel=host._kernel,
                    conn=conn,
                    component_id=target,
                    source_kind=payload["source_kind"],
                    package_digest=payload["package_digest"],
                    manifest_digest=payload["manifest_digest"],
                    approved_capabilities=tuple(payload["approved_capabilities"]),
                    desired_enabled=desired_enabled,
                    expected_selection_revision=current.revision,
                    expected_graph_revision=payload["expected_graph_revision"],
                    expected_configuration_revision=expected_configuration_revision,
                    ordered_configuration_locks=True,
                )
                if preview.plan_digest != payload["plan_digest"]:
                    raise ForestError(
                        "COMPONENT_SELECTION_PLAN_CHANGED",
                        "Component candidate facts changed before commit",
                        409,
                    )

            yield verify_in

    return preflight
