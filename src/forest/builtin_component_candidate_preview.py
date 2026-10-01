"""Restricted read-only bridge from Worker Host to Host Graph compilation."""

from __future__ import annotations

import re

from forest.errors import ForestError

_COMPONENT_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")
_FIELDS = {
    "protocol", "component_id", "source_kind", "package_digest",
    "manifest_digest", "approved_capabilities", "desired_enabled",
    "expected_selection_revision", "expected_graph_revision",
    "expected_configuration_revision",
}


def component_candidate_preview_service(context):
    async def read(request):
        if not isinstance(request, dict) or set(request) != _FIELDS:
            raise ForestError(
                "COMPONENT_PREVIEW_INPUT_INVALID",
                "Component candidate preview input is invalid",
                422,
            )
        capabilities = request["approved_capabilities"]
        source_kind = request["source_kind"]
        enabled = request["desired_enabled"]
        revision = request["expected_configuration_revision"]
        if (
            request["protocol"] != "forest.component-candidate-preview-request/1"
            or not isinstance(request["component_id"], str)
            or _COMPONENT_ID.fullmatch(request["component_id"]) is None
            or not isinstance(source_kind, str)
            or source_kind not in {"builtin", "installed"}
            or type(enabled) is not bool
            or type(request["expected_selection_revision"]) is not int
            or request["expected_selection_revision"] < 0
            or type(request["expected_graph_revision"]) is not int
            or request["expected_graph_revision"] < 1
            or not isinstance(capabilities, list)
            or len(capabilities) > 64
            or any(not isinstance(item, str) for item in capabilities)
            or len(capabilities) != len(set(capabilities))
            or (
                source_kind == "builtin"
                and (
                    request["package_digest"] is not None
                    or request["manifest_digest"] is not None
                    or capabilities
                    or revision is not None
                )
            )
            or (
                source_kind == "installed"
                and (
                    not isinstance(request["package_digest"], str)
                    or _DIGEST.fullmatch(request["package_digest"]) is None
                    or not isinstance(request["manifest_digest"], str)
                    or _DIGEST.fullmatch(request["manifest_digest"]) is None
                    or (enabled and (type(revision) is not int or revision < 1))
                    or (not enabled and revision is not None)
                )
            )
        ):
            raise ForestError(
                "COMPONENT_PREVIEW_INPUT_INVALID",
                "Component candidate preview input is invalid",
                422,
            )
        preview = await context.component_candidate_previewer(
            component_id=request["component_id"],
            source_kind=source_kind,
            package_digest=request["package_digest"],
            manifest_digest=request["manifest_digest"],
            approved_capabilities=tuple(capabilities),
            desired_enabled=enabled,
            expected_selection_revision=request["expected_selection_revision"],
            expected_graph_revision=request["expected_graph_revision"],
            expected_configuration_revision=revision,
        )
        return {
            "protocol": "forest.component-candidate-preview/1",
            "bot_id": context.store.bot_id,
            "source_graph_id": preview.source_graph_id,
            "source_graph_revision": preview.source_graph_revision,
            "selection_revision": preview.selection.revision,
            "selection_digest": preview.selection.digest,
            "target_component_id": preview.target_component_id,
            "desired_enabled": enabled,
            "plan_digest": preview.plan_digest,
            "impact": {
                field: list(getattr(preview.impact, field))
                for field in ("replace", "retain", "remove", "add", "drain", "prepare")
            },
            "packages": [
                {
                    "component_id": item.component_id,
                    "trust_level": item.trust_level,
                    "package_digest": item.package_digest,
                    "configuration_revision": item.configuration_revision,
                    "configuration_schema_digest": item.configuration_schema_digest,
                    "storage_digest": item.storage_digest,
                    "worker_limits": item.worker_limits.as_fact(),
                }
                for item in preview.packages
            ],
        }

    return read
