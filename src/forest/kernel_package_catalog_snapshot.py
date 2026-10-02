"""Scope-limited bridge from Bot Workers to the active deployment package module."""

from __future__ import annotations

from forest.errors import ForestError


def package_catalog_page_service(context):
    async def read(request):
        if (
            not isinstance(request, dict)
            or request.get("protocol") != "forest.package-catalog-page-request/1"
            or set(request) - {
                "protocol", "after_package_id", "after_version", "limit", "catalog_revision"
            }
        ):
            raise ForestError(
                "PACKAGE_CATALOG_PAGE_INVALID", "Package catalog page input is invalid", 422
            )
        return await context.package_registry_reader(
            "packages",
            {
                "limit": request.get("limit", 32),
                "scope": "bot",
                **{
                    key: request[key]
                    for key in ("after_package_id", "after_version", "catalog_revision")
                    if key in request
                },
            },
        )

    return read


def package_catalog_artifact_service(context):
    async def read(request):
        if (
            not isinstance(request, dict)
            or set(request) != {"protocol", "package_digest"}
            or request.get("protocol") != "forest.package-catalog-artifact-request/1"
        ):
            raise ForestError(
                "PACKAGE_CATALOG_ARTIFACT_INVALID",
                "Package artifact request is invalid",
                422,
            )
        artifact = await context.package_registry_reader(
            "artifact", {"package_digest": request["package_digest"]}
        )
        if artifact["scope"] != "bot":
            raise ForestError(
                "PACKAGE_SCOPE_UNSUPPORTED",
                "Bot package directory cannot expose another scope", 422,
            )
        return artifact

    return read
