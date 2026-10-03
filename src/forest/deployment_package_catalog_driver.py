"""Generation-bound read port for the deployment-wide package artifact catalog."""

from __future__ import annotations

import asyncio

from forest.errors import ForestError
from forest.package_effects import PackageArtifactDownload

_SERVICES = frozenset({
    "deployment.catalog.page", "deployment.catalog.artifact",
    "deployment.catalog.import", "deployment.catalog.import-receipt",
    "deployment.catalog.import-close",
})


def _check_external_manifest(manifest: dict) -> None:
    package_id = manifest["package_id"]
    if package_id == "forest" or package_id.startswith("forest."):
        raise ForestError(
            "PACKAGE_RESERVED_NAMESPACE",
            "Web imports cannot reserve the official package namespace", 403,
        )


class DeploymentPackageCatalogEffectDriver:
    def __init__(self, configuration, configuration_revision, identity, declaration, *, catalog):
        if (
            declaration.get("name") not in _SERVICES
            or declaration.get("kind") != "deployment.package-catalog"
            or declaration.get("semantics") != (
                "idempotent" if declaration.get("name") in {
                    "deployment.catalog.import", "deployment.catalog.import-close",
                } else "read"
            )
            or identity.component_id != "deployment.package-registry"
            or catalog is None
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Deployment package catalog driver received another port", 500,
            )
        self.service_id = declaration["name"]
        self.catalog = catalog
        self.identity = identity
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False

    async def invoke(self, payload, *, scope, invocation) -> dict:
        if (
            not self.accepting
            or scope.scope_kind != "deployment"
            or scope.component_id != "deployment.package-registry"
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Deployment catalog Generation is not admitting reads", 409,
            )
        if self.service_id == "deployment.catalog.import":
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Package import accepts only the declared artifact upload operation", 403,
            )
        if self.service_id == "deployment.catalog.import-receipt":
            receipt = await asyncio.to_thread(self.catalog.import_receipt, **payload)
            return {
                "protocol": "forest.package-import-receipt/1",
                "found": receipt is not None,
                "package_digest": receipt["package_digest"] if receipt is not None else None,
            }
        if self.service_id == "deployment.catalog.import-close":
            return await asyncio.to_thread(self.catalog.close_import_if_absent, **payload)
        if self.service_id == "deployment.catalog.page":
            return await asyncio.to_thread(self.catalog.page, **payload)
        return await asyncio.to_thread(self.catalog.artifact, payload["package_digest"])

    async def download(self, payload: dict, *, max_bytes: int) -> PackageArtifactDownload:
        if not self.accepting or self.service_id != "deployment.catalog.artifact":
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Package archive port is unavailable", 409
            )
        digest = payload["package_digest"]
        content = await asyncio.to_thread(self.catalog.export_archive, digest)
        if len(content) > max_bytes:
            raise ForestError(
                "PACKAGE_TOO_LARGE", "Package archive exceeds the declared download bound", 413
            )
        return PackageArtifactDownload(
            content, "application/zip", "forest-package-" + digest[7:23] + ".zip"
        )

    async def upload(
        self, payload: dict, content: bytes, *, operation_id: str,
        actor_ref: str, content_sha256: str, max_bytes: int,
    ) -> dict:
        if not self.accepting or self.service_id != "deployment.catalog.import":
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Package import port is unavailable", 409,
            )
        if len(content) > max_bytes:
            raise ForestError("PACKAGE_TOO_LARGE", "Package ZIP exceeds its declared limit", 413)
        snapshot = await asyncio.to_thread(
            self.catalog.import_archive_idempotent, content,
            operation_id=operation_id, actor_ref=actor_ref,
            archive_sha256=content_sha256,
            check_manifest=_check_external_manifest,
        )
        return await asyncio.to_thread(self.catalog.artifact, snapshot.digest)

    async def close_upload(self, payload: dict) -> dict:
        if not self.accepting or self.service_id != "deployment.catalog.import-close":
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE", "Package import close port is unavailable", 409,
            )
        return await asyncio.to_thread(self.catalog.close_import_if_absent, **payload)
