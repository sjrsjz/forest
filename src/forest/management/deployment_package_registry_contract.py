"""Deployment-owned immutable package registry management contract."""

from __future__ import annotations

import asyncio

from pydantic import BaseModel, ConfigDict, Field

from forest.errors import ForestError
from forest.management.module_api import (
    ArtifactResult,
    ModuleManagementContract,
    ModuleOperation,
)
from forest.packages import MAX_ARCHIVE_BYTES, MAX_MANAGEMENT_UI_BYTES


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PackageIdentity(Body):
    package_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class DeploymentPackageRegistryManagementContract(ModuleManagementContract):
    def __init__(self, runtime):
        self.runtime = runtime
        super().__init__(
            (
                ModuleOperation(
                    "packages", "query", "deployment.packages.read", Body, self.packages
                ),
                ModuleOperation(
                    "package.import",
                    "upload",
                    "deployment.packages.import",
                    Body,
                    self.import_package,
                    principal="admin",
                    media_type="application/zip",
                    max_bytes=MAX_ARCHIVE_BYTES,
                    success_status=201,
                ),
                ModuleOperation(
                    "package.archive",
                    "download",
                    "deployment.packages.read",
                    PackageIdentity,
                    self.archive,
                    media_type="application/zip",
                    max_bytes=MAX_ARCHIVE_BYTES,
                    disposition="attachment",
                ),
                ModuleOperation(
                    "package.management-ui",
                    "download",
                    "deployment.packages.read",
                    PackageIdentity,
                    self.management_ui,
                    media_type="text/html; charset=utf-8",
                    max_bytes=MAX_MANAGEMENT_UI_BYTES,
                    disposition="inline",
                ),
            )
        )

    async def packages(self, request, body):
        return {
            "packages": self.runtime.fleet.available_packages(),
            "errors": self.runtime.fleet.package_errors,
        }

    async def import_package(self, request, body):
        snapshot = await self.runtime.fleet.registry.import_archive(
            request.artifact,
            subject_id=request.principal["subject_id"],
            session_token_hash=request.principal["session_token_hash"],
            idempotency_key=request.operation_id,
        )
        metadata = await asyncio.to_thread(self.runtime.fleet.describe_package, snapshot)
        self.runtime.fleet.package_metadata[snapshot.digest] = metadata
        self.runtime.fleet.packages[snapshot.digest] = snapshot
        self.runtime.fleet.package_errors.pop(snapshot.digest, None)
        return metadata

    async def archive(self, request, body):
        if body.package_digest not in self.runtime.fleet.packages:
            raise ForestError("PACKAGE_UNAVAILABLE", "Package is not registered", 404)
        content = await asyncio.to_thread(
            self.runtime.fleet.artifacts.export_archive, body.package_digest
        )
        return ArtifactResult(content, "forest-plugin.zip")

    async def management_ui(self, request, body):
        package = self.runtime.fleet.packages.get(body.package_digest)
        if package is None:
            raise ForestError("PACKAGE_UNAVAILABLE", "Package is not registered", 404)
        asset = await asyncio.to_thread(package.management_ui)
        if asset is None:
            raise ForestError("MANAGEMENT_UI_UNAVAILABLE", "Package has no management UI", 404)
        _, content = asset
        return ArtifactResult(content)
