"""Deployment bindings for the shared control-scope package compiler."""

from forest.control_packages import (
    bootstrap_installed_control_package,
    bootstrap_trusted_control_package,
)
from forest.errors import ForestError


def _require_deployment(runtime) -> None:
    if getattr(runtime, "scope_kind", None) != "deployment":
        raise ForestError(
            "DEPLOYMENT_PACKAGE_SCOPE_INVALID", "Deployment compiler requires deployment scope", 422,
        )


async def bootstrap_builtin_deployment_package(
    snapshot, runtime, *, plan_only=False, frozen_configuration=None,
):
    _require_deployment(runtime)
    return await bootstrap_trusted_control_package(
        snapshot, runtime, plan_only=plan_only,
        frozen_configuration=frozen_configuration,
    )


async def bootstrap_installed_deployment_package(
    snapshot, runtime, *, plan_only=False, frozen_configuration=None,
):
    _require_deployment(runtime)
    return await bootstrap_installed_control_package(
        snapshot, runtime, plan_only=plan_only,
        frozen_configuration=frozen_configuration,
    )
