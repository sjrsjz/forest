"""Official deployment-scoped management services."""

from __future__ import annotations

from dataclasses import dataclass

from forest.management.audit import DeploymentAudit
from forest.management.deployment_audit_contract import DeploymentAuditManagementContract
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    official_artifact,
)


@dataclass(frozen=True)
class DeploymentModuleHost:
    scope_kind: str
    scope_id: str
    store: object


class DeploymentAuditRuntime:
    def __init__(self, store):
        self.repository = DeploymentAudit(store)
        self.management_contract = DeploymentAuditManagementContract()


def _audit(host, _modules):
    return DeploymentAuditRuntime(host.store)


OFFICIAL_DEPLOYMENT_MODULES = (
    RuntimeModuleDescriptor(
        "deployment.audit",
        (),
        _audit,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "audit",
                "部署审计",
                ("deployment",),
                "/static/modules/deployment.audit.html",
                "/static/deployment-audit.js",
                group="security",
                order=20,
            ),
        ),
    ),
)
