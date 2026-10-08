"""Deployment-owned management audit authority."""

from forest.storage.control_audit import ControlAuditRepository


class DeploymentAuditRepository(ControlAuditRepository):
    scope_kind = "deployment"
