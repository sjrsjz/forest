"""Deployment binding for control-scope Web receipts."""

from forest.storage.control_web_authority import ControlWebAuthorityRepository


class DeploymentWebAuthorityRepository(ControlWebAuthorityRepository):
    scope = "deployment"
