"""Deployment-scoped RuntimeGraph authority."""

from forest.storage.control_generation import ControlGenerationAuthority


class DeploymentGenerationAuthority(ControlGenerationAuthority):
    scope = "deployment"
