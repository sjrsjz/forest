"""Deployment-scoped package SQL installer."""

from forest.storage.control_schema_bundles import ControlScopeSchemaBundleInstaller


class DeploymentSchemaBundleInstaller(ControlScopeSchemaBundleInstaller):
    scope = "deployment"
