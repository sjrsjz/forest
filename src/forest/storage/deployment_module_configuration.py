"""Deployment binding for shared control-scope configuration."""

from forest.storage.control_module_configuration import ControlModuleConfigurationRepository


class DeploymentModuleConfigurationRepository(ControlModuleConfigurationRepository):
    scope = "deployment"
