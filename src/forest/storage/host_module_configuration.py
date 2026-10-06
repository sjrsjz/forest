"""Host binding for shared control-scope configuration."""

from forest.storage.control_module_configuration import ControlModuleConfigurationRepository


class HostModuleConfigurationRepository(ControlModuleConfigurationRepository):
    scope = "host"
