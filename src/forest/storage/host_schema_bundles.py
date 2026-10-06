"""Host-scoped package SQL installer."""

from forest.storage.control_schema_bundles import ControlScopeSchemaBundleInstaller


class HostSchemaBundleInstaller(ControlScopeSchemaBundleInstaller):
    scope = "host"
