"""Host binding for the control-scope package Graph controller."""

from forest.control_package_controller import ControlPackageController


class HostPackageController(ControlPackageController):
    def __init__(self, runtime, catalog, *, official_sources):
        if getattr(runtime, "scope_kind", None) != "host":
            raise ValueError("Host controller requires Host scope")
        super().__init__(runtime, catalog, official_sources=official_sources)
