"""Host binding for the control-scope package Graph controller."""

from forest.control_driver_binding_gate import ControlDriverBindingGate
from forest.control_package_controller import ControlPackageController


class HostPackageController(ControlPackageController):
    def __init__(
        self, runtime, catalog, *, official_sources,
        driver_binding_gate: ControlDriverBindingGate | None = None,
        joint_driver_transition=None,
    ):
        if getattr(runtime, "scope_kind", None) != "host":
            raise ValueError("Host controller requires Host scope")
        super().__init__(
            runtime, catalog, official_sources=official_sources,
            driver_binding_gate=driver_binding_gate,
            joint_driver_transition=joint_driver_transition,
        )
