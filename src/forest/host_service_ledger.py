"""Host binding for the shared control-scope service ledger."""

from forest.control_service_ledger import ControlPackageServiceOperationLedger


class HostPackageServiceOperationLedger(ControlPackageServiceOperationLedger):
    scope = "host"
