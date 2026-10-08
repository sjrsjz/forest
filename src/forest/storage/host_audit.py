"""Host-owned management audit authority."""

from forest.storage.control_audit import ControlAuditRepository


class HostAuditRepository(ControlAuditRepository):
    scope_kind = "host"
