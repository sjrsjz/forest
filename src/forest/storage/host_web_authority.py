"""Host binding for signed control-scope Web receipts."""

from forest.storage.control_web_authority import ControlWebAuthorityRepository


class HostWebAuthorityRepository(ControlWebAuthorityRepository):
    scope = "host"
