"""Host-scoped RuntimeGraph authority."""

from forest.storage.control_generation import ControlGenerationAuthority


class HostGenerationAuthority(ControlGenerationAuthority):
    scope = "host"
