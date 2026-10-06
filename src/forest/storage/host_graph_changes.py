"""Host binding for durable control-scope Graph changes."""

from forest.storage.control_graph_changes import ControlGraphChangeAuthority


class HostGraphChangeAuthority(ControlGraphChangeAuthority):
    scope = "host"
