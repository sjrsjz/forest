"""Host bindings for official, Generation-fenced native control ports."""

from forest.control_kernel_management import (
    ControlKernelManagementAuthority,
    ControlKernelQueryAuthority,
)


class HostKernelQueryAuthority(ControlKernelQueryAuthority):
    scope_kind = "host"


class HostKernelManagementAuthority(ControlKernelManagementAuthority):
    scope_kind = "host"
