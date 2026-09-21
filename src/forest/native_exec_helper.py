"""Child-side guard for configured native execution profiles.

The parent never passes a shell command.  This helper receives one canonical
manifest on stdin, applies process limits and the requested network policy,
then replaces itself with the profile's fixed executable.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import json
import os
import sys

try:
    import resource
except ImportError:  # pragma: no cover - Windows development host
    resource = None


def seccomp_available() -> bool:
    return sys.platform.startswith("linux") and ctypes.util.find_library("seccomp") is not None


def _deny_network() -> None:
    library_name = ctypes.util.find_library("seccomp")
    if not sys.platform.startswith("linux") or library_name is None:
        raise RuntimeError("network isolation is unavailable")
    library = ctypes.CDLL(library_name, use_errno=True)
    library.seccomp_init.argtypes = (ctypes.c_uint32,)
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_release.argtypes = (ctypes.c_void_p,)
    library.seccomp_rule_add.argtypes = (
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint,
    )
    library.seccomp_rule_add.restype = ctypes.c_int
    library.seccomp_load.argtypes = (ctypes.c_void_p,)
    library.seccomp_load.restype = ctypes.c_int
    library.seccomp_syscall_resolve_name.argtypes = (ctypes.c_char_p,)
    library.seccomp_syscall_resolve_name.restype = ctypes.c_int
    allow = 0x7FFF0000
    deny = 0x00050000 | errno.EPERM
    context = library.seccomp_init(allow)
    if not context:
        raise RuntimeError("unable to initialize seccomp")
    try:
        for name in (
            "socket",
            "socketpair",
            "connect",
            "accept",
            "accept4",
            "bind",
            "listen",
            "sendto",
            "sendmsg",
            "sendmmsg",
            "recvfrom",
            "recvmsg",
            "recvmmsg",
            "shutdown",
            "getsockname",
            "getpeername",
            "setsockopt",
            "getsockopt",
            "io_uring_setup",
            "io_uring_enter",
            "io_uring_register",
        ):
            number = library.seccomp_syscall_resolve_name(name.encode("ascii"))
            if number >= 0 and library.seccomp_rule_add(context, deny, number, 0) != 0:
                raise RuntimeError("unable to install seccomp rule")
        if library.seccomp_load(context) != 0:
            raise RuntimeError("unable to load seccomp policy")
    finally:
        library.seccomp_release(context)


def _limit(kind: int, value: int) -> None:
    resource.setrlimit(kind, (value, value))


def main() -> None:
    manifest = json.loads(sys.stdin.buffer.read())
    limits = manifest["limits"]
    if os.name == "posix" and resource is not None:
        identity = manifest["run_as"]
        if os.getuid() != identity["uid"] or os.getgid() != identity["gid"]:
            raise RuntimeError("execution identity differs")
        _limit(resource.RLIMIT_CPU, limits["cpu_seconds"])
        _limit(resource.RLIMIT_AS, limits["memory_bytes"])
        _limit(resource.RLIMIT_FSIZE, limits["output_bytes"])
        _limit(resource.RLIMIT_NOFILE, limits["open_files"])
        if hasattr(resource, "RLIMIT_NPROC"):
            _limit(resource.RLIMIT_NPROC, limits["processes"])
    elif manifest["require_posix_limits"]:
        raise RuntimeError("POSIX resource limits are unavailable")
    if manifest["network_policy"] == "deny":
        _deny_network()
    os.umask(0o077)
    os.chdir(manifest["working_directory"])
    os.execve(manifest["executable"], manifest["argv"], manifest["environment"])


if __name__ == "__main__":
    main()
