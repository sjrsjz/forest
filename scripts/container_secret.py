"""Atomically install one allowlisted application secret into the Compose volume."""

from __future__ import annotations

import codecs
import fcntl
import os
import secrets
import stat
import sys
from pathlib import Path

APP_UID = 10001
APP_GID = 10001
MAX_SECRET_BYTES = 65536
SECRET_NAMES = {
    "deployment_management_token",
    "deployment_master_key",
}


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_secret() -> bytes:
    value = sys.stdin.buffer.read(MAX_SECRET_BYTES + 2)
    if value.startswith(codecs.BOM_UTF8):
        value = value[len(codecs.BOM_UTF8) :]
    if value.endswith(b"\r\n"):
        value = value[:-2]
    elif value.endswith(b"\n"):
        value = value[:-1]
    if not 1 <= len(value) <= MAX_SECRET_BYTES or b"\x00" in value or b"\n" in value or b"\r" in value:
        raise RuntimeError("Secret input must be one bounded nonempty line")
    if any(not 33 <= item <= 126 for item in value):
        raise RuntimeError("Secret input must contain printable ASCII only")
    return value


def install_secret(path: Path, name: str, value: bytes) -> None:
    if name not in SECRET_NAMES:
        raise RuntimeError("FOREST_SECRET_NAME is not allowlisted")
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, APP_UID}:
        raise RuntimeError("Secret volume ownership is invalid")
    lock_path = path / ".initialize.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "r+b") as lock:
        lock_info = os.fstat(lock.fileno())
        if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1 or lock_info.st_uid != 0:
            raise RuntimeError("Invalid secret initialization lock")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        target = path / name
        if target.exists() or target.is_symlink():
            saved = target.lstat()
            if (
                not stat.S_ISREG(saved.st_mode)
                or saved.st_nlink != 1
                or saved.st_uid != APP_UID
                or saved.st_gid != APP_GID
                or stat.S_IMODE(saved.st_mode) != 0o400
            ):
                raise RuntimeError("Existing application credential is invalid")
        temporary = path / f".secret-{secrets.token_hex(12)}"
        temporary_descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        try:
            with os.fdopen(temporary_descriptor, "wb") as output:
                output.write(value)
                os.fchown(output.fileno(), APP_UID, APP_GID)
                os.fchmod(output.fileno(), 0o400)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
            _fsync_directory(path)
        finally:
            temporary.unlink(missing_ok=True)


def main() -> None:
    try:
        if os.geteuid() != 0:
            raise RuntimeError("Secret installation requires the dedicated bootstrap container")
        name = os.environ.get("FOREST_SECRET_NAME", "")
        install_secret(Path("/run/forest-secrets"), name, _read_secret())
    except (OSError, RuntimeError) as exc:
        print(f"Forest secret installation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    print(f"Forest application credential {name} is installed.")


if __name__ == "__main__":
    main()
