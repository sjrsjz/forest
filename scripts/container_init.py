"""Prepare dedicated Compose volumes without replacing credentials or data.

Runs once as root with no network access. The application runs as UID 10001;
the Alpine PostgreSQL image's postgres account uses GID 70. The generated
database password is readable by these two identities and is never printed.
"""

from __future__ import annotations

import fcntl
import os
import re
import secrets
import stat
import sys
from pathlib import Path

APP_UID = 10001
APP_GID = 10001
POSTGRES_GID = 70


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def directory_info(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise RuntimeError("A deployment volume is not a regular directory")
    return info


def prepare_data(path: Path) -> None:
    info = directory_info(path)
    if info.st_uid not in {0, APP_UID}:
        raise RuntimeError("Application data belongs to another service account")
    if info.st_uid == 0 and any(path.iterdir()):
        raise RuntimeError("Refusing to take ownership of a nonempty foreign data volume")
    # Never recursively chown user files, backups, or a mounted foreign tree.
    os.chown(path, APP_UID, APP_GID)
    os.chmod(path, 0o700)
    fsync_directory(path)


def prepare_secrets(path: Path) -> None:
    info = directory_info(path)
    if info.st_uid not in {0, APP_UID}:
        raise RuntimeError("Secret volume belongs to another service account")
    permitted = {".initialize.lock", "postgres_password"}
    if any(
        item.name not in permitted and not item.name.startswith(".password-")
        for item in path.iterdir()
    ):
        raise RuntimeError("Refusing to repurpose a volume containing unrelated files")
    lock_path = path / ".initialize.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "r+b") as lock:
        lock_info = os.fstat(lock.fileno())
        if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1 or lock_info.st_uid != 0:
            raise RuntimeError("Invalid secret initialization lock")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        secret_path = path / "postgres_password"
        if secret_path.exists() or secret_path.is_symlink():
            saved = secret_path.lstat()
            if (
                not stat.S_ISREG(saved.st_mode)
                or saved.st_nlink != 1
                or saved.st_uid != APP_UID
                or saved.st_gid != POSTGRES_GID
                or stat.S_IMODE(saved.st_mode) != 0o440
            ):
                raise RuntimeError(
                    "Existing database credential has unexpected ownership or permissions"
                )
            password = secret_path.read_bytes()
            if not re.fullmatch(rb"[A-Za-z0-9_-]{64}", password):
                raise RuntimeError(
                    "Existing database credential is invalid; restore recovery material"
                )
        else:
            if Path("/var/lib/forest-postgres/pgdata/PG_VERSION").exists() or any(
                Path("/data").iterdir()
            ):
                raise RuntimeError(
                    "Database credential is missing for an existing deployment; restore protected recovery material"
                )
            temporary = path / f".password-{secrets.token_hex(12)}"
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
            )
            try:
                with os.fdopen(descriptor, "wb") as output:
                    output.write(secrets.token_urlsafe(48).encode("ascii"))
                    os.fchown(output.fileno(), APP_UID, POSTGRES_GID)
                    os.fchmod(output.fileno(), 0o440)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temporary, secret_path)
                fsync_directory(path)
            finally:
                temporary.unlink(missing_ok=True)
        os.chown(path, APP_UID, POSTGRES_GID)
        os.chmod(path, 0o550)
        fsync_directory(path)


def main() -> None:
    try:
        if os.geteuid() != 0:
            raise RuntimeError(
                "Volume initialization must run through the dedicated installer service"
            )
        prepare_data(Path("/data"))
        prepare_secrets(Path("/run/forest-secrets"))
    except (OSError, RuntimeError) as exc:
        # OSError text can expose paths, but no credential is used in a path.
        # No configuration, credential content, or environment is dumped here.
        print(f"Forest volume initialization failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    print("Forest data and protected database credential are ready.")


if __name__ == "__main__":
    main()
