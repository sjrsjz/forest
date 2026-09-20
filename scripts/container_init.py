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
APPLICATION_SECRET_NAMES = {
    "deployment_management_token",
    "deployment_master_key",
}


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


def prepare_secrets(path: Path) -> None:
    info = directory_info(path)
    if info.st_uid not in {0, APP_UID}:
        raise RuntimeError("Secret volume belongs to another service account")
    permitted = {".initialize.lock", "postgres_password", *APPLICATION_SECRET_NAMES}
    if any(
        item.name not in permitted
        and not item.name.startswith(".password-")
        and not item.name.startswith(".secret-")
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
            if Path("/var/lib/forest-postgres/pgdata/PG_VERSION").exists():
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
        for name in APPLICATION_SECRET_NAMES:
            application_secret = path / name
            if not application_secret.exists() and not application_secret.is_symlink():
                continue
            saved = application_secret.lstat()
            if (
                not stat.S_ISREG(saved.st_mode)
                or saved.st_nlink != 1
                or saved.st_uid != APP_UID
                or saved.st_gid != APP_GID
                or stat.S_IMODE(saved.st_mode) != 0o400
                or not 1 <= saved.st_size <= 65536
            ):
                raise RuntimeError(
                    f"Existing {name} credential has unexpected ownership or permissions"
                )
            value = application_secret.read_bytes()
            if any(not 33 <= item <= 126 for item in value):
                raise RuntimeError(f"Existing {name} credential is not printable ASCII")
        os.chown(path, APP_UID, POSTGRES_GID)
        os.chmod(path, 0o550)
        fsync_directory(path)


def main() -> None:
    try:
        if os.geteuid() != 0:
            raise RuntimeError(
                "Volume initialization must run through the dedicated installer service"
            )
        prepare_secrets(Path("/run/forest-secrets"))
    except (OSError, RuntimeError) as exc:
        # OSError text can expose paths, but no credential is used in a path.
        # No configuration, credential content, or environment is dumped here.
        print(f"Forest volume initialization failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    print("Forest protected deployment credentials are ready.")


if __name__ == "__main__":
    main()
