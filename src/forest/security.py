"""Host-owned password hashing and encrypted local secret storage.

Call expensive password and filesystem operations from a worker thread. These
primitives do not grant a plugin permission to resolve a secret.
"""

from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import os
import secrets
import stat
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from cryptography.fernet import Fernet, InvalidToken

from forest.errors import ForestError

_SCRYPT_N = 32768
_SCRYPT_R = 8
_SCRYPT_P = 1
_MAX_PASSWORD_BYTES = 1024


def random_token() -> str:
    return secrets.token_urlsafe(32)


def digest_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _password_bytes(password: str) -> bytes:
    if not isinstance(password, str):
        raise ValueError("Password must be text")
    encoded = password.encode("utf-8")
    if len(password) < 12 or len(encoded) > _MAX_PASSWORD_BYTES:
        raise ValueError(
            "Password must contain at least 12 characters and at most 1024 UTF-8 bytes"
        )
    return encoded


def hash_password(password: str) -> str:
    try:
        encoded = _password_bytes(password)
    except (ValueError, UnicodeError) as exc:
        raise ForestError(
            "VALIDATION_FAILED",
            "Password must contain at least 12 characters and at most 1024 UTF-8 bytes",
            status=422,
        ) from exc
    salt = secrets.token_bytes(16)
    key = hashlib.scrypt(
        encoded, salt=salt, n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, maxmem=64 * 1024 * 1024, dklen=32
    )
    return "$".join(
        (
            "scrypt",
            str(_SCRYPT_N),
            str(_SCRYPT_R),
            str(_SCRYPT_P),
            base64.b64encode(salt).decode("ascii"),
            base64.b64encode(key).decode("ascii"),
        )
    )


def _decode_password_hash(encoded: str) -> tuple[bytes, bytes]:
    # Never allow a stored or attacker-supplied encoding to choose CPU/RAM costs.
    if not isinstance(encoded, str) or len(encoded) > 256:
        raise ValueError("Invalid password hash")
    algorithm, n, r, p, salt_text, key_text = encoded.split("$")
    if (algorithm, n, r, p) != ("scrypt", str(_SCRYPT_N), str(_SCRYPT_R), str(_SCRYPT_P)):
        raise ValueError("Unsupported password hash")
    salt = base64.b64decode(salt_text, validate=True)
    key = base64.b64decode(key_text, validate=True)
    if len(salt) != 16 or len(key) != 32:
        raise ValueError("Invalid password hash")
    return salt, key


def verify_password(password: str, encoded: str) -> bool:
    try:
        value = _password_bytes(password)
        salt, expected = _decode_password_hash(encoded)
    except (ValueError, TypeError, UnicodeError):
        return False
    actual = hashlib.scrypt(
        value, salt=salt, n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, maxmem=64 * 1024 * 1024, dklen=32
    )
    return hmac.compare_digest(actual, expected)


def _windows_protect(path: Path) -> None:
    """Replace the DACL with current process user + SYSTEM; fail closed."""
    from ctypes import wintypes

    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    advapi.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.LPWSTR)]
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi.SetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, ctypes.c_void_p]
    handle = wintypes.HANDLE()
    sid_text = wintypes.LPWSTR()
    descriptor = ctypes.c_void_p()
    try:
        if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 0x0008, ctypes.byref(handle)):
            raise ctypes.WinError(ctypes.get_last_error())
        size = wintypes.DWORD()
        advapi.GetTokenInformation(handle, 1, None, 0, ctypes.byref(size))
        if not size.value:
            raise ctypes.WinError(ctypes.get_last_error())
        buffer = ctypes.create_string_buffer(size.value)
        if not advapi.GetTokenInformation(handle, 1, buffer, size, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        sid = ctypes.c_void_p.from_buffer(buffer).value
        if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_text)):
            raise ctypes.WinError(ctypes.get_last_error())
        inheritance = "OICI" if path.is_dir() else ""
        sddl = f"D:P(A;{inheritance};FA;;;{sid_text.value})(A;{inheritance};FA;;;SY)"
        if not advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl, 1, ctypes.byref(descriptor), None
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        if not advapi.SetFileSecurityW(str(path), 0x00000004 | 0x80000000, descriptor):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        if descriptor.value:
            kernel.LocalFree(descriptor)
        if sid_text:
            kernel.LocalFree(ctypes.cast(sid_text, ctypes.c_void_p))
        if handle.value:
            kernel.CloseHandle(handle)


def protect_path(path: Path) -> None:
    """Apply real platform access controls, never a Windows chmod placebo."""
    if path.is_symlink():
        raise ForestError(
            "UNSAFE_DATA_PATH", "Protected storage must not be a symbolic link", status=500
        )
    try:
        if os.name == "nt":
            _windows_protect(path)
        else:
            if path.stat().st_uid != os.geteuid():
                raise ForestError(
                    "DATA_OWNER_MISMATCH",
                    "Deployment storage must be owned by the current service account",
                    status=500,
                )
            path.chmod(0o700 if path.is_dir() else 0o600)
    except OSError as exc:
        raise ForestError(
            "DATA_PERMISSIONS_FAILED", "Cannot restrict access to deployment storage", status=500
        ) from exc


def private_directory(data_dir: str | Path) -> Path:
    path = Path(data_dir).absolute()
    if path.is_symlink():
        raise ForestError(
            "UNSAFE_DATA_PATH", "Deployment storage must not be a symbolic link", status=500
        )
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    protect_path(path)
    return path


def private_file(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        os.close(descriptor)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ForestError(
            "UNSAFE_DATA_PATH", "Protected storage must be a regular unlinked file", status=500
        )
    protect_path(path)


@contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    private_file(path)
    with path.open("r+b") as lock:
        if os.fstat(lock.fileno()).st_size == 0:
            lock.write(b"\0")
            lock.flush()
        deadline = time.monotonic() + 15
        while True:
            try:
                lock.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise ForestError(
                        "SECRET_STORAGE_BUSY", "Secret storage lock is busy", status=503
                    ) from exc
                time.sleep(0.025)
        try:
            yield
        finally:
            lock.seek(0)
            if os.name == "nt":
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _sync_directory(path: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _durable_replace(source: Path, target: Path) -> None:
    if os.name == "nt":
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.MoveFileExW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        if not kernel.MoveFileExW(str(source), str(target), 0x00000001 | 0x00000008):
            raise ForestError(
                "SECRET_STORAGE_WRITE_FAILED",
                "Cannot durably publish deployment master key",
                status=503,
            )
    else:
        os.replace(source, target)
        _sync_directory(target.parent)


class SecretVault:
    """Authenticated encryption with a protected, persistent host master key.

    Loss of this key cannot be repaired by generating another key. Backups must
    protect and preserve it separately from ordinary configuration exports.
    """

    def __init__(self, data_dir: str | Path):
        directory = private_directory(data_dir)
        key_path = directory / "master.key"
        with _file_lock(directory / "master.lock"):
            if not key_path.exists():
                # Existing ciphertext without its key must never silently get a
                # replacement key during a restart.
                state_path = directory / "bootstrap.sqlite3"
                if state_path.exists() and state_path.stat().st_size:
                    raise ForestError(
                        "MASTER_KEY_MISSING",
                        "Deployment master key is missing; restore protected recovery material",
                        status=503,
                    )
                temporary = directory / f".master-{secrets.token_hex(8)}.tmp"
                try:
                    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(descriptor, "wb") as output:
                        output.write(Fernet.generate_key())
                        output.flush()
                        os.fsync(output.fileno())
                    protect_path(temporary)
                    _durable_replace(temporary, key_path)
                finally:
                    temporary.unlink(missing_ok=True)
            private_file(key_path)
            try:
                key = key_path.read_bytes()
                self._fernet = Fernet(key)
            except (ValueError, OSError) as exc:
                raise ForestError(
                    "MASTER_KEY_INVALID",
                    "Deployment master key is invalid or inaccessible",
                    status=503,
                ) from exc

    def encrypt(self, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("Secret value must be text")
        return self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str) -> str:
        try:
            return self._fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeError, ValueError, AttributeError) as exc:
            raise ForestError(
                "SECRET_DECRYPTION_FAILED",
                "Secret integrity or master key verification failed",
                status=503,
            ) from exc
