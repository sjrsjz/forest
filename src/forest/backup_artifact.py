"""Bounded, passphrase-recoverable encrypted Forest backup containers."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import struct
import zipfile
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from forest.errors import ForestError
from forest.security import private_file

MAGIC = b"FOREST-BACKUP-V1"
FORMAT = "forest.backup.v1"
CHUNK = 1024 * 1024
MAX_HEADER = 16 * 1024
MAX_ARCHIVE = 64 * 1024 * 1024 * 1024
MAX_MEMBERS = 1_000_000
MAX_MEMBER = 16 * 1024 * 1024 * 1024
SCRYPT_N = 32768
SCRYPT_R = 8
SCRYPT_P = 1


def _error(code: str, message: str, status: int = 422) -> ForestError:
    return ForestError(code, message, status)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: object, size: int) -> bytes:
    if not isinstance(value, str) or not 1 <= len(value) <= 512:
        raise ValueError
    result = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    if len(result) != size:
        raise ValueError
    return result


def _passphrase(value: str) -> bytes:
    if not isinstance(value, str) or len(value) < 16:
        raise _error("BACKUP_PASSPHRASE_WEAK", "Recovery passphrase must contain at least 16 characters")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise _error("BACKUP_PASSPHRASE_INVALID", "Recovery passphrase is invalid") from None
    if len(encoded) > 1024:
        raise _error("BACKUP_PASSPHRASE_INVALID", "Recovery passphrase is too long")
    return encoded


def _derive(passphrase: bytes, salt: bytes) -> bytes:
    return hashlib.scrypt(
        passphrase, salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P,
        maxmem=64 * 1024 * 1024, dklen=32,
    )


def prepare_header(backup_id: str, passphrase: str, *, data_key: bytes | None = None) -> tuple[dict, bytes]:
    if not isinstance(backup_id, str) or not 1 <= len(backup_id) <= 128:
        raise _error("BACKUP_ID_INVALID", "Backup identity is invalid")
    key = data_key or secrets.token_bytes(32)
    if len(key) != 32:
        raise ValueError("Backup data key must contain 32 bytes")
    password = _passphrase(passphrase)
    salt, wrap_nonce, payload_nonce = secrets.token_bytes(16), secrets.token_bytes(12), secrets.token_bytes(12)
    wrap_key = _derive(password, salt)
    wrapped = AESGCM(wrap_key).encrypt(wrap_nonce, key, MAGIC + backup_id.encode("utf-8"))
    return {
        "format": FORMAT,
        "backup_id": backup_id,
        "kdf": {"name": "scrypt", "n": SCRYPT_N, "r": SCRYPT_R, "p": SCRYPT_P},
        "salt": _b64(salt),
        "wrap_nonce": _b64(wrap_nonce),
        "wrapped_key": _b64(wrapped),
        "payload_nonce": _b64(payload_nonce),
    }, key


def canonical_header(header: dict) -> bytes:
    try:
        result = json.dumps(
            header, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError):
        raise _error("BACKUP_HEADER_INVALID", "Backup header is invalid") from None
    if len(result) > MAX_HEADER:
        raise _error("BACKUP_HEADER_INVALID", "Backup header exceeds its limit")
    return result


def encrypt_file(source: Path, destination: Path, header: dict, data_key: bytes) -> dict:
    source = source.resolve(strict=True)
    size = source.stat().st_size
    if size > MAX_ARCHIVE:
        raise _error("BACKUP_TOO_LARGE", "Backup archive exceeds its size limit", 413)
    header = {**header, "plaintext_size": size}
    encoded = canonical_header(header)
    nonce = _unb64(header.get("payload_nonce"), 12)
    encryptor = Cipher(algorithms.AES(data_key), modes.GCM(nonce)).encryptor()
    encryptor.authenticate_additional_data(MAGIC + encoded)
    private_file(destination)
    digest = hashlib.sha256()
    try:
        with source.open("rb") as incoming, destination.open("wb") as outgoing:
            prefix = MAGIC + struct.pack(">I", len(encoded)) + encoded
            outgoing.write(prefix)
            digest.update(prefix)
            while chunk := incoming.read(CHUNK):
                encrypted = encryptor.update(chunk)
                outgoing.write(encrypted)
                digest.update(encrypted)
            tail = encryptor.finalize() + encryptor.tag
            outgoing.write(tail)
            digest.update(tail)
            outgoing.flush()
            os.fsync(outgoing.fileno())
    except OSError as exc:
        raise _error("BACKUP_STORAGE_ERROR", "Cannot publish backup archive", 500) from exc
    return {
        "header": header,
        "ciphertext_digest": "sha256:" + digest.hexdigest(),
        "byte_size": destination.stat().st_size,
    }


def _read_header(stream: BinaryIO, file_size: int) -> tuple[dict, bytes, int]:
    if file_size < len(MAGIC) + 4 + 16 or stream.read(len(MAGIC)) != MAGIC:
        raise _error("BACKUP_FORMAT_INVALID", "Backup container is invalid")
    length_bytes = stream.read(4)
    if len(length_bytes) != 4:
        raise _error("BACKUP_FORMAT_INVALID", "Backup container is truncated")
    length = struct.unpack(">I", length_bytes)[0]
    if not 1 <= length <= MAX_HEADER:
        raise _error("BACKUP_HEADER_INVALID", "Backup header length is invalid")
    encoded = stream.read(length)
    try:
        header = json.loads(encoded.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError):
        raise _error("BACKUP_HEADER_INVALID", "Backup header is invalid") from None
    required = {
        "format", "backup_id", "kdf", "salt", "wrap_nonce", "wrapped_key",
        "payload_nonce", "plaintext_size",
    }
    if not isinstance(header, dict) or set(header) != required or canonical_header(header) != encoded:
        raise _error("BACKUP_HEADER_INVALID", "Backup header is not canonical")
    if header["format"] != FORMAT or header["kdf"] != {
        "name": "scrypt", "n": SCRYPT_N, "r": SCRYPT_R, "p": SCRYPT_P
    }:
        raise _error("BACKUP_FORMAT_UNSUPPORTED", "Backup format is not supported")
    if (
        not isinstance(header["backup_id"], str)
        or not 1 <= len(header["backup_id"]) <= 128
        or type(header["plaintext_size"]) is not int
        or not 0 <= header["plaintext_size"] <= MAX_ARCHIVE
    ):
        raise _error("BACKUP_HEADER_INVALID", "Backup header fields are invalid")
    payload_size = file_size - len(MAGIC) - 4 - length - 16
    if payload_size != header["plaintext_size"]:
        raise _error("BACKUP_FORMAT_INVALID", "Backup payload size is inconsistent")
    return header, encoded, payload_size


def decrypt_file(source: Path, destination: Path, passphrase: str) -> dict:
    source = source.resolve(strict=True)
    file_size = source.stat().st_size
    if file_size > MAX_ARCHIVE + MAX_HEADER + 64:
        raise _error("BACKUP_TOO_LARGE", "Backup archive exceeds its size limit", 413)
    password = _passphrase(passphrase)
    with source.open("rb") as incoming:
        header, encoded, payload_size = _read_header(incoming, file_size)
        try:
            salt = _unb64(header["salt"], 16)
            wrap_nonce = _unb64(header["wrap_nonce"], 12)
            wrapped = _unb64(header["wrapped_key"], 48)
            nonce = _unb64(header["payload_nonce"], 12)
            data_key = AESGCM(_derive(password, salt)).decrypt(
                wrap_nonce, wrapped, MAGIC + header["backup_id"].encode("utf-8")
            )
        except Exception:
            raise _error("BACKUP_PASSPHRASE_INVALID", "Recovery passphrase or archive is invalid", 403) from None
        incoming.seek(file_size - 16)
        tag = incoming.read(16)
        incoming.seek(len(MAGIC) + 4 + len(encoded))
        decryptor = Cipher(algorithms.AES(data_key), modes.GCM(nonce, tag)).decryptor()
        decryptor.authenticate_additional_data(MAGIC + encoded)
        private_file(destination)
        remaining = payload_size
        try:
            with destination.open("wb") as outgoing:
                while remaining:
                    chunk = incoming.read(min(CHUNK, remaining))
                    if not chunk:
                        raise ValueError
                    remaining -= len(chunk)
                    outgoing.write(decryptor.update(chunk))
                outgoing.write(decryptor.finalize())
                outgoing.flush()
                os.fsync(outgoing.fileno())
        except Exception:
            destination.unlink(missing_ok=True)
            raise _error("BACKUP_INTEGRITY_FAILED", "Backup authentication failed") from None
    return header


def inspect_zip(path: Path) -> dict:
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if not 1 <= len(entries) <= MAX_MEMBERS:
                raise ValueError
            if any(
                entry.is_dir() or entry.compress_type != zipfile.ZIP_STORED
                or entry.file_size > MAX_MEMBER or entry.compress_size != entry.file_size
                for entry in entries
            ):
                raise ValueError
            names = [entry.filename for entry in entries]
            if sum(entry.file_size for entry in entries) > MAX_ARCHIVE:
                raise ValueError
            if any(
                "\\" in name or PurePosixPath(name).is_absolute()
                or str(PurePosixPath(name)) != name
                or any(part in {"", ".", ".."} for part in PurePosixPath(name).parts)
                for name in names
            ):
                raise ValueError
            if len(set(names)) != len(names) or "manifest.json" not in names:
                raise ValueError
            manifest_info = archive.getinfo("manifest.json")
            if manifest_info.file_size > 8 * 1024 * 1024:
                raise ValueError
            manifest = json.loads(archive.read(manifest_info).decode("utf-8"))
            if not isinstance(manifest, dict) or manifest.get("format") != FORMAT:
                raise ValueError
            expected = manifest.get("members")
            if not isinstance(expected, list) or len(expected) != len(entries) - 1:
                raise ValueError
            by_name = {}
            for item in expected:
                if (
                    not isinstance(item, dict) or set(item) != {"name", "size", "digest"}
                    or not isinstance(item["name"], str) or item["name"] == "manifest.json"
                    or type(item["size"]) is not int or not 0 <= item["size"] <= MAX_MEMBER
                    or not isinstance(item["digest"], str)
                    or len(item["digest"]) != 71 or not item["digest"].startswith("sha256:")
                    or any(char not in "0123456789abcdef" for char in item["digest"][7:])
                ):
                    raise ValueError
                by_name[item["name"]] = item
            if set(by_name) != set(names) - {"manifest.json"}:
                raise ValueError
            for name, item in by_name.items():
                info = archive.getinfo(name)
                digest, size = hashlib.sha256(), 0
                with archive.open(info) as stream:
                    while chunk := stream.read(CHUNK):
                        size += len(chunk)
                        digest.update(chunk)
                if size != item["size"] or "sha256:" + digest.hexdigest() != item["digest"]:
                    raise ValueError
            return manifest
    except (OSError, ValueError, KeyError, UnicodeError, json.JSONDecodeError, zipfile.BadZipFile):
        raise _error("BACKUP_INTEGRITY_FAILED", "Backup manifest or members are invalid") from None
