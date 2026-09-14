"""Encrypted backup containers reject wrong credentials and byte tampering."""

import hashlib
import json
import zipfile

import pytest

from forest.backup_artifact import decrypt_file, encrypt_file, inspect_zip, prepare_header
from forest.errors import ForestError


def archive(path, content=b"database-copy-bytes", *, member_digest=None):
    member = {
        "name": "deployment/tables/000-example.bin",
        "size": len(content),
        "digest": member_digest or "sha256:" + hashlib.sha256(content).hexdigest(),
    }
    manifest = {
        "format": "forest.backup.v1",
        "backup_id": "backup-test",
        "members": [member],
        "databases": [],
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as output:
        output.writestr(member["name"], content)
        output.writestr(
            "manifest.json",
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(),
        )
    return manifest


def test_backup_container_round_trip_and_manifest_verification(tmp_path):
    plain, encrypted, restored = (
        tmp_path / "plain.zip", tmp_path / "backup.forest", tmp_path / "restored.zip"
    )
    expected = archive(plain)
    header, key = prepare_header("backup-test", "correct horse battery staple")
    sealed = encrypt_file(plain, encrypted, header, key)
    opened = decrypt_file(encrypted, restored, "correct horse battery staple")
    assert opened["backup_id"] == "backup-test"
    assert opened["plaintext_size"] == plain.stat().st_size
    assert sealed["byte_size"] == encrypted.stat().st_size
    assert sealed["ciphertext_digest"].startswith("sha256:")
    assert inspect_zip(restored) == expected


def test_wrong_passphrase_and_ciphertext_tamper_are_indistinguishable(tmp_path):
    plain, encrypted = tmp_path / "plain.zip", tmp_path / "backup.forest"
    archive(plain)
    header, key = prepare_header("backup-test", "correct horse battery staple")
    encrypt_file(plain, encrypted, header, key)
    with pytest.raises(ForestError) as wrong:
        decrypt_file(encrypted, tmp_path / "wrong.zip", "another recovery passphrase")
    assert wrong.value.code == "BACKUP_PASSPHRASE_INVALID"

    data = bytearray(encrypted.read_bytes())
    data[-17] ^= 1
    encrypted.write_bytes(data)
    with pytest.raises(ForestError) as damaged:
        decrypt_file(encrypted, tmp_path / "damaged.zip", "correct horse battery staple")
    assert damaged.value.code == "BACKUP_INTEGRITY_FAILED"


def test_manifest_member_tamper_and_unsupported_compression_are_rejected(tmp_path):
    bad_digest, compressed = tmp_path / "digest.zip", tmp_path / "compressed.zip"
    archive(bad_digest, member_digest="sha256:" + "0" * 64)
    with pytest.raises(ForestError) as digest:
        inspect_zip(bad_digest)
    assert digest.value.code == "BACKUP_INTEGRITY_FAILED"

    content = b"content"
    member = {
        "name": "deployment/tables/000-example.bin",
        "size": len(content),
        "digest": "sha256:" + hashlib.sha256(content).hexdigest(),
    }
    with zipfile.ZipFile(compressed, "w", compression=zipfile.ZIP_DEFLATED) as output:
        output.writestr(member["name"], content)
        output.writestr(
            "manifest.json",
            json.dumps({
                "format": "forest.backup.v1", "backup_id": "backup-test",
                "members": [member], "databases": [],
            }).encode(),
        )
    with pytest.raises(ForestError) as compression:
        inspect_zip(compressed)
    assert compression.value.code == "BACKUP_INTEGRITY_FAILED"

    traversal = tmp_path / "traversal.zip"
    content = b"private"
    member["name"] = "../outside.bin"
    member["size"] = len(content)
    member["digest"] = "sha256:" + hashlib.sha256(content).hexdigest()
    with zipfile.ZipFile(traversal, "w", compression=zipfile.ZIP_STORED) as output:
        output.writestr(member["name"], content)
        output.writestr("manifest.json", json.dumps({
            "format": "forest.backup.v1", "backup_id": "backup-test",
            "members": [member], "databases": [],
        }).encode())
    with pytest.raises(ForestError) as unsafe:
        inspect_zip(traversal)
    assert unsafe.value.code == "BACKUP_INTEGRITY_FAILED"
