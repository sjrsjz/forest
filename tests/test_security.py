from __future__ import annotations

import multiprocessing
import os
import stat
from pathlib import Path

import pytest

from forest.errors import ForestError
from forest.security import (
    SecretVault,
    digest_token,
    hash_password,
    private_directory,
    random_token,
    verify_password,
)


def _vault_writer(directory: str, barrier, queue) -> None:
    barrier.wait(timeout=15)
    vault = SecretVault(directory)
    queue.put(vault.encrypt("simulated credential"))


def test_password_hash_is_salted_and_checks_unicode() -> None:
    password = "synthetic-password-中文"
    first = hash_password(password)
    second = hash_password(password)
    assert first != second
    assert password not in first
    assert verify_password(password, first)
    assert verify_password(password, second)
    assert not verify_password("another-password-中文", first)


@pytest.mark.parametrize(
    "password",
    ["", "short", "x" * 1025, "图" * 342, None],
    ids=["empty", "short", "large-ascii", "large-unicode", "wrong-type"],
)
def test_password_size_is_bounded(password) -> None:
    with pytest.raises(ForestError, match="Password"):
        hash_password(password)
    assert not verify_password(password, "malformed")


@pytest.mark.parametrize(
    "encoded",
    [
        "",
        "scrypt$999999999999$8$1$AA==$AA==",
        "scrypt$32768$8$1$!!$!!",
        "scrypt$32768$8$1$AA==$AA==",
        "x" * 10000,
        None,
    ],
    ids=["empty", "unbounded-cost", "bad-base64", "wrong-size", "large", "wrong-type"],
)
def test_verify_rejects_unbounded_or_malformed_hashes(encoded) -> None:
    assert not verify_password("synthetic-password", encoded)


def test_token_randomness_and_digest() -> None:
    tokens = {random_token() for _ in range(100)}
    assert len(tokens) == 100
    assert all(len(token) >= 43 for token in tokens)
    assert len(digest_token(next(iter(tokens)))) == 64


def test_vault_survives_restart_and_detects_ciphertext_tampering(tmp_path: Path) -> None:
    directory = tmp_path / "private"
    first = SecretVault(directory)
    ciphertext = first.encrypt("synthetic-api-key")
    assert "synthetic-api-key" not in ciphertext
    assert SecretVault(directory).decrypt(ciphertext) == "synthetic-api-key"
    with pytest.raises(ForestError) as error:
        first.decrypt(ciphertext[:15] + ("A" if ciphertext[15] != "A" else "B") + ciphertext[16:])
    assert error.value.code == "SECRET_DECRYPTION_FAILED"


def test_vault_concurrent_creation_has_one_key(tmp_path: Path) -> None:
    directory = str(tmp_path / "private")
    context = multiprocessing.get_context("spawn")
    barrier, queue = context.Barrier(2), context.Queue()
    workers = [
        context.Process(target=_vault_writer, args=(directory, barrier, queue)) for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    try:
        ciphertexts = [queue.get(timeout=25) for _ in workers]
        for worker in workers:
            worker.join(timeout=15)
            assert worker.exitcode == 0
        vault = SecretVault(directory)
        assert all(vault.decrypt(value) == "simulated credential" for value in ciphertexts)
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        queue.close()


def test_missing_master_key_never_rekeys_a_deployment(tmp_path: Path) -> None:
    SecretVault(tmp_path)
    (tmp_path / "bootstrap.sqlite3").write_bytes(b"existing state")
    (tmp_path / "master.key").unlink()
    with pytest.raises(ForestError) as error:
        SecretVault(tmp_path)
    assert error.value.code == "MASTER_KEY_MISSING"
    assert not (tmp_path / "master.key").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership/mode contract")
def test_posix_storage_modes(tmp_path: Path) -> None:
    directory = tmp_path / "private"
    SecretVault(directory)
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE((directory / "master.key").stat().st_mode) == 0o600
    assert stat.S_IMODE((directory / "master.lock").stat().st_mode) == 0o600


@pytest.mark.skipif(os.name == "nt", reason="POSIX ownership contract")
def test_posix_refuses_other_owner_before_chmod(tmp_path: Path, monkeypatch) -> None:
    owner = tmp_path.stat().st_uid
    original_mode = stat.S_IMODE(tmp_path.stat().st_mode)
    monkeypatch.setattr("forest.security.os.geteuid", lambda: owner + 1)
    with pytest.raises(ForestError) as error:
        private_directory(tmp_path)
    assert error.value.code == "DATA_OWNER_MISMATCH"
    assert stat.S_IMODE(tmp_path.stat().st_mode) == original_mode


@pytest.mark.skipif(
    os.name == "nt", reason="Portable symlink creation unavailable on all Windows hosts"
)
def test_refuses_linked_storage(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "alias"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(ForestError) as error:
        private_directory(link)
    assert error.value.code == "UNSAFE_DATA_PATH"


@pytest.mark.skipif(os.name != "nt", reason="Windows DACL contract")
def test_windows_acl_is_protected_and_only_service_user_and_system(tmp_path: Path) -> None:
    import json
    import subprocess

    directory = tmp_path / "private"
    SecretVault(directory)
    # A fixed script receives the path through the environment, never shell
    # interpolation. This inspects the actual OS ACL instead of mocking chmod.
    script = """
    $items = @($env:FOREST_TEST_ACL_PATH, (Join-Path $env:FOREST_TEST_ACL_PATH 'master.key'))
    $result = foreach ($item in $items) {
      $acl = Get-Acl -LiteralPath $item
      @{protected=$acl.AreAccessRulesProtected; identities=@($acl.Access | ForEach-Object {$_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value}); inherited=@($acl.Access | ForEach-Object {$_.IsInherited})}
    }
    @{current=[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value; entries=@($result)} | ConvertTo-Json -Depth 5 -Compress
    """
    environment = dict(os.environ, FOREST_TEST_ACL_PATH=str(directory))
    output = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    result = json.loads(output.stdout)
    for entry in result["entries"]:
        assert entry["protected"] is True
        assert set(entry["identities"]) == {result["current"], "S-1-5-18"}
        assert not any(entry["inherited"])
