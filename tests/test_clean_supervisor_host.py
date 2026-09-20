from __future__ import annotations

import base64
import json

import pytest

from forest.clean_supervisor_host import CleanSupervisorSettings
from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.errors import ForestError


def test_supervisor_settings_builds_hidden_dsn_from_protected_files(tmp_path, monkeypatch):
    master = tmp_path / "keyring"
    token = tmp_path / "token"
    password = tmp_path / "postgres"
    master.write_text(
        json.dumps(
            {
                "protocol": "forest.deployment-master-keyring/1",
                "active_key_id": "master/test/2",
                "keys": {
                    "master/test/1": base64.b64encode(b"a" * 32).decode(),
                    "master/test/2": base64.b64encode(bytes(range(32))).decode(),
                },
            },
            separators=(",", ":"),
        ),
        encoding="ascii",
    )
    token.write_text("deployment-management-token", encoding="utf-8")
    password.write_text("database-password", encoding="utf-8")
    monkeypatch.delenv("FOREST_DEPLOYMENT_DSN", raising=False)
    monkeypatch.setenv("FOREST_DEPLOYMENT_MASTER_KEYRING_FILE", str(master))
    monkeypatch.setenv("FOREST_DEPLOYMENT_MANAGEMENT_TOKEN_FILE", str(token))
    monkeypatch.setenv("FOREST_DEPLOYMENT_DATABASE_PASSWORD_FILE", str(password))
    monkeypatch.setenv("FOREST_DEPLOYMENT_DATABASE_HOST", "postgres")
    settings = CleanSupervisorSettings.from_environment()
    assert settings.dsn == (
        "postgresql://forest:database-password@postgres:5432/forest_deployment"
    )
    rendered = repr(settings)
    assert "database-password" not in rendered
    assert "deployment-management-token" not in rendered
    assert repr(bytes(range(32))) not in rendered
    assert settings.master_keyring.active_key_id == "master/test/2"
    assert settings.master_keyring.resolve("master/test/1") == b"a" * 32


@pytest.mark.parametrize(
    "content",
    (
        b"{}",
        b'{"protocol":"forest.deployment-master-keyring/1","active_key_id":"k/1",'
        b'"keys":{"k/1":"invalid"}}',
        b'{"protocol":"forest.deployment-master-keyring/1","active_key_id":"k/2",'
        b'"keys":{"k/1":"YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE="}}',
        b'{"protocol":"forest.deployment-master-keyring/1","protocol":"wrong",'
        b'"active_key_id":"k/1","keys":{}}',
    ),
)
def test_deployment_master_keyring_rejects_malformed_or_incomplete_documents(content):
    with pytest.raises(ForestError) as caught:
        DeploymentMasterKeyring.from_bytes(content)
    assert caught.value.code == "SUPERVISOR_MASTER_KEYRING_INVALID"
