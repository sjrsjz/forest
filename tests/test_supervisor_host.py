from __future__ import annotations

import asyncio
import base64
import json
import signal

import pytest
import uvicorn

from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.deployment_web_bootstrap import DeploymentWebBootstrap
from forest.errors import ForestError
from forest.supervisor_host import SupervisorSettings, _EmbeddedManagementServer, _host_id


async def test_embedded_management_server_does_not_claim_process_signals(monkeypatch):
    async def app(scope, receive, send):
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

    server = _EmbeddedManagementServer(
        uvicorn.Config(app, host="127.0.0.1", port=0, lifespan="off", log_level="critical")
    )

    def reject_signal_claim(_signal, _handler):
        raise AssertionError("Embedded Uvicorn tried to replace a process signal handler")

    with monkeypatch.context() as scoped_patch:
        scoped_patch.setattr(signal, "signal", reject_signal_claim)
        task = asyncio.create_task(server.serve())
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    if task.done():
                        await task
                        raise AssertionError("Embedded management server stopped before startup")
                    await asyncio.sleep(0.01)
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, timeout=5)


def test_supervisor_settings_builds_hidden_dsn_from_protected_files(tmp_path, monkeypatch):
    master = tmp_path / "keyring"
    web_bootstrap = tmp_path / "web-bootstrap"
    password = tmp_path / "postgres"
    host_password = tmp_path / "host-postgres"
    host_id = tmp_path / "host-id"
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
    web_bootstrap.write_text(
        json.dumps(
            {
                "protocol": "forest.deployment-web-bootstrap/2",
                "setup_id": "setup/0123456789abcdef",
                "pairing_code": "p" * 48,
                "issued_at": "2029-12-31T23:00:00+00:00",
                "expires_at": "2030-01-01T00:00:00+00:00",
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    password.write_text("database-password", encoding="utf-8")
    host_password.write_text("host-database-password", encoding="utf-8")
    host_id.write_text("host/0123456789abcdef0123456789abcdef", encoding="ascii")
    monkeypatch.delenv("FOREST_HOST_ID", raising=False)
    monkeypatch.setenv("FOREST_HOST_ID_FILE", str(host_id))
    monkeypatch.delenv("FOREST_DEPLOYMENT_DSN", raising=False)
    monkeypatch.setenv("FOREST_DEPLOYMENT_MASTER_KEYRING_FILE", str(master))
    monkeypatch.setenv("FOREST_DEPLOYMENT_WEB_BOOTSTRAP_FILE", str(web_bootstrap))
    monkeypatch.setenv("FOREST_DEPLOYMENT_MANAGEMENT_ORIGIN", "http://127.0.0.1:8080")
    monkeypatch.setenv("FOREST_DEPLOYMENT_DATABASE_PASSWORD_FILE", str(password))
    monkeypatch.setenv("FOREST_DEPLOYMENT_DATABASE_HOST", "postgres")
    monkeypatch.delenv("FOREST_HOST_DSN", raising=False)
    monkeypatch.setenv("FOREST_HOST_DATABASE_PASSWORD_FILE", str(host_password))
    monkeypatch.setenv("FOREST_HOST_DATABASE_HOST", "postgres")
    settings = SupervisorSettings.from_environment()
    assert settings.dsn == (
        "postgresql://forest:database-password@postgres:5432/forest_deployment"
    )
    assert settings.host_dsn == (
        "postgresql://forest_host:host-database-password@postgres:5432/forest_host"
    )
    rendered = repr(settings)
    assert "database-password" not in rendered
    assert "host-database-password" not in rendered
    assert "p" * 48 not in rendered
    assert repr(bytes(range(32))) not in rendered
    assert settings.master_keyring.active_key_id == "master/test/2"
    assert settings.master_keyring.resolve("master/test/1") == b"a" * 32
    assert settings.web_bootstrap.setup_id == "setup/0123456789abcdef"
    assert settings.web_bootstrap.pairing_code == "p" * 48
    assert settings.host_id == "host/0123456789abcdef0123456789abcdef"


def test_supervisor_host_identity_has_one_explicit_stable_source(tmp_path, monkeypatch):
    monkeypatch.delenv("FOREST_HOST_ID", raising=False)
    monkeypatch.delenv("FOREST_HOST_ID_FILE", raising=False)
    with pytest.raises(ForestError) as missing:
        _host_id()
    assert missing.value.code == "SUPERVISOR_HOST_ID_REQUIRED"

    identity = tmp_path / "host-id"
    identity.write_text("host/0123456789abcdef0123456789abcdef", encoding="ascii")
    monkeypatch.setenv("FOREST_HOST_ID_FILE", str(identity))
    monkeypatch.setenv("FOREST_HOST_ID", "host/abcdef0123456789abcdef0123456789")
    with pytest.raises(ForestError) as conflicting:
        _host_id()
    assert conflicting.value.code == "SUPERVISOR_HOST_ID_REQUIRED"

    monkeypatch.delenv("FOREST_HOST_ID", raising=False)
    identity.write_text("deployment-host/deployment-id", encoding="ascii")
    with pytest.raises(ForestError) as invalid:
        _host_id()
    assert invalid.value.code == "SUPERVISOR_HOST_ID_INVALID"


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


@pytest.mark.parametrize(
    "content",
    (
        b'{"protocol":"forest.deployment-web-bootstrap/1","setup_id":"setup/0123456789abcdef",'
        b'"pairing_code":"pppppppppppppppppppppppppppppppppppppppppppppppp",'
        b'"issued_at":"2030-01-01T00:00:00+00:00","expires_at":"2030-01-01T01:00:00+00:00"}',
        b'{"protocol":"forest.deployment-web-bootstrap/2","setup_id":"setup/0123456789abcdef",'
        b'"pairing_code":"short","issued_at":"2030-01-01T00:00:00+00:00",'
        b'"expires_at":"2030-01-01T01:00:00+00:00"}',
        b'{"protocol":"forest.deployment-web-bootstrap/2","setup_id":"setup/0123456789abcdef",'
        b'"pairing_code":"pppppppppppppppppppppppppppppppppppppppppppppppp",'
        b'"issued_at":"2030-01-01T00:00:00+00:00","expires_at":"2030-01-03T00:00:00+00:00"}',
    ),
)
def test_deployment_web_pairing_credential_rejects_old_or_unbounded_documents(content):
    with pytest.raises(ForestError) as caught:
        DeploymentWebBootstrap.from_bytes(content)
    assert caught.value.code == "SUPERVISOR_WEB_BOOTSTRAP_INVALID"
