from __future__ import annotations

import base64

from forest.clean_supervisor_host import CleanSupervisorSettings


def test_supervisor_settings_builds_hidden_dsn_from_protected_files(tmp_path, monkeypatch):
    master = tmp_path / "master"
    token = tmp_path / "token"
    password = tmp_path / "postgres"
    master.write_bytes(base64.b64encode(bytes(range(32))))
    token.write_text("deployment-management-token", encoding="utf-8")
    password.write_text("database-password", encoding="utf-8")
    monkeypatch.delenv("FOREST_DEPLOYMENT_DSN", raising=False)
    monkeypatch.setenv("FOREST_DEPLOYMENT_MASTER_KEY_FILE", str(master))
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
