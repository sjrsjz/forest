"""Process-local credentials needed to read the deployment authority."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

from forest.deployment_master_keyring import DeploymentMasterKeyring
from forest.errors import ForestError


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ForestError("SUPERVISOR_CONFIGURATION_REQUIRED", f"{name} is required", 422)
    return value


def bounded_environment_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = -1
    if not minimum <= value <= maximum:
        raise ForestError(
            "SUPERVISOR_CONFIGURATION_INVALID", f"{name} is outside its bound", 422
        )
    return value


def deployment_database_dsn() -> str:
    configured = os.environ.get("FOREST_DEPLOYMENT_DSN")
    if configured:
        return configured
    password_path = Path(required_environment("FOREST_DEPLOYMENT_DATABASE_PASSWORD_FILE"))
    try:
        password = password_path.read_text(encoding="utf-8").strip()
    except OSError:
        raise ForestError(
            "SUPERVISOR_DATABASE_CREDENTIAL_UNAVAILABLE",
            "Deployment database credential is unavailable",
            503,
        ) from None
    host = os.environ.get("FOREST_DEPLOYMENT_DATABASE_HOST", "postgres")
    port = bounded_environment_int("FOREST_DEPLOYMENT_DATABASE_PORT", 5432, 1, 65535)
    database = os.environ.get("FOREST_DEPLOYMENT_DATABASE_NAME", "forest_deployment")
    user = os.environ.get("FOREST_DEPLOYMENT_DATABASE_USER", "forest")
    if not password or not host or not database or not user:
        raise ForestError(
            "SUPERVISOR_CONFIGURATION_INVALID", "Deployment database identity is invalid", 422
        )
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{quote(database, safe='')}"
    )


@dataclass(frozen=True)
class DeploymentAccessSettings:
    deployment_id: str
    dsn: str = field(repr=False)
    master_keyring: DeploymentMasterKeyring = field(repr=False)

    @classmethod
    def from_environment(cls) -> DeploymentAccessSettings:
        keyring = DeploymentMasterKeyring.read(
            Path(required_environment("FOREST_DEPLOYMENT_MASTER_KEYRING_FILE"))
        )
        return cls(
            deployment_id=os.environ.get("FOREST_DEPLOYMENT_ID", "forest/default"),
            dsn=deployment_database_dsn(),
            master_keyring=keyring,
        )
