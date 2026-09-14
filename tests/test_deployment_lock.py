from __future__ import annotations

from pathlib import Path

import pytest

from forest.deployment_lock import DeploymentLock
from forest.errors import ForestError


def test_deployment_lock_excludes_second_owner_and_releases(tmp_path: Path) -> None:
    first = DeploymentLock(tmp_path).acquire()
    try:
        with pytest.raises(ForestError) as busy:
            DeploymentLock(tmp_path).acquire()
        assert busy.value.code == "DEPLOYMENT_ALREADY_RUNNING"
    finally:
        first.close()
    with DeploymentLock(tmp_path):
        assert (tmp_path / "deployment.lock").is_file()
