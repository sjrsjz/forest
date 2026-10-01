"""Run isolated PostgreSQL tests inside the Compose test network.

Mount the Forest secrets volume read-only and use the test-stage image. The
database URL is derived in-process and never passed in the Docker command line.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import quote

import pytest


def main() -> int:
    password = Path("/run/forest-secrets/postgres_password").read_text(
        encoding="ascii"
    ).strip()
    os.environ["FOREST_TEST_DSN"] = (
        "postgresql://forest:"
        + quote(password, safe="")
        + "@postgres/forest_deployment"
    )
    return pytest.main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
