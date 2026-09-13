"""Run the exact published 041 writer in an isolated interpreter.

The frozen package includes its SQL, data handlers, and inline imports. Tests
send seed parameters through stdin; the fixture needs neither Git nor the
current source package at runtime. Seed programs print one JSON result.
"""

import asyncio
import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath
from uuid import uuid4

COMMIT = "f86ba34902b19b6c073e9f24f5f48684e696f526"
ARCHIVE_SHA256 = "3e3096a4f98a20ef7d9da3dcd8b9c3ff3efabe5461d05d87d47ca991c27728ac"


def extract_legacy_041(directory):
    """Create a new checked fixture directory under the test's temporary root."""
    fixture = Path(__file__).parent / "fixtures" / "legacy_041"
    data = (fixture / "forest.zip").read_bytes()
    manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(data).hexdigest() == ARCHIVE_SHA256
    assert manifest["commit"] == COMMIT and manifest["archive_sha256"] == ARCHIVE_SHA256
    parent = Path(directory).resolve(strict=True)
    destination = parent / ("legacy-041-" + uuid4().hex)
    destination.mkdir()
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        assert len(archive.namelist()) == len(set(archive.namelist())) == 171
        assert set(archive.namelist()) == set(manifest["files"])
        for name in archive.namelist():
            parts = PurePosixPath(name).parts
            assert parts[0] == "forest" and ".." not in parts and "\\" not in name
            content = archive.read(name)
            assert hashlib.sha256(content).hexdigest() == manifest["files"][name]
            path = destination.joinpath(*parts)
            path.parent.mkdir(parents=True, exist_ok=True)
            assert path.resolve().is_relative_to(destination)
            path.write_bytes(content)
    return destination


async def run_legacy_041(directory, source, parameters, *, timeout=120):
    """Run a trusted test seed program with PARAMETERS and return its JSON stdout.

    The seed owns old storage setup and cleanup, and can import the frozen
    forest normally. No modules from the current test process are reused.
    asyncio.to_thread also works with Windows' selector event loop.
    """
    package_root = extract_legacy_041(directory)
    bootstrap = package_root / "seed.py"
    bootstrap.write_text(
        "import json, sys\n"
        f"sys.path.insert(0, {str(package_root)!r})\n"
        "PARAMETERS = json.load(sys.stdin)\n"
        + source,
        encoding="utf-8", newline="\n",
    )
    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-I", str(bootstrap)],
        input=json.dumps(parameters).encode(), capture_output=True,
        timeout=timeout, cwd=package_root, check=False,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    return json.loads(result.stdout.decode("utf-8"))
