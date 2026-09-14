"""Process-level exclusion for one Forest deployment data directory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO

from forest.errors import ForestError
from forest.security import private_directory, private_file


class DeploymentLock:
    """Hold one non-blocking OS lock until explicitly closed.

    The byte-range lock on Windows and flock on POSIX are released by the OS if
    the process exits.  The file itself is durable and contains no authority.
    """

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = private_directory(data_dir)
        self.path = self.data_dir / "deployment.lock"
        self._stream: BinaryIO | None = None

    def acquire(self) -> "DeploymentLock":
        if self._stream is not None:
            return self
        private_file(self.path)
        stream = self.path.open("r+b", buffering=0)
        try:
            if os.fstat(stream.fileno()).st_size == 0:
                stream.write(b"\0")
                os.fsync(stream.fileno())
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            stream.close()
            raise ForestError(
                "DEPLOYMENT_ALREADY_RUNNING",
                "Another Forest process is using this deployment data directory",
                status=409,
            ) from None
        self._stream = stream
        return self

    def close(self) -> None:
        stream, self._stream = self._stream, None
        if stream is None:
            return
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()

    def __enter__(self) -> "DeploymentLock":
        return self.acquire()

    def __exit__(self, *_: object) -> None:
        self.close()
