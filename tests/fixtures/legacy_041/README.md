# Published 041 writer

`forest.zip` contains all 171 tracked files under `src/forest` at commit
`f86ba34902b19b6c073e9f24f5f48684e696f526`, copied byte for byte from Git blobs.
The archive has deterministic paths, timestamps and file permissions.

Archive SHA-256: `3e3096a4f98a20ef7d9da3dcd8b9c3ff3efabe5461d05d87d47ca991c27728ac`.
`manifest.json` records each original file's SHA-256. The loader pins the archive
digest in code and verifies every member before extracting it.

`tests/legacy_041.py` starts a separate interpreter with this package first on
its import path. SQL migrations, data handlers and inline imports therefore all
come from the published writer. Parameters are passed through stdin, and seed
programs print a single JSON result. Runtime tests require no Git checkout or
access to sibling worktrees.
