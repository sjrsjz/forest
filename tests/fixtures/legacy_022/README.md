These four gzip files contain exact source bytes from Forest commit `394c7cb`:
`src/forest/storage/messages.py`, `memory_sources.py`, `delivery.py`, and `payloads.py`.

`tests/legacy_022.py` verifies their SHA-256 before loading them. They seed actual
001–022 PostgreSQL tables using the released behavior; the current repositories
perform migration, recovery and subsequent work. This avoids adding a legacy
plaintext fallback to application code just to construct upgrade fixtures.

`FrozenPayloadStore` also replaces the payload dependency imported by the frozen
`MemorySourceStore`. Old-schema seeds therefore cannot call newer SQL functions;
post-upgrade verification continues to use the current `PayloadStore`.

Do not update these snapshots when current application code changes. To verify
or reproduce one, read the named Git blob and gzip its bytes with `mtime=0`.
