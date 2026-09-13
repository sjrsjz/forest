# Frozen pre-control-payload writer

`control.py.gz` is the exact `src/forest/storage/control.py` from commit
`52207c0`, before the 039–041 control payload family. Its decompressed bytes are
19,659 bytes, SHA-256
`6423724f2e97330b339886e9474d9f67e8146b04cac7f35f32a96eca6d16e2ae`.
Gzip mtime is zero. The test verifies this checksum before executing the old
writer against the existing checksum-frozen 038 schema. No Git checkout,
network, or adjacent worktree is needed at test execution time.

The frozen writer uses the existing canonical digest and old `_event` call
signature. Its actual signed plan/apply path supplies the old records; current
code is used only after the migration under test.

`agent_config.py.gz` freezes the same commit's actual Agent configuration writer:
9,654 decompressed bytes, SHA-256
`338a63a503d00bf594ba93418948835e79e2d80eda027b1c3d9e440df9e2fc27`.
`tests/legacy_control.py` binds that frozen module's `BotControl` to the frozen
control writer. Its plan/apply methods use tables already present in 022/028,
so historical Agent seeds do not accidentally run today's 039+ writer against
the old schema. Only the pre-upgrade seed uses this path; current reads and
execution after migration use the live implementations.
