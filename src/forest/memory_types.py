"""Small immutable source references shared by storage and pure memory reducers."""

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class SourceRef:
    """A content dependency; '*' depends on the whole source, including all parts.

    Bot scope comes from the trusted repository/projection, never from a caller's
    reference. Revisions are assigned by the authority, not worker completion.
    Suppression of a source or part applies across all of its revisions.
    """

    source_unit_id: str
    source_revision: int = 1
    part_id: str = "*"

    def __post_init__(self):
        for name in ("source_unit_id", "part_id"):
            value = getattr(self, name)
            if (not isinstance(value, str) or not value or len(value.encode()) > 256
                    or any(ord(char) < 32 for char in value)):
                raise ValueError(f"Invalid {name}")
        if type(self.source_revision) is not int or not 1 <= self.source_revision < 2**63:
            raise ValueError("source_revision must be a positive int64")
