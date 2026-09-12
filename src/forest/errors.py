"""Public errors contain safe descriptions, never credentials or SQL details."""

from typing import Any


class ForestError(Exception):
    def __init__(self, code: str, message: str, status: int = 409, details: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details is not None:
            result["details"] = self.details
        return result
