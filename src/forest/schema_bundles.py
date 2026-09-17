"""Content-addressed module storage identities and admission checks."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from forest.errors import ForestError

MAX_STORAGE_BASELINE_BYTES = 1024 * 1024
_IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")
_VERSION = re.compile(r"\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?\Z")
_REPOSITORY_CONTRACT = re.compile(r"[a-z][a-z0-9_.-]*/[1-9]\d*\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_FORBIDDEN_SQL = re.compile(
    r"\b(?:"
    r"CREATE\s+(?:DATABASE|ROLE|USER|EXTENSION|SCHEMA|EVENT\s+TRIGGER)|"
    r"ALTER\s+(?:SYSTEM|DATABASE|ROLE|USER)|"
    r"DROP\s+(?:DATABASE|ROLE|USER|SCHEMA)|"
    r"GRANT|REVOKE|SET\b|RESET\b|COPY\b|SECURITY\s+DEFINER|"
    r"INSERT\b|UPDATE\b|DELETE\b|MERGE\b|TRUNCATE\b|SELECT\b|"
    r"DO\b|CALL\b|BEGIN\b|COMMIT\b|ROLLBACK\b|SAVEPOINT\b|"
    r"CREATE\s+(?:FUNCTION|PROCEDURE|TRIGGER|VIEW|MATERIALIZED\s+VIEW|RULE|POLICY|"
    r"FOREIGN\s+TABLE|SERVER|USER\s+MAPPING|PUBLICATION|SUBSCRIPTION)|"
    r"DROP\b|ALTER\b|IF\s+NOT\s+EXISTS|TABLESPACE\b|INHERITS\b|PARTITION\b"
    r")\b",
    re.IGNORECASE,
)
_ALLOWED_STATEMENT = re.compile(
    r"CREATE\s+(?:(?:UNIQUE\s+)?INDEX|TABLE|TYPE|DOMAIN|SEQUENCE)\b|COMMENT\s+ON\b",
    re.IGNORECASE,
)
_QUALIFIED_IDENTIFIER = re.compile(r"[a-zA-Z_][a-zA-Z0-9_$]*\s*\.")


def _invalid(message: str) -> None:
    raise ForestError("INVALID_STORAGE_BUNDLE", message, 422)


def module_schema_namespace(schema_id: str) -> str:
    """Return the host-owned namespace; packages never choose a SQL identifier."""

    if not isinstance(schema_id, str) or not _IDENTIFIER.fullmatch(schema_id):
        _invalid("Storage schema identity is invalid")
    readable = re.sub(r"[^a-z0-9]+", "_", schema_id.lower()).strip("_")[:30]
    digest = hashlib.sha256(schema_id.encode("utf-8")).hexdigest()[:16]
    return f"forest_m_{readable}_{digest}"


@dataclass(frozen=True)
class StorageBundleSpec:
    component_id: str
    schema_id: str
    schema_version: str
    baseline_sha256: str
    repository_contract: str

    def __post_init__(self) -> None:
        if not isinstance(self.component_id, str) or not _IDENTIFIER.fullmatch(
            self.component_id
        ):
            _invalid("Storage component identity is invalid")
        module_schema_namespace(self.schema_id)
        if not isinstance(self.schema_version, str) or not _VERSION.fullmatch(
            self.schema_version
        ):
            _invalid("Storage schema version is invalid")
        if not isinstance(self.baseline_sha256, str) or not _DIGEST.fullmatch(
            self.baseline_sha256
        ):
            _invalid("Storage baseline digest is invalid")
        if not isinstance(self.repository_contract, str) or not _REPOSITORY_CONTRACT.fullmatch(
            self.repository_contract
        ):
            _invalid("Storage repository contract is invalid")

    @property
    def namespace(self) -> str:
        return module_schema_namespace(self.schema_id)

    def as_fact(self) -> dict:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "namespace": self.namespace,
            "baseline_sha256": self.baseline_sha256,
            "scope": "bot",
            "repository_contract": self.repository_contract,
        }


def validate_storage_baseline(source: bytes, expected_sha256: str) -> str:
    if not isinstance(source, bytes) or not source or len(source) > MAX_STORAGE_BASELINE_BYTES:
        _invalid("Storage baseline exceeds its byte limit")
    actual = hashlib.sha256(source).hexdigest()
    if not isinstance(expected_sha256, str) or not _DIGEST.fullmatch(expected_sha256):
        _invalid("Storage baseline digest is invalid")
    if actual != expected_sha256:
        _invalid("Storage baseline digest does not match its bytes")
    try:
        sql = source.decode("utf-8")
    except UnicodeError as exc:
        raise ForestError(
            "INVALID_STORAGE_BUNDLE", "Storage baseline must be UTF-8 SQL", 422
        ) from exc
    code = _lexical_sql(sql)
    statements = [statement.strip() for statement in code.split(";") if statement.strip()]
    if (
        "\x00" in sql
        or re.search(r"(?m)^\s*\\", sql)
        or not statements
        or any(not _ALLOWED_STATEMENT.match(statement) for statement in statements)
        or _FORBIDDEN_SQL.search(code)
        or _QUALIFIED_IDENTIFIER.search(code)
    ):
        _invalid("Storage baseline contains forbidden SQL")
    return sql


def _lexical_sql(sql: str) -> str:
    """Remove comments and string bodies before structural admission checks."""

    output = []
    index = 0
    block_depth = 0
    state = "code"
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if state == "line-comment":
            if char in "\r\n":
                state = "code"
                output.append(char)
            else:
                output.append(" ")
            index += 1
            continue
        if state == "block-comment":
            if char == "/" and following == "*":
                block_depth += 1
                output.extend((" ", " "))
                index += 2
            elif char == "*" and following == "/":
                block_depth -= 1
                output.extend((" ", " "))
                index += 2
                if block_depth == 0:
                    state = "code"
            else:
                output.append("\n" if char == "\n" else " ")
                index += 1
            continue
        if state == "string":
            output.append(" ")
            if char == "'":
                if following == "'":
                    output.append(" ")
                    index += 2
                    continue
                state = "code"
            index += 1
            continue
        if char == "-" and following == "-":
            state = "line-comment"
            output.extend((" ", " "))
            index += 2
        elif char == "/" and following == "*":
            state = "block-comment"
            block_depth = 1
            output.extend((" ", " "))
            index += 2
        elif char == "'":
            state = "string"
            output.append(" ")
            index += 1
        elif char in {'"', '$'}:
            _invalid("Storage baseline uses unsupported SQL quoting")
        else:
            output.append(char)
            index += 1
    if state in {"block-comment", "string"}:
        _invalid("Storage baseline contains unterminated SQL input")
    return "".join(output)
