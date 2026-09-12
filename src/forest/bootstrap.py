"""Crash-safe local setup authority; never permits changing a frozen target.

SQLite is used only for installation before the deployment PostgreSQL exists.
The host must first validate the connection, then freeze, then execute the same
plan idempotently in PostgreSQL, and finally mark it complete here. A timeout is
reconciled against that original target. No operation holds this local database
transaction while awaiting a network call.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from psycopg.conninfo import conninfo_to_dict, make_conninfo

from forest.errors import ForestError
from forest.security import (
    SecretVault,
    _decode_password_hash,
    digest_token,
    private_directory,
    private_file,
    random_token,
)


def normalize_dsn(dsn: str) -> tuple[str, dict[str, str]]:
    """Canonicalize one explicit PostgreSQL locator without leaking credentials.

    A locator is not proof of physical database identity (DNS aliases exist).
    Local setup pins one locator and target setup records enforce deployment ID.
    Environment-selected service files, multi-host failover, and search-path
    options are intentionally not installation inputs in this first slice.
    """
    if (
        not isinstance(dsn, str)
        or len(dsn) > 8192
        or not dsn.startswith(("postgresql://", "postgres://"))
    ):
        raise ForestError(
            "VALIDATION_FAILED", "Storage must be an explicit PostgreSQL connection URI", status=422
        )
    try:
        params = conninfo_to_dict(dsn)
        allowed = {
            "host",
            "hostaddr",
            "port",
            "dbname",
            "user",
            "password",
            "sslmode",
            "connect_timeout",
            "application_name",
        }
        if params.keys() - allowed:
            raise ValueError("Unsupported connection option")
        if not all(params.get(name) for name in ("host", "dbname", "user")):
            raise ValueError("Explicit host, database, and user required")
        if "," in params["host"] or "," in params.get("hostaddr", ""):
            raise ValueError("One storage target required")
        if not params["host"].startswith("/"):
            params["host"] = params["host"].lower().rstrip(".")
        port = int(params.get("port", "5432"))
        if not 1 <= port <= 65535:
            raise ValueError("Invalid port")
        params["port"] = str(port)
        timeout = int(params.get("connect_timeout", "5"))
        if not 1 <= timeout <= 30:
            raise ValueError("Connection timeout must be 1 to 30 seconds")
        params["connect_timeout"] = str(timeout)
        params.setdefault("sslmode", "prefer")
        if params["sslmode"] not in {
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        }:
            raise ValueError("Invalid TLS policy")
        normalized = make_conninfo(**dict(sorted(params.items())))
    except Exception:
        # libpq parse errors can embed the complete URI: never forward them.
        raise ForestError(
            "VALIDATION_FAILED",
            "Invalid PostgreSQL URI or unsupported connection options",
            status=422,
        ) from None
    target = {name: params[name] for name in ("host", "port", "dbname", "user")}
    if "hostaddr" in params:
        target["hostaddr"] = params["hostaddr"]
    return normalized, target


class BootstrapState:
    def __init__(self, data_dir: str | Path, *, token_ttl_seconds: int = 900):
        if not 1 <= token_ttl_seconds <= 86400:
            raise ValueError("Setup token lifetime must be 1 to 86400 seconds")
        self.data_dir = private_directory(data_dir)
        self.vault = SecretVault(self.data_dir)
        self.path = self.data_dir / "bootstrap.sqlite3"
        self.token_ttl_seconds = token_ttl_seconds
        private_file(self.path)
        with self._transaction() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS bootstrap_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    deployment_id TEXT NOT NULL,
                    setup_id TEXT NOT NULL,
                    token_digest TEXT,
                    token_expiry REAL NOT NULL,
                    stage TEXT NOT NULL CHECK(stage IN ('UNINITIALIZED','FROZEN','COMPLETE')),
                    target_identity TEXT,
                    plan_digest TEXT,
                    encrypted_plan TEXT,
                    owner_epoch INTEGER NOT NULL DEFAULT 1 CHECK(owner_epoch = 1)
                )
            """)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> str | None:
        """Return the setup credential only to the first successful installer."""
        with self._transaction() as connection:
            if connection.execute("SELECT 1 FROM bootstrap_state WHERE singleton=1").fetchone():
                return None
            token = random_token()
            connection.execute(
                "INSERT INTO bootstrap_state(singleton,deployment_id,setup_id,token_digest,token_expiry,stage) VALUES(1,?,?,?,?, 'UNINITIALIZED')",
                (
                    str(uuid4()),
                    str(uuid4()),
                    digest_token(token),
                    time.time() + self.token_ttl_seconds,
                ),
            )
        return token

    def rotate_setup_token(self) -> str:
        """Local operator recovery only; never expose as an unauthenticated API."""
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone()
            if not row or row["stage"] != "UNINITIALIZED":
                raise ForestError(
                    "SETUP_TOKEN_ROTATION_FORBIDDEN",
                    "Only an uninitialized deployment may rotate its setup credential",
                    status=409,
                )
            token = random_token()
            connection.execute(
                "UPDATE bootstrap_state SET token_digest=?, token_expiry=? WHERE singleton=1 AND stage='UNINITIALIZED'",
                (digest_token(token), time.time() + self.token_ttl_seconds),
            )
            return token

    @staticmethod
    def _safe_status(row: sqlite3.Row | None) -> dict[str, Any]:
        return {
            "setup_required": row is None or row["stage"] != "COMPLETE",
            "stage": row["stage"] if row else "UNINITIALIZED",
            "deployment_id": row["deployment_id"] if row else None,
            "setup_id": row["setup_id"] if row else None,
        }

    def status(self) -> dict[str, Any]:
        with self._transaction() as connection:
            return self._safe_status(
                connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone()
            )

    @staticmethod
    def _token_matches(row: sqlite3.Row | None, token: str) -> bool:
        if not isinstance(token, str) or len(token) > 256:
            return False
        try:
            supplied = digest_token(token)
        except UnicodeError:
            return False
        expected = row["token_digest"] if row and row["token_digest"] else "0" * 64
        matches = hmac.compare_digest(supplied, expected)
        return bool(
            matches and row and row["stage"] != "COMPLETE" and row["token_expiry"] > time.time()
        )

    def verify_token(self, token: str) -> bool:
        with self._transaction() as connection:
            return self._token_matches(
                connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone(),
                token,
            )

    def freeze(self, token: str, dsn: str, username: str, password_hash: str) -> dict[str, Any]:
        """Choose exactly one immutable plan before any target database mutation.

        There is deliberately no reclaimable execution lease in this slice:
        duplicates can execute only this immutable plan and target-side setup
        must be idempotent. owner_epoch=1 documents that no takeover API exists.
        """
        canonical_dsn, target = normalize_dsn(dsn)
        if not isinstance(username, str) or not username.strip() or len(username) > 128:
            raise ForestError(
                "VALIDATION_FAILED",
                "Administrator name must contain 1 to 128 characters",
                status=422,
            )
        username = username.strip()
        try:
            _decode_password_hash(password_hash)
        except (ValueError, TypeError):
            raise ForestError(
                "VALIDATION_FAILED",
                "Administrator credential must be a supported password hash",
                status=422,
            ) from None
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone()
            if not self._token_matches(row, token):
                raise ForestError(
                    "SETUP_TOKEN_INVALID",
                    "Setup credential is invalid, expired, or already consumed",
                    status=403,
                )
            plan = {
                "deployment_id": row["deployment_id"],
                "setup_id": row["setup_id"],
                "target_identity": target,
                "dsn": canonical_dsn,
                "username": username,
                "password_hash": password_hash,
                "owner_epoch": 1,
            }
            serialized = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            plan_digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
            if row["stage"] == "FROZEN":
                if not hmac.compare_digest(row["plan_digest"], plan_digest):
                    raise ForestError(
                        "SETUP_PLAN_FROZEN",
                        "Initialization is already bound to another immutable plan; recover the original target",
                        status=409,
                    )
                return self._read_plan(row)
            plan["plan_digest"] = plan_digest
            ciphertext = self.vault.encrypt(
                json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            )
            connection.execute(
                "UPDATE bootstrap_state SET stage='FROZEN', target_identity=?, plan_digest=?, encrypted_plan=? WHERE singleton=1 AND stage='UNINITIALIZED'",
                (json.dumps(target, sort_keys=True), plan_digest, ciphertext),
            )
            return plan

    def _read_plan(self, row: sqlite3.Row) -> dict[str, Any]:
        plan = json.loads(self.vault.decrypt(row["encrypted_plan"]))
        digest = plan.pop("plan_digest", None)
        actual = hashlib.sha256(
            json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        if (
            not isinstance(digest, str)
            or not hmac.compare_digest(actual, digest)
            or not hmac.compare_digest(digest, row["plan_digest"])
            or plan["deployment_id"] != row["deployment_id"]
            or plan["setup_id"] != row["setup_id"]
            or plan["owner_epoch"] != row["owner_epoch"]
            or plan["target_identity"] != json.loads(row["target_identity"])
        ):
            raise ForestError(
                "SETUP_STATE_INVALID",
                "Frozen initialization plan integrity check failed",
                status=503,
            )
        plan["plan_digest"] = digest
        return plan

    def load_plan(self) -> dict[str, Any] | None:
        """Private host recovery API. Never serialize this result to a browser."""
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone()
            return self._read_plan(row) if row and row["stage"] in {"FROZEN", "COMPLETE"} else None

    def load_dsn(self) -> str | None:
        plan = self.load_plan()
        return plan["dsn"] if plan else None

    def complete(self, setup_id: str, plan_digest: str) -> dict[str, Any]:
        """Only call after reconciling the target's matching committed setup."""
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone()
            if (
                not row
                or row["stage"] == "UNINITIALIZED"
                or row["setup_id"] != setup_id
                or row["plan_digest"] != plan_digest
            ):
                raise ForestError(
                    "SETUP_PLAN_CONFLICT",
                    "Completion does not match the frozen initialization plan",
                    status=409,
                )
            self._read_plan(row)
            connection.execute(
                "UPDATE bootstrap_state SET stage='COMPLETE', token_digest=NULL WHERE singleton=1"
            )
            result = self._safe_status(row)
            result.update(stage="COMPLETE", setup_required=False)
            return result
