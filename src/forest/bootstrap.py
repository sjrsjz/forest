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
from uuid import UUID, uuid4

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


def _recovery_digest(value: dict[str, Any]) -> str:
    serialized = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


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
            connection.execute("""
                CREATE TABLE IF NOT EXISTS deployment_recovery_head (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    revision INTEGER NOT NULL CHECK (revision >= 0),
                    recovery_id TEXT
                )
            """)
            connection.execute(
                "INSERT OR IGNORE INTO deployment_recovery_head(singleton,revision,recovery_id) "
                "VALUES (1,0,NULL)"
            )
            connection.execute("""
                CREATE TABLE IF NOT EXISTS host_identity (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    host_id TEXT NOT NULL UNIQUE
                )
            """)
            connection.execute(
                "INSERT OR IGNORE INTO host_identity(singleton,host_id) VALUES (1,?)",
                (str(uuid4()),),
            )
            connection.execute("""
                CREATE TABLE IF NOT EXISTS deployment_recovery_plans (
                    recovery_id TEXT PRIMARY KEY,
                    expected_head_revision INTEGER NOT NULL CHECK (expected_head_revision >= 0),
                    base_plan_digest TEXT NOT NULL,
                    previous_target_identity TEXT NOT NULL,
                    target_identity TEXT NOT NULL,
                    artifact_digest TEXT NOT NULL,
                    manifest_digest TEXT NOT NULL,
                    content_digest TEXT NOT NULL,
                    target_marker TEXT NOT NULL,
                    recovery_digest TEXT NOT NULL,
                    encrypted_plan TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('PLANNED','APPLIED')),
                    created_at REAL NOT NULL,
                    applied_at REAL
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

    def host_identity(self) -> str:
        """Return the stable identity of this local Forest host."""
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT host_id FROM host_identity WHERE singleton=1"
            ).fetchone()
            if row is None:
                raise ForestError("HOST_IDENTITY_UNAVAILABLE", "Host identity is unavailable", 503)
            try:
                return str(UUID(row["host_id"]))
            except (TypeError, ValueError, AttributeError):
                raise ForestError(
                    "HOST_IDENTITY_INVALID", "Host identity failed integrity checks", 503
                ) from None

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

    def _read_recovery_plan(
        self, row: sqlite3.Row, root_plan: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            plan = json.loads(self.vault.decrypt(row["encrypted_plan"]))
            digest = plan.pop("recovery_digest", None)
            target = json.loads(row["target_identity"])
            previous = json.loads(row["previous_target_identity"])
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            raise ForestError(
                "RECOVERY_STATE_INVALID", "Deployment recovery state failed integrity checks", 503
            ) from None
        expected = {
            "recovery_id", "expected_head_revision", "base_plan_digest", "previous_dsn",
            "previous_target_identity", "target_dsn", "target_identity", "target_dsn_digest",
            "artifact_path", "artifact_digest", "backup_id", "manifest_digest",
            "content_digest", "restore_id", "target_marker", "created_at",
        }
        if (
            not isinstance(plan, dict)
            or set(plan) != expected
            or not isinstance(digest, str)
            or not hmac.compare_digest(_recovery_digest(plan), digest)
            or not hmac.compare_digest(digest, row["recovery_digest"])
            or plan["recovery_id"] != row["recovery_id"]
            or plan["expected_head_revision"] != row["expected_head_revision"]
            or plan["base_plan_digest"] != root_plan["plan_digest"]
            or plan["base_plan_digest"] != row["base_plan_digest"]
            or plan["previous_target_identity"] != previous
            or plan["target_identity"] != target
            or plan["artifact_digest"] != row["artifact_digest"]
            or plan["manifest_digest"] != row["manifest_digest"]
            or plan["content_digest"] != row["content_digest"]
            or plan["target_marker"] != row["target_marker"]
            or _recovery_digest({"dsn": plan["target_dsn"]}) != plan["target_dsn_digest"]
        ):
            raise ForestError(
                "RECOVERY_STATE_INVALID", "Deployment recovery state failed integrity checks", 503
            )
        plan["recovery_digest"] = digest
        return plan

    def _effective_plan(
        self, connection: sqlite3.Connection, root: sqlite3.Row
    ) -> dict[str, Any]:
        plan = self._read_plan(root)
        head = connection.execute(
            "SELECT revision,recovery_id FROM deployment_recovery_head WHERE singleton=1"
        ).fetchone()
        if head is None or type(head["revision"]) is not int or head["revision"] < 0:
            raise ForestError(
                "RECOVERY_STATE_INVALID", "Deployment recovery head is invalid", 503
            )
        if head["recovery_id"] is None:
            if head["revision"] != 0:
                raise ForestError(
                    "RECOVERY_STATE_INVALID", "Deployment recovery head is invalid", 503
                )
            return plan
        recovery = connection.execute(
            "SELECT * FROM deployment_recovery_plans WHERE recovery_id=?",
            (head["recovery_id"],),
        ).fetchone()
        if (
            recovery is None
            or recovery["state"] != "APPLIED"
            or recovery["expected_head_revision"] + 1 != head["revision"]
        ):
            raise ForestError(
                "RECOVERY_STATE_INVALID", "Deployment recovery head is invalid", 503
            )
        recovered = self._read_recovery_plan(recovery, plan)
        plan["dsn"] = recovered["target_dsn"]
        plan["target_identity"] = recovered["target_identity"]
        return plan

    def load_plan(self) -> dict[str, Any] | None:
        """Private host recovery API. Never serialize this result to a browser."""
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM bootstrap_state WHERE singleton=1").fetchone()
            return (
                self._effective_plan(connection, row)
                if row and row["stage"] in {"FROZEN", "COMPLETE"}
                else None
            )

    @staticmethod
    def _safe_recovery(plan: dict[str, Any], state: str) -> dict[str, Any]:
        return {
            "recovery_id": plan["recovery_id"],
            "state": state,
            "expected_head_revision": plan["expected_head_revision"],
            "head_revision": plan["expected_head_revision"] + (state == "APPLIED"),
            "plan_digest": plan["recovery_digest"],
            "backup_id": plan["backup_id"],
            "artifact_digest": plan["artifact_digest"],
            "manifest_digest": plan["manifest_digest"],
            "content_digest": plan["content_digest"],
            "target_identity": plan["target_identity"],
        }

    def plan_recovery(
        self,
        *,
        recovery_id: str,
        target_dsn: str,
        artifact_path: str | Path,
        artifact_digest: str,
        backup_id: str,
        manifest_digest: str,
        content_digest: str,
        restore_id: str,
        target_marker: str,
    ) -> dict[str, Any]:
        """Freeze one validated target without changing the effective database."""
        canonical_dsn, target = normalize_dsn(target_dsn)
        if not isinstance(recovery_id, str) or not 1 <= len(recovery_id) <= 128:
            raise ForestError("RECOVERY_PLAN_INVALID", "Recovery identity is invalid", 422)
        artifact = str(Path(artifact_path).resolve(strict=True))
        now = time.time()
        with self._transaction() as connection:
            root = connection.execute(
                "SELECT * FROM bootstrap_state WHERE singleton=1"
            ).fetchone()
            if root is None or root["stage"] != "COMPLETE":
                raise ForestError(
                    "RECOVERY_BOOTSTRAP_REQUIRED",
                    "A completed local bootstrap state is required for deployment recovery",
                    409,
                )
            root_plan = self._read_plan(root)
            current = self._effective_plan(connection, root)
            head = connection.execute(
                "SELECT revision FROM deployment_recovery_head WHERE singleton=1"
            ).fetchone()
            plan = {
                "recovery_id": recovery_id,
                "expected_head_revision": head["revision"],
                "base_plan_digest": root_plan["plan_digest"],
                "previous_dsn": current["dsn"],
                "previous_target_identity": current["target_identity"],
                "target_dsn": canonical_dsn,
                "target_identity": target,
                "target_dsn_digest": _recovery_digest({"dsn": canonical_dsn}),
                "artifact_path": artifact,
                "artifact_digest": artifact_digest,
                "backup_id": backup_id,
                "manifest_digest": manifest_digest,
                "content_digest": content_digest,
                "restore_id": restore_id,
                "target_marker": target_marker,
                "created_at": now,
            }
            digest = _recovery_digest(plan)
            saved = connection.execute(
                "SELECT * FROM deployment_recovery_plans WHERE recovery_id=?", (recovery_id,)
            ).fetchone()
            if saved is not None:
                existing = self._read_recovery_plan(saved, root_plan)
                if not hmac.compare_digest(existing["recovery_digest"], digest):
                    raise ForestError(
                        "RECOVERY_PLAN_CONFLICT", "Recovery identity is already bound", 409
                    )
                return self._safe_recovery(existing, saved["state"])
            encrypted = self.vault.encrypt(json.dumps(
                {**plan, "recovery_digest": digest},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ))
            connection.execute(
                "INSERT INTO deployment_recovery_plans("
                "recovery_id,expected_head_revision,base_plan_digest,previous_target_identity,"
                "target_identity,artifact_digest,manifest_digest,content_digest,target_marker,"
                "recovery_digest,encrypted_plan,state,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,'PLANNED',?)",
                (
                    recovery_id,
                    head["revision"],
                    root_plan["plan_digest"],
                    json.dumps(current["target_identity"], sort_keys=True),
                    json.dumps(target, sort_keys=True),
                    artifact_digest,
                    manifest_digest,
                    content_digest,
                    target_marker,
                    digest,
                    encrypted,
                    now,
                ),
            )
            plan["recovery_digest"] = digest
            return self._safe_recovery(plan, "PLANNED")

    def read_recovery_plan(self, recovery_id: str) -> dict[str, Any]:
        """Private CLI API containing encrypted-at-rest paths and DSNs."""
        with self._transaction() as connection:
            root = connection.execute(
                "SELECT * FROM bootstrap_state WHERE singleton=1"
            ).fetchone()
            row = connection.execute(
                "SELECT * FROM deployment_recovery_plans WHERE recovery_id=?", (recovery_id,)
            ).fetchone()
            if root is None or row is None:
                raise ForestError("RECOVERY_PLAN_NOT_FOUND", "Recovery plan was not found", 404)
            plan = self._read_recovery_plan(row, self._read_plan(root))
            plan["state"] = row["state"]
            return plan

    def apply_recovery(self, recovery_id: str, recovery_digest: str) -> dict[str, Any]:
        """Atomically advance the effective target after caller revalidation."""
        with self._transaction() as connection:
            root = connection.execute(
                "SELECT * FROM bootstrap_state WHERE singleton=1"
            ).fetchone()
            row = connection.execute(
                "SELECT * FROM deployment_recovery_plans WHERE recovery_id=?", (recovery_id,)
            ).fetchone()
            if root is None or row is None:
                raise ForestError("RECOVERY_PLAN_NOT_FOUND", "Recovery plan was not found", 404)
            plan = self._read_recovery_plan(row, self._read_plan(root))
            if not isinstance(recovery_digest, str) or not hmac.compare_digest(
                plan["recovery_digest"], recovery_digest
            ):
                raise ForestError("RECOVERY_PLAN_CHANGED", "Recovery plan digest differs", 409)
            head = connection.execute(
                "SELECT revision,recovery_id FROM deployment_recovery_head WHERE singleton=1"
            ).fetchone()
            if row["state"] == "APPLIED" and head["recovery_id"] == recovery_id:
                return self._safe_recovery(plan, "APPLIED")
            if (
                row["state"] != "PLANNED"
                or head["revision"] != plan["expected_head_revision"]
                or head["recovery_id"] == recovery_id
            ):
                raise ForestError("RECOVERY_PLAN_STALE", "Recovery plan no longer names the head", 409)
            changed = connection.execute(
                "UPDATE deployment_recovery_head SET revision=revision+1,recovery_id=? "
                "WHERE singleton=1 AND revision=?",
                (recovery_id, plan["expected_head_revision"]),
            )
            if changed.rowcount != 1:
                raise ForestError("RECOVERY_PLAN_STALE", "Recovery plan no longer names the head", 409)
            changed = connection.execute(
                "UPDATE deployment_recovery_plans SET state='APPLIED',applied_at=? "
                "WHERE recovery_id=? AND state='PLANNED'",
                (time.time(), recovery_id),
            )
            if changed.rowcount != 1:
                raise ForestError("RECOVERY_PLAN_STALE", "Recovery plan no longer names the head", 409)
            return self._safe_recovery(plan, "APPLIED")

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
