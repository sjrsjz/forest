"""Deployment-scoped Web accounts and revocable server-side sessions."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.security import hash_password, random_token, verify_password

_USERNAME = re.compile(r"[a-z][a-z0-9._-]{2,63}\Z")
_ROLE = {"ADMIN", "AUDITOR"}
_FAKE_HASH = "scrypt$32768$8$1$AAAAAAAAAAAAAAAAAAAAAA==$" + "A" * 43 + "="


def _text(value: object, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ForestError("WEB_IDENTITY_INPUT_INVALID", f"Invalid {field}", 422)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= maximum or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("WEB_IDENTITY_INPUT_INVALID", f"Invalid {field}", 422)
    return value


def _username(value: object) -> str:
    if not isinstance(value, str) or _USERNAME.fullmatch(value) is None:
        raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Username is invalid", 422)
    return value


def _role(value: object) -> str:
    if value not in _ROLE:
        raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Role is invalid", 422)
    return value


def _token_digest(value: object) -> str:
    if not isinstance(value, str) or not 32 <= len(value) <= 256:
        raise ForestError("WEB_SESSION_INVALID", "Web session is invalid", 401)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class WebSubject:
    subject_id: str
    username: str
    display_name: str
    role: str
    enabled: bool
    revision: int

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class WebSession:
    session_token: str = field(repr=False)
    csrf_token: str = field(repr=False)
    session_ref: str
    subject: WebSubject
    expires_at: datetime


@dataclass(frozen=True)
class AuthenticatedWebSession:
    session_id_hash: str
    session_ref: str
    csrf_hash: str
    subject: WebSubject
    expires_at: datetime

    def verify_csrf(self, token: object) -> None:
        if not isinstance(token, str) or not 32 <= len(token) <= 256:
            raise ForestError("MANAGEMENT_CSRF_REQUIRED", "CSRF token is invalid", 403)
        if not hmac.compare_digest(
            hashlib.sha256(token.encode("utf-8")).hexdigest(), self.csrf_hash
        ):
            raise ForestError("MANAGEMENT_CSRF_REQUIRED", "CSRF token is invalid", 403)


class DeploymentWebIdentityAuthority:
    def __init__(self, store, *, active_key_id: str, key_resolver) -> None:
        self.store = store
        self.active_key_id = _text(active_key_id, "active_key_id", 256)
        self.key_resolver = key_resolver

    async def ensure_bootstrap_admin(
        self,
        *,
        operation_id: str,
        username: str,
        display_name: str,
        password: str,
    ) -> WebSubject:
        return await self._create_account(
            operation_id=operation_id,
            operation_kind="BOOTSTRAP",
            username=username,
            display_name=display_name,
            password=password,
            role="ADMIN",
            require_empty=True,
        )

    async def create_account(
        self,
        *,
        operation_id: str,
        username: str,
        display_name: str,
        password: str,
        role: str,
    ) -> WebSubject:
        return await self._create_account(
            operation_id=operation_id,
            operation_kind="CREATE",
            username=username,
            display_name=display_name,
            password=password,
            role=role,
            require_empty=False,
        )

    async def _create_account(
        self,
        *,
        operation_id: str,
        operation_kind: str,
        username: str,
        display_name: str,
        password: str,
        role: str,
        require_empty: bool,
    ) -> WebSubject:
        operation_id = _text(operation_id, "operation_id", 256)
        username = _username(username)
        display_name = _text(display_name, "display_name", 128)
        role = _role(role)
        password_hash = await asyncio.to_thread(hash_password, password)
        subject_id = "web-subject/" + hashlib.sha256(username.encode()).hexdigest()[:32]
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.web-accounts",),
            )
            replay = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_account_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                input_digest = await self._credential_input_digest(
                    key_id=replay["credential_key_id"],
                    operation_id=operation_id,
                    password=password,
                    payload=[operation_kind, username, display_name, role],
                )
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "WEB_ACCOUNT_OPERATION_CONFLICT", "Account operation differs", 409
                    )
                return self._operation_subject(replay)
            if require_empty:
                occupied = await (
                    await conn.execute(
                        "SELECT EXISTS(SELECT 1 FROM forest_deployment.web_accounts) AS value"
                    )
                ).fetchone()
                if occupied["value"]:
                    raise ForestError(
                        "WEB_BOOTSTRAP_ALREADY_COMPLETE",
                        "The first administrator already exists",
                        409,
                    )
            exists = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.web_accounts "
                    "WHERE subject_id=%s OR username=%s",
                    (subject_id, username),
                )
            ).fetchone()
            if exists is not None:
                raise ForestError("WEB_ACCOUNT_EXISTS", "Account already exists", 409)
            input_digest = await self._credential_input_digest(
                key_id=self.active_key_id,
                operation_id=operation_id,
                password=password,
                payload=[operation_kind, username, display_name, role],
            )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.web_accounts("
                    "subject_id,username,display_name,password_hash,role) "
                    "VALUES (%s,%s,%s,%s,%s) RETURNING *",
                    (subject_id, username, display_name, password_hash, role),
                )
            ).fetchone()
            subject = self._subject(saved)
            await conn.execute(
                "INSERT INTO forest_deployment.web_account_operations("
                "operation_id,operation_kind,input_digest,credential_key_id,subject_id,"
                "resulting_revision,result) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    operation_id,
                    operation_kind,
                    input_digest,
                    self.active_key_id,
                    subject_id,
                    saved["revision"],
                    Jsonb(subject.as_dict()),
                ),
            )
            return subject

    async def update_account(
        self,
        *,
        operation_id: str,
        subject_id: str,
        display_name: str,
        role: str,
        enabled: bool,
        expected_revision: int,
    ) -> WebSubject:
        operation_id = _text(operation_id, "operation_id", 256)
        subject_id = _text(subject_id, "subject_id", 128)
        display_name = _text(display_name, "display_name", 128)
        role = _role(role)
        if type(enabled) is not bool or type(expected_revision) is not int or expected_revision < 1:
            raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Account update is invalid", 422)
        input_digest = digest_json(
            [subject_id, display_name, role, enabled, expected_revision]
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.web-accounts",),
            )
            replay = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_account_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "WEB_ACCOUNT_OPERATION_CONFLICT", "Account operation differs", 409
                    )
                return self._operation_subject(replay)
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_accounts "
                    "WHERE subject_id=%s FOR UPDATE",
                    (subject_id,),
                )
            ).fetchone()
            if row is None:
                raise ForestError("WEB_ACCOUNT_NOT_FOUND", "Account does not exist", 404)
            if row["revision"] != expected_revision:
                raise ForestError(
                    "WEB_ACCOUNT_REVISION_CONFLICT",
                    "Account revision changed",
                    409,
                    details={"revision": row["revision"]},
                )
            if row["role"] == "ADMIN" and row["enabled"] and (
                role != "ADMIN" or not enabled
            ):
                administrators = await (
                    await conn.execute(
                        "SELECT subject_id FROM forest_deployment.web_accounts "
                        "WHERE role='ADMIN' AND enabled FOR UPDATE"
                    )
                ).fetchall()
                if len(administrators) == 1:
                    raise ForestError(
                        "WEB_LAST_ADMINISTRATOR_REQUIRED",
                        "The final enabled administrator cannot be removed",
                        409,
                    )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.web_accounts SET display_name=%s,role=%s,"
                    "enabled=%s,revision=revision+1,updated_at=clock_timestamp() "
                    "WHERE subject_id=%s RETURNING *",
                    (display_name, role, enabled, subject_id),
                )
            ).fetchone()
            subject = self._subject(saved)
            await conn.execute(
                "INSERT INTO forest_deployment.web_account_operations("
                "operation_id,operation_kind,input_digest,subject_id,resulting_revision,result) "
                "VALUES (%s,'UPDATE',%s,%s,%s,%s)",
                (
                    operation_id,
                    input_digest,
                    subject_id,
                    saved["revision"],
                    Jsonb(subject.as_dict()),
                ),
            )
            if not enabled:
                await self._revoke_sessions_in(conn, subject_id)
            return subject

    async def change_password(
        self,
        *,
        operation_id: str,
        subject_id: str,
        password: str,
        expected_revision: int,
    ) -> WebSubject:
        operation_id = _text(operation_id, "operation_id", 256)
        subject_id = _text(subject_id, "subject_id", 128)
        if type(expected_revision) is not int or expected_revision < 1:
            raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Account revision is invalid", 422)
        password_hash = await asyncio.to_thread(hash_password, password)
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.web-accounts",),
            )
            replay = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_account_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                input_digest = await self._credential_input_digest(
                    key_id=replay["credential_key_id"],
                    operation_id=operation_id,
                    password=password,
                    payload=[subject_id, expected_revision],
                )
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "WEB_ACCOUNT_OPERATION_CONFLICT", "Account operation differs", 409
                    )
                return self._operation_subject(replay)
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_accounts "
                    "WHERE subject_id=%s FOR UPDATE",
                    (subject_id,),
                )
            ).fetchone()
            if row is None:
                raise ForestError("WEB_ACCOUNT_NOT_FOUND", "Account does not exist", 404)
            if row["revision"] != expected_revision:
                raise ForestError(
                    "WEB_ACCOUNT_REVISION_CONFLICT",
                    "Account revision changed",
                    409,
                    details={"revision": row["revision"]},
                )
            input_digest = await self._credential_input_digest(
                key_id=self.active_key_id,
                operation_id=operation_id,
                password=password,
                payload=[subject_id, expected_revision],
            )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.web_accounts SET password_hash=%s,"
                    "revision=revision+1,updated_at=clock_timestamp() "
                    "WHERE subject_id=%s RETURNING *",
                    (password_hash, subject_id),
                )
            ).fetchone()
            subject = self._subject(saved)
            await conn.execute(
                "INSERT INTO forest_deployment.web_account_operations("
                "operation_id,operation_kind,input_digest,credential_key_id,subject_id,"
                "resulting_revision,result) VALUES (%s,'PASSWORD',%s,%s,%s,%s,%s)",
                (
                    operation_id,
                    input_digest,
                    self.active_key_id,
                    subject_id,
                    saved["revision"],
                    Jsonb(subject.as_dict()),
                ),
            )
            await self._revoke_sessions_in(conn, subject_id)
            return subject

    async def revoke_sessions(
        self,
        *,
        operation_id: str,
        subject_id: str,
        expected_revision: int,
    ) -> WebSubject:
        operation_id = _text(operation_id, "operation_id", 256)
        subject_id = _text(subject_id, "subject_id", 128)
        if type(expected_revision) is not int or expected_revision < 1:
            raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Account revision is invalid", 422)
        input_digest = digest_json([subject_id, expected_revision])
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("forest.deployment.web-accounts",),
            )
            replay = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_account_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "WEB_ACCOUNT_OPERATION_CONFLICT", "Account operation differs", 409
                    )
                return self._operation_subject(replay)
            subject = await self._subject_in(conn, subject_id)
            if subject.revision != expected_revision:
                raise ForestError(
                    "WEB_ACCOUNT_REVISION_CONFLICT",
                    "Account revision changed",
                    409,
                    details={"revision": subject.revision},
                )
            await self._revoke_sessions_in(conn, subject_id)
            await conn.execute(
                "INSERT INTO forest_deployment.web_account_operations("
                "operation_id,operation_kind,input_digest,subject_id,resulting_revision,result) "
                "VALUES (%s,'REVOKE_SESSIONS',%s,%s,%s,%s)",
                (
                    operation_id,
                    input_digest,
                    subject_id,
                    subject.revision,
                    Jsonb(subject.as_dict()),
                ),
            )
            return subject

    async def login(
        self, *, username: str, password: str, ttl_seconds: int = 28_800
    ) -> WebSession:
        username = _username(username)
        if type(ttl_seconds) is not int or not 300 <= ttl_seconds <= 86_400:
            raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Session TTL is invalid", 422)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_accounts WHERE username=%s",
                    (username,),
                )
            ).fetchone()
        encoded = row["password_hash"] if row is not None else _FAKE_HASH
        verified = await asyncio.to_thread(verify_password, password, encoded)
        if row is None or not verified or not row["enabled"]:
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Credentials are invalid", 401
            )
        session_token = random_token()
        csrf_token = random_token()
        session_hash = _token_digest(session_token)
        csrf_hash = _token_digest(csrf_token)
        async with self.store.pool.connection() as conn, conn.transaction():
            current = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_accounts "
                    "WHERE subject_id=%s FOR UPDATE",
                    (row["subject_id"],),
                )
            ).fetchone()
            if (
                current is None
                or not current["enabled"]
                or current["revision"] != row["revision"]
                or current["password_hash"] != row["password_hash"]
            ):
                raise ForestError(
                    "MANAGEMENT_AUTHENTICATION_REQUIRED", "Credentials are invalid", 401
                )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.web_sessions("
                    "session_id_hash,csrf_hash,subject_id,authn_strength,expires_at) "
                    "VALUES (%s,%s,%s,'PASSWORD',clock_timestamp()+(%s * interval '1 second')) "
                    "RETURNING *",
                    (session_hash, csrf_hash, row["subject_id"], ttl_seconds),
                )
            ).fetchone()
        return WebSession(
            session_token,
            csrf_token,
            "web-session/" + session_hash[:32],
            self._subject(current),
            saved["expires_at"],
        )

    async def authenticate(self, session_token: str) -> AuthenticatedWebSession:
        session_hash = _token_digest(session_token)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT s.*,a.username,a.display_name,a.role,a.enabled,"
                    "a.revision AS account_revision FROM forest_deployment.web_sessions s "
                    "JOIN forest_deployment.web_accounts a USING(subject_id) "
                    "WHERE s.session_id_hash=%s",
                    (session_hash,),
                )
            ).fetchone()
        now = datetime.now(timezone.utc)
        if (
            row is None
            or row["revoked_at"] is not None
            or row["expires_at"] <= now
            or not row["enabled"]
        ):
            raise ForestError(
                "MANAGEMENT_AUTHENTICATION_REQUIRED", "Web session is invalid", 401
            )
        subject = WebSubject(
            row["subject_id"],
            row["username"],
            row["display_name"],
            row["role"],
            row["enabled"],
            row["account_revision"],
        )
        return AuthenticatedWebSession(
            session_hash,
            "web-session/" + session_hash[:32],
            row["csrf_hash"],
            subject,
            row["expires_at"],
        )

    async def logout(self, session_token: str) -> None:
        session_hash = _token_digest(session_token)
        async with self.store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.web_sessions "
                    "WHERE session_id_hash=%s FOR UPDATE",
                    (session_hash,),
                )
            ).fetchone()
            if row is None or row["revoked_at"] is not None:
                return
            await conn.execute(
                "UPDATE forest_deployment.web_sessions SET revoked_at=clock_timestamp(),"
                "revision=revision+1 WHERE session_id_hash=%s",
                (session_hash,),
            )

    async def list_accounts(self) -> tuple[dict, ...]:
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT subject_id,username,display_name,role,enabled,revision,"
                    "created_at,updated_at FROM forest_deployment.web_accounts "
                    "ORDER BY username"
                )
            ).fetchall()
        return tuple(dict(row) for row in rows)

    @staticmethod
    async def _revoke_sessions_in(conn, subject_id: str) -> None:
        rows = await (
            await conn.execute(
                "SELECT session_id_hash FROM forest_deployment.web_sessions "
                "WHERE subject_id=%s AND revoked_at IS NULL FOR UPDATE",
                (subject_id,),
            )
        ).fetchall()
        for row in rows:
            await conn.execute(
                "UPDATE forest_deployment.web_sessions SET revoked_at=clock_timestamp(),"
                "revision=revision+1 WHERE session_id_hash=%s",
                (row["session_id_hash"],),
            )

    async def _subject_in(self, conn, subject_id: str) -> WebSubject:
        row = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.web_accounts WHERE subject_id=%s",
                (subject_id,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("WEB_ACCOUNT_NOT_FOUND", "Account does not exist", 404)
        return self._subject(row)

    async def _credential_input_digest(
        self,
        *,
        key_id: str,
        operation_id: str,
        password: str,
        payload: list,
    ) -> str:
        key = self.key_resolver(key_id)
        if inspect.isawaitable(key):
            key = await key
        if not isinstance(key, bytes) or len(key) != 32:
            raise ForestError(
                "WEB_CREDENTIAL_KEY_UNAVAILABLE",
                "Web credential fingerprint key is unavailable",
                503,
            )
        try:
            password_bytes = password.encode("utf-8")
        except (AttributeError, UnicodeError) as exc:
            raise ForestError("WEB_IDENTITY_INPUT_INVALID", "Password is invalid", 422) from exc
        message = (
            b"forest.web-account-operation/1\0"
            + self.store.deployment_id.encode("utf-8")
            + b"\0"
            + operation_id.encode("utf-8")
            + b"\0"
            + password_bytes
        )
        credential_proof = "hmac-sha256:" + hmac.new(key, message, hashlib.sha256).hexdigest()
        return digest_json([*payload, key_id, credential_proof])

    @staticmethod
    def _subject(row) -> WebSubject:
        return WebSubject(
            row["subject_id"],
            row["username"],
            row["display_name"],
            row["role"],
            row["enabled"],
            row["revision"],
        )

    @staticmethod
    def _operation_subject(row) -> WebSubject:
        value = row["result"]
        if not isinstance(value, dict) or set(value) != {
            "subject_id",
            "username",
            "display_name",
            "role",
            "enabled",
            "revision",
        }:
            raise ForestError(
                "WEB_ACCOUNT_OPERATION_CORRUPT", "Account operation result is invalid", 503
            )
        enabled = value["enabled"]
        revision = value["revision"]
        if type(enabled) is not bool or type(revision) is not int or revision < 1:
            raise ForestError(
                "WEB_ACCOUNT_OPERATION_CORRUPT", "Account operation result is invalid", 503
            )
        try:
            return WebSubject(
                _text(value["subject_id"], "subject_id", 128),
                _username(value["username"]),
                _text(value["display_name"], "display_name", 128),
                _role(value["role"]),
                enabled,
                revision,
            )
        except ForestError as exc:
            raise ForestError(
                "WEB_ACCOUNT_OPERATION_CORRUPT", "Account operation result is invalid", 503
            ) from exc
