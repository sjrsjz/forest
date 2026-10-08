"""Write-only deployment secret authority with versioned AEAD ciphertext."""

from __future__ import annotations

import hashlib
import inspect
import re
from dataclasses import dataclass
from os import urandom
from urllib.parse import unquote, urlsplit

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from forest.control_action_authorization import authorize_control_action_in
from forest.errors import ForestError
from forest.packages import digest_json

_SECRET_REF = re.compile(r"secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,501}\Z")


def _bounded(value: object, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ForestError("SECRET_INPUT_INVALID", f"Invalid {field}", 422)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        size = 0
    if not 1 <= size <= limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ForestError("SECRET_INPUT_INVALID", f"Invalid {field}", 422)
    return value


@dataclass(frozen=True)
class SecretMetadata:
    secret_ref: str
    purpose: str
    version: int
    revision: int
    key_id: str


class DeploymentSecretAuthority:
    """Encrypt, rotate and resolve secrets without exposing them in projections."""

    def __init__(self, store, *, active_key_id: str, key_resolver) -> None:
        self.store = store
        self.active_key_id = _bounded(active_key_id, "active_key_id", 256)
        self.key_resolver = key_resolver

    async def put(
        self,
        *,
        operation_id: str,
        secret_ref: str,
        purpose: str,
        plaintext: bytes,
        expected_revision: int,
        authorization=None,
    ) -> SecretMetadata:
        async with self.store.pool.connection() as conn, conn.transaction():
            await authorize_control_action_in(
                conn, authorization, operation_id=operation_id
            )
            return await self.put_in(
                conn, operation_id=operation_id, secret_ref=secret_ref,
                purpose=purpose, plaintext=plaintext,
                expected_revision=expected_revision,
            )

    async def put_in(
        self, conn, *, operation_id: str, secret_ref: str, purpose: str,
        plaintext: bytes, expected_revision: int,
        pinned_request_id: str | None = None,
    ) -> SecretMetadata:
        """Commit one kernel-owned Secret write in the caller's fenced transaction."""
        _bounded(operation_id, "operation_id", 256)
        _bounded(purpose, "purpose", 256)
        if not isinstance(secret_ref, str) or _SECRET_REF.fullmatch(secret_ref) is None:
            raise ForestError("SECRET_INPUT_INVALID", "Secret reference is invalid", 422)
        if (
            not isinstance(plaintext, bytes)
            or not 1 <= len(plaintext) <= 65_536
            or type(expected_revision) is not int
            or expected_revision < 0
        ):
            raise ForestError("SECRET_INPUT_INVALID", "Secret value or revision is invalid", 422)
        plaintext_digest = "sha256:" + hashlib.sha256(plaintext).hexdigest()
        input_digest = digest_json(
            {
                "secret_ref": secret_ref,
                "purpose": purpose,
                "plaintext_digest": plaintext_digest,
                "expected_revision": expected_revision,
                "key_id": self.active_key_id,
            }
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.storage-request/dsn/{secret_ref}",),
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.storage-request/credential/{secret_ref}",),
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.secret-operation/{operation_id}",),
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.secret/{secret_ref}",),
        )
        reservation = await (
            await conn.execute(
                "SELECT request_id FROM forest_deployment.storage_requests "
                "WHERE deployment_id=%s AND dsn_secret_ref=%s LIMIT 1",
                (self.store.deployment_id, secret_ref),
            )
        ).fetchone()
        if reservation is not None and (
            reservation["request_id"] != pinned_request_id
            or purpose != "storage.postgresql.dsn"
            or expected_revision != 0
        ):
            raise ForestError(
                "STORAGE_REQUEST_DSN_RESERVED",
                "Host storage request reserves this DSN Secret reference", 409,
            )
        if pinned_request_id is not None:
            pinned = None
            if reservation is not None:
                pinned = await (
                    await conn.execute(
                        "SELECT credential_digest,physical_name,role_name "
                        "FROM forest_deployment.storage_request_pins "
                        "WHERE request_id=%s FOR SHARE", (pinned_request_id,),
                    )
                ).fetchone()
            if pinned is None:
                raise ForestError(
                    "STORAGE_REQUEST_NOT_PINNED",
                    "Host storage request has no pinned resource", 409,
                )
            try:
                dsn = urlsplit(plaintext.decode("utf-8"))
                _ = dsn.port
                password = unquote(dsn.password or "")
                valid_dsn = (
                    dsn.scheme in {"postgres", "postgresql"}
                    and dsn.hostname is not None
                    and unquote(dsn.username or "") == pinned["role_name"]
                    and dsn.path == "/" + pinned["physical_name"]
                    and dsn.fragment == ""
                    and "sha256:" + hashlib.sha256(password.encode()).hexdigest()
                    == pinned["credential_digest"]
                )
            except (UnicodeError, ValueError):
                valid_dsn = False
            if not valid_dsn:
                raise ForestError(
                    "STORAGE_SETTLEMENT_MISMATCH",
                    "Pinned role, database or credential differs from DSN", 409,
                )
            occupied = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.secret_slots "
                    "WHERE secret_ref=%s", (secret_ref,),
                )
            ).fetchone()
            if occupied is not None:
                raise ForestError(
                    "STORAGE_REQUEST_DSN_SETTLED",
                    "Pinned storage DSN Secret is already written", 409,
                )
        replay = await (
            await conn.execute(
                "SELECT v.*,s.purpose,v.version AS revision "
                "FROM forest_deployment.secret_versions v "
                "JOIN forest_deployment.secret_slots s USING(secret_ref) "
                "WHERE v.operation_id=%s",
                (operation_id,),
            )
        ).fetchone()
        if replay is not None:
            if replay["input_digest"] != input_digest:
                raise ForestError("SECRET_OPERATION_CONFLICT", "Secret operation differs", 409)
            return self._metadata(replay)
        credential_request = await (
            await conn.execute(
                "SELECT 1 FROM forest_deployment.storage_requests "
                "WHERE deployment_id=%s AND credential_ref=%s LIMIT 1",
                (self.store.deployment_id, secret_ref),
            )
        ).fetchone()
        if credential_request is not None:
            raise ForestError(
                "STORAGE_REQUEST_CREDENTIAL_FROZEN",
                "Storage request freezes this role password Secret", 409,
            )
        slot = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.secret_slots WHERE secret_ref=%s FOR UPDATE",
                (secret_ref,),
            )
        ).fetchone()
        observed_revision = 0 if slot is None else slot["revision"]
        if observed_revision != expected_revision:
            raise ForestError(
                "SECRET_REVISION_CONFLICT",
                "Secret revision changed",
                409,
                details={"revision": observed_revision},
            )
        if slot is not None and slot["purpose"] != purpose:
            raise ForestError("SECRET_PURPOSE_CONFLICT", "Secret purpose differs", 409)
        version = 1 if slot is None else slot["current_version"] + 1
        key = await self._key(self.active_key_id)
        nonce = urandom(12)
        ciphertext = AESGCM(key).encrypt(
            nonce, plaintext, self._associated_data(secret_ref, purpose, version)
        )
        if slot is None:
            await conn.execute(
                "INSERT INTO forest_deployment.secret_slots("
                "secret_ref,purpose,current_version) VALUES (%s,%s,%s)",
                (secret_ref, purpose, version),
            )
        else:
            await conn.execute(
                "UPDATE forest_deployment.secret_slots SET current_version=%s,"
                "revision=revision+1,updated_at=clock_timestamp() WHERE secret_ref=%s",
                (version, secret_ref),
            )
        saved = await (
            await conn.execute(
                "INSERT INTO forest_deployment.secret_versions("
                "secret_ref,version,operation_id,input_digest,key_id,nonce,ciphertext,"
                "plaintext_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *,"
                "(SELECT purpose FROM forest_deployment.secret_slots WHERE secret_ref=%s) "
                "AS purpose,(SELECT revision FROM forest_deployment.secret_slots "
                "WHERE secret_ref=%s) AS revision",
                (
                    secret_ref,
                    version,
                    operation_id,
                    input_digest,
                    self.active_key_id,
                    nonce,
                    ciphertext,
                    plaintext_digest,
                    secret_ref,
                    secret_ref,
                ),
            )
        ).fetchone()
        return self._metadata(saved)

    async def web_operation_result(
        self, *, operation_id: str, web_subject_ref: str,
    ) -> dict:
        """Read one original Web Secret result without replaying its write ticket."""
        _bounded(operation_id, "operation_id", 256)
        _bounded(web_subject_ref, "web_subject_ref", 128)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT r.target_operation_ref,v.secret_ref,v.version,v.key_id,"
                    "s.purpose FROM forest_deployment.web_authority_receipts r "
                    "LEFT JOIN forest_deployment.secret_versions v "
                    "ON v.operation_id=r.target_operation_ref "
                    "LEFT JOIN forest_deployment.secret_slots s "
                    "ON s.secret_ref=v.secret_ref "
                    "WHERE r.deployment_id=%s AND r.target_operation_ref=%s "
                    "AND r.web_subject_ref=%s "
                    "AND r.target_scope_kind='deployment' "
                    "AND r.target_component_id='deployment.secrets' "
                    "AND r.action_key='secret.put'",
                    (self.store.deployment_id, operation_id, web_subject_ref),
                )
            ).fetchone()
        if row is None:
            return {"status": "NOT_FOUND", "operation_id": operation_id, "result": None}
        if row["secret_ref"] is None or row["purpose"] is None:
            raise ForestError(
                "SECRET_OPERATION_RECEIPT_INCOMPLETE",
                "Committed Secret write has no result", 503,
            )
        return {
            "status": "SUCCEEDED", "operation_id": operation_id,
            "result": {
                "secret_ref": row["secret_ref"], "purpose": row["purpose"],
                "version": row["version"], "revision": row["version"],
                "key_id": row["key_id"],
            },
        }

    async def metadata(self, secret_ref: str) -> SecretMetadata:
        row = await self._current(secret_ref)
        return self._metadata(row)

    async def metadata_in(self, conn, secret_ref: str) -> SecretMetadata:
        return self._metadata(await self._current_in(conn, secret_ref))

    async def list_metadata(self, *, limit: int = 200) -> tuple[SecretMetadata, ...]:
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ForestError("SECRET_INPUT_INVALID", "Secret list limit is invalid", 422)
        async with self.store.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT s.*,s.current_version AS version,v.key_id "
                    "FROM forest_deployment.secret_slots s "
                    "JOIN forest_deployment.secret_versions v ON v.secret_ref=s.secret_ref "
                    "AND v.version=s.current_version ORDER BY s.created_at,s.secret_ref LIMIT %s",
                    (limit,),
                )
            ).fetchall()
        return tuple(self._metadata(row) for row in rows)

    async def resolve(self, secret_ref: str) -> bytes:
        row = await self._current(secret_ref)
        return await self._decrypt(secret_ref, row)

    async def resolve_in(self, conn, secret_ref: str) -> bytes:
        return await self._decrypt(secret_ref, await self._current_in(conn, secret_ref))

    async def _decrypt(self, secret_ref: str, row) -> bytes:
        key = await self._key(row["key_id"])
        try:
            plaintext = AESGCM(key).decrypt(
                bytes(row["nonce"]),
                bytes(row["ciphertext"]),
                self._associated_data(secret_ref, row["purpose"], row["version"]),
            )
        except InvalidTag as exc:
            raise ForestError(
                "SECRET_DECRYPTION_FAILED", "Secret ciphertext cannot be authenticated", 503
            ) from exc
        if "sha256:" + hashlib.sha256(plaintext).hexdigest() != row["plaintext_digest"]:
            raise ForestError("SECRET_DIGEST_MISMATCH", "Secret plaintext digest differs", 503)
        return plaintext

    async def resolve_text(self, secret_ref: str) -> str:
        return self._decode_text(await self.resolve(secret_ref))

    async def verify_reference_for_purpose(self, secret_ref: str, purpose: str) -> None:
        """Check an install-time Secret reference without accessing plaintext."""

        if not isinstance(secret_ref, str) or _SECRET_REF.fullmatch(secret_ref) is None:
            raise ForestError("SECRET_INPUT_INVALID", "Secret reference is invalid", 422)
        _bounded(purpose, "purpose", 256)
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT s.purpose FROM forest_deployment.secret_slots s "
                    "JOIN forest_deployment.secret_versions v "
                    "ON v.secret_ref=s.secret_ref AND v.version=s.current_version "
                    "WHERE s.secret_ref=%s",
                    (secret_ref,),
                )
            ).fetchone()
        if row is None:
            raise ForestError("SECRET_NOT_FOUND", "Secret reference does not exist", 404)
        if row["purpose"] != purpose:
            raise ForestError("SECRET_PURPOSE_CONFLICT", "Secret purpose differs", 409)

    async def resolve_text_for_purpose(self, secret_ref: str, purpose: str) -> str:
        """Resolve one current secret through a purpose-limited native port."""

        _bounded(purpose, "purpose", 256)
        async with self.store.pool.connection() as conn:
            row = await self._current_in(conn, secret_ref)
        if row["purpose"] != purpose:
            raise ForestError("SECRET_PURPOSE_CONFLICT", "Secret purpose differs", 409)
        return self._decode_text(await self._decrypt(secret_ref, row))

    async def resolve_text_in(self, conn, secret_ref: str) -> str:
        return self._decode_text(await self.resolve_in(conn, secret_ref))

    @staticmethod
    def _decode_text(plaintext: bytes) -> str:
        try:
            value = plaintext.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ForestError("SECRET_TEXT_INVALID", "Secret is not UTF-8 text", 422) from exc
        if not value or "\x00" in value:
            raise ForestError("SECRET_TEXT_INVALID", "Secret text is invalid", 422)
        return value

    async def _current(self, secret_ref: str):
        async with self.store.pool.connection() as conn:
            return await self._current_in(conn, secret_ref)

    async def _current_in(self, conn, secret_ref: str):
        if not isinstance(secret_ref, str) or _SECRET_REF.fullmatch(secret_ref) is None:
            raise ForestError("SECRET_INPUT_INVALID", "Secret reference is invalid", 422)
        row = await (
            await conn.execute(
                "SELECT v.*,s.purpose,s.revision FROM forest_deployment.secret_slots s "
                "JOIN forest_deployment.secret_versions v ON v.secret_ref=s.secret_ref "
                "AND v.version=s.current_version WHERE s.secret_ref=%s",
                (secret_ref,),
            )
        ).fetchone()
        if row is None:
            raise ForestError("SECRET_NOT_FOUND", "Secret reference does not exist", 404)
        return row

    async def _key(self, key_id: str) -> bytes:
        value = self.key_resolver(key_id)
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, bytes) or len(value) != 32:
            raise ForestError("SECRET_MASTER_KEY_UNAVAILABLE", "Secret master key is unavailable", 503)
        return value

    def _associated_data(self, secret_ref: str, purpose: str, version: int) -> bytes:
        return (
            "forest.deployment-secret/1\0"
            + self.store.deployment_id
            + "\0"
            + secret_ref
            + "\0"
            + purpose
            + "\0"
            + str(version)
        ).encode("utf-8")

    @staticmethod
    def _metadata(row) -> SecretMetadata:
        return SecretMetadata(
            row["secret_ref"],
            row["purpose"],
            row["version"],
            row["revision"],
            row["key_id"],
        )
