"""Write-only deployment secret authority with versioned AEAD ciphertext."""

from __future__ import annotations

import hashlib
import inspect
import re
from dataclasses import dataclass
from os import urandom

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

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
    ) -> SecretMetadata:
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
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.secret/{secret_ref}",),
            )
            replay = await (
                await conn.execute(
                    "SELECT v.*,s.purpose,s.revision FROM forest_deployment.secret_versions v "
                    "JOIN forest_deployment.secret_slots s USING(secret_ref) "
                    "WHERE v.operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError("SECRET_OPERATION_CONFLICT", "Secret operation differs", 409)
                return self._metadata(replay)
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

    async def metadata(self, secret_ref: str) -> SecretMetadata:
        row = await self._current(secret_ref)
        return self._metadata(row)

    async def resolve(self, secret_ref: str) -> bytes:
        row = await self._current(secret_ref)
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
        try:
            value = (await self.resolve(secret_ref)).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ForestError("SECRET_TEXT_INVALID", "Secret is not UTF-8 text", 422) from exc
        if not value or "\x00" in value:
            raise ForestError("SECRET_TEXT_INVALID", "Secret text is invalid", 422)
        return value

    async def _current(self, secret_ref: str):
        if not isinstance(secret_ref, str) or _SECRET_REF.fullmatch(secret_ref) is None:
            raise ForestError("SECRET_INPUT_INVALID", "Secret reference is invalid", 422)
        async with self.store.pool.connection() as conn:
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
