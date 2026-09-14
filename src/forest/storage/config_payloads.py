"""Removable configuration values under immutable Host-selected identities.

Callers authorize and freeze the real plan/revision identity before using these
transactional helpers. Acquire their existing authority/business locks before
the memory clock and retain their final authority/time checks. This layer does
not authorize an action or require a business row to exist already: a plan's
values must be admitted before inserting that immutable plan and its receipt.

Parents are actual Host reads/admissions, never references supplied by a Worker
or Web DTO. New values require parents and create no independent source domain.
The caller supplies its defined nonsecret values DTO; credentials retain their
separate lifecycle. This helper is not a recursive secret detector.

The envelope digest is independent of existing plan/input/business digests.
Readers return a closed DTO, so stale row values, defaults and secret columns
cannot become a fallback. ACTIVE JSON null and unavailable content both use
values=None and are distinguished by values_payload_state. Missing identities
or mismatched receipts are errors, including on metadata-only reads.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from itertools import islice

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import MAX_PARENTS, PayloadRef, PayloadStore, _json

FAMILIES = frozenset({
    "config", "agent.config", "memory.config", "binding", "watch", "job", "job.delegation",
})
KINDS = frozenset({"PLAN", "REVISION", "INITIAL"})
UNAVAILABLE = frozenset({
    "PAYLOAD_ERASED", "PAYLOAD_INCOMPLETE", "SOURCE_SUPPRESSED", "SOURCE_LINK_SCOPE_LIMIT",
})
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_REVISION = re.compile(r"(?:0|[1-9][0-9]*)\Z")


def _text(value, *, empty=False, maximum=4096):
    if not isinstance(value, str) or (not value and not empty):
        return False
    try:
        return (len(value.encode("utf-8")) <= maximum
                and not any(ord(c) < 32 or ord(c) == 127 for c in value))
    except UnicodeError:
        return False


class ConfigPayloadStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)

    @staticmethod
    def _identity(*, family, kind, resource, identity):
        if (not isinstance(family, str) or family not in FAMILIES
                or not isinstance(kind, str) or kind not in KINDS
                or not _text(resource, empty=True) or not _text(identity)
                or (kind == "REVISION" and not _REVISION.fullmatch(identity))):
            raise ForestError("INVALID_CONFIG_IDENTITY", "Invalid configuration identity", 422)

    @classmethod
    def owner_id(cls, *, family, kind, resource, identity):
        """Versioned identity shared with the corresponding frozen SQL reader."""
        cls._identity(family=family, kind=kind, resource=resource, identity=identity)
        return "config-values-v1:" + digest_json([family, kind, resource, identity])

    @staticmethod
    def _conflict():
        return ForestError("CONFIG_PAYLOAD_IDENTITY_CONFLICT", "Configuration payload identity differs", 409)

    @staticmethod
    def _revoked():
        return ForestError("CONTROL_CONTENT_REVOKED", "Configuration content dependencies are unavailable", 409)

    @staticmethod
    def _parents(parents):
        # Bound even an iterator before materializing. The limit is deliberately
        # conservative on supplied references; duplicates do not permit larger
        # input lists. Within that bound their order/duplication is immaterial.
        if isinstance(parents, (str, bytes)):
            raise ForestError("INVALID_PAYLOAD_REFERENCE", "Invalid configuration parent references", 422)
        try:
            values = tuple(islice(iter(parents), MAX_PARENTS + 1))
        except TypeError:
            raise ForestError("INVALID_PAYLOAD_REFERENCE", "Invalid configuration parent references", 422) from None
        if not values:
            raise ForestError("CONFIG_PAYLOAD_PARENTS_REQUIRED", "Configuration values require actual parents", 422)
        if len(values) > MAX_PARENTS:
            raise ForestError("PAYLOAD_DEPENDENCY_LIMIT", "Too many configuration parent references", 422)
        if any(not _text(value, maximum=256) for value in values):
            raise ForestError("INVALID_PAYLOAD_REFERENCE", "Invalid configuration parent references", 422)
        return tuple(sorted(set(values)))

    async def put_values_in(
        self, conn, *, family, kind, resource, identity, values, parents, epoch,
    ) -> PayloadRef:
        owner = self.owner_id(family=family, kind=kind, resource=resource, identity=identity)
        parent_ids = self._parents(parents)
        # Freeze JSON before any await. A caller retaining a mutable dictionary
        # cannot change the admitted envelope while waiting for the clock.
        envelope = json.loads(_json({
            "family": family, "kind": kind, "resource": resource,
            "identity": identity, "values": values,
        }))
        try:
            ref = await self.payloads.put_json_in(
                conn, owner_kind="control.config", owner_id=owner, value=envelope,
                parents=parent_ids, expected_deletion_epoch=epoch,
            )
            # A prior owner may be a permanent erased receipt. Check the stored
            # envelope AND its real parents, even when put returns an old ref.
            await self._load(
                conn, family=family, kind=kind, resource=resource, identity=identity,
                payload_id=ref.payload_id, expected_digest=ref.digest, epoch=epoch,
                required=True,
            )
            return ref
        except ForestError as exc:
            if exc.code == "PAYLOAD_CONFLICT":
                raise self._conflict() from None
            if exc.code in UNAVAILABLE:
                raise self._revoked() from None
            raise

    async def _load(
        self, conn, *, family, kind, resource, identity, payload_id,
        expected_digest, epoch, required,
    ):
        owner = self.owner_id(family=family, kind=kind, resource=resource, identity=identity)
        header = await self.payloads._header_in(conn, payload_id)
        manifest = header["manifest"]
        if (header["bot_id"] != self.store.bot_id or header["owner_kind"] != "control.config"
                or header["owner_id"] != owner or header["encoding"] != "json"
                or header["digest"] != expected_digest
                or payload_id != "payload:" + digest_json([self.store.bot_id, "control.config", owner])
                or not isinstance(manifest, dict) or set(manifest) != {"parents", "sources"}
                or not isinstance(manifest["parents"], list)):
            raise self._conflict()
        try:
            parents = self._parents(manifest["parents"])
        except ForestError:
            raise self._conflict() from None
        actual = await (await conn.execute(
            "SELECT parent_payload_id FROM public.bot_payload_derivations "
            "WHERE bot_id=%s AND payload_id=%s ORDER BY parent_payload_id LIMIT %s",
            (self.store.bot_id, payload_id, MAX_PARENTS + 1),
        )).fetchall()
        if (list(parents) != manifest["parents"]
                or tuple(row["parent_payload_id"] for row in actual) != parents):
            raise self._conflict()
        try:
            # Source flattening is not a substitute for actual parent body
            # availability. Validate parents even if this child still has bytes.
            await self.payloads.require_available_in(
                conn, parents, expected_deletion_epoch=epoch,
            )
            envelope = await self.payloads.read_json_in(
                conn, payload_id, expected_deletion_epoch=epoch,
            )
        except ForestError as exc:
            if exc.code not in UNAVAILABLE:
                raise
            if required:
                raise self._revoked() from None
            return None, "UNAVAILABLE"
        except (ValueError, UnicodeError, RecursionError):
            raise self._conflict() from None
        if (not isinstance(envelope, dict)
                or set(envelope) != {"family", "kind", "resource", "identity", "values"}
                or envelope["family"] != family or envelope["kind"] != kind
                or envelope["resource"] != resource or envelope["identity"] != identity):
            raise self._conflict()
        try:
            digest = digest_json(envelope)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise self._conflict() from None
        if digest != expected_digest:
            raise self._conflict()
        return envelope["values"], "ACTIVE"

    async def hydrate_values_in(
        self, conn, row, *, family, kind, resource, identity, epoch, required=True,
    ):
        self._identity(family=family, kind=kind, resource=resource, identity=identity)
        if not isinstance(row, Mapping):
            raise ForestError("CONFIG_PAYLOAD_MISSING", "Configuration has no payload identity", 409)
        # Read only identity facts before awaiting. Never copy legacy values,
        # defaults, credentials or arbitrary metadata out of the supplied row.
        payload_id = row.get("values_payload_id")
        digest = row.get("values_payload_digest")
        if payload_id is None or digest is None:
            raise ForestError("CONFIG_PAYLOAD_MISSING", "Configuration has no payload identity or digest", 409)
        if (("bot_id" in row and row["bot_id"] != self.store.bot_id)
                or not isinstance(digest, str) or not _DIGEST.fullmatch(digest)):
            raise self._conflict()
        value, state = await self._load(
            conn, family=family, kind=kind, resource=resource, identity=identity,
            payload_id=payload_id, expected_digest=digest, epoch=epoch, required=required,
        )
        return {
            "family": family, "kind": kind, "resource": resource, "identity": identity,
            "values_payload_id": payload_id, "values_payload_digest": digest,
            "values_payload_state": state, "values": value,
        }
