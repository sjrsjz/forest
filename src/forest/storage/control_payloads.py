"""Immutable control input/result bodies with explicit Host-proven dependencies.

All methods run in the caller's authorized transaction: acquire runtime/Host or
Web authority and business locks before the memory clock. These helpers neither
authenticate tickets nor accept provenance from a Worker/Web request body. The
caller must propagate errors so a failed admission rolls back the whole write.
Inputs are Host-normalized management DTOs with raw API keys/tokens removed;
credentials retain their separate encrypted lifecycle. Do not persist the raw
authentication/guard request merely because it has a valid operation digest.

Payload digests identify {action, operation_id, value} envelopes. They do not
replace the existing operation input digest or plan digest. Removing a body
preserves the identity and facts but never authorizes recreating it.
"""

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore
from forest.storage.source_domains import SourceDomainStore

UNAVAILABLE = frozenset({
    "PAYLOAD_ERASED", "PAYLOAD_INCOMPLETE", "SOURCE_SUPPRESSED", "SOURCE_LINK_SCOPE_LIMIT",
})


class ControlPayloadStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)
        self.domains = SourceDomainStore(store)

    @staticmethod
    def _identity(operation_id, action):
        if any(not isinstance(value, str) or not value
               or len(value.encode("utf-8")) > maximum
               or any(ord(c) < 32 or ord(c) == 127 for c in value)
               for value, maximum in ((operation_id, 4096), (action, 256))):
            raise ForestError("INVALID_CONTROL_IDENTITY", "Invalid control operation or action", 422)

    @staticmethod
    def _owner(kind, operation_id):
        return kind + ":" + digest_json([operation_id])

    @staticmethod
    def _revoked():
        return ForestError("CONTROL_CONTENT_REVOKED", "Control content dependencies are unavailable", 409)

    @staticmethod
    def _conflict():
        return ForestError("CONTROL_PAYLOAD_IDENTITY_CONFLICT", "Control payload identity differs", 409)

    async def require_in(self, conn, refs, *, epoch):
        try:
            await self.payloads.require_available_in(
                conn, tuple(refs), expected_deletion_epoch=epoch,
            )
        except ForestError as exc:
            if exc.code not in UNAVAILABLE:
                raise
            raise self._revoked() from None

    async def _put(self, conn, kind, operation_id, action, value, *, parents, sources=(), epoch):
        try:
            ref = await self.payloads.put_json_in(
                conn, owner_kind="control." + kind, owner_id=self._owner(kind, operation_id),
                value={"action": action, "operation_id": operation_id, "value": value},
                sources=sources, parents=tuple(parents), expected_deletion_epoch=epoch,
            )
            # PayloadStore may return a prior erased receipt. A control writer
            # cannot treat that receipt as a fresh admitted body.
            await self.require_in(conn, (ref.payload_id,), epoch=epoch)
            return ref
        except ForestError as exc:
            if exc.code == "PAYLOAD_CONFLICT":
                raise self._conflict() from None
            if exc.code not in UNAVAILABLE:
                raise
            raise self._revoked() from None

    async def put_input_in(self, conn, operation_id, value, *, action, parents=(), epoch):
        self._identity(operation_id, action)
        # Check the epoch before registering an independent Host source. A
        # forgotten domain keeps its stable identity even with a newer epoch.
        await self.payloads._epoch_in(conn, epoch)
        source = await self.domains.source_in(
            conn, "CONTROL_INPUT", self._owner("input", operation_id),
        )
        return await self._put(
            conn, "input", operation_id, action, value, parents=parents, sources=(source,), epoch=epoch,
        )

    async def put_result_in(
        self, conn, operation_id, value, *, action, input_payload_id, parents=(), epoch,
    ):
        self._identity(operation_id, action)
        # The exact input must belong to this operation/action and still be
        # readable. Other parents are actual Host reads (plan/current/previous),
        # not guessed ancestry inferred from values or another operation.
        await self._load(
            conn, "input", operation_id, action, input_payload_id,
            expected_digest=None, epoch=epoch, required=True,
        )
        return await self._put(
            conn, "result", operation_id, action, value,
            parents=(input_payload_id, *parents), epoch=epoch,
        )

    async def _load(
        self, conn, kind, operation_id, action, payload_id, *, expected_digest, epoch, required,
        legacy=False,
    ):
        if payload_id is None:
            raise ForestError("CONTROL_PAYLOAD_MISSING", "Control content has no payload identity", 409)
        header = await self.payloads._header_in(conn, payload_id)
        if (header["owner_kind"] != "control." + kind
                or header["owner_id"] != self._owner(kind, operation_id)
                or header["encoding"] != "json"
                or (expected_digest is not None and header["digest"] != expected_digest)):
            raise self._conflict()
        if kind == "input":
            domain = await (await conn.execute(
                "SELECT 1 FROM public.bot_payload_sources s JOIN public.bot_payload_domains d "
                "ON d.bot_id=s.bot_id AND d.source_unit_id=s.source_unit_id "
                "WHERE s.bot_id=%s AND s.payload_id=%s AND s.source_revision=1 AND s.part_id='*' "
                "AND d.kind='CONTROL_INPUT' AND d.owner_id=%s",
                (self.store.bot_id, payload_id, self._owner(kind, operation_id)),
            )).fetchone()
            if domain is None:
                raise self._conflict()
        elif not legacy:
            parent = await (await conn.execute(
                "SELECT h.payload_id FROM public.bot_payload_derivations d "
                "JOIN public.bot_payload_headers h "
                "ON h.bot_id=d.bot_id AND h.payload_id=d.parent_payload_id "
                "WHERE d.bot_id=%s AND d.payload_id=%s AND h.owner_kind='control.input' "
                "AND h.owner_id=%s",
                (self.store.bot_id, payload_id, self._owner("input", operation_id)),
            )).fetchone()
            if parent is None:
                raise self._conflict()
            _, input_state = await self._load(
                conn, "input", operation_id, action, parent["payload_id"],
                expected_digest=None, epoch=epoch, required=required,
            )
            if input_state == "UNAVAILABLE":
                return None, input_state
        try:
            envelope = await self.payloads.read_json_in(
                conn, payload_id, expected_deletion_epoch=epoch,
            )
        except ForestError as exc:
            if exc.code not in UNAVAILABLE:
                raise
            if required:
                raise self._revoked() from None
            return None, "UNAVAILABLE"
        if (not isinstance(envelope, dict) or set(envelope) != {"action", "operation_id", "value"}
                or envelope["action"] != action or envelope["operation_id"] != operation_id
                or digest_json(envelope) != header["digest"]):
            raise self._conflict()
        return envelope["value"], "ACTIVE"

    async def _legacy_result_in(self, conn, row):
        """Only an actual immutable 040 row grants the missing-input exception.

        No boolean from a supplied dictionary authorizes a legacy result. Final
        040 SQL prohibits creating or changing this marker at runtime.
        """
        if row.get("input_legacy_unknown") is not True:
            return False
        actual = await (await conn.execute(
            "SELECT 1 FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s "
            "AND action=%s AND result_payload_id=%s AND result_payload_digest=%s "
            "AND input_legacy_unknown AND input_payload_id IS NULL AND input_payload_digest IS NULL",
            (self.store.bot_id, row.get("operation_id"), row.get("action"),
             row.get("result_payload_id"), row.get("result_payload_digest")),
        )).fetchone()
        if actual is None:
            raise self._conflict()
        return True

    async def _hydrate(self, conn, row, kind, *, epoch, required):
        if row is None:
            return None
        operation_id, action = row.get("operation_id"), row.get("action")
        legacy = kind == "result" and await self._legacy_result_in(conn, row)
        # A proven old identity can predate today's length/format limits. Its
        # actual DB facts and exact envelope still bind action and operation.
        if not legacy:
            self._identity(operation_id, action)
        expected = row.get(kind + "_payload_digest")
        if expected is None:
            raise ForestError("CONTROL_PAYLOAD_MISSING", "Control content has no payload digest", 409)
        value, state = await self._load(
            conn, kind, operation_id, action, row.get(kind + "_payload_id"),
            expected_digest=expected, epoch=epoch, required=required, legacy=legacy,
        )
        return {**row, kind: value, kind + "_payload_state": state}

    async def hydrate_input_in(self, conn, row, *, epoch, required=True):
        return await self._hydrate(conn, row, "input", epoch=epoch, required=required)

    async def hydrate_result_in(self, conn, row, *, epoch, required=True):
        return await self._hydrate(conn, row, "result", epoch=epoch, required=required)
