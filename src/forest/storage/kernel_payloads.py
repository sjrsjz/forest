"""Kernel content references; callers retain ownership, exposure and epoch guards.

Nothing here authorizes a Worker or revives an erased immutable receipt. Current
and historical state share value payloads at a fixed component/key/revision.
"""

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore

UNAVAILABLE = frozenset(
    {
        "PAYLOAD_ERASED",
        "PAYLOAD_INCOMPLETE",
        "SOURCE_SUPPRESSED",
        "SOURCE_LINK_SCOPE_LIMIT",
    }
)


class KernelPayloadStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)

    @staticmethod
    def _owner(family, *identity):
        return family + ":" + digest_json(list(identity))

    @staticmethod
    def _verify(value, expected):
        actual = digest_json(value)
        if expected not in {actual, actual.removeprefix("sha256:")}:
            raise ForestError(
                "KERNEL_PAYLOAD_IDENTITY_CONFLICT",
                "Kernel content differs from its fact digest",
                409,
            )

    async def put_in(self, conn, kind, owner, value, *, parents=(), sources=(), epoch):
        try:
            ref = await self.payloads.put_json_in(
                conn,
                owner_kind=kind,
                owner_id=owner,
                value=value,
                parents=tuple(parents),
                sources=tuple(sources),
                expected_deletion_epoch=epoch,
            )
            # The same immutable owner may return an erased receipt. It is not
            # permission to publish another result using that removed body.
            await self.payloads.require_available_in(
                conn, (ref.payload_id,), expected_deletion_epoch=epoch
            )
            return ref
        except ForestError as exc:
            if exc.code not in UNAVAILABLE:
                raise
            raise ForestError(
                "KERNEL_CONTENT_REVOKED", "Kernel content dependencies are unavailable", 409
            ) from None

    async def _load(self, conn, payload_id, *, epoch, required):
        if payload_id is None:
            raise ForestError(
                "KERNEL_PAYLOAD_MISSING", "Kernel content has no payload identity", 409
            )
        try:
            value = await self.payloads.read_json_in(
                conn, payload_id, expected_deletion_epoch=epoch
            )
            return value, "ACTIVE"
        except ForestError as exc:
            if exc.code not in UNAVAILABLE:
                raise
            if required:
                raise ForestError(
                    "KERNEL_CONTENT_REVOKED", "Kernel content dependencies are unavailable", 409
                ) from None
            return None, "UNAVAILABLE"

    async def read_in(self, conn, payload_id, *, epoch, required=True):
        return (await self._load(conn, payload_id, epoch=epoch, required=required))[0]

    async def require_in(self, conn, refs, *, epoch):
        try:
            await self.payloads.require_available_in(
                conn, tuple(refs), expected_deletion_epoch=epoch
            )
        except ForestError as exc:
            if exc.code not in UNAVAILABLE:
                raise
            raise ForestError(
                "KERNEL_CONTENT_REVOKED", "Kernel dependencies are unavailable", 409
            ) from None

    async def put_call_input_in(self, conn, call_id, value, *, parents=(), sources=(), epoch):
        return await self.put_in(
            conn,
            "kernel.input",
            self._owner("input", call_id),
            value,
            parents=parents,
            sources=sources,
            epoch=epoch,
        )

    async def put_call_result_in(self, conn, call_id, value, *, parents=(), sources=(), epoch):
        return await self.put_in(
            conn,
            "kernel.result",
            self._owner("result", call_id),
            value,
            parents=parents,
            sources=sources,
            epoch=epoch,
        )

    async def hydrate_call_in(self, conn, row, *, epoch, required=True):
        if row is None:
            return None
        result = {**row, "result": None, "payload_state": "ABSENT"}
        if (
            row.get("result_payload_id") is None
            and row["status"] != "COMPLETED"
            and row.get("result_digest") is None
        ):
            return result
        value, state = await self._load(
            conn, row.get("result_payload_id"), epoch=epoch, required=required
        )
        if state == "ACTIVE":
            self._verify(value, row.get("result_digest"))
        return {**result, "result": value, "payload_state": state}

    async def put_state_in(
        self, conn, component, key, revision, value, *, parents=(), sources=(), epoch
    ):
        return await self.put_in(
            conn,
            "state.value",
            self._owner("state", component, key, revision),
            value,
            parents=parents,
            sources=sources,
            epoch=epoch,
        )

    async def hydrate_state_in(self, conn, row, *, epoch, required=True):
        if row is None:
            return None
        if row.get("deleted", False) or row["revision"] == 0:
            return {
                **row,
                "value": None,
                "payload_state": "TOMBSTONE" if row.get("deleted", False) else "ABSENT",
            }
        value, state = await self._load(
            conn, row.get("value_payload_id"), epoch=epoch, required=required
        )
        if state == "ACTIVE":
            self._verify(value, row.get("value_digest"))
        return {**row, "value": value, "payload_state": state}

    async def hydrate_step_in(self, conn, row, *, epoch, required=True):
        if row is None:
            return None
        value, state = await self._load(
            conn, row.get("value_payload_id"), epoch=epoch, required=required
        )
        if state == "ACTIVE":
            self._verify(value, row.get("value_digest"))
        return {
            **row,
            "value": value,
            "payload_state": state,
            "receipt": {**row["receipt_metadata"], "value": value},
        }

    async def put_migration_source_in(
        self, conn, operation_id, value, *, parents=(), sources=(), epoch
    ):
        return await self.put_in(
            conn,
            "state.migration",
            self._owner("source", operation_id),
            value,
            parents=parents,
            sources=sources,
            epoch=epoch,
        )

    async def put_migration_result_in(
        self, conn, operation_id, value, *, parents=(), sources=(), epoch
    ):
        return await self.put_in(
            conn,
            "state.migration",
            self._owner("candidate", operation_id),
            value,
            parents=parents,
            sources=sources,
            epoch=epoch,
        )

    async def hydrate_migration_source_in(self, conn, row, *, epoch, required=True):
        return await self._hydrate_migration(
            conn, row, "source_rows", "source_payload_id", "source_digest", epoch, required
        )

    async def hydrate_migration_result_in(self, conn, row, *, epoch, required=True):
        return await self._hydrate_migration(
            conn, row, "result_values", "result_payload_id", "result_digest", epoch, required
        )

    async def _hydrate_migration(self, conn, row, field, ref, digest, epoch, required):
        if row is None:
            return None
        value, state = await self._load(conn, row.get(ref), epoch=epoch, required=required)
        if state == "ACTIVE":
            self._verify(value, row[digest])
        return {**row, field: value, "payload_state": state}
