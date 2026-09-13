"""Canonical context content; immutable positions survive removal of bodies.

This repository requires the caller's Actor/Host transaction. It does not turn
payload references into conversation or model authority.
"""

from __future__ import annotations

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore

UNAVAILABLE_CODES = frozenset({
    "PAYLOAD_ERASED", "PAYLOAD_INCOMPLETE", "SOURCE_SUPPRESSED", "SOURCE_LINK_SCOPE_LIMIT",
})


class ContextPayloadStore:
    def __init__(self, store):
        self.store = store
        self.payloads = PayloadStore(store)

    def reply_source(self, reply_id):
        return SourceRef("source-unit:" + digest_json([
            self.store.bot_id, "source-reply:" + reply_id,
        ]))

    async def read_parts_in(self, conn, payload_id, *, expected_deletion_epoch, required=False):
        if not payload_id:
            raise ForestError(
                "CONTEXT_PAYLOAD_MIGRATION_REQUIRED", "Canonical content has no payload reference", 409
            )
        try:
            value = await self.payloads.read_json_in(
                conn, payload_id, expected_deletion_epoch=expected_deletion_epoch
            )
        except ForestError as exc:
            if required or exc.code not in UNAVAILABLE_CODES:
                raise
            return {"payload_id": payload_id, "payload_state": exc.code, "value": None}
        if not isinstance(value, list):
            raise ForestError("CONTEXT_PAYLOAD_INVALID", "Canonical parts must be an ordered list", 409)
        return {"payload_id": payload_id, "payload_state": "ACTIVE", "value": value}

    async def read_reply_in(self, conn, reply, *, expected_deletion_epoch, required=False):
        content = await self.read_parts_in(
            conn, reply.get("payload_id"), expected_deletion_epoch=expected_deletion_epoch,
            required=required,
        )
        return {**reply, "payload_state": content["payload_state"], "parts": content["value"]}

    async def put_reply_in(self, conn, reply_id, parts, *, parents, expected_deletion_epoch):
        return await self.payloads.put_json_in(
            conn, owner_kind="reply.parts", owner_id=reply_id, value=parts,
            sources=(self.reply_source(reply_id),), parents=parents,
            expected_deletion_epoch=expected_deletion_epoch,
        )

    async def put_control_in(
        self, conn, unit_id, parts, *, parents, sources=(), expected_deletion_epoch,
    ):
        return await self.payloads.put_json_in(
            conn, owner_kind="context.parts", owner_id=unit_id, value=parts,
            parents=parents, sources=sources, expected_deletion_epoch=expected_deletion_epoch,
        )
