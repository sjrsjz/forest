"""Delivery content lives behind source suppression; effect receipts outlive it."""

import hashlib
import re

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore

UNAVAILABLE = frozenset({
    "PAYLOAD_ERASED", "PAYLOAD_INCOMPLETE", "SOURCE_SUPPRESSED", "SOURCE_LINK_SCOPE_LIMIT",
})


def receipt_metadata(value):
    """Only typed platform identifiers/status survive physical content erasure."""
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in ("message_id", "error_code"):
        item = value.get(key)
        if isinstance(item, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,256}", item):
            result[key] = item
    if value.get("status") in ("SENT", "NOT_SENT", "UNKNOWN", "ok", "failed", "async"):
        result["status"] = value["status"]
    if type(value.get("retcode")) is int and -(2**31) <= value["retcode"] < 2**31:
        result["retcode"] = value["retcode"]
    return result


class DeliveryContentStore:
    def __init__(self, store):
        self.store, self.payloads = store, PayloadStore(store)

    async def read_part_in(self, conn, part, *, expected_deletion_epoch):
        if not part.get("payload_id"):
            raise ForestError(
                "DELIVERY_PAYLOAD_MIGRATION_REQUIRED", "Delivery body has not migrated", 409
            )
        body = await self.payloads.read_json_in(
            conn, part["payload_id"], expected_deletion_epoch=expected_deletion_epoch
        )
        if (
            not isinstance(body, list)
            or digest_json(body).removeprefix("sha256:") != part["payload_digest"]
        ):
            raise ForestError(
                "DELIVERY_PAYLOAD_CONFLICT", "Delivery body differs from its semantic receipt", 409
            )
        return body

    async def read_attempt_in(self, conn, attempt, *, expected_deletion_epoch):
        if not attempt.get("wire_payload_id"):
            raise ForestError(
                "DELIVERY_PAYLOAD_MIGRATION_REQUIRED", "Exact wire body has not migrated", 409
            )
        body = await self.payloads.read_bytes_in(
            conn, attempt["wire_payload_id"], expected_deletion_epoch=expected_deletion_epoch
        )
        if hashlib.sha256(body).hexdigest() != attempt["wire_digest"]:
            raise ForestError(
                "DELIVERY_PAYLOAD_CONFLICT", "Exact wire differs from its permitted digest", 409
            )
        return body

    async def read_receipt_in(self, conn, payload_id, *, expected_deletion_epoch):
        return await self.payloads.read_json_in(
            conn, payload_id, expected_deletion_epoch=expected_deletion_epoch
        )

    async def retain_observation_in(
        self, conn, *, owner_id, value, parent, expected_deletion_epoch
    ):
        """Late old-owner evidence never needs permission to recreate a body."""
        try:
            await self.payloads.require_available_in(
                conn, (parent,), expected_deletion_epoch=expected_deletion_epoch
            )
        except ForestError as exc:
            if exc.code in UNAVAILABLE:
                return None
            raise
        payload = await self.payloads.put_json_in(
            conn,
            owner_kind="delivery.receipt",
            owner_id=owner_id,
            value=value,
            parents=(parent,),
            expected_deletion_epoch=expected_deletion_epoch,
        )
        return payload.payload_id if payload.state == "ACTIVE" else None
