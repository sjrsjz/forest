"""Source-bound, removable arguments for admitted business commands."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore


def owner_id(request_id: str) -> str:
    if not isinstance(request_id, str) or not 1 <= len(request_id.encode()) <= 256:
        raise ForestError("BUSINESS_COMMAND_INVALID", "Command request identity is invalid", 422)
    return "business-command-arguments-v1:" + digest_json([request_id])


class BusinessCommandPayloadStore:
    def __init__(self, store):
        self.store, self.payloads = store, PayloadStore(store)

    async def put_arguments_in(
        self,
        conn,
        *,
        request_id: str,
        command_id: str,
        name: str,
        argument_mode: str,
        argument_text: str,
        input_payload_id: str,
        epoch: int,
    ):
        value = {
            "request_id": request_id,
            "command_id": command_id,
            "name": name,
            "argument_mode": argument_mode,
            "argument_text": argument_text,
        }
        ref = await self.payloads.put_json_in(
            conn,
            owner_kind="command.arguments",
            owner_id=owner_id(request_id),
            value=value,
            parents=(input_payload_id,),
            expected_deletion_epoch=epoch,
        )
        if ref.state != "ACTIVE":
            raise ForestError(
                "CONTROL_CONTENT_REVOKED", "Business command arguments are unavailable", 409
            )
        return ref, digest_json(value)
