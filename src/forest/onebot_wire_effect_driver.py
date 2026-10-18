"""Host byte materialization for an Adapter-owned OneBot message template."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from types import SimpleNamespace

from forest.adapters.onebot import MAX_SEND_BYTES, PreparedSend
from forest.errors import ForestError


class OneBotWireEffectDriver:
    def __init__(self, identity, *, runtime, payloads):
        self.identity, self.runtime, self.payloads = identity, runtime, payloads
        self.accepting = False

    async def start(self):
        self.accepting = True

    async def quiesce(self):
        self.accepting = False

    async def resume(self):
        self.accepting = True

    async def close(self):
        self.accepting = False

    async def invoke(self, payload, *, scope, invocation):
        if (not self.accepting or scope.component_id != self.identity.component_id
                or scope.generation_id != self.identity.generation_id):
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "OneBot wire materializer is not active", 409)
        await self.runtime.verify_generation_member(component_id=self.identity.component_id,
                                                     generation_id=self.identity.generation_id)
        body = copy.deepcopy(payload["body"])
        images = [part for part in body["message"] if part["type"] == "image"]
        references = []
        for image in images:
            value = image["data"].get("file")
            if (set(image) != {"type", "data"} or set(image["data"]) != {"file"}
                    or not isinstance(value, dict) or set(value) != {"payload"}):
                raise ForestError("ONEBOT_MEDIA_REFERENCE_INVALID", "Wire image requires an exact managed Payload", 422)
            ref = value["payload"]
            if (set(ref) != {"payload_id", "digest", "owner_component_id", "owner_kind", "owner_id"}
                    or ref["owner_component_id"] != "media.capture" or ref["owner_kind"] != "media.asset"):
                raise ForestError("ONEBOT_MEDIA_REFERENCE_INVALID", "Wire image is not a managed asset", 422)
            references.append(ref)
        epoch = await self.payloads.epoch()
        total = 0
        for image, ref in zip(images, references, strict=True):
            (content,) = await self.payloads.read_verified_batch((SimpleNamespace(**ref),))
            if not 0 < len(content) <= 8 * 1024 * 1024:
                raise ForestError("ONEBOT_MEDIA_SIZE_EXCEEDED", "Managed image exceeds the send limit", 413)
            total += (len(content) + 2) // 3 * 4
            if total > MAX_SEND_BYTES:
                raise ForestError("ONEBOT_MEDIA_SIZE_EXCEEDED", "Images exceed the aggregate send limit", 413)
            image["data"] = {"file": "base64://" + base64.b64encode(content).decode(), "proxy": "0", "timeout": "10"}
        wire = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        prepared = PreparedSend(payload["action"], wire, hashlib.sha256(wire).hexdigest())
        saved = await self.payloads.put_bytes(
            owner_component_id="kernel.effect", owner_kind="effect.wire", owner_id=payload["wire_owner_id"],
            media_type="application/json", content=wire, parents=tuple(dict.fromkeys(ref["payload_id"] for ref in references)),
            expected_deletion_epoch=epoch,
        )
        return {"action": prepared.action, "wire_digest": prepared.wire_digest, "wire_payload": {
            "payload_id": saved.payload_id, "digest": saved.content_digest,
            "owner_component_id": "kernel.effect", "owner_kind": "effect.wire", "owner_id": payload["wire_owner_id"],
        }}
