"""Generation-fenced JSON projection over the kernel Payload authority."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime

from forest.errors import ForestError
from forest.storage.payload_authority import PayloadSource


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError) as exc:
        raise ForestError("PAYLOAD_JSON_INVALID", "Payload value is not finite JSON", 422) from exc


class KernelPayloadJsonEffectDriver:
    """Read exact JSON references or materialize Generation-owned JSON bytes."""

    def __init__(
        self,
        _configuration,
        _configuration_revision,
        identity,
        declaration,
        *,
        runtime,
        payloads,
    ) -> None:
        name = declaration.get("name")
        kind = declaration.get("kind")
        if not (
            (
                kind == "kernel.payload-json"
                and name
                in {
                    "kernel.payload.json-read",
                    "kernel.payload.json-write",
                    "kernel.effect.json-materialize",
                }
            )
            or (kind == "kernel.payload-bytes" and name == "kernel.payload.bytes-write")
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Payload JSON driver received another effect service",
                500,
            )
        self.identity = identity
        self.declaration = declaration
        self.runtime = runtime
        self.payloads = payloads
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def invoke(self, payload, *, scope, invocation):
        if (
            not self.accepting
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Payload JSON driver is not accepting effects",
                409,
            )
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        if self.declaration["name"] == "kernel.payload.json-read":
            return await self._read(payload, invocation.operation_id)
        if self.declaration["name"] == "kernel.payload.json-write":
            return await self._write(payload, scope.component_id, invocation.operation_id)
        if self.declaration["name"] == "kernel.payload.bytes-write":
            return await self._write_bytes(
                payload, scope.component_id, invocation.operation_id
            )
        return await self._materialize(payload, invocation.operation_id)

    async def _read(self, payload: dict, operation_id: str) -> dict:
        reference = payload.get("payload")
        fields = {
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        }
        if not isinstance(reference, dict) or set(reference) != fields:
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Payload reference is invalid", 422)
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            await self.payloads.verify_in(
                conn,
                payload_id=reference["payload_id"],
                digest=reference["digest"],
                owner_component_id=reference["owner_component_id"],
                owner_kind=reference["owner_kind"],
                owner_id=reference["owner_id"],
            )
            content = await self.payloads.read_bytes_in(
                conn,
                reference["payload_id"],
            )
        try:
            value = json.loads(content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "PAYLOAD_JSON_INVALID", "Payload does not contain valid JSON", 422
            ) from exc
        _canonical(value)
        return {
            "protocol": "forest.payload-json/1",
            "operation_id": operation_id,
            "value": value,
        }

    async def _materialize(self, payload: dict, operation_id: str) -> dict:
        if (
            set(payload) != {"protocol", "owner_kind", "owner_id", "value"}
            or payload.get("protocol") != "forest.effect-json-materialize/1"
        ):
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Effect payload input is invalid", 422)
        if payload["owner_kind"] not in {"effect.wire", "effect.evidence"}:
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Effect payload kind is invalid", 422)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind=payload["owner_kind"],
            owner_id=payload["owner_id"],
            media_type="application/json",
            content=_canonical(payload["value"]),
            expected_deletion_epoch=epoch,
        )
        return {
            "protocol": "forest.effect-json-materialized/1",
            "operation_id": operation_id,
            "payload": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": "kernel.effect",
                "owner_kind": payload["owner_kind"],
                "owner_id": payload["owner_id"],
            },
        }

    async def _write(
        self, payload: dict, owner_component_id: str, operation_id: str
    ) -> dict:
        if (
            set(payload) != {"protocol", "owner_kind", "owner_id", "value"}
            or payload.get("protocol") != "forest.payload-json-write/1"
            or not isinstance(payload.get("owner_kind"), str)
            or not 1 <= len(payload["owner_kind"].encode("utf-8")) <= 128
            or not isinstance(payload.get("owner_id"), str)
            or not 1 <= len(payload["owner_id"].encode("utf-8")) <= 512
        ):
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Payload write input is invalid", 422)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id=owner_component_id,
            owner_kind=payload["owner_kind"],
            owner_id=payload["owner_id"],
            media_type="application/json",
            content=_canonical(payload["value"]),
            expected_deletion_epoch=epoch,
        )
        return {
            "protocol": "forest.payload-json-written/1",
            "operation_id": operation_id,
            "payload": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": owner_component_id,
                "owner_kind": payload["owner_kind"],
                "owner_id": payload["owner_id"],
            },
        }

    async def _write_bytes(
        self, payload: dict, owner_component_id: str, operation_id: str
    ) -> dict:
        fields = {
            "protocol",
            "owner_kind",
            "owner_id",
            "media_type",
            "content_base64",
            "parents",
            "sources",
        }
        if (
            set(payload) != fields
            or payload.get("protocol") != "forest.payload-bytes-write/1"
            or not isinstance(payload.get("owner_kind"), str)
            or not 1 <= len(payload["owner_kind"].encode("utf-8")) <= 128
            or not isinstance(payload.get("owner_id"), str)
            or not 1 <= len(payload["owner_id"].encode("utf-8")) <= 512
            or not isinstance(payload.get("media_type"), str)
            or not 1 <= len(payload["media_type"].encode("utf-8")) <= 128
            or not isinstance(payload.get("parents"), list)
            or not all(isinstance(item, str) for item in payload["parents"])
            or not isinstance(payload.get("sources"), list)
        ):
            raise ForestError("PAYLOAD_BYTES_INPUT_INVALID", "Payload write input is invalid", 422)
        try:
            content = base64.b64decode(payload["content_base64"], validate=True)
            sources = tuple(
                PayloadSource(
                    source_component_id=item["source_component_id"],
                    source_kind=item["source_kind"],
                    source_id=item["source_id"],
                    source_revision=item["source_revision"],
                    part_id=item.get("part_id"),
                    source_trunk_id=item.get("source_trunk_id"),
                    observation_conversation_id=item.get("observation_conversation_id"),
                    observation_binding_id=item.get("observation_binding_id"),
                    author_subject_id=item.get("author_subject_id"),
                    occurred_at=datetime.fromisoformat(item["occurred_at"]),
                )
                for item in payload["sources"]
            )
        except (KeyError, TypeError, ValueError, binascii.Error) as exc:
            raise ForestError(
                "PAYLOAD_BYTES_INPUT_INVALID", "Payload bytes or provenance is invalid", 422
            ) from exc
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id=owner_component_id,
            owner_kind=payload["owner_kind"],
            owner_id=payload["owner_id"],
            media_type=payload["media_type"],
            content=content,
            sources=sources,
            parents=tuple(payload["parents"]),
            expected_deletion_epoch=epoch,
        )
        return {
            "protocol": "forest.payload-bytes-written/1",
            "operation_id": operation_id,
            "byte_length": len(content),
            "payload": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": owner_component_id,
                "owner_kind": payload["owner_kind"],
                "owner_id": payload["owner_id"],
            },
        }

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False


def kernel_payload_json_effect_driver_factory(*, runtime, payloads):
    def create(configuration, configuration_revision, identity, declaration):
        return KernelPayloadJsonEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            runtime=runtime,
            payloads=payloads,
        )

    return create
